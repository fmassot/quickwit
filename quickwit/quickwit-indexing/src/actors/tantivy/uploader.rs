// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{ActorContext, ActorExitStatus, Handler};
use quickwit_common::pubsub::EventBroker;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{SplitMaturity, SplitMetadata, StageSplitsRequestExt};
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, SplitRecoveryMetadata, StageSplitsRequest,
};
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use quickwit_storage::{SplitPayload, SplitPayloadBuilder};
use tracing::instrument;

use super::publisher::TantivyPublication;
use crate::actors::uploader::{
    PublicationMailbox, UploadBudget, UploadEngine, Uploader, UploaderCounters, UploaderType,
};
use crate::merge_policy::MergePolicy;
use crate::models::{
    EmptySplit, PackagedSplit, PackagedSplitBatch, PublishLock, SplitsUpdate, create_split_metadata,
};
use crate::split_store::IndexingSplitStore;

static BUDGET: UploadBudget = UploadBudget::new("indexer", "merger");

#[derive(Clone)]
pub struct TantivyUpload {
    metastore: MetastoreServiceClient,
    merge_policy: Arc<dyn MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    split_store: IndexingSplitStore,
    event_broker: EventBroker,
}

pub type TantivyUploader = Uploader<TantivyUpload>;
pub type SplitsUpdateMailbox = PublicationMailbox<TantivyPublication>;

pub struct PreparedTantivyUpload {
    batch: PackagedSplitBatch,
    splits: Vec<(SplitMetadata, SplitPayload)>,
}

impl TantivyUploader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        role: UploaderType,
        metastore: MetastoreServiceClient,
        merge_policy: Arc<dyn MergePolicy>,
        retention_policy: Option<RetentionPolicy>,
        split_store: IndexingSplitStore,
        destination: SplitsUpdateMailbox,
        maximum: usize,
        event_broker: EventBroker,
    ) -> Self {
        Self::for_engine(
            TantivyUpload {
                metastore,
                merge_policy,
                retention_policy,
                split_store,
                event_broker,
            },
            role,
            destination,
            maximum,
        )
    }
}

#[async_trait]
impl UploadEngine for TantivyUpload {
    type Publisher = TantivyPublication;
    type Batch = PackagedSplitBatch;
    type Prepared = PreparedTantivyUpload;
    const QUEUE_CAPACITY: usize = 0;
    fn actor_name(role: UploaderType) -> String {
        format!("{role:?}")
    }
    fn budget() -> &'static UploadBudget {
        &BUDGET
    }
    fn publish_lock(batch: &Self::Batch) -> &PublishLock {
        &batch.publish_lock
    }
    fn split_count(prepared: &Self::Prepared) -> usize {
        prepared.splits.len()
    }

    fn prepare(&self, batch: Self::Batch) -> anyhow::Result<Self::Prepared> {
        fail_point!("uploader:intask:before", |_| Err(anyhow::anyhow!(
            "uploader:intask:before"
        )));
        anyhow::ensure!(
            !batch.splits.is_empty(),
            "packaged split batch must not be empty"
        );
        let index_uid = batch.index_uid();
        anyhow::ensure!(
            batch
                .splits
                .iter()
                .all(|split| split.index_uid() == &index_uid),
            "mixed index incarnations in upload batch"
        );
        let splits = batch
            .splits
            .iter()
            .map(|split| {
                prepare_split_for_upload(split, &self.merge_policy, self.retention_policy.as_ref())
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(PreparedTantivyUpload { batch, splits })
    }

    async fn stage(&self, prepared: &Self::Prepared) -> anyhow::Result<()> {
        let metadata: Vec<_> = prepared
            .splits
            .iter()
            .map(|(metadata, _)| metadata.clone())
            .collect();
        let request =
            StageSplitsRequest::try_from_splits_metadata(prepared.batch.index_uid(), metadata)?;
        self.metastore.stage_splits(request).await?;
        Ok(())
    }

    async fn upload(
        &self,
        prepared: Self::Prepared,
        counters: &UploaderCounters,
    ) -> anyhow::Result<SplitsUpdate> {
        let PreparedTantivyUpload { batch, splits } = prepared;
        let index_uid = batch.index_uid();
        let report_splits = batch
            .splits
            .iter()
            .map(|split| ReportSplit {
                storage_uri: self.split_store.remote_uri().to_string(),
                split_id: split.split_id().to_string(),
            })
            .collect();
        self.event_broker
            .publish(ReportSplitsRequest { report_splits });
        let replaced_split_ids = batch
            .splits
            .iter()
            .flat_map(|split| split.split_attrs.replaced_split_ids.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut new_splits = Vec::with_capacity(splits.len());
        for (packaged, (metadata, payload)) in batch.splits.into_iter().zip(splits) {
            self.split_store
                .store_split(
                    &metadata,
                    packaged.split_scratch_directory.path(),
                    Box::new(payload),
                )
                .await?;
            counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
            new_splits.push(metadata);
        }
        Ok(SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            checkpoint_delta_opt: batch.checkpoint_delta_opt,
            publish_lock: batch.publish_lock,
            merge_task: batch.merge_task_opt,
            parent_span: batch.batch_parent_span,
        })
    }
}

#[async_trait]
impl Handler<PackagedSplitBatch> for TantivyUploader {
    type Reply = ();
    #[instrument(name = "uploader", parent = batch.batch_parent_span.id(), skip_all)]
    async fn handle(
        &mut self,
        batch: PackagedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("uploader:before");
        self.upload(batch, ctx).await?;
        fail_point!("uploader:intask:after");
        Ok(())
    }
}

#[async_trait]
impl Handler<EmptySplit> for TantivyUploader {
    type Reply = ();
    async fn handle(
        &mut self,
        empty: EmptySplit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.forward(
            SplitsUpdate {
                index_uid: empty.index_uid,
                new_splits: Vec::new(),
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: Some(empty.checkpoint_delta),
                publish_lock: empty.publish_lock,
                merge_task: None,
                parent_span: empty.batch_parent_span,
            },
            ctx,
        )
        .await?;
        Ok(())
    }
}

fn create_split_recovery_metadata(
    split: &SplitMetadata,
    parents: &[quickwit_proto::types::SplitId],
) -> SplitRecoveryMetadata {
    let maturation_period_millis = match split.maturity {
        SplitMaturity::Mature => None,
        SplitMaturity::Immature { maturation_period } => Some(
            maturation_period
                .as_millis()
                .try_into()
                .expect("maturation period should fit in u64 milliseconds"),
        ),
    };
    SplitRecoveryMetadata {
        split_id: split.split_id.to_string(),
        index_uid: Some(split.index_uid.clone()),
        source_id: split.source_id.clone(),
        node_id: split.node_id.clone(),
        doc_mapping_uid: Some(split.doc_mapping_uid),
        partition_id: split.partition_id,
        num_docs: split.num_docs as u64,
        uncompressed_docs_size_bytes: split.uncompressed_docs_size_in_bytes,
        time_range_start_inclusive: split.time_range.as_ref().map(|range| *range.start()),
        time_range_end_inclusive: split.time_range.as_ref().map(|range| *range.end()),
        create_timestamp: split.create_timestamp,
        tags: split.tags.iter().cloned().collect(),
        delete_opstamp: split.delete_opstamp,
        num_merge_ops: split.num_merge_ops as u64,
        parent_split_ids: parents.iter().map(ToString::to_string).collect(),
        maturation_period_millis,
    }
}

fn prepare_split_for_upload(
    packaged: &PackagedSplit,
    policy: &Arc<dyn MergePolicy>,
    retention: Option<&RetentionPolicy>,
) -> anyhow::Result<(SplitMetadata, SplitPayload)> {
    let metadata = create_split_metadata(
        policy,
        retention,
        &packaged.split_attrs,
        packaged.tags.clone(),
        Default::default(),
    );
    let recovery =
        create_split_recovery_metadata(&metadata, &packaged.split_attrs.replaced_split_ids)
            .serialize();
    let payload = SplitPayloadBuilder::get_split_payload(
        &packaged.split_files,
        &packaged.serialized_split_fields,
        Some(&recovery),
        &packaged.hotcache_bytes,
    )?;
    let metadata = SplitMetadata {
        footer_offsets: payload.footer_range.clone(),
        ..metadata
    };
    Ok((metadata, payload))
}
