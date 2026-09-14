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

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_metastore::ParquetSplits;
use quickwit_parquet_engine::merge::policy::ParquetMergePolicy;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::Storage;
use tracing::{Span, instrument, warn};

use super::{ParquetPublicationEngine, ParquetPublisher, ParquetSplitBatch, ParquetSplitsUpdate};
use crate::actors::Sequencer;
use crate::actors::uploader::{
    UploadBudget, UploadEngine, Uploader, UploaderCounters, UploaderType,
};
use crate::models::PublishLock;

static BUDGET: UploadBudget = UploadBudget::new("metrics_indexer", "metrics_merger");

#[derive(Clone)]
pub struct ParquetUpload {
    metastore: MetastoreServiceClient,
    split_store: Arc<dyn Storage>,
    merge_policy: Arc<dyn ParquetMergePolicy>,
}

/// Parquet storage operations with the shared upload lifecycle.
///
/// ```
/// use quickwit_actors::Handler;
/// use quickwit_indexing::actors::{ParquetUploader, ParquetSplitBatch};
/// fn accepts<A: Handler<ParquetSplitBatch>>() {}
/// accepts::<ParquetUploader>();
/// ```
///
/// Upload edges cannot mix engines:
/// ```compile_fail,E0277
/// use quickwit_actors::Handler;
/// use quickwit_indexing::actors::{Uploader, ParquetSplitBatch};
/// fn accepts<A: Handler<ParquetSplitBatch>>() {}
/// accepts::<Uploader>();
/// ```
pub type ParquetUploader = Uploader<ParquetUpload>;

impl ParquetUploader {
    pub fn new(
        role: UploaderType,
        metastore: MetastoreServiceClient,
        split_store: Arc<dyn Storage>,
        sequencer: Mailbox<Sequencer<ParquetPublisher>>,
        maximum: usize,
        merge_policy: Arc<dyn ParquetMergePolicy>,
    ) -> Self {
        Self::for_engine(
            ParquetUpload {
                metastore,
                split_store,
                merge_policy,
            },
            role,
            sequencer.into(),
            maximum,
        )
    }
}

#[async_trait]
impl UploadEngine for ParquetUpload {
    type Publisher = ParquetPublicationEngine;
    type Batch = ParquetSplitBatch;
    type Prepared = ParquetSplitBatch;
    const QUEUE_CAPACITY: usize = 3;
    fn actor_name(role: UploaderType) -> String {
        format!("ParquetUploader({role:?})")
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
    fn prepare(&self, mut batch: Self::Batch) -> anyhow::Result<Self::Prepared> {
        for split in &mut batch.splits {
            split.maturity = self
                .merge_policy
                .split_maturity(split.size_bytes, split.num_merge_ops);
        }
        Ok(batch)
    }
    async fn stage(&self, batch: &Self::Prepared) -> anyhow::Result<()> {
        let first = batch
            .splits
            .first()
            .context("empty batch must use checkpoint-only delivery")?;
        ParquetSplits::new(self.metastore.clone(), batch.index_uid.clone(), first.kind)
            .stage(&batch.splits)
            .await?;
        Ok(())
    }
    async fn upload(
        &self,
        batch: Self::Prepared,
        counters: &UploaderCounters,
    ) -> anyhow::Result<ParquetSplitsUpdate> {
        // Keep scratch files alive through the last read. The merge guard goes on to publication.
        let scratch = batch._scratch_directory_opt;
        for split in &batch.splits {
            let filename = split.parquet_filename();
            let path = batch.output_dir.join(&filename);
            let content = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            self.split_store
                .put(Path::new(&filename), Box::new(content))
                .await?;
            counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
            if let Err(error) = tokio::fs::remove_file(&path).await {
                warn!(%error, local_path = %path.display(), "failed to remove uploaded scratch file");
            }
        }
        if !batch.replaced_split_ids.is_empty() {
            for split in &batch.splits {
                record_merge_pipeline_event(&MergePipelineEvent::UploadMergeOutput {
                    index_uid: batch.index_uid.to_string(),
                    merge_id: split.split_id.to_string(),
                    output_split_id: split.split_id.to_string(),
                    output_num_rows: split.num_rows,
                    output_window: split.window.clone().unwrap_or(
                        split.time_range.start_secs as i64..split.time_range.end_secs as i64,
                    ),
                    output_merge_ops: split.num_merge_ops,
                });
            }
        }
        drop(scratch);
        Ok(ParquetSplitsUpdate {
            index_uid: batch.index_uid,
            new_splits: batch.splits,
            replaced_split_ids: batch.replaced_split_ids,
            checkpoint_delta_opt: batch.checkpoint_delta_opt,
            publish_lock: batch.publish_lock,
            parent_span: Span::current(),
            merge_task: batch._merge_task_opt,
        })
    }
}

#[async_trait]
impl Handler<ParquetSplitBatch> for ParquetUploader {
    type Reply = ();
    #[instrument(name = "parquet_uploader", skip_all)]
    async fn handle(
        &mut self,
        batch: ParquetSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if batch.splits.is_empty() {
            self.forward(
                ParquetSplitsUpdate {
                    index_uid: batch.index_uid,
                    new_splits: Vec::new(),
                    replaced_split_ids: batch.replaced_split_ids,
                    checkpoint_delta_opt: batch.checkpoint_delta_opt,
                    publish_lock: batch.publish_lock,
                    parent_span: Span::current(),
                    merge_task: batch._merge_task_opt,
                },
                ctx,
            )
            .await?;
            return Ok(());
        }
        self.upload(batch, ctx).await
    }
}
