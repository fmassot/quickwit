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

use async_trait::async_trait;
use quickwit_actors::{Mailbox, QueueCapacity};
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_metastore::{ParquetPublication, ParquetSplits};
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::{MetastoreResult, MetastoreServiceClient};

use super::{ParquetMergePlanner, ParquetMergeTask, ParquetNewSplits};
use crate::actors::publisher::{Publication, PublicationEngine, Publisher};
use crate::models::SharedPublishToken;
use crate::source::SourceActor;

pub(crate) const METRICS_PUBLISHER_NAME: &str = "ParquetPublisher";

#[derive(Clone)]
pub struct ParquetPublicationEngine {
    metastore: MetastoreServiceClient,
    kind: ParquetSplitKind,
}

/// A Parquet-only publisher, including its feedback edge.
///
/// ```
/// use quickwit_actors::{Handler, Mailbox};
/// use quickwit_indexing::actors::{ParquetPublisher, ParquetSplitsUpdate, ParquetMergePlanner};
/// fn accepts<A: Handler<ParquetSplitsUpdate>>() {}
/// accepts::<ParquetPublisher>();
/// fn connect(publisher: ParquetPublisher, planner: Mailbox<ParquetMergePlanner>) {
///     publisher.with_merge_planner(planner);
/// }
/// ```
///
/// A Tantivy publisher cannot accept Parquet publications:
/// ```compile_fail,E0277
/// use quickwit_actors::Handler;
/// use quickwit_indexing::actors::{Publisher, ParquetSplitsUpdate};
/// fn accepts<A: Handler<ParquetSplitsUpdate>>() {}
/// accepts::<Publisher>();
/// ```
///
/// Nor can it connect to a Parquet merge planner:
/// ```compile_fail,E0308
/// use quickwit_actors::Mailbox;
/// use quickwit_indexing::actors::{Publisher, ParquetMergePlanner};
/// fn connect(publisher: Publisher, planner: Mailbox<ParquetMergePlanner>) {
///     publisher.with_merge_planner(planner);
/// }
/// ```
pub type ParquetPublisher = Publisher<ParquetPublicationEngine>;

impl ParquetPublisher {
    pub fn new_parquet(
        kind: ParquetSplitKind,
        queue_capacity: QueueCapacity,
        metastore: MetastoreServiceClient,
        source: Option<Mailbox<SourceActor>>,
        token: SharedPublishToken,
    ) -> Self {
        Self::for_engine(
            ParquetPublicationEngine { metastore, kind },
            METRICS_PUBLISHER_NAME,
            queue_capacity,
            None,
            source,
            token,
        )
    }
}

#[async_trait]
impl PublicationEngine for ParquetPublicationEngine {
    type Split = ParquetSplitMetadata;
    type MergeTask = ParquetMergeTask;
    type NewSplits = ParquetNewSplits;
    type Planner = ParquetMergePlanner;

    fn split_id(split: &Self::Split) -> &str {
        split.split_id.as_str()
    }
    fn validate(&self, update: &Publication<Self>) -> anyhow::Result<()> {
        let index_uid = update.index_uid.to_string();
        anyhow::ensure!(
            update
                .new_splits
                .iter()
                .all(|split| split.kind == self.kind && split.index_uid == index_uid),
            "split identity does not match the Parquet publication"
        );
        Ok(())
    }

    async fn publish(
        &self,
        update: &Publication<Self>,
        token: Option<String>,
    ) -> MetastoreResult<()> {
        let catalog =
            ParquetSplits::new(self.metastore.clone(), update.index_uid.clone(), self.kind);
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: update
                    .new_splits
                    .iter()
                    .map(|split| split.split_id.to_string())
                    .collect(),
                replaced_split_ids: update.replaced_split_ids.iter().map(String::from).collect(),
                checkpoint_delta: update.checkpoint_delta_opt.clone(),
                publish_token: token,
            })
            .await
    }

    fn record_published(&self, update: &Publication<Self>) {
        tracing::info!("publish-parquet-splits");
        for split in &update.new_splits {
            let window = split
                .window
                .clone()
                .unwrap_or(split.time_range.start_secs as i64..split.time_range.end_secs as i64);
            let event = if update.replaced_split_ids.is_empty() {
                MergePipelineEvent::IngestSplit {
                    index_uid: update.index_uid.to_string(),
                    split_id: split.split_id.to_string(),
                    num_rows: split.num_rows,
                    window,
                }
            } else {
                MergePipelineEvent::PublishMergeAndFeedback {
                    index_uid: update.index_uid.to_string(),
                    merge_id: split.split_id.to_string(),
                    output_split_id: split.split_id.to_string(),
                    replaced_split_ids: update
                        .replaced_split_ids
                        .iter()
                        .map(String::from)
                        .collect(),
                    output_window: window,
                    output_merge_ops: split.num_merge_ops,
                }
            };
            record_merge_pipeline_event(&event);
        }
    }

    fn new_splits(new_splits: Vec<Self::Split>) -> Self::NewSplits {
        ParquetNewSplits { new_splits }
    }
}
