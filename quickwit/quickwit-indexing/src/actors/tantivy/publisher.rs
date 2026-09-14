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
use fail::fail_point;
use quickwit_actors::{Mailbox, QueueCapacity};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::{
    MetastoreResult, MetastoreService, MetastoreServiceClient, PublishSplitsRequest, serde_utils,
};
use tracing::info;

use crate::actors::MergePlanner;
use crate::actors::publisher::{Publication, PublicationEngine, Publisher};
use crate::merge_policy::MergeTask;
use crate::metrics::record_published_split;
use crate::models::{NewSplits, SharedPublishToken};
use crate::source::SourceActor;

pub(crate) const PUBLISHER_NAME: &str = "Publisher";
pub const MERGE_PUBLISHER_NAME: &str = "MergePublisher";

#[derive(Clone)]
pub struct TantivyPublication {
    metastore: MetastoreServiceClient,
}

pub type TantivyPublisher = Publisher<TantivyPublication>;

impl TantivyPublisher {
    pub fn new(
        name: &'static str,
        queue_capacity: QueueCapacity,
        metastore: MetastoreServiceClient,
        planner: Option<Mailbox<MergePlanner>>,
        source: Option<Mailbox<SourceActor>>,
        token: SharedPublishToken,
    ) -> Self {
        Self::for_engine(
            TantivyPublication { metastore },
            name,
            queue_capacity,
            planner,
            source,
            token,
        )
    }
}

#[async_trait]
impl PublicationEngine for TantivyPublication {
    type Split = SplitMetadata;
    type MergeTask = MergeTask;
    type NewSplits = NewSplits;
    type Planner = MergePlanner;

    fn split_id(split: &Self::Split) -> &str {
        split.split_id.as_str()
    }
    fn validate(&self, _: &Publication<Self>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish(
        &self,
        update: &Publication<Self>,
        token: Option<String>,
    ) -> MetastoreResult<()> {
        let request = PublishSplitsRequest {
            index_uid: Some(update.index_uid.clone()),
            staged_split_ids: update
                .new_splits
                .iter()
                .map(|split| split.split_id.to_string())
                .collect(),
            replaced_split_ids: update.replaced_split_ids.iter().map(String::from).collect(),
            index_checkpoint_delta_json_opt: update
                .checkpoint_delta_opt
                .as_ref()
                .map(serde_utils::to_json_str)
                .transpose()?,
            publish_token_opt: token,
        };
        self.metastore.publish_splits(request).await?;
        Ok(())
    }

    fn record_published(&self, update: &Publication<Self>) {
        for split in &update.new_splits {
            record_published_split(&update.index_uid.index_id, split);
        }
        let num_docs: usize = update.new_splits.iter().map(|split| split.num_docs).sum();
        let split_size_bytes: u64 = update
            .new_splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        info!(
            num_splits = update.new_splits.len(),
            num_docs, split_size_bytes, "publish-new-splits"
        );
    }

    fn new_splits(new_splits: Vec<Self::Split>) -> Self::NewSplits {
        NewSplits { new_splits }
    }
    fn before_publish(&self) -> anyhow::Result<()> {
        fail_point!("publisher:before", |_| Err(anyhow::anyhow!(
            "publisher:before"
        )));
        Ok(())
    }
    fn after_publish(&self) -> anyhow::Result<()> {
        fail_point!("publisher:after", |_| Err(anyhow::anyhow!(
            "publisher:after"
        )));
        Ok(())
    }
}
