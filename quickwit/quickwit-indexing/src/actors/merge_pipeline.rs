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

//! Tantivy merge graph and metastore reseeding. Supervision is engine-independent.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    ActorContext, ActorHandle, HEARTBEAT, Inbox, Mailbox, QueueCapacity, SpawnContext,
};
use quickwit_common::io::{IoControls, Limiter};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::RetentionPolicy;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_metrics::{counter, label_values};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{
    ListSplitsRequest, MetastoreResult, MetastoreService, MetastoreServiceClient,
};
use time::OffsetDateTime;
use tokio::sync::Semaphore;

use super::merge_planner::RunFinalizeMergePolicyAndQuit;
pub use super::pipeline_supervisor::FinishPendingMergesAndShutdownPipeline;
use super::pipeline_supervisor::{
    DrainablePipeline, MERGE_SPAWN_SEMAPHORE, Pipeline, PipelineActors, PipelineSupervisor,
};
use super::publisher::DisconnectMergePlanner;
use super::{
    MergeExecutor, MergePlanner, MergeSchedulerService, MergeSplitDownloader, Packager, Publisher,
    Uploader, UploaderType,
};
use crate::merge_policy::MergePolicy;
use crate::metrics::{ACTOR_NAME, BACKPRESSURE_MICROS, ONGOING_MERGE_OPERATIONS};
use crate::models::{MergeStatistics, SharedPublishToken};
use crate::split_store::IndexingSplitStore;
pub type MergePipeline = PipelineSupervisor<TantivyMerge>;

pub struct TantivyMerge {
    params: MergePipelineParams,
    merge_planner_mailbox: Mailbox<MergePlanner>,
    merge_planner_inbox: Inbox<MergePlanner>,
    initial_immature_splits_opt: Option<Vec<SplitMetadata>>,
}

pub struct TantivyMergeRunning {
    planner: Arc<ActorHandle<MergePlanner>>,
    uploader: Arc<ActorHandle<Uploader>>,
    publisher: Arc<ActorHandle<Publisher>>,
}

impl MergePipeline {
    /// Reuse the planner mailbox across generations so existing publishers keep their feedback
    /// path.
    pub fn new(
        params: MergePipelineParams,
        initial_immature_splits_opt: Option<Vec<SplitMetadata>>,
        spawn_ctx: &SpawnContext,
    ) -> Self {
        let (merge_planner_mailbox, merge_planner_inbox) = spawn_ctx
            .create_mailbox::<MergePlanner>("MergePlanner", MergePlanner::queue_capacity());
        Self::from_pipeline(TantivyMerge {
            params,
            merge_planner_mailbox,
            merge_planner_inbox,
            initial_immature_splits_opt,
        })
    }

    pub fn merge_planner_mailbox(&self) -> &Mailbox<MergePlanner> {
        &self.pipeline.merge_planner_mailbox
    }
}

#[async_trait]
impl Pipeline for TantivyMerge {
    type Statistics = MergeStatistics;
    type Running = TantivyMergeRunning;
    const NAME: &'static str = "MergePipeline";

    fn index_uid(&self) -> &quickwit_proto::types::IndexUid {
        &self.params.pipeline_id.index_uid
    }

    fn spawn_semaphore(&self) -> &'static Semaphore {
        &MERGE_SPAWN_SEMAPHORE
    }
    fn restart_delay(&self) -> Duration {
        *HEARTBEAT
    }

    async fn spawn(
        &mut self,
        ctx: &ActorContext<MergePipeline>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running> {
        let immature_splits = self.fetch_immature_splits(ctx).await?;
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let publisher = Publisher::new(
            super::MERGE_PUBLISHER_NAME,
            QueueCapacity::Unbounded,
            self.params.metastore.clone(),
            Some(self.merge_planner_mailbox.clone()),
            None,
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "merge_publisher")])), publisher,
        );
        let (uploader_mailbox, uploader) = actors.spawn(
            ctx.spawn_actor(),
            Uploader::new(
                UploaderType::MergeUploader,
                self.params.metastore.clone(),
                self.params.merge_policy.clone(),
                self.params.retention_policy.clone(),
                self.params.split_store.clone(),
                publisher_mailbox.into(),
                self.params.max_concurrent_split_uploads,
                self.params.event_broker.clone(),
            ),
        );
        let (packager_mailbox, _) = actors.spawn(
            ctx.spawn_actor(),
            Packager::new("MergePackager", tag_fields, uploader_mailbox),
        );
        // The downloader and merger share the same throughput limiter.
        let io_controls = IoControls::default()
            .set_throughput_limiter_opt(self.params.merge_io_throughput_limiter_opt.clone())
            .set_component("split_downloader_merge");
        let executor = MergeExecutor::new(
            self.params.pipeline_id.clone(),
            self.params.metastore.clone(),
            self.params.doc_mapper.clone(),
            io_controls.clone().set_component("merger"),
            packager_mailbox,
            None,
        );
        let (executor_mailbox, _) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "merge_executor")])), executor,
        );
        let downloader = MergeSplitDownloader {
            scratch_directory: self.params.indexing_directory.clone(),
            split_store: self.params.split_store.clone(),
            executor_mailbox,
            io_controls,
        };
        let (downloader_mailbox, _) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "merge_split_downloader")])), downloader,
        );
        let planner = MergePlanner::new(
            &self.params.pipeline_id,
            immature_splits,
            self.params.merge_policy.clone(),
            downloader_mailbox,
            self.params.merge_scheduler_service.clone(),
        );
        let (_, planner) = actors.spawn(
            ctx.spawn_actor().set_mailboxes(
                self.merge_planner_mailbox.clone(),
                self.merge_planner_inbox.clone(),
            ),
            planner,
        );
        Ok(TantivyMergeRunning {
            planner,
            uploader,
            publisher,
        })
    }

    fn observe(&self, running: &Self::Running, previous: MergeStatistics) -> MergeStatistics {
        running.uploader.refresh_observe();
        running.publisher.refresh_observe();
        previous
            .add_actor_counters(
                &running.uploader.last_observation(),
                &running.publisher.last_observation(),
            )
            .set_ongoing_merges(ONGOING_MERGE_OPERATIONS.get().max(0.0) as usize)
    }
}

#[async_trait]
impl DrainablePipeline for TantivyMerge {
    async fn drain(&self, running: &Self::Running) -> anyhow::Result<()> {
        running
            .publisher
            .mailbox()
            .send_message(DisconnectMergePlanner)
            .await?;
        running
            .planner
            .mailbox()
            .send_message(RunFinalizeMergePolicyAndQuit)
            .await?;
        Ok(())
    }
}

impl TantivyMerge {
    async fn fetch_immature_splits(
        &mut self,
        ctx: &ActorContext<MergePipeline>,
    ) -> MetastoreResult<Vec<SplitMetadata>> {
        if let Some(splits) = self.initial_immature_splits_opt.take() {
            return Ok(splits);
        }
        let query = ListSplitsQuery::for_index(self.params.pipeline_id.index_uid.clone())
            .with_node_id(self.params.pipeline_id.node_id.clone())
            .with_split_state(SplitState::Published)
            .retain_immature(OffsetDateTime::now_utc());
        let request = ListSplitsRequest::try_from_list_splits_query(&query)?;
        let stream = ctx
            .protect_future(self.params.metastore.list_splits(request))
            .await?;
        ctx.protect_future(stream.collect_splits_metadata()).await
    }
}

#[derive(Clone)]
pub struct MergePipelineParams {
    pub pipeline_id: MergePipelineId,
    pub doc_mapper: Arc<DocMapper>,
    pub indexing_directory: TempDirectory,
    pub metastore: MetastoreServiceClient,
    pub merge_scheduler_service: Mailbox<MergeSchedulerService>,
    pub split_store: IndexingSplitStore,
    pub merge_policy: Arc<dyn MergePolicy>,
    pub retention_policy: Option<RetentionPolicy>,
    pub max_concurrent_split_uploads: usize,
    pub merge_io_throughput_limiter_opt: Option<Limiter>,
    pub event_broker: EventBroker,
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::ServiceStream;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_metastore::ListSplitsRequestExt;
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, NodeId};
    use quickwit_storage::RamStorage;

    use super::{MergePipeline, MergePipelineParams};
    use crate::IndexingSplitStore;
    use crate::actors::{MergePlanner, Publisher};
    use crate::merge_policy::default_merge_policy;

    #[tokio::test]
    async fn test_merge_pipeline_simple() -> anyhow::Result<()> {
        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let pipeline_id = MergePipelineId {
            index_uid: index_uid.clone(),
            source_id,
            node_id,
        };
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_splits()
            .times(1)
            .withf(move |list_splits_request| {
                let list_split_query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(list_split_query.index_uids, Some(vec![index_uid.clone()]));
                assert_eq!(
                    list_split_query.split_states,
                    vec![quickwit_metastore::SplitState::Published]
                );
                let Bound::Excluded(_) = list_split_query.mature else {
                    panic!("expected `Bound::Excluded`");
                };
                true
            })
            .returning(|_| Ok(ServiceStream::empty()));
        let universe = Universe::with_accelerated_time();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let pipeline_params = MergePipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            indexing_directory: TempDirectory::for_test(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            merge_scheduler_service: universe.get_or_spawn_one(),
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads: 2,
            merge_io_throughput_limiter_opt: None,
            event_broker: Default::default(),
        };
        let pipeline = MergePipeline::new(pipeline_params, None, universe.spawn_ctx());
        let _merge_planner_mailbox = pipeline.merge_planner_mailbox().clone();
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        pipeline_mailbox
            .ask(super::FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();

        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert!(matches!(pipeline_exit_status, ActorExitStatus::Success));

        // Checking that the merge pipeline actors have been properly cleaned up.
        assert!(universe.get_one::<MergePlanner>().is_none());
        assert!(universe.get_one::<Publisher>().is_none());
        assert!(universe.get_one::<MergePipeline>().is_none());

        universe.assert_quit().await;
        Ok(())
    }
}
