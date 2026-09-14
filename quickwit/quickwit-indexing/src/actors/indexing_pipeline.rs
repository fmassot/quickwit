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

//! Tantivy indexing graph. Lifecycle and source assignment handling live in `pipeline_supervisor`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorHandle, Mailbox, QueueCapacity};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::{IndexingSettings, RetentionPolicy, SourceConfig};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::IngesterPool;
use quickwit_metrics::{counter, label_values};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::{Storage, StorageResolver};
use tokio::sync::Semaphore;

use super::pipeline_supervisor::{
    INDEXING_SPAWN_SEMAPHORE, Pipeline, PipelineActors, PipelineSupervisor, SourcePipeline,
    SourceState,
};
use super::{
    DocProcessor, IndexSerializer, Indexer, MergePlanner, Packager, Publisher, Sequencer, Uploader,
    UploaderType,
};
use crate::SplitsUpdateMailbox;
use crate::docs_clustering::Fingerprinter;
use crate::merge_policy::MergePolicy;
use crate::metrics::{ACTOR_NAME, BACKPRESSURE_MICROS};
use crate::models::IndexingStatistics;
use crate::source::{SourceActor, SourceRuntime};
use crate::split_store::IndexingSplitStore;

pub type IndexingPipeline = PipelineSupervisor<TantivyIndexing>;

pub struct TantivyIndexing {
    params: IndexingPipelineParams,
    source: SourceState,
}

pub struct TantivyIndexingRunning {
    source: Arc<ActorHandle<SourceActor>>,
    doc_processor: Arc<ActorHandle<DocProcessor>>,
    indexer: Arc<ActorHandle<Indexer>>,
    uploader: Arc<ActorHandle<Uploader>>,
    publisher: Arc<ActorHandle<Publisher>>,
}

impl IndexingPipeline {
    pub fn new(params: IndexingPipelineParams) -> Self {
        let source = SourceState::new(&params.pipeline_id, params.params_fingerprint);
        Self::from_pipeline(TantivyIndexing { params, source })
    }
}

#[async_trait]
impl Pipeline for TantivyIndexing {
    type Statistics = IndexingStatistics;
    type Running = TantivyIndexingRunning;
    const NAME: &'static str = "IndexingPipeline";

    fn index_uid(&self) -> &quickwit_proto::types::IndexUid {
        &self.params.pipeline_id.index_uid
    }

    fn spawn_semaphore(&self) -> &'static Semaphore {
        &INDEXING_SPAWN_SEMAPHORE
    }
    fn restart_delay(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn spawn(
        &mut self,
        ctx: &ActorContext<IndexingPipeline>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running> {
        let index_id = &self.params.pipeline_id.index_uid.index_id;
        let source_id = &self.params.pipeline_id.source_id;
        // Validate before starting any actors whenever possible.
        let tag_fields = self.params.doc_mapper.tag_named_fields()?;
        let (source_mailbox, source_inbox) = ctx
            .spawn_ctx()
            .create_mailbox::<SourceActor>("SourceActor", QueueCapacity::Unbounded);

        let publisher = Publisher::new(
            super::PUBLISHER_NAME,
            QueueCapacity::Bounded(1),
            self.params.metastore.clone(),
            self.params.merge_planner_mailbox_opt.clone(),
            Some(source_mailbox.clone()),
            self.source.publish_token.clone(),
        );
        let (publisher_mailbox, publisher) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "publisher")])), publisher,
        );
        let (sequencer_mailbox, _) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "sequencer")])),
            Sequencer::new(publisher_mailbox),
        );
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            self.params.metastore.clone(),
            self.params.merge_policy.clone(),
            self.params.retention_policy.clone(),
            self.params.split_store.clone(),
            SplitsUpdateMailbox::Sequencer(sequencer_mailbox),
            self.params.max_concurrent_split_uploads_index,
            self.params.event_broker.clone(),
        );
        let (uploader_mailbox, uploader) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "uploader")])), uploader,
        );
        let (packager_mailbox, _) = actors.spawn(
            ctx.spawn_actor(),
            Packager::new("Packager", tag_fields, uploader_mailbox),
        );
        let (serializer_mailbox, _) =
            actors.spawn(ctx.spawn_actor(), IndexSerializer::new(packager_mailbox));
        let indexer = Indexer::new(
            self.params.pipeline_id.clone(),
            self.params.doc_mapper.clone(),
            self.params.metastore.clone(),
            self.params.indexing_directory.clone(),
            self.params.indexing_settings.clone(),
            self.params.cooperative_indexing_permits.clone(),
            serializer_mailbox,
            self.params.fingerprinter_opt.clone(),
        );
        let (indexer_mailbox, indexer) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "indexer")])), indexer,
        );
        let processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            self.params.doc_mapper.clone(),
            indexer_mailbox,
            self.params.source_config.transform_config.clone(),
            self.params.source_config.input_format,
            self.params.fingerprinter_opt.clone(),
        )?;
        let (processor_mailbox, doc_processor) = actors.spawn(
            ctx.spawn_actor().set_backpressure_micros_counter(counter!(parent: BACKPRESSURE_MICROS, labels: [label_values!(ACTOR_NAME => "doc_processor")])), processor,
        );
        let runtime = SourceRuntime {
            pipeline_id: self.params.pipeline_id.clone(),
            source_config: self.params.source_config.clone(),
            metastore: self.params.metastore.clone(),
            ingester_pool: self.params.ingester_pool.clone(),
            queues_dir_path: self.params.queues_dir_path.clone(),
            storage_resolver: self.params.source_storage_resolver.clone(),
            event_broker: self.params.event_broker.clone(),
            indexing_setting: self.params.indexing_settings.clone(),
            publish_token: self.source.publish_token.clone(),
        };
        let source = self
            .source
            .spawn(
                ctx,
                actors,
                runtime,
                processor_mailbox,
                (source_mailbox, source_inbox),
            )
            .await?;
        Ok(TantivyIndexingRunning {
            source,
            doc_processor,
            indexer,
            uploader,
            publisher,
        })
    }

    fn observe(&self, running: &Self::Running, previous: IndexingStatistics) -> IndexingStatistics {
        running.doc_processor.refresh_observe();
        running.indexer.refresh_observe();
        running.uploader.refresh_observe();
        running.publisher.refresh_observe();
        let mut statistics = previous.add_actor_counters(
            &running.doc_processor.last_observation(),
            &running.indexer.last_observation(),
            &running.uploader.last_observation(),
            &running.publisher.last_observation(),
        );
        statistics.pipeline_metrics_opt = running.indexer.last_observation().pipeline_metrics_opt;
        statistics
    }

    fn update_metadata(&self, statistics: &mut IndexingStatistics) {
        self.source.update_metadata(statistics);
    }
}

impl SourcePipeline for TantivyIndexing {
    fn source(&mut self) -> &mut SourceState {
        &mut self.source
    }
    fn source_mailbox(running: &Self::Running) -> &Mailbox<SourceActor> {
        running.source.mailbox()
    }
}

pub struct IndexingPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub metastore: MetastoreServiceClient,
    pub storage: Arc<dyn Storage>,
    pub doc_mapper: Arc<DocMapper>,
    pub indexing_directory: TempDirectory,
    pub indexing_settings: IndexingSettings,
    pub fingerprinter_opt: Option<Fingerprinter>,
    pub split_store: IndexingSplitStore,
    pub max_concurrent_split_uploads_index: usize,
    pub cooperative_indexing_permits: Option<Arc<Semaphore>>,
    pub merge_policy: Arc<dyn MergePolicy>,
    pub retention_policy: Option<RetentionPolicy>,
    pub merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    pub max_concurrent_split_uploads_merge: usize,
    pub source_config: SourceConfig,
    pub source_storage_resolver: StorageResolver,
    pub ingester_pool: IngesterPool,
    pub queues_dir_path: PathBuf,
    pub params_fingerprint: u64,
    pub event_broker: EventBroker,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{ActorState, Command, Health, Supervisable, Universe};
    use quickwit_common::ServiceStream;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_config::{IndexingSettings, SourceInputFormat, SourceParams};
    use quickwit_doc_mapper::{DocMapper, default_doc_mapper_for_test};
    use quickwit_metastore::checkpoint::IndexCheckpointDelta;
    use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt, PublishSplitsRequestExt};
    use quickwit_proto::metastore::{
        EmptyResponse, IndexMetadataResponse, LastDeleteOpstampResponse, MetastoreError,
        MockMetastoreService,
    };
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, ShardId};
    use quickwit_storage::RamStorage;

    use super::{IndexingPipeline, *};
    use crate::actors::pipeline_supervisor::wait_duration_before_retry;
    use crate::actors::{MergePipeline, MergePipelineParams};
    use crate::merge_policy::default_merge_policy;
    use crate::source::{AssignShards, Assignment};

    #[test]
    fn test_wait_duration() {
        assert_eq!(wait_duration_before_retry(0), Duration::from_secs(1));
        assert_eq!(wait_duration_before_retry(1), Duration::from_secs(2));
        assert_eq!(wait_duration_before_retry(2), Duration::from_secs(4));
        assert_eq!(wait_duration_before_retry(3), Duration::from_secs(8));
        assert_eq!(wait_duration_before_retry(9), Duration::from_secs(512));
        assert_eq!(wait_duration_before_retry(10), Duration::from_secs(600));
    }

    async fn test_indexing_pipeline_num_fails_before_success(
        mut num_fails: usize,
        test_file: &str,
    ) -> anyhow::Result<()> {
        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::for_test("test-index", 2);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid,
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                if num_fails == 0 {
                    let mut index_metadata =
                        IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                    index_metadata
                        .add_source(source_config_clone.clone())
                        .unwrap();
                    let response =
                        IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap();
                    return Ok(response);
                }
                num_fails -= 1;
                Err(MetastoreError::Timeout("timeout error".to_string()))
            });
        mock_metastore
            .expect_last_delete_opstamp()
            .returning(move |_last_delete_opstamp_request| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore
            .expect_mark_splits_for_deletion()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_stage_splits()
            .withf(|stage_splits_request| -> bool {
                stage_splits_request.index_uid() == &("test-index", 2)
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_publish_splits()
            .withf(|publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &("test-index", 2)
                    && checkpoint_delta.source_id == "test-source"
                    && publish_splits_request.staged_split_ids.len() == 1
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));

        let universe = Universe::new();
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            queues_dir_path: PathBuf::from("./queues"),
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox),
            event_broker: EventBroker::default(),
            params_fingerprint: 42u64,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        assert_eq!(
            pipeline_statistics.generation, 1,
            "generation is {}, expected 1",
            pipeline_statistics.generation
        );
        assert_eq!(
            pipeline_statistics.num_spawn_attempts,
            1 + num_fails,
            "num spawn attempts is {}, expected 1 + {}",
            pipeline_statistics.num_spawn_attempts,
            1 + num_fails
        );
        assert!(pipeline_exit_status.is_success());
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_0() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(0, "data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_1() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(1, "data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_0_gz() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(0, "data/test_corpus.json.gz").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_retry_1_gz() -> anyhow::Result<()> {
        test_indexing_pipeline_num_fails_before_success(1, "data/test_corpus.json.gz").await
    }

    fn spawn_pipeline_failing_to_publish(
        universe: &Universe,
        publish_error: MetastoreError,
    ) -> ActorHandle<IndexingPipeline> {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 1);
        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from_str("test-node"),
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str("data/test_corpus.json").unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().returning(move |_| {
            let mut index_metadata =
                IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
            index_metadata
                .add_source(source_config_clone.clone())
                .unwrap();
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore
            .expect_last_delete_opstamp()
            .returning(move |_| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore
            .expect_mark_splits_for_deletion()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_stage_splits()
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_publish_splits()
            .returning(move |_| Err(publish_error.clone()));

        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox),
            params_fingerprint: 42u64,
            event_broker: EventBroker::default(),
        };
        let (_pipeline_mailbox, pipeline_handle) = universe
            .spawn_builder()
            .spawn(IndexingPipeline::new(pipeline_params));
        pipeline_handle
    }

    #[tokio::test]
    async fn test_indexing_pipeline_stops_for_good_on_revoked_publish_token() {
        let universe = Universe::with_accelerated_time();
        let pipeline_handle = spawn_pipeline_failing_to_publish(
            &universe,
            MetastoreError::InvalidPublishToken {
                queue_id: "test-index:1/test-source/0".to_string(),
            },
        );
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;

        assert!(pipeline_exit_status.is_success());
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_pipeline_respawns_on_other_publish_errors() {
        let universe = Universe::with_accelerated_time();
        let pipeline_handle = spawn_pipeline_failing_to_publish(
            &universe,
            MetastoreError::InvalidArgument {
                message: "failed to apply checkpoint delta".to_string(),
            },
        );
        wait_until_predicate(
            || async { pipeline_handle.last_observation().generation >= 2 },
            Duration::from_secs(30),
            Duration::from_millis(25),
        )
        .await
        .expect("pipeline should respawn after a publish error other than a revoked token");

        universe.assert_quit().await;
    }

    async fn indexing_pipeline_simple(test_file: &str) -> anyhow::Result<()> {
        let node_id = NodeId::from_str("test-node");
        let index_uid: IndexUid = IndexUid::for_test("test-index", 1);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 1)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_last_delete_opstamp()
            .withf(move |last_delete_opstamp| last_delete_opstamp.index_uid() == &index_uid_clone)
            .returning(move |_| Ok(LastDeleteOpstampResponse::new(10)));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| stage_splits_request.index_uid() == &index_uid_clone)
            .returning(|_| Ok(EmptyResponse {}));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &index_uid_clone
                    && publish_splits_request.staged_split_ids.len() == 1
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_id == "test-source"
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));

        let universe = Universe::new();
        let storage = Arc::new(RamStorage::default());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox),
            event_broker: Default::default(),
            params_fingerprint: 42u64,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 1);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_simple() -> anyhow::Result<()> {
        indexing_pipeline_simple("data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_simple_gz() -> anyhow::Result<()> {
        indexing_pipeline_simple("data/test_corpus.json.gz").await
    }
    #[tokio::test]
    async fn test_merge_pipeline_does_not_stop_on_indexing_pipeline_failure() {
        let node_id = NodeId::from_str("test-node");
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        mock_metastore
            .expect_list_splits()
            .returning(|_| Ok(ServiceStream::empty()));
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let universe = Universe::with_accelerated_time();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: pipeline_id.merge_pipeline_id(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: TempDirectory::for_test(),
            metastore: metastore.clone(),
            split_store: split_store.clone(),
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads: 2,
            merge_io_throughput_limiter_opt: None,
            merge_scheduler_service: universe.get_or_spawn_one(),
            event_broker: Default::default(),
        };
        let merge_pipeline = MergePipeline::new(merge_pipeline_params, None, universe.spawn_ctx());
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_merge_pipeline_mailbox, merge_pipeline_handler) =
            universe.spawn_builder().spawn(merge_pipeline);
        let indexing_pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper,
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore,
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox.clone()),
            event_broker: Default::default(),
            params_fingerprint: 42u64,
        };
        let indexing_pipeline = IndexingPipeline::new(indexing_pipeline_params);
        let (_indexing_pipeline_mailbox, indexing_pipeline_handler) =
            universe.spawn_builder().spawn(indexing_pipeline);
        let obs = indexing_pipeline_handler
            .process_pending_and_observe()
            .await;
        assert_eq!(obs.generation, 1);
        // Let's shutdown the indexer, this will trigger the indexing pipeline failure and the
        // restart.
        let indexer = universe.get::<Indexer>().into_iter().next().unwrap();
        let _ = indexer.ask(Command::Quit).await;
        for _ in 0..10 {
            universe.sleep(*quickwit_actors::HEARTBEAT).await;
            // Check indexing pipeline has restarted.
            let obs = indexing_pipeline_handler
                .process_pending_and_observe()
                .await;
            if obs.generation == 2 {
                assert_eq!(merge_pipeline_handler.check_health(true), Health::Healthy);
                universe.quit().await;
                return;
            }
        }
        panic!("Pipeline was apparently not restarted.");
    }

    /// Assigning shards to a pipeline whose source has just died must not kill the pipeline
    /// actor itself. The source mailbox is closed as soon as the source actor exits, but the
    /// pipeline only clears its handles on its next supervision tick, so there is a window
    /// during which `AssignShards` is forwarded to a closed mailbox.
    #[tokio::test]
    async fn test_assign_shards_to_dead_source_does_not_fail_pipeline() {
        let node_id = NodeId::from_str("test-node");
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().returning(move |_| {
            let mut index_metadata =
                IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
            index_metadata
                .add_source(source_config_clone.clone())
                .unwrap();
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        let universe = Universe::new();
        let storage = Arc::new(RamStorage::default());
        let indexing_pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore,
            queues_dir_path: PathBuf::from("./queues"),
            storage: storage.clone(),
            split_store: IndexingSplitStore::create_without_local_store_for_test(storage),
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: None,
            event_broker: Default::default(),
            params_fingerprint: 42u64,
        };
        let indexing_pipeline = IndexingPipeline::new(indexing_pipeline_params);
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(indexing_pipeline);
        let observation = pipeline_handle.process_pending_and_observe().await;
        assert_eq!(observation.generation, 1);

        // Pause the pipeline so that its supervise loop, which is delivered on the low priority
        // channel, cannot run and clear `handles_opt` while we set the scenario up. High
        // priority messages are still processed while paused, which is how we get the
        // `AssignShards` in without racing the supervision tick.
        pipeline_handle.pause();

        // Kill the source and wait for its inbox to be dropped: the mailbox the pipeline holds
        // is then closed, while the pipeline still believes the source is alive.
        let source_mailbox = universe
            .get::<SourceActor>()
            .into_iter()
            .next()
            .expect("source actor should be running");
        let _ = source_mailbox.ask(Command::Quit).await;
        wait_until_predicate(
            || async { source_mailbox.is_disconnected() },
            Duration::from_secs(3),
            Duration::from_millis(30),
        )
        .await
        .expect("source mailbox was not dropped within 3s");
        // Forwarding this assignment to the dead source used to exit the pipeline with
        // `ActorExitStatus::DownstreamClosed`, in which case the reply is never sent.
        let reply_rx = pipeline_mailbox
            .send_message_with_high_priority(AssignShards(Assignment {
                shard_ids: BTreeSet::from_iter([ShardId::from(1u64)]),
                indexing_plan_id: "indexing_plan_id".to_string(),
            }))
            .expect("pipeline mailbox should be open");
        reply_rx
            .await
            .expect("pipeline should have handled the assignment and survived");

        assert_ne!(pipeline_handle.state(), ActorState::Failure);

        // The shard ids are recorded regardless, so the next generation picks them up.
        pipeline_handle.resume();
        let observation = pipeline_handle.process_pending_and_observe().await;
        assert_eq!(
            observation.shard_ids,
            BTreeSet::from_iter([ShardId::from(1u64)])
        );

        universe.quit().await;
    }

    async fn indexing_pipeline_all_failures_handling(test_file: &str) -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let node_id = NodeId::from_str("test-node");
        let index_uid: IndexUid = IndexUid::for_test("test-index", 2);
        let pipeline_id = IndexingPipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let source_config = SourceConfig {
            source_id: "test-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::file_from_str(test_file).unwrap(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let source_config_clone = source_config.clone();

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .withf(|index_metadata_request| {
                index_metadata_request.index_uid.as_ref().unwrap() == &("test-index", 2)
            })
            .returning(move |_| {
                let mut index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                index_metadata
                    .add_source(source_config_clone.clone())
                    .unwrap();

                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_last_delete_opstamp()
            .withf(move |last_delete_opstamp| last_delete_opstamp.index_uid() == &index_uid_clone)
            .returning(move |_| Ok(LastDeleteOpstampResponse::new(10)));
        mock_metastore
            .expect_stage_splits()
            .never()
            .returning(|_| Ok(EmptyResponse {}));
        let index_uid_clone = index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| -> bool {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &index_uid_clone
                    && publish_splits_request.staged_split_ids.is_empty()
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_id == "test-source"
                    && format!("{:?}", checkpoint_delta.source_delta)
                        .ends_with(":(00000000000000000000..~00000000000000001030])")
            })
            .returning(|_| Ok(EmptyResponse {}));
        let universe = Universe::new();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _) = universe.create_test_mailbox();
        // Create a minimal mapper with wrong date format to ensure that all documents will fail
        let broken_mapper = serde_json::from_str::<DocMapper>(
            r#"
                {
                    "store_source": true,
                    "timestamp_field": "timestamp",
                    "field_mappings": [
                        {
                            "name": "timestamp",
                            "type": "datetime",
                            "input_formats": ["iso8601"],
                            "fast": true
                        }
                    ]
                }"#,
        )
        .unwrap();

        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(broken_mapper),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox),
            params_fingerprint: 42u64,
            event_broker: Default::default(),
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        let (pipeline_exit_status, pipeline_statistics) = pipeline_handler.join().await;
        assert!(pipeline_exit_status.is_success());
        // flaky. Sometimes generations is 2.
        assert_eq!(pipeline_statistics.generation, 1);
        assert_eq!(pipeline_statistics.num_spawn_attempts, 1);
        assert_eq!(pipeline_statistics.num_published_splits, 0);
        assert_eq!(pipeline_statistics.num_empty_splits, 1);
        assert_eq!(
            pipeline_statistics.num_docs,
            pipeline_statistics.num_invalid_docs
        );
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexing_pipeline_all_failures_handling() -> anyhow::Result<()> {
        indexing_pipeline_all_failures_handling("data/test_corpus.json").await
    }

    #[tokio::test]
    async fn test_indexing_pipeline_all_failures_handling_gz() -> anyhow::Result<()> {
        indexing_pipeline_all_failures_handling("data/test_corpus.json.gz").await
    }
}
