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

//! Parquet merge graph: planner → downloader → executor → uploader → sequencer → publisher.
//! The common supervisor owns health, restart and drain state for both storage engines.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    ActorContext, ActorHandle, HEARTBEAT, Inbox, Mailbox, QueueCapacity, SpawnContext,
};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_metastore::ParquetSplits;
use quickwit_parquet_engine::merge::policy::ParquetMergePolicy;
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::IndexUid;
use quickwit_storage::Storage;
use time::OffsetDateTime;
use tokio::sync::Semaphore;

use super::parquet_merge_planner::RunFinalizeMergePolicyAndQuit;
use super::{
    ParquetMergeExecutor, ParquetMergePlanner, ParquetMergeSplitDownloader, ParquetUploader,
};
#[cfg(test)]
use crate::actors::pipeline_supervisor::FinishPendingMergesAndShutdownPipeline;
use crate::actors::pipeline_supervisor::{
    DrainablePipeline, MERGE_SPAWN_SEMAPHORE, Pipeline, PipelineActors, PipelineSupervisor,
};
use crate::actors::publisher::DisconnectMergePlanner;
use crate::actors::{
    MergeSchedulerService, ParquetPublisher as Publisher, Sequencer, UploaderType,
};
use crate::metrics::ONGOING_MERGE_OPERATIONS;
use crate::models::{MergeStatistics, SharedPublishToken};
pub const PARQUET_MERGE_SKIP_INITIAL_SEED_ENV_KEY: &str = "QW_PARQUET_MERGE_SKIP_INITIAL_SEED";
pub type ParquetMergePipeline = PipelineSupervisor<ParquetMerge>;

pub struct ParquetMerge {
    params: ParquetMergePipelineParams,
    merge_planner_mailbox: Mailbox<ParquetMergePlanner>,
    merge_planner_inbox: Inbox<ParquetMergePlanner>,
    initial_immature_splits_opt: Option<Vec<ParquetSplitMetadata>>,
}

pub struct ParquetMergeRunning {
    planner: Arc<ActorHandle<ParquetMergePlanner>>,
    uploader: Arc<ActorHandle<ParquetUploader>>,
    publisher: Arc<ActorHandle<Publisher>>,
}

impl ParquetMergePipeline {
    pub fn new(
        params: ParquetMergePipelineParams,
        initial_immature_splits_opt: Option<Vec<ParquetSplitMetadata>>,
        spawn_ctx: &SpawnContext,
    ) -> Self {
        // Stable mailboxes preserve the feedback path across generations.
        let (merge_planner_mailbox, merge_planner_inbox) = spawn_ctx
            .create_mailbox::<ParquetMergePlanner>(
                "ParquetMergePlanner",
                QueueCapacity::Bounded(1),
            );
        Self::from_pipeline(ParquetMerge {
            params,
            merge_planner_mailbox,
            merge_planner_inbox,
            initial_immature_splits_opt,
        })
    }

    pub fn merge_planner_mailbox(&self) -> &Mailbox<ParquetMergePlanner> {
        &self.pipeline.merge_planner_mailbox
    }
}

#[async_trait]
impl Pipeline for ParquetMerge {
    type Statistics = MergeStatistics;
    type Running = ParquetMergeRunning;
    const NAME: &'static str = "ParquetMergePipeline";

    fn index_uid(&self) -> &IndexUid {
        &self.params.index_uid
    }

    fn spawn_semaphore(&self) -> &'static Semaphore {
        &MERGE_SPAWN_SEMAPHORE
    }
    fn restart_delay(&self) -> Duration {
        *HEARTBEAT
    }

    async fn spawn(
        &mut self,
        ctx: &ActorContext<ParquetMergePipeline>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running> {
        let immature_splits = self.fetch_immature_splits(ctx).await?;
        record_merge_pipeline_event(&MergePipelineEvent::Restart {
            index_uid: self.params.index_uid.to_string(),
            re_seeded_immature_split_ids: immature_splits
                .iter()
                .map(|split| split.split_id.to_string())
                .collect(),
        });
        let publisher = Publisher::new_parquet(
            self.params.split_kind,
            QueueCapacity::Unbounded,
            self.params.metastore.clone(),
            None,
            SharedPublishToken::default(),
        )
        .with_merge_planner(self.merge_planner_mailbox.clone());
        let (publisher_mailbox, publisher) = actors.spawn(ctx.spawn_actor(), publisher);
        let (sequencer_mailbox, _) =
            actors.spawn(ctx.spawn_actor(), Sequencer::new(publisher_mailbox));
        let (uploader_mailbox, uploader) = actors.spawn(
            ctx.spawn_actor(),
            ParquetUploader::new(
                UploaderType::MergeUploader,
                self.params.metastore.clone(),
                self.params.storage.clone(),
                sequencer_mailbox,
                self.params.max_concurrent_split_uploads,
                self.params.merge_policy.clone(),
            ),
        );
        let (executor_mailbox, _) = actors.spawn(
            ctx.spawn_actor(),
            ParquetMergeExecutor::new(
                uploader_mailbox,
                self.params.writer_config.clone(),
                self.params.use_streaming_engine,
                self.params.target_split_size_bytes,
            ),
        );
        let (downloader_mailbox, _) = actors.spawn(
            ctx.spawn_actor(),
            ParquetMergeSplitDownloader::new(
                self.params.indexing_directory.clone(),
                self.params.storage.clone(),
                executor_mailbox,
            ),
        );
        let planner = ParquetMergePlanner::new(
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
        Ok(ParquetMergeRunning {
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
impl DrainablePipeline for ParquetMerge {
    async fn drain(&self, running: &Self::Running) -> anyhow::Result<()> {
        running
            .publisher
            .mailbox()
            .send_message(DisconnectMergePlanner)
            .await?;
        record_merge_pipeline_event(&MergePipelineEvent::DisconnectMergePlanner {
            index_uid: self.params.index_uid.to_string(),
        });
        running
            .planner
            .mailbox()
            .send_message(RunFinalizeMergePolicyAndQuit)
            .await?;
        record_merge_pipeline_event(&MergePipelineEvent::RunFinalizeAndQuit {
            index_uid: self.params.index_uid.to_string(),
            finalize_merges_emitted: 0,
        });
        Ok(())
    }
}

impl ParquetMerge {
    async fn fetch_immature_splits(
        &mut self,
        ctx: &ActorContext<ParquetMergePipeline>,
    ) -> anyhow::Result<Vec<ParquetSplitMetadata>> {
        if let Some(splits) = self.initial_immature_splits_opt.take() {
            return Ok(splits);
        }
        if self.params.skip_initial_seed {
            return Ok(Vec::new());
        }
        ctx.protect_future(fetch_published_parquet_splits_paginated(
            self.params.metastore.clone(),
            self.params.index_uid.clone(),
            self.params.split_kind,
        ))
        .await
    }
}

async fn fetch_published_parquet_splits_paginated(
    metastore: MetastoreServiceClient,
    index_uid: IndexUid,
    kind: ParquetSplitKind,
) -> anyhow::Result<Vec<ParquetSplitMetadata>> {
    let catalog = ParquetSplits::new(metastore, index_uid, kind);
    let query = catalog.query().retain_immature(OffsetDateTime::now_utc());
    let records = catalog.list_all(query).await?;
    Ok(records.into_iter().map(|record| record.metadata).collect())
}

#[derive(Clone)]
pub struct ParquetMergePipelineParams {
    pub index_uid: IndexUid,
    pub split_kind: ParquetSplitKind,
    pub indexing_directory: TempDirectory,
    pub metastore: MetastoreServiceClient,
    pub storage: Arc<dyn Storage>,
    pub merge_policy: Arc<dyn ParquetMergePolicy>,
    pub merge_scheduler_service: Mailbox<MergeSchedulerService>,
    pub max_concurrent_split_uploads: usize,
    pub event_broker: EventBroker,
    /// Start without historical splits (for deployments with an external compaction owner).
    pub skip_initial_seed: bool,
    pub writer_config: quickwit_parquet_engine::storage::ParquetWriterConfig,
    pub use_streaming_engine: bool,
    pub target_split_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use quickwit_actors::{ActorExitStatus, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_metastore::{
        ListParquetSplitsRequestExt, ListParquetSplitsResponseExt, PARQUET_SPLITS_PAGE_SIZE,
        ParquetSplitRecord, SplitState,
    };
    use quickwit_parquet_engine::merge::policy::{
        ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig, ParquetSplitMaturity,
    };
    use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};

    use super::*;

    fn make_pipeline_params(universe: &Universe) -> ParquetMergePipelineParams {
        let mut mock_metastore = MockMetastoreService::new();
        // Allow list_metrics_splits for respawn seeding (returns empty).
        mock_metastore.expect_list_metrics_splits().returning(|_| {
            Ok(quickwit_proto::metastore::ListMetricsSplitsResponse {
                splits_serialized_json: Vec::new(),
            })
        });
        let storage = Arc::new(quickwit_storage::RamStorage::default());
        let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                max_merge_ops: 5,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_hours(1),
                max_finalize_merge_operations: 3,
            },
        ));
        ParquetMergePipelineParams {
            index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
            split_kind: quickwit_parquet_engine::split::ParquetSplitKind::Metrics,
            indexing_directory: TempDirectory::for_test(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            storage,
            merge_policy,
            merge_scheduler_service: universe.get_or_spawn_one(),
            max_concurrent_split_uploads: 4,
            event_broker: EventBroker::default(),
            skip_initial_seed: false,
            writer_config: quickwit_parquet_engine::storage::ParquetWriterConfig::default(),
            use_streaming_engine: false,
            target_split_size_bytes: 256 * 1024 * 1024,
        }
    }

    fn make_split(split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid(IndexUid::for_test("test-merge-index", 0).to_string())
            .partition_id(0)
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(1_000_000)
            .sort_fields("metric_name|host|timestamp_secs/V2")
            .window_start_secs(0)
            .window_duration_secs(3600)
            .maturity(ParquetSplitMaturity::Immature {
                maturation_period: Duration::from_hours(1),
            })
            .build()
    }

    fn make_split_record(split_id: &str) -> ParquetSplitRecord {
        ParquetSplitRecord {
            state: SplitState::Published,
            update_timestamp: 0,
            metadata: make_split(split_id),
        }
    }

    #[tokio::test]
    async fn test_pipeline_spawns_and_supervises() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let pipeline = ParquetMergePipeline::new(params, None, universe.spawn_ctx());
        let (_pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Give the pipeline time to initialize and spawn actors.
        universe.sleep(Duration::from_secs(2)).await;

        let observation = pipeline_handle.process_pending_and_observe().await;
        assert_eq!(
            observation.obs_type,
            quickwit_actors::ObservationType::Alive
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_pipeline_shutdown_drain() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let pipeline = ParquetMergePipeline::new(params, None, universe.spawn_ctx());
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Let it initialize.
        universe.sleep(Duration::from_secs(2)).await;

        // Initiate shutdown.
        pipeline_mailbox
            .send_message(FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();

        // The pipeline should eventually exit with Success.
        let (exit_status, _) = pipeline_handle.join().await;
        assert!(
            matches!(exit_status, ActorExitStatus::Success),
            "expected Success exit, got {:?}",
            exit_status
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_pipeline_accepts_initial_splits() {
        let universe = Universe::with_accelerated_time();
        let params = make_pipeline_params(&universe);

        let initial_splits = Some(vec![make_split("s0"), make_split("s1")]);
        let pipeline = ParquetMergePipeline::new(params, initial_splits, universe.spawn_ctx());
        let planner_mailbox = pipeline.merge_planner_mailbox().clone();
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Let it initialize.
        universe.sleep(Duration::from_secs(2)).await;

        // The planner mailbox should be accessible.
        assert!(!planner_mailbox.is_disconnected());

        // Gracefully shut down before asserting quit.
        pipeline_mailbox
            .send_message(FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();
        let (exit_status, _) = pipeline_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_pipeline_skip_initial_seed_does_not_list_published_splits() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_list_metrics_splits().times(0);

        let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                max_merge_ops: 5,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_hours(1),
                max_finalize_merge_operations: 3,
            },
        ));
        let params = ParquetMergePipelineParams {
            index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
            split_kind: quickwit_parquet_engine::split::ParquetSplitKind::Metrics,
            indexing_directory: TempDirectory::for_test(),
            metastore: MetastoreServiceClient::from_mock(mock_metastore),
            storage: Arc::new(quickwit_storage::RamStorage::default()),
            merge_policy,
            merge_scheduler_service: universe.get_or_spawn_one(),
            max_concurrent_split_uploads: 4,
            event_broker: EventBroker::default(),
            skip_initial_seed: true,
            writer_config: quickwit_parquet_engine::storage::ParquetWriterConfig::default(),
            use_streaming_engine: false,
            target_split_size_bytes: 256 * 1024 * 1024,
        };

        let pipeline = ParquetMergePipeline::new(params, None, universe.spawn_ctx());
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        universe.sleep(Duration::from_secs(2)).await;

        pipeline_mailbox
            .send_message(FinishPendingMergesAndShutdownPipeline)
            .await
            .unwrap();
        let (exit_status, _) = pipeline_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_fetch_published_parquet_splits_paginates_metastore_requests() {
        let mut mock_metastore = MockMetastoreService::new();
        let pages = Arc::new(Mutex::new(vec![
            (0..PARQUET_SPLITS_PAGE_SIZE)
                .map(|split_idx| make_split_record(&format!("split-{split_idx:04}")))
                .collect::<Vec<_>>(),
            (PARQUET_SPLITS_PAGE_SIZE..PARQUET_SPLITS_PAGE_SIZE * 2)
                .map(|split_idx| make_split_record(&format!("split-{split_idx:04}")))
                .collect::<Vec<_>>(),
            vec![make_split_record(&format!(
                "split-{:04}",
                PARQUET_SPLITS_PAGE_SIZE * 2
            ))],
        ]));
        let after_split_ids = Arc::new(Mutex::new(Vec::new()));
        let pages_for_mock = pages.clone();
        let after_split_ids_for_mock = after_split_ids.clone();

        mock_metastore
            .expect_list_metrics_splits()
            .times(3)
            .returning(move |request| {
                let query = request.deserialize_query().unwrap();
                assert_eq!(query.limit, Some(PARQUET_SPLITS_PAGE_SIZE));
                after_split_ids_for_mock
                    .lock()
                    .unwrap()
                    .push(query.after_split_id);

                let page = pages_for_mock.lock().unwrap().remove(0);
                quickwit_proto::metastore::ListMetricsSplitsResponse::try_from_splits(&page)
            });

        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let splits = fetch_published_parquet_splits_paginated(
            metastore,
            quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
            ParquetSplitKind::Metrics,
        )
        .await
        .unwrap();

        assert_eq!(splits.len(), PARQUET_SPLITS_PAGE_SIZE * 2 + 1);
        assert_eq!(
            *after_split_ids.lock().unwrap(),
            vec![
                None,
                Some(format!("split-{:04}", PARQUET_SPLITS_PAGE_SIZE - 1)),
                Some(format!("split-{:04}", PARQUET_SPLITS_PAGE_SIZE * 2 - 1)),
            ]
        );
    }
}
