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

//! Parquet indexing graph: source → processor → indexer → packager → uploader → sequencer →
//! publisher. Metrics and sketches differ only in processor and writer kind, not in supervision.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorHandle, Mailbox, QueueCapacity};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::{IndexingSettings, SourceConfig};
use quickwit_ingest::IngesterPool;
use quickwit_parquet_engine::merge::policy::ParquetMergePolicy;
use quickwit_parquet_engine::split::ParquetSplitKind;
use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
use quickwit_parquet_engine::table_config::TableConfig;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::{Storage, StorageResolver};
use tokio::sync::Semaphore;

use super::parquet_doc_processor::IngestProcessor;
use super::{
    ParquetDocProcessor, ParquetIndexer, ParquetMergePlanner, ParquetPackager, ParquetUploader,
};
use crate::actors::pipeline_supervisor::{
    INDEXING_SPAWN_SEMAPHORE, Pipeline, PipelineActors, PipelineSupervisor, SourcePipeline,
    SourceState,
};
use crate::actors::{Publisher, Sequencer, UploaderType};
use crate::models::IndexingStatistics;
use crate::source::{SourceActor, SourceRuntime};

pub type ParquetIndexingPipeline = PipelineSupervisor<ParquetIndexing>;

pub struct ParquetIndexing {
    params: ParquetIndexingPipelineParams,
    source: SourceState,
}

pub struct ParquetIndexingRunning {
    source: Arc<ActorHandle<SourceActor>>,
    doc_processor: Arc<ActorHandle<ParquetDocProcessor>>,
    indexer: Arc<ActorHandle<ParquetIndexer>>,
    uploader: Arc<ActorHandle<ParquetUploader>>,
    publisher: Arc<ActorHandle<Publisher>>,
}

impl ParquetIndexingPipeline {
    pub fn new(params: ParquetIndexingPipelineParams) -> Self {
        let source = SourceState::new(&params.pipeline_id, params.params_fingerprint);
        Self::from_pipeline(ParquetIndexing { params, source })
    }
}

#[async_trait]
impl Pipeline for ParquetIndexing {
    type Statistics = IndexingStatistics;
    type Running = ParquetIndexingRunning;
    const NAME: &'static str = "ParquetIndexingPipeline";

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
        ctx: &ActorContext<ParquetIndexingPipeline>,
        actors: &mut PipelineActors,
    ) -> anyhow::Result<Self::Running> {
        let parquet_config = self.params.indexing_settings.parquet_indexing();
        let table_config = TableConfig {
            sort_fields: parquet_config.sort_fields.clone(),
            window_duration_secs: parquet_config.window_duration_secs,
            ..Default::default()
        };
        let writer = ParquetSplitWriter::new(
            self.params.split_kind,
            ParquetWriterConfig::default(),
            self.params.indexing_directory.path(),
            &table_config,
        )?;
        let (source_mailbox, source_inbox) = ctx
            .spawn_ctx()
            .create_mailbox::<SourceActor>("SourceActor", QueueCapacity::Unbounded);
        let mut publisher = Publisher::new_parquet(
            self.params.split_kind,
            QueueCapacity::Bounded(1),
            self.params.metastore.clone(),
            Some(source_mailbox.clone()),
            self.source.publish_token.clone(),
        );
        if let Some(planner) = &self.params.parquet_merge_planner_mailbox_opt {
            publisher = publisher.set_parquet_merge_planner_mailbox(planner.clone());
        }
        let (publisher_mailbox, publisher) = actors.spawn(ctx.spawn_actor(), publisher);
        let (sequencer_mailbox, _) =
            actors.spawn(ctx.spawn_actor(), Sequencer::new(publisher_mailbox));
        let (uploader_mailbox, uploader) = actors.spawn(
            ctx.spawn_actor(),
            ParquetUploader::new(
                UploaderType::IndexUploader,
                self.params.metastore.clone(),
                self.params.storage.clone(),
                sequencer_mailbox,
                self.params.max_concurrent_split_uploads,
                self.params.parquet_merge_policy.clone(),
            ),
        );
        let (packager_mailbox, _) = actors.spawn(
            ctx.spawn_actor(),
            ParquetPackager::new(writer, uploader_mailbox),
        );
        let indexer = ParquetIndexer::new_with_partition_key_and_max_num_partitions(
            self.params.pipeline_id.index_uid.clone(),
            self.params.pipeline_id.source_id.clone(),
            None,
            packager_mailbox,
            self.params.partition_key.clone(),
            self.params.max_num_partitions,
            Some(Duration::from_secs(
                self.params.indexing_settings.commit_timeout_secs as u64,
            )),
        );
        let (indexer_mailbox, indexer) = actors.spawn(ctx.spawn_actor(), indexer);
        let processor = match self.params.split_kind {
            ParquetSplitKind::Sketches => IngestProcessor::Sketches(
                quickwit_parquet_engine::ingest::SketchParquetIngestProcessor::new(),
            ),
            ParquetSplitKind::Metrics => {
                IngestProcessor::Metrics(quickwit_parquet_engine::ingest::ParquetIngestProcessor)
            }
        };
        let (processor_mailbox, doc_processor) = actors.spawn(
            ctx.spawn_actor(),
            ParquetDocProcessor::new(
                processor,
                self.params.pipeline_id.index_uid.index_id.clone(),
                self.params.pipeline_id.source_id.clone(),
                indexer_mailbox,
            ),
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
        Ok(ParquetIndexingRunning {
            source,
            doc_processor,
            indexer,
            uploader,
            publisher,
        })
    }

    fn observe(
        &self,
        running: &Self::Running,
        mut stats: IndexingStatistics,
    ) -> IndexingStatistics {
        running.doc_processor.refresh_observe();
        running.indexer.refresh_observe();
        running.uploader.refresh_observe();
        running.publisher.refresh_observe();
        let docs = running.doc_processor.last_observation();
        let uploader = running.uploader.last_observation();
        let publisher = running.publisher.last_observation();
        stats.num_docs += docs.valid_rows;
        stats.num_invalid_docs += docs.num_errors();
        stats.total_bytes_processed += docs.bytes_total;
        stats.num_local_splits += running.indexer.last_observation().batches_flushed;
        stats.num_staged_splits += uploader.num_staged_splits.load(Ordering::Relaxed);
        stats.num_uploaded_splits += uploader.num_uploaded_splits.load(Ordering::Relaxed);
        stats.num_published_splits += publisher.num_published_splits;
        stats.num_empty_splits += publisher.num_empty_splits;
        stats
    }

    fn update_metadata(&self, stats: &mut IndexingStatistics) {
        self.source.update_metadata(stats);
    }
}

impl SourcePipeline for ParquetIndexing {
    fn source(&mut self) -> &mut SourceState {
        &mut self.source
    }
    fn source_mailbox(running: &Self::Running) -> &Mailbox<SourceActor> {
        running.source.mailbox()
    }
}

pub struct ParquetIndexingPipelineParams {
    pub pipeline_id: IndexingPipelineId,
    pub metastore: MetastoreServiceClient,
    pub storage: Arc<dyn Storage>,
    pub indexing_directory: TempDirectory,
    pub indexing_settings: IndexingSettings,
    pub max_concurrent_split_uploads: usize,
    pub source_config: SourceConfig,
    pub source_storage_resolver: StorageResolver,
    pub ingester_pool: IngesterPool,
    pub queues_dir_path: std::path::PathBuf,
    pub params_fingerprint: u64,
    pub event_broker: EventBroker,
    pub split_kind: ParquetSplitKind,
    pub partition_key: quickwit_doc_mapper::RoutingExpr,
    pub max_num_partitions: NonZeroU32,
    pub parquet_merge_policy: Arc<dyn ParquetMergePolicy>,
    pub parquet_merge_planner_mailbox_opt: Option<Mailbox<ParquetMergePlanner>>,
}
