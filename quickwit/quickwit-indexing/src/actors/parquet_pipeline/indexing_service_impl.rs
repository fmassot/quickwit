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

//! Construction of the Parquet indexing graph from explicit index configuration.

use quickwit_actors::ActorContext;
use quickwit_common::temp_dir;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_doc_mapper::RoutingExpr;
use quickwit_proto::indexing::{IndexingError, IndexingPipelineId};

use crate::actors::pipeline_handle::ActorPipeline;
use crate::actors::{ParquetIndexingPipeline, ParquetIndexingPipelineParams};
use crate::{BoxedPipelineHandle, IndexingService};

impl IndexingService {
    pub(crate) async fn spawn_parquet_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
        params_fingerprint: u64,
    ) -> Result<BoxedPipelineHandle, IndexingError> {
        let pipeline_uid_str = indexing_pipeline_id.pipeline_uid.to_string();
        let indexing_directory = temp_dir::Builder::default()
            .join(&indexing_pipeline_id.index_uid.index_id)
            .join(&indexing_pipeline_id.index_uid.incarnation_id.to_string())
            .join(&indexing_pipeline_id.source_id)
            .join(&pipeline_uid_str)
            .tempdir_in(&self.indexing_root_directory)
            .map_err(|error| {
                let message = format!("failed to create indexing directory: {error}");
                IndexingError::Internal(message)
            })?;
        let storage = self
            .storage_resolver
            .resolve(&index_config.index_uri)
            .await
            .map_err(|error| {
                let message = format!("failed to spawn metrics pipeline: {error}");
                IndexingError::Internal(message)
            })?;

        let partition_key_str = index_config
            .doc_mapping
            .partition_key
            .as_deref()
            .unwrap_or("");
        let partition_key = RoutingExpr::new(partition_key_str).map_err(|error| {
            IndexingError::Internal(format!("failed to parse partition_key: {error}"))
        })?;
        let parquet_merge_policy = crate::merge_policy::parquet_merge_policy_from_settings(
            &index_config.indexing_settings,
        );

        // Spawn the Parquet merge pipeline (or reuse an existing one for this
        // index). The planner mailbox is wired into the ParquetIndexingPipeline's
        // Publisher so newly ingested splits are fed back for merging.
        // Returns `None` when there is no local merge scheduler — the metrics
        // pipeline then runs without local merging (mirrors the log path).
        let merge_planner_mailbox_opt = self.get_or_create_parquet_merge_pipeline(
            indexing_pipeline_id.index_uid.clone(),
            &index_config,
            storage.clone(),
            indexing_directory.clone(),
            // None here means the pipeline's fetch_immature_splits() will
            // query the metastore on first spawn (same path as respawn).
            None,
            ctx,
        )?;

        let pipeline_params = ParquetIndexingPipelineParams {
            pipeline_id: indexing_pipeline_id.clone(),
            metastore: self.metastore.clone(),
            storage,
            indexing_directory,
            indexing_settings: index_config.indexing_settings.clone(),
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
            source_config,
            ingester_pool: self.ingester_pool.clone(),
            queues_dir_path: self.queue_dir_path.clone(),
            source_storage_resolver: self.storage_resolver.clone(),
            params_fingerprint,
            event_broker: self.event_broker.clone(),
            split_kind: super::parquet_split_kind(index_config.index_type),
            partition_key,
            max_num_partitions: index_config.doc_mapping.max_num_partitions,
            parquet_merge_policy,
            parquet_merge_planner_mailbox_opt: merge_planner_mailbox_opt,
        };
        let index_type = index_config.index_type;
        let pipeline = ParquetIndexingPipeline::new(pipeline_params);
        let (mailbox, handle) = ctx.spawn_actor().spawn(pipeline);
        Ok(Box::new(ActorPipeline {
            pipeline_id: indexing_pipeline_id,
            index_type,
            mailbox,
            handle,
        }))
    }
}
