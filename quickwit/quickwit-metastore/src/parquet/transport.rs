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

//! Transport adapter for the existing metrics/sketch RPCs. Keep this choice out of
//! lifecycle callers; unifying the persisted tables/protocol is a separate change.

use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::{
    DeleteMetricsSplitsRequest, DeleteSketchSplitsRequest, ListMetricsSplitsRequest,
    ListSketchSplitsRequest, MarkMetricsSplitsForDeletionRequest,
    MarkSketchSplitsForDeletionRequest, MetastoreResult, MetastoreService,
    PublishMetricsSplitsRequest, PublishSketchSplitsRequest, StageMetricsSplitsRequest,
    StageSketchSplitsRequest, serde_utils,
};

use super::{ParquetPublication, ParquetSplits};
use crate::{
    ListParquetSplitsQuery, ListParquetSplitsRequestExt, ListParquetSplitsResponseExt,
    ParquetSplitRecord, StageParquetSplitsRequestExt,
};

impl ParquetSplits {
    pub(super) async fn stage_request(
        &self,
        splits: &[ParquetSplitMetadata],
    ) -> MetastoreResult<()> {
        match self.kind {
            ParquetSplitKind::Metrics => {
                let request = StageMetricsSplitsRequest::try_from_splits_metadata(
                    self.index_uid.clone(),
                    splits,
                )?;
                self.metastore.stage_metrics_splits(request).await?;
            }
            ParquetSplitKind::Sketches => {
                let request = StageSketchSplitsRequest::try_from_splits_metadata(
                    self.index_uid.clone(),
                    splits,
                )?;
                self.metastore.stage_sketch_splits(request).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn publish_request(
        &self,
        publication: &ParquetPublication,
    ) -> MetastoreResult<()> {
        let index_checkpoint_delta_json_opt = publication
            .checkpoint_delta
            .as_ref()
            .map(serde_utils::to_json_str)
            .transpose()?;
        match self.kind {
            ParquetSplitKind::Metrics => {
                self.metastore
                    .publish_metrics_splits(PublishMetricsSplitsRequest {
                        index_uid: Some(self.index_uid.clone()),
                        staged_split_ids: publication.staged_split_ids.clone(),
                        replaced_split_ids: publication.replaced_split_ids.clone(),
                        index_checkpoint_delta_json_opt,
                        publish_token_opt: publication.publish_token.clone(),
                    })
                    .await?;
            }
            ParquetSplitKind::Sketches => {
                self.metastore
                    .publish_sketch_splits(PublishSketchSplitsRequest {
                        index_uid: Some(self.index_uid.clone()),
                        staged_split_ids: publication.staged_split_ids.clone(),
                        replaced_split_ids: publication.replaced_split_ids.clone(),
                        index_checkpoint_delta_json_opt,
                        publish_token_opt: publication.publish_token.clone(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn list_request(
        &self,
        query: &ListParquetSplitsQuery,
    ) -> MetastoreResult<Vec<ParquetSplitRecord>> {
        match self.kind {
            ParquetSplitKind::Metrics => {
                let request =
                    ListMetricsSplitsRequest::try_from_query(self.index_uid.clone(), query)?;
                self.metastore
                    .list_metrics_splits(request)
                    .await?
                    .deserialize_splits()
            }
            ParquetSplitKind::Sketches => {
                let request =
                    ListSketchSplitsRequest::try_from_query(self.index_uid.clone(), query)?;
                self.metastore
                    .list_sketch_splits(request)
                    .await?
                    .deserialize_splits()
            }
        }
    }

    pub(super) async fn mark_request(&self, split_ids: &[String]) -> MetastoreResult<()> {
        match self.kind {
            ParquetSplitKind::Metrics => {
                self.metastore
                    .mark_metrics_splits_for_deletion(MarkMetricsSplitsForDeletionRequest {
                        index_uid: Some(self.index_uid.clone()),
                        split_ids: split_ids.to_vec(),
                    })
                    .await?;
            }
            ParquetSplitKind::Sketches => {
                self.metastore
                    .mark_sketch_splits_for_deletion(MarkSketchSplitsForDeletionRequest {
                        index_uid: Some(self.index_uid.clone()),
                        split_ids: split_ids.to_vec(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn delete_request(&self, split_ids: &[String]) -> MetastoreResult<()> {
        match self.kind {
            ParquetSplitKind::Metrics => {
                self.metastore
                    .delete_metrics_splits(DeleteMetricsSplitsRequest {
                        index_uid: Some(self.index_uid.clone()),
                        split_ids: split_ids.to_vec(),
                    })
                    .await?;
            }
            ParquetSplitKind::Sketches => {
                self.metastore
                    .delete_sketch_splits(DeleteSketchSplitsRequest {
                        index_uid: Some(self.index_uid.clone()),
                        split_ids: split_ids.to_vec(),
                    })
                    .await?;
            }
        }
        Ok(())
    }
}
