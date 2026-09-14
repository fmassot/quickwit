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

use std::path::Path;
use std::sync::Arc;

use quickwit_config::{CLI_SOURCE_ID, IndexConfig, IndexType, StorageBackend};
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_metastore::{
    IndexMetadataResponseExt, MetastoreServiceExt, PARQUET_SPLITS_PAGE_SIZE, ParquetPublication,
    metastore_for_test,
};
use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService};
use quickwit_storage::{
    BulkDeleteError, DeleteFailure, MockStorage, MockStorageFactory, StorageResolver,
};

use super::*;
use crate::IndexService;

struct Fixture {
    service: IndexService,
    metadata: IndexMetadata,
    catalog: ParquetSplits,
    storage: Arc<dyn Storage>,
}

impl Fixture {
    async fn new(index_type: IndexType, count: usize) -> Self {
        let metastore = metastore_for_test();
        let resolver = StorageResolver::for_test();
        let mut service = IndexService::new(metastore.clone(), resolver.clone());
        let mut config = IndexConfig::for_test("cpu", "ram://indexes/cpu");
        config.index_type = index_type;
        let metadata = service.create_index(config, false).await.unwrap();
        let storage = resolver.resolve(metadata.index_uri()).await.unwrap();
        let catalog = ParquetSplits::from_index_metadata(metastore, &metadata).unwrap();
        let mut splits = Vec::new();
        for ordinal in 0..count {
            let id = format!("split-{ordinal:04}");
            let mut split = ParquetSplitMetadata::metrics_builder()
                .index_uid(metadata.index_uid.to_string())
                .split_id(ParquetSplitId::new(&id))
                .parquet_file(format!("{id}.parquet"))
                .time_range(TimeRange::new(1000, 2000))
                .num_rows(7)
                .size_bytes(3)
                .build();
            split.kind = catalog.kind();
            storage
                .put(
                    Path::new(&split.parquet_filename()),
                    Box::new(vec![1u8, 2, 3]),
                )
                .await
                .unwrap();
            splits.push(split);
        }
        catalog.stage(&splits).await.unwrap();
        catalog
            .publish(&ParquetPublication {
                staged_split_ids: splits
                    .iter()
                    .step_by(3)
                    .map(|split| split.split_id.to_string())
                    .collect(),
                checkpoint_delta: Some(IndexCheckpointDelta {
                    source_id: CLI_SOURCE_ID.to_string(),
                    source_delta: SourceCheckpointDelta::from_range(0..10),
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        catalog
            .mark_for_deletion(
                &splits
                    .iter()
                    .skip(1)
                    .step_by(3)
                    .map(|split| split.split_id.to_string())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        Self {
            service,
            metadata,
            catalog,
            storage,
        }
    }

    async fn metadata(&self) -> IndexMetadata {
        self.service
            .metastore()
            .index_metadata(IndexMetadataRequest::for_index_uid(
                self.metadata.index_uid.clone(),
            ))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap()
    }

    async fn assert_files_exist(&self, count: usize, exist: bool) {
        for ordinal in 0..count {
            assert_eq!(
                self.storage
                    .exists(Path::new(&format!("split-{ordinal:04}.parquet")))
                    .await
                    .unwrap(),
                exist
            );
        }
    }
}

#[tokio::test]
async fn dry_run_lists_all_states_without_mutating() {
    for kind in [IndexType::Metrics, IndexType::Sketches] {
        let mut fixture = Fixture::new(kind, 3).await;
        let before = fixture.metadata().await;
        let infos = fixture.service.delete_index("cpu", true).await.unwrap();
        assert_eq!(infos.len(), 3);
        assert!(
            infos
                .iter()
                .all(|info| info.num_docs == 7 && info.file_size_bytes.as_u64() == 3)
        );
        fixture.assert_files_exist(3, true).await;
        let states: Vec<_> = fixture
            .catalog
            .list_all(fixture.catalog.query().with_split_states([]))
            .await
            .unwrap()
            .into_iter()
            .map(|split| split.state)
            .collect();
        assert_eq!(
            states,
            [
                SplitState::Published,
                SplitState::MarkedForDeletion,
                SplitState::Staged
            ]
        );
        assert_eq!(fixture.metadata().await.checkpoint, before.checkpoint);
    }
}

#[tokio::test]
async fn delete_removes_every_page_before_index_metadata() {
    for kind in [IndexType::Metrics, IndexType::Sketches] {
        let count = PARQUET_SPLITS_PAGE_SIZE + 3;
        let mut fixture = Fixture::new(kind, count).await;
        let infos = fixture.service.delete_index("cpu", false).await.unwrap();
        assert_eq!(infos.len(), count);
        fixture.assert_files_exist(count, false).await;
        assert!(
            !fixture
                .service
                .metastore()
                .index_exists("cpu")
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn clear_retains_index_and_resets_checkpoint_after_deletion() {
    for kind in [IndexType::Metrics, IndexType::Sketches] {
        let mut fixture = Fixture::new(kind, 3).await;
        assert!(
            !fixture
                .metadata()
                .await
                .checkpoint
                .source_checkpoint(CLI_SOURCE_ID)
                .unwrap()
                .is_empty()
        );
        fixture.service.clear_index("cpu").await.unwrap();
        fixture.assert_files_exist(3, false).await;
        let after = fixture.metadata().await;
        assert_eq!(after.index_uid, fixture.metadata.index_uid);
        assert!(after.checkpoint.is_empty());
        assert_eq!(after.sources.len(), fixture.metadata.sources.len());
        assert!(
            fixture
                .catalog
                .list_all(fixture.catalog.query().with_split_states([]))
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn failed_storage_deletion_preserves_metadata_and_checkpoint_for_retry() {
    for kind in [IndexType::Metrics, IndexType::Sketches] {
        for clear in [false, true] {
            let mut fixture = Fixture::new(kind, 3).await;
            let before = fixture.metadata().await;
            let mut storage = MockStorage::new();
            storage.expect_bulk_delete().times(1).returning(|paths| {
                Err(BulkDeleteError {
                    failures: paths
                        .iter()
                        .map(|path| {
                            (
                                path.to_path_buf(),
                                DeleteFailure {
                                    code: Some("AccessDenied".to_string()),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                    ..Default::default()
                })
            });
            let storage: Arc<dyn Storage> = Arc::new(storage);
            let mut factory = MockStorageFactory::new();
            factory.expect_backend().returning(|| StorageBackend::Ram);
            factory
                .expect_resolve()
                .times(1)
                .return_once(move |_| Ok(storage));
            let resolver = StorageResolver::builder()
                .register(factory)
                .build()
                .unwrap();
            let mut failing_service = IndexService::new(fixture.service.metastore(), resolver);
            let failed = if clear {
                failing_service.clear_index("cpu").await
            } else {
                failing_service.delete_index("cpu", false).await.map(|_| ())
            };
            assert!(failed.is_err());
            assert_eq!(fixture.metadata().await.checkpoint, before.checkpoint);
            let remaining = fixture
                .catalog
                .list_all(fixture.catalog.query().with_split_states([]))
                .await
                .unwrap();
            assert_eq!(remaining.len(), 3);
            assert!(
                remaining
                    .iter()
                    .all(|split| split.state == SplitState::MarkedForDeletion)
            );
            fixture.assert_files_exist(3, true).await;
            if clear {
                fixture.service.clear_index("cpu").await.unwrap();
            } else {
                fixture.service.delete_index("cpu", false).await.unwrap();
            }
            fixture.assert_files_exist(3, false).await;
        }
    }
}

#[tokio::test]
async fn metastore_deletion_failure_does_not_remove_index_or_reset_checkpoint() {
    use quickwit_metastore::{ListParquetSplitsResponseExt, ParquetSplitRecord};
    use quickwit_proto::metastore::{
        EmptyResponse, IndexMetadataResponse, ListMetricsSplitsResponse, MetastoreError,
        MockMetastoreService,
    };

    for clear in [false, true] {
        let fixture = Fixture::new(IndexType::Metrics, 3).await;
        let metadata = fixture.metadata().await;
        let response = IndexMetadataResponse::try_from_index_metadata(&metadata).unwrap();
        let records: Vec<ParquetSplitRecord> = fixture
            .catalog
            .list_all(fixture.catalog.query().with_split_states([]))
            .await
            .unwrap();
        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .times(1)
            .return_once(move |_| Ok(response));
        mock.expect_list_metrics_splits()
            .times(1)
            .return_once(move |_| ListMetricsSplitsResponse::try_from_splits(&records));
        mock.expect_mark_metrics_splits_for_deletion()
            .times(1)
            .return_once(|_| Ok(EmptyResponse {}));
        mock.expect_delete_metrics_splits()
            .times(1)
            .return_once(|_| Err(MetastoreError::Unavailable("offline".to_string())));
        mock.expect_delete_index().never();
        mock.expect_reset_source_checkpoint().never();
        let storage = fixture.storage.clone();
        let mut factory = MockStorageFactory::new();
        factory.expect_backend().returning(|| StorageBackend::Ram);
        factory.expect_resolve().return_once(move |_| Ok(storage));
        let resolver = StorageResolver::builder()
            .register(factory)
            .build()
            .unwrap();
        let mut service = IndexService::new(MetastoreServiceClient::from_mock(mock), resolver);
        let result = if clear {
            service.clear_index("cpu").await
        } else {
            service.delete_index("cpu", false).await.map(|_| ())
        };
        assert!(result.is_err());
        fixture.assert_files_exist(3, false).await;
    }
}
