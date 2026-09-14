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

//! Index resolution for the metrics data source.
//!
//! `MetastoreIndexResolver::resolve()` performs a single `index_metadata`
//! metastore RPC and returns the split provider plus the index's storage
//! `Uri`. The actual `Storage` (and its `ObjectStore` wrapper) is built
//! lazily by [`crate::object_store_registry::QuickwitObjectStoreRegistry`]
//! on the first read — see the docstring there for the overall flow.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use quickwit_common::uri::Uri;
use quickwit_metastore::{IndexMetadataResponseExt, ListIndexesMetadataResponseExt};
use quickwit_parquet_engine::split::ParquetSplitKind;
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use tracing::debug;

use super::metastore_provider::MetastoreSplitProvider;
use super::table_provider::MetricsSplitProvider;

/// A resolved Parquet index.
pub struct ResolvedParquetIndex {
    pub split_provider: Arc<dyn MetricsSplitProvider>,
    pub index_uri: Uri,
    pub split_kind: ParquetSplitKind,
}

/// Resolves per-index resources needed to scan a metrics index.
#[async_trait]
pub trait MetricsIndexResolver: Send + Sync + std::fmt::Debug {
    /// Returns the split provider, storage URI and split kind for `index_name`, or `None` if
    /// the index does not exist or is not a Parquet index. The `ObjectStore` for the URI is
    /// built on demand by the registry the first time DataFusion reads from it.
    async fn resolve(&self, index_name: &str) -> DFResult<Option<ResolvedParquetIndex>>;

    /// Names of the Parquet (metrics / sketches) indexes.
    async fn list_parquet_index_names(&self) -> DFResult<Vec<String>>;
}

// ── Production implementation ─────────────────────────────────────────

/// Production `MetricsIndexResolver` backed by the Quickwit metastore.
///
/// `resolve()` fetches `IndexMetadata` and returns the URI. No
/// `StorageResolver` is held here — the object store registry resolves
/// storage lazily on first read.
#[derive(Clone)]
pub struct MetastoreIndexResolver {
    metastore: MetastoreServiceClient,
}

impl MetastoreIndexResolver {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self { metastore }
    }
}

impl std::fmt::Debug for MetastoreIndexResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreIndexResolver").finish()
    }
}

#[async_trait]
impl MetricsIndexResolver for MetastoreIndexResolver {
    async fn resolve(&self, index_name: &str) -> DFResult<Option<ResolvedParquetIndex>> {
        debug!(index_name, "resolving parquet index");

        let response = match self
            .metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_name.to_string()))
            .await
        {
            Ok(response) => response,
            Err(quickwit_proto::metastore::MetastoreError::NotFound(_)) => return Ok(None),
            Err(err) => return Err(datafusion::error::DataFusionError::External(Box::new(err))),
        };

        let index_metadata = response
            .deserialize_index_metadata()
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let index_type = index_metadata.index_config.index_type;
        if !index_type.is_parquet() {
            return Ok(None);
        }
        let split_kind = if index_type.is_sketches() {
            ParquetSplitKind::Sketches
        } else {
            ParquetSplitKind::Metrics
        };
        let index_uid = index_metadata.index_uid.clone();
        let index_uri = index_metadata.index_config.index_uri.clone();

        debug!(%index_uid, %index_uri, ?split_kind, "resolved index metadata");

        let split_provider: Arc<dyn MetricsSplitProvider> = Arc::new(MetastoreSplitProvider::new(
            self.metastore.clone(),
            index_uid,
            split_kind,
        ));

        Ok(Some(ResolvedParquetIndex {
            split_provider,
            index_uri,
            split_kind,
        }))
    }

    async fn list_parquet_index_names(&self) -> DFResult<Vec<String>> {
        let response = self
            .metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let indexes = response
            .deserialize_indexes_metadata()
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        Ok(indexes
            .into_iter()
            .filter(|idx| idx.index_config.index_type.is_parquet())
            .map(|idx| idx.index_config.index_id)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::IndexType;
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::metastore::{
        EntityKind, IndexMetadataResponse, ListIndexesMetadataResponse, MetastoreError,
        MockMetastoreService,
    };

    use super::*;

    #[tokio::test]
    async fn test_resolver_uses_type_not_name() {
        let mut indexes = Vec::new();
        let mut mock = MockMetastoreService::new();
        for (name, index_type, expected_kind) in [
            ("cpu", IndexType::Metrics, Some(ParquetSplitKind::Metrics)),
            (
                "latencies",
                IndexType::Sketches,
                Some(ParquetSplitKind::Sketches),
            ),
            ("metrics-logs", IndexType::Tantivy, None),
        ] {
            let mut metadata = IndexMetadata::for_test(name, "ram:///indexes/test");
            metadata.index_config.index_type = index_type;
            indexes.push(metadata.clone());
            let mut resolver_mock = MockMetastoreService::new();
            resolver_mock
                .expect_index_metadata()
                .times(1)
                .returning(move |_| IndexMetadataResponse::try_from_index_metadata(&metadata));
            let resolver =
                MetastoreIndexResolver::new(MetastoreServiceClient::from_mock(resolver_mock));
            let resolved = resolver.resolve(name).await.unwrap();
            assert_eq!(resolved.map(|index| index.split_kind), expected_kind);
        }
        mock.expect_list_indexes_metadata()
            .times(1)
            .returning(move |_| Ok(ListIndexesMetadataResponse::for_test(indexes.clone())));
        let resolver = MetastoreIndexResolver::new(MetastoreServiceClient::from_mock(mock));
        assert_eq!(
            resolver.list_parquet_index_names().await.unwrap(),
            ["cpu", "latencies"]
        );
    }

    #[tokio::test]
    async fn test_resolver_only_swallows_not_found() {
        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata().times(1).returning(|_| {
            Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: "missing".to_string(),
            }))
        });
        mock.expect_index_metadata()
            .times(1)
            .returning(|_| Err(MetastoreError::Unavailable("offline".to_string())));
        let resolver = MetastoreIndexResolver::new(MetastoreServiceClient::from_mock(mock));
        assert!(resolver.resolve("missing").await.unwrap().is_none());
        assert!(resolver.resolve("cpu").await.is_err());
    }
}
