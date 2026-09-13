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

//! Metrics-specific DataFusion integration.
//!
//! `MetricsDataSource` carries the runtime and Substrait pieces.
//! `MetricsSchemaProvider` carries the native OSS catalog/schema integration.
//!
//! All metrics-specific code lives in this module; none leaks into the generic
//! session / worker / Substrait layer.

pub(crate) mod factory;
pub(crate) mod index_resolver;
pub(crate) mod metastore_provider;
pub(crate) mod optimizer;
pub(crate) mod predicate;
pub(crate) mod sketch_udf;
pub(crate) mod table_provider;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use quickwit_df_core::{
    QuickwitRuntimePlugin, QuickwitRuntimeRegistration, QuickwitSubstraitConsumerExt,
};
use quickwit_parquet_engine::split::ParquetSplitKind;
use quickwit_proto::metastore::MetastoreServiceClient;

use self::factory::{METRICS_FILE_TYPE, MetricsTableProviderFactory, SKETCHES_FILE_TYPE};
use self::index_resolver::{MetastoreIndexResolver, MetricsIndexResolver};
use self::optimizer::SortedSeriesStreamingAggregateRule;
use self::sketch_udf::{create_dd_quantile_udf, create_dd_sketch_udaf};
use self::table_provider::MetricsTableProvider;

/// Runtime/Substrait integration for OSS parquet metrics.
///
/// Backed by the Quickwit metastore for split discovery. Object-store
/// construction is delegated to
/// [`crate::object_store_registry::QuickwitObjectStoreRegistry`]: the source
/// never materialises `Arc<dyn ObjectStore>` itself, and there is no
/// per-query registration path. The node-local [`StorageResolver`] is used
/// once, at first read for each URL, inside the lazy
/// [`crate::storage_bridge::QuickwitObjectStore`].
#[derive(Debug)]
pub struct MetricsDataSource {
    index_resolver: Arc<dyn MetricsIndexResolver>,
}

impl MetricsDataSource {
    /// Create a production `MetricsDataSource` backed by the metastore.
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self {
            index_resolver: Arc::new(MetastoreIndexResolver::new(metastore)),
        }
    }

    /// Create with a custom resolver (for tests).
    pub fn with_resolver(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self { index_resolver }
    }

    pub fn schema_provider(&self) -> Arc<dyn SchemaProvider> {
        Arc::new(MetricsSchemaProvider::new(Arc::clone(&self.index_resolver)))
    }
}

/// Resolves `index_name` through the metastore; `None` if it is not a Parquet index. The
/// table schema is `schema_hint` if given, else the minimal schema of the index's split kind.
async fn resolve_metrics_table_provider(
    index_resolver: &dyn MetricsIndexResolver,
    index_name: &str,
    schema_hint: Option<SchemaRef>,
) -> DFResult<Option<Arc<dyn TableProvider>>> {
    let Some(resolved) = index_resolver.resolve(index_name).await? else {
        return Ok(None);
    };
    let schema = schema_hint.unwrap_or_else(|| minimal_schema_for_kind(resolved.split_kind));
    let provider = MetricsTableProvider::new(schema, resolved.split_provider, resolved.index_uri)?;
    Ok(Some(Arc::new(provider)))
}

/// Minimal 4-column schema — always present in every OSS metrics parquet file.
fn minimal_base_schema() -> SchemaRef {
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Minimal sketch schema — always present in every DDSketch parquet file.
fn minimal_sketch_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(
        quickwit_parquet_engine::schema::sketch_fields::SketchParquetField::all()
            .iter()
            .map(|field| field.to_arrow_field())
            .collect::<Vec<_>>(),
    ))
}

fn minimal_schema_for_kind(split_kind: ParquetSplitKind) -> SchemaRef {
    match split_kind {
        ParquetSplitKind::Metrics => minimal_base_schema(),
        ParquetSplitKind::Sketches => minimal_sketch_schema(),
    }
}

/// Native OSS `SchemaProvider` for metrics indexes.
pub struct MetricsSchemaProvider {
    index_resolver: Arc<dyn MetricsIndexResolver>,
    ddl_tables: MemorySchemaProvider,
}

impl MetricsSchemaProvider {
    pub fn new(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self {
            index_resolver,
            ddl_tables: MemorySchemaProvider::new(),
        }
    }
}

impl std::fmt::Debug for MetricsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsSchemaProvider")
            .field("num_ddl_tables", &self.ddl_tables.table_names().len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for MetricsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let resolver = Arc::clone(&self.index_resolver);
        let mut names = self.ddl_tables.table_names();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Ok(mut resolved_names) = resolver.list_parquet_index_names().await {
                    names.append(&mut resolved_names);
                }
            })
        });
        names.sort();
        names.dedup();
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self.ddl_tables.table(name).await? {
            return Ok(Some(provider));
        }

        resolve_metrics_table_provider(self.index_resolver.as_ref(), name, None).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.ddl_tables.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.deregister_table(name)
    }
}

#[async_trait]
impl QuickwitRuntimePlugin for MetricsDataSource {
    fn registration(&self) -> QuickwitRuntimeRegistration {
        let factory: Arc<dyn TableProviderFactory> = Arc::new(MetricsTableProviderFactory::new(
            Arc::clone(&self.index_resolver),
            ParquetSplitKind::Metrics,
        ));
        let sketches_factory: Arc<dyn TableProviderFactory> =
            Arc::new(MetricsTableProviderFactory::new(
                Arc::clone(&self.index_resolver),
                ParquetSplitKind::Sketches,
            ));
        QuickwitRuntimeRegistration::default()
            .with_session_config_setter(|config| {
                config
                    .options_mut()
                    .optimizer
                    .enable_round_robin_repartition = false;
                config.options_mut().optimizer.repartition_file_scans = false;
            })
            .with_physical_optimizer_rule(Arc::new(SortedSeriesStreamingAggregateRule))
            .with_table_factory(METRICS_FILE_TYPE, factory)
            .with_table_factory(SKETCHES_FILE_TYPE, sketches_factory)
            .with_udaf(Arc::new(create_dd_sketch_udaf()))
            .with_udf(Arc::new(create_dd_quantile_udf()))
    }
}

#[async_trait]
impl QuickwitSubstraitConsumerExt for MetricsDataSource {
    /// Handle `ReadRel` nodes in incoming Substrait plans.
    ///
    /// ## OSS path — `NamedTable` with the index name as the leaf segment
    ///
    /// Treats `NamedTable.names.last()` as the index name and tries to
    /// resolve it against the metastore. If the index exists, returns a
    /// `MetricsTableProvider` using `schema_hint` (derived from
    /// `ReadRel.base_schema` by the caller). If the resolver reports
    /// "not found", returns `Ok(None)` so the standard catalog path can
    /// try. Any other error is propagated.
    ///
    /// Producers are free to use any `NamedTable.names` shape their
    /// table-reference scheme requires (single name, catalog/schema/table,
    /// etc.) — only the leaf segment is interpreted here. Mapping a
    /// caller-side logical table to a concrete Quickwit index is a
    /// producer concern (e.g. an upstream bridge that rewrites
    /// `metrics.points` to a concrete index name before emitting
    /// the Substrait plan).
    ///
    /// ## Extension path — custom protos (downstream callers)
    ///
    /// A downstream caller registers its own `QuickwitSubstraitConsumerExt` that
    /// handles `ExtensionTable<MetricRead>`. This default implementation
    /// only handles `NamedTable` — `ExtensionTable` always returns
    /// `Ok(None)` here.
    async fn try_consume_read_rel(
        &self,
        rel: &datafusion_substrait::substrait::proto::ReadRel,
        schema_hint: Option<arrow::datatypes::SchemaRef>,
    ) -> DFResult<Option<(String, Arc<dyn TableProvider>)>> {
        use datafusion_substrait::substrait::proto::read_rel::ReadType;

        // Only handle NamedTable reads. ExtensionTable (downstream callers) returns None.
        let Some(ReadType::NamedTable(nt)) = &rel.read_type else {
            return Ok(None);
        };
        // `NamedTable::names` is a path like ["catalog", "schema", "table"];
        // the last element is the effective table name. An empty list is a
        // malformed plan — skip rather than silently resolving to index "".
        let Some(index_name) = nt.names.last() else {
            return Ok(None);
        };
        let index_name = index_name.as_str();

        // Use the producer-declared schema if available; fall back to the
        // minimal schema for the index family.
        let provider =
            resolve_metrics_table_provider(self.index_resolver.as_ref(), index_name, schema_hint)
                .await?;
        Ok(provider.map(|provider| (index_name.to_string(), provider)))
    }
}
