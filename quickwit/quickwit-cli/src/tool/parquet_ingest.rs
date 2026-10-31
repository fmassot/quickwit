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

//! Parquet input of `quickwit tool local-ingest`. See `docs/internals/parquet-bulk-load.md`.

use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_common::uri::Uri;
use quickwit_config::CLI_SOURCE_ID;
use quickwit_indexing::source::SourceLoader;
use quickwit_indexing::source::parquet_file::{
    DEFAULT_PARQUET_BATCH_NUM_ROWS, ParquetLoadPlan, ParquetSourceFactory,
};
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::metastore::{
    IndexMetadataRequest, MetastoreService, MetastoreServiceClient, SourceType,
};
use thousands::Separable;

/// Reads and validates the Parquet footer, then returns the number of rows to load and a source
/// loader whose `file` sources share the file's row groups.
///
/// Rows already recorded in the checkpoint are skipped, unless `overwrite` resets it.
pub(super) async fn prepare_parquet_source(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    input_uri: Uri,
    overwrite: bool,
) -> anyhow::Result<(u64, Arc<SourceLoader>)> {
    let plan = tokio::task::spawn_blocking(move || {
        ParquetLoadPlan::try_new(input_uri, DEFAULT_PARQUET_BATCH_NUM_ROWS)
    })
    .await??;
    let num_checkpointed_rows = if overwrite {
        0
    } else {
        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await?
            .deserialize_index_metadata()?;
        index_metadata
            .checkpoint
            .source_checkpoint(CLI_SOURCE_ID)
            .map(|checkpoint| plan.num_checkpointed_rows_total(checkpoint))
            .unwrap_or(0)
    };
    println!(
        "Parquet file: {} rows ({} already loaded), {} row groups, {} uncompressed.",
        plan.num_rows().separate_with_commas(),
        num_checkpointed_rows.separate_with_commas(),
        plan.num_row_groups().separate_with_commas(),
        ByteSize(plan.num_uncompressed_bytes()),
    );
    let num_rows_to_load = plan.num_rows() - num_checkpointed_rows;
    let mut source_loader = SourceLoader::default();
    source_loader.add_source(SourceType::File, ParquetSourceFactory::new(Arc::new(plan)));
    Ok((num_rows_to_load, Arc::new(source_loader)))
}
