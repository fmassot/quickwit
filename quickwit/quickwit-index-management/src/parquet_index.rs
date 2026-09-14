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

//! Administrative deletion, distinct from grace-period-based garbage collection.

use quickwit_metastore::{IndexMetadata, ParquetSplits, SplitInfo, SplitState};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::Storage;

use crate::index::IndexServiceError;
use crate::parquet_garbage_collection::delete_parquet_splits_from_storage_and_metastore;

/// Delete all states, including previously marked splits left by interrupted cleanup.
/// Index metadata and checkpoints must only be removed/reset after this succeeds.
/// As with Tantivy clear/delete, callers must quiesce writers before invoking this;
/// pagination is not a snapshot or an ingestion fence.
pub(crate) async fn delete_parquet_index_splits(
    metastore: MetastoreServiceClient,
    metadata: &IndexMetadata,
    storage: &dyn Storage,
    dry_run: bool,
) -> Result<Vec<SplitInfo>, IndexServiceError> {
    let catalog = ParquetSplits::from_index_metadata(metastore.clone(), metadata)?;
    let mut query = catalog.query().with_split_states([
        SplitState::Staged,
        SplitState::Published,
        SplitState::MarkedForDeletion,
    ]);
    let mut deleted = Vec::new();
    loop {
        let page = catalog.list_page(&mut query).await?;
        let infos: Vec<SplitInfo> = page
            .splits
            .iter()
            .map(|record| record.as_split_info())
            .collect::<Result<_, _>>()?;
        if !dry_run && !page.splits.is_empty() {
            let ids: Vec<String> = infos.iter().map(|info| info.split_id.to_string()).collect();
            catalog.mark_for_deletion(&ids).await?;
            let (_, failed) = delete_parquet_splits_from_storage_and_metastore(
                &metastore,
                catalog.index_uid(),
                catalog.kind(),
                storage,
                &page.splits,
                None,
            )
            .await;
            if !failed.is_empty() {
                return Err(IndexServiceError::Internal(format!(
                    "failed to delete Parquet splits: {:?}; index metadata and checkpoints \
                     retained for retry",
                    failed
                        .iter()
                        .map(|split| &split.split_id)
                        .collect::<Vec<_>>()
                )));
            }
        }
        deleted.extend(infos);
        if !page.has_next_page {
            return Ok(deleted);
        }
    }
}

#[cfg(test)]
mod tests;
