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

//! Index-scoped Parquet split lifecycle. Only `transport` knows which RPC family to use.

#[cfg(test)]
mod tests;
mod transport;

use std::collections::HashSet;

use bytesize::ByteSize;
use quickwit_config::IndexType;
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::{MetastoreError, MetastoreResult, MetastoreServiceClient};
use quickwit_proto::types::IndexUid;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListParquetSplitsQuery, ParquetSplitRecord, SplitInfo};

impl ParquetSplitRecord {
    /// Summarize a Parquet file for index administration. Original JSON size is not
    /// recorded for Parquet, so `uncompressed_docs_size_bytes` is zero (unknown).
    pub fn as_split_info(&self) -> MetastoreResult<SplitInfo> {
        let split = &self.metadata;
        Ok(SplitInfo {
            split_id: split.split_id.to_string().into(),
            num_docs: usize::try_from(split.num_rows).map_err(|error| {
                MetastoreError::Internal {
                    message: "Parquet row count exceeds platform size".to_string(),
                    cause: error.to_string(),
                }
            })?,
            uncompressed_docs_size_bytes: ByteSize::b(0),
            file_name: split.parquet_filename().into(),
            file_size_bytes: ByteSize::b(split.size_bytes),
        })
    }
}

/// Maximum number of records per cursor page.
pub const PARQUET_SPLITS_PAGE_SIZE: usize = 500;

/// One page of splits. A full page may be followed by an empty terminal page.
#[derive(Debug)]
pub struct ParquetSplitsPage {
    /// Validated, strictly ordered records belonging to this catalog.
    pub splits: Vec<ParquetSplitRecord>,
    /// Whether the page was full and another request is needed.
    pub has_next_page: bool,
}

/// Atomic publication of new splits, replacement of old splits, and source progress.
///
/// Empty split lists are valid: EOF/checkpoint-only updates must still reach the
/// configured engine. Merge publication normally has no checkpoint or publish token.
#[derive(Clone, Debug, Default)]
pub struct ParquetPublication {
    /// Staged splits to make queryable.
    pub staged_split_ids: Vec<String>,
    /// Published merge inputs to mark for deletion atomically with the outputs.
    pub replaced_split_ids: Vec<String>,
    /// Source progress to commit with this publication, including empty flushes.
    pub checkpoint_delta: Option<IndexCheckpointDelta>,
    /// Current source ownership token, required by shard-based sources.
    pub publish_token: Option<String>,
}

/// A split catalog bound to one index incarnation and one Parquet engine.
///
/// Construct from index metadata at discovery boundaries, or from explicit pipeline
/// configuration. Index names and split ID prefixes never select an engine. This
/// handle does not cache split state, retry mutations, or split a publication across
/// transactions: checkpoint validation and atomicity remain the metastore's job.
#[derive(Clone, Debug)]
pub struct ParquetSplits {
    metastore: MetastoreServiceClient,
    index_uid: IndexUid,
    kind: ParquetSplitKind,
}

impl ParquetSplits {
    /// Bind an explicit pipeline configuration to its split catalog.
    pub fn new(
        metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        kind: ParquetSplitKind,
    ) -> Self {
        Self {
            metastore,
            index_uid,
            kind,
        }
    }

    /// Bind discovered index metadata; Tantivy is rejected regardless of its name.
    pub fn from_index_metadata(
        metastore: MetastoreServiceClient,
        metadata: &IndexMetadata,
    ) -> MetastoreResult<Self> {
        let kind = match metadata.index_config.index_type {
            IndexType::Metrics => ParquetSplitKind::Metrics,
            IndexType::Sketches => ParquetSplitKind::Sketches,
            IndexType::Tantivy => return Err(invalid("index is not a Parquet index")),
        };
        Ok(Self::new(metastore, metadata.index_uid.clone(), kind))
    }

    /// The index incarnation to which all operations are scoped.
    pub fn index_uid(&self) -> &IndexUid {
        &self.index_uid
    }

    /// The configured Parquet engine.
    pub fn kind(&self) -> ParquetSplitKind {
        self.kind
    }

    /// Start a query for published splits in this index.
    pub fn query(&self) -> ListParquetSplitsQuery {
        ListParquetSplitsQuery::for_index(self.index_uid.clone())
    }

    /// Stage a homogeneous batch. Invalid batches are rejected before any RPC.
    pub async fn stage(&self, splits: &[ParquetSplitMetadata]) -> MetastoreResult<()> {
        let mut ids = HashSet::with_capacity(splits.len());
        for split in splits {
            self.validate_metadata(split).map_err(invalid)?;
            if !ids.insert(split.split_id.as_str()) {
                return Err(invalid("duplicate split ID in staged batch"));
            }
        }
        if splits.is_empty() {
            return Ok(());
        }
        self.stage_request(splits).await
    }

    /// Publish atomically, including checkpoint-only publications.
    pub async fn publish(&self, publication: &ParquetPublication) -> MetastoreResult<()> {
        let mut ids = HashSet::new();
        for id in publication
            .staged_split_ids
            .iter()
            .chain(&publication.replaced_split_ids)
        {
            if !ids.insert(id) {
                return Err(invalid(
                    "publication contains duplicate or overlapping split IDs",
                ));
            }
        }
        self.publish_request(publication).await
    }

    /// List one bounded page and advance the cursor only after validating the response.
    ///
    /// Filters and the starting cursor are preserved; `limit` is the page size, not
    /// a total-result limit. Bad identity, oversized pages, or non-advancing cursors
    /// are errors, rather than potential cross-index deletion or infinite loops.
    pub async fn list_page(
        &self,
        query: &mut ListParquetSplitsQuery,
    ) -> MetastoreResult<ParquetSplitsPage> {
        if query.index_uid != self.index_uid {
            return Err(invalid("query index does not match the Parquet catalog"));
        }
        query.limit = Some(PARQUET_SPLITS_PAGE_SIZE);
        let splits = self.list_request(query).await?;
        let invalid_response = |cause: String| MetastoreError::Internal {
            message: "invalid Parquet split listing".to_string(),
            cause,
        };
        if splits.len() > PARQUET_SPLITS_PAGE_SIZE {
            return Err(invalid_response("page exceeds requested limit".to_string()));
        }
        let mut previous = query.after_split_id.as_deref();
        for split in &splits {
            self.validate_metadata(&split.metadata)
                .map_err(invalid_response)?;
            if !query.split_states.is_empty() && !query.split_states.contains(&split.state) {
                return Err(invalid_response(
                    "split does not match the requested states".to_string(),
                ));
            }
            let id = split.metadata.split_id.as_str();
            if let Some(previous) = previous
                && id <= previous
            {
                return Err(invalid_response(
                    "split cursor did not advance strictly".to_string(),
                ));
            }
            previous = Some(id);
        }
        let has_next_page = splits.len() == PARQUET_SPLITS_PAGE_SIZE;
        if let Some(last) = splits.last() {
            query.after_split_id = Some(last.metadata.split_id.to_string());
        }
        Ok(ParquetSplitsPage {
            splits,
            has_next_page,
        })
    }

    /// Collect all cursor pages. Prefer `list_page` for bounded-memory processing.
    pub async fn list_all(
        &self,
        mut query: ListParquetSplitsQuery,
    ) -> MetastoreResult<Vec<ParquetSplitRecord>> {
        let mut splits = Vec::new();
        loop {
            let mut page = self.list_page(&mut query).await?;
            splits.append(&mut page.splits);
            if !page.has_next_page {
                return Ok(splits);
            }
        }
    }

    /// Mark splits without deleting storage files. Callers retain grace-period policy.
    pub async fn mark_for_deletion(&self, split_ids: &[String]) -> MetastoreResult<()> {
        if split_ids.is_empty() {
            return Ok(());
        }
        self.mark_request(split_ids).await
    }

    /// Remove marked metadata *after* storage deletion succeeds. Errors propagate so
    /// callers can retry; this method never marks or deletes storage files itself.
    pub async fn delete(&self, split_ids: &[String]) -> MetastoreResult<()> {
        if split_ids.is_empty() {
            return Ok(());
        }
        self.delete_request(split_ids).await
    }

    fn validate_metadata(&self, split: &ParquetSplitMetadata) -> Result<(), String> {
        if split.index_uid != self.index_uid.to_string() || split.kind != self.kind {
            return Err(format!(
                "split `{}` does not belong to {} ({})",
                split.split_id, self.index_uid, self.kind
            ));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> MetastoreError {
    MetastoreError::InvalidArgument {
        message: message.into(),
    }
}
