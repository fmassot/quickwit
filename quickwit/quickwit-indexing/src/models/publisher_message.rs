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

use std::fmt;

use quickwit_metastore::SplitMetadata;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::types::{IndexUid, SplitId};
use tracing::Span;

use crate::merge_policy::MergeTask;
use crate::models::PublishLock;

/// The common publication envelope. The split and merge-task types keep storage
/// engines distinct at mailbox boundaries; no runtime engine dispatch is needed.
pub struct SplitUpdate<Split, Task> {
    pub index_uid: IndexUid,
    pub new_splits: Vec<Split>,
    pub replaced_split_ids: Vec<SplitId>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    /// Kept alive through metastore publication, source notification and planner
    /// feedback. Dropping it earlier can release in-flight merge inventory/permits.
    pub merge_task: Option<Task>,
    pub parent_span: Span,
}

impl<Split, Task> fmt::Debug for SplitUpdate<Split, Task> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("SplitUpdate")
            .field("index_uid", &self.index_uid)
            .field("num_new_splits", &self.new_splits.len())
            .field("replaced_split_ids", &self.replaced_split_ids)
            .field("checkpoint_delta", &self.checkpoint_delta_opt)
            .finish()
    }
}

/// Tantivy publication; Parquet uses its own split metadata and merge-task types.
pub type SplitsUpdate = SplitUpdate<SplitMetadata, MergeTask>;
