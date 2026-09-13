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

//! Object-store write-ahead log.
//!
//! - [`format`]: encoding/decoding of a WAL object.
//! - [`writer`]: in-memory buffering, group commit, durability notifications, fencing detection.
//! - [`reader`]: tail discovery, footer scan, per-queue replay.
//! - [`fence`]: closing another ingester's log.

pub mod fence;
pub mod format;
pub mod reader;
pub mod writer;

use std::fmt;
use std::path::PathBuf;

use quickwit_storage::{StorageError, StorageErrorKind};

/// Monotonically increasing, contiguous id of an object within one ingester's log.
///
/// `0` is never written: it is the "nothing flushed yet" boundary, so the first object of a log
/// has id `1`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WalId(pub u64);

impl WalId {
    /// The boundary before the first object.
    pub const ZERO: WalId = WalId(0);

    /// Returns the next id.
    pub fn next(self) -> WalId {
        WalId(self.0 + 1)
    }
}

impl fmt::Display for WalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

impl From<u64> for WalId {
    fn from(value: u64) -> Self {
        WalId(value)
    }
}

const WAL_DIR: &str = "wal";
const WAL_EXTENSION: &str = "wal";

/// Returns the directory of an ingester's log, relative to the WAL storage root.
pub fn wal_dir(ingester_id: &str) -> PathBuf {
    PathBuf::from(ingester_id).join(WAL_DIR)
}

/// Returns the path of a WAL object, relative to the WAL storage root:
/// `<ingester_id>/wal/<wal_id:020>.wal`.
pub fn wal_object_path(ingester_id: &str, wal_id: WalId) -> PathBuf {
    wal_dir(ingester_id).join(format!("{wal_id}.{WAL_EXTENSION}"))
}

/// Parses the [`WalId`] out of a WAL object path (relative to the WAL storage root).
pub fn parse_wal_object_path(path: &std::path::Path) -> Option<WalId> {
    if path.extension()?.to_str()? != WAL_EXTENSION {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    if stem.len() != 20 {
        return None;
    }
    stem.parse::<u64>().ok().map(WalId)
}

/// Errors returned by the WAL.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WalError {
    /// Another writer claimed a later epoch: this writer must stop.
    #[error("WAL writer was fenced")]
    Fenced,
    /// The in-memory buffer exceeded its configured capacity; retry after a flush.
    #[error("WAL buffer is full")]
    BufferFull,
    /// The writer was shut down.
    #[error("WAL writer is closed")]
    Closed,
    /// A WAL object failed integrity or format checks.
    #[error("WAL object is corrupted: {0}")]
    Corrupted(String),
    /// The underlying storage failed.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

impl WalError {
    /// Whether the error is [`WalError::Storage`] with the given kind.
    pub fn is_storage_kind(&self, kind: StorageErrorKind) -> bool {
        matches!(self, WalError::Storage(error) if error.kind() == kind)
    }
}

pub type WalResult<T> = Result<T, WalError>;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_wal_object_path_roundtrip() {
        let path = wal_object_path("ingester-1", WalId(42));
        assert_eq!(path, Path::new("ingester-1/wal/00000000000000000042.wal"));
        assert_eq!(parse_wal_object_path(&path), Some(WalId(42)));
        assert_eq!(
            parse_wal_object_path(Path::new("ingester-1/wal/foo.wal")),
            None
        );
        assert_eq!(
            parse_wal_object_path(Path::new("ingester-1/wal/42.wal")),
            None
        );
        assert_eq!(
            parse_wal_object_path(Path::new("ingester-1/wal/00000000000000000042.tmp")),
            None
        );
    }

    #[test]
    fn test_wal_id_sorts_lexicographically() {
        assert!(WalId(9).to_string() < WalId(10).to_string());
        assert!(WalId(u64::MAX - 1).to_string() < WalId(u64::MAX).to_string());
    }
}
