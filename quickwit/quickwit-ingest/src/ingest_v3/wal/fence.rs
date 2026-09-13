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

//! Fencing: closing an ingester's log so that its current writer can no longer append to it,
//! and claiming the next epoch.
//!
//! The epoch is issued by the log itself. The fencer reads the epoch of the tail object,
//! proposes `tail_epoch + 1`, and claims it by writing a fence object at `tail + 1` with a
//! conditional PUT. The PUT is the linearization point: of any number of concurrent fencers
//! proposing the same epoch, exactly one succeeds; the others read the winner's fence back and
//! give up. No clock and no external store are involved, so the protocol is the same whether the
//! log is reopened by the same node id or taken over by another node.
//!
//! Who *should* fence is decided elsewhere (the control plane assigns shards to ingesters);
//! this module only guarantees that whoever fences last is the only one that can append.

use std::sync::Arc;

use quickwit_storage::{Storage, StorageErrorKind};
use tracing::{debug, info};

use super::format::encode_fence_object;
use super::reader::WalReader;
use super::{WalError, WalId, WalResult, wal_object_path};

/// A successfully written fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fence {
    /// Id of the fence object. The log is closed at this id; the new writer starts at
    /// `wal_id + 1`.
    pub wal_id: WalId,
    /// Epoch claimed by the fence, to be used by the new writer.
    pub epoch: u64,
}

/// Closes the log of `ingester_id` by writing an empty fence object at `tail + 1`, and returns
/// the fence together with the epoch it claimed.
///
/// After this returns, the log is a finite sequence `..=fence.wal_id`: the previous writer's
/// next `put_if_absent` at `fence.wal_id` fails with `AlreadyExists` and it stops. Everything
/// the previous writer acknowledged is at ids `< fence.wal_id`, so
/// `replay_after + 1..=fence.wal_id` is the complete range to recover.
///
/// `lower_bound` must be an id at or below the current tail such that all ids in
/// `lower_bound + 1..=tail` exist (e.g. the highest id from a listing, or [`WalId::ZERO`] for a
/// log that was never garbage collected).
///
/// `generation_id` is stamped on the fence object for observability.
///
/// Returns [`WalError::Fenced`] if another fencer claimed the slot with an epoch at least as
/// high as the one proposed here: it now owns the log, and the caller must not.
pub async fn fence_log(
    storage: Arc<dyn Storage>,
    ingester_id: &str,
    generation_id: u64,
    lower_bound: WalId,
) -> WalResult<Fence> {
    let reader = WalReader::new(storage.clone(), ingester_id);
    let mut lower_bound = lower_bound;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let tail = reader.last_wal_id(lower_bound).await?;
        let tail_epoch = if tail > WalId::ZERO {
            match reader.read_footer(tail, None).await {
                Ok(footer) => footer.header.epoch,
                Err(WalError::Storage(error)) if error.kind() == StorageErrorKind::NotFound => {
                    // Collected between the probe and the read. Fences are never collected,
                    // so there is always an object to find at or below the tail eventually.
                    debug!(
                        ingester_id,
                        wal_id = tail.0,
                        "tail object vanished, re-probing"
                    );
                    lower_bound = WalId::ZERO;
                    continue;
                }
                Err(error) => return Err(error),
            }
        } else {
            0
        };
        let epoch = tail_epoch + 1;
        let candidate = tail.next();
        let fence_bytes = encode_fence_object(ingester_id, epoch, generation_id, candidate);
        let path = wal_object_path(ingester_id, candidate);
        match storage
            .put_if_absent(&path, Box::new(fence_bytes.to_vec()))
            .await
        {
            Ok(()) => {
                info!(
                    ingester_id,
                    epoch,
                    fence_wal_id = candidate.0,
                    attempt,
                    "fenced WAL"
                );
                return Ok(Fence {
                    wal_id: candidate,
                    epoch,
                });
            }
            Err(error) if error.kind() == StorageErrorKind::AlreadyExists => {
                // Somebody wrote `candidate` between our probe and our PUT: either the writer
                // we are fencing (lower epoch: it is still alive, move up), or a concurrent
                // fencer that claimed the same or a higher epoch (we lost).
                let footer = reader.read_footer(candidate, None).await?;
                if footer.header.epoch >= epoch {
                    return Err(WalError::Fenced);
                }
                debug!(
                    ingester_id,
                    epoch,
                    wal_id = candidate.0,
                    attempt,
                    "WAL slot taken by the previous writer, retrying at the next id"
                );
                lower_bound = candidate;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::ingest_v3::wal::format::{WalObjectHeader, encode_wal_object};

    async fn write_data_object(storage: &RamStorage, wal_id: u64, epoch: u64) {
        let header = WalObjectHeader {
            ingester_id: "node".to_string(),
            epoch,
            generation_id: 0,
            wal_id: WalId(wal_id),
            is_fence: false,
        };
        storage
            .put(
                &wal_object_path("node", WalId(wal_id)),
                Box::new(encode_wal_object(&header, &[]).to_vec()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fence_empty_log_claims_epoch_1() {
        let storage = Arc::new(RamStorage::default());
        let fence = fence_log(storage.clone(), "node", 42, WalId::ZERO)
            .await
            .unwrap();
        assert_eq!(
            fence,
            Fence {
                wal_id: WalId(1),
                epoch: 1
            }
        );
        let reader = WalReader::new(storage, "node");
        let footer = reader.read_footer(WalId(1), None).await.unwrap();
        assert!(footer.header.is_fence);
        assert_eq!(footer.header.epoch, 1);
        assert_eq!(footer.header.generation_id, 42);
    }

    #[tokio::test]
    async fn test_epochs_increase_with_each_fence() {
        let storage = Arc::new(RamStorage::default());
        let first = fence_log(storage.clone(), "node", 0, WalId::ZERO)
            .await
            .unwrap();
        write_data_object(&storage, 2, first.epoch).await;
        write_data_object(&storage, 3, first.epoch).await;
        let second = fence_log(storage.clone(), "node", 0, WalId::ZERO)
            .await
            .unwrap();
        assert_eq!(
            second,
            Fence {
                wal_id: WalId(4),
                epoch: 2
            }
        );
        // Same lower bound semantics with a hint above the collected prefix.
        let third = fence_log(storage.clone(), "node", 0, WalId(3))
            .await
            .unwrap();
        assert_eq!(
            third,
            Fence {
                wal_id: WalId(5),
                epoch: 3
            }
        );

        // The writer of epoch 1 tries to write its next object: it must fail.
        let stale = encode_fence_object("node", first.epoch, 0, WalId(5));
        let error = storage
            .put_if_absent(&wal_object_path("node", WalId(5)), Box::new(stale.to_vec()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::AlreadyExists);
    }

    #[tokio::test]
    async fn test_sequential_fencers_each_take_over() {
        // Without contention, every later fencer legitimately supersedes the previous one.
        let storage = Arc::new(RamStorage::default());
        write_data_object(&storage, 1, 1).await;
        for i in 0..4u64 {
            let fence = fence_log(storage.clone(), "node", i, WalId::ZERO)
                .await
                .unwrap();
            assert_eq!(
                fence,
                Fence {
                    wal_id: WalId(2 + i),
                    epoch: 2 + i
                }
            );
        }
    }

    #[tokio::test]
    async fn test_concurrent_fencers_exactly_one_wins() {
        for _ in 0..20 {
            let inner = RamStorage::default();
            write_data_object(&inner, 1, 1).await;
            let storage = Arc::new(LaggingStorage {
                inner,
                hidden: std::sync::Mutex::new(None),
                barrier: Some(Arc::new(tokio::sync::Barrier::new(4))),
            });
            let results = futures::future::join_all(
                (0..4u64).map(|i| fence_log(storage.clone(), "node", i, WalId::ZERO)),
            )
            .await;
            let winners: Vec<&Fence> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
            assert_eq!(winners.len(), 1, "{results:?}");
            assert_eq!(winners[0].epoch, 2);
            assert!(
                results
                    .iter()
                    .filter_map(|r| r.as_ref().err())
                    .all(|error| matches!(error, WalError::Fenced))
            );
        }
    }

    /// Delegates to a `RamStorage`, with two knobs to build races:
    /// - `hidden`: reported as absent to `file_num_bytes` (hence `exists`) until the first
    ///   `put_if_absent`, reproducing the window between a fencer's tail probe and its PUT;
    /// - `barrier`: every `put_if_absent` waits for `n` callers before proceeding, so that
    ///   concurrent fencers all probe the same tail before any of them writes.
    #[derive(Debug)]
    struct LaggingStorage {
        inner: RamStorage,
        hidden: std::sync::Mutex<Option<std::path::PathBuf>>,
        barrier: Option<Arc<tokio::sync::Barrier>>,
    }

    #[async_trait::async_trait]
    impl Storage for LaggingStorage {
        async fn check_connectivity(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn put(
            &self,
            path: &std::path::Path,
            payload: Box<dyn quickwit_storage::PutPayload>,
        ) -> quickwit_storage::StorageResult<()> {
            self.inner.put(path, payload).await
        }
        async fn put_if_absent(
            &self,
            path: &std::path::Path,
            payload: Box<dyn quickwit_storage::PutPayload>,
        ) -> quickwit_storage::StorageResult<()> {
            // The fencer's PUT: from now on the hidden object is visible.
            self.hidden.lock().unwrap().take();
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            self.inner.put_if_absent(path, payload).await
        }
        fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            path: &'life1 std::path::Path,
            output: &'life2 mut dyn quickwit_storage::SendableAsync,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = quickwit_storage::StorageResult<()>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.copy_to(path, output)
        }
        async fn get_slice(
            &self,
            path: &std::path::Path,
            range: std::ops::Range<usize>,
        ) -> quickwit_storage::StorageResult<quickwit_storage::OwnedBytes> {
            self.inner.get_slice(path, range).await
        }
        async fn get_slice_stream(
            &self,
            path: &std::path::Path,
            range: std::ops::Range<usize>,
        ) -> quickwit_storage::StorageResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
            self.inner.get_slice_stream(path, range).await
        }
        async fn get_all(
            &self,
            path: &std::path::Path,
        ) -> quickwit_storage::StorageResult<quickwit_storage::OwnedBytes> {
            self.inner.get_all(path).await
        }
        async fn delete(&self, path: &std::path::Path) -> quickwit_storage::StorageResult<()> {
            self.inner.delete(path).await
        }
        async fn bulk_delete<'a>(
            &self,
            paths: &[&'a std::path::Path],
        ) -> Result<(), quickwit_storage::BulkDeleteError> {
            self.inner.bulk_delete(paths).await
        }
        async fn file_num_bytes(
            &self,
            path: &std::path::Path,
        ) -> quickwit_storage::StorageResult<u64> {
            if self.hidden.lock().unwrap().as_deref() == Some(path) {
                return Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("hidden")));
            }
            self.inner.file_num_bytes(path).await
        }
        fn uri(&self) -> &quickwit_common::uri::Uri {
            self.inner.uri()
        }
    }

    #[tokio::test]
    async fn test_fencer_racing_a_live_writer_moves_up() {
        // The old writer (epoch 1) appends object 2 between our tail probe (which saw 1) and
        // our PUT at 2. The PUT fails with a lower-epoch object: we retry at 3.
        let inner = RamStorage::default();
        write_data_object(&inner, 1, 1).await;
        write_data_object(&inner, 2, 1).await;
        let storage = Arc::new(LaggingStorage {
            inner,
            hidden: std::sync::Mutex::new(Some(wal_object_path("node", WalId(2)))),
            barrier: None,
        });
        let fence = fence_log(storage.clone(), "node", 0, WalId::ZERO)
            .await
            .unwrap();
        assert_eq!(
            fence,
            Fence {
                wal_id: WalId(3),
                epoch: 2
            }
        );
        // Object 2 is intact.
        let reader = WalReader::new(storage, "node");
        assert_eq!(
            reader
                .read_footer(WalId(2), None)
                .await
                .unwrap()
                .header
                .epoch,
            1
        );
    }
}
