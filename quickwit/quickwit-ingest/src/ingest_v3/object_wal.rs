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

//! [`ObjectWal`]: the handle the ingester holds on its object-store log.
//!
//! It ties together the pieces of [`super::wal`] for one ingester:
//!
//! - **open**: fence the previous incarnation of this node's log, collect the records it left
//!   behind for replay, and start a writer right after the fence;
//! - **append / wait_durable**: the persist path;
//! - **GC**: remember which queue positions each object holds, learn about truncations (shard
//!   positions published by indexers), and delete objects once every record in them is truncated.
//!
//! # Epoch
//!
//! The epoch is what fences the previous writer of the log. It is issued by the log itself
//! (`tail epoch + 1`, claimed with a conditional PUT; see [`super::wal::fence::fence_log`]), so
//! it needs neither a clock nor the metastore, and it works the same whether the log is being
//! reopened by the same node id or taken over by another node. The cluster generation id of the
//! node is stamped on objects for observability only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use quickwit_proto::types::QueueId;
use quickwit_storage::Storage;
use tracing::{error, info, warn};

use super::wal::fence::fence_log;
use super::wal::reader::{WalReader, WalRecord};
use super::wal::writer::{Durability, WalWriter, WalWriterConfig, WalWriterStatus};
use super::wal::{WalError, WalId, WalResult};
use super::{IngestV3Config, metrics};

/// Interval of the background GC task.
const GC_INTERVAL: Duration = Duration::from_secs(30);

/// What pins one object, per queue: the position of its last record, or `None` for a queue
/// marker (pinned until the queue is deleted).
type ObjectIndex = HashMap<QueueId, Option<u64>>;

#[derive(Default)]
struct GcState {
    /// Objects that may still hold live records, with their last position per queue.
    objects: BTreeMap<WalId, ObjectIndex>,
    /// Highest truncated position (inclusive) per queue.
    truncated_up_to: HashMap<QueueId, u64>,
    /// Queues that no longer exist: their records never pin an object.
    deleted_queues: HashSet<QueueId>,
}

impl GcState {
    fn is_covered(&self, queue_id: &str, last_position: Option<u64>) -> bool {
        if self.deleted_queues.contains(queue_id) {
            return true;
        }
        let Some(last_position) = last_position else {
            return false;
        };
        matches!(
            self.truncated_up_to.get(queue_id),
            Some(&truncated) if truncated >= last_position
        )
    }

    /// Objects at or below `durable_wal_id` whose every record is truncated.
    fn collectable(&self, durable_wal_id: WalId) -> Vec<WalId> {
        self.objects
            .iter()
            .take_while(|(wal_id, _)| **wal_id <= durable_wal_id)
            .filter(|(_, index)| {
                index
                    .iter()
                    .all(|(queue_id, last_position)| self.is_covered(queue_id, *last_position))
            })
            .map(|(wal_id, _)| *wal_id)
            .collect()
    }
}

struct Inner {
    ingester_id: String,
    epoch: u64,
    writer: WalWriter,
    reader: WalReader,
    gc: Mutex<GcState>,
}

/// An ingester's object-store WAL. Cheap to clone.
#[derive(Clone)]
pub struct ObjectWal {
    inner: Arc<Inner>,
}

/// Result of [`ObjectWal::open`].
pub struct OpenedObjectWal {
    pub object_wal: ObjectWal,
    /// Records left in the log by previous incarnations of this ingester, in `(queue, position)`
    /// order. The caller decides what to do with them (typically: re-insert into the local
    /// queue structure so they can be served to indexers).
    pub replay: Vec<WalRecord>,
    /// Queues that exist according to the log, including ones without any record to replay.
    pub queues: Vec<QueueId>,
}

impl ObjectWal {
    /// Opens the log of `ingester_id`: fences any previous writer, collects its leftover records,
    /// and starts a new writer. `generation_id` is the node's cluster generation id, recorded on
    /// objects for observability.
    pub async fn open(
        storage: Arc<dyn Storage>,
        ingester_id: impl Into<String>,
        generation_id: u64,
        config: &IngestV3Config,
    ) -> WalResult<OpenedObjectWal> {
        let ingester_id = ingester_id.into();
        let reader = WalReader::new(storage.clone(), ingester_id.clone());

        // Everything the previous incarnation left behind. `list` also gives us a lower bound
        // for tail discovery that is above the GC boundary, which `fence_log` requires.
        let existing = reader.list().await?;
        let listed_tail = existing
            .last()
            .map(|object| object.wal_id)
            .unwrap_or(WalId::ZERO);
        let fence = fence_log(storage.clone(), &ingester_id, generation_id, listed_tail).await?;
        let (fence_id, epoch) = (fence.wal_id, fence.epoch);

        let mut gc = GcState::default();
        let mut replay = Vec::new();
        let mut queues: std::collections::BTreeSet<QueueId> = std::collections::BTreeSet::new();
        // Objects written between the listing and the fence are in `listed_tail + 1..fence_id`.
        let wal_ids = existing
            .iter()
            .map(|object| object.wal_id)
            .chain((listed_tail.0 + 1..fence_id.0).map(WalId));
        for wal_id in wal_ids {
            let (header, blocks) = match reader.read_object(wal_id).await {
                Ok(object) => object,
                Err(WalError::Storage(error))
                    if error.kind() == quickwit_storage::StorageErrorKind::NotFound =>
                {
                    // Collected between the listing and now.
                    continue;
                }
                Err(error) => return Err(error),
            };
            if header.is_fence {
                continue;
            }
            let mut index = ObjectIndex::new();
            for block in blocks {
                queues.insert(block.queue_id.clone());
                if block.is_queue_marker() {
                    index.insert(block.queue_id.clone(), None);
                    continue;
                }
                let last_position = block.last_position();
                index
                    .entry(block.queue_id.clone())
                    .and_modify(|last| *last = last.map(|l| l.max(last_position)))
                    .or_insert(Some(last_position));
                for (i, record) in block.records.into_iter().enumerate() {
                    replay.push(WalRecord {
                        queue_id: block.queue_id.clone(),
                        position: block.first_position + i as u64,
                        record,
                    });
                }
            }
            gc.objects.insert(wal_id, index);
        }
        replay.sort_by(|left, right| {
            left.queue_id
                .cmp(&right.queue_id)
                .then(left.position.cmp(&right.position))
        });
        metrics::WAL_OBJECT_REPLAYED_RECORDS_TOTAL.inc_by(replay.len() as u64);
        info!(
            ingester_id,
            epoch,
            fence_wal_id = fence_id.0,
            num_replay_records = replay.len(),
            "opened object WAL"
        );

        let writer_config = WalWriterConfig {
            flush_interval: config.flush_interval,
            flush_num_bytes: config.flush_num_bytes,
            max_buffered_num_bytes: config.max_buffered_num_bytes,
        };
        let writer = WalWriter::spawn(
            storage,
            ingester_id.clone(),
            epoch,
            generation_id,
            fence_id.next(),
            writer_config,
        );
        let inner = Arc::new(Inner {
            ingester_id,
            epoch,
            writer,
            reader,
            gc: Mutex::new(gc),
        });
        let object_wal = ObjectWal { inner };
        object_wal.spawn_gc_task();
        Ok(OpenedObjectWal {
            object_wal,
            replay,
            queues: queues.into_iter().collect(),
        })
    }

    pub fn ingester_id(&self) -> &str {
        &self.inner.ingester_id
    }

    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Buffers records for `queue_id` at consecutive positions from `first_position`. See
    /// [`WalWriter::append`].
    pub fn append(
        &self,
        queue_id: &QueueId,
        first_position: u64,
        records: Vec<Bytes>,
    ) -> WalResult<WalId> {
        let last_position = first_position + records.len() as u64 - 1;
        let wal_id = self
            .inner
            .writer
            .append(queue_id, first_position, records)?;
        let mut gc = self.inner.gc.lock().unwrap();
        gc.objects
            .entry(wal_id)
            .or_default()
            .entry(queue_id.clone())
            .and_modify(|last| *last = last.map(|l| l.max(last_position)))
            .or_insert(Some(last_position));
        Ok(wal_id)
    }

    /// Records durably that `queue_id` exists. The object holding the marker is retained until
    /// the queue is deleted ([`ObjectWal::on_delete_queue`]).
    pub fn append_queue_marker(&self, queue_id: &QueueId) -> WalResult<WalId> {
        let wal_id = self.inner.writer.append_queue_marker(queue_id)?;
        let mut gc = self.inner.gc.lock().unwrap();
        gc.objects
            .entry(wal_id)
            .or_default()
            .entry(queue_id.clone())
            .or_insert(None);
        Ok(wal_id)
    }

    /// See [`WalWriter::durability_receiver`].
    pub fn durability_receiver(&self) -> tokio::sync::watch::Receiver<Durability> {
        self.inner.writer.durability_receiver()
    }

    /// Resolves once `wal_id` is durable. See [`WalWriter::wait_durable`].
    pub async fn wait_durable(&self, wal_id: WalId) -> WalResult<()> {
        self.inner.writer.wait_durable(wal_id).await
    }

    /// Flushes and waits. See [`WalWriter::flush`].
    pub async fn flush(&self) -> WalResult<()> {
        self.inner.writer.flush().await
    }

    pub fn status(&self) -> WalWriterStatus {
        self.inner.writer.status()
    }

    /// Records that `queue_id` was truncated up to `position_inclusive`: objects holding only
    /// records at or below it become collectable.
    pub fn on_truncate(&self, queue_id: &QueueId, position_inclusive: u64) {
        let mut gc = self.inner.gc.lock().unwrap();
        gc.truncated_up_to
            .entry(queue_id.clone())
            .and_modify(|truncated| *truncated = (*truncated).max(position_inclusive))
            .or_insert(position_inclusive);
    }

    /// Records that `queue_id` was deleted: its records never pin an object anymore.
    pub fn on_delete_queue(&self, queue_id: &QueueId) {
        let mut gc = self.inner.gc.lock().unwrap();
        gc.deleted_queues.insert(queue_id.clone());
        gc.truncated_up_to.remove(queue_id);
    }

    /// Deletes every durable object whose records are all truncated. Returns the number of
    /// objects deleted. Fence objects are never deleted here.
    pub async fn gc(&self) -> WalResult<usize> {
        let durable_wal_id = self.inner.writer.status().durable_wal_id;
        let collectable = self.inner.gc.lock().unwrap().collectable(durable_wal_id);
        let mut num_deleted = 0;
        for wal_id in collectable {
            match self.inner.reader.delete(wal_id).await {
                Ok(()) => {
                    self.inner.gc.lock().unwrap().objects.remove(&wal_id);
                    metrics::WAL_OBJECT_GC_DELETED_BY_INGESTER.inc();
                    num_deleted += 1;
                }
                Err(error) => {
                    warn!(
                        ingester_id = %self.inner.ingester_id,
                        wal_id = wal_id.0,
                        %error,
                        "failed to delete WAL object"
                    );
                    return Err(error);
                }
            }
        }
        Ok(num_deleted)
    }

    /// Number of objects that may still hold live records.
    pub fn num_live_objects(&self) -> usize {
        self.inner.gc.lock().unwrap().objects.len()
    }

    /// Flushes and stops the writer.
    pub async fn close(&self) -> WalResult<()> {
        self.inner.writer.close().await
    }

    fn spawn_gc_task(&self) {
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(GC_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let object_wal = ObjectWal { inner };
                if let Err(error) = object_wal.gc().await {
                    error!(%error, "object WAL GC failed");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use quickwit_common::uri::Uri;
    use quickwit_storage::RamStorage;

    use super::*;

    fn config() -> IngestV3Config {
        let mut config = IngestV3Config::with_wal_uri(Uri::for_test("ram:///wal"));
        config.flush_interval = Duration::from_secs(3600);
        config
    }

    fn records(prefix: &str, num: usize) -> Vec<Bytes> {
        (0..num)
            .map(|i| Bytes::from(format!("{prefix}-{i}")))
            .collect()
    }

    #[tokio::test]
    async fn test_open_append_gc() {
        let storage = Arc::new(RamStorage::default());
        let opened = ObjectWal::open(storage.clone(), "node", 0, &config())
            .await
            .unwrap();
        assert!(opened.replay.is_empty());
        let wal = opened.object_wal;

        let q1 = "idx/src/1".to_string();
        let q2 = "idx/src/2".to_string();
        let id1 = wal.append(&q1, 0, records("a", 3)).unwrap();
        wal.append(&q2, 0, records("b", 1)).unwrap();
        wal.flush().await.unwrap();
        let id2 = wal.append(&q1, 3, records("c", 2)).unwrap();
        wal.flush().await.unwrap();
        assert_ne!(id1, id2);
        assert_eq!(wal.num_live_objects(), 2);

        // Nothing truncated: nothing collectable.
        assert_eq!(wal.gc().await.unwrap(), 0);

        // q1 truncated past object 1, but q2 still pins it.
        wal.on_truncate(&q1, 2);
        assert_eq!(wal.gc().await.unwrap(), 0);
        wal.on_truncate(&q2, 0);
        assert_eq!(wal.gc().await.unwrap(), 1);
        assert_eq!(wal.num_live_objects(), 1);

        // Deleting q1 releases object 2.
        wal.on_delete_queue(&q1);
        assert_eq!(wal.gc().await.unwrap(), 1);
        assert_eq!(wal.num_live_objects(), 0);

        // Only the fence remains.
        let reader = WalReader::new(storage.clone(), "node");
        let listed = reader.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert!(
            reader
                .read_footer(listed[0].wal_id, None)
                .await
                .unwrap()
                .header
                .is_fence
        );
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_reopen_replays_and_fences_previous_incarnation() {
        let storage = Arc::new(RamStorage::default());
        let first = ObjectWal::open(storage.clone(), "node", 0, &config())
            .await
            .unwrap()
            .object_wal;
        let q1 = "idx/src/1".to_string();
        first.append(&q1, 0, records("a", 2)).unwrap();
        first.flush().await.unwrap();
        first.append(&q1, 2, records("b", 1)).unwrap();
        first.flush().await.unwrap();
        // Buffered but never flushed: must not be replayed.
        first.append(&q1, 3, records("lost", 1)).unwrap();

        // Simulated restart of the same node (the first incarnation is still "alive").
        let reopened = ObjectWal::open(storage.clone(), "node", 0, &config())
            .await
            .unwrap();
        assert_eq!(first.epoch(), 1);
        assert_eq!(reopened.object_wal.epoch(), 2);
        let positions: Vec<u64> = reopened.replay.iter().map(|r| r.position).collect();
        assert_eq!(positions, vec![0, 1, 2]);
        assert_eq!(&reopened.replay[2].record[..], b"b-0");
        // Replayed objects are tracked for GC.
        assert_eq!(reopened.object_wal.num_live_objects(), 2);

        // The zombie is fenced on its next flush.
        assert!(matches!(first.flush().await, Err(WalError::Fenced)));

        // The new incarnation continues.
        let second = reopened.object_wal;
        second.append(&q1, 3, records("c", 1)).unwrap();
        second.flush().await.unwrap();
        let reader = WalReader::new(storage.clone(), "node");
        let tail = reader.last_wal_id(WalId::ZERO).await.unwrap();
        let all = reader.replay_queue(&q1, 0, 1..=tail.0).await.unwrap();
        assert_eq!(all.len(), 4);

        // Truncating everything then GC leaves only fences.
        second.on_truncate(&q1, 3);
        assert_eq!(second.gc().await.unwrap(), 3);
        second.close().await.unwrap();
    }
}
