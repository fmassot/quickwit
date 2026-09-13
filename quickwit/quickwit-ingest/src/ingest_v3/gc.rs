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

//! Cluster-wide garbage collection of the ingest WAL, run by the janitor.
//!
//! An ingester deletes its own objects as its shards get truncated (see
//! [`super::ObjectWal`]). This pass covers everything that has no owner to do it: logs of
//! ingesters that left the cluster (drained by indexers, §3.11 of the design doc), objects
//! an ingester did not get to delete before dying, and old fence objects.
//!
//! One pass:
//!
//! 1. list the WAL root, group objects per ingester log;
//! 2. read the footer of every object (cached: objects are immutable);
//! 3. ask the metastore for the publish position of every shard referenced;
//! 4. delete every object older than `min_age` whose records are all published (or whose shards no
//!    longer exist), and every fence but the newest one of its log.
//!
//! The newest fence of a log is never deleted: it carries the log's current epoch when no
//! data object follows it (see [`super::wal::fence::fence_log`]). One ~60-byte object per
//! node id ever seen is the price.
//!
//! Deleting an object out from under a live ingester is safe: it only happens once every
//! record is published, and the ingester's own GC tolerates `NotFound`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_proto::types::{IndexUid, Position, ShardId, SourceId, split_queue_id};
use quickwit_storage::{Storage, StorageErrorKind};
use tracing::{info, warn};

use super::wal::format::WalFooter;
use super::wal::reader::WalReader;
use super::wal::{WalError, WalId, parse_wal_object_path};

/// Where the records of a shard stand, as far as the metastore knows.
#[derive(Clone, Debug)]
pub enum ShardPublishStatus {
    /// Published up to this position (inclusive). `Position::Beginning` = nothing yet.
    Published(Position),
    /// The shard, its source or its index no longer exists: nothing will ever read it.
    Gone,
}

impl ShardPublishStatus {
    /// `None` is a queue marker: only a gone shard covers it.
    fn covers(&self, last_position: Option<u64>) -> bool {
        let Some(last_position) = last_position else {
            return matches!(self, ShardPublishStatus::Gone);
        };
        match self {
            ShardPublishStatus::Gone => true,
            ShardPublishStatus::Published(position) => match position {
                Position::Eof(_) => true,
                Position::Offset(offset) => {
                    matches!(offset.as_u64(), Some(p) if p >= last_position)
                }
                _ => false,
            },
        }
    }
}

/// Source of publish positions (the metastore in production).
#[async_trait]
pub trait PublishedPositions: Send + Sync {
    /// Returns the publish status of every shard of the source, or `None` if the source or the
    /// index no longer exists. Shards missing from the map are gone.
    async fn list_shard_positions(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
    ) -> anyhow::Result<Option<HashMap<ShardId, Position>>>;
}

/// Parameters of a GC pass.
#[derive(Clone, Debug)]
pub struct IngestWalGcParams {
    /// Objects younger than this are never deleted. Guards against listing staleness and
    /// against racing an ingester's own bookkeeping; not needed for correctness.
    pub min_age: Duration,
    /// Report what would be deleted without deleting it.
    pub dry_run: bool,
}

impl Default for IngestWalGcParams {
    fn default() -> Self {
        Self {
            min_age: Duration::from_secs(10 * 60),
            dry_run: false,
        }
    }
}

/// Outcome of a GC pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IngestWalGcReport {
    pub num_logs: usize,
    pub num_objects: usize,
    pub num_deleted_objects: usize,
    pub num_deleted_bytes: u64,
    /// Objects that could not be assessed (footer read failed, metastore error) and were kept.
    pub num_skipped_objects: usize,
    pub num_failed_deletions: usize,
}

/// Footers cache, keyed by object path. Objects are immutable so entries never go stale;
/// entries for deleted objects are dropped after each pass.
pub type FooterCache = HashMap<PathBuf, WalFooter>;

struct ListedObject {
    ingester_id: String,
    wal_id: WalId,
    path: PathBuf,
    num_bytes: u64,
    last_modified: SystemTime,
}

/// Runs one GC pass over the WAL rooted at `storage`.
pub async fn collect_ingest_wal_garbage(
    storage: Arc<dyn Storage>,
    positions: &dyn PublishedPositions,
    params: &IngestWalGcParams,
    footer_cache: &mut FooterCache,
    now: SystemTime,
) -> anyhow::Result<IngestWalGcReport> {
    let mut report = IngestWalGcReport::default();

    // 1. List and group per log.
    let mut logs: BTreeMap<String, Vec<ListedObject>> = BTreeMap::new();
    let mut stream = storage.list(std::path::Path::new(""));
    while let Some(batch) = stream.next().await {
        for object in batch? {
            let Some((ingester_id, wal_id)) = parse_ingester_object_path(&object.path) else {
                continue;
            };
            logs.entry(ingester_id.clone())
                .or_default()
                .push(ListedObject {
                    ingester_id,
                    wal_id,
                    path: object.path,
                    num_bytes: object.size.as_u64(),
                    last_modified: object.last_modified,
                });
        }
    }
    report.num_logs = logs.len();
    report.num_objects = logs.values().map(Vec::len).sum();

    // 2. Footers.
    let mut footers: HashMap<PathBuf, WalFooter> = HashMap::new();
    for objects in logs.values() {
        let reader = WalReader::new(storage.clone(), objects[0].ingester_id.clone());
        for object in objects {
            if let Some(footer) = footer_cache.get(&object.path) {
                footers.insert(object.path.clone(), footer.clone());
                continue;
            }
            match reader
                .read_footer(object.wal_id, Some(object.num_bytes))
                .await
            {
                Ok(footer) => {
                    footer_cache.insert(object.path.clone(), footer.clone());
                    footers.insert(object.path.clone(), footer);
                }
                Err(WalError::Storage(error)) if error.kind() == StorageErrorKind::NotFound => {
                    // Deleted since the listing (by its owner).
                }
                Err(error) => {
                    warn!(path = %object.path.display(), %error, "failed to read WAL object footer");
                    report.num_skipped_objects += 1;
                }
            }
        }
    }

    // 3. Publish positions for every (index, source) referenced.
    let mut sources: BTreeMap<(IndexUid, SourceId), Option<HashMap<ShardId, Position>>> =
        BTreeMap::new();
    let mut sources_in_error: BTreeMap<(IndexUid, SourceId), ()> = BTreeMap::new();
    for footer in footers.values() {
        for block in &footer.blocks {
            let Some((index_uid, source_id, _shard_id)) = split_queue_id(&block.queue_id) else {
                continue;
            };
            let key = (index_uid, source_id);
            if sources.contains_key(&key) || sources_in_error.contains_key(&key) {
                continue;
            }
            match positions.list_shard_positions(&key.0, &key.1).await {
                Ok(shard_positions) => {
                    sources.insert(key, shard_positions);
                }
                Err(error) => {
                    warn!(index_uid = %key.0, source_id = %key.1, %error, "failed to list shard positions");
                    sources_in_error.insert(key, ());
                }
            }
        }
    }
    let shard_status = |queue_id: &str| -> Option<ShardPublishStatus> {
        let (index_uid, source_id, shard_id) = split_queue_id(queue_id)?;
        let key = (index_uid, source_id);
        if sources_in_error.contains_key(&key) {
            return None;
        }
        Some(match sources.get(&key) {
            // Unknown source: never asked, which can only happen for an unparsable queue id.
            None => return None,
            Some(None) => ShardPublishStatus::Gone,
            Some(Some(shard_positions)) => match shard_positions.get(&shard_id) {
                None => ShardPublishStatus::Gone,
                Some(position) => ShardPublishStatus::Published(position.clone()),
            },
        })
    };

    // 4. Decide and delete.
    let mut to_delete: Vec<&ListedObject> = Vec::new();
    for objects in logs.values() {
        let newest_fence = objects
            .iter()
            .filter(|object| {
                matches!(footers.get(&object.path), Some(footer) if footer.header.is_fence)
            })
            .map(|object| object.wal_id)
            .max();
        for object in objects {
            let Some(footer) = footers.get(&object.path) else {
                continue;
            };
            let age = now.duration_since(object.last_modified).unwrap_or_default();
            if age < params.min_age {
                continue;
            }
            let deletable = if footer.header.is_fence {
                Some(object.wal_id) != newest_fence
            } else {
                let mut all_covered = true;
                for block in &footer.blocks {
                    let last_position = if block.is_queue_marker() {
                        None
                    } else {
                        Some(block.last_position())
                    };
                    match shard_status(&block.queue_id) {
                        Some(status) if status.covers(last_position) => {}
                        Some(_) => {
                            all_covered = false;
                            break;
                        }
                        None => {
                            all_covered = false;
                            report.num_skipped_objects += 1;
                            break;
                        }
                    }
                }
                all_covered
            };
            if deletable {
                to_delete.push(object);
            }
        }
    }

    if to_delete.is_empty() {
        return Ok(report);
    }
    info!(
        num_objects = to_delete.len(),
        num_bytes = to_delete.iter().map(|o| o.num_bytes).sum::<u64>(),
        dry_run = params.dry_run,
        "ingest WAL GC: deleting objects"
    );
    if params.dry_run {
        report.num_deleted_objects = to_delete.len();
        report.num_deleted_bytes = to_delete.iter().map(|o| o.num_bytes).sum();
        return Ok(report);
    }
    let paths: Vec<&std::path::Path> = to_delete.iter().map(|o| o.path.as_path()).collect();
    match storage.bulk_delete(&paths).await {
        Ok(()) => {
            report.num_deleted_objects = to_delete.len();
            report.num_deleted_bytes = to_delete.iter().map(|o| o.num_bytes).sum();
            for object in &to_delete {
                footer_cache.remove(&object.path);
            }
        }
        Err(bulk_delete_error) => {
            let failed: std::collections::HashSet<&PathBuf> = bulk_delete_error
                .failures
                .keys()
                .chain(bulk_delete_error.unattempted.iter())
                .collect();
            for object in &to_delete {
                if failed.contains(&object.path) {
                    report.num_failed_deletions += 1;
                } else {
                    report.num_deleted_objects += 1;
                    report.num_deleted_bytes += object.num_bytes;
                    footer_cache.remove(&object.path);
                }
            }
            warn!(
                num_failed = report.num_failed_deletions,
                error = %bulk_delete_error,
                "ingest WAL GC: some deletions failed"
            );
        }
    }
    Ok(report)
}

/// Parses `<ingester_id>/wal/<wal_id>.wal`.
fn parse_ingester_object_path(path: &std::path::Path) -> Option<(String, WalId)> {
    let wal_id = parse_wal_object_path(path)?;
    let mut components = path.components();
    let ingester_id = components.next()?.as_os_str().to_str()?.to_string();
    if components.next()?.as_os_str() != "wal" {
        return None;
    }
    Some((ingester_id, wal_id))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use quickwit_proto::types::queue_id;
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::ingest_v3::wal::fence::fence_log;
    use crate::ingest_v3::wal::writer::{WalWriter, WalWriterConfig};

    struct FakePositions(HashMap<(IndexUid, SourceId), Option<HashMap<ShardId, Position>>>);

    #[async_trait]
    impl PublishedPositions for FakePositions {
        async fn list_shard_positions(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
        ) -> anyhow::Result<Option<HashMap<ShardId, Position>>> {
            match self.0.get(&(index_uid.clone(), source_id.clone())) {
                Some(positions) => Ok(positions.clone()),
                None => anyhow::bail!("metastore unavailable"),
            }
        }
    }

    async fn write_log(
        storage: &Arc<dyn Storage>,
        ingester_id: &str,
        appends: &[(&str, u64, usize)],
    ) {
        // One object per append, after a fence at 1.
        let fence = fence_log(storage.clone(), ingester_id, 0, WalId::ZERO)
            .await
            .unwrap();
        let writer = WalWriter::spawn(
            storage.clone(),
            ingester_id,
            fence.epoch,
            0,
            fence.wal_id.next(),
            WalWriterConfig {
                flush_interval: Duration::from_secs(3600),
                ..Default::default()
            },
        );
        for (queue_id, first_position, num_records) in appends {
            let records = (0..*num_records)
                .map(|i| Bytes::from(format!("{queue_id}:{}", first_position + i as u64)))
                .collect();
            writer.append(queue_id, *first_position, records).unwrap();
            writer.flush().await.unwrap();
        }
        writer.close().await.unwrap();
    }

    async fn list_ids(storage: &Arc<dyn Storage>, ingester_id: &str) -> Vec<u64> {
        WalReader::new(storage.clone(), ingester_id)
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.wal_id.0)
            .collect()
    }

    #[tokio::test]
    async fn test_gc_deletes_published_objects_and_old_fences() {
        let storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
        let index_uid = IndexUid::for_test("idx", 0);
        let source_id = "src".to_string();
        let q1 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let q2 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        // Log of node A: fence(1), obj2 {q1: 0..=2}, obj3 {q2: 0..=0}, obj4 {q1: 3..=4}.
        write_log(&storage, "A", &[(&q1, 0, 3), (&q2, 0, 1), (&q1, 3, 2)]).await;
        // A restarted once: another fence at 5.
        fence_log(storage.clone(), "A", 0, WalId::ZERO)
            .await
            .unwrap();
        assert_eq!(list_ids(&storage, "A").await, vec![1, 2, 3, 4, 5]);

        let positions = FakePositions(HashMap::from([(
            (index_uid.clone(), source_id.clone()),
            Some(HashMap::from([
                (ShardId::from(1), Position::offset(2u64)),
                (ShardId::from(2), Position::Beginning),
            ])),
        )]));
        let params = IngestWalGcParams {
            min_age: Duration::ZERO,
            dry_run: false,
        };
        let mut cache = FooterCache::new();
        let far_future = SystemTime::now() + Duration::from_secs(3600);

        let report = collect_ingest_wal_garbage(
            storage.clone(),
            &positions,
            &params,
            &mut cache,
            far_future,
        )
        .await
        .unwrap();
        // obj2 (q1 ≤ 2: published) and fence 1 (not the newest) go; obj3 (q2 nothing
        // published), obj4 (q1 3..=4 > 2) and fence 5 (newest) stay.
        assert_eq!(report.num_deleted_objects, 2);
        assert_eq!(report.num_skipped_objects, 0);
        assert_eq!(list_ids(&storage, "A").await, vec![3, 4, 5]);

        // q1 reaches EOF, q2 is deleted from the metastore.
        let positions = FakePositions(HashMap::from([(
            (index_uid.clone(), source_id.clone()),
            Some(HashMap::from([(ShardId::from(1), Position::eof(4u64))])),
        )]));
        let report = collect_ingest_wal_garbage(
            storage.clone(),
            &positions,
            &params,
            &mut cache,
            far_future,
        )
        .await
        .unwrap();
        assert_eq!(report.num_deleted_objects, 2);
        assert_eq!(list_ids(&storage, "A").await, vec![5]);
        assert_eq!(cache.len(), 1);

        // Index gone entirely: only the newest fence survives, forever.
        let positions = FakePositions(HashMap::from([((index_uid, source_id), None)]));
        let report = collect_ingest_wal_garbage(
            storage.clone(),
            &positions,
            &params,
            &mut cache,
            far_future,
        )
        .await
        .unwrap();
        assert_eq!(report.num_deleted_objects, 0);
        assert_eq!(list_ids(&storage, "A").await, vec![5]);
    }

    #[tokio::test]
    async fn test_gc_respects_min_age_dry_run_and_metastore_errors() {
        let storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
        let index_uid = IndexUid::for_test("idx", 0);
        let source_id = "src".to_string();
        let q1 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        write_log(&storage, "A", &[(&q1, 0, 1)]).await;
        write_log(&storage, "B", &[(&q1, 1, 1)]).await;

        let gone = FakePositions(HashMap::from([(
            (index_uid.clone(), source_id.clone()),
            None,
        )]));
        let mut cache = FooterCache::new();

        // Too young (RamStorage reports UNIX_EPOCH as mtime, so use `now = UNIX_EPOCH`).
        let params = IngestWalGcParams {
            min_age: Duration::from_secs(60),
            dry_run: false,
        };
        let report = collect_ingest_wal_garbage(
            storage.clone(),
            &gone,
            &params,
            &mut cache,
            SystemTime::UNIX_EPOCH,
        )
        .await
        .unwrap();
        assert_eq!(report.num_logs, 2);
        assert_eq!(report.num_objects, 4);
        assert_eq!(report.num_deleted_objects, 0);

        // Dry run: reports, deletes nothing.
        let params = IngestWalGcParams {
            min_age: Duration::ZERO,
            dry_run: true,
        };
        let far_future = SystemTime::now() + Duration::from_secs(3600);
        let report =
            collect_ingest_wal_garbage(storage.clone(), &gone, &params, &mut cache, far_future)
                .await
                .unwrap();
        assert_eq!(report.num_deleted_objects, 2);
        assert_eq!(list_ids(&storage, "A").await.len(), 2);

        // Metastore error: data objects are kept and counted as skipped; fences are still
        // handled since they need no positions.
        let erroring = FakePositions(HashMap::new());
        let params = IngestWalGcParams {
            min_age: Duration::ZERO,
            dry_run: false,
        };
        let report =
            collect_ingest_wal_garbage(storage.clone(), &erroring, &params, &mut cache, far_future)
                .await
                .unwrap();
        assert_eq!(report.num_deleted_objects, 0);
        assert_eq!(report.num_skipped_objects, 2);
        assert_eq!(list_ids(&storage, "A").await.len(), 2);
    }
}
