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

use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use itertools::Itertools;
use mrecordlog::error::{DeleteQueueError, TruncateError};
use quickwit_cluster::Cluster;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::shared_consts::INGESTER_STATUS_KEY;
use quickwit_doc_mapper::DocMapper;
use quickwit_metrics::{gauge, histogram, labels};
use quickwit_proto::control_plane::AdviseResetShardsResponse;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, ShardIds, ShardState};
use quickwit_proto::types::{DocMappingUid, IndexUid, Position, QueueId, SourceId, split_queue_id};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockMappedWriteGuard, RwLockWriteGuard, watch};
use tracing::{error, info, instrument, warn};

use super::models::IngesterShard;
use super::rate_meter::RateMeter;
use super::wal_capacity_tracker::WalCapacityTracker;
use crate::OpenShardCounts;
use crate::ingest_v3::mem_queue::{MemQueue, MemQueueUsage};
use crate::ingest_v3::{ObjectWal, OpenedObjectWal};
use crate::mrecordlog_async::MultiRecordLogAsync;

/// Stores the state of the ingester and attempts to prevent deadlocks by exposing an API that
/// guarantees that the internal data structures are always locked in the same order.
///
/// `lock_partially` locks `inner` only, while `lock_fully` locks both `inner` and `mrecordlog`. Use
/// the former when you only need to access the in-memory state of the ingester and the latter when
/// you need to access both the in-memory state AND the WAL.
#[derive(Clone)]
pub(super) struct IngesterState {
    // `inner` is a mutex because it's almost always accessed mutably.
    inner: Arc<Mutex<InnerIngesterState>>,
    mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
    /// Ingest v3: shared with `inner.mem_queue_usage`; readable without the lock.
    mem_queue_usage: Arc<MemQueueUsage>,
    pub status_rx: watch::Receiver<IngesterStatus>,
}

pub(super) struct InnerIngesterState {
    pub shards: HashMap<QueueId, IngesterShard>,
    pub doc_mappers: HashMap<DocMappingUid, Weak<DocMapper>>,
    cluster: Cluster,
    pub wal_capacity_tracker: WalCapacityTracker,
    /// Ingest v3: the object-store WAL, when enabled. Records are appended to it and to the
    /// shards' in-memory queues; persist requests are acknowledged once they are durable in it.
    pub object_wal: Option<ObjectWal>,
    /// Ingest v3: node-wide accounting of the bytes buffered in the shards' in-memory queues.
    pub mem_queue_usage: Arc<MemQueueUsage>,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    status_tx: watch::Sender<IngesterStatus>,
}

impl InnerIngesterState {
    pub fn status(&self) -> IngesterStatus {
        *self.status_tx.borrow()
    }

    /// Sets the status and notifies observation streams, even if the status has not changed.
    fn set_status_and_notify(&self, status: IngesterStatus) {
        self.status_tx.send(status).expect("channel should be open");
    }

    pub async fn set_status(&mut self, status: IngesterStatus) {
        self.set_status_and_notify(status);
        self.cluster
            .set_self_key_value(INGESTER_STATUS_KEY, status.as_json_str_name())
            .await;
    }

    /// Checks whether the ingester is fully decommissioned and updates its status accordingly.
    pub async fn check_decommissioning_status(&mut self) {
        if self.status() != IngesterStatus::Decommissioning {
            return;
        }
        // An ingester is decommissioned if:
        // - `self.shards` is empty OR
        // - all shards are non-advertisable and empty
        //
        // see `IngesterShard::is_empty_orphan` for why the latter are never going to be deleted
        // by any other cleanup mechanism, so we must not wait on them here.
        let is_decommissioned = self.shards.values().all(|shard| shard.is_empty_orphan());

        if is_decommissioned {
            self.set_status(IngesterStatus::Decommissioned).await;
        } else {
            self.set_status_and_notify(IngesterStatus::Decommissioning);
        }
    }

    /// Returns the shard with the most available permits for this index and source.
    pub fn find_most_capacity_shard_mut(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
    ) -> Option<&mut IngesterShard> {
        self.shards
            .values_mut()
            .filter(|shard| {
                shard.is_open() && shard.index_uid == *index_uid && shard.source_id == *source_id
            })
            .map(|shard| (shard.rate_limiter.available_permits(), shard))
            .max_by_key(|(available_permits, _)| *available_permits)
            .map(|(_, shard)| shard)
    }

    /// Returns per-source open shard counts and closed shard IDs for all advertisable shards.
    pub fn get_shard_snapshot(&self) -> (OpenShardCounts, Vec<ShardIds>) {
        let grouped = self
            .shards
            .values()
            .filter(|shard| shard.is_advertisable)
            .map(|shard| ((shard.index_uid.clone(), shard.source_id.clone()), shard))
            .into_group_map();

        let mut open_counts = Vec::new();
        let mut closed_shards = Vec::new();

        for ((index_uid, source_id), shards) in grouped {
            let mut open_count = 0;
            let mut closed_ids = Vec::new();

            for shard in shards {
                if shard.is_open() {
                    open_count += 1;
                } else if shard.is_closed() {
                    closed_ids.push(shard.shard_id.clone());
                }
            }
            open_counts.push((index_uid.clone(), source_id.clone(), open_count));
            if !closed_ids.is_empty() {
                closed_shards.push(ShardIds {
                    index_uid: Some(index_uid),
                    source_id,
                    shard_ids: closed_ids,
                });
            }
        }
        (open_counts, closed_shards)
    }
}

impl IngesterState {
    async fn create(cluster: Cluster, disk_capacity: ByteSize, memory_capacity: ByteSize) -> Self {
        let status = IngesterStatus::Initializing;
        let (status_tx, status_rx) = watch::channel(status);
        let mem_queue_usage = Arc::new(MemQueueUsage::default());
        let mut inner = InnerIngesterState {
            shards: Default::default(),
            doc_mappers: Default::default(),
            cluster,
            wal_capacity_tracker: WalCapacityTracker::new(disk_capacity, memory_capacity),
            object_wal: None,
            mem_queue_usage: mem_queue_usage.clone(),
            disk_capacity,
            memory_capacity,
            status_tx,
        };
        // We call `set_status` here instead of setting it directly because it also updates the
        // ingester status in chitchat.
        inner.set_status(IngesterStatus::Initializing).await;

        let inner = Arc::new(Mutex::new(inner));
        let mrecordlog = Arc::new(RwLock::new(None));

        Self {
            inner,
            mrecordlog,
            mem_queue_usage,
            status_rx,
        }
    }

    pub async fn load(
        cluster: Cluster,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        object_wal_opt: Option<OpenedObjectWal>,
    ) -> Self {
        let state = Self::create(cluster, disk_capacity, memory_capacity).await;
        let state_clone = state.clone();
        let wal_dir_path = wal_dir_path.to_path_buf();

        let init_future = async move {
            state_clone
                .init(
                    &wal_dir_path,
                    disk_capacity,
                    memory_capacity,
                    rate_limiter_settings,
                    object_wal_opt,
                )
                .await;
        };
        tokio::spawn(init_future);

        state
    }

    #[cfg(test)]
    pub async fn for_test(cluster: Cluster) -> (tempfile::TempDir, Self) {
        Self::for_test_with_disk_capacity(cluster, ByteSize::mb(256)).await
    }

    #[cfg(test)]
    pub async fn for_test_with_disk_capacity(
        cluster: Cluster,
        disk_capacity: ByteSize,
    ) -> (tempfile::TempDir, Self) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut state = IngesterState::load(
            cluster,
            temp_dir.path(),
            disk_capacity,
            ByteSize::mb(256),
            RateLimiterSettings::default(),
            None,
        )
        .await;

        state.wait_for_ready().await;

        (temp_dir, state)
    }

    /// Initializes the internal state of the ingester. It loads the local WAL, then lists all its
    /// queues. Every queue is recovered as a closed shard, including empty ones.
    ///
    /// With ingest v3, records left in the object-store WAL by a previous incarnation of this
    /// node are first re-inserted into the local WAL, so that the shards they belong to are
    /// recovered like any other.
    pub async fn init(
        &self,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        object_wal_opt: Option<OpenedObjectWal>,
    ) {
        // Acquire locks in the same order as `lock_fully` (mrecordlog first, then inner) to
        // prevent ABBA deadlocks with the broadcast capacity task.
        let mut mrecordlog_guard = self.mrecordlog.write().await;
        let mut inner_guard = self.inner.lock().await;

        let now = Instant::now();

        let mut local_queues: Vec<QueueId> = Vec::new();
        let persist_policy = if object_wal_opt.is_some() {
            // Ingest v3: the object WAL is the only source of durability and of recovery. The
            // local WAL is a serving buffer for fetch streams. Whatever it holds from a previous
            // run is either already in the object WAL (replayed below) or was never
            // acknowledged nor served to an indexer (fetch is bounded by the durable position),
            // so it is discarded. Not persisting it also removes the fsync/flush churn.
            //
            // The *names* of the local queues are kept: a queue with no marker in the object
            // WAL (a node switched from ingest v2 without being drained first) is recovered
            // empty and closed rather than forgotten while the control plane still routes to it.
            // Its v2-era records are lost: drain a node before switching it to ingest v3.
            info!(
                "ingest v3: discarding local WAL located at `{}`",
                wal_dir_path.display()
            );
            match MultiRecordLogAsync::open_with_prefs(
                wal_dir_path,
                mrecordlog::PersistPolicy::DoNothing,
            )
            .await
            {
                Ok(local_wal) => {
                    local_queues = local_wal.list_queues().map(str::to_string).collect();
                    if !local_queues.is_empty() {
                        warn!(
                            "ingest v3: {} queue(s) found in the local WAL, recovering them \
                             empty; their records, if any, are discarded",
                            local_queues.len()
                        );
                    }
                }
                Err(error) => {
                    warn!("failed to open the local WAL before discarding it: {error}");
                }
            }
            if let Err(error) = clear_dir(wal_dir_path).await {
                error!("failed to clear local WAL directory: {error}");
                inner_guard.set_status(IngesterStatus::Failed).await;
                return;
            }
            mrecordlog::PersistPolicy::DoNothing
        } else {
            mrecordlog::PersistPolicy::OnDelay {
                interval: Duration::from_secs(5),
                // TODO maybe we want to fsync too?
                action: mrecordlog::PersistAction::Flush,
            }
        };
        info!("opening WAL located at `{}`", wal_dir_path.display());
        let open_result = MultiRecordLogAsync::open_with_prefs(wal_dir_path, persist_policy).await;

        let mrecordlog = match open_result {
            Ok(mrecordlog) => {
                info!(
                    "opened WAL successfully in {}",
                    now.elapsed().pretty_display()
                );
                mrecordlog
            }
            Err(error) => {
                error!("failed to open WAL: {error}");
                inner_guard.set_status(IngesterStatus::Failed).await;
                return;
            }
        };
        // Ingest v3: shards recovered from the object WAL, backed by in-memory queues.
        let mut recovered_mem_queues: Vec<(QueueId, Arc<MemQueue>)> = Vec::new();
        if let Some(mut opened_object_wal) = object_wal_opt {
            opened_object_wal.queues.extend(local_queues);
            recovered_mem_queues = replay_object_wal_into_mem_queues(
                &inner_guard.mem_queue_usage,
                opened_object_wal.queues,
                opened_object_wal.replay,
            );
            inner_guard.object_wal = Some(opened_object_wal.object_wal);
        }
        let queues_summary = mrecordlog.summary();

        if !queues_summary.queues.is_empty() {
            info!("recovering {} shard(s)", queues_summary.queues.len());
        }
        let now = Instant::now();
        let mut num_closed_shards = 0;

        for (queue_id, queue_summary) in queues_summary.queues {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(&queue_id) else {
                // `split_queue_id` already logs an error.
                continue;
            };
            // We recover every shard found in the WAL as a closed shard, including empty ones.
            //
            // We used to delete empty shards here, but that silently diverged from the control
            // plane, which kept advertising the shard as available even though it no longer
            // existed on the ingester (resulting in "no shards available" errors). Instead, we
            // recover an empty shard as a closed shard. An indexer will drain it, immediately
            // reach EOF (there is nothing to read), and the resulting EOF gossip will delete the
            // shard from the ingester, the control plane, and the metastore.
            let replication_position_inclusive = queue_summary
                .end
                .map(Position::offset)
                .unwrap_or(Position::Beginning); // The queue was created but never written to.
            let truncation_position_inclusive = queue_summary
                .start
                .checked_sub(1)
                .map(Position::offset)
                .unwrap_or(Position::Beginning);
            let rate_limiter = RateLimiter::from_settings(rate_limiter_settings);
            let rate_meter = RateMeter::default();

            let shard =
                IngesterShard::builder(index_uid.clone(), source_id.clone(), shard_id.clone())
                    .with_state(ShardState::Closed)
                    .with_replication_position_inclusive(replication_position_inclusive)
                    .with_truncation_position_inclusive(truncation_position_inclusive)
                    .with_rate_limiter(rate_limiter)
                    .with_rate_meter(rate_meter)
                    .with_last_write(now)
                    .advertisable() // We want to advertise the shard as read-only right away.
                    .build();
            inner_guard.shards.insert(queue_id.clone(), shard);

            num_closed_shards += 1;
        }
        for (queue_id, mem_queue) in recovered_mem_queues {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(&queue_id) else {
                continue;
            };
            let replication_position_inclusive = mem_queue
                .last_position()
                .map(Position::offset)
                .unwrap_or(Position::Beginning);
            let truncation_position_inclusive = mem_queue
                .first_position()
                .checked_sub(1)
                .map(Position::offset)
                .unwrap_or(Position::Beginning);
            let shard = IngesterShard::builder(index_uid, source_id, shard_id)
                .with_state(ShardState::Closed)
                .with_replication_position_inclusive(replication_position_inclusive)
                .with_truncation_position_inclusive(truncation_position_inclusive)
                .with_rate_limiter(RateLimiter::from_settings(rate_limiter_settings))
                .with_rate_meter(RateMeter::default())
                .with_last_write(now)
                .with_mem_queue(mem_queue)
                .advertisable()
                .build();
            inner_guard.shards.insert(queue_id, shard);
            num_closed_shards += 1;
        }
        if num_closed_shards > 0 {
            info!("recovered and closed {num_closed_shards} shard(s)");
        }
        let wal_usage = mrecordlog.resource_usage();
        mrecordlog_guard.replace(mrecordlog);
        crate::ingest_v2::metrics::report_wal_usage(wal_usage, disk_capacity, memory_capacity);
        inner_guard.set_status(IngesterStatus::Ready).await;
    }

    pub async fn wait_for_ready(&mut self) {
        self.status_rx
            .wait_for(|status| *status == IngesterStatus::Ready)
            .await
            .expect("channel should be open");
    }

    #[instrument(name = "ingester.lock_partially", skip_all, fields(operation))]
    pub async fn lock_partially(
        &self,
        operation: &'static str,
    ) -> IngestV2Result<PartiallyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        let (inner_guard, acquired_at) =
            track_acquire_lock(operation, "partial", self.inner.lock()).await;

        if inner_guard.status() == IngesterStatus::Failed {
            return Err(IngestV2Error::Internal(
                "failed to initialize ingester".to_string(),
            ));
        }
        let partially_locked_state = PartiallyLockedIngesterState {
            inner: inner_guard,
            operation,
            acquired_at,
        };
        Ok(partially_locked_state)
    }

    #[instrument(name = "ingester.lock_fully", skip_all, fields(operation))]
    pub async fn lock_fully(
        &self,
        operation: &'static str,
    ) -> IngestV2Result<FullyLockedIngesterState<'_>> {
        if *self.status_rx.borrow() == IngesterStatus::Initializing {
            return Err(IngestV2Error::Internal(
                "ingester is initializing".to_string(),
            ));
        }
        // We assume that the mrecordlog lock is the most "expensive" one to acquire, so we
        // acquire it first.
        let ((mrecordlog_opt_guard, inner_guard), acquired_at) =
            track_acquire_lock(operation, "full", async {
                let mrecordlog_opt_guard = self.mrecordlog.write().await;
                let inner_guard = self.inner.lock().await;
                (mrecordlog_opt_guard, inner_guard)
            })
            .await;

        if inner_guard.status() == IngesterStatus::Failed {
            return Err(IngestV2Error::Internal(
                "failed to initialize ingester".to_string(),
            ));
        }
        let mrecordlog_guard = RwLockWriteGuard::map(mrecordlog_opt_guard, |mrecordlog_opt| {
            mrecordlog_opt
                .as_mut()
                .expect("mrecordlog should be initialized")
        });
        let fully_locked_state = FullyLockedIngesterState {
            inner: inner_guard,
            mrecordlog: mrecordlog_guard,
            operation,
            acquired_at,
        };
        Ok(fully_locked_state)
    }

    // Leaks the mrecordlog lock for use in fetch tasks. It's safe to do so because fetch tasks
    // never attempt to lock the inner state.
    pub fn mrecordlog(&self) -> Arc<RwLock<Option<MultiRecordLogAsync>>> {
        self.mrecordlog.clone()
    }

    /// Ingest v3: the node-wide in-memory queue accounting. Cheap, no lock.
    pub fn mem_queue_usage(&self) -> Arc<MemQueueUsage> {
        self.mem_queue_usage.clone()
    }

    pub fn weak(&self) -> WeakIngesterState {
        WeakIngesterState {
            inner: Arc::downgrade(&self.inner),
            mrecordlog: Arc::downgrade(&self.mrecordlog),
            mem_queue_usage: self.mem_queue_usage.clone(),
            status_rx: self.status_rx.clone(),
        }
    }
}

pub(super) struct PartiallyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
    operation: &'static str,
    acquired_at: Instant,
}

impl fmt::Debug for PartiallyLockedIngesterState<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PartiallyLockedIngesterState").finish()
    }
}

impl Deref for PartiallyLockedIngesterState<'_> {
    type Target = InnerIngesterState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PartiallyLockedIngesterState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PartiallyLockedIngesterState<'_> {
    fn drop(&mut self) {
        warn_on_long_lock_hold(self.operation, "partial", self.acquired_at);
    }
}

pub(super) struct FullyLockedIngesterState<'a> {
    pub inner: MutexGuard<'a, InnerIngesterState>,
    pub mrecordlog: RwLockMappedWriteGuard<'a, MultiRecordLogAsync>,
    operation: &'static str,
    acquired_at: Instant,
}

impl fmt::Debug for FullyLockedIngesterState<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FullyLockedIngesterState").finish()
    }
}

impl Deref for FullyLockedIngesterState<'_> {
    type Target = InnerIngesterState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FullyLockedIngesterState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for FullyLockedIngesterState<'_> {
    fn drop(&mut self) {
        warn_on_long_lock_hold(self.operation, "full", self.acquired_at);
    }
}

pub(super) fn warn_on_long_lock_hold(
    operation: &'static str,
    lock_type: &'static str,
    acquired_at: Instant,
) {
    let elapsed = acquired_at.elapsed();

    let labels = labels!("operation" => operation, "type" => lock_type);
    histogram!(
        parent: crate::ingest_v2::metrics::WAL_LOCK_HOLD_DURATION_SECS,
        labels: [labels],
    )
    .observe(elapsed.as_secs_f64());

    if elapsed > Duration::from_secs(1) {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 6,
            "held {} lock for {} operation for {}",
            lock_type,
            operation,
            elapsed.pretty_display()
        );
    }
}

/// Wraps a lock-acquisition future with the in-flight gauge, the acquire duration histogram, and
/// a rate-limited warning when acquisition takes longer than 1s. Used by `lock_partially` /
/// `lock_fully` and by other ingest_v2 sites that acquire WAL-related locks (e.g. fetch tasks
/// reading the mrecordlog directly).
pub(super) async fn track_acquire_lock<F, R>(
    operation: &'static str,
    lock_type: &'static str,
    acquire_future: F,
) -> (R, Instant)
where
    F: std::future::Future<Output = R>,
{
    let labels = labels!("operation" => operation, "type" => lock_type);

    gauge!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT,
        labels: [labels],
    )
    .inc();

    let now = Instant::now();
    let guard = acquire_future.await;
    let acquired_at = Instant::now();

    let elapsed = acquired_at.duration_since(now);

    if elapsed > Duration::from_secs(1) {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 6,
            "acquiring {} lock for {} operation took {}",
            lock_type,
            operation,
            elapsed.pretty_display()
        );
    }
    gauge!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT,
        labels: [labels],
    )
    .dec();
    histogram!(
        parent: crate::ingest_v2::metrics::WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS,
        labels: [labels],
    )
    .observe(elapsed.as_secs_f64());

    (guard, acquired_at)
}

impl FullyLockedIngesterState<'_> {
    /// Reports the current WAL disk/memory usage and usage-ratio metrics against the configured
    /// capacity limits. Called after any operation that grows or shrinks WAL usage so that
    /// dashboards and alerts relying on these metrics stay accurate even when the ingester is
    /// otherwise idle (e.g. after shards are cleaned up via gossip or a control-plane RPC).
    fn report_wal_usage(&self) {
        let wal_usage = self.mrecordlog.resource_usage();
        crate::ingest_v2::metrics::report_wal_usage(
            wal_usage,
            self.disk_capacity,
            self.memory_capacity,
        );
    }

    /// Deletes the shard identified by `queue_id` from the ingester state. It removes the
    /// mrecordlog queue first and then removes the associated in-memory shard and rate trackers.
    #[instrument(name = "ingester.delete_shard", skip_all, fields(queue_id, initiator))]
    pub async fn delete_shard(&mut self, queue_id: &QueueId, initiator: &'static str) {
        if let Some(shard) = self.shards.get(queue_id)
            && let Some(mem_queue) = &shard.mem_queue
        {
            // Ingest v3: the records live in memory; dropping the shard releases them.
            mem_queue.clear();
        }
        match self.mrecordlog.delete_queue(queue_id).await {
            Ok(_) | Err(DeleteQueueError::MissingQueue(_)) => {
                if let Some(object_wal) = &self.inner.object_wal {
                    object_wal.on_delete_queue(queue_id);
                }
                // Log only if the shard was actually removed.
                if let Some(shard) = self.shards.remove(queue_id) {
                    info!("deleted shard `{queue_id}` initiated via `{initiator}`");

                    if let Some(doc_mapper) = shard.doc_mapper_opt {
                        // At this point, we hold the lock so we can safely check the strong count.
                        // The other locations where the doc mapper is cloned also require holding
                        // the lock.
                        if Arc::strong_count(&doc_mapper) == 1 {
                            let doc_mapping_uid = doc_mapper.doc_mapping_uid();

                            if self.doc_mappers.remove(&doc_mapping_uid).is_some() {
                                info!("evicted doc mapper `{doc_mapping_uid}` from cache`");
                            }
                        }
                    }
                    self.report_wal_usage();
                }
            }
            Err(DeleteQueueError::IoError(io_error)) => {
                error!("failed to delete shard `{queue_id}`: {io_error}");
            }
        };
    }

    /// Truncates the shard identified by `queue_id` up to `truncate_up_to_position_inclusive` only
    /// if the current truncation position of the shard is smaller.
    #[instrument(
        name = "ingester.truncate_shard",
        skip_all,
        fields(queue_id, truncate_up_to_position_inclusive, initiator)
    )]
    pub async fn truncate_shard(
        &mut self,
        queue_id: &QueueId,
        truncate_up_to_position_inclusive: Position,
        initiator: &'static str,
    ) {
        let Some(shard) = self.inner.shards.get_mut(queue_id) else {
            return;
        };
        if shard.truncation_position_inclusive >= truncate_up_to_position_inclusive {
            return;
        }
        if let (Some(mem_queue), Some(truncate_up_to_offset_inclusive)) =
            (&shard.mem_queue, truncate_up_to_position_inclusive.as_u64())
        {
            // Ingest v3: in-memory queue.
            mem_queue.truncate(truncate_up_to_offset_inclusive);
        } else if let Some(truncate_up_to_offset_inclusive) =
            truncate_up_to_position_inclusive.as_u64()
        {
            match self
                .mrecordlog
                .truncate(queue_id, truncate_up_to_offset_inclusive)
                .await
            {
                Ok(_) => {}
                Err(TruncateError::MissingQueue(_)) => {
                    error!("failed to truncate shard `{queue_id}`: WAL queue not found");
                    self.shards.remove(queue_id);
                    info!("deleted dangling shard `{queue_id}`");
                    return;
                }
                Err(TruncateError::IoError(io_error)) => {
                    error!("failed to truncate shard `{queue_id}`: {io_error}");
                    return;
                }
            }
        }
        info!(
            "truncated shard `{queue_id}` at {truncate_up_to_position_inclusive} initiated via \
             `{initiator}`"
        );
        shard.truncation_position_inclusive = truncate_up_to_position_inclusive.clone();
        if let (Some(object_wal), Some(position)) = (
            &self.inner.object_wal,
            truncate_up_to_position_inclusive.as_u64(),
        ) {
            object_wal.on_truncate(queue_id, position);
        }
        self.report_wal_usage();
    }

    /// Deletes and truncates the shards as directed by the `advise_reset_shards_response` returned
    /// by the control plane.
    pub async fn reset_shards(&mut self, advise_reset_shards_response: &AdviseResetShardsResponse) {
        info!("resetting shards");
        for shard_ids in &advise_reset_shards_response.shards_to_delete {
            for queue_id in shard_ids.queue_ids() {
                self.delete_shard(&queue_id, "control-plane-reset-shards-rpc")
                    .await;
            }
        }
        for shard_id_positions in &advise_reset_shards_response.shards_to_truncate {
            for (queue_id, publish_position) in shard_id_positions.queue_id_positions() {
                self.truncate_shard(
                    &queue_id,
                    publish_position,
                    "control-plane-reset-shards-rpc",
                )
                .await;
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct WeakIngesterState {
    inner: Weak<Mutex<InnerIngesterState>>,
    mrecordlog: Weak<RwLock<Option<MultiRecordLogAsync>>>,
    mem_queue_usage: Arc<MemQueueUsage>,
    status_rx: watch::Receiver<IngesterStatus>,
}

impl WeakIngesterState {
    pub fn upgrade(&self) -> Option<IngesterState> {
        let inner = self.inner.upgrade()?;
        let mrecordlog = self.mrecordlog.upgrade()?;
        let status_rx = self.status_rx.clone();
        let state = IngesterState {
            inner,
            mrecordlog,
            mem_queue_usage: self.mem_queue_usage.clone(),
            status_rx,
        };
        Some(state)
    }
}

/// Removes the contents of `dir` (creating it if needed). The directory itself is kept: it may
/// be a symlink or a mount point.
async fn clear_dir(dir: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dir).await?;
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    Ok(())
}

/// Builds the in-memory queues of the shards recovered from the object-store WAL: one per queue
/// name, holding the replayed records at their original positions.
fn replay_object_wal_into_mem_queues(
    usage: &Arc<MemQueueUsage>,
    queues: Vec<QueueId>,
    records: Vec<crate::ingest_v3::wal::reader::WalRecord>,
) -> Vec<(QueueId, Arc<MemQueue>)> {
    let mut mem_queues: HashMap<QueueId, Arc<MemQueue>> = HashMap::new();
    // Queues first: a queue may exist without any record to replay.
    for queue_id in queues {
        mem_queues
            .entry(queue_id)
            .or_insert_with(|| Arc::new(MemQueue::new(usage.clone(), 0)));
    }
    let mut num_replayed = 0usize;
    // `records` is sorted by (queue_id, position).
    for record in records {
        let mem_queue = mem_queues
            .entry(record.queue_id)
            .or_insert_with(|| Arc::new(MemQueue::new(usage.clone(), 0)));
        if mem_queue.append_at(record.position, record.record) {
            num_replayed += 1;
        }
    }
    if num_replayed > 0 {
        info!(
            num_replayed,
            num_bytes = usage.num_bytes(),
            "replayed records from the object WAL into memory"
        );
    }
    let mut recovered: Vec<(QueueId, Arc<MemQueue>)> = mem_queues.into_iter().collect();
    recovered.sort_by(|left, right| left.0.cmp(&right.0));
    recovered
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::types::{ShardId, SourceId, queue_id};
    use tokio::time::timeout;

    use super::*;

    async fn test_cluster() -> Cluster {
        create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_ingester_state_does_not_lock_while_initializing() {
        let cluster = test_cluster().await;
        let state = IngesterState::create(cluster, ByteSize::mb(256), ByteSize::mb(256)).await;
        let inner_guard = state.inner.lock().await;

        assert_eq!(inner_guard.status(), IngesterStatus::Initializing);
        assert_eq!(*state.status_rx.borrow(), IngesterStatus::Initializing);

        let error = state.lock_partially("test").await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));

        let error = state.lock_fully("test").await.unwrap_err().to_string();
        assert!(error.contains("ingester is initializing"));
    }

    #[tokio::test]
    async fn test_ingester_state_failed() {
        let cluster = test_cluster().await;
        let state = IngesterState::create(cluster, ByteSize::mb(256), ByteSize::mb(256)).await;

        state
            .inner
            .lock()
            .await
            .set_status(IngesterStatus::Failed)
            .await;

        let error = state.lock_partially("test").await.unwrap_err().to_string();
        assert!(error.to_string().ends_with("failed to initialize ingester"));

        let error = state.lock_fully("test").await.unwrap_err().to_string();
        assert!(error.contains("failed to initialize ingester"));
    }

    #[tokio::test]
    async fn test_ingester_state_init_v3_discards_local_wal_and_replays_object_wal() {
        use std::sync::Arc;

        use quickwit_common::uri::Uri;
        use quickwit_storage::RamStorage;

        use crate::ingest_v3::{IngestV3Config, ObjectWal};

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let queue_local = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let queue_remote = queue_id(&index_uid, &source_id, &ShardId::from(2));

        // Leftovers on the local disk from a previous run: must be ignored.
        let temp_dir = tempfile::tempdir().unwrap();
        {
            let mut mrecordlog = MultiRecordLogAsync::open(temp_dir.path()).await.unwrap();
            mrecordlog.create_queue(&queue_local).await.unwrap();
            mrecordlog
                .append_records(&queue_local, None, [&b"stale"[..]].into_iter())
                .await
                .unwrap();
        }

        // The object WAL holds what a previous incarnation acknowledged.
        let storage: Arc<dyn quickwit_storage::Storage> = Arc::new(RamStorage::default());
        let mut config = IngestV3Config::with_wal_uri(Uri::for_test("ram:///wal"));
        config.flush_interval = Duration::from_secs(3600);
        let previous = ObjectWal::open(storage.clone(), "node", 0, &config)
            .await
            .unwrap()
            .object_wal;
        previous
            .append(
                &queue_remote,
                0,
                vec![crate::MRecord::new_doc("durable").encode_to_bytes()],
            )
            .unwrap();
        previous.flush().await.unwrap();

        let opened = ObjectWal::open(storage, "node", 0, &config).await.unwrap();
        assert_eq!(opened.replay.len(), 1);

        let cluster = test_cluster().await;
        let mut state = IngesterState::create(cluster, ByteSize::mb(256), ByteSize::mb(256)).await;
        state
            .init(
                temp_dir.path(),
                ByteSize::mb(256),
                ByteSize::mb(256),
                RateLimiterSettings::default(),
                Some(opened),
            )
            .await;
        timeout(Duration::from_millis(100), state.wait_for_ready())
            .await
            .unwrap();

        let state_guard = state.lock_fully("test").await.unwrap();
        // The local queue is recovered by name, empty and closed: its records are discarded.
        let local_shard = state_guard
            .shards
            .get(&queue_local)
            .expect("local queue recovered empty");
        assert!(local_shard.is_closed());
        assert_eq!(
            local_shard.replication_position_inclusive,
            Position::Beginning
        );
        assert!(local_shard.mem_queue.as_ref().unwrap().is_empty());
        let shard = state_guard.shards.get(&queue_remote).unwrap();
        assert!(shard.is_closed());
        assert_eq!(shard.replication_position_inclusive, Position::offset(0u64));
        let records = shard.mem_queue.as_ref().unwrap().range(.., usize::MAX);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 0);
        assert_eq!(&records[0].1[2..], b"durable");
        assert!(!state_guard.mrecordlog.queue_exists(&queue_remote));
        assert_eq!(state_guard.mem_queue_usage.num_records(), 1);
        assert!(state_guard.object_wal.is_some());
    }

    #[tokio::test]
    async fn test_ingester_state_init() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        // Queue with live records, partially truncated.
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        // Queue written to and then fully truncated: empty, but it remembers its position.
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));
        // Queue created but never written to.
        let queue_id_03 = queue_id(&index_uid, &source_id, &ShardId::from(3));

        let temp_dir = tempfile::tempdir().unwrap();

        // Populate a WAL then close it, so `init` reopens it from disk.
        {
            let mut mrecordlog = MultiRecordLogAsync::open(temp_dir.path()).await.unwrap();

            mrecordlog.create_queue(&queue_id_01).await.unwrap();
            mrecordlog
                .append_records(
                    &queue_id_01,
                    None,
                    [
                        &b"test-doc-foo"[..],
                        &b"test-doc-bar"[..],
                        &b"test-doc-qux"[..],
                    ]
                    .into_iter(),
                )
                .await
                .unwrap();
            // Records 0..=2 remain; truncate record 0 so `start` advances to 1.
            mrecordlog.truncate(&queue_id_01, 0).await.unwrap();

            mrecordlog.create_queue(&queue_id_02).await.unwrap();
            mrecordlog
                .append_records(
                    &queue_id_02,
                    None,
                    [&b"test-doc-foo"[..], &b"test-doc-bar"[..]].into_iter(),
                )
                .await
                .unwrap();
            // Truncate everything: the queue is now empty but remembers position 1.
            mrecordlog.truncate(&queue_id_02, 1).await.unwrap();

            mrecordlog.create_queue(&queue_id_03).await.unwrap();
        }
        let cluster = test_cluster().await;
        let mut state = IngesterState::create(cluster, ByteSize::mb(256), ByteSize::mb(256)).await;
        state
            .init(
                temp_dir.path(),
                ByteSize::mb(256),
                ByteSize::mb(256),
                RateLimiterSettings::default(),
                None,
            )
            .await;
        timeout(Duration::from_millis(100), state.wait_for_ready())
            .await
            .unwrap();

        let state_guard = state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.status(), IngesterStatus::Ready);
        assert_eq!(*state_guard.status_tx.borrow(), IngesterStatus::Ready);

        // Non-empty queue: recovers at its last position, truncated up to the first kept record.
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        assert_eq!(shard_01.shard_state, ShardState::Closed);
        assert_eq!(
            shard_01.replication_position_inclusive,
            Position::offset(2u64)
        );
        assert_eq!(
            shard_01.truncation_position_inclusive,
            Position::offset(0u64)
        );

        // Fully truncated queue: recovers at its last position rather than the beginning.
        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        assert_eq!(shard_02.shard_state, ShardState::Closed);
        assert_eq!(
            shard_02.replication_position_inclusive,
            Position::offset(1u64)
        );
        assert_eq!(
            shard_02.truncation_position_inclusive,
            Position::offset(1u64)
        );

        // Never-written queue: recovers at the beginning.
        let shard_03 = state_guard.shards.get(&queue_id_03).unwrap();
        assert_eq!(shard_03.shard_state, ShardState::Closed);
        assert_eq!(shard_03.replication_position_inclusive, Position::Beginning);
        assert_eq!(shard_03.truncation_position_inclusive, Position::Beginning);
    }

    fn insert_shard_with_used_capacity(
        state: &mut InnerIngesterState,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        shard_state: ShardState,
        used_capacity: ByteSize,
    ) {
        let mut shard = IngesterShard::builder(index_uid, source_id, shard_id)
            .with_state(shard_state)
            .build();
        shard.rate_limiter.acquire_bytes(used_capacity);

        let queue_id = shard.queue_id();
        state.shards.insert(queue_id, shard);
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_returns_shard_with_least_used_capacity() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut state_guard = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        // Shard 1: 1KB used (most available capacity)
        // Shard 2: 2KB used
        // ...
        // Shard 5: 5KB used (least available capacity)
        for i in 1..=5u64 {
            insert_shard_with_used_capacity(
                &mut state_guard,
                index_uid.clone(),
                source_id.clone(),
                ShardId::from(i),
                ShardState::Open,
                ByteSize::kb(i),
            );
        }

        let shard = state_guard
            .find_most_capacity_shard_mut(&index_uid, &source_id)
            .unwrap();

        assert_eq!(shard.shard_id, ShardId::from(1));
        assert_eq!(shard.shard_state, ShardState::Open);

        let expected_available_permits =
            RateLimiterSettings::default().burst_limit - ByteSize::kb(1).as_u64();
        assert_eq!(
            shard.rate_limiter.available_permits(),
            expected_available_permits
        );
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_skips_closed_shards() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut locked_state = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(1),
            ShardState::Open,
            ByteSize::kb(1),
        );
        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(2),
            ShardState::Open,
            ByteSize::kb(2),
        );

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(3),
            ShardState::Closed,
            ByteSize::kb(0),
        );

        let shard = locked_state
            .find_most_capacity_shard_mut(&index_uid, &source_id)
            .unwrap();

        // Should pick shard 1 (most capacity among open shards), not shard 3 (closed)
        assert_eq!(shard.shard_id, ShardId::from(1));
    }

    #[tokio::test]
    async fn test_find_most_capacity_shard_returns_none_for_unknown_index_or_source() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut locked_state = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        insert_shard_with_used_capacity(
            &mut locked_state,
            index_uid.clone(),
            source_id.clone(),
            ShardId::from(1),
            ShardState::Open,
            ByteSize::kb(0),
        );

        let shard_opt = locked_state
            .find_most_capacity_shard_mut(&IndexUid::for_test("other-index", 0), &source_id);
        assert!(shard_opt.is_none());

        let shard_opt =
            locked_state.find_most_capacity_shard_mut(&index_uid, &SourceId::from("other-source"));
        assert!(shard_opt.is_none());
    }

    #[tokio::test]
    async fn test_ingester_state_set_status() {
        let cluster = test_cluster().await;
        let state =
            IngesterState::create(cluster.clone(), ByteSize::mb(256), ByteSize::mb(256)).await;
        let temp_dir = tempfile::tempdir().unwrap();

        state
            .init(
                temp_dir.path(),
                ByteSize::mb(256),
                ByteSize::mb(256),
                RateLimiterSettings::default(),
                None,
            )
            .await;

        let mut state_guard = state.lock_fully("test").await.unwrap();
        state_guard.set_status(IngesterStatus::Failed).await;
        assert_eq!(state_guard.status(), IngesterStatus::Failed);
        assert_eq!(*state.status_rx.borrow(), IngesterStatus::Failed);

        let status_json_str = cluster
            .get_self_key_value(INGESTER_STATUS_KEY)
            .await
            .unwrap();
        let status = IngesterStatus::from_json_str_name(&status_json_str).unwrap();
        assert_eq!(status, IngesterStatus::Failed);
    }

    fn open_shard(index_uid: IndexUid, source_id: SourceId, shard_id: ShardId) -> IngesterShard {
        IngesterShard::builder(index_uid, source_id, shard_id)
            .advertisable()
            .build()
    }

    #[tokio::test]
    async fn test_get_shard_snapshot() {
        let cluster = test_cluster().await;
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;
        let mut state_guard = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);

        // source-a: 2 open shards + 1 closed shard.
        let shard = open_shard(index_uid.clone(), "source-a".into(), ShardId::from(1));
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = open_shard(index_uid.clone(), "source-a".into(), ShardId::from(2));
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = IngesterShard::builder(index_uid.clone(), "source-a".into(), ShardId::from(3))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);

        // source-b: 2 closed shards, no open shards.
        let shard = IngesterShard::builder(index_uid.clone(), "source-b".into(), ShardId::from(5))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);
        let shard = IngesterShard::builder(index_uid.clone(), "source-b".into(), ShardId::from(6))
            .with_state(ShardState::Closed)
            .advertisable()
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);

        let (mut open_counts, mut closed_shards) = state_guard.get_shard_snapshot();

        // Open counts: source-a has 2, source-b has 0.
        open_counts.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(open_counts.len(), 2);
        assert_eq!(
            open_counts[0],
            (index_uid.clone(), SourceId::from("source-a"), 2)
        );
        assert_eq!(
            open_counts[1],
            (index_uid.clone(), SourceId::from("source-b"), 0)
        );

        // Closed shards: source-a has shard 3, source-b has shards 5 and 6.
        closed_shards.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        assert_eq!(closed_shards.len(), 2);

        assert_eq!(closed_shards[0].source_id, "source-a");
        assert_eq!(closed_shards[0].shard_ids, vec![ShardId::from(3)]);

        assert_eq!(closed_shards[1].source_id, "source-b");
        let mut source_b_ids = closed_shards[1].shard_ids.clone();
        source_b_ids.sort();
        assert_eq!(source_b_ids, vec![ShardId::from(5), ShardId::from(6)]);
    }

    #[tokio::test]
    async fn test_truncate_shard() {
        let cluster = test_cluster().await;
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        // Shard 1 is empty (never written): its EOF is `Eof(None)`, with no WAL offset.
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        // Shard 2 holds two records: its EOF is `Eof(Some(1))`.
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        let mut state_guard = state.lock_fully("test").await.unwrap();

        state_guard
            .mrecordlog
            .create_queue(&queue_id_01)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .create_queue(&queue_id_02)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .append_records(
                &queue_id_02,
                None,
                [&b"test-doc-foo"[..], &b"test-doc-bar"[..]].into_iter(),
            )
            .await
            .unwrap();

        let shard_01 =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(1))
                .with_state(ShardState::Closed)
                .build();
        state_guard.shards.insert(queue_id_01.clone(), shard_01);
        let shard_02 =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(2))
                .with_state(ShardState::Closed)
                .with_replication_position_inclusive(Position::offset(1u64))
                .build();
        state_guard.shards.insert(queue_id_02.clone(), shard_02);

        state_guard
            .truncate_shard(&queue_id_01, Position::Beginning.as_eof(), "test")
            .await;
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        assert_eq!(
            shard_01.truncation_position_inclusive,
            Position::Beginning.as_eof()
        );
        assert!(state_guard.mrecordlog.queue_exists(&queue_id_01));
        assert_eq!(
            state_guard
                .shards
                .get(&queue_id_01)
                .unwrap()
                .truncation_position_inclusive,
            Position::Beginning.as_eof()
        );

        state_guard
            .truncate_shard(&queue_id_02, Position::eof(1u64), "test")
            .await;
        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        assert_eq!(shard_02.truncation_position_inclusive, Position::eof(1u64));
        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[]);

        assert_eq!(
            state_guard
                .shards
                .get(&queue_id_02)
                .unwrap()
                .truncation_position_inclusive,
            Position::eof(1u64)
        );
    }
}
