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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use bytesize::ByteSize;
use futures::StreamExt;
use mrecordlog::Record;
use quickwit_common::metrics::{IN_FLIGHT_FETCH_STREAM, IN_FLIGHT_MULTI_FETCH_STREAM};
use quickwit_common::retry::RetryParams;
use quickwit_common::stream_utils::{InFlightValue, TrackedSender};
use quickwit_common::{ServiceStream, spawn_named_task};
use quickwit_proto::ingest::ingester::{
    FetchEof, FetchMessage, FetchPayload, IngesterService, OpenFetchStreamRequest, fetch_message,
};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, MRecordBatch};
use quickwit_proto::types::{IndexUid, NodeId, Position, QueueId, ShardId, SourceId, queue_id};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::models::ShardStatus;
use super::state::{track_acquire_lock, warn_on_long_lock_hold};
use crate::ingest_v3::mem_queue::MemQueue;
use crate::ingest_v3::metrics as v3_metrics;
use crate::ingest_v3::wal::fence::fence_log;
use crate::ingest_v3::wal::reader::WalReader;
use crate::ingest_v3::wal::{WalError, WalId};
use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{ClientId, IngesterPool};

/// Ingest v3: how a fetch stream drains a shard whose ingester is gone.
///
/// Once the ingester hosting a shard has been absent from the ingester pool for `grace`, the
/// fetch stream fences the ingester's object-store log and serves the shard's remaining records
/// straight from it, then reports EOF. The metastore checkpoint makes this safe even if the
/// ingester comes back and serves the same records again: overlapping positions are rejected at
/// publish time.
#[derive(Clone)]
pub struct ObjectWalFallback {
    /// Storage holding the ingest WAL (`QW_INGEST_WAL_URI`).
    pub storage: Arc<dyn quickwit_storage::Storage>,
    /// How long an ingester must be missing before its log is fenced and drained.
    pub grace: Duration,
}

impl fmt::Debug for ObjectWalFallback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ObjectWalFallback")
            .field("grace", &self.grace)
            .finish()
    }
}

/// Upper bound on the size of a fetch payload built from the object WAL.
const OBJECT_WAL_FETCH_BATCH_NUM_BYTES: usize = 8 * 1024 * 1024;

/// A fetch stream task is responsible for waiting and pushing new records written to a shard's
/// record log into a channel named `fetch_message_tx`.
pub(super) struct FetchStreamTask {
    /// Uniquely identifies the consumer of the fetch task for logging and debugging purposes.
    client_id: ClientId,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    /// The position of the next record fetched.
    from_position_inclusive: u64,
    mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
    /// Ingest v3: the shard's in-memory queue, read instead of the mrecordlog.
    mem_queue_opt: Option<Arc<MemQueue>>,
    fetch_message_tx: TrackedSender<IngestV2Result<FetchMessage>>,
    /// This channel notifies the fetch task when new records are available. This way the fetch
    /// task does not need to grab the lock and poll the mrecordlog queue unnecessarily.
    shard_status_rx: watch::Receiver<ShardStatus>,
    batch_num_bytes: usize,
    /// Ingest v3: only serve records up to the shard's replication position, which the
    /// ingester advances once records are durable in the object-store WAL. Without this bound,
    /// an indexer could read (and publish) records that a crash would erase from the log.
    bound_by_replication_position: bool,
}

impl fmt::Debug for FetchStreamTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FetchStreamTask")
            .field("client_id", &self.client_id)
            .field("index_uid", &self.index_uid)
            .field("source_id", &self.source_id)
            .field("shard_id", &self.shard_id)
            .finish()
    }
}

impl FetchStreamTask {
    #[cfg(test)]
    pub fn spawn(
        open_fetch_stream_request: OpenFetchStreamRequest,
        mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
    ) -> (ServiceStream<IngestV2Result<FetchMessage>>, JoinHandle<()>) {
        Self::spawn_with_options(
            open_fetch_stream_request,
            mrecordlog,
            None,
            shard_status_rx,
            batch_num_bytes,
            false,
        )
    }

    pub fn spawn_with_options(
        open_fetch_stream_request: OpenFetchStreamRequest,
        mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
        mem_queue_opt: Option<Arc<MemQueue>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
        bound_by_replication_position: bool,
    ) -> (ServiceStream<IngestV2Result<FetchMessage>>, JoinHandle<()>) {
        let from_position_inclusive = open_fetch_stream_request
            .from_position_exclusive()
            .as_u64()
            .map(|offset| offset + 1)
            .unwrap_or_default();
        let (fetch_message_tx, fetch_stream) =
            ServiceStream::new_bounded_with_gauge(3, &IN_FLIGHT_FETCH_STREAM);
        let mut fetch_task = Self {
            shard_id: open_fetch_stream_request.shard_id().clone(),
            queue_id: open_fetch_stream_request.queue_id(),
            index_uid: open_fetch_stream_request.index_uid().clone(),
            client_id: open_fetch_stream_request.client_id,
            source_id: open_fetch_stream_request.source_id,
            from_position_inclusive,
            mrecordlog,
            mem_queue_opt,
            fetch_message_tx,
            shard_status_rx,
            batch_num_bytes,
            bound_by_replication_position,
        };
        let future = async move { fetch_task.run().await };
        let fetch_task_handle: JoinHandle<()> = spawn_named_task(future, "fetch_task");
        (fetch_stream, fetch_task_handle)
    }

    /// Runs the fetch task. It waits for new records in the log and pushes them into the fetch
    /// response channel until it reaches the end of the shard marked by an EOF record.
    async fn run(&mut self) {
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            from_position_inclusive=%self.from_position_inclusive,
            "spawning fetch task"
        );
        let mut has_drained_queue = false;
        let mut to_position_inclusive = if self.from_position_inclusive == 0 {
            Position::Beginning
        } else {
            Position::offset(self.from_position_inclusive - 1)
        };

        loop {
            if has_drained_queue && self.shard_status_rx.changed().await.is_err() {
                // The shard was dropped.
                break;
            }
            has_drained_queue = true;

            let mut mrecord_buffer = BytesMut::with_capacity(self.batch_num_bytes);
            let mut mrecord_lengths = Vec::new();

            // Exclusive upper bound of the records we may serve.
            let to_position_exclusive: u64 = if self.bound_by_replication_position {
                self.shard_status_rx
                    .borrow()
                    .1
                    .as_u64()
                    .map(|position| position + 1)
                    .unwrap_or(0)
            } else {
                u64::MAX
            };

            if let Some(mem_queue) = &self.mem_queue_opt {
                // Ingest v3: per-queue mutex, no node-wide lock.
                let range = self.from_position_inclusive
                    ..to_position_exclusive.max(self.from_position_inclusive);
                let records = mem_queue.range(range.clone(), self.batch_num_bytes);
                let num_available = range.end - range.start;
                for (_position, payload) in &records {
                    mrecord_buffer.put_slice(payload);
                    mrecord_lengths.push(payload.len() as u32);
                }
                if (records.len() as u64) < num_available {
                    // More records are available than fit in one batch (or gaps).
                    has_drained_queue = false;
                }
            } else {
                let (mrecordlog_guard, acquired_at) =
                    track_acquire_lock("fetch_stream", "partial", self.mrecordlog.read()).await;

                let Ok(mrecords) = mrecordlog_guard
                    .as_ref()
                    .expect("mrecordlog should be initialized")
                    .range(
                        &self.queue_id,
                        self.from_position_inclusive
                            ..to_position_exclusive.max(self.from_position_inclusive),
                    )
                else {
                    // The queue was dropped.
                    break;
                };
                for Record { payload, .. } in mrecords {
                    // Accept at least one message
                    if !mrecord_buffer.is_empty()
                        && (mrecord_buffer.len() + payload.len() > mrecord_buffer.capacity())
                    {
                        has_drained_queue = false;
                        break;
                    }
                    mrecord_buffer.put(payload.borrow());
                    mrecord_lengths.push(payload.len() as u32);
                }
                // Drop the lock while we send the message.
                drop(mrecordlog_guard);

                warn_on_long_lock_hold("fetch_stream", "partial", acquired_at);
            }

            if !mrecord_lengths.is_empty() {
                let from_position_exclusive = if self.from_position_inclusive == 0 {
                    Position::Beginning
                } else {
                    Position::offset(self.from_position_inclusive - 1)
                };
                self.from_position_inclusive += mrecord_lengths.len() as u64;

                to_position_inclusive = Position::offset(self.from_position_inclusive - 1);

                let mrecord_batch = MRecordBatch {
                    mrecord_buffer: mrecord_buffer.freeze(),
                    mrecord_lengths,
                };
                let batch_size = mrecord_batch.estimate_size();
                let fetch_payload = FetchPayload {
                    index_uid: Some(self.index_uid.clone()),
                    source_id: self.source_id.clone(),
                    shard_id: Some(self.shard_id.clone()),
                    mrecord_batch: Some(mrecord_batch),
                    from_position_exclusive: Some(from_position_exclusive),
                    to_position_inclusive: Some(to_position_inclusive.clone()),
                };
                let fetch_message = FetchMessage::new_payload(fetch_payload);

                if self
                    .fetch_message_tx
                    .send(Ok(fetch_message), batch_size)
                    .await
                    .is_err()
                {
                    // The consumer was dropped.
                    return;
                }
            }
            if has_drained_queue {
                let has_reached_eof = {
                    let shard_status = self.shard_status_rx.borrow();
                    let shard_state = &shard_status.0;
                    let replication_position = &shard_status.1;
                    shard_state.is_closed() && to_position_inclusive >= *replication_position
                };
                if has_reached_eof {
                    debug!(
                        client_id=%self.client_id,
                        index_uid=%self.index_uid,
                        source_id=%self.source_id,
                        shard_id=%self.shard_id,
                        %to_position_inclusive,
                        "fetch stream reached end of shard"
                    );
                    let eof_position = to_position_inclusive.as_eof();

                    let fetch_eof = FetchEof {
                        index_uid: Some(self.index_uid.clone()),
                        source_id: self.source_id.clone(),
                        shard_id: Some(self.shard_id.clone()),
                        eof_position: Some(eof_position),
                    };
                    let fetch_message = FetchMessage::new_eof(fetch_eof);
                    let _ = self
                        .fetch_message_tx
                        .send(Ok(fetch_message), ByteSize(0))
                        .await;
                    return;
                }
            }
        }
        if !to_position_inclusive.is_eof() {
            // This can happen if we delete the associated source or index.
            warn!(
                client_id=%self.client_id,
                index_uid=%self.index_uid,
                source_id=%self.source_id,
                shard_id=%self.shard_id,
                "fetch stream ended before reaching end of shard"
            );
            let _ = self
                .fetch_message_tx
                .send(
                    Err(IngestV2Error::Internal(
                        "fetch stream ended before reaching end of shard".to_string(),
                    )),
                    ByteSize(0),
                )
                .await;
        }
    }
}

#[derive(Debug)]
pub struct FetchStreamError {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub shard_id: ShardId,
    pub ingest_error: IngestV2Error,
}

/// Combines multiple fetch streams originating from different ingesters into a single stream.
pub struct MultiFetchStream {
    client_id: ClientId,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    object_wal_fallback: Option<ObjectWalFallback>,
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_message_rx: mpsc::Receiver<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
}

impl MultiFetchStream {
    pub fn new(
        client_id: ClientId,
        ingester_pool: IngesterPool,
        retry_params: RetryParams,
    ) -> Self {
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        Self {
            client_id,
            ingester_pool,
            retry_params,
            object_wal_fallback: None,
            fetch_task_handles: HashMap::new(),
            fetch_message_rx,
            fetch_message_tx,
        }
    }

    /// Enables draining shards of absent ingesters from the object-store WAL (ingest v3).
    pub fn with_object_wal_fallback(mut self, fallback: ObjectWalFallback) -> Self {
        self.object_wal_fallback = Some(fallback);
        self
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn fetch_message_tx(
        &self,
    ) -> mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>> {
        self.fetch_message_tx.clone()
    }

    /// Subscribes to a shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &mut self,
        ingester_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        from_position_exclusive: Position,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);
        let entry = self.fetch_task_handles.entry(queue_id.clone());

        if let Entry::Occupied(_) = entry {
            return Err(IngestV2Error::Internal(format!(
                "stream has already subscribed to shard `{queue_id}`"
            )));
        }
        let fetch_stream_future = retrying_fetch_stream(
            self.client_id.clone(),
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_id,
            self.ingester_pool.clone(),
            self.retry_params,
            self.object_wal_fallback.clone(),
            self.fetch_message_tx.clone(),
        );
        let fetch_task_handle = spawn_named_task(fetch_stream_future, "fetch_stream");
        self.fetch_task_handles.insert(queue_id, fetch_task_handle);
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        index_uid: &IndexUid,
        source_id: &str,
        shard_id: ShardId,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid, source_id, &shard_id);

        if let Some(fetch_stream_handle) = self.fetch_task_handles.remove(&queue_id) {
            fetch_stream_handle.abort();
        }
        Ok(())
    }

    /// Returns the next fetch response. This method blocks until a response is available.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn next(&mut self) -> Result<FetchMessage, FetchStreamError> {
        // Because we always hold a sender and never call `close()` on the receiver, the channel is
        // always open.
        self.fetch_message_rx
            .recv()
            .await
            .expect("channel should be open")
            .map(|value: InFlightValue<FetchMessage>| value.into_inner())
    }

    /// Resets the stream by aborting all the active fetch tasks and dropping all queued responses.
    ///
    /// The borrow checker guarantees that both `next()` and `reset()` cannot be called
    /// simultaneously because they are both `&mut self` methods.
    pub fn reset(&mut self) {
        for (_queue_id, fetch_stream_handle) in self.fetch_task_handles.drain() {
            fetch_stream_handle.abort();
        }
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        self.fetch_message_tx = fetch_message_tx;
        self.fetch_message_rx = fetch_message_rx;
    }
}

impl Drop for MultiFetchStream {
    fn drop(&mut self) {
        self.reset();
    }
}

/// Performs multiple fault-tolerant fetch stream attempts until the stream reaches
/// the end of the shard.
#[allow(clippy::too_many_arguments)]
async fn retrying_fetch_stream(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    mut from_position_exclusive: Position,
    ingester_id: NodeId,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    object_wal_fallback: Option<ObjectWalFallback>,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    // Ingest v3: since when the ingester has been continuously missing from the pool.
    let mut ingester_absent_since: Option<Instant> = None;

    for num_attempts in 1..=retry_params.max_attempts {
        if let Some(fallback) = &object_wal_fallback {
            if ingester_pool.contains_key(&ingester_id) {
                ingester_absent_since = None;
            } else {
                let absent_since = *ingester_absent_since.get_or_insert_with(Instant::now);
                if absent_since.elapsed() >= fallback.grace {
                    object_wal_fetch_stream(
                        fallback,
                        client_id.clone(),
                        index_uid.clone(),
                        source_id.clone(),
                        shard_id.clone(),
                        &mut from_position_exclusive,
                        &ingester_id,
                        fetch_message_tx.clone(),
                    )
                    .await;
                    if from_position_exclusive.is_eof() {
                        break;
                    }
                    let delay = retry_params.compute_delay(num_attempts);
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }
        }
        fetch_stream_once(
            client_id.clone(),
            index_uid.clone(),
            source_id.clone(),
            shard_id.clone(),
            &mut from_position_exclusive,
            &ingester_id,
            ingester_pool.clone(),
            fetch_message_tx.clone(),
        )
        .await;

        if from_position_exclusive.is_eof() {
            break;
        }
        let delay = retry_params.compute_delay(num_attempts);
        tokio::time::sleep(delay).await;
    }
}

/// Ingest v3: fences the object-store log of `ingester_id` and streams the shard's records
/// from it, from `from_position_exclusive` to the end of the log, then EOF.
///
/// On a storage error, sends an `Unavailable` fetch stream error and leaves
/// `from_position_exclusive` where it got to; the caller retries later and the replay resumes
/// from there (the fence is idempotent: a second call finds the log already closed).
#[allow(clippy::too_many_arguments)]
async fn object_wal_fetch_stream(
    fallback: &ObjectWalFallback,
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    from_position_exclusive: &mut Position,
    ingester_id: &NodeId,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    let queue_id = queue_id(&index_uid, &source_id, &shard_id);
    info!(
        client_id=%client_id,
        queue_id=%queue_id,
        from_position_exclusive=%from_position_exclusive,
        "ingester `{ingester_id}` is absent: draining shard from its object WAL"
    );
    let send_error = |ingest_error: IngestV2Error| {
        let fetch_stream_error = FetchStreamError {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: shard_id.clone(),
            ingest_error,
        };
        let fetch_message_tx = fetch_message_tx.clone();
        async move {
            let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
        }
    };
    match object_wal_replay(
        fallback,
        &index_uid,
        &source_id,
        &shard_id,
        &queue_id,
        from_position_exclusive,
        ingester_id,
        &fetch_message_tx,
    )
    .await
    {
        Ok(()) => {
            v3_metrics::WAL_OBJECT_FALLBACK_DRAINS_SUCCESS.inc();
        }
        Err(error) => {
            v3_metrics::WAL_OBJECT_FALLBACK_DRAINS_ERROR.inc();
            error!(
                client_id=%client_id,
                queue_id=%queue_id,
                %error,
                "failed to drain shard from the object WAL of ingester `{ingester_id}`"
            );
            send_error(IngestV2Error::Unavailable(format!(
                "failed to drain shard from the object WAL of ingester `{ingester_id}`: {error}"
            )))
            .await;
        }
    }
}

/// Does the work of [`object_wal_fetch_stream`]. Returns `Ok(())` once EOF has been sent, or if
/// the consumer went away.
#[allow(clippy::too_many_arguments)]
async fn object_wal_replay(
    fallback: &ObjectWalFallback,
    index_uid: &IndexUid,
    source_id: &SourceId,
    shard_id: &ShardId,
    queue_id: &QueueId,
    from_position_exclusive: &mut Position,
    ingester_id: &NodeId,
    fetch_message_tx: &mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) -> Result<(), WalError> {
    let storage = fallback.storage.clone();
    let reader = WalReader::new(storage.clone(), ingester_id.as_str());

    // Close the log so that the tail is final, then scan everything up to the fence.
    let existing = reader.list().await?;
    let listed_tail = existing
        .last()
        .map(|object| object.wal_id)
        .unwrap_or(WalId::ZERO);
    let fence = match fence_log(storage, ingester_id.as_str(), 0, listed_tail).await {
        Ok(fence) => fence,
        // Someone else (another indexer, or the ingester itself restarting) fenced with a
        // higher epoch in the meantime. The log is closed either way: the objects we need
        // are all at or below the current tail.
        Err(WalError::Fenced) => {
            let tail = reader
                .list()
                .await?
                .last()
                .map(|o| o.wal_id)
                .unwrap_or(listed_tail);
            crate::ingest_v3::wal::fence::Fence {
                wal_id: tail,
                epoch: 0,
            }
        }
        Err(error) => return Err(error),
    };
    let wal_ids: Vec<(WalId, Option<u64>)> = existing
        .iter()
        .map(|object| (object.wal_id, Some(object.num_bytes)))
        .chain((listed_tail.0 + 1..=fence.wal_id.0).map(|wal_id| (WalId(wal_id), None)))
        .collect();

    let mut from_position_inclusive: u64 = from_position_exclusive
        .as_u64()
        .map(|position| position + 1)
        .unwrap_or(0);

    let mut mrecord_buffer = BytesMut::with_capacity(OBJECT_WAL_FETCH_BATCH_NUM_BYTES);
    let mut mrecord_lengths: Vec<u32> = Vec::new();
    let mut batch_to_position_inclusive: u64 = 0;

    for (wal_id, num_bytes) in wal_ids {
        let footer = match reader.read_footer(wal_id, num_bytes).await {
            Ok(footer) => footer,
            Err(WalError::Storage(error))
                if error.kind() == quickwit_storage::StorageErrorKind::NotFound =>
            {
                // Garbage collected: every record it held was below the publish position,
                // hence below `from_position_inclusive`.
                continue;
            }
            Err(error) => return Err(error),
        };
        for block_meta in &footer.blocks {
            if block_meta.queue_id != *queue_id
                || block_meta.is_queue_marker()
                || block_meta.last_position() < from_position_inclusive
            {
                continue;
            }
            let records = reader.read_block(wal_id, block_meta).await?;
            for (i, record) in records.into_iter().enumerate() {
                let position = block_meta.first_position + i as u64;
                if position < from_position_inclusive {
                    continue;
                }
                let batch_is_full = !mrecord_lengths.is_empty()
                    && mrecord_buffer.len() + record.len() > OBJECT_WAL_FETCH_BATCH_NUM_BYTES;
                if batch_is_full
                    && !send_object_wal_payload(
                        index_uid,
                        source_id,
                        shard_id,
                        from_position_exclusive,
                        batch_to_position_inclusive,
                        std::mem::replace(
                            &mut mrecord_buffer,
                            BytesMut::with_capacity(OBJECT_WAL_FETCH_BATCH_NUM_BYTES),
                        ),
                        std::mem::take(&mut mrecord_lengths),
                        fetch_message_tx,
                    )
                    .await
                {
                    return Ok(());
                }
                mrecord_buffer.put_slice(&record);
                mrecord_lengths.push(record.len() as u32);
                batch_to_position_inclusive = position;
                from_position_inclusive = position + 1;
                v3_metrics::WAL_OBJECT_FALLBACK_DRAINED_RECORDS_TOTAL.inc();
            }
        }
    }
    if !mrecord_lengths.is_empty()
        && !send_object_wal_payload(
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            batch_to_position_inclusive,
            mrecord_buffer,
            mrecord_lengths,
            fetch_message_tx,
        )
        .await
    {
        return Ok(());
    }
    let eof_position = from_position_exclusive.as_eof();
    let fetch_eof = FetchEof {
        index_uid: Some(index_uid.clone()),
        source_id: source_id.clone(),
        shard_id: Some(shard_id.clone()),
        eof_position: Some(eof_position.clone()),
    };
    let fetch_message = FetchMessage::new_eof(fetch_eof);
    let in_flight_value =
        InFlightValue::new(fetch_message, ByteSize(0), &IN_FLIGHT_MULTI_FETCH_STREAM);
    let _ = fetch_message_tx.send(Ok(in_flight_value)).await;
    *from_position_exclusive = eof_position;
    info!(
        queue_id=%queue_id,
        fence_wal_id=fence.wal_id.0,
        "drained shard from the object WAL of ingester `{ingester_id}`"
    );
    Ok(())
}

/// Sends one payload built from object WAL records and advances `from_position_exclusive`.
/// Returns `false` if the consumer went away.
#[allow(clippy::too_many_arguments)]
async fn send_object_wal_payload(
    index_uid: &IndexUid,
    source_id: &SourceId,
    shard_id: &ShardId,
    from_position_exclusive: &mut Position,
    to_position_inclusive: u64,
    mrecord_buffer: BytesMut,
    mrecord_lengths: Vec<u32>,
    fetch_message_tx: &mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) -> bool {
    let to_position_inclusive = Position::offset(to_position_inclusive);
    let mrecord_batch = MRecordBatch {
        mrecord_buffer: mrecord_buffer.freeze(),
        mrecord_lengths,
    };
    let fetch_payload = FetchPayload {
        index_uid: Some(index_uid.clone()),
        source_id: source_id.clone(),
        shard_id: Some(shard_id.clone()),
        mrecord_batch: Some(mrecord_batch),
        // Always continue from what the consumer last saw, so its checkpoint delta chains
        // even if the log has a gap.
        from_position_exclusive: Some(from_position_exclusive.clone()),
        to_position_inclusive: Some(to_position_inclusive.clone()),
    };
    let batch_size = fetch_payload.estimate_size();
    let fetch_message = FetchMessage::new_payload(fetch_payload);
    let in_flight_value =
        InFlightValue::new(fetch_message, batch_size, &IN_FLIGHT_MULTI_FETCH_STREAM);
    if fetch_message_tx.send(Ok(in_flight_value)).await.is_err() {
        return false;
    }
    *from_position_exclusive = to_position_inclusive;
    true
}

/// Streams records from an ingester until the stream ends or fails.
#[allow(clippy::too_many_arguments)]
async fn fetch_stream_once(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    from_position_exclusive: &mut Position,
    ingester_id: &NodeId,
    ingester_pool: IngesterPool,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    let Some(ingester) = ingester_pool.get(ingester_id) else {
        error!(
            client_id=%client_id,
            index_uid=%index_uid,
            source_id=%source_id,
            shard_id=%shard_id,
            "ingester `{ingester_id}` is unavailable: closing fetch stream"
        );
        let ingest_error = IngestV2Error::Unavailable(format!(
            "ingester `{ingester_id}` is unavailable: closing fetch stream"
        ));
        let fetch_stream_error = FetchStreamError {
            index_uid,
            source_id,
            shard_id,
            ingest_error,
        };
        let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
        return;
    };
    let open_fetch_stream_request = OpenFetchStreamRequest {
        client_id: client_id.clone(),
        index_uid: index_uid.clone().into(),
        source_id: source_id.clone(),
        shard_id: Some(shard_id.clone()),
        from_position_exclusive: Some(from_position_exclusive.clone()),
    };
    let mut fetch_stream = match ingester
        .client
        .open_fetch_stream(open_fetch_stream_request)
        .await
    {
        Ok(fetch_stream) => fetch_stream,
        Err(ingest_error) => {
            let is_shard_not_found = matches!(&ingest_error, IngestV2Error::ShardNotFound { .. });

            if is_shard_not_found {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    "failed to open fetch stream from ingester `{ingester_id}`: shard not found"
                );
            } else {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    error=%ingest_error,
                    "failed to open fetch stream from ingester `{ingester_id}`: closing fetch stream"
                );
            }
            let fetch_stream_error = FetchStreamError {
                index_uid,
                source_id,
                shard_id,
                ingest_error,
            };
            let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;

            if is_shard_not_found {
                from_position_exclusive.to_eof();
            }
            return;
        }
    };
    while let Some(fetch_message_result) = fetch_stream.next().await {
        match fetch_message_result {
            Ok(fetch_message) => match &fetch_message.message {
                Some(fetch_message::Message::Payload(fetch_payload)) => {
                    let batch_size = fetch_payload.estimate_size();
                    let to_position_inclusive = fetch_payload.to_position_inclusive();
                    let in_flight_value = InFlightValue::new(
                        fetch_message,
                        batch_size,
                        &IN_FLIGHT_MULTI_FETCH_STREAM,
                    );
                    if fetch_message_tx.send(Ok(in_flight_value)).await.is_err() {
                        // The consumer was dropped.
                        return;
                    }
                    *from_position_exclusive = to_position_inclusive;
                }
                Some(fetch_message::Message::Eof(fetch_eof)) => {
                    let eof_position = fetch_eof.eof_position();
                    let in_flight_value = InFlightValue::new(
                        fetch_message,
                        ByteSize(0),
                        &IN_FLIGHT_MULTI_FETCH_STREAM,
                    );
                    // We ignore the send error if the consumer was dropped because we're going
                    // to return anyway.
                    let _ = fetch_message_tx.send(Ok(in_flight_value)).await;

                    *from_position_exclusive = eof_position;
                    return;
                }
                None => {
                    warn!("received empty fetch message");
                    continue;
                }
            },
            Err(ingest_error) => {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    error=%ingest_error,
                    "failed to fetch records from ingester `{ingester_id}`: closing fetch stream"
                );
                let fetch_stream_error = FetchStreamError {
                    index_uid,
                    source_id,
                    shard_id,
                    ingest_error,
                };
                let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                return;
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, MockIngesterService};
    use quickwit_proto::types::queue_id;
    use tokio::time::timeout;

    use super::*;
    use crate::{IngesterPoolEntry, MRecord};

    pub fn into_fetch_payload(fetch_message: FetchMessage) -> FetchPayload {
        match fetch_message.message.unwrap() {
            fetch_message::Message::Payload(fetch_payload) => fetch_payload,
            other => panic!("expected fetch payload, got `{other:?}`"),
        }
    }

    pub fn into_fetch_eof(fetch_message: FetchMessage) -> FetchEof {
        match fetch_message.message.unwrap() {
            fetch_message::Message::Eof(fetch_eof) => fetch_eof,
            other => panic!("expected fetch EOF, got `{other:?}`"),
        }
    }

    #[tokio::test]
    async fn test_fetch_task_happy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();
        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(fetch_payload.index_uid(), &index_uid);
        assert_eq!(fetch_payload.source_id, source_id);
        assert_eq!(fetch_payload.shard_id(), shard_id);
        assert_eq!(fetch_payload.from_position_exclusive(), Position::Beginning);
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-foo"
        );

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        // Trigger a spurious notification.
        let shard_status = (ShardState::Open, Position::offset(0u64));
        shard_status_tx.send(shard_status).unwrap();

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-bar").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status.clone()).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-bar"
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        let mrecords = [
            MRecord::new_doc("test-doc-baz").encode(),
            MRecord::new_doc("test-doc-qux").encode(),
        ]
        .into_iter();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(&queue_id, None, mrecords)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(3u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(3u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14, 14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-baz\0\0test-doc-qux"
        );

        let shard_status = (ShardState::Closed, Position::offset(3u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.index_uid(), &index_uid);
        assert_eq!(fetch_eof.source_id, source_id);
        assert_eq!(fetch_eof.shard_id(), shard_id);
        assert_eq!(fetch_eof.eof_position, Some(Position::eof(3u64)));

        fetch_task_handle.await.unwrap();
    }

    /// Spawns a fetch task against a closed shard whose queue holds `num_records` records and whose
    /// replication position is `replication_position_inclusive`, then asserts that fetching from
    /// `from_position_exclusive` immediately signals EOF at `expected_eof_position`.
    async fn check_fetch_task_signals_eof(
        num_records: usize,
        replication_position_inclusive: Position,
        from_position_exclusive: Position,
        expected_eof_position: Position,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        if num_records > 0 {
            let records = (0..num_records).map(|_| MRecord::new_doc("test-doc-foo").encode());
            mrecordlog_guard
                .as_mut()
                .unwrap()
                .append_records(&queue_id, None, records)
                .await
                .unwrap();
        }
        drop(mrecordlog_guard);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id,
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(from_position_exclusive.clone()),
        };
        let shard_status = (ShardState::Closed, replication_position_inclusive);
        let (_shard_status_tx, shard_status_rx) = watch::channel(shard_status);

        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.index_uid(), &index_uid);
        assert_eq!(fetch_eof.source_id, source_id);
        assert_eq!(fetch_eof.shard_id(), shard_id);
        assert_eq!(
            fetch_eof.eof_position,
            Some(expected_eof_position),
            "unexpected EOF position when fetching from `{from_position_exclusive:?}`"
        );

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_signals_eof() {
        check_fetch_task_signals_eof(
            0,
            Position::Beginning,
            Position::Beginning,
            Position::Beginning.as_eof(),
        )
        .await;

        check_fetch_task_signals_eof(
            0,
            Position::Beginning,
            Position::offset(0u64),
            Position::eof(0u64),
        )
        .await;

        check_fetch_task_signals_eof(
            0,
            Position::Beginning,
            Position::offset(42u64),
            Position::eof(42u64),
        )
        .await;

        check_fetch_task_signals_eof(
            1,
            Position::offset(0u64),
            Position::offset(0u64),
            Position::eof(0u64),
        )
        .await;

        check_fetch_task_signals_eof(
            1,
            Position::offset(0u64),
            Position::offset(42u64),
            Position::eof(42u64),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_task_from_position_exclusive() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::offset(0u64)),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(0u64));
        shard_status_tx.send(shard_status).unwrap();

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-bar").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(fetch_payload.index_uid(), &index_uid);
        assert_eq!(fetch_payload.source_id, source_id);
        assert_eq!(fetch_payload.shard_id(), shard_id);
        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-bar"
        );
    }

    #[tokio::test]
    async fn test_fetch_task_error() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (_shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let ingest_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(ingest_error, IngestV2Error::Internal(_)));

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            30,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        let records = [
            Bytes::from_static(b"test-doc-foo"),
            Bytes::from_static(b"test-doc-bar"),
            Bytes::from_static(b"test-doc-baz"),
        ]
        .into_iter();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(&queue_id, None, records)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(2u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "test-doc-footest-doc-bar"
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "test-doc-baz"
        );
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes_less_than_record_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            10, //< we request batch larger than 10 bytes.
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                // This doc is longer than 10 bytes.
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();

        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-foo"
        );
    }

    #[tokio::test]
    async fn test_fetch_stream_once_shard_not_found() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_id = NodeId::from_str("test-ingester-0");
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::ShardNotFound {
                    shard_id: ShardId::from(1),
                })
            });
        let ingester_0 =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_0));
        ingester_pool.insert(NodeId::from_str("test-ingester-0"), ingester_0);

        fetch_stream_once(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_id,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            fetch_stream_error.ingest_error,
            IngestV2Error::ShardNotFound { shard_id } if shard_id == ShardId::from(1)
        ));
        assert!(from_position_exclusive.is_eof());
    }

    #[tokio::test]
    async fn test_retrying_fetch_stream() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let from_position_exclusive = Position::offset(0u64);

        let ingester_id = NodeId::from_str("test-ingester");
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();
        let (service_stream_tx_2, service_stream_2) = ServiceStream::new_unbounded();

        let mut retry_params = RetryParams::for_test();
        retry_params.max_attempts = 3;

        let mut mock_ingester = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .returning(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::Internal(
                    "open fetch stream error".to_string(),
                ))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_1)
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(1u64));

                Ok(service_stream_2)
            });
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));

        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #1".into());
        service_stream_tx_1.send(Err(ingest_error)).unwrap();

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-bar"]),
            from_position_exclusive: Some(Position::offset(1u64)),
            to_position_inclusive: Some(Position::offset(2u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_2.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #2".into());
        service_stream_tx_2.send(Err(ingest_error)).unwrap();

        retrying_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_id,
            ingester_pool,
            retry_params,
            None,
            fetch_message_tx,
        )
        .await;

        let ingest_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .ingest_error;
        assert!(
            matches!(ingest_error, IngestV2Error::Internal(message) if message == "open fetch stream error")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #1")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(2u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #2")
        );

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {
        let client_id = "test-client".to_string();
        let ingester_pool = IngesterPool::default();
        let retry_params = RetryParams::for_test();
        let _multi_fetch_stream = MultiFetchStream::new(client_id, ingester_pool, retry_params);
        // TODO: Backport from original branch.
    }

    // ---- Ingest v3: draining an absent ingester's shard from its object WAL ----

    async fn write_dead_ingester_log(
        storage: &Arc<dyn quickwit_storage::Storage>,
        queue_id: &str,
        num_records: u64,
    ) {
        use crate::ingest_v3::wal::writer::{WalWriter, WalWriterConfig};
        let writer = WalWriter::spawn(
            storage.clone(),
            "dead-ingester",
            1,
            0,
            WalId(1),
            WalWriterConfig {
                flush_interval: Duration::from_secs(3600),
                ..Default::default()
            },
        );
        // Spread the records over several objects.
        for position in 0..num_records {
            writer
                .append(
                    queue_id,
                    position,
                    vec![MRecord::new_doc(format!("doc-{position}")).encode_to_bytes()],
                )
                .unwrap();
            if position % 2 == 1 {
                writer.flush().await.unwrap();
            }
        }
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_multi_fetch_stream_drains_absent_ingester_from_object_wal() {
        let storage: Arc<dyn quickwit_storage::Storage> =
            Arc::new(quickwit_storage::RamStorage::default());
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);
        write_dead_ingester_log(&storage, &queue_id, 5).await;

        // Nobody in the pool: the ingester is gone.
        let ingester_pool = IngesterPool::default();
        let fallback = ObjectWalFallback {
            storage: storage.clone(),
            grace: Duration::ZERO,
        };
        let mut fetch_stream = MultiFetchStream::new(
            "test-client".to_string(),
            ingester_pool,
            RetryParams::for_test(),
        )
        .with_object_wal_fallback(fallback);

        // Resume from position 1: records 0 and 1 were already published.
        fetch_stream
            .subscribe(
                NodeId::from_str("dead-ingester"),
                index_uid.clone(),
                source_id.clone(),
                shard_id.clone(),
                Position::offset(1u64),
            )
            .await
            .unwrap();

        let mut positions = Vec::new();
        let mut docs = Vec::new();
        let mut last_from_position_exclusive = Position::offset(1u64);
        loop {
            let fetch_message = timeout(Duration::from_secs(5), fetch_stream.next())
                .await
                .unwrap()
                .unwrap();
            match fetch_message.message.unwrap() {
                fetch_message::Message::Payload(payload) => {
                    assert_eq!(
                        payload.from_position_exclusive(),
                        last_from_position_exclusive
                    );
                    last_from_position_exclusive = payload.to_position_inclusive();
                    positions.push(payload.to_position_inclusive());
                    for mrecord in crate::decoded_mrecords(payload.mrecord_batch.as_ref().unwrap())
                    {
                        if let MRecord::Doc(doc) = mrecord {
                            docs.push(String::from_utf8(doc.to_vec()).unwrap());
                        }
                    }
                }
                fetch_message::Message::Eof(eof) => {
                    assert_eq!(eof.eof_position(), Position::eof(4u64));
                    break;
                }
            }
        }
        assert_eq!(docs, vec!["doc-2", "doc-3", "doc-4"]);
        assert_eq!(positions.last(), Some(&Position::offset(4u64)));

        // The log is fenced: the dead ingester cannot append anymore.
        let reader = WalReader::new(storage.clone(), "dead-ingester");
        let tail = reader.last_wal_id(WalId::ZERO).await.unwrap();
        let footer = reader.read_footer(tail, None).await.unwrap();
        assert!(footer.header.is_fence);
        assert_eq!(footer.header.epoch, 2);
    }

    #[tokio::test]
    async fn test_multi_fetch_stream_object_wal_fallback_waits_for_grace() {
        let storage: Arc<dyn quickwit_storage::Storage> =
            Arc::new(quickwit_storage::RamStorage::default());
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);
        write_dead_ingester_log(&storage, &queue_id, 1).await;

        let fallback = ObjectWalFallback {
            storage: storage.clone(),
            grace: Duration::from_secs(3600),
        };
        let mut fetch_stream = MultiFetchStream::new(
            "test-client".to_string(),
            IngesterPool::default(),
            RetryParams::for_test(),
        )
        .with_object_wal_fallback(fallback);
        fetch_stream
            .subscribe(
                NodeId::from_str("dead-ingester"),
                index_uid,
                source_id,
                shard_id,
                Position::Beginning,
            )
            .await
            .unwrap();
        // Within the grace period, the stream keeps reporting the ingester as unavailable.
        let error = timeout(Duration::from_secs(1), fetch_stream.next())
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error.ingest_error, IngestV2Error::Unavailable(_)));
        // ... and the log has not been fenced.
        let reader = WalReader::new(storage, "dead-ingester");
        let tail = reader.last_wal_id(WalId::ZERO).await.unwrap();
        assert!(
            !reader
                .read_footer(tail, None)
                .await
                .unwrap()
                .header
                .is_fence
        );
    }
}
