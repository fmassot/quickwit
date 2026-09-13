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

//! The WAL writer: buffers records in memory and flushes them as one object per flush.
//!
//! ```text
//! append(queue, pos, records) ──► current object (in memory) ──┐
//!                                                              │ flush: interval | size | explicit
//!                                                              ▼
//!                                  encode ──► put_if_absent(<ingester>/wal/<wal_id>.wal)
//!                                                              │
//!                                     Ok ──► durable_wal_id = wal_id ──► wake waiters
//!                                     AlreadyExists ──► ours? (retried PUT) ──► same as Ok
//!                                                   └─► not ours ──► FENCED, fail everything
//!                                     other error ──► retry with backoff
//! ```
//!
//! Objects are encoded and uploaded in a two-stage pipeline: the flusher freezes and encodes
//! object N+1 (block compression in parallel on the CPU pool) while the uploader PUTs object N.
//! PUTs themselves are sequential, so object ids are assigned in append order and
//! `durable_wal_id` moves monotonically. Group commit is implicit: every record appended between
//! two flushes shares one PUT.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use bytesize::ByteSize;
use quickwit_proto::types::QueueId;
use quickwit_storage::{Storage, StorageErrorKind};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::format::{WalBlock, WalObjectHeader, encode_wal_object};
use super::reader::WalReader;
use super::{WalError, WalId, WalResult, wal_object_path};
use crate::ingest_v3::metrics;

/// Writer configuration.
#[derive(Clone, Debug)]
pub struct WalWriterConfig {
    /// Maximum time records wait in memory before being written.
    pub flush_interval: Duration,
    /// Write as soon as the current object reaches this size.
    pub flush_num_bytes: ByteSize,
    /// Reject appends once this many bytes are buffered and not yet durable.
    pub max_buffered_num_bytes: ByteSize,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(250),
            flush_num_bytes: ByteSize::mib(8),
            max_buffered_num_bytes: ByteSize::mib(256),
        }
    }
}

/// Snapshot of the writer state.
#[derive(Clone, Debug)]
pub struct WalWriterStatus {
    /// Id of the last object written durably. [`WalId::ZERO`] if none yet.
    pub durable_wal_id: WalId,
    /// Bytes buffered in memory and not yet durable (records only, before encoding).
    pub buffered_num_bytes: u64,
    /// Number of records buffered in memory and not yet durable.
    pub buffered_num_records: u64,
    /// Terminal error, if the writer stopped.
    pub closed_reason: Option<WalError>,
}

/// What the durability watch channel carries.
#[derive(Clone, Debug)]
pub enum Durability {
    /// Objects up to this id are durable.
    Flushed(WalId),
    /// The writer stopped; nothing further will become durable.
    Failed(WalError),
}

/// Records buffered for the object currently being assembled.
struct OpenObject {
    wal_id: WalId,
    blocks: Vec<WalBlock>,
    /// Index of the last block of each queue in `blocks`, to extend it when the next append is
    /// contiguous.
    last_block_per_queue: HashMap<QueueId, usize>,
    num_bytes: u64,
    num_records: u64,
    /// When the first record was appended: the start of the durability latency of this object.
    first_append_at: Option<Instant>,
}

impl OpenObject {
    fn new(wal_id: WalId) -> Self {
        Self {
            wal_id,
            blocks: Vec::new(),
            last_block_per_queue: HashMap::new(),
            num_bytes: 0,
            num_records: 0,
            first_append_at: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn append_queue_marker(&mut self, queue_id: &str) {
        self.first_append_at.get_or_insert_with(Instant::now);
        self.blocks
            .push(WalBlock::queue_marker(queue_id.to_string()));
    }

    fn append(&mut self, queue_id: &str, first_position: u64, records: Vec<Bytes>) {
        let num_bytes: u64 = records.iter().map(|record| record.len() as u64).sum();
        self.num_bytes += num_bytes;
        self.num_records += records.len() as u64;
        self.first_append_at.get_or_insert_with(Instant::now);

        if let Some(&block_idx) = self.last_block_per_queue.get(queue_id) {
            let block = &mut self.blocks[block_idx];
            if !block.is_queue_marker() && block.last_position() + 1 == first_position {
                block.records.extend(records);
                return;
            }
        }
        self.last_block_per_queue
            .insert(queue_id.to_string(), self.blocks.len());
        self.blocks.push(WalBlock {
            queue_id: queue_id.to_string(),
            first_position,
            records,
        });
    }
}

struct WriterState {
    current: OpenObject,
    /// Bytes of the object being written, if a flush is in flight.
    in_flight_num_bytes: u64,
    in_flight_num_records: u64,
    durable_wal_id: WalId,
    closed_reason: Option<WalError>,
}

/// Buffers records and writes them to object storage. Cheap to clone.
#[derive(Clone)]
pub struct WalWriter {
    inner: Arc<WalWriterInner>,
}

struct WalWriterInner {
    ingester_id: String,
    epoch: u64,
    generation_id: u64,
    config: WalWriterConfig,
    state: Mutex<WriterState>,
    durability_rx: watch::Receiver<Durability>,
    flush_notify: Notify,
    shutdown_notify: Notify,
    flusher_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WalWriter {
    /// Starts a writer for `ingester_id`'s log at `epoch` (obtained from
    /// [`super::fence::fence_log`]). The first object written gets id `first_wal_id`
    /// (`fence_id + 1`). `generation_id` is stamped on objects for observability. Spawns the
    /// flusher on the current tokio runtime.
    pub fn spawn(
        storage: Arc<dyn Storage>,
        ingester_id: impl Into<String>,
        epoch: u64,
        generation_id: u64,
        first_wal_id: WalId,
        config: WalWriterConfig,
    ) -> Self {
        let ingester_id = ingester_id.into();
        let (durability_tx, durability_rx) =
            watch::channel(Durability::Flushed(WalId(first_wal_id.0.saturating_sub(1))));
        let inner = Arc::new(WalWriterInner {
            ingester_id: ingester_id.clone(),
            epoch,
            generation_id,
            config,
            state: Mutex::new(WriterState {
                current: OpenObject::new(first_wal_id),
                in_flight_num_bytes: 0,
                in_flight_num_records: 0,
                durable_wal_id: WalId(first_wal_id.0.saturating_sub(1)),
                closed_reason: None,
            }),
            durability_rx,
            flush_notify: Notify::new(),
            shutdown_notify: Notify::new(),
            flusher_handle: Mutex::new(None),
        });
        let flusher = Flusher {
            inner: inner.clone(),
            storage,
            durability_tx,
        };
        let handle = tokio::spawn(flusher.run());
        *inner.flusher_handle.lock().unwrap() = Some(handle);
        info!(
            ingester_id,
            epoch,
            first_wal_id = first_wal_id.0,
            "started WAL writer"
        );
        Self { inner }
    }

    pub fn ingester_id(&self) -> &str {
        &self.inner.ingester_id
    }

    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Buffers `records` for `queue_id` at consecutive positions starting at `first_position`.
    /// Returns the id of the object that will contain them; await it with
    /// [`WalWriter::wait_durable`].
    ///
    /// A single call is written atomically into one object. Fails with
    /// [`WalError::BufferFull`] when the unflushed bytes exceed the configured maximum, with
    /// [`WalError::Fenced`] or [`WalError::Closed`] when the writer stopped.
    pub fn append(
        &self,
        queue_id: &str,
        first_position: u64,
        records: Vec<Bytes>,
    ) -> WalResult<WalId> {
        assert!(!records.is_empty(), "cannot append zero records");
        let num_bytes: u64 = records.iter().map(|record| record.len() as u64).sum();
        let mut state = self.inner.state.lock().unwrap();
        if let Some(reason) = &state.closed_reason {
            return Err(reason.clone());
        }
        let buffered = state.current.num_bytes + state.in_flight_num_bytes;
        if buffered + num_bytes > self.inner.config.max_buffered_num_bytes.as_u64() {
            return Err(WalError::BufferFull);
        }
        state.current.append(queue_id, first_position, records);
        let wal_id = state.current.wal_id;
        let should_flush = state.current.num_bytes >= self.inner.config.flush_num_bytes.as_u64();
        metrics::WAL_OBJECT_BUFFERED_BYTES
            .set((state.current.num_bytes + state.in_flight_num_bytes) as f64);
        drop(state);
        if should_flush {
            self.inner.flush_notify.notify_one();
        }
        Ok(wal_id)
    }

    /// Records that `queue_id` exists (a queue marker, see [`WalBlock::queue_marker`]). Returns
    /// the id of the object that will contain the marker.
    pub fn append_queue_marker(&self, queue_id: &str) -> WalResult<WalId> {
        let mut state = self.inner.state.lock().unwrap();
        if let Some(reason) = &state.closed_reason {
            return Err(reason.clone());
        }
        state.current.append_queue_marker(queue_id);
        Ok(state.current.wal_id)
    }

    /// A receiver of durability events: the last durable id, or the terminal error.
    pub fn durability_receiver(&self) -> watch::Receiver<Durability> {
        self.inner.durability_rx.clone()
    }

    /// Resolves once object `wal_id` is durable, or with the terminal error of the writer.
    pub async fn wait_durable(&self, wal_id: WalId) -> WalResult<()> {
        let mut rx = self.inner.durability_rx.clone();
        loop {
            match &*rx.borrow_and_update() {
                Durability::Flushed(durable) if *durable >= wal_id => return Ok(()),
                Durability::Failed(error) => return Err(error.clone()),
                Durability::Flushed(_) => {}
            }
            if rx.changed().await.is_err() {
                return Err(WalError::Closed);
            }
        }
    }

    /// Writes everything buffered so far and waits for it to be durable.
    pub async fn flush(&self) -> WalResult<()> {
        let target = {
            let state = self.inner.state.lock().unwrap();
            if let Some(reason) = &state.closed_reason {
                return Err(reason.clone());
            }
            if state.current.is_empty() {
                // Nothing new: wait for whatever is in flight.
                WalId(state.current.wal_id.0 - 1)
            } else {
                state.current.wal_id
            }
        };
        self.inner.flush_notify.notify_one();
        self.wait_durable(target).await
    }

    pub fn status(&self) -> WalWriterStatus {
        let state = self.inner.state.lock().unwrap();
        WalWriterStatus {
            durable_wal_id: state.durable_wal_id,
            buffered_num_bytes: state.current.num_bytes + state.in_flight_num_bytes,
            buffered_num_records: state.current.num_records + state.in_flight_num_records,
            closed_reason: state.closed_reason.clone(),
        }
    }

    /// Flushes what is buffered (best effort) and stops the flusher. Subsequent appends fail
    /// with [`WalError::Closed`].
    pub async fn close(&self) -> WalResult<()> {
        let flush_result = self.flush().await;
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.closed_reason.is_none() {
                state.closed_reason = Some(WalError::Closed);
            }
        }
        self.inner.shutdown_notify.notify_one();
        let handle = self.inner.flusher_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        flush_result
    }
}

/// An encoded object on its way to the uploader.
struct EncodedObject {
    header: WalObjectHeader,
    bytes: Bytes,
    num_bytes: u64,
    num_records: u64,
    first_append_at: Option<Instant>,
}

/// Encoded objects waiting for upload: one in flight, this many queued behind it.
const UPLOAD_QUEUE_DEPTH: usize = 1;

struct Flusher {
    inner: Arc<WalWriterInner>,
    storage: Arc<dyn Storage>,
    durability_tx: watch::Sender<Durability>,
}

impl Flusher {
    async fn run(self) {
        let (upload_tx, upload_rx) =
            tokio::sync::mpsc::channel::<EncodedObject>(UPLOAD_QUEUE_DEPTH);
        let uploader = Uploader {
            inner: self.inner.clone(),
            storage: self.storage.clone(),
            durability_tx: self.durability_tx.clone(),
        };
        let uploader_handle = tokio::spawn(uploader.run(upload_rx));

        // `interval_at`: the first tick must happen one period from now, not immediately.
        let period = self.inner.config.flush_interval;
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = self.inner.flush_notify.notified() => {}
                _ = self.inner.shutdown_notify.notified() => {
                    // Final best-effort flush, then exit.
                    let _ = self.freeze_and_encode(&upload_tx).await;
                    drop(upload_tx);
                    let _ = uploader_handle.await;
                    self.fail_remaining(WalError::Closed);
                    return;
                }
            }
            if self.inner.state.lock().unwrap().closed_reason.is_some() {
                // The uploader hit a terminal error (fenced).
                drop(upload_tx);
                let _ = uploader_handle.await;
                return;
            }
            if self.freeze_and_encode(&upload_tx).await.is_err() {
                // The uploader is gone.
                let _ = uploader_handle.await;
                return;
            }
        }
    }

    /// Freezes the current object, encodes it, and hands it to the uploader. Blocks while the
    /// upload queue is full (backpressure). `Err` means the uploader stopped.
    async fn freeze_and_encode(
        &self,
        upload_tx: &tokio::sync::mpsc::Sender<EncodedObject>,
    ) -> Result<(), ()> {
        let frozen = {
            let mut state = self.inner.state.lock().unwrap();
            if state.current.is_empty() {
                return Ok(());
            }
            let next = OpenObject::new(state.current.wal_id.next());
            let frozen = std::mem::replace(&mut state.current, next);
            state.in_flight_num_bytes += frozen.num_bytes;
            state.in_flight_num_records += frozen.num_records;
            frozen
        };
        let header = WalObjectHeader {
            ingester_id: self.inner.ingester_id.clone(),
            epoch: self.inner.epoch,
            generation_id: self.inner.generation_id,
            wal_id: frozen.wal_id,
            is_fence: false,
        };
        let num_bytes = frozen.num_bytes;
        let num_records = frozen.num_records;
        let first_append_at = frozen.first_append_at;
        let encode_header = header.clone();
        let bytes = match quickwit_common::thread_pool::run_cpu_intensive(move || {
            encode_wal_object(&encode_header, &frozen.blocks)
        })
        .await
        {
            Ok(bytes) => bytes,
            Err(_panicked) => {
                error!(ingester_id = %self.inner.ingester_id, "WAL object encoding panicked");
                self.fail_remaining(WalError::Closed);
                return Err(());
            }
        };
        upload_tx
            .send(EncodedObject {
                header,
                bytes,
                num_bytes,
                num_records,
                first_append_at,
            })
            .await
            .map_err(|_| ())
    }

    /// Fails every waiter and marks the writer closed with `error`.
    fn fail_remaining(&self, error: WalError) {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed_reason.is_none() {
            state.closed_reason = Some(error.clone());
        }
        let anything_pending = !state.current.is_empty() || state.in_flight_num_bytes > 0;
        drop(state);
        if anything_pending || matches!(error, WalError::Fenced) {
            let _ = self.durability_tx.send(Durability::Failed(error));
        }
    }
}

/// Uploads encoded objects in order and advances the durable id.
struct Uploader {
    inner: Arc<WalWriterInner>,
    storage: Arc<dyn Storage>,
    durability_tx: watch::Sender<Durability>,
}

impl Uploader {
    async fn run(self, mut upload_rx: tokio::sync::mpsc::Receiver<EncodedObject>) {
        while let Some(object) = upload_rx.recv().await {
            if let Err(error) = self.upload(object).await {
                error!(ingester_id = %self.inner.ingester_id, %error, "WAL writer stopped");
                self.fail_remaining(error);
                self.inner.flush_notify.notify_one();
                return;
            }
        }
    }

    /// Fails every waiter and marks the writer closed with `error`.
    fn fail_remaining(&self, error: WalError) {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed_reason.is_none() {
            state.closed_reason = Some(error.clone());
        }
        drop(state);
        let _ = self.durability_tx.send(Durability::Failed(error));
    }

    /// Writes one object and advances the durable id. Returns an error only for terminal
    /// conditions (fenced); transient storage errors are retried forever.
    async fn upload(&self, object: EncodedObject) -> WalResult<()> {
        let EncodedObject {
            header,
            bytes,
            num_bytes,
            num_records,
            first_append_at,
        } = object;
        self.write_object(&header, bytes).await?;

        metrics::WAL_OBJECT_RECORDS_WRITTEN_TOTAL.inc_by(num_records);
        if let Some(first_append_at) = first_append_at {
            metrics::WAL_OBJECT_FLUSH_LATENCY_SECS.observe(first_append_at.elapsed().as_secs_f64());
        }
        {
            let mut state = self.inner.state.lock().unwrap();
            state.durable_wal_id = header.wal_id;
            state.in_flight_num_bytes -= num_bytes;
            state.in_flight_num_records -= num_records;
            metrics::WAL_OBJECT_BUFFERED_BYTES
                .set((state.current.num_bytes + state.in_flight_num_bytes) as f64);
        }
        let _ = self.durability_tx.send(Durability::Flushed(header.wal_id));
        Ok(())
    }

    async fn write_object(&self, header: &WalObjectHeader, bytes: Bytes) -> WalResult<()> {
        let path = wal_object_path(&self.inner.ingester_id, header.wal_id);
        let num_bytes = bytes.len() as u64;
        let mut backoff = Duration::from_millis(50);
        loop {
            let put_start = Instant::now();
            match self
                .storage
                .put_if_absent(&path, Box::new(bytes.to_vec()))
                .await
            {
                Ok(()) => {
                    metrics::WAL_OBJECT_PUTS_SUCCESS.inc();
                    metrics::WAL_OBJECT_PUT_DURATION_SECS
                        .observe(put_start.elapsed().as_secs_f64());
                    metrics::WAL_OBJECT_BYTES_WRITTEN_TOTAL.inc_by(num_bytes);
                    return Ok(());
                }
                Err(error) if error.kind() == StorageErrorKind::AlreadyExists => {
                    // Either a previous attempt landed but we did not see the response (our own
                    // object), or another writer took the slot (we are fenced).
                    return self.disambiguate_existing(header).await;
                }
                Err(error) => {
                    metrics::WAL_OBJECT_PUTS_ERROR.inc();
                    warn!(
                        ingester_id = %self.inner.ingester_id,
                        wal_id = header.wal_id.0,
                        %error,
                        "failed to write WAL object, retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn disambiguate_existing(&self, header: &WalObjectHeader) -> WalResult<()> {
        let reader = WalReader::new(self.storage.clone(), self.inner.ingester_id.clone());
        let footer = reader.read_footer(header.wal_id, None).await?;
        if footer.header == *header {
            metrics::WAL_OBJECT_PUTS_ALREADY_WRITTEN.inc();
            info!(
                ingester_id = %self.inner.ingester_id,
                wal_id = header.wal_id.0,
                "WAL object already written by an earlier attempt"
            );
            return Ok(());
        }
        metrics::WAL_OBJECT_PUTS_FENCED.inc();
        metrics::WAL_OBJECT_FENCED_TOTAL.inc();
        warn!(
            ingester_id = %self.inner.ingester_id,
            wal_id = header.wal_id.0,
            our_epoch = header.epoch,
            found_epoch = footer.header.epoch,
            "WAL slot taken by another writer: fenced"
        );
        Err(WalError::Fenced)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::ingest_v3::wal::format::encode_fence_object;

    fn records(prefix: &str, num: usize) -> Vec<Bytes> {
        (0..num)
            .map(|i| Bytes::from(format!("{prefix}-{i}")))
            .collect()
    }

    fn config() -> WalWriterConfig {
        WalWriterConfig {
            // Long interval: tests drive flushes explicitly unless stated otherwise.
            flush_interval: Duration::from_secs(3600),
            flush_num_bytes: ByteSize::kib(64),
            max_buffered_num_bytes: ByteSize::kib(256),
        }
    }

    #[tokio::test]
    async fn test_append_flush_replay() {
        let storage = Arc::new(RamStorage::default());
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), config());

        let id_a = writer.append("q1", 0, records("a", 3)).unwrap();
        let id_b = writer.append("q2", 10, records("b", 1)).unwrap();
        // Contiguous append on q1 extends the block.
        let id_c = writer.append("q1", 3, records("c", 2)).unwrap();
        assert_eq!(id_a, WalId(1));
        assert_eq!(id_b, WalId(1));
        assert_eq!(id_c, WalId(1));
        assert_eq!(writer.status().buffered_num_records, 6);

        writer.flush().await.unwrap();
        assert_eq!(writer.status().durable_wal_id, WalId(1));
        assert_eq!(writer.status().buffered_num_bytes, 0);

        // Next append goes to object 2.
        let id_d = writer.append("q1", 5, records("d", 1)).unwrap();
        assert_eq!(id_d, WalId(2));
        writer.wait_durable(WalId(1)).await.unwrap(); // already durable
        writer.flush().await.unwrap();
        writer.wait_durable(WalId(2)).await.unwrap();

        let reader = WalReader::new(storage.clone(), "node");
        let (header, blocks) = reader.read_object(WalId(1)).await.unwrap();
        assert_eq!(header.epoch, 1);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].queue_id, "q1");
        assert_eq!(blocks[0].records.len(), 5);
        assert_eq!(blocks[0].last_position(), 4);

        let replayed = reader.replay_queue("q1", 2, 1..=2).await.unwrap();
        let positions: Vec<u64> = replayed.iter().map(|r| r.position).collect();
        assert_eq!(positions, vec![2, 3, 4, 5]);

        writer.close().await.unwrap();
        assert!(matches!(
            writer.append("q1", 6, records("e", 1)),
            Err(WalError::Closed)
        ));
    }

    #[tokio::test]
    async fn test_flush_on_size_threshold() {
        let storage = Arc::new(RamStorage::default());
        let mut cfg = config();
        cfg.flush_num_bytes = ByteSize::b(100);
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), cfg);
        let wal_id = writer
            .append("q", 0, vec![Bytes::from(vec![0u8; 200])])
            .unwrap();
        // No explicit flush: the size trigger must do it.
        tokio::time::timeout(Duration::from_secs(5), writer.wait_durable(wal_id))
            .await
            .expect("size-triggered flush")
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_on_interval() {
        let storage = Arc::new(RamStorage::default());
        let mut cfg = config();
        cfg.flush_interval = Duration::from_millis(20);
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), cfg);
        let wal_id = writer.append("q", 0, records("a", 1)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), writer.wait_durable(wal_id))
            .await
            .expect("interval-triggered flush")
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_many_waiters() {
        let storage = Arc::new(RamStorage::default());
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), config());
        let mut waiters = Vec::new();
        for i in 0..100u64 {
            let wal_id = writer.append("q", i, records("r", 1)).unwrap();
            let writer = writer.clone();
            waiters.push(tokio::spawn(
                async move { writer.wait_durable(wal_id).await },
            ));
        }
        writer.flush().await.unwrap();
        for waiter in waiters {
            waiter.await.unwrap().unwrap();
        }
        // One object for 100 appends.
        let reader = WalReader::new(storage.clone(), "node");
        assert_eq!(reader.list().await.unwrap().len(), 1);
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_backpressure() {
        let storage = Arc::new(RamStorage::default());
        let mut cfg = config();
        cfg.max_buffered_num_bytes = ByteSize::b(100);
        cfg.flush_num_bytes = ByteSize::mib(1);
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), cfg);
        writer
            .append("q", 0, vec![Bytes::from(vec![0u8; 60])])
            .unwrap();
        let error = writer
            .append("q", 1, vec![Bytes::from(vec![0u8; 60])])
            .unwrap_err();
        assert!(matches!(error, WalError::BufferFull));
        writer.flush().await.unwrap();
        writer
            .append("q", 1, vec![Bytes::from(vec![0u8; 60])])
            .unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_fenced_writer_fails_waiters_and_rejects_appends() {
        let storage = Arc::new(RamStorage::default());
        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), config());
        let wal_id = writer.append("q", 0, records("a", 1)).unwrap();

        // A newer owner fences the log at the slot we are about to write.
        let fence = encode_fence_object("node", 2, 0, WalId(1));
        storage
            .put_if_absent(&wal_object_path("node", WalId(1)), Box::new(fence.to_vec()))
            .await
            .unwrap();

        let error = writer.flush().await.unwrap_err();
        assert!(matches!(error, WalError::Fenced), "{error:?}");
        assert!(matches!(
            writer.wait_durable(wal_id).await,
            Err(WalError::Fenced)
        ));
        assert!(matches!(
            writer.append("q", 1, records("b", 1)),
            Err(WalError::Fenced)
        ));
        assert!(matches!(
            writer.status().closed_reason,
            Some(WalError::Fenced)
        ));

        // The fence is intact.
        let reader = WalReader::new(storage, "node");
        let footer = reader.read_footer(WalId(1), None).await.unwrap();
        assert!(footer.header.is_fence);
        assert_eq!(footer.header.epoch, 2);
    }

    #[tokio::test]
    async fn test_already_exists_with_our_own_header_is_success() {
        // Simulates a PUT that landed but whose response was lost: the retry sees
        // AlreadyExists, reads the header back, recognizes itself, and proceeds.
        let storage = Arc::new(RamStorage::default());
        let header = WalObjectHeader {
            ingester_id: "node".to_string(),
            epoch: 1,
            generation_id: 0,
            wal_id: WalId(1),
            is_fence: false,
        };
        let block = WalBlock {
            queue_id: "q".to_string(),
            first_position: 0,
            records: records("a", 1),
        };
        storage
            .put(
                &wal_object_path("node", WalId(1)),
                Box::new(encode_wal_object(&header, &[block]).to_vec()),
            )
            .await
            .unwrap();

        let writer = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), config());
        writer.append("q", 0, records("a", 1)).unwrap();
        writer.flush().await.unwrap();
        assert_eq!(writer.status().durable_wal_id, WalId(1));
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_writer_after_fence_continues_from_fence_id_plus_one() {
        use crate::ingest_v3::wal::fence::fence_log;

        let storage = Arc::new(RamStorage::default());
        // Old owner, epoch 1, writes two objects.
        let old = WalWriter::spawn(storage.clone(), "node", 1, 0, WalId(1), config());
        old.append("q", 0, records("a", 2)).unwrap();
        old.flush().await.unwrap();
        old.append("q", 2, records("b", 1)).unwrap();
        old.flush().await.unwrap();
        // ... and buffers one more that it never gets to flush before being fenced.
        let lost_id = old.append("q", 3, records("c", 1)).unwrap();

        // New owner, epoch 2, fences and takes over.
        let fence = fence_log(storage.clone(), "node", 0, WalId::ZERO)
            .await
            .unwrap();
        let fence_id = fence.wal_id;
        assert_eq!(fence_id, WalId(3));
        assert_eq!(fence.epoch, 2);
        let reader = WalReader::new(storage.clone(), "node");
        let replayed = reader.replay_queue("q", 0, 1..=fence_id.0).await.unwrap();
        assert_eq!(replayed.len(), 3); // positions 0..=2; the buffered record was never acked.

        // The old writer now fails.
        assert!(matches!(old.flush().await, Err(WalError::Fenced)));
        assert!(matches!(
            old.wait_durable(lost_id).await,
            Err(WalError::Fenced)
        ));

        // The new writer continues right after the fence.
        let new = WalWriter::spawn(
            storage.clone(),
            "node",
            fence.epoch,
            0,
            fence_id.next(),
            config(),
        );
        let id = new.append("q", 3, records("c", 1)).unwrap();
        assert_eq!(id, WalId(4));
        new.flush().await.unwrap();
        let replayed = reader.replay_queue("q", 0, 1..=4).await.unwrap();
        assert_eq!(replayed.len(), 4);
        new.close().await.unwrap();
    }
}
