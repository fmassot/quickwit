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
use std::io::Read;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::Progress;
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::uri::Uri;
use quickwit_proto::metastore::SourceType;
use quickwit_storage::StorageResolver;
use tokio::task::JoinHandle;

use super::chunked_line_reader::READ_CHUNK_SIZE;
use super::doc_file_reader::DocFileReader;
use super::{BATCH_NUM_BYTES_LIMIT, BatchBuilder, Source, SourceContext};
use crate::actors::DocProcessor;
use crate::models::RawDocBatch;

/// Cloneable work-stealing receiver of pre-chunked `RawDocBatch`es.
///
/// Multiple [`ChannelSource`] instances sharing clones of the same `LocalBatchReceiver`
/// pull batches concurrently on demand (MPMC), enabling multi-pipeline local ingestion
/// from a single stdin stream or input file.
#[derive(Clone, Debug)]
pub struct LocalBatchReceiver {
    batch_rx: flume::Receiver<RawDocBatch>,
}

impl LocalBatchReceiver {
    pub(crate) fn new(batch_rx: flume::Receiver<RawDocBatch>) -> Self {
        Self { batch_rx }
    }
}

/// Source that pulls `RawDocBatch`es from a shared in-process channel.
pub struct ChannelSource {
    receiver: LocalBatchReceiver,
    num_bytes_processed: u64,
    num_lines_processed: u64,
}

impl ChannelSource {
    pub fn new(receiver: LocalBatchReceiver) -> Self {
        Self {
            receiver,
            num_bytes_processed: 0,
            num_lines_processed: 0,
        }
    }
}

impl fmt::Debug for ChannelSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ChannelSource")
    }
}

#[async_trait]
impl Source for ChannelSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let batch_opt = ctx
            .protect_future(self.receiver.batch_rx.recv_async())
            .await
            .ok();
        match batch_opt {
            Some(batch) => {
                let batch_num_bytes: u64 = batch.docs.iter().map(|doc| doc.len() as u64).sum();
                self.num_bytes_processed += batch_num_bytes;
                self.num_lines_processed += batch.docs.len() as u64;
                ctx.send_message(doc_processor_mailbox, batch).await?;
                Ok(Duration::ZERO)
            }
            None => {
                ctx.send_exit_with_success(doc_processor_mailbox).await?;
                Err(ActorExitStatus::Success)
            }
        }
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "num_bytes_processed": self.num_bytes_processed,
            "num_lines_processed": self.num_lines_processed,
        })
    }
}

/// Spawns a background reader task that reads newline-delimited documents from either
/// `input_path_opt` (file, `.gz`, or object storage URI) or `stdin` in 5 MiB zero-copy
/// chunks and pushes them into a bounded MPMC work-stealing channel.
///
/// Batches carry an empty checkpoint delta so concurrent pipelines publishing to the same
/// index never conflict on checkpoints.
pub async fn spawn_local_batch_reader(
    input_path_opt: Option<Uri>,
    storage_resolver: StorageResolver,
    channel_capacity: usize,
) -> anyhow::Result<(LocalBatchReceiver, JoinHandle<anyhow::Result<()>>)> {
    let (batch_tx, batch_rx) = flume::bounded::<RawDocBatch>(channel_capacity.max(1));
    let runtime_handle = RuntimeType::NonBlocking.get_runtime_handle();

    let reader_handle = match input_path_opt {
        Some(uri) => {
            let mut file_reader = DocFileReader::from_uri(&storage_resolver, &uri, 0).await?;
            runtime_handle.spawn(async move {
                let progress = Progress::default();
                while !file_reader.is_eof() {
                    let mut batch_builder = BatchBuilder::new(SourceType::File);
                    file_reader
                        .read_batch_into(
                            BATCH_NUM_BYTES_LIMIT as usize,
                            &progress,
                            &mut batch_builder,
                        )
                        .await?;
                    if batch_builder.docs.is_empty() {
                        break;
                    }
                    if batch_tx.send_async(batch_builder.build()).await.is_err() {
                        break;
                    }
                }
                Ok(())
            })
        }
        None => runtime_handle.spawn_blocking(move || {
            let stdin = std::io::stdin();
            let mut locked = stdin.lock();
            read_sync_stream_into_channel(
                &mut locked,
                SourceType::Stdin,
                BATCH_NUM_BYTES_LIMIT as usize,
                &batch_tx,
            )
        }),
    };

    Ok((LocalBatchReceiver::new(batch_rx), reader_handle))
}

/// Reads newline-delimited documents from a synchronous `Read` (such as locked `stdin`) in
/// 256 KiB chunks directly into a `BytesMut` slab and dispatches 5 MiB zero-copy batches.
///
/// Using a single dedicated blocking task with `std::io::stdin().lock()` avoids
/// `tokio::io::stdin()`'s 8 KiB per-read `spawn_blocking` cap.
fn read_sync_stream_into_channel<R: Read>(
    reader: &mut R,
    source_type: SourceType,
    max_batch_bytes: usize,
    batch_tx: &flume::Sender<RawDocBatch>,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(max_batch_bytes + READ_CHUNK_SIZE);
    let mut reader_eof = false;

    loop {
        let cut_end = loop {
            if max_batch_bytes > 0 && buf.len() >= max_batch_bytes {
                let search_from = max_batch_bytes - 1;
                if let Some(rel_newline) = memchr::memchr(b'\n', &buf[search_from..]) {
                    break search_from + rel_newline + 1;
                }
            }
            if reader_eof {
                break buf.len();
            }
            if buf.capacity() - buf.len() < READ_CHUNK_SIZE {
                let target_cap =
                    (max_batch_bytes + READ_CHUNK_SIZE).max(buf.len() + READ_CHUNK_SIZE);
                buf.reserve(target_cap - buf.len());
            }
            let spare = buf.spare_capacity_mut();
            let to_read = spare.len().min(READ_CHUNK_SIZE);
            // Zero-initialize only the slice we pass to `Read::read` (safe, no unsafe needed).
            let write_len = buf.len();
            buf.resize(write_len + to_read, 0);
            let bytes_read = reader.read(&mut buf[write_len..write_len + to_read])?;
            buf.truncate(write_len + bytes_read);
            if bytes_read == 0 {
                reader_eof = true;
                break buf.len();
            }
        };

        if cut_end == 0 {
            break;
        }

        let chunk: Bytes = buf.split_to(cut_end).freeze();
        let mut batch_builder = BatchBuilder::new(source_type);
        let mut line_start = 0;
        for newline_pos in memchr::memchr_iter(b'\n', &chunk) {
            let line_end = newline_pos + 1;
            batch_builder.add_doc(chunk.slice(line_start..line_end));
            line_start = line_end;
        }
        if line_start < chunk.len() {
            batch_builder.add_doc(chunk.slice(line_start..));
        }
        if batch_tx.send(batch_builder.build()).is_err() {
            break;
        }
        if reader_eof && buf.is_empty() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::source::doc_file_reader::file_test_helpers::generate_index_doc_file;

    #[test]
    fn test_read_sync_stream_into_channel() {
        let mut data = Vec::new();
        for idx in 0..500 {
            data.extend_from_slice(format!("{{\"id\": {idx}}}\n").as_bytes());
        }
        data.extend_from_slice(b"{\"id\": 500}");
        let (batch_tx, batch_rx) = flume::bounded(4);
        let producer = std::thread::spawn(move || {
            let mut cursor = std::io::Cursor::new(data);
            read_sync_stream_into_channel(&mut cursor, SourceType::Stdin, 256, &batch_tx).unwrap();
        });

        let mut total_docs = 0usize;
        let mut num_batches = 0usize;
        while let Ok(batch) = batch_rx.recv() {
            total_docs += batch.docs.len();
            num_batches += 1;
        }
        producer.join().unwrap();
        assert!(num_batches > 1);
        assert_eq!(total_docs, 501);
    }

    #[tokio::test]
    async fn test_spawn_local_batch_reader_fanout() {
        let num_lines = 200;
        let temp_file = generate_index_doc_file(false, num_lines).await;
        let uri = Uri::from_str(temp_file.path().to_str().unwrap()).unwrap();
        let storage_resolver = StorageResolver::for_test();

        let (receiver, reader_handle) = spawn_local_batch_reader(Some(uri), storage_resolver, 4)
            .await
            .unwrap();

        let mut total_docs = 0usize;
        while let Ok(batch) = receiver.batch_rx.recv_async().await {
            total_docs += batch.docs.len();
        }
        reader_handle.await.unwrap().unwrap();
        assert_eq!(total_docs, num_lines);
    }
}
