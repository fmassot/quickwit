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

use std::io;

use bytes::{Bytes, BytesMut};
use quickwit_common::Progress;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::BatchBuilder;

/// Chunk size used for bulk async reads and gzip decompression buffers.
pub(crate) const READ_CHUNK_SIZE: usize = 256 * 1024;

/// High-throughput newline-delimited reader that reads large chunks directly into a
/// `BytesMut` slab and slices individual lines via SIMD `memchr`, avoiding per-line heap
/// allocations and extra buffer copies.
pub(crate) struct ChunkedLineReader<R> {
    reader: R,
    /// Unread bytes buffered from `reader`. Always begins at a line boundary between calls.
    buf: BytesMut,
    /// True once the underlying `reader` has returned `0` bytes (EOF).
    reader_eof: bool,
}

impl<R: AsyncRead + Unpin> ChunkedLineReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            buf: BytesMut::new(),
            reader_eof: false,
        }
    }

    /// Returns `true` once the underlying stream has reached EOF and all buffered bytes
    /// have been emitted.
    pub(crate) fn is_eof(&self) -> bool {
        self.reader_eof && self.buf.is_empty()
    }

    /// Skips exactly `num_bytes` bytes from the stream, returning an `UnexpectedEof` error
    /// if the stream ends early.
    pub(crate) async fn skip_exact(&mut self, mut num_bytes: usize) -> io::Result<()> {
        if num_bytes == 0 {
            return Ok(());
        }
        if !self.buf.is_empty() {
            let take = num_bytes.min(self.buf.len());
            let _ = self.buf.split_to(take);
            num_bytes -= take;
        }
        if num_bytes == 0 {
            return Ok(());
        }
        let mut scratch = vec![0u8; 64_000];
        while num_bytes > 0 {
            let to_read = num_bytes.min(scratch.len());
            let bytes_read = self.reader.read_exact(&mut scratch[..to_read]).await?;
            num_bytes -= bytes_read;
        }
        Ok(())
    }

    /// Reads up to one batch of complete newline-delimited lines into `batch_builder`,
    /// stopping at the first newline at or after `max_batch_bytes` (or at EOF).
    ///
    /// Returns the total number of bytes added to `batch_builder`.
    /// When the end of the returned batch coincides with stream EOF, this method peeks once
    /// so `is_eof()` is immediately `true` (avoiding a trailing empty batch).
    pub(crate) async fn read_batch_into(
        &mut self,
        max_batch_bytes: usize,
        source_progress: &Progress,
        batch_builder: &mut BatchBuilder,
    ) -> io::Result<usize> {
        if self.is_eof() {
            return Ok(0);
        }

        let mut reserved_full_batch = false;
        let cut_end = loop {
            if max_batch_bytes > 0 && self.buf.len() >= max_batch_bytes {
                let search_from = max_batch_bytes - 1;
                if let Some(rel_newline) = memchr::memchr(b'\n', &self.buf[search_from..]) {
                    break search_from + rel_newline + 1;
                }
            }
            if self.reader_eof {
                break self.buf.len();
            }

            // Avoid allocating a full 5 MiB slab for tiny inputs: read one 256 KiB chunk first,
            // and only reserve the full batch slab if the stream has more data.
            if !reserved_full_batch && !self.buf.is_empty() && max_batch_bytes > READ_CHUNK_SIZE {
                let target_cap = max_batch_bytes + READ_CHUNK_SIZE;
                let needed = target_cap.saturating_sub(self.buf.len());
                self.buf.reserve(needed);
                reserved_full_batch = true;
            } else if self.buf.capacity() - self.buf.len() < READ_CHUNK_SIZE {
                self.buf.reserve(READ_CHUNK_SIZE);
            }

            let bytes_read = source_progress
                .protect_future(self.reader.read_buf(&mut self.buf))
                .await?;
            if bytes_read == 0 {
                self.reader_eof = true;
                break self.buf.len();
            }
        };

        if cut_end == 0 {
            return Ok(0);
        }

        // Freeze the batch chunk before peeking so we don't keep a 5 MiB slab live if the
        // peek allocates a fresh buffer for subsequent data.
        let chunk: Bytes = self.buf.split_to(cut_end).freeze();

        // If we consumed every buffered byte and haven't observed EOF yet, peek once so that
        // exact-boundary files report `is_eof() == true` immediately on this batch.
        if self.buf.is_empty() && !self.reader_eof {
            self.buf.reserve(READ_CHUNK_SIZE);
            let bytes_read = source_progress
                .protect_future(self.reader.read_buf(&mut self.buf))
                .await?;
            if bytes_read == 0 {
                self.reader_eof = true;
            }
        }

        let mut line_start = 0;
        for newline_pos in memchr::memchr_iter(b'\n', &chunk) {
            let line_end = newline_pos + 1;
            batch_builder.add_doc(chunk.slice(line_start..line_end));
            line_start = line_end;
        }
        if line_start < chunk.len() {
            batch_builder.add_doc(chunk.slice(line_start..));
        }

        Ok(chunk.len())
    }

    /// Reads a single line (including its trailing `\n` if present) and reports whether the
    /// stream has reached EOF right after this line.
    #[cfg(test)]
    pub(crate) async fn read_line_and_peek(&mut self) -> io::Result<(Bytes, bool)> {
        if self.is_eof() {
            return Ok((Bytes::new(), true));
        }

        let mut scanned_len = 0;
        let line_end = loop {
            if let Some(rel_newline) = memchr::memchr(b'\n', &self.buf[scanned_len..]) {
                break scanned_len + rel_newline + 1;
            }
            scanned_len = self.buf.len();
            if self.reader_eof {
                break self.buf.len();
            }
            if self.buf.capacity() - self.buf.len() < READ_CHUNK_SIZE {
                self.buf.reserve(READ_CHUNK_SIZE);
            }
            let bytes_read = self.reader.read_buf(&mut self.buf).await?;
            if bytes_read == 0 {
                self.reader_eof = true;
                break self.buf.len();
            }
        };

        if line_end == 0 {
            return Ok((Bytes::new(), true));
        }

        let line = self.buf.split_to(line_end).freeze();
        if self.buf.is_empty() && !self.reader_eof {
            self.buf.reserve(READ_CHUNK_SIZE);
            let bytes_read = self.reader.read_buf(&mut self.buf).await?;
            if bytes_read == 0 {
                self.reader_eof = true;
            }
        }
        Ok((line, self.is_eof()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use quickwit_proto::metastore::SourceType;

    use super::*;

    #[tokio::test]
    async fn test_chunked_line_reader_empty() {
        let mut reader = ChunkedLineReader::new(Cursor::new(Vec::<u8>::new()));
        let progress = Progress::default();
        let mut batch = BatchBuilder::new(SourceType::Stdin);
        let bytes = reader
            .read_batch_into(1024, &progress, &mut batch)
            .await
            .unwrap();
        assert_eq!(bytes, 0);
        assert!(batch.docs.is_empty());
        assert!(reader.is_eof());
    }

    #[tokio::test]
    async fn test_chunked_line_reader_exact_boundary_and_no_trailing_nl() {
        let input = b"line001\nline002\nline003\n";
        let mut reader = ChunkedLineReader::new(Cursor::new(input.as_slice()));
        let progress = Progress::default();
        let mut batch = BatchBuilder::new(SourceType::File);
        let bytes = reader
            .read_batch_into(input.len(), &progress, &mut batch)
            .await
            .unwrap();
        assert_eq!(bytes, input.len());
        assert_eq!(batch.docs.len(), 3);
        assert!(reader.is_eof());

        let input_no_nl = b"first\nsecond";
        let mut reader_no_nl = ChunkedLineReader::new(Cursor::new(input_no_nl.as_slice()));
        let mut batch_no_nl = BatchBuilder::new(SourceType::File);
        let bytes_no_nl = reader_no_nl
            .read_batch_into(1024, &progress, &mut batch_no_nl)
            .await
            .unwrap();
        assert_eq!(bytes_no_nl, input_no_nl.len());
        assert_eq!(
            batch_no_nl.docs,
            vec![
                Bytes::from_static(b"first\n"),
                Bytes::from_static(b"second")
            ]
        );
        assert!(reader_no_nl.is_eof());
    }

    #[tokio::test]
    async fn test_chunked_line_reader_multi_batch_and_long_line() {
        let long_line = format!("{}\n", "a".repeat(READ_CHUNK_SIZE + 1234));
        let mut data = Vec::new();
        for idx in 0..10 {
            data.extend_from_slice(format!("doc-{idx:04}\n").as_bytes());
        }
        data.extend_from_slice(long_line.as_bytes());
        data.extend_from_slice(b"tail-line");

        let mut reader = ChunkedLineReader::new(Cursor::new(data.as_slice()));
        let progress = Progress::default();
        let mut all_docs = Vec::new();
        let mut num_batches = 0;
        while !reader.is_eof() {
            let mut batch = BatchBuilder::new(SourceType::File);
            reader
                .read_batch_into(32, &progress, &mut batch)
                .await
                .unwrap();
            all_docs.extend(batch.docs);
            num_batches += 1;
        }
        assert!(num_batches >= 3);
        assert_eq!(all_docs.len(), 12);
        assert_eq!(all_docs[0], Bytes::from_static(b"doc-0000\n"));
        assert_eq!(all_docs[10].len(), long_line.len());
        assert_eq!(all_docs[11], Bytes::from_static(b"tail-line"));
    }
}
