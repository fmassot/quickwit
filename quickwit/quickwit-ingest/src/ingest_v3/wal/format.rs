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

//! WAL object format.
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │ header   : magic "QWL3" | version u8 | flags u8 | epoch u64          │
//! │            | generation_id u64 | wal_id u64                          │
//! │            | ingester_id_len u16 | ingester_id                       │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │ block 0  : queue_id_len u16 | queue_id | first_position u64          │
//! │            | num_records u32 | codec u8 | uncompressed_len u32       │
//! │            | payload_len u32 | payload | payload_crc32 u32           │
//! │ block 1  : ...                                                       │
//! │ block N                                                              │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │ footer   : header (again) | num_blocks u32                           │
//! │            | { queue_id_len u16 | queue_id | first_position u64      │
//! │            |   num_records u32 | byte_offset u64 | byte_len u32 }*   │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │ trailer  : footer_len u32 | footer_crc32 u32 | magic "3LWQ"          │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! All integers are little-endian. A block payload is the concatenation of
//! `record_len u32 | record_bytes`, optionally zstd-compressed. Records within a block occupy
//! consecutive positions `first_position..first_position + num_records` of their queue.
//!
//! The footer repeats the header so that reading the tail of an object (one range GET, size
//! known from a LIST) is enough to know who wrote it, in which epoch, and which queue positions
//! it holds. Recovery, GC and fence validation never need to download data blocks they do not
//! care about.
//!
//! A *fence* object has the `FENCE` flag set and no blocks.

use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use quickwit_proto::types::QueueId;

use super::{WalError, WalId, WalResult};

const HEAD_MAGIC: &[u8; 4] = b"QWL3";
const TAIL_MAGIC: &[u8; 4] = b"3LWQ";

/// Current format version.
pub const FORMAT_VERSION: u8 = 1;

/// Size of the fixed trailer at the end of every object.
pub const TRAILER_LEN: usize = 4 + 4 + 4;

const FLAG_FENCE: u8 = 1 << 0;

const CODEC_NONE: u8 = 0;
const CODEC_ZSTD: u8 = 1;

/// Payloads smaller than this are stored uncompressed.
const ZSTD_MIN_PAYLOAD_LEN: usize = 256;

/// Objects with at least this many record bytes have their blocks compressed in parallel.
const PARALLEL_ENCODE_MIN_LEN: usize = 1024 * 1024;
const ZSTD_LEVEL: i32 = 1;

/// Identifies who wrote an object and where it sits in the log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalObjectHeader {
    /// Node id of the ingester owning the log.
    pub ingester_id: String,
    /// Writer epoch. Strictly increases each time ownership of the log changes. Issued by the
    /// log itself: `epoch of the tail object + 1` at fencing time (see
    /// [`super::fence::fence_log`]).
    pub epoch: u64,
    /// Cluster generation id of the node that wrote the object (its start time in the
    /// cluster's view). Informational: ties objects to a node incarnation in logs and metrics.
    /// Not used for ordering.
    pub generation_id: u64,
    /// Id of this object within the log.
    pub wal_id: WalId,
    /// Whether this object is a fence (no data, closes the log for earlier epochs).
    pub is_fence: bool,
}

/// A run of consecutive records of one queue, or a *queue marker*: a block with no records
/// recording that the queue was created. Markers make a queue's existence durable even when it
/// never held a record (or all of them were collected), so that a restarted ingester recovers
/// the queue and the control plane's view stays consistent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalBlock {
    pub queue_id: QueueId,
    /// Position of the first record. Meaningless (0) for a queue marker.
    pub first_position: u64,
    /// Opaque records (ingest v2 `MRecord` encoding, or anything else). Empty for a marker.
    pub records: Vec<Bytes>,
}

impl WalBlock {
    /// A block recording that `queue_id` exists, holding no records.
    pub fn queue_marker(queue_id: QueueId) -> Self {
        Self {
            queue_id,
            first_position: 0,
            records: Vec::new(),
        }
    }

    pub fn is_queue_marker(&self) -> bool {
        self.records.is_empty()
    }

    /// Position of the last record. Panics on a queue marker.
    pub fn last_position(&self) -> u64 {
        assert!(!self.records.is_empty(), "queue markers have no positions");
        self.first_position + self.records.len() as u64 - 1
    }
}

/// Footer entry describing a block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalBlockMeta {
    pub queue_id: QueueId,
    pub first_position: u64,
    pub num_records: u32,
    /// Offset of the block within the object.
    pub byte_offset: u64,
    /// Length of the block within the object.
    pub byte_len: u32,
}

impl WalBlockMeta {
    pub fn is_queue_marker(&self) -> bool {
        self.num_records == 0
    }

    /// Position of the last record in the block. Panics on a queue marker.
    pub fn last_position(&self) -> u64 {
        assert!(self.num_records > 0, "queue markers have no positions");
        self.first_position + self.num_records as u64 - 1
    }

    /// Byte range of the block within the object.
    pub fn byte_range(&self) -> Range<usize> {
        let start = self.byte_offset as usize;
        start..start + self.byte_len as usize
    }
}

/// Decoded footer of a WAL object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalFooter {
    pub header: WalObjectHeader,
    pub blocks: Vec<WalBlockMeta>,
}

impl WalFooter {
    /// Whether the object holds records for `queue_id` at or after `position`.
    pub fn has_records_after(&self, queue_id: &str, position_inclusive: u64) -> bool {
        self.blocks.iter().any(|block_meta| {
            block_meta.queue_id == queue_id
                && !block_meta.is_queue_marker()
                && block_meta.last_position() >= position_inclusive
        })
    }
}

/// Encodes a WAL object.
pub fn encode_wal_object(header: &WalObjectHeader, blocks: &[WalBlock]) -> Bytes {
    let estimated_len: usize = blocks
        .iter()
        .map(|block| {
            block.queue_id.len()
                + 64
                + block
                    .records
                    .iter()
                    .map(|record| record.len() + 4)
                    .sum::<usize>()
        })
        .sum::<usize>()
        + 256;
    let mut buffer = BytesMut::with_capacity(estimated_len);

    let mut header = header.clone();
    header.is_fence = header.is_fence || blocks.is_empty();
    encode_header(&header, &mut buffer);

    // Blocks are independent: compress them in parallel when there is enough work.
    let total_len: usize = blocks
        .iter()
        .map(|block| block.records.iter().map(Bytes::len).sum::<usize>())
        .sum();
    let encoded_blocks: Vec<BytesMut> = if blocks.len() > 1 && total_len >= PARALLEL_ENCODE_MIN_LEN
    {
        use rayon::prelude::*;
        blocks
            .par_iter()
            .map(|block| {
                let mut block_buffer = BytesMut::new();
                encode_block(block, &mut block_buffer);
                block_buffer
            })
            .collect()
    } else {
        blocks
            .iter()
            .map(|block| {
                let mut block_buffer = BytesMut::new();
                encode_block(block, &mut block_buffer);
                block_buffer
            })
            .collect()
    };

    let mut block_metas = Vec::with_capacity(blocks.len());
    for (block, encoded_block) in blocks.iter().zip(encoded_blocks) {
        let byte_offset = buffer.len() as u64;
        buffer.put_slice(&encoded_block);
        let byte_len = (buffer.len() as u64 - byte_offset) as u32;
        block_metas.push(WalBlockMeta {
            queue_id: block.queue_id.clone(),
            first_position: block.first_position,
            num_records: block.records.len() as u32,
            byte_offset,
            byte_len,
        });
    }

    let footer_offset = buffer.len();
    encode_header(&header, &mut buffer);
    buffer.put_u32_le(block_metas.len() as u32);
    for block_meta in &block_metas {
        put_str(&mut buffer, &block_meta.queue_id);
        buffer.put_u64_le(block_meta.first_position);
        buffer.put_u32_le(block_meta.num_records);
        buffer.put_u64_le(block_meta.byte_offset);
        buffer.put_u32_le(block_meta.byte_len);
    }
    let footer_len = buffer.len() - footer_offset;
    let footer_crc = crc32fast::hash(&buffer[footer_offset..]);
    buffer.put_u32_le(footer_len as u32);
    buffer.put_u32_le(footer_crc);
    buffer.put_slice(TAIL_MAGIC);
    buffer.freeze()
}

/// Encodes a fence object.
pub fn encode_fence_object(
    ingester_id: &str,
    epoch: u64,
    generation_id: u64,
    wal_id: WalId,
) -> Bytes {
    let header = WalObjectHeader {
        ingester_id: ingester_id.to_string(),
        epoch,
        generation_id,
        wal_id,
        is_fence: true,
    };
    encode_wal_object(&header, &[])
}

/// Decodes the trailer (last [`TRAILER_LEN`] bytes) and returns the byte range of the footer
/// within the object.
pub fn footer_range(object_len: u64, trailer: &[u8]) -> WalResult<Range<usize>> {
    if trailer.len() != TRAILER_LEN {
        return Err(WalError::Corrupted(format!(
            "expected {TRAILER_LEN}-byte trailer, got {} bytes",
            trailer.len()
        )));
    }
    let mut cursor = trailer;
    let footer_len = cursor.get_u32_le() as u64;
    let _footer_crc = cursor.get_u32_le();
    if cursor != TAIL_MAGIC {
        return Err(WalError::Corrupted("bad trailer magic".to_string()));
    }
    let footer_end = object_len - TRAILER_LEN as u64;
    if footer_len > footer_end {
        return Err(WalError::Corrupted(format!(
            "footer length {footer_len} exceeds object length {object_len}"
        )));
    }
    Ok((footer_end - footer_len) as usize..footer_end as usize)
}

/// Decodes a footer given the bytes `footer | trailer` (i.e. the last `footer_len + TRAILER_LEN`
/// bytes of the object). Verifies the footer checksum.
pub fn decode_footer(footer_and_trailer: &[u8]) -> WalResult<WalFooter> {
    if footer_and_trailer.len() < TRAILER_LEN {
        return Err(WalError::Corrupted("object too short".to_string()));
    }
    let (footer_bytes, trailer) =
        footer_and_trailer.split_at(footer_and_trailer.len() - TRAILER_LEN);
    let mut cursor = trailer;
    let footer_len = cursor.get_u32_le() as usize;
    let footer_crc = cursor.get_u32_le();
    if cursor != TAIL_MAGIC {
        return Err(WalError::Corrupted("bad trailer magic".to_string()));
    }
    if footer_len > footer_bytes.len() {
        return Err(WalError::Corrupted(
            "footer length exceeds provided bytes".to_string(),
        ));
    }
    let footer_bytes = &footer_bytes[footer_bytes.len() - footer_len..];
    if crc32fast::hash(footer_bytes) != footer_crc {
        return Err(WalError::Corrupted("footer checksum mismatch".to_string()));
    }
    let mut reader = Reader(footer_bytes);
    let header = decode_header(&mut reader)?;
    let num_blocks = reader.u32()? as usize;
    let mut blocks = Vec::with_capacity(num_blocks);
    for _ in 0..num_blocks {
        let queue_id = reader.str()?;
        let first_position = reader.u64()?;
        let num_records = reader.u32()?;
        let byte_offset = reader.u64()?;
        let byte_len = reader.u32()?;
        blocks.push(WalBlockMeta {
            queue_id,
            first_position,
            num_records,
            byte_offset,
            byte_len,
        });
    }
    if !reader.0.is_empty() {
        return Err(WalError::Corrupted("trailing bytes in footer".to_string()));
    }
    if header.is_fence && !blocks.is_empty() {
        return Err(WalError::Corrupted("fence object with blocks".to_string()));
    }
    Ok(WalFooter { header, blocks })
}

/// Decodes the records of a block from its bytes (the slice described by
/// [`WalBlockMeta::byte_range`]). Verifies the payload checksum and the block metadata.
pub fn decode_block(block_meta: &WalBlockMeta, block_bytes: &[u8]) -> WalResult<Vec<Bytes>> {
    let block = decode_block_inner(&mut Reader(block_bytes))?;
    if block.queue_id != block_meta.queue_id
        || block.first_position != block_meta.first_position
        || block.records.len() as u32 != block_meta.num_records
    {
        return Err(WalError::Corrupted(format!(
            "block does not match footer entry for queue `{}` at position {}",
            block_meta.queue_id, block_meta.first_position
        )));
    }
    Ok(block.records)
}

/// Decodes a whole object. Used for replay of small objects and in tests; production paths
/// prefer [`decode_footer`] + [`decode_block`].
pub fn decode_wal_object(bytes: &[u8]) -> WalResult<(WalObjectHeader, Vec<WalBlock>)> {
    let footer = decode_footer(tail_for_footer(bytes)?)?;
    let mut blocks = Vec::with_capacity(footer.blocks.len());
    for block_meta in &footer.blocks {
        let range = block_meta.byte_range();
        let block_bytes = bytes
            .get(range)
            .ok_or_else(|| WalError::Corrupted("block range out of bounds".to_string()))?;
        let records = decode_block(block_meta, block_bytes)?;
        blocks.push(WalBlock {
            queue_id: block_meta.queue_id.clone(),
            first_position: block_meta.first_position,
            records,
        });
    }
    let mut head_reader = Reader(bytes);
    let head = decode_header(&mut head_reader)?;
    if head != footer.header {
        return Err(WalError::Corrupted(
            "object header does not match footer header".to_string(),
        ));
    }
    Ok((footer.header, blocks))
}

fn tail_for_footer(bytes: &[u8]) -> WalResult<&[u8]> {
    if bytes.len() < TRAILER_LEN {
        return Err(WalError::Corrupted("object too short".to_string()));
    }
    let range = footer_range(bytes.len() as u64, &bytes[bytes.len() - TRAILER_LEN..])?;
    Ok(&bytes[range.start..])
}

fn encode_header(header: &WalObjectHeader, buffer: &mut BytesMut) {
    buffer.put_slice(HEAD_MAGIC);
    buffer.put_u8(FORMAT_VERSION);
    buffer.put_u8(if header.is_fence { FLAG_FENCE } else { 0 });
    buffer.put_u64_le(header.epoch);
    buffer.put_u64_le(header.generation_id);
    buffer.put_u64_le(header.wal_id.0);
    put_str(buffer, &header.ingester_id);
}

fn decode_header(reader: &mut Reader<'_>) -> WalResult<WalObjectHeader> {
    if reader.bytes(4)? != HEAD_MAGIC {
        return Err(WalError::Corrupted("bad header magic".to_string()));
    }
    let version = reader.u8()?;
    if version != FORMAT_VERSION {
        return Err(WalError::Corrupted(format!(
            "unsupported WAL format version {version}"
        )));
    }
    let flags = reader.u8()?;
    let epoch = reader.u64()?;
    let generation_id = reader.u64()?;
    let wal_id = WalId(reader.u64()?);
    let ingester_id = reader.str()?;
    Ok(WalObjectHeader {
        ingester_id,
        epoch,
        generation_id,
        wal_id,
        is_fence: flags & FLAG_FENCE != 0,
    })
}

fn encode_block(block: &WalBlock, buffer: &mut BytesMut) {
    let uncompressed_len: usize = block.records.iter().map(|record| 4 + record.len()).sum();
    let mut payload = Vec::with_capacity(uncompressed_len);
    for record in &block.records {
        payload.extend_from_slice(&(record.len() as u32).to_le_bytes());
        payload.extend_from_slice(record);
    }
    let (codec, payload) = if payload.len() >= ZSTD_MIN_PAYLOAD_LEN {
        match zstd::bulk::compress(&payload, ZSTD_LEVEL) {
            Ok(compressed) if compressed.len() < payload.len() => (CODEC_ZSTD, compressed),
            _ => (CODEC_NONE, payload),
        }
    } else {
        (CODEC_NONE, payload)
    };
    put_str(buffer, &block.queue_id);
    buffer.put_u64_le(block.first_position);
    buffer.put_u32_le(block.records.len() as u32);
    buffer.put_u8(codec);
    buffer.put_u32_le(uncompressed_len as u32);
    buffer.put_u32_le(payload.len() as u32);
    buffer.put_slice(&payload);
    buffer.put_u32_le(crc32fast::hash(&payload));
}

fn decode_block_inner(reader: &mut Reader<'_>) -> WalResult<WalBlock> {
    let queue_id = reader.str()?;
    let first_position = reader.u64()?;
    let num_records = reader.u32()? as usize;
    let codec = reader.u8()?;
    let uncompressed_len = reader.u32()? as usize;
    let payload_len = reader.u32()? as usize;
    let payload = reader.bytes(payload_len)?;
    let payload_crc = reader.u32()?;
    if crc32fast::hash(payload) != payload_crc {
        return Err(WalError::Corrupted(format!(
            "block checksum mismatch for queue `{queue_id}` at position {first_position}"
        )));
    }
    let decompressed: Vec<u8> = match codec {
        CODEC_NONE => payload.to_vec(),
        CODEC_ZSTD => zstd::bulk::decompress(payload, uncompressed_len)
            .map_err(|error| WalError::Corrupted(format!("zstd decompression failed: {error}")))?,
        _ => {
            return Err(WalError::Corrupted(format!("unknown codec {codec}")));
        }
    };
    if decompressed.len() != uncompressed_len {
        return Err(WalError::Corrupted(
            "uncompressed length mismatch".to_string(),
        ));
    }
    let decompressed = Bytes::from(decompressed);
    let mut records = Vec::with_capacity(num_records);
    let mut offset = 0usize;
    for _ in 0..num_records {
        let len_bytes = decompressed
            .get(offset..offset + 4)
            .ok_or_else(|| WalError::Corrupted("truncated record length".to_string()))?;
        let record_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        offset += 4;
        if offset + record_len > decompressed.len() {
            return Err(WalError::Corrupted("truncated record".to_string()));
        }
        records.push(decompressed.slice(offset..offset + record_len));
        offset += record_len;
    }
    if offset != decompressed.len() {
        return Err(WalError::Corrupted("trailing bytes in block".to_string()));
    }
    if !reader.0.is_empty() {
        return Err(WalError::Corrupted(
            "trailing bytes after block".to_string(),
        ));
    }
    Ok(WalBlock {
        queue_id,
        first_position,
        records,
    })
}

fn put_str(buffer: &mut BytesMut, value: &str) {
    assert!(
        value.len() <= u16::MAX as usize,
        "string too long for WAL format"
    );
    buffer.put_u16_le(value.len() as u16);
    buffer.put_slice(value.as_bytes());
}

/// Bounds-checked little-endian reader.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bytes(&mut self, len: usize) -> WalResult<&'a [u8]> {
        if self.0.len() < len {
            return Err(WalError::Corrupted("unexpected end of data".to_string()));
        }
        let (head, tail) = self.0.split_at(len);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> WalResult<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> WalResult<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> WalResult<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn str(&mut self) -> WalResult<String> {
        let len = u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()) as usize;
        let bytes = self.bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| WalError::Corrupted("invalid UTF-8 string".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(wal_id: u64) -> WalObjectHeader {
        WalObjectHeader {
            ingester_id: "ingester-1".to_string(),
            epoch: 7,
            generation_id: 70,
            wal_id: WalId(wal_id),
            is_fence: false,
        }
    }

    fn block(queue_id: &str, first_position: u64, records: &[&str]) -> WalBlock {
        WalBlock {
            queue_id: queue_id.to_string(),
            first_position,
            records: records
                .iter()
                .map(|record| Bytes::copy_from_slice(record.as_bytes()))
                .collect(),
        }
    }

    #[test]
    fn test_roundtrip_small_and_large_blocks() {
        let big_record = "x".repeat(10_000);
        let blocks = vec![
            block("idx/src/00000000000000000000", 0, &["a", "b", "c"]),
            block("idx/src/00000000000000000001", 41, &[&big_record, "tail"]),
            block("idx/src/00000000000000000000", 3, &["d"]),
        ];
        let encoded = encode_wal_object(&header(1), &blocks);
        // Compression kicked in for the large block.
        assert!(encoded.len() < 10_000);

        let (decoded_header, decoded_blocks) = decode_wal_object(&encoded).unwrap();
        assert_eq!(decoded_header, header(1));
        assert_eq!(decoded_blocks, blocks);

        let footer = decode_footer(&encoded[encoded.len() - 400.min(encoded.len())..]).unwrap();
        assert_eq!(footer.header, header(1));
        assert_eq!(footer.blocks.len(), 3);
        assert_eq!(footer.blocks[1].last_position(), 42);
        assert!(footer.has_records_after("idx/src/00000000000000000000", 3));
        assert!(!footer.has_records_after("idx/src/00000000000000000000", 4));
        assert!(!footer.has_records_after("idx/src/00000000000000000009", 0));

        // Range-read one block using footer metadata only.
        let block_meta = &footer.blocks[1];
        let records = decode_block(block_meta, &encoded[block_meta.byte_range()]).unwrap();
        assert_eq!(records[0].len(), 10_000);
        assert_eq!(&records[1][..], b"tail");
    }

    #[test]
    fn test_footer_range_from_trailer() {
        let encoded = encode_wal_object(&header(1), &[block("q", 0, &["a"])]);
        let trailer = &encoded[encoded.len() - TRAILER_LEN..];
        let range = footer_range(encoded.len() as u64, trailer).unwrap();
        let footer = decode_footer(&encoded[range.start..]).unwrap();
        assert_eq!(footer.blocks.len(), 1);
    }

    #[test]
    fn test_fence_object() {
        let encoded = encode_fence_object("ingester-1", 9, 90, WalId(12));
        let (decoded_header, blocks) = decode_wal_object(&encoded).unwrap();
        assert!(decoded_header.is_fence);
        assert_eq!(decoded_header.epoch, 9);
        assert_eq!(decoded_header.generation_id, 90);
        assert_eq!(decoded_header.wal_id, WalId(12));
        assert!(blocks.is_empty());
        assert!(encoded.len() < 128);
    }

    #[test]
    fn test_corruption_is_detected() {
        let encoded = encode_wal_object(&header(1), &[block("q", 0, &["hello", "world"])]);

        // Flip a byte in the payload.
        let mut corrupted = encoded.to_vec();
        let footer = decode_footer(&encoded).unwrap();
        let payload_offset = footer.blocks[0].byte_offset as usize + 2 + 1 + 8 + 4 + 1 + 4 + 4;
        corrupted[payload_offset] ^= 0xff;
        let error = decode_wal_object(&corrupted).unwrap_err();
        assert!(matches!(error, WalError::Corrupted(msg) if msg.contains("checksum")));

        // Flip a byte in the footer.
        let mut corrupted = encoded.to_vec();
        let len = corrupted.len();
        corrupted[len - TRAILER_LEN - 3] ^= 0xff;
        assert!(matches!(
            decode_footer(&corrupted),
            Err(WalError::Corrupted(_))
        ));

        // Truncate.
        assert!(matches!(
            decode_footer(&encoded[..encoded.len() - 1]),
            Err(WalError::Corrupted(_))
        ));
        assert!(matches!(
            decode_footer(&encoded[..5]),
            Err(WalError::Corrupted(_))
        ));
    }

    #[test]
    fn test_queue_marker_roundtrip() {
        let blocks = vec![
            WalBlock::queue_marker("q-new".to_string()),
            block("q", 5, &["a"]),
        ];
        let encoded = encode_wal_object(&header(1), &blocks);
        let (decoded_header, decoded_blocks) = decode_wal_object(&encoded).unwrap();
        assert!(!decoded_header.is_fence);
        assert_eq!(decoded_blocks, blocks);
        let footer = decode_footer(&encoded).unwrap();
        assert!(footer.blocks[0].is_queue_marker());
        assert!(!footer.blocks[1].is_queue_marker());
        assert!(!footer.has_records_after("q-new", 0));
        assert!(footer.has_records_after("q", 5));
    }
}
