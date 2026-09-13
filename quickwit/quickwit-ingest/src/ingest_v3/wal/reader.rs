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

//! Reading one ingester's log: tail discovery, footers, blocks, and per-queue replay.

use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use quickwit_proto::types::QueueId;
use quickwit_storage::{Storage, StorageErrorKind};

use super::format::{
    self, TRAILER_LEN, WalBlock, WalBlockMeta, WalFooter, WalObjectHeader, decode_block,
    decode_footer, footer_range,
};
use super::{WalError, WalId, WalResult, parse_wal_object_path, wal_dir, wal_object_path};

/// Bytes speculatively read from the end of an object to get its footer in one request.
/// Footers are ~50 bytes per block, so this covers objects with a few hundred blocks.
const SPECULATIVE_TAIL_LEN: u64 = 16 * 1024;

/// A listed WAL object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalObjectRef {
    pub wal_id: WalId,
    pub num_bytes: u64,
}

/// A replayed record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub queue_id: QueueId,
    pub position: u64,
    pub record: Bytes,
}

/// Reads one ingester's log.
#[derive(Clone)]
pub struct WalReader {
    storage: Arc<dyn Storage>,
    ingester_id: String,
}

impl WalReader {
    pub fn new(storage: Arc<dyn Storage>, ingester_id: impl Into<String>) -> Self {
        Self {
            storage,
            ingester_id: ingester_id.into(),
        }
    }

    pub fn ingester_id(&self) -> &str {
        &self.ingester_id
    }

    /// Lists every object of the log, sorted by id. Requires a storage that supports `list`.
    pub async fn list(&self) -> WalResult<Vec<WalObjectRef>> {
        let mut objects = Vec::new();
        let mut stream = self.storage.list(&wal_dir(&self.ingester_id));
        while let Some(batch) = stream.next().await {
            for object_metadata in batch? {
                if let Some(wal_id) = parse_wal_object_path(&object_metadata.path) {
                    objects.push(WalObjectRef {
                        wal_id,
                        num_bytes: object_metadata.size.as_u64(),
                    });
                }
            }
        }
        objects.sort_by_key(|object| object.wal_id);
        Ok(objects)
    }

    /// Finds the highest existing id in the log, or `start_after` if no object above it exists.
    ///
    /// `start_after` must be a known lower bound such that all ids in `start_after + 1..=tail`
    /// exist (the fence protocol keeps live ids contiguous, and GC only deletes below the replay
    /// boundary). Uses only `exists` (HEAD): a parallel exponential probe to bracket the tail,
    /// then a binary search. `O(log N)` requests for a gap of `N` objects.
    pub async fn last_wal_id(&self, start_after: WalId) -> WalResult<WalId> {
        const ROUND_SIZE: u32 = 8;
        const MAX_EXP: u32 = 48;

        let base = start_after.0;
        let mut lo_offset: Option<u64> = None;
        let mut hi_offset: Option<u64> = None;
        let mut next_exp = 0u32;

        while hi_offset.is_none() {
            if next_exp >= MAX_EXP {
                return Err(WalError::Corrupted(format!(
                    "could not find the tail of WAL `{}` after probing 2^{MAX_EXP} ids",
                    self.ingester_id
                )));
            }
            let end_exp = (next_exp + ROUND_SIZE).min(MAX_EXP);
            let exps: Vec<u32> = (next_exp..end_exp).collect();
            let probes = exps
                .iter()
                .map(|&exp| self.exists(WalId(base + (1u64 << exp))));
            let results = futures::future::join_all(probes).await;
            for (exp, result) in exps.iter().zip(results) {
                let offset = 1u64 << exp;
                if result? {
                    lo_offset = Some(offset);
                } else {
                    hi_offset = Some(offset);
                    break;
                }
            }
            next_exp = end_exp;
        }
        let hi = hi_offset.expect("loop exits only with an upper bound");
        let Some(lo) = lo_offset else {
            return Ok(start_after);
        };
        // Invariant: base + lo exists, base + hi does not.
        let mut left = lo + 1;
        let mut right = hi;
        while left < right {
            let mid = left + (right - left) / 2;
            if self.exists(WalId(base + mid)).await? {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Ok(WalId(base + left - 1))
    }

    /// Whether the object exists.
    pub async fn exists(&self, wal_id: WalId) -> WalResult<bool> {
        let path = wal_object_path(&self.ingester_id, wal_id);
        Ok(self.storage.exists(&path).await?)
    }

    /// Reads the footer of an object. Pass `num_bytes` when known (from a listing) to save a
    /// HEAD request.
    pub async fn read_footer(&self, wal_id: WalId, num_bytes: Option<u64>) -> WalResult<WalFooter> {
        let path = wal_object_path(&self.ingester_id, wal_id);
        let num_bytes = match num_bytes {
            Some(num_bytes) => num_bytes,
            None => self.storage.file_num_bytes(&path).await?,
        };
        if num_bytes < TRAILER_LEN as u64 {
            return Err(WalError::Corrupted(format!(
                "WAL object `{}` is too short ({num_bytes} bytes)",
                path.display()
            )));
        }
        let tail_len = num_bytes.min(SPECULATIVE_TAIL_LEN);
        let tail = self
            .storage
            .get_slice(&path, (num_bytes - tail_len) as usize..num_bytes as usize)
            .await?;
        let footer_range = footer_range(num_bytes, &tail[tail.len() - TRAILER_LEN..])?;
        let footer_len = footer_range.end - footer_range.start;
        if footer_len + TRAILER_LEN <= tail.len() {
            return decode_footer(&tail[tail.len() - footer_len - TRAILER_LEN..]);
        }
        // Footer larger than the speculative read: fetch exactly what is needed.
        let footer_and_trailer = self
            .storage
            .get_slice(&path, footer_range.start..num_bytes as usize)
            .await?;
        decode_footer(&footer_and_trailer)
    }

    /// Reads and decodes one block.
    pub async fn read_block(
        &self,
        wal_id: WalId,
        block_meta: &WalBlockMeta,
    ) -> WalResult<Vec<Bytes>> {
        let path = wal_object_path(&self.ingester_id, wal_id);
        let block_bytes = self
            .storage
            .get_slice(&path, block_meta.byte_range())
            .await?;
        decode_block(block_meta, &block_bytes)
    }

    /// Downloads and decodes a whole object.
    pub async fn read_object(&self, wal_id: WalId) -> WalResult<(WalObjectHeader, Vec<WalBlock>)> {
        let path = wal_object_path(&self.ingester_id, wal_id);
        let bytes = self.storage.get_all(&path).await?;
        format::decode_wal_object(&bytes)
    }

    /// Returns the records of `queue_id` at positions `>= from_position_inclusive` found in
    /// objects `wal_ids`, in position order.
    ///
    /// Objects are scanned through their footers; only blocks that belong to the queue and
    /// overlap the requested positions are downloaded. A missing object in the range yields
    /// [`WalError::Storage`] with [`StorageErrorKind::NotFound`]: either the range is wrong or
    /// the object was garbage collected.
    pub async fn replay_queue(
        &self,
        queue_id: &str,
        from_position_inclusive: u64,
        wal_ids: RangeInclusive<u64>,
    ) -> WalResult<Vec<WalRecord>> {
        let mut records = Vec::new();
        for wal_id in wal_ids.map(WalId) {
            let footer = self.read_footer(wal_id, None).await?;
            for block_meta in &footer.blocks {
                if block_meta.queue_id != queue_id
                    || block_meta.is_queue_marker()
                    || block_meta.last_position() < from_position_inclusive
                {
                    continue;
                }
                let block_records = self.read_block(wal_id, block_meta).await?;
                for (i, record) in block_records.into_iter().enumerate() {
                    let position = block_meta.first_position + i as u64;
                    if position >= from_position_inclusive {
                        records.push(WalRecord {
                            queue_id: queue_id.to_string(),
                            position,
                            record,
                        });
                    }
                }
            }
        }
        Ok(records)
    }

    /// Deletes an object. Missing objects are not an error.
    pub async fn delete(&self, wal_id: WalId) -> WalResult<()> {
        let path = wal_object_path(&self.ingester_id, wal_id);
        match self.storage.delete(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::ingest_v3::wal::format::encode_wal_object;

    async fn write_object(
        storage: &RamStorage,
        ingester_id: &str,
        wal_id: u64,
        blocks: &[WalBlock],
    ) {
        let header = WalObjectHeader {
            ingester_id: ingester_id.to_string(),
            epoch: 1,
            generation_id: 0,
            wal_id: WalId(wal_id),
            is_fence: false,
        };
        let bytes = encode_wal_object(&header, blocks);
        storage
            .put(
                &wal_object_path(ingester_id, WalId(wal_id)),
                Box::new(bytes.to_vec()),
            )
            .await
            .unwrap();
    }

    fn block(queue_id: &str, first_position: u64, num_records: usize) -> WalBlock {
        WalBlock {
            queue_id: queue_id.to_string(),
            first_position,
            records: (0..num_records)
                .map(|i| Bytes::from(format!("{queue_id}:{}", first_position + i as u64)))
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_last_wal_id_probe() {
        let storage = Arc::new(RamStorage::default());
        let reader = WalReader::new(storage.clone(), "node");
        assert_eq!(reader.last_wal_id(WalId::ZERO).await.unwrap(), WalId::ZERO);

        for tail in [1u64, 2, 3, 7, 8, 9, 100, 257] {
            let storage = Arc::new(RamStorage::default());
            let reader = WalReader::new(storage.clone(), "node");
            for wal_id in 1..=tail {
                write_object(&storage, "node", wal_id, &[]).await;
            }
            assert_eq!(
                reader.last_wal_id(WalId::ZERO).await.unwrap(),
                WalId(tail),
                "tail={tail}"
            );
            // Any lower bound below the tail works too.
            assert_eq!(
                reader.last_wal_id(WalId(tail / 2)).await.unwrap(),
                WalId(tail)
            );
        }

        // Contiguity above the GC boundary: ids 1..=4 collected, 5..=9 live.
        let storage = Arc::new(RamStorage::default());
        let reader = WalReader::new(storage.clone(), "node");
        for wal_id in 5..=9 {
            write_object(&storage, "node", wal_id, &[]).await;
        }
        assert_eq!(reader.last_wal_id(WalId(4)).await.unwrap(), WalId(9));
        let listed = reader.list().await.unwrap();
        assert_eq!(listed.len(), 5);
        assert_eq!(listed[0].wal_id, WalId(5));
    }

    #[tokio::test]
    async fn test_read_footer_and_replay_queue() {
        let storage = Arc::new(RamStorage::default());
        let reader = WalReader::new(storage.clone(), "node");
        write_object(&storage, "node", 1, &[block("q1", 0, 3), block("q2", 0, 1)]).await;
        write_object(&storage, "node", 2, &[block("q1", 3, 2)]).await;
        write_object(&storage, "node", 3, &[block("q2", 1, 4), block("q1", 5, 1)]).await;

        let footer = reader.read_footer(WalId(3), None).await.unwrap();
        assert_eq!(footer.header.wal_id, WalId(3));
        assert_eq!(footer.blocks.len(), 2);

        let listed = reader.list().await.unwrap();
        let footer_with_len = reader
            .read_footer(listed[2].wal_id, Some(listed[2].num_bytes))
            .await
            .unwrap();
        assert_eq!(footer, footer_with_len);

        let records = reader.replay_queue("q1", 2, 1..=3).await.unwrap();
        let positions: Vec<u64> = records.iter().map(|record| record.position).collect();
        assert_eq!(positions, vec![2, 3, 4, 5]);
        assert_eq!(&records[0].record[..], b"q1:2");
        assert!(records.iter().all(|record| record.queue_id == "q1"));

        let records = reader.replay_queue("q2", 0, 1..=3).await.unwrap();
        let positions: Vec<u64> = records.iter().map(|record| record.position).collect();
        assert_eq!(positions, vec![0, 1, 2, 3, 4]);

        assert!(
            reader
                .replay_queue("q3", 0, 1..=3)
                .await
                .unwrap()
                .is_empty()
        );

        // Missing object in range.
        let error = reader.replay_queue("q1", 0, 1..=4).await.unwrap_err();
        assert!(error.is_storage_kind(StorageErrorKind::NotFound));

        reader.delete(WalId(1)).await.unwrap();
        reader.delete(WalId(1)).await.unwrap();
        assert!(!reader.exists(WalId(1)).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_footer_larger_than_speculative_tail() {
        let storage = Arc::new(RamStorage::default());
        let reader = WalReader::new(storage.clone(), "node");
        // ~50 bytes per footer entry; 1000 blocks > 16 KiB.
        let blocks: Vec<WalBlock> = (0..1000)
            .map(|i| block(&format!("queue-{i:04}"), 0, 1))
            .collect();
        write_object(&storage, "node", 1, &blocks).await;
        let footer = reader.read_footer(WalId(1), None).await.unwrap();
        assert_eq!(footer.blocks.len(), 1000);
    }
}
