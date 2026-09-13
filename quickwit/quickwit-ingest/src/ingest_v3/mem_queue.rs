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

//! In-memory per-shard record buffer for ingest v3.
//!
//! With the object-store WAL providing durability, the local copy of a shard's records only
//! serves fetch streams until the records are indexed and the shard truncated. It needs no
//! disk, and it needs no node-wide lock: each queue has its own mutex, held for microseconds
//! and never across an `.await`. A node-wide atomic tracks the total buffered bytes for
//! backpressure.
//!
//! Positions are the same as mrecordlog's: consecutive `u64`s from 0, assigned at append.

use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

/// Node-wide accounting of buffered bytes, shared by all queues of an ingester.
#[derive(Debug, Default)]
pub struct MemQueueUsage {
    num_bytes: AtomicU64,
    num_records: AtomicU64,
}

impl MemQueueUsage {
    pub fn num_bytes(&self) -> u64 {
        self.num_bytes.load(Ordering::Relaxed)
    }

    pub fn num_records(&self) -> u64 {
        self.num_records.load(Ordering::Relaxed)
    }
}

struct Inner {
    /// Position of `records[0]`.
    first_position: u64,
    /// Position the next appended record gets.
    next_position: u64,
    records: VecDeque<Bytes>,
    num_bytes: u64,
}

/// One shard's records, in memory.
pub struct MemQueue {
    inner: std::sync::Mutex<Inner>,
    usage: Arc<MemQueueUsage>,
}

impl std::fmt::Debug for MemQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("MemQueue")
            .field("first_position", &inner.first_position)
            .field("next_position", &inner.next_position)
            .field("num_records", &inner.records.len())
            .field("num_bytes", &inner.num_bytes)
            .finish()
    }
}

impl MemQueue {
    /// An empty queue whose next record gets position `next_position`.
    pub fn new(usage: Arc<MemQueueUsage>, next_position: u64) -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner {
                first_position: next_position,
                next_position,
                records: VecDeque::new(),
                num_bytes: 0,
            }),
            usage,
        }
    }

    /// Appends records at consecutive positions and returns the position of the last one.
    /// `records` must not be empty.
    pub fn append(&self, records: impl IntoIterator<Item = Bytes>) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let mut num_bytes = 0u64;
        let mut num_records = 0u64;
        for record in records {
            num_bytes += record.len() as u64;
            num_records += 1;
            inner.records.push_back(record);
        }
        assert!(num_records > 0, "cannot append zero records");
        inner.next_position += num_records;
        inner.num_bytes += num_bytes;
        let last_position = inner.next_position - 1;
        drop(inner);
        self.usage.num_bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.usage
            .num_records
            .fetch_add(num_records, Ordering::Relaxed);
        last_position
    }

    /// Appends a record at an explicit position, for replay. Positions must not go backwards;
    /// gaps are allowed (they advance `next_position`). Returns `false` if the record is at or
    /// before an already present position (already replayed / truncated).
    pub fn append_at(&self, position: u64, record: Bytes) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if position < inner.next_position {
            return false;
        }
        if inner.records.is_empty() {
            inner.first_position = position;
        } else if position > inner.next_position {
            // Gap: nothing to store for the missing positions, but `range` must not report
            // records at the wrong positions. Fill with empty placeholders, which `range`
            // skips.
            let gap = position - inner.next_position;
            for _ in 0..gap {
                inner.records.push_back(Bytes::new());
            }
        }
        let num_bytes = record.len() as u64;
        inner.records.push_back(record);
        inner.next_position = position + 1;
        inner.num_bytes += num_bytes;
        drop(inner);
        self.usage.num_bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.usage.num_records.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Returns `(position, record)` pairs within `range`, in order, stopping before the record
    /// that would take the total past `max_num_bytes` (at least one record is returned if any
    /// is in range).
    pub fn range<R: RangeBounds<u64>>(&self, range: R, max_num_bytes: usize) -> Vec<(u64, Bytes)> {
        let inner = self.inner.lock().unwrap();
        let start = match range.start_bound() {
            Bound::Included(&p) => p,
            Bound::Excluded(&p) => p + 1,
            Bound::Unbounded => inner.first_position,
        }
        .max(inner.first_position);
        let end = match range.end_bound() {
            Bound::Included(&p) => p.saturating_add(1),
            Bound::Excluded(&p) => p,
            Bound::Unbounded => inner.next_position,
        }
        .min(inner.next_position);
        if start >= end {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut total = 0usize;
        for position in start..end {
            let record = &inner.records[(position - inner.first_position) as usize];
            if record.is_empty() {
                // Placeholder for a gap.
                continue;
            }
            if !out.is_empty() && total + record.len() > max_num_bytes {
                break;
            }
            total += record.len();
            out.push((position, record.clone()));
        }
        out
    }

    /// Drops every record at or before `position_inclusive`.
    pub fn truncate(&self, position_inclusive: u64) {
        let mut inner = self.inner.lock().unwrap();
        let mut freed_bytes = 0u64;
        let mut freed_records = 0u64;
        while inner.first_position <= position_inclusive {
            let Some(record) = inner.records.pop_front() else {
                // Truncating past the end: remember the position so that the next append
                // continues after it.
                inner.first_position = position_inclusive + 1;
                inner.next_position = inner.next_position.max(position_inclusive + 1);
                break;
            };
            if !record.is_empty() {
                freed_bytes += record.len() as u64;
                freed_records += 1;
            }
            inner.first_position += 1;
        }
        inner.num_bytes -= freed_bytes;
        drop(inner);
        self.usage
            .num_bytes
            .fetch_sub(freed_bytes, Ordering::Relaxed);
        self.usage
            .num_records
            .fetch_sub(freed_records, Ordering::Relaxed);
    }

    /// Position the next appended record gets.
    pub fn next_position(&self) -> u64 {
        self.inner.lock().unwrap().next_position
    }

    /// Position of the last record, if any record was ever appended.
    pub fn last_position(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.next_position.checked_sub(1)
    }

    /// Position of the first retained record (== `next_position` when empty).
    pub fn first_position(&self) -> u64 {
        self.inner.lock().unwrap().first_position
    }

    pub fn num_bytes(&self) -> u64 {
        self.inner.lock().unwrap().num_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().records.is_empty()
    }

    /// Releases the accounting of every record (the queue is being deleted).
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        let num_records = inner.records.iter().filter(|r| !r.is_empty()).count() as u64;
        let num_bytes = inner.num_bytes;
        inner.records.clear();
        inner.first_position = inner.next_position;
        inner.num_bytes = 0;
        drop(inner);
        self.usage.num_bytes.fetch_sub(num_bytes, Ordering::Relaxed);
        self.usage
            .num_records
            .fetch_sub(num_records, Ordering::Relaxed);
    }
}

impl Drop for MemQueue {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn test_append_range_truncate() {
        let usage = Arc::new(MemQueueUsage::default());
        let queue = MemQueue::new(usage.clone(), 0);
        assert!(queue.is_empty());
        assert_eq!(queue.last_position(), None);

        assert_eq!(queue.append([b("a"), b("bb")]), 1);
        assert_eq!(queue.append([b("ccc")]), 2);
        assert_eq!(usage.num_bytes(), 6);
        assert_eq!(usage.num_records(), 3);

        let all = queue.range(.., usize::MAX);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], (0, b("a")));
        assert_eq!(all[2], (2, b("ccc")));
        assert_eq!(queue.range(1.., usize::MAX).len(), 2);
        assert_eq!(queue.range(1..2, usize::MAX), vec![(1, b("bb"))]);
        assert!(queue.range(3.., usize::MAX).is_empty());
        // Byte cap: at least one, then stop before exceeding.
        assert_eq!(queue.range(.., 1).len(), 1);
        assert_eq!(queue.range(.., 3).len(), 2);

        queue.truncate(1);
        assert_eq!(queue.first_position(), 2);
        assert_eq!(queue.range(.., usize::MAX), vec![(2, b("ccc"))]);
        assert_eq!(usage.num_bytes(), 3);
        // Positions keep increasing after truncation.
        assert_eq!(queue.append([b("d")]), 3);
        // Truncating everything, including past the end.
        queue.truncate(10);
        assert!(queue.is_empty());
        assert_eq!(queue.next_position(), 11);
        assert_eq!(usage.num_bytes(), 0);
        assert_eq!(queue.append([b("e")]), 11);
        drop(queue);
        assert_eq!(usage.num_bytes(), 0);
        assert_eq!(usage.num_records(), 0);
    }

    #[test]
    fn test_append_at_for_replay() {
        let usage = Arc::new(MemQueueUsage::default());
        let queue = MemQueue::new(usage.clone(), 0);
        assert!(queue.append_at(5, b("f")));
        assert!(queue.append_at(6, b("g")));
        // Already present.
        assert!(!queue.append_at(6, b("dup")));
        // Gap.
        assert!(queue.append_at(9, b("j")));
        let all = queue.range(.., usize::MAX);
        assert_eq!(all, vec![(5, b("f")), (6, b("g")), (9, b("j"))]);
        assert_eq!(queue.next_position(), 10);
        assert_eq!(usage.num_records(), 3);
        queue.truncate(7);
        assert_eq!(queue.range(.., usize::MAX), vec![(9, b("j"))]);
        assert_eq!(queue.append([b("k")]), 10);
    }
}
