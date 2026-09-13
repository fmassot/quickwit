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

//! Ingest v3: a write-ahead log stored on object storage.
//!
//! Ingest v2 gets durability from replicating a local (`mrecordlog`) WAL to a follower
//! ingester. Ingest v3 instead makes acknowledged data durable by writing it to object
//! storage, so that any ingester can be killed at any time and its shards recovered elsewhere.
//!
//! The design is described in `quickwit-object-store-wal.md`. In short:
//!
//! - One log per ingester: `<ingest_wal_uri>/<ingester_id>/wal/<wal_id:020>.wal`. Every object
//!   multiplexes the records of every shard the ingester leads.
//! - Records are buffered in memory and flushed as one object per flush interval (or once the
//!   buffer reaches a size threshold). One flush is in flight at a time, so `wal_id` order is
//!   position order. Requests are acknowledged once the object that contains them is durable.
//! - Objects are written with create-if-absent ([`quickwit_storage::Storage::put_if_absent`]). A
//!   writer that observes `AlreadyExists` for an object it did not write has been fenced.
//! - Taking over a dead ingester's log = write an empty *fence* object at `tail + 1`, which closes
//!   the log, then replay from it.
//!
//! This module is gated behind the `QW_ENABLE_INGEST_V3` environment variable while it is being
//! developed and is not wired into the ingester yet.

pub mod gc;
pub mod mem_queue;
pub mod metrics;
pub mod object_wal;
pub mod wal;

use std::time::Duration;

use bytesize::ByteSize;
pub use object_wal::{ObjectWal, OpenedObjectWal};
use quickwit_common::get_bool_from_env;
use quickwit_common::uri::Uri;

/// Environment variable enabling the ingest v3 object-store WAL.
pub const ENABLE_INGEST_V3_ENV_KEY: &str = "QW_ENABLE_INGEST_V3";

/// Environment variable holding the URI of the object store where WAL objects are written.
/// Cluster-level: WAL objects multiplex several indexes and cannot live under an `index_uri`.
pub const INGEST_WAL_URI_ENV_KEY: &str = "QW_INGEST_WAL_URI";

/// Environment variable overriding the WAL flush interval, in milliseconds.
pub const INGEST_WAL_FLUSH_INTERVAL_MS_ENV_KEY: &str = "QW_INGEST_WAL_FLUSH_INTERVAL_MS";

/// Environment variable overriding the size of records at which an object is written without
/// waiting for the flush interval (e.g. `16MiB`). Under load this is what sets the object size,
/// hence the PUT count per byte ingested.
pub const INGEST_WAL_FLUSH_NUM_BYTES_ENV_KEY: &str = "QW_INGEST_WAL_FLUSH_NUM_BYTES";

/// Environment variable overriding how long an ingester must be absent before indexers drain
/// its shards from its object WAL, in seconds.
pub const INGEST_WAL_FETCH_FALLBACK_GRACE_SECS_ENV_KEY: &str =
    "QW_INGEST_WAL_FETCH_FALLBACK_GRACE_SECS";

/// Environment variable overriding the minimum age of a WAL object before the janitor may
/// delete it, in seconds.
pub const INGEST_WAL_GC_MIN_AGE_SECS_ENV_KEY: &str = "QW_INGEST_WAL_GC_MIN_AGE_SECS";

/// Returns whether ingest v3 is enabled via [`ENABLE_INGEST_V3_ENV_KEY`].
pub fn is_ingest_v3_enabled() -> bool {
    get_bool_from_env(ENABLE_INGEST_V3_ENV_KEY, false)
}

/// Runtime configuration of the ingest v3 WAL.
#[derive(Clone, Debug)]
pub struct IngestV3Config {
    /// Object store URI where WAL objects are written.
    pub wal_uri: Uri,
    /// Maximum time records wait in memory before an object is written. Bounds the
    /// acknowledgement latency and the object-store PUT rate (`1 / flush_interval` per
    /// ingester while there is traffic).
    pub flush_interval: Duration,
    /// Once the buffer holds this many bytes, an object is written without waiting for the
    /// interval. Under load this makes object size, not the timer, drive the flush.
    pub flush_num_bytes: ByteSize,
    /// Maximum number of bytes buffered in memory and not yet durable. Beyond it, appends are
    /// rejected with [`wal::WalError::BufferFull`] until a flush completes (backpressure).
    pub max_buffered_num_bytes: ByteSize,
    /// Indexer side: how long an ingester must be missing from the cluster before the fetch
    /// stream fences its log and drains the shard from object storage.
    pub fetch_fallback_grace: Duration,
    /// Janitor side: objects younger than this are never deleted by the cluster-wide GC.
    pub gc_min_age: Duration,
}

impl IngestV3Config {
    /// Reads the configuration from the environment. Returns `None` if ingest v3 is disabled.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        if !is_ingest_v3_enabled() {
            return Ok(None);
        }
        let wal_uri_str = std::env::var(INGEST_WAL_URI_ENV_KEY).map_err(|_| {
            anyhow::anyhow!(
                "`{ENABLE_INGEST_V3_ENV_KEY}` is set but `{INGEST_WAL_URI_ENV_KEY}` is missing"
            )
        })?;
        let wal_uri: Uri = wal_uri_str.parse()?;
        let flush_interval_ms: u64 =
            quickwit_common::get_from_env(INGEST_WAL_FLUSH_INTERVAL_MS_ENV_KEY, 250u64, false);
        let fetch_fallback_grace_secs: u64 = quickwit_common::get_from_env(
            INGEST_WAL_FETCH_FALLBACK_GRACE_SECS_ENV_KEY,
            60u64,
            false,
        );
        let mut config = Self::with_wal_uri(wal_uri);
        config.flush_interval = Duration::from_millis(flush_interval_ms);
        if let Some(flush_num_bytes) =
            quickwit_common::get_from_env_opt::<ByteSize>(INGEST_WAL_FLUSH_NUM_BYTES_ENV_KEY, false)
        {
            config.flush_num_bytes = flush_num_bytes;
            config.max_buffered_num_bytes = config
                .max_buffered_num_bytes
                .max(ByteSize::b(flush_num_bytes.as_u64() * 8));
        }
        config.fetch_fallback_grace = Duration::from_secs(fetch_fallback_grace_secs);
        let gc_min_age_secs: u64 =
            quickwit_common::get_from_env(INGEST_WAL_GC_MIN_AGE_SECS_ENV_KEY, 600u64, false);
        config.gc_min_age = Duration::from_secs(gc_min_age_secs);
        Ok(Some(config))
    }

    /// Default configuration for the given WAL URI.
    pub fn with_wal_uri(wal_uri: Uri) -> Self {
        Self {
            wal_uri,
            flush_interval: Duration::from_millis(250),
            flush_num_bytes: ByteSize::mib(8),
            max_buffered_num_bytes: ByteSize::mib(256),
            fetch_fallback_grace: Duration::from_secs(60),
            gc_min_age: Duration::from_secs(600),
        }
    }
}
