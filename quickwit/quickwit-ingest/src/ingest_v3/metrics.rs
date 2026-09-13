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

//! Metrics of the ingest v3 object-store WAL. All under the `ingest` subsystem, prefixed
//! `wal_object_`.
//!
//! What to watch:
//! - `wal_object_puts_total{result}` and `wal_object_put_duration_secs`: the cost driver (one PUT
//!   per flush) and the ack-latency floor.
//! - `wal_object_flush_latency_secs`: append → durable for the oldest record of each object, i.e.
//!   the durability latency clients actually experience.
//! - `wal_object_buffered_bytes`: unflushed data; hitting the configured maximum means backpressure
//!   (`WalFull`).
//! - `wal_object_fenced_total`: should be zero outside of node restarts.
//! - `wal_object_fallback_drains_total{result}`: indexers draining dead ingesters.

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{
    LazyCounter, LazyGauge, LazyHistogram, lazy_counter, lazy_gauge, lazy_histogram,
};

static WAL_OBJECT_PUTS_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_puts_total",
        description: "Number of object-store WAL PUT attempts, by result.",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_PUTS_SUCCESS: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_PUTS_TOTAL, "result" => "success");

/// The PUT landed on a previous attempt whose response was lost.
pub(crate) static WAL_OBJECT_PUTS_ALREADY_WRITTEN: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_PUTS_TOTAL, "result" => "already_written");

pub(crate) static WAL_OBJECT_PUTS_FENCED: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_PUTS_TOTAL, "result" => "fenced");

pub(crate) static WAL_OBJECT_PUTS_ERROR: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_PUTS_TOTAL, "result" => "error");

pub(crate) static WAL_OBJECT_PUT_DURATION_SECS: LazyHistogram = lazy_histogram!(
        name: "wal_object_put_duration_secs",
        description: "Duration of object-store WAL PUTs, successful attempts only.",
        subsystem: "ingest",
        buckets: exponential_buckets(0.005, 2.0, 12).unwrap(),
);

pub(crate) static WAL_OBJECT_BYTES_WRITTEN_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_bytes_written_total",
        description: "Bytes of WAL objects written to object storage (encoded, compressed).",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_RECORDS_WRITTEN_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_records_written_total",
        description: "Records written durably to the object-store WAL.",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_FLUSH_LATENCY_SECS: LazyHistogram = lazy_histogram!(
        name: "wal_object_flush_latency_secs",
        description: "Time between the first append into a WAL object and the object being \
                      durable: the durability latency of the oldest record of each flush.",
        subsystem: "ingest",
        buckets: exponential_buckets(0.01, 2.0, 12).unwrap(),
);

pub(crate) static WAL_OBJECT_BUFFERED_BYTES: LazyGauge = lazy_gauge!(
        name: "wal_object_buffered_bytes",
        description: "Bytes of records buffered in memory and not yet durable in the \
                      object-store WAL.",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_FENCED_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_fenced_total",
        description: "Number of times this node's WAL writer found its log fenced by another \
                      writer.",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_REPLAYED_RECORDS_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_replayed_records_total",
        description: "Records replayed from the object-store WAL when the ingester started.",
        subsystem: "ingest",
);

static WAL_OBJECT_GC_DELETED_OBJECTS_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_gc_deleted_objects_total",
        description: "WAL objects deleted, by collector (the owning ingester or the janitor).",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_GC_DELETED_BY_INGESTER: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_GC_DELETED_OBJECTS_TOTAL, "collector" => "ingester");

pub static WAL_OBJECT_GC_DELETED_BY_JANITOR: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_GC_DELETED_OBJECTS_TOTAL, "collector" => "janitor");

pub static WAL_OBJECT_GC_DELETED_BYTES_BY_JANITOR_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_gc_janitor_deleted_bytes_total",
        description: "Bytes of WAL objects deleted by the janitor.",
        subsystem: "ingest",
);

pub static WAL_OBJECT_LIVE_OBJECTS: LazyGauge = lazy_gauge!(
        name: "wal_object_live_objects",
        description: "WAL objects found by the last janitor GC pass, all ingesters included.",
        subsystem: "ingest",
);

static WAL_OBJECT_FALLBACK_DRAINS_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_fallback_drains_total",
        description: "Shards drained by an indexer directly from an absent ingester's \
                      object-store WAL, by result.",
        subsystem: "ingest",
);

pub(crate) static WAL_OBJECT_FALLBACK_DRAINS_SUCCESS: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_FALLBACK_DRAINS_TOTAL, "result" => "success");

pub(crate) static WAL_OBJECT_FALLBACK_DRAINS_ERROR: LazyCounter =
    lazy_counter!(parent: WAL_OBJECT_FALLBACK_DRAINS_TOTAL, "result" => "error");

pub(crate) static WAL_OBJECT_FALLBACK_DRAINED_RECORDS_TOTAL: LazyCounter = lazy_counter!(
        name: "wal_object_fallback_drained_records_total",
        description: "Records served to indexers straight from an absent ingester's \
                      object-store WAL.",
        subsystem: "ingest",
);
