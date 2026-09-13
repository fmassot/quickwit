# Ingest v3: write-ahead log on object storage

Status: experimental, behind `QW_ENABLE_INGEST_V3=true`.

```
QW_ENABLE_INGEST_V3=true
QW_INGEST_WAL_URI=s3://bucket/ingest-wal      # cluster-level, one prefix per ingester
QW_INGEST_WAL_FLUSH_INTERVAL_MS=100           # ack latency bound under light load
QW_INGEST_WAL_FLUSH_NUM_BYTES=8MiB            # object size under load (PUT count per byte)
QW_INGEST_WAL_FETCH_FALLBACK_GRACE_SECS=60    # indexers drain a dead ingester after this
QW_INGEST_WAL_GC_MIN_AGE_SECS=600             # janitor never deletes younger objects
```

## Why

Ingest v2 acknowledges a write once it is in a local `mrecordlog` and replicated to one
follower's disk. Losing the nodes loses acknowledged data, ingesters are stateful, and
replication is cross-AZ traffic. Ingest v3 acknowledges a write once it is in an object on S3.
Any ingester can be killed at any time; nothing acknowledged is lost; ingesters keep no state
on disk at all.

## Architecture

```
client ──► router ──► ingester
                        │  persist: pick shard (ingester lock, ~20 µs, no await),
                        │           validate docs, append to the shard's in-memory queue
                        │           and to the WAL buffer, wait for durability
                        ▼
              WalWriter buffer ──flush: interval | size──► encode (zstd, blocks in ‖)
                                                                   │
                                                  put_if_absent  <wal_uri>/<ingester>/wal/<id:020>.wal
                                                                   │
                        ◄──────── durable(id) ──────────────────────┘
                        │  positions applied once per flush, waiters acknowledged
   indexer ◄── fetch ───┘  (in-memory queues, only up to the durable position)
      │
      └── publish split ──► metastore ──► truncate ──► ingester GC deletes covered objects
```

**One log per ingester.** Every object multiplexes the records of every shard the ingester
leads. PUT count scales with node count, not shard or tenant count. Objects carry a footer
index `(queue, first/last position, byte range)`, so recovery and GC read a tail range, never
the data they do not need.

**Object = header | blocks (per queue, zstd, crc32) | footer index | trailer.** A block with
no records is a *queue marker*: the durable record that a shard exists, written by
`init_shards` before the control plane is told about the shard.

**Fencing, epochs issued by the log.** Objects are written with create-if-absent
(`If-None-Match: *`). A new writer reads the tail's epoch, proposes `epoch + 1`, and claims it
by writing an empty *fence* object at `tail + 1`; the previous writer's next PUT fails with
`AlreadyExists` and stops. Exactly one of any number of concurrent fencers wins. No clock, no
metastore involvement. `AlreadyExists` for an object whose header is our own (a PUT whose
response was lost) is a success.

**Durability boundary.** A request is acknowledged, and the shard's replication position
advanced, only once the object holding its records is durable. Fetch streams serve only up to
that position, so an indexer never publishes a position the log could lose.

**Stateless ingesters.** Records live in per-shard in-memory queues (std mutex, no node-wide
lock on the persist path). On restart, an ingester fences its own log and replays it from S3
into memory; shards come back closed and are drained by indexers. The local disk is not used.
A node switched from v2 must be drained first: its v2 records exist only on its disk.

**Dead ingester.** The indexer's fetch stream, after a grace period without the ingester in
the pool, fences the ingester's log and serves the shard's remaining records straight from
S3, then reports EOF. No takeover RPC, no metastore change: the checkpoint rejects overlapping
publishes, so the same records served twice are harmless; the fence makes the tail final.

**GC.** The ingester deletes its own objects once every record in them is truncated. The
janitor covers the rest every 10 min (dead ingesters' logs, old fences) using publish
positions from the metastore; the newest fence of a log is never deleted (it carries the
epoch).

Code: `quickwit-ingest/src/ingest_v3/` (`wal/{format,writer,reader,fence}.rs`,
`object_wal.rs`, `mem_queue.rs`, `gc.rs`, `metrics.rs`), `ingest_v2/ingester.rs::persist_inner_v3`,
`ingest_v2/fetch.rs` (object WAL fallback), `quickwit-janitor/.../ingest_wal_garbage_collector.rs`,
`quickwit-storage` (`Storage::put_if_absent`). Metrics: `quickwit_ingest_wal_object_*`.

## Results

One `c6i.4xlarge` (8 physical cores, 16 vCPU), S3 Standard in the same region for WAL,
metastore and indexes; indexer co-located; merges on a standalone compactor (none running).
Scripts in `quickwit-ingest/scripts/`.

**Cost.** PUT rate is `1 / flush_interval` under light load and `throughput / flush_num_bytes`
under load; S3 PUT takes ~35 ms up to 2.5 MB, ~50 MB/s beyond.

| flush interval | PUT/s (light load) | $/month per ingester |
|---|---|---|
| 100 ms | 10 | $130 |
| 1 s | 1 | $13 |

| object size (`flush_num_bytes`) | $/TB ingested (busy) |
|---|---|
| 8 MiB (default) | $0.60 |
| 16 MiB | $0.30 |
| 32 MiB | $0.15 — needs ≥ 32 MB in flight from clients, else every request waits for the interval |

**Latency** (16 closed-loop clients, 100 docs/request): ack p50 ≈ flush interval, p99 ≤ 150 ms
at 50–100 ms intervals. Open-loop arrivals see ~uniform(0, interval) + one PUT.

**Throughput**, 100 ms interval, 500 docs (~104 KB) per request:

| | 256 clients | 512 | 1024 |
|---|---|---|---|
| v2 (mrecordlog on gp3) | 56 MB/s, p99 3.5 s | 51 MB/s, p99 4.5 s | — |
| v3, first version (mrecordlog on gp3) | 65 MB/s, p99 0.6 s | 64 MB/s, **p99 16 s** | — |
| v2, mrecordlog on tmpfs, no merges | 114 MB/s | 117 MB/s | — |
| **v3, in-memory queues, gp3 idle** | 101 MB/s, p99 0.7 s | 106 MB/s | **108–123 MB/s** |

- The 16 s tail was the gp3 volume under mrecordlog + split writes. v3 no longer touches it.
- v3 reaches v2's ceiling once enough is in flight (its ack includes the wait for a flush;
  closed-loop throughput = in-flight bytes / ack latency).
- The ceiling is CPU: ~10.7 busy vCPUs on 8 cores, JSON parsing + tantivy (`perf`); the WAL
  is 2–3% of CPU. The ingester lock is held 2–3% of the time.
- At ~110–120 MB/s the single S3 connection is also saturated (~45–50 MB/s compressed);
  objects grow to absorb it. Next step for one ingester past that: multipart upload of one
  object (parallel parts, conditional completion, same fencing), or S3 Express.

**Recovery.** `kill -9` under load with 21 shards: 370,500 acknowledged docs, 370,500 recovered
from S3. Two-node cluster: kill the ingester holding a shard; the other node's indexer drains
its 3 un-indexed docs from the log within the grace period, the shard reaches EOF.

## Not done

- Streaming, batched replay: a restart after a large un-indexed backlog (11M records seen)
  loads everything before the node is Ready.
- Multipart upload for objects above ~16 MB; S3 Express One Zone run.
- Per-source `commit: durable | replicated` setting instead of a global flag.
- Integration test for the dead-ingester drain (done by hand on a two-node cluster).
- Why v3 yields smaller splits than v2 at equal load when merges run on the indexer.
