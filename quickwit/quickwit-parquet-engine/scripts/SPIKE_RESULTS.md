# Metrics engine spike: does it work, is it worth keeping?

One `c6i.4xlarge` (8 physical cores), S3 Standard same region for metastore and index, single
node with all services, branch `metrics-spike` (main + the OTLP metrics service mounted in
`quickwit-serve`, ~30 lines). Release build with `--features metrics`.

Load: TSBS "devops / cpu-only" shape from `scripts/tsbs_otlp` — 1,000 hosts, 10 `cpu.*`
gauges per host, 10 host tags as attributes, one point per 10 s, 6 hours = 21.6M points,
sent as OTLP over gRPC as fast as accepted. Queries: TSBS devops query types translated to SQL
over the long-format table, via the DataFusion gRPC endpoint (`scripts/bench/sql.sh`).

## Findings

**1. It did not work out of the box, for one reason: nothing mounted the OTLP metrics
service.** `OtlpGrpcMetricsService` existed and was tested, but `quickwit-serve` only
registered logs and traces, and never created the `otel-metrics-v0_9` index. With that fixed
(mirror of the logs wiring), the whole path works on the first run: OTLP → ingest v2 (Arrow
IPC batches) → parquet indexing pipeline → split on S3 → published in `metrics_splits` →
SQL over DataFusion. Every acknowledged point was queryable (18,803,000 = 18,743,000 + 60,000).

**2. Ingestion**: 237–272k points/s on one node, quickwit at 3.5–4.3 cores and 1.7–2 GB RSS,
export p50 27 ms / p99 40 ms. Errors only at shard scale-up (2,857 of 21,600 requests on a
cold index, 15 on a warm one).

**3. Storage**: after merge, **1.33 bytes per data point** (integer values, as in TSBS;
13.7M rows in 18.2 MB). With incompressible full-precision doubles: ~10 bytes/point.
Reference TSBS numbers: Prometheus ~1.3, VictoriaMetrics ~0.5–1, InfluxDB ~2, ClickHouse ~1–3.

**4. Merges**: 11-way merges ran continuously during ingestion; RSS stayed flat at ~2 GB, i.e.
the bounded-memory streaming merge does what it claims. Merge outputs were ~0.8–7 MB; the
256 MB target means several more levels before the split count settles.

**5. Queries** (18.8M points, ~100 live splits on S3, harness overhead ~70 ms included):

| TSBS query | rows | latency |
|---|---|---|
| single-groupby-1-1-1 (1 host, 1 metric, 1 h, per-minute max) | 50 | 270 ms |
| single-groupby-1-8-1 | 400 | 285 ms |
| single-groupby-5-8-1 | 2,000 | 540 ms |
| double-groupby-1 (all hosts, 1 metric, 6 h, per-hour mean) | 7,000 | 320 ms |
| double-groupby-all (10 metrics) | 70,000 | 1.5 s |
| high-cpu-all (value > 90, 6 h) | 100,000 | 430 ms |
| lastpoint | 1,000 | 210 ms |
| groupby-orderby-limit | 5 | 280 ms |
| cpu-max-all-1 (1 host, 10 metrics, 6 h, per-hour max) | 70 | 600 ms |

Correct results. A second pass is not faster: every query re-reads split footers/pages from
S3, so latency is per-split round trips, not compute. Dedicated TSDBs answer the single-host
queries in single-digit ms from local disk; this is an object-store engine before compaction
settled and without a split cache on the parquet path. The number to compare against is the
product's own SLO, and it will move a lot with (a) compaction to 256 MB splits, (b) a sort
schema with `hostname` early (default order scans a metric across all hosts), (c) a cache.

## Verdict

The engine is real and worth keeping: ingestion at 250k points/s per node, 1.3 bytes/point,
bounded-memory merges, correct SQL over a distributed DataFusion runtime. What was "done
fast" is everything around it: no entry point mounted, index type by id prefix, a parallel
split lifecycle, no query surface a user can reach (grpcurl + Arrow IPC decoding), no docs.
That is the plumbing work described as option C in the analysis, not an engine rewrite.

## Not measured

4,000+ hosts / high cardinality, query latency after full compaction, the parquet pipeline's
`kill -9` behaviour mid-merge, baseline runs of ClickHouse / VictoriaMetrics on the same box.

## Reproduce

```
quickwit run --config node-metrics.yaml   # indexer.enable_otlp_endpoint: true, QW_ENABLE_DATAFUSION_ENDPOINT=true
tsbs_otlp --hosts 1000 --rounds 2160 --concurrency 8
bash tsbs_queries.sh
```
