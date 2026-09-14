# Metrics integration refactor

Keep the Parquet storage/merge algorithms and DataFusion runtime; replace the POC's
integration boundaries. Metrics compatibility is not a constraint. Tantivy behavior
and defaults remain supported. This branch starts from `main`, independently of ingest v3.

## Explicit engine selection

`IndexConfig.index_type` and index templates select `tantivy`, `metrics`, or `sketches`.
Omission always means Tantivy. There is no prefix inference, warning shim, or automatic
migration of POC metrics metadata. Recreate POC indexes with the correct type and reingest.
Index type is immutable through config updates.

Pipeline selection, ingest-v1 eligibility, DataFusion discovery, GC, retention, and
merge reseeding use explicit types. Parquet publishers carry their split kind, so an
empty checkpoint-only sketch publication still uses the correct metastore operation.
The existing OTLP metrics gRPC service is mounted only with the `metrics` Cargo feature
and the OTLP endpoint enabled on an indexer.

## One pipeline supervisor

The four duplicated indexing/merge supervision loops are replaced by:

```rust
pub type IndexingPipeline = PipelineSupervisor<TantivyIndexing>;
pub type ParquetIndexingPipeline = PipelineSupervisor<ParquetIndexing>;
pub type MergePipeline = PipelineSupervisor<TantivyMerge>;
pub type ParquetMergePipeline = PipelineSupervisor<ParquetMerge>;
```

The implementation lives in `quickwit-indexing/src/actors/pipeline_supervisor/`.
Engine-specific files describe actor graphs rather than implementing another lifecycle.

| Boundary | Responsibility |
| --- | --- |
| `Pipeline` | Build a typed graph, report counters, identify the index and construction budget. |
| `PipelineSupervisor<P>` | Spawn/retry, generation counters, health checks, terminal states, final observations. |
| `PipelineActors` | Register every stage, install its child kill switch, stop and join the whole generation. |
| `SourcePipeline` / `SourceState` | Indexing-only capability: source loading, publish token, saved assignments and replay. |
| `DrainablePipeline` | Merge-only capability: disconnect feedback and ask the planner to finalize. |
| `PipelineHandle` | Type erasure only at the indexing-service boundary. |

Actor construction stays ordinary Rust, with compiler-checked mailbox types:

```rust
let (publisher_mailbox, publisher_handle) =
    actors.spawn(ctx.spawn_actor(), publisher);
let (sequencer_mailbox, _) =
    actors.spawn(ctx.spawn_actor(), Sequencer::new(publisher_mailbox));
```

Discarding a returned handle does **not** remove the actor from supervision. There is
no separately maintained list of healthy/killable actors. Typed handles retained by
the engine's `Running` state are only for observations and commands.

### Lifecycle guarantees

- A graph is installed only after successful construction. A partial spawn failure
  kills and joins every already-started stage before retrying. Dropping a construction
  guard during cancellation also signals its child kill switch.
- All four graphs share health aggregation and exponential backoff. Every stage,
  including the Parquet merge sequencer, is supervised.
- The first progress deadline starts **after** graph construction, giving even the
  last-spawned stage a full heartbeat. Construction time is not an actor's progress budget.
- Restart waits for the old generation to terminate before building the next one.
  Final observations are collected after actor finalizers complete.
- Index deletion is terminal, including for Parquet merge spawns.
- Draining is terminal: no restart, including on failure. A drain request while waiting
  to retry exits immediately rather than leaving an idle supervisor behind forever.
- Shard assignments and publish tokens outlive generations. A closed source mailbox
  does not lose an assignment or kill the supervisor; the next generation receives it.
- Planner mailboxes remain stable across merge generations. Storage-specific split
  discovery and maturity rules stay in the engine adapters.

`ActorHandle::wait(&self)` supports retaining final observations while the generation
owner waits for termination. `SpawnBuilder` is now nameable so the generation owner
can install supervision around ordinary configured actor builders.

## Typed publication and upload actors

The common stages are generic actors, not Tantivy actors with extra Parquet handlers:

| Shared actor | Engine-specific implementation |
| --- | --- |
| `Publisher<E: PublicationEngine>` | `tantivy/publisher.rs` and `parquet_pipeline/publisher.rs` |
| `Uploader<E: UploadEngine>` | `tantivy/uploader.rs` and `parquet_pipeline/parquet_uploader.rs` |
| `Sequencer<A>` | Orders delivery to the engine's typed publisher. |

`PublicationEngine` associates split metadata, merge-task ownership, planner messages,
and planner actor types. `SplitUpdate<Split, Task>` is the shared envelope. A Tantivy
publisher cannot accept Parquet updates or connect to a Parquet planner; there is no
alternate-engine mailbox or runtime engine flag inside the common publisher. Existing
Tantivy `Publisher`/`Uploader` exports remain concrete aliases; Parquet pipelines use
`ParquetPublisher`/`ParquetUploader`.

The publisher owns locking, revoked-token retries with token refresh, source truncation,
feedback, counters, and drain-time disconnection. Engines own validation, metastore
operations, and storage-specific metrics/events. Merge tasks remain alive through both
publication and feedback, including checkpoint-only updates.

The uploader owns ordered-slot reservation, semaphore acquisition, counters, task
ownership, and failure propagation. Engines prepare, stage, and store their artifacts:
Tantivy retains its split bundle, recovery footer and cache behavior; Parquet retains
its maturity policy, file layout and lifecycle events. Existing per-engine ingest/merge
budgets and queue capacities are retained. Storage formats and wire protocols do not change.

Upload workers are owned in a `JoinSet`: successful exit drains them, failure/cancellation
aborts and joins them before final observations. Unstarted templates remain cloneable for
the Tantivy delete-task supervisor; cloning outstanding worker sets is an invariant violation.
A worker panic faults the generation,
including direct-to-publisher pipelines. A failure during successful draining cannot
become a successful actor exit. Closed publication edges now fail rather than warning
and dropping a Parquet checkpoint. Only a deliberately revoked publish lock discards
its reserved slot without publication. I/O already submitted to the OS or storage backend
is not rolled back by local task cancellation; staged/orphan cleanup remains necessary.

Document processing, indexing, packaging and merge algorithms remain separate concrete
engine implementations. This extracts actual shared lifecycle behavior rather than
forcing unlike algorithms into an abstract actor or introducing a graph DSL. Pipeline
construction parameters and source/storage setup still need separate consolidation.

## One Parquet split lifecycle API

`quickwit-metastore::ParquetSplits` binds a metastore client to an index incarnation
and explicit split kind. Upload, publication, merge reseeding, DataFusion discovery,
GC, retention, and index administration use this boundary:

```rust
let catalog = ParquetSplits::from_index_metadata(metastore, &index_metadata)?;
catalog.stage(&split_metadata).await?;
catalog.publish(&ParquetPublication {
    staged_split_ids,
    replaced_split_ids,
    checkpoint_delta, // typed IndexCheckpointDelta, not caller-serialized JSON
    publish_token,
}).await?;
let published = catalog.list_all(catalog.query()).await?;
catalog.mark_for_deletion(&obsolete_ids).await?;
// Delete storage files successfully before removing their metadata.
catalog.delete(&deleted_file_ids).await?;
```

- Staging rejects mixed kinds, wrong index incarnations, and duplicate IDs before RPC.
  Publication rejects duplicate/overlapping IDs and preserves empty checkpoint updates.
- `list_page` bounds responses to 500 records, checks identity, requested states and
  strict cursor ordering, and advances the cursor only after a valid response. `list_all`
  collects these pages; it is not a bounded-memory or snapshot API.
- The catalog does not retry mutations or split a publication into multiple RPCs.
  Publisher retry/token-refresh policy stays with the actor; split/checkpoint atomicity
  stays with the backend. File-backed publication now rejects missing or non-published
  replacement inputs, matching PostgreSQL's existing requirement, with checkpoint rollback.
- Delete/clear now route Parquet indexes correctly, process all split states pagewise,
  and fail if cleanup fails. Index metadata and source checkpoints are retained on split
  cleanup failure so it can be retried. Dry-run does not mutate files or metadata.
  GC listing failures also propagate rather than becoming successful partial scans.
- Administrative operations require quiesced writers: pagination is not an ingestion
  fence, and deletion plus checkpoint reset is not a single distributed transaction.

The metrics/sketch RPCs and persisted tables **still exist** behind the private
`parquet/transport.rs` adapter. This unifies the native lifecycle boundary, not the wire
protocol or database schema; it adds no prefix fallback or metrics migration shim.
Pipeline construction parameters and source/storage setup are not yet consolidated.

## Verification

Commands from the `quickwit/` workspace (formatting from the repository root):

```sh
make fmt
cargo check -p quickwit-cli --tests
cargo test -p quickwit-indexing
cargo test --no-fail-fast --features metrics \
  -p quickwit-metastore -p quickwit-indexing -p quickwit-actors -p quickwit-config \
  -p quickwit-datafusion -p quickwit-index-management -p quickwit-janitor \
  -p quickwit-control-plane -p quickwit-opentelemetry -p quickwit-serve
cargo check -p quickwit-metastore --features postgres --tests
cargo clippy --features metrics --tests \
  -p quickwit-cli -p quickwit-metastore -p quickwit-indexing \
  -p quickwit-index-management -p quickwit-janitor -p quickwit-datafusion -- -D warnings
```

The earlier supervisor validation passed **927 tests** across the nine crates (one unit test and one
doctest ignored), including **304 indexing tests** with metrics enabled. The default
indexing suite passed **240 tests** in three consecutive runs. Formatting, the default
CLI check, and metrics-enabled Clippy with warnings denied also passed. One earlier
default-suite run showed transient generation-count assertion failures; the harness's
500ms wall-clock heartbeat remains sensitive to scheduling, and load-related flakiness
has not been fully ruled out.

New tests cover partial-spawn cleanup, generation isolation, counter accumulation,
duplicate spawns, deletion, draining before retry, drain success/failure, construction
cancellation, and the progress budget. Existing indexing, merge, sketch, recovery, and
publication tests exercise the engine adapters.

The trace-conformance fixtures now use matching index UIDs in their configs and splits.
Their collector captures all events for each test's index independently; one test no
longer consumes another's trace. Row-conservation and other model invariants are
unchanged, and a regression test verifies capture isolation. Teardown tests detach
merges before indexing (as the CLI does) and wait for actual asynchronous termination.
The explicit-commit E2E test uses non-accelerated time so a simulated timeout cannot
flush between its two phases.

The split-catalog follow-up passed **1,075 unit/integration tests plus one doctest**
across ten crates, including 304 metrics indexing tests, 142 metastore tests, and 21
index-management tests. Default indexing passed 240 tests again; formatting, default
CLI checking, PostgreSQL-feature test compilation, and metrics-enabled Clippy with
warnings denied passed.

Coverage includes both Parquet kinds, checkpoint-only publication, malformed batches
and pages, cursor preservation on errors, failed replacement/checkpoint rollback,
503-split administrative deletion, dry-run, clear, and cleanup failure/retry behavior.
Uploader, merge and retention fixtures now use matching index incarnations and ordered
list responses. The crash test now reseeds only committed splits (not failed staged
outputs) and checks that every original input is replaced and visible row count is
conserved. Trace/model invariants were not relaxed.

The actor-separation follow-up passed **310 metrics-enabled indexing tests and five
doctests**, **246 default indexing tests**, **181 janitor/serve tests**, and **11 failpoint
tests**. One indexing unit test and one existing doctest remain ignored. Default CLI test
compilation, formatting, and metrics/failpoints-enabled Clippy with warnings denied passed:

```sh
cargo test -p quickwit-indexing --features metrics --no-fail-fast
cargo test -p quickwit-indexing
cargo test -p quickwit-janitor -p quickwit-serve --features metrics
cargo test -p quickwit-indexing --features metrics,fail/failpoints \
  --test failpoints -- --test-threads=1
cargo clippy -p quickwit-cli -p quickwit-indexing \
  --features metrics,fail/failpoints --tests -- -D warnings
```

New coverage checks direct-upload faults/panics, ordered cancellation, resource release
before actor termination, successful draining, failure during draining, and configuration-only
cloning. Positive and compile-fail doctests verify engine-specific publication, upload and
feedback edges. The worker-panic failpoint checks document conservation across restart;
the existing post-spawn panic test now targets an actual failpoint and checks it fired.
The downstream staging fixture now validates its five-row output instead of leaving an
unconfigured mock in background work. One partitioning fixture uses real time to prevent
a simulated timeout from splitting its explicit commit; timeout tests remain accelerated.
The previously observed generation-count flakiness recurred in an intermediate full run;
isolated and subsequent full runs passed. No fix for that flakiness is claimed, and generation
and row-conservation assertions were not relaxed.

No new EC2 run, PostgreSQL migration test, mixed-version cluster test, or network-level
OTLP-to-SQL smoke test has been performed for this refactor. Live PostgreSQL tests were
unavailable locally: no server was listening and Docker socket access was denied.

## Next boundaries to replace

1. Replace the adapter's separate metrics/sketch RPCs and persisted tables with a
   unified protocol/schema. Audit server-side engine validation, restart semantics and
   file-backed/PostgreSQL parity; the native catalog is not a substitute for these.
2. Consolidate remaining pipeline construction parameters and storage/source setup;
   keep per-engine actor graphs explicit rather than introduce an actor-graph DSL.
3. Replace Husky-specific sort configuration with a plain, validated schema.
4. Add deterministic OTLP-to-query integration coverage and SQL-over-REST with bounded
   streaming and typed errors.
5. Validate sustained ingestion, compaction memory, crash recovery, and query correctness
   before making production-readiness or performance claims.
