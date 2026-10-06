# The CDC and micro-batch North Star — standing defaults when the owner is away (2026-10-05)

**Date:** 2026-10-05 · **Filed by:** the owner's delegate, at the owner's request · **Applies to:** every
unit on the change-data and micro-batch track: the 1.6 connectors (C-1 onward), the 1.7 sink slices
(MB-0 onward), `repark-cdc`, `repark-crawler`, the Silver driver in `repark-core`, and any design sketch
that feeds them. **Source plans:** the
[micro-batch change-data sink plan](microbatch-cdc-sink-plan-2026-10-04.md) (O-1…O-10, D-1…D-8),
[contracts ahead of code](contracts-ahead-of-code-2026-10-01.md) (CC-1…CC-10, ES-1…ES-10) and the
[crate layout through 1.8](crate-layout-1-8-2026-10-01.md) (CL rulings).

## 1. What this document is

The owner is not always available. A sketch or an executor that meets a question none of the ruled rows
answers used to halt and wait. This document gives the answer it reaches for instead, so the track keeps
moving and the owner reads a record afterwards rather than a queue of questions.

**Precedence.** Below every dated owner ruling, every O, D, CC, ES and CL row, [AGENTS.md](../../../AGENTS.md)
and [docs/testing.md](../../../docs/testing.md). Above the judgment of any executor, verifier or
orchestrator. It never overrides a numeric gate, never creates a crate, never changes a ruled row and never
authorises a push to `main`.

**How a default is used.** The agent writes four lines before acting: the question, what Apache Flink does,
what Apache Spark does, and the default this document selects. If Flink, Spark and the default agree, it
acts and records the four lines as a dated row in the unit's ledger. If they disagree, it still acts on the
default below and files the disagreement as a question for the owner, without halting. It halts only when
§8 says so.

## 2. The authority order for an unruled behaviour

| rank | authority | governs |
|---|---|---|
| 1 | **Apache Flink** (checkpointing, two-phase-commit sinks, restart, backpressure, CDC connectors) | every *guarantee*: what is exactly-once, what survives a crash, what a restart reads, what fails loud |
| 2 | **Apache Spark Structured Streaming** (and the Iceberg Spark streaming source and sink) | every *surface*: option names, `StreamingQuery` lifecycle, progress fields, error wording, what the facade accepts |
| 3 | **Iceberg's own Flink and Spark sinks** | anything that touches a snapshot summary, a table property, a commit or a retry against the catalog |
| 4 | **Refuse, with a dated registry row** that names the fix | anything the three above do not settle |

When Flink and Spark differ, Spark wins on the surface and Flink wins on the guarantee, and the pair is
recorded. Flink and Spark are the choice because they are the two production streaming engines that have
carried this workload for a decade; a behaviour both share is as close to a safe default as the field has.

## 3. The guarantee set — what every slice is built to keep

- **NS-1 · Exactly-once is a triad.** A replayable source (the Iceberg append scan over a snapshot range),
  an idempotent commit keyed by epoch (D-4), and offsets written in the same atomic commit as the data
  (D-1). Remove any one and the slice is not done. Iceberg's Flink sink keeps `flink.max-committed-checkpoint-id`
  in the summary for the same reason; the Spark sink keeps the query id and epoch id.
- **NS-2 · One state store.** Every durable bit lives in the catalog (O-3, D-6). No checkpoint directory, no
  sidecar file, no second table that says what the summary already says. A proposal that adds a second
  store is refused on sight.
- **NS-3 · Batch boundaries are snapshot ranges, never wall-clock.** A batch is `(start-snapshot-id,
  end-snapshot-id]` plus a position inside the end snapshot's added files when split (D-2). The same
  offsets replay to the same batch on any machine, which is what makes the crash matrix meaningful.
- **NS-4 · Restart reads the sink and nothing else.** No inference from timestamps, no "probably committed".
  An unknown commit outcome is an explicit recovery-required state (CC-1, D-3); Flink retries the
  committable because it is idempotent, and so does this driver, by re-checking the epoch in the summary
  history before any retry.
- **NS-5 · Fencing by generation.** The generation field (CC-2) rides on every commit; a stale generation
  loses at `validate_from_snapshot` (D-4) and reports it. Two drivers on one sink are safe by construction,
  never by convention.
- **NS-6 · Fail loud on a contract violation.** A delete or overwrite snapshot inside a Bronze window
  refuses (O-5, D-2). A schema change that is not additive refuses. A source table that stops being
  keyed refuses (O-6). Both engines fail the query here; neither skips silently without an explicit option.
- **NS-7 · Bounded everything.** One batch in flight per query (D-3); a window split by file count and row
  count with Spark's option names; bounded channels; a timeout on every network call; memory inside the
  worker slice. Unbounded buffering is a defect, not a tuning problem.
- **NS-8 · The query fails and reports; the operator restarts.** This is Spark's model and 1.7's default.
  Automatic restart with Flink's fixed-delay and failure-rate strategies is a later card, reserved by name,
  not improvised inside a slice.
- **NS-9 · Event time, watermarks and late data are out of 1.7.** The surface names are Spark's
  (`withWatermark`, `allowedLateness` has no Spark twin and is not reserved) and refuse with a dated row
  until a card opens them. Nothing in the driver may depend on event time.

## 4. Rust organisation — where code goes when the CL rows do not say

The ruled layout stands: fourteen crates through 1.8, connect at 1.6, crawler and cdc at 1.7 with the Silver
driver a module of core, io at 1.8, the dialect crate below `repark-spark`. Planned homes are pre-declared
in `repo-manifest.toml` and `check_crate_dag.py` and never pre-created. Inside that layout, placement follows
the practice of the Rust data systems whose shape this repository already resembles:

| project | the habit worth copying |
|---|---|
| **DataFusion** | one crate per layer; the trait sits in the lower crate (`TableProvider`, `ExecutionPlan`), implementations sit above it; one error enum per crate with a `Result` alias; the umbrella crate only re-exports |
| **Polars** | the core crate does no IO; `polars-io` is separate; the streaming engine (`polars-stream`) is its own crate beside the lazy planner; every optional engine or format is a cargo feature, default-off |
| **RisingWave** | one connector crate with `source/<name>` and `sink/<name>` modules; stream executors in their own crate; the meta service owns coordination and the catalog; anything that crosses a process boundary is a versioned schema, never a Rust struct |
| **Supabase ETL** | the pipeline core exposes `Destination`, `StateStore` and `SchemaStore` traits; destinations live in a separate feature-gated crate; the per-table lifecycle is an explicit enum, never a set of booleans |
| **Apache Iggy** | typed configuration structs validated once at startup; strict layering transport → protocol → storage; offsets held server-side; persisted state is an append-only log |
| **Sail** | a crate per protocol layer; plan and execution split; the Spark-compatibility surface is its own crate over the engine, which is exactly `repark-spark` over `repark-core` |

The rules this yields:

- **NS-10 · Traits below, implementations above.** A capability (`Source`, `Sink`, `OffsetStore`,
  `Committer`) is a trait in the lowest crate that has every type it needs and none it does not. The
  implementation lives in the crate that owns the dependency it pulls in.
- **NS-11 · Separate the data path from the commit path.** Flink's Sink v2 splits `SinkWriter` from
  `Committer`; DataFusion splits planning from execution. The driver does the same: staging files is one
  type, committing a batch is another, so two-phase commit is expressible and testable without a catalog.
- **NS-12 · A connector is a module until its dependencies diverge.** Postgres and SQL Server live as
  modules under `repark-connect` with their drivers behind features (CC-5); a connector earns its own
  crate only when a dependency cannot be feature-gated, and that is a CL ruling, not a slice decision.
- **NS-13 · No IO in `repark-common` or in a type that other crates share.** Identity kernels, offsets,
  generations and conversions are pure; anything that opens a socket or a file lives above them.
- **NS-14 · Newtypes for every identifier.** `SnapshotId`, `Epoch`, `QueryId`, `Generation`, `Lsn`; never a
  bare `u64` across a function boundary. The `0 = unassigned` sentinel exists only in serialised form; in
  Rust it is `Option<Generation>` or a non-zero type.
- **NS-15 · One error enum per crate, `thiserror`, structured meaning first (CC-4).** A string in an error
  is a message, never the thing a caller matches on. Spark's wording is used where it was measured.
- **NS-16 · State machines are enums.** The query lifecycle (`Registered`, `Running`, `Draining`,
  `Stopped`, `Failed`, `RecoveryRequired`) and the per-table CDC phase are `enum`s with the transitions
  written as functions. A boolean that means "sort of started" is a defect.
- **NS-17 · `#![forbid(unsafe_code)]`** in the driver, cdc, crawler and connect crates. Arrow kernels in
  common may use `unsafe` only with a `map.md` reason and a test that exercises the bound.
- **NS-18 · The facade forwards.** Python builds options and hands a callable to `foreachBatch`; it never
  holds offsets, never retries, never decides a commit (D-8, Rust-first).
- **NS-19 · Edges are the DAG gate's business.** When a change needs an edge the gate denies (CC-8), the
  answer is to move the code down or lift a trait, not to add an exception.

## 5. Production grade — the four lists

**Safety.**
- No `unwrap`, `expect` or `panic!` on a product path (`make rust-panic-ban`).
- Cancellation-safe async: no state mutation straddles an `.await` inside a `select!` arm; the in-flight
  batch is the only soft state and it is rebuilt from the sink (D-3).
- Configuration structs carry `deny_unknown_fields`; a misspelt option refuses at start, never at batch 400.
- Every summary key the driver writes is namespaced `repark.` and carries `repark.cdc.format-version`; a
  newer version than the running build understands refuses to resume and names the version.
- Retention guard: resuming from a start snapshot that `expire_snapshots` has removed refuses, naming
  the oldest snapshot still available and the `expire_snapshots` policy to fix it. The docs state the rule
  that retention must exceed the longest expected lag.

**Security.**
- No credential, DSN, token or key ever enters a snapshot summary, a table property, a progress report, a
  log line or an error message. A test greps every one of those surfaces in the C-0 harness.
- TLS is the default for every database connector; plaintext is an explicit option with a dated row, the
  posture Supabase ETL and the cloud warehouses already take. Auth methods follow ES-1.
- Identifiers are quoted, values are bound; no SQL is built by string concatenation on a product path.
- The capture role is least-privilege and the docs show the exact grants. A missing grant produces the
  structured error that names it.
- No process spawning, no shelling out from the driver or a connector.
- Supply chain per ES-9: SBOM and `cargo-deny` stay green on every connector dependency added.

**Performance.**
- Arrow from the scan to the commit; no per-row Python, no per-row allocation on the hot path.
- Admission control before optimisation: a batch is bounded by files and rows (NS-7); a slow batch is a
  smaller batch, not a larger buffer.
- Measure before ruling, the repository's standing rule, and every ratio gate is read on a release build
  of both sides. A debug-build ratio informs; it does not pass or fail a gate.
- One catalog commit per batch per table is the hot path; a design that adds a second commit per batch
  must show why.
- Snapshot expiry and compaction are the operator's scheduled procedures, documented beside the trigger
  interval; the driver never runs them implicitly (ruled 2026-10-05).

**Solidity.**
- The crash matrix is a gate, not a nice-to-have: crash before staging, after staging before commit,
  after commit, during a retry, and two drivers at once, in the C-0 harness, for every slice that
  touches the commit path.
- Every Spark behaviour matched is a recorded oracle cell; every deliberate divergence is a dated row.
  The parity matrix is the specification.
- Property tests over offset arithmetic (ranges, splits, vector offsets across several Bronze inputs)
  with the invariant that replaying any recorded offset sequence yields the same Silver rows.
- Idempotent everything: a procedure, a `MERGE`, a commit, a watermark write. The question "what if this
  runs twice" is answered in the design sketch for every effectful step, in writing.

## 6. The operator's surface — Spark's names, measured

- Reader and writer options are Spark's and the Iceberg Spark source's: `streaming-max-files-per-micro-batch`,
  `streaming-max-rows-per-micro-batch`, `streaming-skip-overwrite-snapshots`,
  `streaming-skip-delete-snapshots` (the last two accepted only when the oracle shows Spark's exact
  behaviour; otherwise they refuse under O-5).
- `StreamingQuery` carries `start`, `awaitTermination`, `stop`, `status`, `lastProgress`,
  `recentProgress`, `exception`, `isActive`, `id`, `runId`, `name`. `lastProgress` uses Spark's field
  names: `batchId`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs`,
  `sources[].startOffset` and `endOffset`, `sink.description`, with the Iceberg snapshot ids inside the
  offsets as the Iceberg source already renders them.
- `checkpointLocation` is accepted and recorded, never read, for an Iceberg sink, unless MB-0 shows Spark
  refuses the combination, in which case the refusal is copied (D-1 leaves this to the oracle).
- A refusal names the fix. "Cannot resume: start snapshot 7731 expired; oldest available is 7790; see
  expire_snapshots retention" is the standard, not "invalid offset".
- `EXPLAIN` on a streaming frame renders the window and the pushed predicates, per CC-1.

## 7. Ideas worth carrying, none of them a slice yet

- **Bootstrap and increment are one code path.** The first run of a sink is a full read pinned at
  `VERSION AS OF end`, every later run is the append scan over `(start, end]`; this is Flink CDC's
  snapshot-then-incremental shape. One driver, one offset type, no "initial load" mode.
- **A read-only progress view** derived from the sink's own summaries, so an operator reads progress
  with `SELECT` and no second store exists (NS-2 kept). A later 1.7 card.
- **Quarantine as an opt-in.** Default is fail-fast like both engines; an explicit option routes rows that
  fail conversion to a quarantine table with the batch epoch, the source position and the reason. Rows
  are never dropped silently.
- **Lineage for free.** The summary already names the query, the epoch and every source snapshot; ES-4's
  OpenLineage emitter is a renderer over that, not a new recorder.
- **A design-sketch checklist**, eight lines, that every micro-batch or connector sketch answers before it
  is read: the guarantee kept, the crash matrix, where every bit of state lives, the fencing token, the
  bounds, the credential surfaces, the Spark names used, and the divergence rows filed. A sketch missing a
  line is returned, not read.
- **Deterministic simulation** of the driver against a fake catalog with injected faults, in the style
  RisingWave uses for its stream engine, as the 2.x successor of the C-0 crash matrix.

## 8. When the agent still halts

Only three cases halt the track while the owner is away:

1. A default here would change a ruled row (O, D, CC, ES, CL) or a numeric gate.
2. Spark and Flink disagree on a *guarantee*, not a surface, and the Flink answer would make a Spark
   workload return different rows.
3. The crash matrix finds a path where a committed batch can apply twice or a committed offset can be
   lost, and no fix fits the slice.

Everything else is acted on under §1's four-line record and read by the owner afterwards.

## 9. Leaves this directory when

The 1.7 sink ships and this document's rules have moved into the crate `map.md` files and the
engineering method, or the owner replaces it with a dated successor.

## Pointers

- Up: [map.md](map.md)
- The plan this guards: [microbatch-cdc-sink-plan-2026-10-04.md](microbatch-cdc-sink-plan-2026-10-04.md)
- Contracts: [contracts-ahead-of-code-2026-10-01.md](contracts-ahead-of-code-2026-10-01.md)
- Layout: [crate-layout-1-8-2026-10-01.md](crate-layout-1-8-2026-10-01.md)
