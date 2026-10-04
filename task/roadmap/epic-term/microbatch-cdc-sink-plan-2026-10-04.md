# The micro-batch change-data sink — Bronze to Silver over Iceberg snapshots (planned 2026-10-04)

**Date:** 2026-10-04 · **Filed by:** the owner's project-lead delegate (a Claude session, claude-fable-5-1) from an owner discussion the same day · **Measured on:** `origin/main` at `1060ebeb` · **Builds on:** [contracts-ahead-of-code-2026-10-01.md](contracts-ahead-of-code-2026-10-01.md) (CC-1, CC-2, CC-6, CC-9), [crate-layout-1-8-2026-10-01.md](crate-layout-1-8-2026-10-01.md) (CL-1, CL-2), [unified-database-query-cdc-silver-plan-2026-09-13.md](unified-database-query-cdc-silver-plan-2026-09-13.md) (the three lifecycles, the capture and recovery protocol, the publication state machine), [deterministic-silver-layer-compiler-2026-09-12.md](deterministic-silver-layer-compiler-2026-09-12.md) (§5 input identity, §6 determinism and identity), [../mid-term/ice-streaming-1-6.md](../mid-term/ice-streaming-1-6.md) (the v1.6.0 card and its step 0), [../../ledgers/staging/ice-changelog-1-ledger.md](../../ledgers/staging/ice-changelog-1-ledger.md), [../../ledgers/staging/silver-s0-ledger.md](../../ledgers/staging/silver-s0-ledger.md) (C-008), [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) (rows 1.7 and 2.2).

The owner asked what RePark needs for a Flink-like micro-batch runtime that can serve as part of a
CDC connector or sink: take an existing Bronze and Silver schema on Iceberg tables, detect changes to
Bronze, and run in-flight ETL upserts into Silver. This file records the answer, the decisions it
rests on, the slices, how they parallelise, and the order in which they open. Everything under
"Decisions" is **proposed** until the owner rules in this change's review; the rows the owner has ruled
so far are marked in §7.

## 1. What "Flink-like" means here

A loop. A source offset per Bronze table, a trigger, one batch per trigger read as an incremental
scan between two snapshots, the Silver transform over that batch, one commit into Silver, and the
offset advanced atomically with that commit. Flink's heavy parts, keyed operator state and event-time
timers, are not needed for a Bronze-to-Silver upsert sink because every piece of durable state lives
in an Iceberg table. The roadmap's 2.2 row already says this honestly: batch-over-snapshots, **not a
streaming engine**. Latency is the Bronze commit cadence, not sub-second.

## 2. What `main` already has (measured at `1060ebeb`)

| capability | where | state |
|---|---|---|
| Incremental APPEND scan `start-snapshot-id` / `end-snapshot-id` over `(from, to]` | `crates/repark-core/src/time_travel/incremental.rs`, the fork's `IncrementalAppendScan` | shipped (ICE-CHANGELOG-1, C-001…C-008) |
| `t.changes` relation with `_change_type`, `_change_ordinal`, `_commit_snapshot_id`; `create_changelog_view` with carryover removal, net changes, update images | `crates/repark-iceberg/src/catalog/changelog.rs`, `crates/repark-spark/src/call/changelog/` | shipped (C-009…C-015); a range holding row-level delete files refuses, as Iceberg 1.11.0 does |
| `MERGE INTO` with copy-on-write and merge-on-read arms, deletion vectors on v3, schema evolution, the `validate_from_snapshot` OCC anchor | `crates/repark-iceberg/src/write/merge/` | shipped |
| Snapshot-summary properties on write | ICE-WRITE-OPTIONS-1 | shipped; the slot an idempotency marker goes in |
| Stage → validate → conditional commit against an expected base; unknown outcome resolved by walking snapshots for the run key, never retrying a replace | silver-s0 C-008 | proven implementable at the fork pin, not built |
| Typed `SilverPlan`: parse, canonical identity, explain | `crates/repark-core/src/silver/` | S-0 and S-1 on `main`; no execution, not bound to Python |
| `readStream`, `writeStream` | registry rows SES-DECL-readStream, SES-DECL-streams | declared refusals with Spark's error class |
| `withWatermark`, `dropDuplicatesWithinWatermark` | DF-STREAM-BATCH-1 | batch no-ops, as Spark's batch planner does |
| Session owns embedded capture; four shutdown rules | CC-1 | ruled, not built |
| Accepted vs durable; checkpoint advances on durable only; a checkpoint written by one producer generation is never advanced by another | CC-9 | ruled, not built |
| Five crash scenarios as pins before any capture code | CC-6, order C-0 | harness landed (#906) with the five scenario pins |
| No `repark-streaming` crate; the producer is `repark-cdc` at 1.7 | CL-2 | ruled |

The storage side is largely in place. What is missing is the loop around it, the offset contract,
the identity rule for records, and the facade.

## 3. The design

**D-1 · Offsets live in the sink.** Each micro-batch commit into Silver stamps the query id, the batch
epoch and the source snapshot id of every Bronze input into the Silver snapshot summary, and writes
the same offset vector as a table property in the same commit so resume is one read, not a walk.
Restart reads the sink. The crash after the sink commit and before the checkpoint is impossible by
construction, which is why Iceberg's own Flink and Spark sinks do the same underneath. **No checkpoint
directory for an Iceberg sink.** A checkpoint store appears only when the batch body writes somewhere
other than Iceberg. This is a deliberate divergence from Spark's `checkpointLocation`, documented as
such; the facade accepts the option and ignores it for an Iceberg sink, or refuses it, which the oracle
decides.

**D-2 · A batch source over the append scan.** A small Rust type that yields the next window, splits an
oversized window by file count or row count as Spark's `streaming-max-files-per-micro-batch` and
`streaming-max-rows-per-micro-batch` do (offset = snapshot id plus position within the snapshot's added
files), and **fails loud** when the window contains an overwrite or delete snapshot instead of the plain
append scan's silent skip (ICE-CHANGELOG-1 C-003). Spark's `streaming-skip-overwrite-snapshots` and
`streaming-skip-delete-snapshots` spell the opt-in to skipping. A Bronze table that receives row-level
deletes is a contract violation: Bronze is append-only by contract and the streaming query refuses (O-5, ruled).

**D-3 · A Session-owned driver.** A tokio task in `repark-core`, one batch in flight per query, triggers
`availableNow` (drain and stop) and `processingTime` (interval). It obeys CC-1's four rules: registering
does not start; explicit asynchronous shutdown waits for the in-flight batch and reports the durable
offset; a timeout or an ambiguous commit produces an explicit recovery-required outcome; dropping a
Python object is never the only shutdown. The driver's only soft state is the in-flight batch, rebuilt
from the sink on restart.

**D-4 · Idempotent replay.** Before committing a batch the driver checks whether its epoch already
appears in the sink's summary history and skips if so. An unknown commit outcome follows the silver-s0
C-008 recipe. Two drivers started by accident for one sink are safe: the second loses at
`validate_from_snapshot`, every batch still applies once. Row-level dedup is D-5's job, because a
source redelivering rows across *different* batches, which Postgres logical replication does after a
slot restart, is not a batch replay and the epoch check cannot see it.

**D-5 · Deterministic identity on every record.** Two ids, both UUIDv5 (or v8 over a truncated SHA-256)
over canonical typed bytes under a RePark namespace, computed by a vectorised Arrow kernel in
`repark-common` since cdc, core and the facade all need it:

- the **event id** on Bronze: source generation (CC-2), source table identity, canonical primary key,
  source position (LSN or commit position plus operation for capture; business key plus the source's
  version column for the daily batch Bronze). Duplicate delivery yields the same id, so dedup is a
  `MERGE` or a distinct on one column;
- the **version id** for Silver: the same without the operation. The SCD2 close-and-open logic keys on
  it and it survives any batching and any replay.

What never goes in: ingestion timestamp, attempt id, batch epoch, DAG run id, anything the pipeline
minted rather than the source. This is the silver compiler's plan-id / run-key / attempt-id split (§6)
applied to rows. Canonical encoding is the whole contract: typed key bytes, decimals with scale,
timestamps with zone, NULL distinct from absent, no string normalisation; the method is the one
`crates/repark-core/src/silver/identity.rs` already uses for plans. A primary-key change mints a new
identity and follows the accepted key-change rule. A keyless table falls back to a whole-row hash that
collapses true duplicates; a composite key is not keyless; a genuinely keyless table needs a declared
answer (O-6). Iceberg v3 row lineage is complementary, not a substitute: the table assigns `_row_id` at
commit, physically, and it neither crosses tables nor lets Trino recompute it from the source. The
function is RePark-owned with a dated registry row (CC-4's pattern), not on a Spark name, because Spark
has no `uuid5` builtin and the parity matrix cannot carry a function Spark lacks. The id is an identity,
not a security primitive; SHA-1 truncation is acceptable and the docs say so. **It goes into the Bronze
contract now**, before the capture producer exists, so the daily batch Bronze and the future capture
Bronze share one rule and Silver is designed against it once.

**D-6 · Catalog-only state.** Every durable bit lives in Iceberg, and the catalog's atomic commit is the
coordination primitive, so a second machine copies nothing. Offsets: the sink summary and property
(D-1). Ids: computed, never stored as state. Capture position: the Postgres replication slot holds it
and retains WAL until acknowledged; the Bronze commit stamps the LSN it covered; acknowledgement follows
the durable Bronze commit (CC-9). Fencing: OCC plus the generation field; no lease store while one driver
owns one sink, the unified plan's "one unfinished job per pair" starting rule. Batch intent before
effect: staged files are invisible until the commit. Plans, policies, configuration: deployed artifacts.
Quarantine, accounting, evidence: tables. Under distributed execution the driver is the coordinator,
executors run the batch body stateless and write to object storage, and the coordinator commits once,
the shape `repark-distributed` already has. **A shared catalog is required** (Glue, S3 Tables, the
Postgres catalog, REST); a filesystem catalog on local disk is dev-only and a streaming query against
one refuses to start. A control database earns its place only at multi-writer per sink (2.x), and then
the Postgres already behind the catalog is its home.

**D-7 · Two transform doors, `foreachBatch` first.** A callable over the batch frame lets an existing
Silver job body run unchanged while the runtime owns offsets, triggers and exactly-once; this is what
makes "take a current Bronze and Silver setup" cheap. The declarative `SilverPlan` (S-2 onward) is the
second door and the long-term one. A Silver table fed by several Bronze tables has a **vector offset**;
inside a batch the changed rows of one input join the other inputs pinned at their own end snapshots,
and late arrivals re-process through `MERGE` idempotency.

**D-8 · The facade.** `readStream.format("iceberg")`, `writeStream`, `trigger`, `foreachBatch`,
`StreamingQuery` with `start`, `awaitTermination`, `stop`, `lastProgress`; the SES-DECL rows flip from
refusal to answer and the three IPI-47 cells get measured. Rust first: the runtime, the offset contract
and the commit protocol are Rust; Python forwards builders.

**Operational costs, stated up front.** A short trigger produces thousands of Silver snapshots a day, so
`expire_snapshots` and compaction policy are part of the design. Merge-on-read sinks accumulate position
deletes and need `rewrite_data_files` on a schedule. One catalog commit per batch per table is the hot
path; Glue rate limits are the first wall and the Postgres or REST catalog the answer.

## 4. Why not one lane, one shot

The owner asked whether a detailed enough prompt could land this in one Opus 5.5 pass. No, for
structural reasons: the build is four to five units the size of ICE-CHANGELOG-1 (that unit landed about
1,750 lines of product Rust and 840 of Python in one day, then needed a remediation round); a verifier
reading one PR of that size has no catch rate, and the review pipeline ruling is one scoped verifier per
product PR with one PR per slice; the v1.6.0 card says every design question is answered from recorded
Spark plus Iceberg cells, and ICE-CHANGELOG-1's careful packet still got two of ten decisions wrong
until the lane read the bytecode; exactly-once is a proof, not a feature ("do not advertise exactly-once
until the crash cases pass"), and the harness that proves it decides MB-2's shape; the likely fork
changes are a separate repository and pin cycle; and the decisions in §7 are the owner's. The hours
spent on the prompt are not wasted: they go into a **design packet** in the shape of the ATTR-ID-1
design order (`task/wo/attr-id-1/`, the grade-B template) that fixes the decisions, the oracle cells,
the fork asks, and the file list and gates per slice, so each slice runs in fewer rounds.

## 5. The slices

Each slice is one card and one PR. Lane-days are rough sizing for one Opus 5.5 lane including its
verifier round; they are not dates.

| slice | what | owns / touches | gates and pins | tier | lane-days |
|---|---|---|---|---|---|
| **Packet** | the decisions of §7 ruled, the oracle cell list, the fork asks, the slice orders with file assignments | `task/wo/microbatch/` | the owner reads the sketch before any executor opens | the delegate and the owner; then an Opus 5.5 sketch | 0.5–1 |
| **MB-0** oracle | record on Spark 4.1.2 + Iceberg 1.11.0 the cells the v1.6.0 card's step 0 lists, plus `foreachBatch` into an Iceberg sink, restart after a committed and after an uncommitted batch, and what the snapshot summary carries per micro-batch | a recorded-oracle JSON beside the tests | cells committed verbatim with a SHA-256 | clerk (Muse at max) | 0.5 |
| **Fork** | a fail-loud option on the append window when it holds a non-append snapshot; summary properties on the row-delta commit path; the pin bump | the fork repository, then `Cargo.toml` | the fork's own ledger; `make verify` on the bump | Opus 5.5 or Grok 4.7 guided, fork repo | 1–2 |
| **Contracts** | the offset type, the batch-source trait, the summary key names, the `StreamingQuery` states, the registry error rows; no behaviour | new modules only: `repark-common` (identity, offset), `repark-core` (traits) | compiles, DAG gate, map.md lockstep | Opus 5.5, small | 0.5 |
| **MB-1** source | the batch source over the incremental append scan: next window, caps, fail-loud, offset arithmetic | new module under `crates/repark-core/src/time_travel/` | pins over the ICE-CHANGELOG-1 fixtures: window arithmetic, split by files and rows, refusal on an overwrite inside the window | Muse at max or Grok 4.7 guided (the scan exists; the packet fixes the shape) | 1 |
| **MB-2a** sink and offsets | summary stamping and the offset table property on `MERGE` and append commits in the same transaction; resume-from-sink | new module beside `crates/repark-iceberg/src/write/merge/` | pins: the stamps round-trip; resume finds the last offset in one read | Opus 5.5 | 1–1.5 |
| **Harness** | the crash harness: kill between sink commit and the next trigger, duplicate delivery, two drivers on one sink, unknown-outcome reconcile, a Bronze overwrite inside a window; test-only, written before MB-2c | `python/repark/tests/` | the five scenarios are red until MB-2c | Muse at max | 1 |
| **MB-2c** replay and reconcile | the epoch check before commit; the C-008 walk on `CommitStateUnknown`; generation fencing | the MB-2a module | the harness goes green | Opus 5.5, same lane as 2a | 1–1.5 |
| **MB-3** driver | the Session-owned task, both triggers, the four shutdown rules, progress reporting | new `crates/repark-core/src/microbatch/` | pins: available-now drains and stops; processing-time ticks; shutdown reports the durable offset; drop does not stop | Opus 5.5 | 1–2 |
| **MB-4** facade | builders, `StreamingQuery`, error classes, the SES-DECL flips, the IPI-47 cells; surface first against a stub binding, wire-up last | `python/repark/src/repark/spark/`, `crates/repark-python/src/` new module | the MB-0 cells; the registry rows | Muse at max | 1–2 |
| **MB-5** multi-source and `SilverPlan` hook | vector offsets; inputs pinned at their end snapshots inside a batch; the hook for S-2 | the MB-3 module | pins: a two-input fact with late arrival on one input converges | Opus 5.5 | 1–2 |
| **Identity** | the `uuid5` kernel and the RePark-owned function with its registry row; the Bronze contract clause | `repark-common`, `repark-functions`, `docs/` | goldens over the canonical encoding: decimals, zones, NULL vs absent, composite keys, a key change, a recreated table (generation) | Muse at max; Opus medium verifier | 1 |
| **Acceptance** | the owner's clinic Bronze tables into Silver through `foreachBatch`; duplicate delivery; a kill between sink commit and the next trigger; the SCD2 dimensions identical to the existing Glue job's | a live cell under the Spark-gated tier | the two outputs diff to zero rows | Muse at max | 1 |

Critical path: packet → contracts → MB-2a → MB-2c → MB-3 → MB-5 → acceptance, about seven lane-days.
Sequential, everything is ten to fourteen.

## 6. Parallel waves and merge order

The width opens after the contracts PR, because every other lane then builds against fixed names and
merges cleanly. The packet pre-assigns file paths so no two lanes edit one file; `session.rs`, `lib.rs`
and `merge.rs` sit at or near their size ceilings, so new modules are the only safe place to add code
anyway. The crash harness is written before the code it tests, the CC-6 shape. Fork work is a different
repository and therefore free parallelism.

| wave | Opus 5.5 lane | Muse at max / Grok 4.7 guided | fork repository |
|---|---|---|---|
| 0 | the sketch from the packet; the owner reads it | MB-0 | file the two asks |
| 1 | contracts, then MB-2a | harness; MB-1; identity | the two changes and the pin bump |
| 2 | MB-2c, then MB-3 | MB-4 surface against a stub | — |
| 3 | MB-5 | MB-4 wire-up; acceptance | — |

Merge order: contracts, fork pin, then MB-2a, MB-1, harness and identity in any order, then MB-2c,
MB-3, MB-4, MB-5, acceptance. A lane that finishes early waits in the queue rather than rebasing
repeatedly. Ceilings that bind: four Muse lanes (box load); three parallel compiling lanes before the
build slot queues; one scoped Opus verifier per product PR, so five product PRs is five verifier runs,
and the test-only and docs PRs skip it. A second Opus lane for MB-1 and MB-3 beside MB-2 shortens the
path by about a day and needs the owner to lift the one-Opus-lane cap for this build (O-8).

## 7. Decisions for the owner

| id | decision | proposed | status |
|---|---|---|---|
| O-1 | offsets live in the Silver snapshot summary and a table property; no checkpoint directory for an Iceberg sink | yes (D-1) | proposed 2026-10-04 |
| O-2 | the deterministic event id and version id enter the Bronze contract now, before the capture producer | yes (D-5) | proposed 2026-10-04 |
| O-3 | catalog-only state; no control database before multi-writer; a shared catalog is required and a local filesystem catalog refuses a streaming query | yes (D-6) | proposed 2026-10-04 |
| O-4 | the release slot: pull the micro-batch sink from the 2.2 row into **1.7** beside `repark-cdc`, since the producer writes Bronze and this driver consumes Bronze into Silver and both share CC-9 | 1.7 | proposed; implied by O-9, the owner confirms; the release-roadmap rows change in the PR that rules it |
| O-5 | a Bronze table that receives row-level deletes | refuse | **ruled 2026-10-04** (owner: "No deletes in bronze"): Bronze is append-only by contract; a delete or overwrite snapshot inside a window is a contract violation and the query refuses; no opt-in skip |
| O-6 | a genuinely keyless source table under D-5 | refuse | proposed |
| O-7 | the next Opus slot is promised to the STAMP-2 re-measure and then the TA single-series sketch; the micro-batch sketch queues behind them unless the owner swaps the order | keep the order | open |
| O-8 | a second Opus lane for this build | no | **ruled 2026-10-04** (owner: default): one Opus lane; the sequential path |
| O-9 | does the 1.7 sink half open before the 1.6 connectors, since it depends only on Iceberg? | no | **ruled 2026-10-04** (owner: "Sink waits for 1.6"): the 1.7 row keeps its dependency on 1.6; the micro-batch executors open after the first 1.6 connector units; the packet, MB-0 and the fork asks need no Opus lane and run meanwhile |
| O-10 | an HTML artifact copy under `docs/artifacts/` in the ruling change, as the 2026-10-01 adjustment carried | yes | **ruled 2026-10-04** (owner: default) |

## 8. The order things happen (no dates)

1. The packet: §7 ruled, the oracle cell list, the fork asks, the slice orders with file assignments
   (the delegate with the owner; no lane).
2. MB-0 on the free Muse slot, as soon as the parity-live lane hands back.
3. The two fork asks filed and the fork PR opened.
4. The Opus slot already committed: #937 lands, the STAMP-2 three-run median is re-measured against the
   repaired `main`, then the TA single-series sketch (or the swap under O-7).
5. The first 1.6 connector units (O-9); the micro-batch executors do not open before them.
6. The micro-batch design sketch from the packet; the owner reads it.
7. The contracts PR.
8. MB-2a, with the harness, MB-1 and identity in parallel on Muse.
9. The fork pin bump merges.
10. MB-2c; the harness goes green.
11. MB-3; MB-4 surface in parallel.
12. MB-4 wire-up.
13. MB-5.
14. Acceptance on the owner's clinic pipeline.
15. The release-roadmap rows and `STATUS.md` updated in the PR that ships MB-5, naming the carve-out
    from IPI-47 closed on the three cells.

## 9. Rules that bind every slice

The comment ban on every actor (no comments in Rust, Python, shell, TOML or YAML; reasons live in
`map.md`); measure before ruling; recorded-oracle pins with a live leg; one scoped Opus medium verifier
per product-Rust PR and none on test-only or docs PRs; relocated code sheds its comments; the Rust-first
ruling (the runtime, the offset contract, the commit protocol and the identity kernel are Rust; Python
forwards builders and the `foreachBatch` callable); the fork rules; probes inside the worker slice or
under a 64 GiB virtual-memory cap; the live-cell rules for the acceptance cell.

## Leaves this directory when

The packet exists under `task/wo/microbatch/` with §7 ruled, and the cards MB-0…MB-5 are filed; then
this file is the dated record and the orders carry the contracts.
