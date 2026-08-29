# Roadmap intake — 2026-08-23

Two mid-term roadmap additions evaluated on the same day, each against the engine this
repository actually pins: **Track A** the six window-operator optimizations DuckDB published
(should this engine add them?), and **Track B** production-grade readiness of Iceberg
merge-on-read upserts and table management at format v2 (what is still between the engine
and "production"). Track C is the pointer to the fork-side handoff both tracks feed.

---

# Track A — window-operator optimizations (the DuckDB list)

**What this is.** A mid-term roadmap addition, evaluated against the engine this repository
actually pins. The owner asked whether the six window-operator optimizations DuckDB published on
2025-02-14 (Richard Wesley, "window function performance" — segment-tree vectorization, constant
aggregation, streaming windows, partition-major evaluation, out-of-memory operation, shared
expressions) should be added to this engine. This note records the evaluation and the candidate
units. It is an **intake**, not a plan of record: [../STATUS.md](../../../STATUS.md) stays the SSOT,
and each unit below graduates into a brief under [../briefs/](../../../briefs/map.md) only when the
owner charters it. Companion to [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md)
(whose A7 performance track points here).

**Evidence date.** Every DataFusion claim below was verified 2026-08-23 by reading the
**DataFusion 54.1.0** sources the workspace pins (`datafusion-physical-plan`,
`datafusion-physical-expr`, `datafusion-functions-aggregate`, `datafusion-optimizer`), not from
memory. Re-verify on any family bump — the version-pin contract in
[../AGENTS.md](../../../AGENTS.md) "Version-pin contract" means these statements have a shelf life.

**Retirement event.** Track A closes when **W-0 is chartered** (its content moves into the W-0
brief) or when the owner declines the track (a dated line in
[../briefs/next-sequence.md](../../../briefs/next-sequence.md) "declined armings"). Track B closes
when **MW-5 merges** and the MW-6…MW-9 candidates below are each chartered or declined. The
file is archived to [../docs/history/](../../../docs/history/map.md) when the later of the two
closes, with the rulings at its top.

---

## 1. The framing constraint

This engine **does not own a window operator**. DataFusion does, and "DataFusion is built ON,
not forked" is a [../PROJECT.md](../../../PROJECT.md) non-negotiable. So every DuckDB item lands in
exactly one of three homes:

1. **Already in DataFusion 54.1** — nothing to build; prove the path is taken.
2. **Repark-owned** through DataFusion's public extension points — analyzer / physical-optimizer
   rules, a custom `WindowExpr` (public trait; `WindowAggExec::try_new` is public), or ordering
   provenance from the `TableProvider`.
3. **Upstream `apache/datafusion`** — operator internals this engine should not own; contribute
   or wait, and pick up at the next family bump.

And the north star is not "a faster window operator than DuckDB". It is deepest Iceberg, bit-exact
TA at `polars_talib` parity, a facade that migrates pipelines unchanged, and "spills where it can,
documented where it cannot". Each item is scored against *those*. DuckDB's own deltas (≈4× for
segment-tree vectorization is Baseline → Fan Out *inside DuckDB*) do not transfer and are not
targets here.

## 2. Item by item

| DuckDB optimization | DataFusion 54.1 today (verified) | Value to the north star | Home |
|---|---|---|---|
| Segment-tree vectorization | No segment trees. Two strategies only: `PlainAggregateWindowExpr` (frame starts at `UNBOUNDED PRECEDING`, append-only) and `SlidingAggregateWindowExpr` (any other start → `update_batch` + `retract_batch`). A **non-retractable aggregate over a sliding frame refuses loud** (`create_sliding_accumulator` → `not_impl_err!`). | **High — a functional parity gap, not a perf gap.** | repark-owned `WindowExpr`, or upstream |
| Constant aggregation | Present: `AggregateWindowExpr::is_constant_in_partition` — an `UNBOUNDED`/`UNBOUNDED` frame computes once per partition and emits a constant column. | Done. | pin / bench only |
| Streaming windows | Present and more general: `BoundedWindowAggExec` streams in bounded memory whenever the input ordering satisfies PARTITION BY + ORDER BY (`InputOrderMode::{Sorted, PartiallySorted, Linear}`). What it cannot do is **skip the sort**. | **High — the TA serving shapes are sort-dominated.** | repark-owned: ordering provenance |
| Partition-major parallel evaluation | Hash-repartition on the PARTITION BY keys across `target_partitions`; no PARTITION BY → `Distribution::SinglePartition` (one thread, whole relation). One large partition is never split across cores. | Medium. | upstream |
| Out-of-memory operation | `WindowAggExec` never spills (zero `spill` references in `physical-plan/src/windows/`); `SortExec` spills; the bounded exec is bounded-memory by construction when the input is sorted. | Medium — an unfilled spill-matrix row. | measure + document; upstream for operator spill |
| Shared expressions | `CommonSubexprEliminate` already handles `LogicalPlan::Window` (shared sub-expressions hoist into a projection beneath the window); functions over the same (PARTITION, ORDER) collapse into one exec and read the same input columns. | Low — the TA multi-output cache *is* this engine's hand-rolled instance. | instrument what exists |
| Future work (window → self-join rewrite, smart `arg_max`) | Absent. | Speculative. | skip |

### 2.1 Segment trees — for this engine a correctness gap

Spark never refuses an aggregate over a frame: when an aggregate has no inverse, its
`SlidingWindowFunctionFrame` re-scans the frame per row (O(n·w)). DataFusion refuses. This
repository has already hit that wall once — `crates/repark-functions/src/aggregate.rs` exists
because Spark-typed `AVG` died on `ROWS BETWEEN … PRECEDING` and a retract-capable kernel was
written by hand (pinned by `python/repark/tests/test_sliding_avg_parity.py`). Every
`collect_list / first / last / percentile_approx / max_by / …` `.over(w.rowsBetween(…))` in a
migrating pipeline is a loud failure today, closed one kernel at a time.

For `sum / count / avg / min / max / stddev / var` the retract path is O(n) and a segment tree
buys nothing. The decision is the **fallback for non-retractable accumulators**:

- Spark's re-scan: simplest, O(n·w), matches the facade's oracle exactly.
- DuckDB's segment tree: O(n log n), more code, faster on wide frames.

Either is implementable without forking: the refusal surfaces at **execution** time
(`SlidingAggregateWindowExpr::get_accumulator`), so a `PhysicalOptimizerRule` that swaps the
`WindowExpr` runs first. It is also a clean upstream contribution. Measure (W-0) before choosing.

### 2.2 Streaming — the lever is ordering provenance, not a new operator

DuckDB streams only with no PARTITION BY and no ORDER BY. DataFusion streams whenever the input
is already ordered. Neither skips the sort, and this engine's TA shapes (many symbols, last-row
serving) pay an O(n log n) `SortExec` before every `OVER (PARTITION BY symbol ORDER BY ts)`
because an Iceberg scan advertises no ordering.
`crates/repark-core/src/sorted_view.rs` (SE-1: declared-sorted temp views, verify-then-advertise
so `EnforceSorting` elides the sort) is the prototype. The north-star version is **Iceberg sort
order / partition spec → `TableProvider` output ordering**, so a table written sorted by
`(symbol, ts)` runs its windows with no sort and in bounded memory. That is fork +
`iceberg-datafusion` work this project owns, and it is already named in
[roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md) A7 as the "Iceberg declared-sort-order
plumbing" remainder.

The TA window UDFs use `PartitionEvaluator::evaluate_all` (the whole ordered partition) and can
never stream. That is correct — the kernels are stateful full-series functions — and they are
not a target.

### 2.3 Partition-major parallelism — measured, and upstream

P-2 ([p2-ta-pipeline-benches-ledger.md](../../ledgers/archive/2026-08/2026-08-15-p2-ta-pipeline-benches-ledger.md)) measured that the
*partitioned* shape is already fine: `target_partitions=64` + `partitionBy` beats polars `.over`
(26 vs 52 ns/row). The unmeasured shape is the **unpartitioned** `Window.orderBy(ts)` — common in
migrated pipelines (Spark warns about it too) and part of the single-symbol host tax (ema 72.5 vs
9.7 ns/row). Splitting one partition across threads with prefix-merge is deep operator work;
[../AGENTS.md](../../../AGENTS.md) "the smallest readable design wins" does not let this engine own it.
File upstream with the W-0 numbers.

### 2.4 Out-of-memory — an unfilled row of the spill-coverage matrix

S-1 (#143) installed the FairSpillPool and made the "one truth" claim true. The window operator
is the next row: a full-partition window over a series larger than the pool fails, and the
matrix should say *how* — per [../PROJECT.md](../../../PROJECT.md) "spills where the engine can,
documented where it cannot". Guaranteeing ordering (§2.2) routes more queries through the
bounded-memory exec; that is the repark-owned half.

### 2.5 Shared expressions — instrument, do not rebuild

The repark instance is the thread-local multi-output cache in `crates/repark-ta/src/udf/mod.rs`
(BBANDS / MACD / STOCH siblings share one kernel run), whose hit/miss counters are still
unpublished (F-P1-1 / F-P2-2). Instrument before touching. If the numbers say the cache misses,
a single multi-output UDF + struct projection is the CSE-friendly replacement.

## 3. The ruling proposed to the owner

Adopt DuckDB's list as a **measurement battery and a parity checklist, not an implementation
slate.** Candidate units, dependency-ordered, all measure-first under the performance campaign's
standing rule ([../PROJECT.md](../../../PROJECT.md) Goals, "measure first, then implement"):

| Unit | Scope | Size | Home |
|---|---|---|---|
| **W-0 — measure** | A window-shape bench modelled on the post's own queries: sliding frames per aggregate class (retract vs not), constant frame, unpartitioned ORDER BY at 1e7 rows, `lead`/`lag` over an unsorted Iceberg scan, a window over > `memory_limit`. Two oracles — the pinned PySpark 4.1.2 and DuckDB (PROJECT.md roadmap item 4 already names DuckDB). Output: numbers **plus the enumerated list of Spark aggregates that refuse over sliding frames**, each as a registry row. No product change. | S/M | repark |
| **W-1 — facade parity** | The fallback for non-retractable accumulators over sliding frames: Spark re-scan vs segment tree, decided on W-0's counts. One `WindowExpr` behind a physical-optimizer rule — **not** a window operator. Entry-point-matrix pins per aggregate class on the Arrow path, value AND type. | M | repark (or upstream) |
| **W-2 — TA north star** | Ordering provenance: Iceberg sort order → DataFusion output ordering, extending SE-1, so `BoundedWindowAggExec` runs without a `SortExec`. The sort pins re-anchored to execution-layer evidence (the PR-D3 remainder in A7). | M/L | repark + fork |
| **W-3 — truth** | The window row of the spill-coverage matrix: which window shapes exceed the pool, how each fails, documented in the guide. | S | repark |
| **W-U — upstream** | Partition splitting and `WindowAggExec` spill: issues against `apache/datafusion` carrying W-0's numbers; picked up at the family bump, never by forking. | — | upstream |

**Constraints to state at charter time:**

- A repark-owned window *operator* is exactly the "parallel manager introduced to look
  extensible" that [../AGENTS.md](../../../AGENTS.md) calls a defect until a second real caller earns
  it. W-1 is one `WindowExpr`, not an exec.
- Segment trees are irrelevant to the TA kernels regardless: they are not aggregates, and the
  goldens forbid any reassociation (`crates/repark-ta/src/lib.rs` numerics contract).
- W-0 runs on the same noisy `schedutil` box as P-2 — ratios over absolutes, and the numbers live
  planning-side, as P-2's do.
- `[OWNER]` Sequence against the open queue in [../briefs/next-sequence.md](../../../briefs/next-sequence.md)
  (MW-4/MW-5 on OD-3, the V3 track, FNP remainder). This note does not decide that.

## 4. Out of scope

- The DuckDB window → self-join rewrite and `arg_max` tricks (post's "future work").
- Any change to the TA kernels' numerics.
- Window frame R4 (120 vs 90, `EXCLUDE`): parked in A7 until a DataFusion-compatible seam
  appears; do not bump dependencies for it.

---

# Track B — Iceberg merge-on-read readiness at format v2: the post-MW-4 remainder

**What this is.** The owner asked whether Iceberg MOR with upserts and table management is
production-grade on the v2 spec. The evaluation (2026-08-23) is recorded here as the roadmap
remainder. **MW-4 merged as [#218](https://github.com/TRO-Wolf/repark/pull/218) while this was
being written** (2026-08-23): the **Glue** MOR leg — unique `testing_mw4_mor_*` table, CTAS at
merge-on-read, MERGEs that strand position-delete files, compact + expire, Arrow row parity —
with a memory-catalog analog that always runs. Two things #218 deliberately did **not** do,
and this track carries: the Glue test is skip-gated locally, so **the live proof is the first
post-merge `aws-acceptance` dispatch**, and **S3 Tables MOR compact + expire is out of MW-4**
(OD-3 grants `s3:DeleteObject` on the Glue warehouse prefix only; table-bucket delete stays
denied). Everything below is what follows #218.

**Evidence date.** Verified 2026-08-23 against the tree, the divergence registry, the MW design
and charter, the tier-2 acceptance harness, and the owned fork's `ENGINE_CONTRACT.md` /
`docs/parity/GAP_MATRIX.md` **at the pinned rev** (`0c5fd58`). Fork capability status lives
only in the fork; rows are cited, never restated.

## 1. The verdict

| Surface | Verdict | Basis |
|---|---|---|
| COW upsert on Glue / S3 Tables | production-grade for a bounded pilot | live-proven: CTAS → MERGE → identical MERGE idempotent, both catalogs (`python/repark/tests/test_aws_acceptance.py`) |
| MOR write path (MERGE / UPDATE / DELETE), correctness | production-grade | MW-0: 1,000 rows stay 1,000 across ten MOR merges; OCC / abort / cardinality / store-assignment batteries closed (BL-3, BL-4, BL-5) |
| MOR operability (compact / expire / orphan) | operable since MW-1…3 (2026-08-21); **unproven at scale** | five procedures wired; only the 1,000-row demo exists and MW-5 has not re-measured it |
| MOR on Glue | **code merged (#218); live proof pending** | the Glue leg is skip-gated locally; the post-merge `aws-acceptance` dispatch is the evidence — until it is green, the claim is unproven |
| MOR on S3 Tables | **no leg** | out of MW-4 by OD-3's scope (no table-bucket `DeleteObject`); needs an owner ruling (OD-3b) before a unit exists |
| Table-management breadth | sufficient for the upsert lifecycle; named gaps in §2 | `rewrite_manifests`, partition-scoped overwrite, `NOT MATCHED BY SOURCE`, delete granularity |
| Concurrency | single-writer-per-table only (standing rule) | OCC pinned; serializable over-rejects by design (DML-5); S3 Tables compacts service-side |

**The engineering for MOR at v2 is done; the evidence is not.** The critical path is the
post-#218 `aws-acceptance` dispatch (owner action) → MW-5. The interim posture in [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md)
A2 ("Interim posture") stands unchanged until MW-5 lands.

## 2. What is solid, and what is not

**Solid (pinned, measured):** the RePark-owned MERGE executor with COW and MOR arms; identity
DELETE/UPDATE through the same commit arms honoring `write.delete.mode` / `write.update.mode`;
default `serializable` with `validate_no_conflicting_data` and the documented `snapshot`
opt-down; rejected commits delete the files they wrote (paths from writer results);
`CommitStateUnknown` skips cleanup and the fork reconciles it in-process (`commit.status-check.*`,
fork R157); position deletes stamped with the deleted data file's `(spec_id, partition)` (M16,
fork §7a); the BUG-001 valve; five `CALL` procedures with oracle-pinned result schemas; reads of
other engines' position **and** equality deletes (fork R117), including Spark-written v3 Puffin
vectors; schema / partition-field / branch-tag DDL; time travel; metadata tables.

**Not solid, ranked by how much a production upsert workload feels it:**

1. No live MOR evidence yet (the #218 Glue leg awaits its dispatch; S3 Tables has no leg) and
   no re-measured baseline (MW-5).
2. Scale unmeasured: 1,000 rows × 10 merges is a correctness demo. No number for 1e7 rows /
   100 merges / partitioned, and nothing for manifest growth.
3. `rewrite_manifests` not wired (fork R100 ✅). Every MOR merge adds manifests; operators
   run it routinely. Engine-side wiring only — the MW-2/MW-3 shape.
4. MOR-2 — `write.delete.granularity` ignored; the engine always writes partition-granularity
   deletes where Spark's default is per file. Contents identical, layout not; matters on tables
   Spark also writes.
5. `WHEN NOT MATCHED BY SOURCE` missing — blocks full-sync / SCD-2 patterns. **dbt-track
   flag:** dbt's `merge` incremental strategy is fine; `delete+insert` is the non-atomic pattern
   the A2 "never" list forbids.
6. Partition-scoped overwrite refuses (DML-1, `overwritePartitions()`) — fork
   `ReplacePartitions` R104 🟡, not wired. Common in backfills.
7. Concurrency: serializable validates against any concurrent append (DML-5, fail-closed); under
   S3 Tables service compaction that is routine retries, documented by MW-1. No engine-level
   MERGE re-run loop (same as Spark).
8. Smaller, real: `rewrite_data_files` has no `where` / `sort_order` / strategy (R135
   deferred); `expire_snapshots` is `ReachableFileCleanup` only (fork R133 🟡: no
   `IncrementalFileCleanup`, `cleanExpiredMetadata`, ref-age); `remove_dangling_delete_files`
   and `convert_equality_delete_files` not procedures (fork ✅ both); branch-targeted writes
   refuse (REF-1, fork API); TRUNCATE refuses (DML-2); MOR-1 (more aggressive than Spark,
   contents identical); MOR writes refuse on v3 by design (A12 owns v3).

## 3. The units proposed, dependency-ordered

| Unit | Scope | Size | Depends on |
|---|---|---|---|
| **Dispatch** (owner action, not a unit) | Run `aws-acceptance` on merged `main` and record the Glue MOR leg's result in STATUS. Green = the first live MOR evidence; red = a finding for MW-5 before anything else. | — | #218 |
| **MW-4b — S3 Tables MOR leg** (chartered 2026-08-28 as **MW-10**, [ledger](../../ledgers/staging/mw-10-s3tables-mor-ledger.md); OD-3b IAM applied the same day) | The same helper against S3 Tables. **`[OWNER]` OD-3b:** table-bucket `DeleteObject` on the acceptance role's scratch table bucket. Without it the unit cannot exist; with it the unit is the Glue leg's twin plus the service-compaction conflict-retry path exercised for real (DML-5 / fork contract §8). | S/M | OD-3b |
| **MW-5** (chartered, queued) | Registry rows, STATUS scorecard, guide/map lockstep, **re-run the MW-0 demo after `rewrite_position_delete_files` + `rewrite_data_files` + `expire_snapshots` and record the delta** — the 2.1× must come back down, and the number is the campaign's closing evidence. | S | the dispatch |
| **MW-6 — `rewrite_manifests`** | Wire `CALL system.rewrite_manifests` over the fork action (R100 ✅). Oracle-pin Spark's result schema (`rewritten_manifests_count:int`, `added_manifests_count:int`) from the jar constant if the 4.1.2 oracle cannot execute it (Q5 precedent). **Handoff F-4 answered (2026-08-23):** the fork's result carries no counts; read `manifests-replaced` → `rewritten_manifests_count` and `manifests-created` → `added_manifests_count` from the new snapshot's summary (fork-pinned keys). `spec_id` refuses loud (no fork filter); `use_caching` is a documented no-op. | S/M | none — can start now |
| **MW-7 — scale measurement** (measure-only) | A partitioned v2 table, 1e7 rows, 100 MERGEs touching ~2 % of rows each, MOR and COW legs: delete files, manifests, manifest-list size, `COUNT(*)` and a predicate scan p50/p99 per 10 merges, then the full maintenance sequence and the same scans again. Peak RSS. Numbers planning-side like P-2; ratios over absolutes on this box. **Gates MW-8's defaults and decides whether MW-9 is urgent.** | M | MW-6 (so manifests are measurable) |
| **MW-8 — the maintenance runbook** | "Table management" in production is a procedure *sequence*, not six procedures. A guide section and an executable local-catalog test of the Airflow-shaped sequence: merge → `rewrite_position_delete_files` → `rewrite_data_files` → `rewrite_manifests` → `expire_snapshots` → `remove_orphan_files` dry-run → armed; the S3 Tables conflict-retry guidance folded in; defaults set from MW-7's numbers. Ships as docs + one test, no engine change. | S | MW-6, MW-7 |
| **MW-9 — MOR-2 close** | Honor `write.delete.granularity` (`file` / `partition`) in the merge-on-read writer (`crates/repark-iceberg/src/write/position_delete.rs` grouping), Spark's default `file`. Write-path work — charter separately from maintenance, with entry-point-matrix pins on delete-file counts per mode and an oracle row for the default flip. | M | MW-7's numbers say whether per-file layout costs scan time here |
| **RP-1 — fork repin (F-1 + F-8a)** | Re-pin to a fork rev at or past `e69f7b0a`. **F-1 landed fork-side (floor 2 → 5, a breaking default):** flip `call_mor1_compacts_below_sparks_min_input_files_floor` to equality, retire MOR-1, check no engine test leaned on two-file compaction. **F-8a:** retire the `a$b` "unresolvable" residue note in `crates/repark-iceberg/map.md`; the ADR-0006 enumeration filter **stays** (the fork still synthesizes in `table_names`). Both standing repin duties re-run. Also pin whether the `snapshot` isolation arm is exposed to the fork-found conflict-guard gap (handoff F-0). | S | fork F-0 landed, ideally |
| **DML-A — `WHEN NOT MATCHED BY SOURCE`** | Engine MERGE surface (RePark-owned): the third arm, COW and MOR, cardinality and store-assignment gates reused, the DML-3 boundary moves. Schedule on workload need (SCD-2 / full sync / dbt). | M | none |
| **DML-B — partition-scoped overwrite** | `INSERT OVERWRITE … PARTITION (…)` static + dynamic and `writeTo().overwritePartitions()` over fork `ReplacePartitions`. **Blocked on fork handoff F-5** (R104 remainder). | M | handoff F-5 |
| **DML-C — TRUNCATE** | A dedicated truncate over an empty overwrite (the DML-2 substitute made first-class). | S | none |
| **Watch, do not schedule** | `remove_dangling_delete_files` / `convert_equality_delete_files` as procedures (equality deletes only arrive from other engines); `rewrite_data_files` `where` / `sort_order` / strategy (R135); `expire_snapshots` incremental cleanup (fork R133 remainder); branch-targeted writes (fork handoff F-6). | — | — |

**`[OWNER]` sequence** against [../briefs/next-sequence.md](../../../briefs/next-sequence.md): the
recommendation is MW-6 now (it needs nothing the dispatch produces), MW-5 as soon as the
dispatch has a result, then MW-7 → MW-8, with MW-4b on the OD-3b ruling and MW-9 and the DML
units by workload need.

## 4. What "production-grade MOR at v2" means when this track closes

1. The #218 Glue leg green on a post-merge dispatch, and an S3 Tables leg (MW-4b) or a dated
owner ruling that S3 Tables MOR is out of scope. 2. MW-5's delta recorded and the scan cost back at its
post-compaction floor. 3. MW-7's scale numbers on the record. 4. `rewrite_manifests` wired.
5. The MW-8 runbook tested. 6. MOR-2 closed before any table is shared with a Spark writer.
The "never" list in A2 stays: no blind retry after `CommitStateUnknown`, no merge-mode flip on
a CDC-consumed table, no DELETE-then-INSERT upsert, no multi-writer on one table.

---

# Track C — the fork-side handoff

Both tracks surface work that belongs in the owned `iceberg-rust` fork, not here. It is
collected once — with the engine-side evidence, the pinned rev, the consumed surfaces and an
acceptance per item — in [iceberg-rust-handoff-2026-08-23.md](iceberg-rust-handoff-2026-08-23.md),
the document handed to the fork orchestrator. This intake carries only the pointer; the fork
items are **not** restated here, and [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md)
A8 points at the same handoff.
