# Charter ledger — H3-SPILL-RESIDUE-1 · the two Never-OOM failure shapes

**Date:** 2026-09-06 · **Branch:** `harden/h3-spill-residue-1` · **Base:** `origin/main`
`282607f5` · **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Risk tier:** `risk_tier: elevated` (product code on the collect fast path
and on every bounded session's memory pool). **Registry:** §7 `H3-SPILL-NLJ-1`,
`H3-SPILL-COLLECT-1`, both flipped **FIXED**.

**Retires:** the orchestrator's departure edit moves this ledger to `../completed/`; this unit
leaves `STATUS.md` and `briefs/next-sequence.md` untouched by instruction.

**Why now.** [H3-SPILL-1](h3-spill-1-ledger.md) measured 180 cells and found exactly two ways
repark runs out of memory badly. Neither is a wrong answer and neither aborts the process, so
that unit pinned them rather than fixing them. This unit fixes both. The claim at stake is
[PROJECT.md](../../../PROJECT.md)'s "predictable memory": a bounded pool must answer with a typed
refusal, and a boundary the pool does not bound must at least say `MemoryError` when the OS
refuses it — never "this is a bug — please report it".

**Not in this unit:** any dependency or lockfile change. The nested-loop-join defect is upstream
in DataFusion 54.1 and stays upstream; repark contains its consequence and files the issue text
below.

## PROPOSITION LEDGER — H3-SPILL-RESIDUE-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every CPython allocation on the collect row fast path checks its pointer, so `collect()` under an address-space ceiling raises `MemoryError` rather than a caught panic — and the happy path is not slower for it. | The rewritten `collect_rows.rs`; the flipped pin measured in a fresh subprocess under `RLIMIT_AS`; a before/after facade-bench distribution on release modules. | **PROVEN** | Every constructor goes through `owned` = `Bound::from_owned_ptr_or_err`. pyo3's safe API could not be used: `PyTuple::new`, `PyList::new` and the scalar `IntoPyObject` impls all reach `assume_owned` → `panic_on_null`, **even where the signature returns `PyResult`** — and `panic_on_null` first *consumes* the `MemoryError` with `PyErr::take` + `write_unraisable`, so catching the panic later cannot recover it. Measured on the base module: `PySparkException: repark internal error in collect_rows.rows_from_record_batch: PyObject pointer is null …`; on the fix: `MemoryError` (no message — CPython's own). The 6 GiB control still returns 4,000,000 rows. Happy path, 5 facade-bench medians per module: `collect/1000000` 1019.7 → 1028.6 ms (ranges [990, 1040] vs [1004, 1055]), `collect/100000` 94.5 → 98.6 ms with the harness's own floor for that cell measured at 3.07-6.18 ms; the in-run pure-Python control `collect_old/1000000` moved 4916.7 → 4906.9, so the box was the same on both sides. |
| C-002 | A bounded pool's refusals are observable outside DataFusion, and a fenced panic that one of them caused is reported as that refusal — so a nested-loop join at a tight pool now refuses like every other operator, while a 1 GiB pool still answers. | `RefusalRecordingPool` + `PoolRefusalLog`; the reader's containment; the flipped pin; the 8 MiB column before and after. | **PROVEN** | `RefusalRecordingPool` delegates name, `Display`, `memory_limit`, both grow paths and `reserved`, and records only `try_grow` errors — so refusal text stays byte-identical and still reads `fair(pool_size: …)`. Measured at 8 MiB / 1e6: base `internal_error` → fix `clean_error`, message `Resources exhausted: Failed to allocate additional 1024.2 KB for NestedLoopJoinLoad[2] … fair(pool_size: 8.0 MB)` plus the containment disclosure and both resize knobs. At 64 MiB / 1e7, the matrix's only `internal_error` cell: `internal_error` ×3 → `clean_error` ×3. The 1 GiB control is still `ok`. **The wiring had a real hole, found by measurement, not by reading:** `getOrCreate()` forwards a builder `datafusion.runtime.memory_limit` as a *runtime* `SET`, which rebuilds the `RuntimeEnv`; installing a fresh pool there disarmed the containment on every bounded facade session, and the pin stayed red until `swap_fair_spill_pool` carried the same log across. |
| C-003 | The nested-loop-join defect is upstream in DataFusion 54.1, is located exactly, and is contained without touching a dependency — so a fixed upstream would make the containment dead rather than wrong. | The vendored-source reading; the unchanged `Cargo.lock`; the issue text below. | **PROVEN** | `NestedLoopJoinExec::execute` loads its build side through `build_side_data.try_once(\|\| self.left.execute(0, ctx))`. On a `ResourcesExhausted` from that load, `handle_buffering_left` → `initiate_fallback` → `left_spill_data.try_once(\|\| plan.execute(0, ctx))` executes **the same child instance** from partition 0 a second time. `RepartitionExec::execute` `remove`s partition 0's channel from its shared state on the first call, so the second finds nothing: `expect("partition not used yet")`, `datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277`. The build side is `SinglePartition`-distributed, so a `RepartitionExec` under it is what the enforcer puts there — this is the ordinary plan shape, not an exotic one. `git diff --stat origin/main -- Cargo.lock Cargo.toml` is empty. Issue text: §"Upstream" below. |
| C-004 | The containment is narrow: it rewrites only a fenced panic, only when the pool refused during that reader's own stream, and never on an unbounded session — each gate separately pinned, so widening it reds a test. | Four `arrow_export.rs` pins plus the end-to-end poll pin; the mutation table. | **PROVEN** | `a_panic_that_follows_a_pool_refusal_is_reported_as_that_refusal` (rewritten, and the engine's raw `partition not used yet` payload is **not** in the user's message — it goes to `tracing::warn!` on `repark::spill`); `a_panic_with_no_pool_refusal_stays_the_internal_error`; `an_unbounded_session_has_no_log_and_leaves_the_internal_error_alone`; `an_ordinary_stream_error_is_never_rewritten_as_a_refusal` (a plain `DataFusionError::Execution` survives verbatim); `the_reader_delivers_the_typed_refusal_from_its_own_poll` (through `Iterator::next`, not the helper). The Python pin asserts the absence of both `a Rust panic was caught` and `partition not used yet`. |
| C-005 | The measurement is reproducible and the documents say what is now true: both registry rows FIXED with dates and the unit id, the baseline's §1/§2/§3/§6/§7 corrected, and the 180-cell tables left as the 2026-09-05 record with the one moved cell called out. | The registry; the baseline; the harness commands. | **PROVEN** | Both §7 rows carry **FIXED 2026-09-06, H3-SPILL-RESIDUE-1**, the measured message, the pin list and the upstream status. Baseline §6 is rewritten as "the defects this matrix found — and how they were closed" with the before/after tables; §7's items 2 and 3 are struck through as done and item 1 (pool-account the facade boundary) is named the new head of the list; §8 carries the three narrowed commands. The frozen 180-cell tables are **not** re-run — §3 says so, and the one moved cell is annotated where it appears, including the census row `internal_error 1 → 0`. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Upstream — the DataFusion issue this unit does not fix

> **`NestedLoopJoinExec` OOM fallback re-executes its build child and panics in `RepartitionExec`**
>
> DataFusion 54.1.0. A nested-loop join under a bounded `FairSpillPool` panics with
> `partition not used yet` instead of spilling or refusing.
>
> `NestedLoopJoinExec::execute` loads the build (left) side once:
> `self.build_side_data.try_once(|| { let stream = self.left.execute(0, context)?; … })`.
> When the pool refuses that load, `NestedLoopJoinStream::handle_buffering_left` sees
> `Poll::Ready(Err(ResourcesExhausted))`, calls `initiate_fallback`, and that calls
> `left_spill_data.try_once(|| { let mut stream = plan.execute(0, ctx)?; … })` on the **same**
> `Arc<dyn ExecutionPlan>` child instance. `ExecutionPlan::execute` is not documented as
> re-entrant per partition, and `RepartitionExec::execute` in particular removes the partition's
> channel from `state.channels` on the first call, so the second call hits
> `.expect("partition not used yet")` (`repartition/mod.rs:1277`). Because
> `NestedLoopJoinExec::required_input_distribution()[0]` is `SinglePartition`, a `RepartitionExec`
> beneath the build side is the ordinary enforcer output, so the fallback is unreachable in
> practice for any plan with parallelism.
>
> Repro: a 1e6-row left side joined to a 64-row right side on a non-equi predicate
> (`ON l.v < r.v`), `datafusion.runtime.memory_limit = '8M'`, `target_partitions = 4`.
> 3/3 reproducible; also at 64 MiB with 1e7 rows.
>
> A fix would either buffer the build side once in a form both paths can read, or make the
> fallback re-plan its child rather than re-execute the same instance.

## Mutation proofs

Each mutation applied alone, `cargo test` run, then reverted.

| # | Mutation | Reds |
|---|---|---|
| M-1 | `owned` uses `Bound::from_owned_ptr` (pyo3's panicking constructor) instead of `…_or_err` | `collect_rows::tests::a_null_cpython_allocation_is_an_error_and_never_a_panic` (1 failed / 57) |
| M-2 | `StreamingBatchReader::next` returns the fenced item unchanged (no `as_pool_refusal`) | `arrow_export::tests::the_reader_delivers_the_typed_refusal_from_its_own_poll` (1 failed / 57) |
| M-3 | `swap_fair_spill_pool` installs a fresh `PoolRefusalLog` instead of carrying the session's | `session::tests::pool_refusals::a_runtime_pool_resize_keeps_the_refusal_log_alive` (1 failed / 230) |
| M-4 | `with_memory_pool` installs a bare `FairSpillPool` (no wrapper) | `a_bounded_session_installs_a_pool_whose_refusals_are_recorded` **and** `a_runtime_pool_resize_keeps_the_refusal_log_alive` (2 failed / 230) |
| M-5 | the whole fix (the base module, built and measured twice today) | both Python pins — `…nlj_1…` and `…collect_1…` |

M-3 is not a hypothetical: it **was** the code, and the NLJ pin stayed red on a release module
until the log was carried. The mutation is a re-run of a measured failure.

## Gates

| Gate | Exit |
|---|---|
| `make ci` | 0 (prerequisite of `verify`, run in the same tree) |
| `make verify` | 0 — 2,685 Rust tests |
| `make check-python-conventions` | 0 |
| `make rust-panic-ban` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -x` | 0 (5,025 passed, 236 skipped, 178 s) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (574 passed) |
| `VIRTUAL_ENV=$PWD/.venv make py-test-dbt` | 0 (59 passed, 1 skipped) |
| `.venv/bin/python -m pytest python/repark/tests/test_h3_spill_matrix.py -q` | 0 (22 passed, 38 s) |
| `make check-map-sync` | 0 (192 maps) |
| `make check-ledger-grammar` | 0 (55 live ledgers, 240 clauses) |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |

No JVM ran in this unit: the Spark comparison claims in the baseline are H3-SPILL-1's recorded
measurements and are cited, not re-measured, so `parity-live` is not in this gate list.

Every number in this ledger comes from a release module (`repark._native.__debug_assertions__`
is `False`, asserted before each measurement); the mutation runs are `cargo test`, which asserts
outcomes and never a wall clock.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: h3-spill-residue-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both flipped pins are measured in fresh subprocesses on a release module, each beside the green control that proves the limit is the cause (a 1 GiB pool answers; 6 GiB of headroom returns all 4e6 rows). Twelve new Rust unit tests cover the NULL check, the value round-trip of every Arrow type on the fast path, the pool wrapper's delegation, and each of the containment's three gates.
      artifacts: [python/repark/tests/test_h3_spill_matrix.py, crates/repark-python/src/collect_rows.rs, crates/repark-python/src/arrow_export.rs, crates/repark-core/src/pool_refusals.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The row fast path is the product's answer path, so the rewrite is pinned by value and not only by non-panic - every scalar constructor round-trips its value, a null Arrow slot is None, an empty string and one with an embedded NUL survive the length form, and a row tuple keeps column order and width. The 8 MiB column was re-run before and after: 17 of 18 operators are identical cell for cell, and the one that moved moved from internal_error to clean_error.
      artifacts: [crates/repark-python/src/collect_rows.rs, docs/perf/spill-matrix-baseline.md]
    - id: AT-3
      status: ATTACKED
      evidence: No AWS, no network, no secrets, no .github. The containment never widens an error class - only a fenced panic is rewritten, and only after a refusal recorded on that reader's own stream - and the raw engine panic payload goes to a tracing warn rather than into a user-facing message.
      artifacts: [crates/repark-python/src/arrow_export.rs]
    - id: AT-4
      status: ATTACKED
      evidence: One subprocess per cell; release asserted rather than assumed; the happy-path claim rests on five facade-bench medians per module with an in-run pure-Python control, and every run records the 1-minute load it started under (10-26 throughout, a busy box, which is why the claim is stated as overlapping distributions rather than as a point difference).
      artifacts: [docs/perf/spill-matrix-baseline.md]
    - id: AT-5
      status: N/A
      justification: No dependency, lockfile, or workflow change. The upstream DataFusion defect is contained on repark's side and filed as issue text in this ledger.
    - id: AT-6
      status: ATTACKED
      evidence: Both registry rows are flipped FIXED with the date, the unit id, the measured message and the pin list; the baseline's honest-limits sections are corrected rather than deleted, and the frozen 180-cell tables are annotated rather than silently re-run.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/perf/spill-matrix-baseline.md, docs/map.md, docs/perf/map.md]
    - id: AT-7
      status: ATTACKED
      evidence: Outcomes come from the harness's own classification and from exception types, never from wall time; the only wall-clock claim (the collect happy path) is stated as two overlapping five-run distributions against the harness's own measured floor for that cell.
      artifacts: [docs/perf/spill-matrix-baseline.md]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change; Cargo.toml and Cargo.lock are byte-identical to the base.
    - id: AT-9
      status: ATTACKED
      evidence: Five mutations, each applied alone and reverted, each killed by a named test; the fifth is the base module itself, measured twice today, which reds both Python pins. M-3 was a real defect in this unit's first implementation, not a hypothetical.
      artifacts: [crates/repark-core/src/session/spill.rs, crates/repark-core/src/session/tests/pool_refusals.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md moves in the same commit, including the two size-gate ratchet logs; the dataframe.rs CAP-1 baseline ratcheted DOWN 1127 to 1126 and is mirrored in both tables.
      artifacts: [crates/repark-core/src/map.md, crates/repark-core/src/session/map.md, crates/repark-core/src/session/tests/map.md, crates/repark-python/src/map.md, python/repark/tests/map.md, python/repark-parity/bench/spill/map.md, scripts/map.md, python/repark-parity/tests/map.md]
  complete: true
```
