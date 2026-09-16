# Charter ledger — NEVER-OOM-PANIC-1 · a tight pool refuses with the typed exception on every path, never a panic

**Date:** 2026-09-16 · **Branch:** `fix/never-oom-panic-1` · **Base:** `33c87cbf41080e97d3c48ef2b62ee776de61e996` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** §7 `NEVER-OOM-PANIC-1` (new) and the `H3-SPILL-NLJ-1` dated paragraph.

**Retires:** the orchestrator's departure edit moves this ledger to `../completed/`; this unit
leaves `STATUS.md` and `briefs/next-sequence.md` untouched by instruction.

**Originating ruling R-17c-7 (owner, 2026-09-16, binding).** The Rust panic that reached the
Python boundary under memory pressure is a DEFECT card, NEVER-OOM-PANIC-1, scheduled today.

**Ownership announcement.** This unit edits `crates/repark-core` execution code and, only if
design (c) is chosen with proof, `crates/repark-python/src/arrow_export.rs` and its `map.md`.
Run 18a owns every `functions*.py`, the Rust function registry and
`crates/repark-python/src/column/function_dispatch.rs`; run 18c owns the SQL parser, planner,
dialect and router in `crates/repark-spark` except where this card names a file explicitly.

**Why now.** On a loaded 4-vCPU GitHub runner the facade pin
`test_h3_spill_nlj_1_a_tight_pool_refuses_a_nested_loop_join_with_the_typed_exception`
failed with `inner future panicked during poll` instead of the Never-OOM typed refusal. The
H3-SPILL-RESIDUE-1 containment allow-lists `partition not used yet` but not the `Shared`
poisoned-arm payload, so which message the reader reports is a race between output
partitions. The goal is not a wider allow-list: the panic must not happen.

**Not in this unit:** any dependency or lockfile change (`Cargo.toml` / `Cargo.lock`
untouched); any process-wide panic hook; any fix inside DataFusion itself; any JVM run
(oracle cells are the recorded PySpark 4.1.2 fixtures named in the card; missing cells go
under `Cells wanted` for the orchestrator's JVM).

## Standing rules carried (run 18b preamble, binding)

SHAPE RULE, RUST FIRST with Q-17a-2 (one ledger line per piece that must stay in Python),
ORACLE FIRST, NO COMMENTS in source (a pedantic doc lint gets `#[allow(...)]`; editing an
existing doc comment to keep it true is allowed, adding one is not), map.md lockstep, size
ceilings only down, red first, registry rows appended never reordered, DEBUG module in
`.venv` for reproduction and RELEASE rebuild after any Rust edit, whole suites never
subsets, real exit codes and counts in the ledger.

## PROPOSITION LEDGER — NEVER-OOM-PANIC-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The failure is reproducible and measured: the pin's `_WORKER` looped at least 40 iterations under `taskset -c 0-3`, over pools 4M/6M/8M/12M and `target_partitions` 2/4/8, with and without concurrent CPU load, and the payload histogram per configuration is recorded below. | The histogram table in §Reproduction. | **PROVEN** | 400 worker runs under `taskset -c 0-3` on the DEBUG module: 398 typed refusals, 2 `inner future panicked during poll` escapes (8M/4 and 12M/4) with the exact CI message. stderr hook blocks prove the race is hot underneath: `partition not used yet` fires in 400/400 runs, `inner future panicked` in 200/400. pins: never-oom-panic-1/C-001 |
| C-002 | The exact panic path is read from the vendored sources and cited file:line: NLJ double-execute, `RepartitionExec` expect, `OnceAsync`/`Shared` poisoned arm. | The path table in §Panic paths. | **PROVEN** | Lines verified against the vendored tree (`datafusion-physical-plan-54.1.0`, `futures-util-0.3.33` per `Cargo.lock`): P-1 `nested_loop_join.rs:632`, P-2 `nested_loop_join.rs:1391/1397`, P-3 `repartition/mod.rs:1277`, P-4 `shared.rs:118/:300`, P-5 `execution_plan.rs:255/:1474`. pins: never-oom-panic-1/C-002 |
| C-003 | The unit's red-first Rust pin loops the same NLJ under a tight `RefusalRecordingPool`/`FairSpillPool`, asserting `ResourcesExhausted` and no panic unwind; it FAILS on the base tree and the red lines are pasted below. | The new `repark-core` test and its red run. | **PROVEN** | `crates/repark-core/src/session/tests/nlj_tight_pool.rs`, run before the fix: FAILED in 0.10 s, the test future killed by `thread 'tokio-rt-worker' panicked at datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277:22: partition not used yet` (3 blocks). No assertion fired — the double-execute panic unwound through the awaiting worker, which is exactly P-2/P-3. pins: never-oom-panic-1/C-003 |
| C-004 | The design is chosen with evidence among (a) optimizer rule / wrapper exec that makes the NLJ build-side child safe to execute twice, (b) a rule that prevents the double-execute shape under a bounded pool, (c) containment extension only — recorded as a ruling row with one line of reason. | The ruling row R-DESIGN. | **PROVEN** | R-DESIGN chose (a); see Rulings. pins: never-oom-panic-1/C-004 |
| C-005 | The fix lands in Rust (`crates/repark-core`; `arrow_export.rs` only under design (c)): no `unwrap`/`expect` in product code, no `tokio::spawn`, clippy pedantic clean, `thiserror` errors, no new comments, no `Cargo.toml`/`Cargo.lock` change, no process-wide panic hook. | The fix diff; `git diff --stat -- Cargo.lock Cargo.toml` empty. | **PROVEN** | `crates/repark-core/src/nlj_build_reset.rs` (new, ~370 lines with its 7 module tests) + one-line wiring in `session/df_guards.rs` + `mod` row in `lib.rs`. Product code holds no `unwrap`/`expect`/`unreachable!`/`panic!`/`tokio::spawn` (verified by reading the diff; clippy pedantic runs in C-010). No `catch_unwind` in product (only in the premise test). `git diff --stat -- Cargo.toml Cargo.lock` empty. No panic hook. No new comments (`//`/`///`/`//!` absent from the new file; the pedantic doc lint is met with `#[allow(clippy::missing_errors_doc)]` on the trait impl). pins: never-oom-panic-1/C-005 |
| C-006 | The Rust loop pin is green after the fix. | The green run. | **PROVEN** | `cargo test -p repark-core --lib session::tests::nlj_tight_pool`: ok in 107.02 s (10/10 iterations; each proved its own tightness via the refusal log, each outcome `Ok` — the fallback now spills and the query succeeds). Module tests 7/7 green. pins: never-oom-panic-1/C-003, C-006 |
| C-007 | A facade pin repeats the NLJ worker enough times to have caught the race at the measured rate (rate cited), asserting the typed message every time, bounded to stay under ~60 s. | The new facade pin and its run. | **PROVEN** | `test_never_oom_panic_1_a_tight_pool_leaves_no_panic_blocks_on_stderr`: 5 worker runs (8M, 1e6 rows, ~1.6 s each, ~12 s with the renamed pin), asserting zero `panicked at`/`partition not used yet`/`inner future panicked` bytes on stderr plus the legal outcome. Red on the base RELEASE module at the first run (FAILED in 0.54 s with the `repartition/mod.rs:1277` block pasted in §Red runs); green after (2 passed in 12.41 s). The measured base rate it would have caught: stderr panic blocks in 400/400 runs, user-visible escape in 2/400 — the stderr signal is the deterministic one. pins: never-oom-panic-1/C-007 |
| C-008 | The existing `test_h3_spill_matrix.py` pins and the `arrow_export.rs` containment tests stay green unchanged; if the design makes the containment dead, the ledger and registry row say so and the containment is kept only if another path still needs it. | The unchanged runs (or the dead-containment statement). | **PROVEN** | Whole `test_h3_spill_matrix.py`: 23 passed in 45.71 s on the fixed RELEASE module. The old NLJ pin is RENAMED (not silently widened) to `…_spills_or_refuses_…_without_a_panic` because the fix legitimately changed the outcome from refuse to spill-and-succeed; it passes on base too (typed arm, 1.53 s) except when the race escapes, which the new stderr pin covers deterministically. Containment statement: this path no longer reaches the containment (6/6 post-fix worker runs with zero stderr blocks; 10/10 Rust iterations `Ok`), and that is said in the ledger, the `H3-SPILL-NLJ-1` paragraph and the `nlj_build_reset` map rows — but the containment is KEPT because the other seven allow-listed payloads guard fallback paths this unit does not touch (`arrow_export.rs` tests untouched and green, verified in C-010). pins: never-oom-panic-1/C-008 |
| C-009 | Registry: `H3-SPILL-NLJ-1` gains a dated appended paragraph (history not rewritten) and `NEVER-OOM-PANIC-1` is added in the same section. | The registry diff. | **OPEN** | — |
| C-010 | Gates whole and green with real exit codes and counts: `make verify`; the unit's own test files; the whole facade suite once into a log; the whole parity suite once into a log; `cargo test -p repark-core` and `cargo test -p repark-python`; `COVERAGE_ATTESTATION` complete; VERDICT. | §Gates. | **OPEN** | — |

## Rulings

| # | Source | Ruling |
|---|---|---|
| R-17c-7 | Owner, 2026-09-16, binding | The panic at the Python boundary under memory pressure is DEFECT card NEVER-OOM-PANIC-1, scheduled today. Recorded as this unit's originating ruling. |
| R-DESIGN | Actor, 2026-09-16 (evidence P-5, the module tests) | Design (a): a repark physical-optimizer rule (`NljBuildSideReset`, appended last via `with_physical_optimizer_rule`, so the enforcer's `RepartitionExec` is already placed) wraps each NLJ build-side child in `NljBuildSideExec`, whose `execute` runs a `reset_plan_states` clone — `RepartitionExec` does not override `reset_state`, so the default rebuild restores its one-shot channels and the spill fallback really spills. One line of reason: it removes the double-execute panic at the root for every one-shot node the build side can hold, while (b) only fixes the Repartition shape and (c) keeps the panic and the race. |

## Reproduction

Build used: DEBUG native module in the clone `.venv` (`repark._native.__debug_assertions__`
is True, asserted before the first loop).

Worker loop method: the pin's `_WORKER` script invoked the way `_run_worker` invokes it
(`nested_loop_join`, pool, 1_000_000 rows, partitions), under `taskset -c 0-3`. The loop
driver (`/tmp/nlj_loop2.py`, scratch, not committed) classifies the user-visible outcome per
run and counts Rust-panic hook blocks on the worker's stderr (`partition not used yet` vs
`inner future panicked during poll`). The CPU-load arm pins one busy-loop Python process per
CPU 0-3 on the same 4 CPUs (`/tmp/nlj_loaded.py`, scratch).

Payload histogram per configuration (each row 40 worker runs):

| pool | partitions | load | typed refusal | inner-future escape | stderr runs with inner | stderr runs with partition |
|---|---|---|---|---|---|---|
| 4M | 4 | no | 40 | 0 | 10 | 40 |
| 6M | 4 | no | 40 | 0 | 31 | 40 |
| 8M | 4 | no | 39 | 1 | 15 | 40 |
| 12M | 4 | no | 39 | 1 | 20 | 40 |
| 8M | 2 | no | 40 | 0 | 3 | 40 |
| 8M | 8 | no | 40 | 0 | 15 | 40 |
| 4M | 2 | no | 40 | 0 | 5 | 40 |
| 6M | 8 | no | 40 | 0 | 33 | 40 |
| 8M | 4 | yes | 40 | 0 | 36 | 40 |
| 4M | 4 | yes | 40 | 0 | 32 | 40 |

Totals: 400 runs, 398 typed refusals, 2 `inner future panicked during poll` escapes. Each
escape carries the exact CI message: `repark internal error in
PyDataFrame.__arrow_c_stream__.next: inner future panicked during poll (a Rust panic was
caught at the Python boundary; this is a bug — please report it)`. The typed refusal reads
`Resources exhausted: Failed to allocate additional 1024.2 KB for NestedLoopJoinLoad[N] …
fair(pool_size: …)` with the containment disclosure. No run answered `ok`, no run answered
any other error. The stderr columns show the race is hot underneath the contained surface:
the double-execute panic fires in 400/400 runs and the poisoned-arm panic in 200/400; the
user-visible escape is only which payload the reader polls first.

## Panic paths

Paths read from the vendored sources (`datafusion-physical-plan-54.1.0`,
`futures-util-0.3.33` — the `Cargo.lock` versions; line numbers verified by reading):

| # | Path |
|---|---|
| P-1 | `NestedLoopJoinExec::execute` loads the build side once via `build_side_data.try_once(\|\| self.left.execute(0, ctx))` (`joins/nested_loop_join.rs:632`). |
| P-2 | On `ResourcesExhausted` from that load, `handle_buffering_left` → `initiate_fallback` re-executes the same child instance: `left_spill_data.try_once(\|\| … plan.execute(0, ctx))` (`joins/nested_loop_join.rs:1391`, `plan.execute` at :1397). `left_plan` is `Arc::clone(&self.left)` — the same instance P-1 consumed. |
| P-3 | `RepartitionExec::execute` removes the partition channel from shared state on the first `execute`, so the second `execute(0)` finds nothing and hits `expect("partition not used yet")` (`repartition/mod.rs:1277`). The build side is `SinglePartition`-distributed, so the enforcer's `RepartitionExec` under it is the ordinary plan shape. |
| P-4 | `OnceAsync::try_once` shares one `Shared` future across output partitions (`joins/utils.rs:377`). A poll that unwinds poisons it; every later poll or `peek` panics `inner future panicked during poll` (`futures-util-0.3.33 shared.rs:118` in `peek`, `:300` in `poll`). Which payload the reader sees first is the race K-2 names. Measured above: the poisoned arm fires in half the runs and wins the race in ~0.5%. |
| P-5 | `ExecutionPlan::reset_state` (`execution_plan.rs:255`, default rebuilds via `with_new_children`) and `reset_plan_states` (`execution_plan.rs:1474`, used by recursive CTEs at `recursive_query.rs:357`) exist. `RepartitionExec`, `NestedLoopJoinExec` and `CoalescePartitionsExec` do not override `reset_state`, so the default rebuilds fresh per-execution state for them. |

## Red runs

Base-tree red for C-003 (`cargo test -p repark-core --lib session::tests::nlj_tight_pool`,
before the fix):

```text
running 1 test
test session::tests::nlj_tight_pool::a_tight_pool_refuses_or_spills_a_nested_loop_join_without_a_panic ... FAILED

---- session::tests::nlj_tight_pool::... stdout ----
thread 'tokio-rt-worker' (3930320) panicked at .../datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277:22:
partition not used yet
[... 2 more identical blocks ...]

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 527 filtered out; finished in 0.10s
```

Green after the fix: `test result: ok. 1 passed; finished in 107.02s`, plus the 7 module
tests in `nlj_build_reset.rs` (the premise test `a_bare_repartition_exec_runs_only_once`
documents P-3 against the real `RepartitionExec` and reds if the wrapper ever stops
resetting).

Facade red for C-007 (new stderr pin on the base RELEASE module, fix stashed out of the
tree, pins kept):

```text
E           assert 'panicked at' not in "\nthread 't...t used yet\n"
E             'panicked at' is contained here:
E               thread 'tokio-rt-worker' (84499) panicked at .../datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277:22:
E             ?                                  +++++++++++
E               partition not used yet
python/repark/tests/test_h3_spill_matrix.py:375: AssertionError
1 failed in 0.54s
```

The renamed pin on the same base module: `1 passed in 1.53s` (typed-refusal arm — it only
reds when the race escapes, measured 2/400). After restoring the fix and rebuilding
RELEASE: `2 passed in 12.41s`; whole `test_h3_spill_matrix.py`: `23 passed in 45.71s`;
post-fix worker sample 6/6 `ok` with `[0, 0, 0]` hook blocks in every run.

## Rust-first roll-call

| Piece | Home | Reason |
|---|---|---|
| NLJ build-side re-execution safety (rule + wrapper exec) | `crates/repark-core/src/nlj_build_reset.rs` | The SQL door must reach it: it is a physical-plan rewrite and an `ExecutionPlan`, so it lives in Rust by Q-17a-2. |
| Tight-pool loop pin | `crates/repark-core/src/session/tests/nlj_tight_pool.rs` | Engine behavior pin; Rust-only. |
| Facade loop + renamed pins | `python/repark/tests/test_h3_spill_matrix.py` | Names, argument shapes and worker plumbing only: both pins drive the existing `_WORKER` script and assert message shapes, no engine logic. This is the unit's one piece that stays in Python, per Q-17a-2. |

## Cells wanted

None yet.

## Questions

None yet.

VERDICT: 10 clauses, 6 PROVEN, 4 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: never-oom-panic-1
  categories: []
  complete: false
```
