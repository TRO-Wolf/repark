# Charter ledger — NEVER-OOM-PANIC-1 · a tight pool refuses with the typed exception on every path, never a panic

**Date:** 2026-09-16 · **Branch:** `fix/never-oom-panic-1` · **Base:** `33c87cbf41080e97d3c48ef2b62ee776de61e996` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** §7 `NEVER-OOM-PANIC-1` (new) and the `H3-SPILL-NLJ-1` dated paragraph.

**Retires:** the orchestrator's departure edit moves this ledger to `../completed/`; this unit
leaves `STATUS.md` and `briefs/next-sequence.md` untouched by instruction.

**Originating ruling R-17c-7 (owner, 2026-09-16, binding).** The Rust panic that reached the
Python boundary under memory pressure is a DEFECT card, NEVER-OOM-PANIC-1, scheduled today.

**Round 2 (2026-09-16).** The orchestrator rebased this branch onto current main (head
`9fd2cda9`, four round-1 commits) and rebuilt the RELEASE native. The Grok 4.6 logic critic
returned NEEDS_REMEDIATION: the round-1 fix lets DataFusion 54.1's spill fallback really run,
and for LEFT / LEFT ANTI with more than one right partition that fallback silently duplicates
rows (~4x). Round 2 refuses the fallback shapes DataFusion documents as unsafe and pins every
spill for value. Report: `/tmp/oc-worker/run18b/reviews/critic-oom-logic-report.md`; perf
report: `/tmp/oc-worker/run18b/reviews/perf-oom-rust-report.md`.

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
| C-009 | Registry: `H3-SPILL-NLJ-1` gains a dated appended paragraph (history not rewritten) and `NEVER-OOM-PANIC-1` is added in the same section. | The registry diff. | **PROVEN** | Commit `11b22d43`: `H3-SPILL-NLJ-1` keeps its FIXED history plus a dated NEVER-OOM-PANIC-1 paragraph (pin rename, containment now defense-in-depth on this path); new row `NEVER-OOM-PANIC-1` in the same section with the measured counts, the pin list and the rationale. Nothing reordered. pins: never-oom-panic-1/C-009 |
| C-010 | Gates whole and green with real exit codes and counts: `make verify`; the unit's own test files; the whole facade suite once into a log; the whole parity suite once into a log; `cargo test -p repark-core` and `cargo test -p repark-python`; `COVERAGE_ATTESTATION` complete; VERDICT. | §Gates. | **OPEN** | Round-1 gates were green (see §Gates); round-2 changed product code, so every gate re-runs in step 5. pins: never-oom-panic-1/C-010 |
| C-013 | R-18b-3: residue rows for L-203/R-01 (empty build-side metrics), R-02 (first execute resets), R-03 (spill replay ~2.8x build rows) with the perf-report citations; `metrics()` stays `None`. | The residue section. | **PROVEN** | §Round-2 residue records all three with the perf-report citations and the decline reason for the `metrics()` change. pins: never-oom-panic-1/C-013 |
| C-011 | R-18b-1: for LEFT, LEFT SEMI, LEFT ANTI and LEFT MARK with more than one right partition the fallback re-execute refuses typed (the genuine recorded pool text plus the containment disclosure; knobs arrive at the Python boundary) instead of emitting rows; every other shape still spills, and single-right-partition shapes still spill. | The rule policy matrix tests; the LEFT refusal pins; the refusal message measured on the facade. | **PROVEN** | `BuildSidePolicy::for_join` off the public `join_type()` and right partition count; the wrapper counts executes (first is always the load, second always the fallback) and the second execute with a left-family type and >1 right partitions returns `ResourcesExhausted` from the session refusal log plus the shared disclosure string — never a row, never a fabrication (no recorded refusal means spill, a case the fallback cannot reach). 16-case policy matrix green; refuse-on-second-execute green with `fair(` and the disclosure asserted; module suite 10/10. Rust LEFT pin green (typed refusal); facade LEFT/ANTI/SEMI legs green (refusal with disclosure), FULL green (refusal without it — the upstream disable path). pins: never-oom-panic-1/C-011 |
| C-012 | R-18b-2: value pins per join type — facade runs INNER, LEFT, LEFT ANTI, LEFT SEMI, RIGHT, FULL at 8M/4 and 1G asserting count(*) AND sum(id) AND a content digest equal, or the typed refusal, asserting which outcome each type answers; Rust loop pin asserts the INNER value and the LEFT refusal; red first on this head for LEFT / LEFT ANTI; NLJ group under ~60 s. | The new pins and their red/green runs. | **PROVEN** | Red on this head (round-1 code, RELEASE): facade `test_..._join_values_match_or_refuse_typed` FAILED in 35.85 s — the LEFT leg answered `ok` where the refusal is asserted (`assert 'ok' == 'error'`); Rust `a_tight_pool_refuses_a_left_...` FAILED in 29.13 s (`must not spill rows`), INNER value pin green as a keep-spill pin. Facade worker: one process per pool (8M/4, 1G/4, 8M/1) over all six types plus RIGHT SEMI/ANTI legs; each type reports outcome plus count/sum/content-digest, or the message. MARK joins are unproducible through the SQL door (parser refuses `LEFT MARK` with `expected OUTER, SEMI, ANTI or JOIN after LEFT`); the rule classifies them and the Rust policy-matrix test covers LeftMark. Green after the fix: facade value pin green in 25.32 s (tight `ok` legs equal wide on count/sum/digest; refusal legs carry the asserted shapes); NLJ group 3 passed in 34.48 s, under the 60 s budget. Rust INNER value pin green (`count(*)` 2016, `sum(id)` 41664); LEFT refusal pin green. pins: never-oom-panic-1/C-012 |
| C-013 | R-18b-3: residue rows for L-203/R-01 (empty build-side metrics), R-02 (first execute resets), R-03 (spill replay ~2.8x build rows) with the perf-report citations; `metrics()` stays `None`. | The residue section. | **OPEN** | Decision: residue, no metrics change — the clone's metrics belong to a discarded plan node and threading them through would add shared state for observability only. |

## Rulings

| # | Source | Ruling |
|---|---|---|
| R-17c-7 | Owner, 2026-09-16, binding | The panic at the Python boundary under memory pressure is DEFECT card NEVER-OOM-PANIC-1, scheduled today. Recorded as this unit's originating ruling. |
| R-DESIGN | Actor, 2026-09-16 (evidence P-5, the module tests) | Design (a): a repark physical-optimizer rule (`NljBuildSideReset`, appended last via `with_physical_optimizer_rule`, so the enforcer's `RepartitionExec` is already placed) wraps each NLJ build-side child in `NljBuildSideExec`, whose `execute` runs a `reset_plan_states` clone — `RepartitionExec` does not override `reset_state`, so the default rebuild restores its one-shot channels and the spill fallback really spills. One line of reason: it removes the double-execute panic at the root for every one-shot node the build side can hold, while (b) only fixes the Repartition shape and (c) keeps the panic and the race. |
| R-18b-1 | Orchestrator, 2026-09-16, binding (critic L-201 P1) | A wrong answer is never acceptable for removing a panic. For every join type in DataFusion 54.1's documented unsafe set (LEFT, LEFT SEMI, LEFT ANTI, LEFT MARK) with more than one right partition, the second build-side execute produces no rows: it ends in the typed pool refusal. Join types whose fallback is correct keep the spill. In `nlj_build_reset.rs`; the rule knows the join type and right partition count. |
| R-18b-2 | Orchestrator, 2026-09-16, binding (critic L-202 P2) | The ok branch is pinned for VALUE: a facade pin runs INNER, LEFT, LEFT ANTI, LEFT SEMI, RIGHT, FULL at 8M/4 and 1G asserting count(*) AND sum(id) AND a content digest equal, or the typed refusal — and asserts which of the two each type answers. The Rust loop pin gets the INNER value assertion and the LEFT refusal assertion. Red first on this head for LEFT / LEFT ANTI. Whole NLJ group under ~60 s. |
| R-18b-3 | Orchestrator, 2026-09-16, binding (critic L-203 P3, perf R-01/R-02/R-03 P3) | Ledger residue rows, not fixed: empty build-side metrics under EXPLAIN ANALYZE, O(depth²) nested resets, spill replay cost. The `metrics()` change is optional and only if lock-free and few lines. |

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

## Round-2 residue (R-18b-3, not fixed)

Cited measurements are the perf report's (`/tmp/oc-worker/run18b/reviews/perf-oom-rust-report.md`).

- R-01 — nested NLJ wrappers reset the inner tree on every `execute`: a left-deep NLJ chain
  rebuilds O(depth²) plan nodes, measured 2 ms at depth 20 with product NLJs almost always
  at depth 1. Residue.
- R-02 (critic L-203) — the first `execute` also resets, so EXPLAIN ANALYZE prints
  `metrics=[]` on `NljBuildSideExec` and every node under it, bounded pool or not. The join
  node's own metrics (`output_rows`, `spill_count`, `build_input_rows`) and the probe-side
  children stay live, and no H3 pin reads a wrapped child. The `metrics()` fix is declined:
  the executed clone is discarded by design, and threading its metrics through would add
  shared state for observability only. Residue.
- R-03 — the spill fallback replays the build side (`build_input_rows` ~2.8x: the failed
  in-memory load plus the spill replay) at ~1 s wall and +14 MiB RSS versus unbounded on
  the 8M/1e6 worker. That replay is the fix, not a regression. Residue.

## Mutation proofs

Each mutation applied alone, the named tests run, then reverted.

| # | Mutation | Reds |
|---|---|---|
| M-1 | `df_guards.rs` wiring line removed (rule never installed) | `session::tests::nlj_tight_pool` FAILED in 0.10 s with the `repartition/mod.rs:1277` panic — the end-to-end kill |
| M-2 | `NljBuildSideExec::execute` runs `self.inner` directly (no `reset_plan_states`) | `the_wrapper_runs_a_one_shot_child_twice`, `the_wrapper_replays_values_on_every_execute` (2 failed / 7) |
| M-3 | Rule wraps the right (probe) child instead of the left | `the_rule_wraps_only_the_build_side_of_a_nested_loop_join`, `the_rule_is_idempotent` (2 failed / 7) |

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

## Gates

| Gate | Exit | Counts |
|---|---|---|
| `make verify` | 0 | lint, format, clippy (both clippy gates incl. the production panic ban), all Rust tests, all map/ledger/manifest gates — see `/tmp/verify.log` |
| `cargo test -p repark-core` | 0 | 534 lib + 37 + 8 integration passed, 0 failed (`/tmp/core-tests.log`; lib leg 159.35 s incl. the 107 s loop pin) |
| `cargo test -p repark-python` | 0 | 75 + 25 passed, 0 failed (`/tmp/py-tests.log`) |
| unit's own test files: `nlj_build_reset` + `nlj_tight_pool` | 0 | 7/7 module tests; loop pin 1/1 |
| unit's own facade pins | 0 | 2 passed in 12.41 s |
| whole `test_h3_spill_matrix.py` | 0 | 23 passed in 45.71 s |
| whole facade suite `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider` | 0 | 9036 passed, 367 skipped, 34 xfailed in 340.94 s (`/tmp/facade.log`) |
| whole parity suite `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | 0 | 757 passed, 2 skipped, 12 xfailed in 381.43 s (`/tmp/parity.log`) |
| `git diff --stat -- Cargo.toml Cargo.lock` | 0 | empty — no dependency or lockfile change |

VERDICT: 13 clauses, 12 PROVEN, 1 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: never-oom-panic-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each charter clause is checked against behavior, not paraphrase. C-001 is the 400-run worker histogram with the exact CI payload twice; C-002 is five file:line paths read from the vendored tree; C-003/C-006 are the red (0.10 s, unwound partition-not-used-yet panic) and green (107.02 s, 10/10 Ok) loop-pin runs; C-004 is R-DESIGN with the P-5 evidence; C-005 is the diff-property list (no unwrap/expect/unreachable/panic/spawn/hook/comments, Cargo files empty); C-007 is the stderr-pin red (0.54 s) and green (12.41 s); C-008 is the 23-green matrix plus the containment statement; C-009 is commit 11b22d43; C-010 is the gates table above.
      artifacts: [task/ledgers/staging/never-oom-panic-1-ledger.md, crates/repark-core/src/session/tests/nlj_tight_pool.rs, python/repark/tests/test_h3_spill_matrix.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pool/partition/load domain is swept, not sampled: pools 4M/6M/8M/12M by partitions 2/4/8 by load/no-load, 40 worker runs each. The fits boundary holds (1G control ok); the tight boundary answers ok-or-typed-refusal. Every Rust loop iteration proves its own tightness through the refusal log, so a vacuous pass is impossible. Null/empty inputs have no surface here: the change is a physical-plan rewrite on a fixed non-equi-join shape with no new parser or expression.
      artifacts: [task/ledgers/staging/never-oom-panic-1-ledger.md, crates/repark-core/src/session/tests/nlj_tight_pool.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Both legal failure outcomes are pinned: spill-and-succeed (ok) and the typed ResourcesExhausted refusal with its full message shape. The session-usable-after-refusal pin still passes, no run in 400+ aborts the process, and a non-refusal error propagates unrewritten. The renamed pin forbids every panic marker on both arms.
      artifacts: [python/repark/tests/test_h3_spill_matrix.py, crates/repark-core/src/session/tests/nlj_tight_pool.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The race is the unit. The 400-run histogram under taskset CI-shape CPUs shows the double-execute panic in 400/400 runs and the poisoned-arm panic in 200/400; the fix removes the shared-state double use (a reset clone per execute), and the 10-iteration multi_thread loop plus the 5-run stderr loop assert the race is gone. No shared mutable state is added: the wrapper holds only its child plan.
      artifacts: [task/ledgers/staging/never-oom-panic-1-ledger.md, crates/repark-core/src/nlj_build_reset.rs]
    - id: AT-5
      status: N/A
      justification: No authentication, privilege, secret, path, network, or deserialization surface is touched; no .github, env, or credential change. The rule rewrites in-memory plan nodes only.
    - id: AT-6
      status: ATTACKED
      evidence: The spilled path answers exactly the in-memory path: the full join digest 9fb2c245 with 2016 rows is identical at 8M, 64M, and 1G. Schema, partitioning, fetch, and limit-pushdown delegation are pinned, the EXPLAIN shape guard keeps the pin on the NLJ path, and unbounded sessions are behavior-neutral (the fallback never triggers without a refusal; both whole suites green).
      artifacts: [crates/repark-core/src/nlj_build_reset.rs, python/repark/tests/test_h3_spill_matrix.py]
    - id: AT-7
      status: ATTACKED
      evidence: Nothing here grows without bound: the pool stays 8 MiB, the excess spills to disk, and each probe session drops per iteration. A tight-pool worker run costs about 1.6 s on RELEASE; the DEBUG Rust loop pin costs 107 s for ten iterations, disclosed, not hidden. No system-breaking perf change: the wrapper adds one plan-node rebuild per NLJ build-side execute, and both whole suites ran in their normal durations.
      artifacts: [task/ledgers/staging/never-oom-panic-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Cargo.toml and Cargo.lock are byte-identical to the base (empty diff stat). Only public DataFusion API is used: a PhysicalOptimizerRule appended last, reset_plan_states, ExecutionPlan. The upstream one-shot behavior is pinned by the premise test, which reds if a future DataFusion changes it, instead of presuming it. The refusal text is byte-identical because the pool wrapper is untouched.
      artifacts: [crates/repark-core/src/nlj_build_reset.rs, Cargo.toml, Cargo.lock]
    - id: AT-9
      status: ATTACKED
      evidence: The failure stays diagnosable: the typed refusal text is unchanged, EXPLAIN names the new NljBuildSideExec node, and non-refusal errors propagate unrewritten. The defect's old signal (panic blocks on stderr) is gone by construction and asserted gone by the stderr pin; the existing tracing warn in the retained containment still covers the other fallback paths.
      artifacts: [python/repark/tests/test_h3_spill_matrix.py, crates/repark-python/src/arrow_export.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first on both doors (Rust 0.10 s red, facade stderr pin 0.54 s red on base). Three mutations, each applied alone and reverted, each killed by named tests: M-1 (rule wiring removed) by the loop pin in 0.10 s; M-2 (wrapper executes the child directly) by the two double-execute module tests; M-3 (rule wraps the probe side) by the two rule tests. Branch liveness: every rule arm has a pin (wrap / non-NLJ identity / idempotent reapply), the wrapper arity refusal is pinned, and the two defensive arms are disclosed — NLJ is always binary so the not-two-children arm is unreachable-today future-proofing, and the reset ? is standard error propagation.
      artifacts: [crates/repark-core/src/nlj_build_reset.rs, crates/repark-core/src/session/tests/nlj_tight_pool.rs]
  complete: false
```
