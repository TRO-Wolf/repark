# Unit ledger — NEVEROOM-1 step 1 · the spill-coverage matrix harness (one CI-tier cell)

**Unit:** NEVEROOM-1 step 1 · **Date:** 2026-09-10 · **Branch:** `feat/neveroom-1` · **Base:** `origin/main`
`52b604ab`
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Card NEVEROOM-1 builds the spill-coverage matrix (roadmap 1.3): every operator
of D-1 at 2×/4×/8× of the 1 GB memory limit, three outcomes per cell, the CI tier guarding
the cheap cells. H3-SPILL-1 measured the 180-cell truth table and its residue unit fixed the
two failure shapes; this unit turns that work into the standing coverage harness. Step 1 is
the harness and one cell; step 2 is the full matrix on an idle release box; step 3 is the
CI golden, the `docs/testing.md` pointer and the ledger close.

**Not in this step:** the full 27-cell matrix (step 2, release build + idle box, D-3), the
`docs/perf/spill-coverage-matrix-*` document (step 2), the CI golden CSV and its comparison
pins (step 3), the `PROJECT.md` pointer line (the orchestrator's at departure), any product
or operator change, any timing claim (another lane is building on this box, and the module
is the debug `.venv` build — `repark._native.__debug_assertions__` is True).

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

## PROPOSITION LEDGER — NEVEROOM-1 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The outcome classifier folds exactly three outcomes — `spilled` (completed, spill bytes > 0), `completed` (completed, no spill), `refused` (a loud `MemoryError`, or a typed facade exception naming the operator) — and no fourth name exists. | `test_classifier_three_outcomes_only` | **PROVEN** | Red first (`ModuleNotFoundError` on the base tree, pasted below), then green. The H3 vocabulary's `degraded` / `clean_error` / `ok` payloads fold to `KILLED`, never to an outcome (`test_classifier_never_folds_a_non_refusal_failure`). The refusal family is measured, not guessed: `issubclass(AnalysisException, PySparkException) == True` on this tree, so one family rule covers the card's named type, and the measured pool refusal surfaces as `PySparkException` naming `ExternalSorterMerge[0]` with `fair(pool_size: …)` in the message (2 MB-pool probe, 2026-09-10). `MemoryError` is accepted without an operator name because CPython's own `MemoryError` carries no message (measured in H3-SPILL-RESIDUE-1 C-001). |
| C-002 | A killed or non-zero-exit worker subprocess without a loud refusal is `KILLED`, and `KILLED` fails the matrix — it is never quietly folded into an outcome. | `test_killed_subprocess_fails_matrix` | **PROVEN** | A SIGKILL, a `SystemExit(3)`, and a silent exit-0 worker that wrote no result JSON each fold to `KILLED`; `require_no_killed_cells` raises `MatrixKilledError` over the three. The runner kills a worker past its cell timeout, which folds the same way. |
| C-003 | Inputs are generated in-engine from `range()` with a string payload column sized so the Arrow bytes hit the target multiple of the limit; no files. | `test_generator_sizes_input_to_the_target_multiple` | **PROVEN** | 1e6 fixed rows; 2× the 64 MB CI limit sizes an 89-char repeat — 134 Arrow bytes per row, within one row of the target (pinned exactly), projected as `concat(md5(cast(id as string)), repeat('x', 89)) AS payload` over `session.range`. `memory_limit_string` renders `64M` because the engine's capacity parser refuses bare byte counts (measured: `invalid datafusion.runtime.memory_limit = '67108864': … Unit must be one of: 'K', 'M', 'G'`), pinned with the `1024M` full-tier rendering. |
| C-004 | One cell runs end to end at CI tier — `sort` at 2× the 64 MB limit of D-5, in its own subprocess whose `RLIMIT_AS` is the measured baseline plus 3 × limit, with spill bytes read off the runtime metrics — and the measured outcome is `spilled`. | `test_ci_tier_sort_cell_end_to_end` | **PROVEN** | Harness-run record pasted below, verbatim: `spilled`, returncode 0, `spill_bytes=211812352`, `spill_count=7`, and `rlimit_as_bytes (8899100672) == vm_size_at_cap (8697774080) + 3 × 67108864`. The probe reuses `bench/spill/plan_metrics.py` — `spill_count` / `spilled_bytes` are measured present in `EXPLAIN ANALYZE` output, so the card's temp-dir fallback probe is not invoked and no ledger note is owed. The cap is the card's D-2 constant as the cell's own headroom; the measured basis and the ruling question are below. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

The classifier and generator pins ran before the harness modules existed and failed on the
base tree:

```text
____________ ERROR collecting tests/spill/test_generator_sizing.py _____________
ImportError while importing test module '/tmp/oc-cfg/python/repark-parity/tests/spill/test_generator_sizing.py'.
E   ModuleNotFoundError: No module named 'matrix_cells'
=========================== short test summary info ============================
ERROR python/repark-parity/tests/spill/test_classifier_pins.py
ERROR python/repark-parity/tests/spill/test_generator_sizing.py
!!!!!!!!!!!!!!!!!!! Interrupted: 2 errors during collection !!!!!!!!!!!!!!!!!!!!
2 errors in 0.12s
```

The classifier file's own traceback named `matrix_harness` the same way. After the harness
modules landed: 5 passed; after the worker and the CI cell landed: 6 passed.

## The harness-run record (verbatim)

`run_cell` over the CI-tier cell, 2026-09-10 (debug `.venv` module, busy box — the wall
clock is observability, not a claim):

```json
{
 "operator": "sort",
 "multiplier": 2,
 "outcome": "spilled",
 "returncode": 0,
 "spill_bytes": 211812352,
 "spill_count": 7,
 "error_type": null,
 "message": null,
 "wall_ms": 8118.296355998609,
 "rlimit_as_bytes": 8899100672,
 "vm_size_at_cap": 8697774080
}
```

`8899100672 == 8697774080 + 3 × 67108864` — the cap arithmetic the pin asserts.

## The two measured deviations from the card, and the ruling questions

Both were forced by measurement on this tree, both follow repo precedent, and both are
filed as hand-back questions (Q-1, Q-2) for the orchestrator before step 2 runs the full
matrix under them.

1. **D-2's `RLIMIT_AS = 3 × limit` is applied as headroom over the measured baseline, not
   as an absolute ceiling.** Measured VmSize on this tree: 22.7 MB bare interpreter, 2.93 GB
   after `import pyarrow`, 4.27 GB after `import repark`, 8.70 GB after a 64 MB-pool session
   build at `target_partitions=1`. An absolute 192 MB cap (CI tier) cannot even import
   pyarrow; an absolute 3 GB cap (full tier) is under the import baseline alone. The worker
   applies `RLIMIT_AS = VmSize_at_apply + 3 × limit` after the session builds and before the
   cell runs, verified by read-back — the formula H3-SPILL-RESIDUE-1 (ledger F-5) measured
   first. The card's purpose (a silent OOM kill is observable as `KILLED`) is preserved: the
   cell's own memory is bounded at 3 × limit, and the measured CI cell used 2.10× of it.
2. **D-2's refused type list is the facade's measured refusal surface.** The card names
   `MemoryError` / `AnalysisException`; the measured pool refusal is `PySparkException`
   ("Not enough memory to continue external sort … Resources exhausted …
   ExternalSorterMerge[0] … fair(pool_size: …)"). `AnalysisException` is a subclass of the
   same family, so the classifier accepts `MemoryError`, `PySparkException`, or
   `AnalysisException`; typed refusals must also name the operator (one of the roster row's
   `refusal_names` — for sort, measured `ExternalSorter`/`SortExec`), and a refusal payload
   that fails that check folds to `KILLED` rather than `refused`.

## Gates

| Gate | Exit |
|---|---|
| `make py-test-spill-matrix` | 0 (6 passed, 8.27 s) |
| `make py-test` | 0 (tests/spill skips itself in the isolated job via the conftest guard) |
| `make verify` | 0 |
| `uvx ruff@0.15.22 check python/repark-parity/tests/spill/` | 0 |
| `uvx ruff@0.15.22 format --check python/repark-parity/tests/spill/` | 0 |
| comment fence `git diff --cached … \| grep -P '^\+\s*(//\|#(?! noqa))'` | no match, both commits |

# Step 2 — the full 27-cell matrix on the idle release box

**Date:** 2026-09-11 · **Branch:** `feat/neveroom-1-step-2` · **Model:** swe-2-high
**Commits:** `7920c70a` (roster + kinds + driver), `0cf94b02` (roster trued against
measured plans).

**Rulings applied:** S2-8 closed Q-1 — `RLIMIT_AS = VmSize_at_apply + 3 × limit`
after session construction, verified by read-back (measured cap ≈ 11.8 GB over a
≈ 8.6 GB baseline). S2-9 closed Q-2 — `refused` is `MemoryError` or a
`PySparkException`-family exception naming the row's measured `refusal_names`;
everything else folds to `KILLED`. The step-1 harness already implements both.

## PROPOSITION LEDGER — NEVEROOM-1 step 2 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-005 | The D-1 roster is complete — the nine carded operators (`sort`, `hash_aggregate`, `hash_join`, `sort_merge_join`, `window_sliding`, `window_unbounded`, `dynamic_flatten`, `nested_loop_join`, `collect`) × the three multipliers 2×/4×/8× at the 1 GB limit = 27 cells, each forcing its named physical operator with measured `refusal_names`. | The `FULL_CELLS` roster in `matrix_cells.py` plus the per-cell `EXPLAIN` sidecar + `plan_operators` payload | **PROVEN** | 27 cells generated. Plan text proves the operator, not the name: `SortMergeJoinExec` appears only under `prefer_hash_join=false`; `NestedLoopJoinExec` carries `RepartitionExec` under `CoalescePartitionsExec` on the build side (the H3 panic shape — a passthrough projection plans without it and measures nothing); `UnnestExec` for the flatten kind; `WindowAggExec`/`BoundedWindowAggExec` for the windows. Two roster drafts failed measurement before the run: window SQLs that dropped `payload` measured a pruned 8-byte scan (0.6 s no-ops), and the NLJ build side had to carry the sized payload to load the pool. |
| C-006 | The full matrix ran on the idle release box — `pgrep -x java`/`cargo`/`rustc`/`maturin` all empty at launch — on the release module (`repark._native.__debug_assertions__ == False`), three repetitions per cell, one subprocess each; every cell's row is in `docs/perf/spill-coverage-matrix-2026-09-11.{csv,md}` with its folded outcome; `KILLED`/`UNSTABLE` are reported as such, never re-labelled. | `matrix_run.py --reps 3` run log + the committed CSV/document | **PROVEN** | 81 worker runs, ~9.7 min total on the idle box. Folded: 24 cells stable, `hash_join-4x` KILLED 3/3 (SIGABRT, "memory allocation of ~35 MB failed"), `hash_aggregate-2x` UNSTABLE (refused/spilled/refused — rep 2 spilled 7 350 412 902 B in 170 files), `window_unbounded-2x` UNSTABLE (refused/refused/KILLED). `datafusion.execution.batch_size=8192` is pinned for the full tier because the default 65536-row `generate_series` batch is ~560 MB at 8× — an un-accounted scan allocation that aborted workers before the operator ran (measured: sort-8x, hash_aggregate, hash_join KILLED pre-fix). |
| C-007 | The H3-SPILL rows reproduce their FIXED outcomes — `nested_loop_join` refuses with a typed exception (the contained panic), `collect` refuses as `MemoryError` — and every cannot-spill cell names its upstream DataFusion issue URL in the document's notes. | The matrix document's H3 section + D-4 notes column | **PROVEN** | `nested_loop_join` refused 9/9 as `PySparkException` naming `NestedLoopJoinLoad[0]` (`fair(pool_size: 1024.0 MB)`) with the contained `repartition/mod.rs:1277: partition not used yet` panic in worker stderr — the H3-SPILL-NLJ-1 fixed outcome. `collect` refused 9/9 as `MemoryError` — the H3-SPILL-COLLECT-1 fixed outcome. D-4 notes cite datafusion#24768 (hash join), #24661 (NLJ fallback re-execution), #22758 (un-accounted operators: windows, UnnestExec, facade boundary) — all fetched and verified open/real 2026-09-11. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED. The matrix documents two honest
boundary failures the card's done-when cannot erase: `hash_join-4x` is KILLED 3/3 and
`hash_aggregate-2x` / `window_unbounded-2x` are UNSTABLE — all three are upstream
memory-boundary behavior (no spill path, no accounting), reported verbatim per the
step's never-re-label rule; the fixes are W-3/upstream, not this card.

## Step-2 gates

| Gate | Exit |
|---|---|
| `make py-test-spill-matrix` | 0 (6 passed) |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | 0 |
| `python3 scripts/check_docs_links.py` | 0 |
| `make verify` | 0 |
| comment fence (each commit) | no match |
| `uvx ruff@0.15.22 check` / `format --check` on `tests/spill/` | 0 |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: neveroom-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The three-outcome vocabulary is pinned over completed, spilled and all three measured refusal shapes, and the fold is total — every payload class the harness can produce has a pinned verdict.
      artifacts: [python/repark-parity/tests/spill/test_classifier_pins.py, python/repark-parity/tests/spill/matrix_harness.py]
    - id: AT-2
      status: ATTACKED
      evidence: KILLED is pinned on a signal kill, a non-zero exit, and a silent exit-0 worker, and the matrix-level failure is pinned through require_no_killed_cells raising.
      artifacts: [python/repark-parity/tests/spill/test_classifier_pins.py]
    - id: AT-3
      status: ATTACKED
      evidence: The generator sizing math is pinned exactly (89-char repeat, 134 Arrow bytes per row, within one row of the target) and the capacity-string contract is pinned against the engine's measured parser refusal.
      artifacts: [python/repark-parity/tests/spill/test_generator_sizing.py, python/repark-parity/tests/spill/matrix_generators.py]
    - id: AT-4
      status: ATTACKED
      evidence: One cell per subprocess; the worker applies the address-space cap after the session builds and verifies it by read-back; the record carries rlimit_as_bytes and vm_size_at_cap so the arithmetic is auditable per run. Step 2 ran the same isolation over all 81 full-tier runs and the records carry the cap fields.
      artifacts: [python/repark-parity/tests/spill/matrix_worker.py, python/repark-parity/tests/spill/test_ci_tier_cell.py, python/repark-parity/tests/spill/matrix_run.py]
    - id: AT-5
      status: N/A
      justification: No dependency, lockfile, or workflow change; the Makefile gains one target only.
    - id: AT-6
      status: N/A
      justification: No product code changed; the harness is measurement-only under tests/.
    - id: AT-7
      status: ATTACKED
      evidence: Outcomes are read from the engine's own runtime metrics through the reused H3 parser, never from wall time; the record's wall_ms is labeled observability, not a claim, and no timing is asserted anywhere. Step 2's measured fixes are recorded in C-005/C-006 (batch_size=8192, payload carried through every measured operator, SortPreservingMergeExec added to refusal names after a measured mis-fold).
      artifacts: [python/repark-parity/tests/spill/matrix_worker.py, python/repark-parity/tests/spill/matrix_cells.py]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change; Cargo.toml and Cargo.lock are untouched.
    - id: AT-9
      status: N/A
      justification: No concurrency or shared state; one worker subprocess per cell, sequential.
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md moves in the same commit — tests/spill/map.md, tests/map.md, docs/perf/map.md and the staging ledger map — and the reused bench parser is pointed at, not copied.
      artifacts: [python/repark-parity/tests/spill/map.md, python/repark-parity/tests/map.md, task/ledgers/staging/map.md, docs/perf/map.md]
  complete: true
```
