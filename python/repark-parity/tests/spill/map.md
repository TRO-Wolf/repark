# map — python/repark-parity/tests/spill

## Purpose

NEVEROOM-1: the spill-coverage matrix. One cell = one worker
subprocess running one operator over an in-engine `range()` input sized to a
multiple of the memory limit, under an address-space cap, with the outcome
folded by a classifier that knows exactly three outcomes: `spilled`
(completed, spill bytes > 0 from the runtime metrics), `completed` (completed,
no spill), `refused` (a loud `MemoryError` / typed facade exception naming the
operator). A killed or non-zero-exit worker without a refusal is `KILLED`,
which fails the matrix and is never folded into an outcome.

Step 1 wired one CI-tier cell end to end: `sort` at 2× the 64 MB limit
(D-5). Step 2 adds the full 27-cell roster (9 operators × 2×/4×/8× at the 1 GB
limit of D-1), run on an idle release box by `matrix_run.py`, three
repetitions per cell (D-3); its document pair is
`docs/perf/spill-coverage-matrix-2026-09-11.{md,csv}`. Step 3 (S2-18) adds the
CI golden (`ci_golden.csv`: `sort` / `hash_aggregate` / `hash_join` at 2× of
the 64 MB pool, outcomes measured three times on the debug module), the
`test_ci_tier_matches_golden` and `test_full_matrix_csv_has_27_cells` pins,
the `## Never-OOM at v1.3` claim on the matrix document, and the
`docs/testing.md` pointer. The three non-outcome full-tier cells stay as
measured (`hash_join` 4× `KILLED`, `hash_aggregate` 2× `UNSTABLE`,
`window_unbounded` 2× `UNSTABLE`); a CI cell whose three reps disagree or
are `KILLED` is left out of the golden and named as residue.

The suite needs the native module (the worker builds a repark session), so
run it through `make py-test-spill-matrix` — not the JVM-free `make py-test`
environment, which has no native build and skips this directory via the
conftest guard.

## Measured facts this harness is built on (2026-09-10, debug `.venv` module)

- **The address-space cap is headroom, not an absolute ceiling.** D-2's
  `RLIMIT_AS = 3 × limit` cannot be an absolute cap: a loaded worker already
  reserves far more virtual address space than any multiple of the limit —
  measured VmSize 22.7 MB bare interpreter, 2.93 GB after `import pyarrow`,
  4.27 GB after `import repark`, 8.70 GB after a 64 MB-pool session build at
  `target_partitions=1`. The worker therefore applies the card's constant as
  the cell's own headroom, `RLIMIT_AS = VmSize_at_apply + 3 × limit`, after
  the session builds and before the cell runs — the formula
  H3-SPILL-RESIDUE-1 (ledger F-5) measured first. The measured CI cell used
  2.10× limit of the 3× headroom and completed. The absolute form is not a
  defensible alternative at any tier: at the CI tier (192 MB) the pyarrow
  import alone fails, and at the full tier (3 GB) the import baseline alone
  is 4.27 GB. **Ruled S2-8 (2026-09-10):** the full matrix runs this way.
- **The refusal surface is the facade's typed family, not only
  `AnalysisException`.** Measured at a 2 MB pool: the pool refusal surfaces
  as `PySparkException` ("Not enough memory to continue external sort …
  caused by Resources exhausted: Failed to allocate additional … for
  ExternalSorterMerge[0] … fair(pool_size: …)"). `AnalysisException` is a
  subclass of the same `PySparkException` family, so the classifier accepts
  `MemoryError`, `PySparkException`, or `AnalysisException`; a typed refusal
  must also name the operator (one of the roster row's `refusal_names`), and
  `MemoryError` is accepted without a name because CPython's own
  `MemoryError` carries no message (measured in H3-SPILL-RESIDUE-1 C-001).
  Any other caught exception exits the worker non-zero with no result JSON,
  which folds to `KILLED`. **Ruled S2-9 (2026-09-10).**
- **The runtime metrics do expose the spill counters**, so the card's
  temp-dir fallback probe is not needed: `EXPLAIN ANALYZE` output carries
  `spill_count` and `spilled_bytes` per executor, and the matrix reads them
  through `bench/spill/plan_metrics.py` (reused, not re-derived — the H3
  harness is the card's named starting point).
- **The engine's capacity parser refuses bare byte counts** (`invalid
  datafusion.runtime.memory_limit = '67108864': … Unit must be one of: 'K',
  'M', 'G'`), so the worker renders the limit as `64M`-shaped strings.

## Step-2 roster facts (2026-09-11, release module)

- **Every cell names its physical operator, and the plan text proves it.**
  The worker writes `EXPLAIN` plan text to a `<cell>.plan.txt` sidecar before
  the measured call (so a refused cell still leaves its plan behind) and
  carries the exec-name set in `plan_operators`. `sort_merge_join` is forced
  by `datafusion.optimizer.prefer_hash_join=false` (measured:
  `SortMergeJoinExec` in the plan); `nested_loop_join` is forced by the
  non-equi predicate `l.v < r.v` and its build side carries the sized
  payload through a computed-`v` subquery — that is the projection shape that
  puts `RepartitionExec` under the `CoalescePartitionsExec` beneath
  `NestedLoopJoinExec`, the H3 panic path the containment reports as a
  refusal. A passthrough `l.id < r.v` projection plans without that
  `RepartitionExec` and measures nothing.
- **Both equi-join sides are generated at the full multiple** (`right =
  "sized"`); the NLJ right side stays 64 rows (`right = "key64"`) so the
  cell terminates, the H3 shape.
- **Full tier runs at `target_partitions = 4`** (`FULL_PARTITIONS`), the H3
  baseline's own parameter and the value whose enforcer output puts
  `RepartitionExec` under the NLJ build side.
- **Measured `refusal_names`** come from the H3 evidence and this tree's
  cells: `ExternalSorter`/`ExternalSorterMerge`/`SortPreservingMergeExec`
  (sort, SMJ, windows — the merge exec's own reservation refuses too),
  `GroupedHashAggregateStream` (hash_aggregate), `HashJoinInput`
  (hash_join), `NestedLoopJoinLoad` (nested_loop_join), `UnnestExec`
  (dynamic_flatten). `collect` carries no names: its refusal is
  `MemoryError`, which needs none — and `--refusal-names ""` splits to the
  empty tuple, never a match-all (the worker filters empties).
- **`datafusion.execution.batch_size = 8192` is pinned for the full tier**
  (`FULL_SESSION_CONF`). The `generate_series` default batches 65536 rows,
  which at the 8× payload width is ~560 MB per batch — one batch's own
  un-accounted scan allocation aborts the worker (SIGABRT) before the
  operator under test is ever reached. 8192-row batches keep a single scan
  batch ≈ 70 MB at 8×, so the address-space cap bounds the operator, not
  the input reader.
- **Window cells select `payload` in their output.** Without it the
  optimizer prunes the column at the scan and the window operator sees an
  8-byte row — a 0.6 s no-op that measures nothing.
- **Cell kinds.** `sql` cells run `EXPLAIN ANALYZE` and read spill totals
  off the plan metrics. `collect` runs `session.sql(…).collect()` — the
  facade boundary is the measured operation, `EXPLAIN` only writes the plan
  sidecar. `flatten` builds the nested frame
  (`named_struct('a', id, 'b', payload)` + `array(payload)`),
  `dynamicFlatten()`s it into the `flat` view, then runs
  `EXPLAIN ANALYZE SELECT * FROM flat` — the `UnnestExec` pipeline under the
  pool; the `to_arrow` materialization cost is the `collect` row's
  boundary, not this one's.

## Contents

- `conftest.py` — the `pytest.importorskip("repark")` guard, same shape as
  `../torture/conftest.py`.
- `matrix_harness.py` — the parent-side core: the outcome vocabulary
  (`spilled` / `completed` / `refused`, plus the `KILLED` failure verdict),
  `classify_cell`, `require_no_killed_cells` (`MatrixKilledError`),
  `is_loud_refusal`, the `CellSpec` / `CellRecord` models, and
  `run_cell` (subprocess spawn with a timeout, worker env with
  `PYTHONPATH` → `bench/` for the plan-metrics parser and a per-cell
  `TMPDIR`, result-JSON read, outcome fold).
- `matrix_generators.py` — the D-1 sizing math: `GENERATOR_ROWS` = 1e6
  fixed rows, the Arrow row-byte constants, `payload_extra` (repeat width
  that lands the rows on the target bytes), `input_select_expr`
  (`range()` + `md5` + `repeat('x', n)` payload, no files),
  `input_arrow_bytes`, and `memory_limit_string`.
- `matrix_cells.py` — the roster: `CI_LIMIT_BYTES` (64 MB, D-5),
  `FULL_LIMIT_BYTES` (1 GB, D-1), `CI_PARTITIONS`, `FULL_PARTITIONS`,
  `CI_CELL_TIMEOUT_S`, `FULL_CELL_TIMEOUT_S`, `FULL_REPS`, `RosterRow`
  (`CellSpec` + `kind`/`conf`/`right`), the nine-operator `_ROSTER`,
  `FULL_CELLS` (roster × multipliers 2/4/8 = 27 cells), `CI_CELLS`
  (`sort`, `hash_aggregate`, `hash_join` at 2×, same roster rows as the
  full tier, 64 MB / `CI_PARTITIONS`), `worker_argv`, `json_out_path`,
  `plan_out_path`.
- `ci_golden.csv` — step-3 CI golden: columns `operator,multiple,outcome`.
  A CI cell whose three debug-module reps agree is a row; a disagreeing
  or `KILLED` cell is omitted and named as `NEVEROOM-1-R-00n` in the
  ledger. `sort` is required.
- `matrix_worker.py` — the worker subprocess entry: builds the bounded-pool
  session with the row's `conf`, registers the sized range input and the
  join rows' `other` input, applies the address-space cap
  (`RLIMIT_AS = VmSize_at_apply + 3 × limit`, verified by read-back),
  writes the `EXPLAIN` plan sidecar, executes the cell by `kind`
  (`EXPLAIN ANALYZE` + spill totals for `sql`/`flatten`, `.collect()` for
  `collect`), and writes the result JSON (`completed` with the spill
  totals and `plan_operators`, or `refused` when the caught error passes
  `is_loud_refusal`); anything else exits non-zero with no result JSON,
  which the parent folds to `KILLED`.
- `matrix_run.py` — the full-tier driver (step 2): `run_rep` runs every
  `FULL_CELLS` cell once per rep in a worker subprocess and appends each
  `CellRecord` as a JSONL line to the results file (so a run is resumable
  and a crash costs one cell); `fold_rows` folds the three reps per cell
  into the CSV (identical outcomes → that outcome, else `UNSTABLE`;
  `spill_bytes` is the max and `seconds` the median across reps); the D-4
  per-operator notes live in `_CELL_NOTES`.
- `test_classifier_pins.py` — the card's step-1 classifier pins:
  `test_classifier_three_outcomes_only` and
  `test_killed_subprocess_fails_matrix` (signal-kill, non-zero exit, and a
  silent exit-0 worker are all `KILLED`; `require_no_killed_cells` raises),
  plus the no-fourth-outcome fold pin (`degraded` / `clean_error` / `ok`
  payloads from the H3 vocabulary are `KILLED` here, never folded).
- `test_generator_sizing.py` — the generator pins: 2× the CI limit sizes
  1e6 rows of 134 Arrow bytes (89-char repeat), and the capacity-string
  rendering the engine's parser requires.
- `test_ci_tier_cell.py` — the CI-tier engine pins:
  `test_ci_tier_sort_cell_end_to_end` asserts the measured `spilled`
  outcome, a zero exit, positive spill bytes and count, and the cap
  arithmetic `rlimit_as_bytes == vm_size_at_cap + 3 × CI_LIMIT_BYTES`;
  `test_ci_tier_matches_golden` runs every `ci_golden.csv` cell once
  through `run_cell`, asserts the golden outcome, keeps the sort
  assertions, and calls `require_no_killed_cells`.
- `test_full_matrix_csv.py` — `test_full_matrix_csv_has_27_cells` reads
  the committed `docs/perf/spill-coverage-matrix-2026-09-11.csv` (no
  engine run): 27 rows = 9 operators × {2,4,8}; every outcome is
  `spilled` / `completed` / `refused` except the three R-1 cells
  (`hash_join` 4× `KILLED` #24768, `hash_aggregate` 2× `UNSTABLE` #22758,
  `window_unbounded` 2× `UNSTABLE` #22758), each of whose `notes` names
  the issue URL.
- `map.md` — this file.

The step's pins: `test_classifier_three_outcomes_only`,
`test_killed_subprocess_fails_matrix`, `test_generator_sizes_input_to_the_target_multiple`,
`test_memory_limit_string_matches_the_engine_capacity_parser`,
`test_ci_tier_sort_cell_end_to_end`, `test_ci_tier_matches_golden`, and
`test_full_matrix_csv_has_27_cells`.
pins: neveroom-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
pins: neveroom-1/C-008, C-009, C-010

The Makefile target `py-test-spill-matrix` is deliberately absent from
`preflight`.

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [../../../../task/ledgers/staging/neveroom-1-ledger.md](../../../../task/ledgers/completed/neveroom-1-ledger.md)
- Reused probe parser: [../../bench/spill/map.md](../../bench/spill/map.md)
  (`spill.plan_metrics`, the H3-SPILL-1 harness)
- Matrix document: [../../../../docs/perf/spill-coverage-matrix-2026-09-11.md](../../../../docs/perf/spill-coverage-matrix-2026-09-11.md)
