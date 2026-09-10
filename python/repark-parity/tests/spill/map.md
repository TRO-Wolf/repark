# map — python/repark-parity/tests/spill

## Purpose

NEVEROOM-1 step 1: the spill-coverage matrix harness. One cell = one worker
subprocess running one operator over an in-engine `range()` input sized to a
multiple of the memory limit, under an address-space cap, with the outcome
folded by a classifier that knows exactly three outcomes: `spilled`
(completed, spill bytes > 0 from the runtime metrics), `completed` (completed,
no spill), `refused` (a loud `MemoryError` / typed facade exception naming the
operator). A killed or non-zero-exit worker without a refusal is `KILLED`,
which fails the matrix and is never folded into an outcome.

Step 1 wires one CI-tier cell end to end: `sort` at 2× the 64 MB limit
(D-5). The full 27-cell matrix (9 operators × 2×/4×/8× at the 1 GB limit of
D-1) is step 2 and runs on an idle box only; the CI golden and its comparison
pins are step 3.

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
  is 4.27 GB. Filed as a ruling question on the step-1 hand-back.
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
  which folds to `KILLED`.
- **The runtime metrics do expose the spill counters**, so the card's
  temp-dir fallback probe is not needed: `EXPLAIN ANALYZE` output carries
  `spill_count` and `spilled_bytes` per executor, and the matrix reads them
  through `bench/spill/plan_metrics.py` (reused, not re-derived — the H3
  harness is the card's named starting point).
- **The engine's capacity parser refuses bare byte counts** (`invalid
  datafusion.runtime.memory_limit = '67108864': … Unit must be one of: 'K',
  'M', 'G'`), so the worker renders the limit as `64M`-shaped strings.

No timing is claimed anywhere in step 1: another lane is building on this
box, the module is the debug `.venv` build (`repark._native.__debug_assertions__`
is True), and D-3's release-build idle-box rules belong to step 2.

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
  `CI_PARTITIONS`, `CI_CELL_TIMEOUT_S`, and `CI_CELLS` (the one step-1
  cell: `sort` at 2×, ordered by the payload itself).
- `test_classifier_pins.py` — the card's step-1 classifier pins:
  `test_classifier_three_outcomes_only` and
  `test_killed_subprocess_fails_matrix` (signal-kill, non-zero exit, and a
  silent exit-0 worker are all `KILLED`; `require_no_killed_cells` raises),
  plus the no-fourth-outcome fold pin (`degraded` / `clean_error` / `ok`
  payloads from the H3 vocabulary are `KILLED` here, never folded).
- `test_generator_sizing.py` — the generator pins: 2× the CI limit sizes
  1e6 rows of 134 Arrow bytes (89-char repeat), and the capacity-string
  rendering the engine's parser requires.
- `map.md` — this file.

Step 2 adds `matrix_worker.py` (the worker subprocess entry), the CI-tier
end-to-end pin, the full-tier roster and its D-3 rules; step 3 adds the CI
golden CSV and its comparison pins. The Makefile target
`py-test-spill-matrix` is deliberately absent from `preflight`.

## Pointers

- Up: [../map.md](../map.md)
- Reused probe parser: [../../bench/spill/map.md](../../bench/spill/map.md)
  (`spill.plan_metrics`, the H3-SPILL-1 harness)
