# map — python/repark-parity/tests/torture

## Purpose

The TORTURE-1 suite: both repark doors (DataFrame read and `spark.sql` over a
temp view) against the seeded torture families, asserting per D-3 that the read completes
or refuses loud (no panic, no silent truncation), the row count equals the generated count,
the schema equals the generator's declared expectation, and for `inference` the inferred
type per column equals the expected one. Generators:
[../../fixtures/torture/map.md](../../fixtures/torture/map.md).

The suite needs the native module (the doors are repark's), so run it through
`make py-test-torture` or the .venv pytest — not the JVM-free `make py-test` environment.

## Contents

- `conftest.py` — one session-scoped repark session and session-scoped tier-aware family
  data for every registered family (ci: 10k rows in a temp dir; full:
  `/tmp/torture/<family>` reuse), seed 7.
- `_support.py` — the shared helpers: manifest-aware generation, the two door readers, the
  loud-outcome readers, and the loud-rows assertion.
- `test_torture_nested.py` — the nested cells: protocol conformance, row counts and
  declared-schema equality on both doors for Parquet, the JSON-text CSV leg's
  completes-or-refuses-loud plus row count, and the `--depth`/`--width` scaling cell
  (D-6) reading a depth-2/width-1 generate against that scale's declared schema.
  pins: torture-1/C-002, C-004
- `test_torture_inference.py` — the inference cells: protocol conformance, row counts on
  both doors, the per-column declared-type cells on both doors (the `boolish` column is
  `xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")` — repark answers `int64` where
  Spark narrows to `int`), and the resolved-type Parquet schema cells.
  pins: torture-1/C-003, C-004, C-006
- `test_torture_extreme_types.py` — the extreme_types cells (step 2): protocol
  conformance, row counts and declared-schema equality on both doors for Parquet
  (`decimal(38,18)`, `decimal(38,0)`, three string columns), CSV row counts on both doors,
  and the per-column CSV type cells (`big38` is `xfail(strict=True,
  reason="CSV-INFER-20DIGIT")`; the three string columns are live green pins).
  pins: torture-1/C-010, C-011, C-013
- `test_torture_smartcsv.py` — the smartcsv cells (step 2): protocol conformance, Parquet
  row counts and declared-schema equality on both doors (capitalised and space-bearing
  names kept verbatim), the capitalised-header CSV leg's completes-or-refuses-loud cell
  (green through the measured loud refusal on both doors), and the CSV schema cell
  `xfail(strict=True, reason="CSV-INFER-HEADER-CASE")`.
  pins: torture-1/C-010, C-011, C-013
- `test_torture_temporal.py` — the temporal cells (step 2): protocol conformance, Parquet
  row counts and the full declared schema (epoch-edge and i64-bound timestamps, the
  whole-day-edge days, `duration[us]`, the MonthDayNano decomposition) on both doors, CSV
  row counts, the per-column CSV type cells (`months`/`days`/`nanos` are strict xfails
  citing `CSV-INFER-INT32-WIDTH`), the `try_add(day, INTERVAL 0 HOUR)` promotion cell
  (`xfail(strict=True, reason="BL-14")`), and the `day + INTERVAL 1 DAY` whole-day-edge
  cell (`xfail(strict=True, reason="DATE-INTERVAL-NSBOUND-1")`).
  pins: torture-1/C-010, C-011, C-013
- `test_torture_decimal_overflow.py` — the decimal_overflow cells (step 2): protocol
  conformance, Parquet row counts and the `decimal(38,0)` schema on both doors, CSV row
  counts, the per-column CSV type cells (both strict xfails citing
  `CSV-INFER-20DIGIT`), the `SUM` overflow cell refusing loud on both doors
  (`xfail(strict=True, reason="SUM-DEC-I128WRAP-1")` — repark answers the wrapped i128
  value today), and the `AVG` overflow cell green on the measured loud refusal of both
  doors.
  pins: torture-1/C-010, C-011, C-013
- `test_torture_secrets.py` — the secrets cells (step 3): protocol conformance and the
  needle-set membership pin (every `FLAGGED_COLUMN_NAMES` entry trips
  `prop_key_is_secret`; every ordinary name does not — D-4a), row counts and the shared
  declared schema on both doors for Parquet and CSV, `test_secret_flag_off_warn_refuse`
  parametrized over both doors (`off` reads clean; `warn` emits exactly one
  `WARNING: flag_secret_columns=warn` stderr line naming every flagged column and no
  ordinary one; `refuse` raises `AnalysisException` naming them — for the sql door the
  temp view is registered from the flagged read itself), the bad-value refusal naming
  the option and its three values, the option refused loud on readers without the csv/json
  option map (parquet direct and `format('parquet').load`), and the JSON-door refuse.
  pins: torture-1/C-017, C-018, C-019
- `test_generate_is_deterministic.py` — byte-identical same-seed CLI runs per family,
  different-seed bytes, the unknown-family and bad-rows refusals, the repository-internal
  output refusal, and the manifest reuse rule (matching rows+seed reuses, a mismatch
  regenerates).
  pins: torture-1/C-001
- `test_ci_tier_under_60s.py` — the CI-tier workload pins: both step-1 families, the
  four step-2 families and the step-3 secrets family each generate and read on both doors
  inside the 60-second budget, at the CI row budget regardless of the active tier.
  pins: torture-1/C-001, C-014, C-022
- `map.md` — this file.
- The step's process clauses are recorded in the ledger this map cites: the red-first run
  (the suite failed with `ModuleNotFoundError` before the package existed), the tiered
  `make py-test-torture` target that is deliberately absent from `preflight`, the map
  lockstep for every new directory, and the green gate runs.
  pins: torture-1/C-005, C-007, C-008, C-009
- The step-2 process clauses (the four families' red-first run, the filed registry rows,
  the honest no-live-Spark labeling, and the gate runs) are in the same ledger under
  C-010 onward.
  pins: torture-1/C-010, C-011, C-012, C-013, C-014, C-015, C-016
- The step-3 process clauses (the family's red-first `ModuleNotFoundError` run, the flag
  cells' red run against the unimplemented option, the Rust parser red, D-4a recorded,
  and the gate runs) are in the same ledger under C-017 onward.
  pins: torture-1/C-021, C-023

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [../../../../task/ledgers/staging/torture-1-ledger.md](../../../../task/ledgers/staging/torture-1-ledger.md)
- Registry: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
  `CSV-INFER-INT32-WIDTH`, `CSV-INFER-HEADER-CASE`, `SUM-DEC-I128WRAP-1`,
  `DATE-INTERVAL-NSBOUND-1`

## Why this directory skips itself in the isolated parity job

`ci.yml`'s python step (and `make py-test`) runs `pytest python/repark-parity/tests -q` in an
isolated env with **no native build**, so `repark` is not importable there. Every cell in this
directory reads *through the product*, so `conftest.py` opens with `pytest.importorskip("repark")`:
the directory skips itself in that job and runs in full under `make py-test-torture`, which
requires the native module. Without the guard the isolated job fails at collection with
`ModuleNotFoundError: No module named 'repark'` — which is exactly what it did on the first push
of TORTURE-1 step 1.

The guard is in `conftest.py`, not in the Makefile: `make py-test` states that it mirrors the
`ci.yml` python step, and adding an `--ignore` there would have made the local target green while
CI stayed red.

The imports below the guard carry no `# noqa: E402` marker: this repo's ruff configuration does
not enable `E402`, so the directive is unused and `RUF100` reds on it.
