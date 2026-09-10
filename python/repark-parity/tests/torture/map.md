# map — python/repark-parity/tests/torture

## Purpose

The TORTURE-1 suite skeleton: both repark doors (DataFrame read and `spark.sql` over a
temp view) against the seeded torture families, asserting per D-3 that the read completes
or refuses loud (no panic, no silent truncation), the row count equals the generated count,
the schema equals the generator's declared expectation, and for `inference` the inferred
type per column equals the expected one. Generators:
[../../fixtures/torture/map.md](../../fixtures/torture/map.md).

The suite needs the native module (the doors are repark's), so run it through
`make py-test-torture` or the .venv pytest — not the JVM-free `make py-test` environment.

## Contents

- `conftest.py` — one session-scoped repark session and session-scoped tier-aware family
  data (ci: 10k rows in a temp dir; full: `/tmp/torture/<family>` reuse), seed 7.
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
- `test_generate_is_deterministic.py` — byte-identical same-seed CLI runs per family,
  different-seed bytes, the unknown-family and bad-rows refusals, the repository-internal
  output refusal, and the manifest reuse rule (matching rows+seed reuses, a mismatch
  regenerates).
  pins: torture-1/C-001
- `test_ci_tier_under_60s.py` — the CI-tier workload pin: both families generate and read
  on both doors inside the 60-second budget, at the CI row budget regardless of the active
  tier.
  pins: torture-1/C-001
- `map.md` — this file.
- The step's process clauses are recorded in the ledger this map cites: the red-first run
  (the suite failed with `ModuleNotFoundError` before the package existed), the tiered
  `make py-test-torture` target that is deliberately absent from `preflight`, the map
  lockstep for every new directory, and the green gate runs.
  pins: torture-1/C-005, C-007, C-008, C-009

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [../../../../task/ledgers/staging/torture-1-ledger.md](../../../../task/ledgers/staging/torture-1-ledger.md)
- Registry: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md) `CSV-INFER-INT32-WIDTH`

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
