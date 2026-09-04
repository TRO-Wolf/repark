# map — python/dbt-repark/tests

## Purpose

The DBT-1 suite. It is **not** in `make ci`: `ci` is the JVM-free Rust-and-lint gate and this
suite needs the built native plus `dbt-core`. Run it explicitly:

```
.venv/bin/python -m pytest python/dbt-repark/tests -q
```

`conftest.py` puts two directories on `sys.path`: `python/dbt-repark/src`, so `dbt.adapters.repark`
resolves without an install, and `python/repark/tests`, so the gold models' SQL stays
**single-homed** in `_sql_harden_cutover_run.py` (the S6 program) instead of being copied here.

## Contents

- `conftest.py` — the two `sys.path` entries above.
- `test_statement_surface.py` — 27 cases: every statement shape dbt emits, run through
  `repark.sql()` on a memory catalog. Ten served, fifteen refused with the exact message, plus
  the two probes that show why `describe extended` is not the column source. This file is the
  design evidence for the route choice and the pin behind every registry row this unit filed.
  pins: dbt-1-adapter/C-001
- `test_gold_models.py` — the acceptance. Builds a dbt project in a tmp directory from the S6
  silver fixture, runs `dbt run` and `dbt test` through `dbtRunner`, and asserts the two gold
  tables answer the S6 measured rows and that all ten test blocks pass. Also holds the
  materialization refusals and the mutation guard.
  pins: dbt-1-adapter/C-002, C-003, C-004
- `test_aws_acceptance_gold.py` — the deferred Glue leg, gated on `REPARK_AWS_ACCEPTANCE=1` and
  the same env variables as `python/repark/tests/test_aws_acceptance.py`. It writes to
  `testing_repark_acceptance` and nowhere else. **The orchestrator runs it; a unit agent never
  does.**
  pins: dbt-1-adapter/C-005

## I want to...

| ...do this | go to |
|---|---|
| Add a statement shape to the measured table | `_served()` / `_refused()` in `test_statement_surface.py` |
| Change the gold project dbt builds | `_write_project` in `test_gold_models.py` |
| See which tests a broken CTAS macro reds | the mutation table in the unit ledger |

## Pointers

- Up: [../map.md](../map.md)
- The S6 program these tests seed from:
  [../../repark/tests/map.md](../../repark/tests/map.md)
- Ledger:
  [../../../task/ledgers/staging/dbt-1-adapter-ledger.md](../../../task/ledgers/staging/dbt-1-adapter-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: _sql_harden_cutover_run` | `conftest.py` did not load — run pytest from the repo root |
| `Invalid function 'date'` | the installed native predates DATE-FN-1; rebuild or repoint `repark.pth` |
| the Glue leg runs by accident | `REPARK_AWS_ACCEPTANCE` is set in the shell; unset it |
