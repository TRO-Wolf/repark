# map — python/dbt-repark/tests

## Purpose

The DBT-1 suite. Run it with the gate target:

```
make py-test-dbt
```

It is wired into **`make preflight`**, immediately after `py-test-facade`, and deliberately
**not** into `make ci`. Three measured reasons, all recorded at
[../../../task/ledgers/staging/dbt-1-adapter-ledger.md](../../../task/ledgers/staging/dbt-1-adapter-ledger.md)
§9: `ci` is native-build-free by design and this suite imports `repark`; CI does not execute
`make ci` at all (`.github/workflows/ci.yml` invokes each target and script individually); and
ci.yml's Python job does not build the native module, so `wheels.yml` `smoke` is the only
existing CI job where the suite could run. The target provisions `dbt-core==1.9.11` and
`dbt-spark==1.9.3` and expects the native module `py-test-facade` builds.

`conftest.py` puts two directories on `sys.path`: `python/dbt-repark/src`, so `dbt.adapters.repark`
resolves without an install, and `python/repark/tests`, so the gold models' SQL stays
**single-homed** in `_sql_harden_cutover_run.py` (the S6 program) instead of being copied here.

## Contents

- `conftest.py` — the two `sys.path` entries above.
- `test_statement_surface.py` — 30 cases: every statement shape dbt emits, run through
  `repark.sql()` on a memory catalog. Twelve served, sixteen refused with the exact message, plus
  the two probes that show why `describe extended` is not the column source. This file is the
  design evidence for the route choice and the pin behind every registry row this unit filed.
  It needs **no adapter**, which is why it stays green when the package is removed — the
  red-first evidence lives in the two files below.
  pins: dbt-1-adapter/C-001
- `test_cursor.py` — 10 cases over the cursor dbt drives: `fetchall` / `fetchmany` / `fetchone`
  across three-row results, `description` across two columns, the zero-column DDL result, the
  refused binding, and two cursors that do not drain each other. Added in round 2: the multi-row
  path had **no** coverage, and truncating `fetchall` to one row survived the whole suite
  (ledger F-DBT-1-5).
  pins: dbt-1-adapter/C-002
  The `description` type of a VALUES-literal column reads `int32` since TYPES-1 (2026-09-05): a bare
  integer literal is Spark `int` on the Spark door.
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
| See which tests a broken CTAS macro reds | the mutation table (§6) in the unit ledger — it has a zero-red control |
| Understand why this suite is not in `make ci` | the unit ledger §9 |

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
