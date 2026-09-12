# map — python/repark-parity/tests/cast

## Purpose

PERF-CAST-1 step 1 pins: the committed nine-cell CAST-cost CSV, the 2500-cast
standalone regression budget (1.5× the measured median on this box), and the
stricter linear-from-50 pin that is `xfail(strict=True)` until a fix or
DataFusion bump restores linear scaling (the spill-golden shape: a committed
measurement plus a pin that flips when the cost moves).

The suite needs the native module (the live cell builds a repark session), so
the isolated parity job (`make py-test`) collects nothing here. Run through
`.venv/bin/python -m pytest python/repark-parity/tests/cast`.

This file closes when PERF-CAST-1 merges, or when the owner closes the slate
row.

## Contents

- `conftest.py` — `pytest.importorskip("repark")`, same shape as
  `../spill/conftest.py` and `../torture/conftest.py`.
- [test_cast_cost_pin.py](test_cast_cost_pin.py) — three pins:
  `test_cast_cost_golden_csv_has_nine_cells` (CSV-only, nine
  shape×count rows), `test_cast_2500_standalone_under_regression_budget`
  (runs `measure_one_cell` and asserts 1.5× the golden median),
  `test_cast_2500_over_aggregate_plan_under_linear_budget` (strict xfail:
  2500-cast over_aggregate plan ≤ 50× the 50-cast plan median).
  pins: perf-cast-1/C-001, C-002, C-003, C-004
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- Bench: [../../bench/cast/map.md](../../bench/cast/map.md)
- Golden CSV: [../../../../docs/perf/cast-cost-2026-09-12.csv](../../../../docs/perf/cast-cost-2026-09-12.csv)
- Ledger: [../../../../task/ledgers/staging/perf-cast-1-ledger.md](../../../../task/ledgers/staging/perf-cast-1-ledger.md)
