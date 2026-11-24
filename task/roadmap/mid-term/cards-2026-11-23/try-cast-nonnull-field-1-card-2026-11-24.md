# Card TRY-CAST-NONNULL-FIELD-1: a function over `TRY_CAST` of a NOT NULL column crashes on its NULLs

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside nullability sweep, checked
here against Spark. **Severity:** S2 (owner, 2026-11-24): loud. Every cell crashes; none answers wrongly.
**Status:** filed, not dispatched.

## Measured

- **Sweep:** 542 cells, on main `e80e6bc8` against Spark 4.1.2. In each ANSI mode, 87 cells answer in Spark and crash
  in RePark with `Arrow error: … Column '…' is declared as non-nullable but contains null values`. The same 87 crash in
  both modes.
- **Wrappers:** `TRY_CAST(s AS INT)`, `-TRY_CAST(…)`, `NOT TRY_CAST(… AS BOOLEAN)`, and `TRY_CAST` to string, double
  and date, under 47 functions. Among them are `abs`, `+`, `-`, `*`, `length`, `concat`, `floor`, `date_add`,
  `date_part`, `extract`, `split_part`, `str_to_map` and `named_struct(…).field`.
- **Separate shapes in the same family:** `CASE` with no `ELSE` under `array_remove`, `array_remove` with a NULL
  element, and `make_dt_interval` over a NaN column.

Re-measured on a build of main at `fa7d75e4`, over `(SELECT 'x' AS s UNION ALL SELECT '7')`:

| Statement | Spark 4.1.2 | RePark |
|---|---|---|
| `SELECT abs(TRY_CAST(s AS INT))` | NULL, 7 | **crash** |
| `SELECT TRY_CAST(s AS INT) + 1` | NULL, 8 | **crash** |
| `SELECT -TRY_CAST(s AS INT)` | NULL, -7 | NULL, -7 |

## Cause

- In `datafusion-physical-expr` 54.1.0, `TryCastExpr::nullable` returns true, but `TryCastExpr::return_field` keeps
  the input field's nullability (`expressions/try_cast.rs`).
- A function that sets its output nullability from its input fields then declares a non-null column, and Arrow
  refuses the batch.

## Not yet known

- The seat. The outside review proposes a physical-planning repair that wraps the field only where a function
  consumes it, so file and Iceberg pruning are kept. It also proposes a check after projection pushdown.
- Whether an upstream fix exists in a later DataFusion.

Up: [map.md](map.md).
