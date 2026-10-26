# Card SHOW-FUNCTIONS-1: bare `SHOW FUNCTIONS` and the routine listing answer on both Python doors

**Date:** 2026-10-26. **Filed by:** Claude Haiku 5.5 (`claude-haiku-5-5`), clerk, from the NS-TRUTHUP-1 measurement
(order [task/wo/ns-truthup-1/order.md](../../wo/ns-truthup-1/order.md); cells
[evidence](../../wo/ns-truthup-1/evidence/cells-2026-10-26.json)).

**Status:** filed, not scheduled. Grade: clerk (no design decision; the work makes a catalog listing answer on two
doors). It blocks registry row `ICE-TSNS-SQL-1-R-013` ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md))
and ledger clause C-002 of NS-TRUTHUP-1.

## Why

The nanosecond wall kernel `__repark_cast_timestamp_ns__` resolves in a `SELECT` (cell `r013.select`: `timestamp[ns]`).
Row R-013 also claims that the reserved name sits in the routine listing beside `__repark_cast_timestamp_ntz__` and
`__repark_cast_map__`. That listing cannot be read on either Python door today, so the row cannot be declared:

| Statement | Measured answer (2026-10-26, main `9fd60149`) |
|---|---|
| `SELECT routine_name FROM information_schema.routines ORDER BY 1` | `table 'spark_catalog.information_schema.routines' not found` |
| `SHOW FUNCTIONS` | `table 'datafusion.information_schema.parameters' not found` (door-wide) |
| `SHOW FUNCTIONS IN ice.system` | answers (the NS-TRUTHUP-1 builder's note; not in the cells file) |

## What the card asks

1. Bare `SHOW FUNCTIONS` answers on both Python doors: the native `repark.sql()` door and the Spark facade.
2. The listing carries the reserved `__repark_` names, so a filter on that prefix returns them and R-013 can name the
   reserved names that sit beside the wall kernel.

## Done when

- `SHOW FUNCTIONS` answers on both Python doors, with one pin per door (the two-doors rule in AGENTS.md).
- R-013 is re-measured on the listing and re-dated: DECLARED with its reserved neighbours listed, or OPEN with a dated reason.
- Ledger clause C-002 of NS-TRUTHUP-1 is re-verdicted with the new cells.

## Retires

When R-013 closes. Out of scope: any change to what `SHOW FUNCTIONS IN ice.system` answers.
