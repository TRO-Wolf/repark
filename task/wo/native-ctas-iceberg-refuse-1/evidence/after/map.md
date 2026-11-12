# map — task/wo/native-ctas-iceberg-refuse-1/evidence/after

## Purpose

The head of `fix/native-ctas-iceberg-refuse-1` (release build): the step-0 cells again, and the
facade pin's red and green runs.

## Contents

- [native-door.jsonl](native-door.jsonl), [spark-door.jsonl](spark-door.jsonl) — `../step0/cells.py`
  on the head build, warehouse path replaced by `<warehouse>`.
- [table.md](table.md) — rendered by `../step0/render.py`.
- `red-main.txt` — `test_native_ctas_iceberg_refuse_1.py` on main's release build `a3f8c7d9`:
  85 failed (77 refusal cells and 8 existing-table cells), 85 passed (every control).
- `green-head.txt` — the same file on the head build: 170 passed.

Compared cell by cell with `../step0/`: on the native door only the Iceberg CTAS cells and the
existing-table cells changed (192 of 380), each to `UnsupportedOperationException` with the card's text,
no table after and an existing table's rows, snapshots and metadata files unchanged; the other 188
native cells (`REPLACE TABLE`, `CREATE TEMPORARY TABLE`, multi-statement, `CREATE EXTERNAL TABLE`,
the workaround, the in-memory CTAS and the controls) and all 66 Spark-door cells are identical.

## Pointers

- Up: [evidence map](../map.md)
