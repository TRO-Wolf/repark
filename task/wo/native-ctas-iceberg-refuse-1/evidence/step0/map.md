# map — task/wo/native-ctas-iceberg-refuse-1/evidence/step0

## Purpose

Step 0 of NATIVE-CTAS-ICEBERG-REFUSE-1 on main `a3f8c7d9`: every CTAS inventory row, target and
query shape on the native door, the two-step workaround, the existing-table case (F4) and the
controls; the same CTAS shapes on RePark's Spark door and on Spark 4.1.2.

## Contents

- [cells.py](cells.py) — the measuring script (`native`, `spark-door` or `spark`).
- [render.py](render.py) — renders the jsonl files into one table.
- [native-door.jsonl](native-door.jsonl), [spark-door.jsonl](spark-door.jsonl),
  [spark.jsonl](spark.jsonl) — the raw cells.
- [table.md](table.md) — the rendered table.
- [provenance.md](provenance.md) — builds, versions, data and commands.

## Pointers

- Up: [evidence map](../map.md)
