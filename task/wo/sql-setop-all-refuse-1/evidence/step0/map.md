# task/wo/sql-setop-all-refuse-1/evidence/step0/ — the table on main

Measured on main `a3f8c7d9` (release build) and Spark 4.1.2; [provenance.md](provenance.md) has the
versions and the commands.

- [table.md](table.md) — the rendered table: one row per cell, the Spark 4.1.2 answer, each door's
  answer and its verdict (EQUAL, WRONG, REFUSED, ACCEPTED).
- `spark.jsonl`, `spark-door.jsonl`, `native-door.jsonl` — the raw cells, one JSON object per line
  (`group`, `op`, `shape`, `kind`, rows or the error class and text; write cells add the table state
  before and after and the stored rows).
- [cells.py](cells.py) — the cell runner: `cells.py <spark|spark-door|native> <warehouse> <out.jsonl>`;
  the Spark column reads the Iceberg runtime jar from `SPARK_ICEBERG_JAR`.
- [render.py](render.py) — `render.py <spark.jsonl> <spark-door.jsonl> <native-door.jsonl> <title>`
  prints the table.
- [provenance.md](provenance.md) — head, profile, versions, data, commands.

Up: [../map.md](../map.md).
