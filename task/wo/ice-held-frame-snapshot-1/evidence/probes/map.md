# task/wo/ice-held-frame-snapshot-1/evidence/probes/ — the cell and cost probes (comment-free Python)

- `common.py` — both engines' session (a `type=hadoop` catalog `lc`, `local[1]` for Spark, UTC, the ANSI mode as
  an argument), the error-condition reader, the row and `show` normalisers.
- `cells.py` — the cells: `cells.py <engine> <warehouse parent> <out> <group> <ansi>`; groups `HW` (holds × writes,
  `collect`), `WA` (`spark.table` × writes × actions), `HA` (holds × actions, `INSERT` and `DELETE`), `P` (time
  travel, refs, metadata tables), `S` (action sequences, the cached cells `C`, the Parquet inverse `Q`), `X` (the
  external writer).
- `writer.py` — the external writer `X` starts in a second process: one statement on the other engine, the same
  warehouse.
- `cmp.py` — verdicts: `cmp.py <raw dir> <verdicts.jsonl>`; EQUAL, WRONG or RAISE against Spark, both raising is
  EQUAL by class.
- `matrix.py` — the domain table's matrices from the verdicts.
- `cost.py` — the design-estimate cost cases over a 2M-row Iceberg table: `cost.py <engine> <warehouse parent> <out>
  <round>` (not sampled in this step 0; for the build to run under the exclusive lock).

Up: [../map.md](../map.md).
