# task/wo/write-body-carriers-1/evidence/ — step 0 outputs

Recorded cells. Do not edit by hand; regenerate them with the recorder named
in each row.

- [spark-cells.json](spark-cells.json) + [spark-cells.md](spark-cells.md) —
  the 20 Spark 4.1.2 cells, written by
  [python/repark/tests/_record_write_body_carriers_1_spark.py](../../../../python/repark/tests/_record_write_body_carriers_1_spark.py).
- [main-cells.json](main-cells.json) + [main-cells.md](main-cells.md) —
  the 54 main cells (40 CTAS cells on format versions 2 and 3, 10
  `EXPLAIN ANALYZE` cells, 4 `PREPARE` / `EXECUTE` cells), written by
  [python/repark/tests/_record_write_body_carriers_1_main.py](../../../../python/repark/tests/_record_write_body_carriers_1_main.py).
- [seam.md](seam.md) — reading only: the two product paths that reach a CTAS
  body, with file and line, and the one candidate refusal point on the Spark
  door.
- [wrappers-ns.md](wrappers-ns.md) — build round 1 (2026-10-14, no product
  change): the instant each wrapper route stores into `timestamptz_ns` against
  plain INSERT's on main's module, the two functions each route reaches with
  file and line, and the parting point.

Up: [../map.md](../map.md).
