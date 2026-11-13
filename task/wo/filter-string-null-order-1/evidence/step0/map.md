# task/wo/filter-string-null-order-1/evidence/step0/

Step 0 of FILTER-STRING-NULL-ORDER-1. The base is #1316's head `35c5de77`, release build; the oracle
is PySpark 4.1.2 `local[2]`, UTC. Each probe ran inside the build-slot wrapper; Spark ran under the
JVM lock, one JVM at a time.

- [probe.py](probe.py) — the probe: 28 predicates on 11 doors (six Spark-door string-filter doors,
  `filter(F.expr)`, `spark.sql` `WHERE`, `selectExpr`, the native door's string filter and `WHERE`),
  3 scalar subqueries on two joined-frame doors, and 12 controls. Usage:
  `probe.py <spark|repark> <out.jsonl>`.
- [render.py](render.py) — labels each RePark cell against Spark (or the base, on the native door)
  and prints the tables.
- [record_oracle.py](record_oracle.py) — writes the facade pin's oracle
  (`python/repark/tests/functions/filter_string_null_order_1_spark_oracle.json`) from the Spark and
  base legs.
- [table-base.md](table-base.md) — the base against Spark: 126 WRONG-READ cells (20 on each of six
  string-filter doors, 3 on each joined-frame door), every control EQUAL.
- [table-head.md](table-head.md) — the base and the fix side by side: all 126 move to EQUAL, no
  other cell moves.
- [raw/](raw/map.md) — the three legs as jsonl.

Up: [../map.md](../map.md).
