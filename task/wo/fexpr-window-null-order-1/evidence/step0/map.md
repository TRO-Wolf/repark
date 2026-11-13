# task/wo/fexpr-window-null-order-1/evidence/step0/

Step 0 of FEXPR-WINDOW-NULL-ORDER-1, 2026-11-12, model `claude-opus-5-5`.

**Provenance.** Main `a3f8c7d9`, built with `maturin develop --profile release` (module sha256
`c1ff69cf205d9c24d53c5e74a877e2995db91f9f5abb1904706cac2cd7004b72`); the fix on the same tree
(module sha256 `44d57fa0ec459c02ae058b70249b6fb30bf7279c28e6d0bb8d4e24bad7e7b455`). PySpark 4.1.2
`local[2]`, JDK 17, one JVM at a time under the JVM lock; every leg ran inside the build wrapper. The
three legs ran with this directory's final `probe.py`; each leg's header names its engine, version,
tree and module.

**Data.** `(id, ki INT, ks STRING, kd DOUBLE, g)`: `(1, 10, 'b', 1.5, 1)`, `(2, NULL, 'e', 2.5, 1)`,
`(3, 30, NULL, 3.5, 2)`, `(4, 20, 'a', NULL, 2)`, `(5, 40, 'd', 0.5, 1)`; one NULL per key column, so
every single-key order is total.

**Result.** On main, 358 cells are WRONG-READ: every window `ORDER BY` with no stated placement through
`F.expr` (169 in `select`, 169 in `withColumn`), 18 dedup cells and 2 scalar-subquery cells. On the fix
all 358 are EQUAL, and every control door answers byte for byte as on main: `spark.sql` (309 cells),
`selectExpr` (303), the native door (303), the sort and typed-`Window` forms (96). The six other moved
cells are aggregate `ORDER BY`, a syntax Spark refuses (card finding F1).

- [probe.py](probe.py) — the inventory probe: `python probe.py spark|repark <out.jsonl>`; one record per (cell, door).
- [render.py](render.py) — labels a RePark leg against the Spark leg (EQUAL, WRONG-READ, the refusal classes) and renders a table; the labels file it writes is not kept.
- [record_oracle.py](record_oracle.py) — writes the facade test's oracle from main's labels (the fixed, explicit-placement, control and native groups).
- [compare_legs.py](compare_legs.py) — main's labels against the fix's: which cells moved, per door.
- [table-main.md](table-main.md) — main's leg labelled: totals by door, by family, every non-EQUAL cell with both answers.
- [table-head.md](table-head.md) — the fix's leg labelled, the same layout.
- [compare.md](compare.md) — the moves between main and the fix, per door, and every moved cell.
- [raw/](raw/map.md) — the three legs as recorded.

Up: [../map.md](../map.md).
