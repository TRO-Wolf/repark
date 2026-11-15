# task/wo/decimal-double-union-coercion-1/evidence/part2-step0/probes/ — the part-2 step-0 runners

Comment-free Python. The commands are in [../map.md](../map.md).

- [p2_cells.py](p2_cells.py) runs the part-2 cells on Spark or RePark: the `IN` / `NOT IN` lists, the multi-branch
  unions, the `F.expr` / `filter(str)` / `selectExpr` subqueries, the MERGE sources and the DML `IN (VALUES …)`
  links, the RANGE-frame re-plan, the tripwire shapes and `INSERT … VALUES` into DECIMAL and DOUBLE targets. On
  RePark it also records the `IN` plan shapes.
- [in_cost.py](in_cost.py) times the `IN` forms: DOUBLE lists (the per-row form on main), decimal lists and decimal
  ranges joined by `OR` (the rewrite's forms), and the planning of a 1,000-constant list.

Up: [../map.md](../map.md).
