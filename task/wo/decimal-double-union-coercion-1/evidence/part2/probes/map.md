# task/wo/decimal-double-union-coercion-1/evidence/part2/probes/ — part 2's build probes

Each probe takes `engine ansi warehouse-parent jar out` (the cost probes say their own arguments) and writes one JSON
line per cell. Run them with the probe interpreter and `PYTHONPATH=<package>`, never with `-I`.

- `build_cells.py` — the build's new cells: right-deep, mixed and five-branch unions, a CTE and a view over a union;
  `INTERSECT` / `EXCEPT` (and their ALL forms) with a mixed `VALUES` leg; standalone `VALUES` shapes; the zero
  neighbours; `EXPLAIN`; DataFrame filters and joins over a `VALUES` frame; the four literal shapes through `F.expr`
  on the Spark door and (repark only) the native door; native frames over `F.expr` / `filter(str)` `VALUES`
  subqueries; DML and MERGE over `VALUES` and union sources, copy-on-write and merge-on-read; `VALUES` writes.
- `fam.py`, `f3.py`, `identity.py`, `bound.py` — the inventory critic's probes, copied unchanged (critic §1 to §4).
- `verdicts.py` — joins Spark, a base and the head per probe and prints the verdict counts (`EQUAL->EQUAL`,
  `WRONG->EQUAL`, `EQUAL->WRONG`, `EQUAL->RAISE`, `still-wrong`), writing every non-EQUAL cell.
- `cost_p2.py` — part 2's new cost rows: `plan_values10k` with and without a double, `INSERT … VALUES` of 10,000 rows
  (DECIMAL and DOUBLE, unmixed and mixed), MERGE with a 1,000-row `VALUES` source, the precision-16 `IN` rows and the
  Iceberg `IN` of 20 with its scan plan.
- `delta_cells.py` — the delta's cells (2026-11-16): F3's family (a literal probe through `F.expr` in `filter`,
  `select`, `withColumn`, `selectExpr` and `filter(str)`, against `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` and a scalar
  comparison over a mixed `VALUES`, probes `1e-20`, `0.0`, `-0.0`, `0.1`), F2's outer references (`LATERAL` comma,
  `JOIN`, `CROSS`, `LEFT`, under and over a `UNION`, correlated scalar, `EXISTS`, `IN`, `HAVING`), F1's stray markers
  (every case and position, `F.expr`, an insert), and (repark only) the same on the native door under `nat/`.
- `delta_verdicts.py` — `delta_cells.py`'s verdicts: `raw tag head base`; it splits still-wrong cells by whether the
  answer or only the error text moved, and compares the native door head against base.
- `phase_split.py` — times `spark.sql` (parse, mark, plan, eager analysis) and `collect` for the 10,000-row `VALUES`
  with and without one double: `label out`.
- `p16_data.py` — writes `cost-data/p16`, a `DECIMAL(16,15)` column of 100M rows derived from `big.d10`.

Up: [../map.md](../map.md).
