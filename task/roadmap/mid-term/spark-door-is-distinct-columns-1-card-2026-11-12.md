# Card SPARK-DOOR-IS-DISTINCT-COLUMNS-1: `a IS DISTINCT FROM b` over two columns refuses on the Spark door

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): step 0 on main `ae247767` and its side check `step0/idf_check.py`, re-measured by item 1 on main
`276dc503`; Fable's review of 2026-11-12, §3.

**Status:** filed 2026-11-12, not dispatched. Severity **S3, over-refusal, raised priority** (Fable). The refusal is
loud and changes no table. The priority is raised because this is the usual MERGE change-detection condition
(`WHEN MATCHED AND t.x IS DISTINCT FROM s.x`), so the owner's G8 workload may hit it.

**Measured on main `276dc503` against Spark 4.1.2** (cell CM-11):

- `SELECT id FROM t WHERE d IS DISTINCT FROM e` refuses on the Spark door for DOUBLE, FLOAT and the decimal twin:
  `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name`
  `` `spark_catalog`.`default`.`e` `` `cannot be resolved`. Spark answers `[7]`.
- The typed door (`~F.col('d').eqNullSafe(F.col('e'))`) and the native door answer. Their decimal twins are EQUAL; the
  float rows are part B part 1's signed-zero cells.
- Pre-existing: `idf_check.py` refuses the same way on a v1.5.6 release-branch build without part A. On both builds
  `NOT (d <=> e)` and `d IS DISTINCT FROM 1.5` answer.

**Fix direction:** the error names the right operand as a table-qualified name, `spark_catalog.default.e`. The likely
cause is a Spark-door name pass that reads `FROM e` inside `IS DISTINCT FROM e` as a relation; a literal operand is
not an identifier, which would explain why `IS DISTINCT FROM 1.5` answers. Not located. Find the pass, keep it out
of the `IS [NOT] DISTINCT FROM` operator, and pin the two-column form in a filter and in a MERGE `WHEN MATCHED AND`
condition on the Spark door.

**Close:** fixed with those pins, or declined by a recorded ruling.
