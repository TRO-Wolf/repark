# Card DROPDUP-SUBSET-SCHEMA-1: `dropDuplicates(subset)` loses field metadata, qualifiers and self-join names

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** SIGNED-ZERO-NORMALIZE-1 part B
delta 2 re-check, 2026-11-13 (#1326, findings C1 and C2, class (c)); owner, 2026-11-13, about 14:52 UTC, verbatim
"Defaults, and merge #1323 when green".

**Status:** filed 2026-11-13, not dispatched. Severity **S3**. Pre-existing on main; part B's head is identical to
main on every field.

## Measured

- **C1, on main.** `dropDuplicates(subset)`, for every subset, float or not:
  - drops user field metadata (`{"c":"k"}` becomes `{}`); Spark keeps it;
  - drops the plan relation qualifiers (`a` becomes none);
  - renames a self-join's engine names (`__repark_l_…` becomes `__repark_sel_N`).
- **C2, errors on every build where Spark answers:**
  - `df.alias("a").dropDuplicates(["d"]).filter(F.col("a.d") >= 0)`: "No field named a.d";
  - `spark.table("sc.db.src").dropDuplicates(["d"]).select("src.d")`: `UNRESOLVED_COLUMN`;
  - a non-self join with two `d` columns, then `dropDuplicates(["d"])`: `AMBIGUOUS_REFERENCE`.
  - Also, `createDataFrame` with NOT NULL fields reports them nullable at the input already.

## Relation

Part B's delta 2 keeps the float-key projection's schema equal to main's (its whole-schema invariant), so these losses
are main's, not part B's.

## Close

`dropDuplicates(subset)` keeps field metadata, qualifiers and engine names, and the three C2 shapes answer as Spark
does; pinned over the re-check's frame shapes.
