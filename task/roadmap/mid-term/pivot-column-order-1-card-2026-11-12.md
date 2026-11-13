# Card PIVOT-COLUMN-ORDER-1: a discovered pivot puts NULL last, names NaN `nan`, and leaves NaN and the zeros in arrival order

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): item 1's re-measure on main `276dc503`, finding F2.

**Status:** filed 2026-11-12, not dispatched. Severity **S3**: positional access to a pivot's columns reads the wrong
column. Not signed zero; part B part 1's normalisation of NaN and the zeros does not remove the NaN sort problem.

**Measured on main `276dc503` against Spark 4.1.2** (cell GB-11, typed door, `pivot(<col>)` with no values list):

| type | Spark's columns | RePark's columns, step 0 (`ae247767`) | RePark's columns, item 1 (`276dc503`) |
|---|---|---|---|
| DOUBLE | `g, null, 0.0, 1.5, NaN` | `g, -0.0, 0.0, 1.5, nan, null` | `g, 1.5, nan, 0.0, -0.0, null` |
| FLOAT | `g, null, 0.0, 1.5, NaN` | `g, nan, 0.0, -0.0, 1.5, null` | `g, -0.0, nan, 0.0, 1.5, null` |
| DECIMAL | `g, null, 0.00, 1.50, 9.99` | `g, 0.00, 1.50, 9.99, null` | same as step 0 |

- The float column order changed between two runs of the same build on the same seed: it is run-dependent. These are
  the only two cells of item 1's re-measure whose answer moved (`remeasure/differences.jsonl`, kind `columns`).
- The `-0.0` column is the signed-zero split that part B part 1 removes.

**Cause (read on `276dc503`):** `_pivot_sort_discovered_values` in
`python/repark/src/repark/spark/dataframe/grouped_pivot.py` sorts with Python's `list.sort`. NaN compares false both
ways and `-0.0 == 0.0`, so those values keep hash-aggregate discovery order. It also appends NULL last, and the
comment beside its caller in `joins_columns.py` says that matches Spark; the measurement says Spark puts NULL first.
`_pivot_value_column_name` names the NaN column `str(nan)`, which is `nan`; Spark writes `NaN`.

**Fix direction:** sort discovered values in Spark's order: NULL first, then ascending with the zeros as one value
and NaN last. Name NaN `NaN`; check the infinities' names against Spark in the same pin (not measured). Pin the three
GB-11 column lists above.

**Close:** fixed with that pin, or declined by a recorded ruling.
