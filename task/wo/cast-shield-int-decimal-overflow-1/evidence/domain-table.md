# CAST-SHIELD-INT-DECIMAL-OVERFLOW-1: the measured table

Measured 2026-11-25 with live Spark 4.1.2 (`iceberg-spark-runtime-4.1_2.13-1.11.0`), main's package
(`BUILT_FROM 57c92b41`; main `8c07d6197` adds only Markdown) and the Ruling 1 head package (`BUILT_FROM 075ce6a6`),
both ANSI modes. Scripts: [probe_step0.py](probe_step0.py) (cells and sources), [compare_step0.py](compare_step0.py)
(this table), [viewcheck.py](viewcheck.py) (the temp-view finding). Verdicts: `same` equals Spark; `SILENT` answers
where Spark raises; `WRONG` answers other rows; `OVER-RAISE` raises where Spark answers; `refuse` raises with another
condition. `moved` names a cell whose answer differs between main and head. `pushdown/accepted` is a RePark-only probe
of the `datafusion.execution.parquet.pushdown_filters` setting (Spark has no such key) and is not a cell. Summary and
seat inventory: [summary.md](summary.md).

## Step-0 cells (1019, both modes)

| cell | door | statement | ANSI | Spark 4.1.2 | main | main vs Spark | head | head vs Spark | moved |
|---|---|---|---|---|---|---|---|---|---|
| `card/filter` | spark.sql | `WHERE CAST(b AS DECIMAL(5,0)) = 1 over (SELECT CAST(123456 AS BIGINT) AS b)` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `card/projection` | spark.sql | `SELECT CAST(b AS DECIMAL(5,0)) over (SELECT CAST(123456 AS BIGINT) AS b)` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `conj/big_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(bi AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(bi AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_int/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(bi AS INT) = 1` | true | [[1]] | ERR [CAST_OVERFLOW] | OVER-RAISE | ERR [CAST_OVERFLOW] | OVER-RAISE |  |
| `conj/big_to_int/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(bi AS INT) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_int/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS INT) = 1` | true | [[1]] | ERR [CAST_OVERFLOW] | OVER-RAISE | ERR [CAST_OVERFLOW] | OVER-RAISE |  |
| `conj/dbl_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(db AS DECIMAL(5,0)) = 1` | true | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/dbl_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(db AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dbl_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(db AS DECIMAL(5,0)) = 1` | true | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/dec_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(dd AS DECIMAL(5,1)) = 1` | true | [[1]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | OVER-RAISE | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | OVER-RAISE |  |
| `conj/dec_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(dd AS DECIMAL(5,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dec_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(dd AS DECIMAL(5,1)) = 1` | true | [[1]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | OVER-RAISE | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | OVER-RAISE |  |
| `conj/int_to_small/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ii AS SMALLINT) = 1` | true | [[1]] | ERR [CAST_OVERFLOW] | OVER-RAISE | ERR [CAST_OVERFLOW] | OVER-RAISE |  |
| `conj/int_to_small/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ii AS SMALLINT) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/int_to_small/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ii AS SMALLINT) = 1` | true | [[1]] | ERR [CAST_OVERFLOW] | OVER-RAISE | ERR [CAST_OVERFLOW] | OVER-RAISE |  |
| `conj/str_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ss AS DECIMAL(5,0)) = 1` | true | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/str_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ss AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/str_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ss AS DECIMAL(5,0)) = 1` | true | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/tiny_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ti AS DECIMAL(3,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/tiny_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ti AS DECIMAL(3,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/tiny_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ti AS DECIMAL(3,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `ctas/empty` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | true | [] | [] | same | [] | same |  |
| `ctas/empty#readback` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | true | [] | [] | same | [] | same |  |
| `ctas/fit` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `ctas/fit#readback` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `ctas/full` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/eq/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/eq/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/eq/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/eq/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/eq/view` | view | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/is_null/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/is_null/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/is_null/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/is_null/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/is_null/view` | view | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/lt/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/lt/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/lt/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/lt/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/lt/view` | view | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t/projection/col_cast` | col_cast | `CAST(bi AS DECIMAL(5,0)) projected over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/projection/expr` | expr | `CAST(bi AS DECIMAL(5,0)) projected over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/projection/select_expr` | select_expr | `CAST(bi AS DECIMAL(5,0)) projected over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/projection/sql` | sql | `CAST(bi AS DECIMAL(5,0)) projected over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t/projection/view` | view | `CAST(bi AS DECIMAL(5,0)) projected over t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t_fit/eq/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/view` | view | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t_fit/is_null/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | true | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | true | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | true | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | true | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/view` | view | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `door/t_fit/lt/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | true | [] | [] | same | [] | same |  |
| `door/t_fit/lt/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | true | [] | [] | same | [] | same |  |
| `door/t_fit/lt/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | true | [] | [] | same | [] | same |  |
| `door/t_fit/lt/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | true | [] | [] | same | [] | same |  |
| `door/t_fit/lt/view` | view | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `door/t_fit/projection/col_cast` | col_cast | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/expr` | expr | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/select_expr` | select_expr | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/sql` | sql | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/view` | view | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/empty` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | true | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `insert_into/empty#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | true | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `insert_into/fit` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | true | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/fit#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | true | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/full` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `insert_into/full#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | true | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `insert_overwrite/empty` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | true | [] | ERR Error during planning: INSERT OVERWRITE empty source evaluates a CAST  | OVER-RAISE | ERR Error during planning: INSERT OVERWRITE empty source evaluates a CAST  | OVER-RAISE |  |
| `insert_overwrite/empty#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | true | [] | [[100, "Dec(7)"]] | WRONG | [[100, "Dec(7)"]] | WRONG |  |
| `insert_overwrite/fit` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_overwrite/fit#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_overwrite/full` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `insert_overwrite/full#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | true | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `kernel/col/bi->18,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->18,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 2` | true | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/col/bi->18,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -9223372036854775808 is too small | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->18,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 9223372036854775807 is too large  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->18,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->18,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 6` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/bi->18,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/bi->19,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->19,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 2` | true | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/col/bi->19,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 3` | true | [["Dec(-9223372036854775808)"]] | [["Dec(-9223372036854775808)"]] | same | [["Dec(-9223372036854775808)"]] | same |  |
| `kernel/col/bi->19,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 4` | true | [["Dec(9223372036854775807)"]] | [["Dec(9223372036854775807)"]] | same | [["Dec(9223372036854775807)"]] | same |  |
| `kernel/col/bi->19,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->19,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 6` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/bi->19,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/bi->20,2/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 1` | true | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/col/bi->20,2/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 2` | true | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/col/bi->20,2/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -9223372036854775808.00 is too sm | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->20,2/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 9223372036854775807.00 is too lar | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->20,2/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->20,2/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 6` | true | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/col/bi->20,2/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 7` | true | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/col/bi->21,2/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 1` | true | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/col/bi->21,2/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 2` | true | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/col/bi->21,2/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 3` | true | [["Dec(-9223372036854775808.00)"]] | [["Dec(-9223372036854775808.00)"]] | same | [["Dec(-9223372036854775808.00)"]] | same |  |
| `kernel/col/bi->21,2/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 4` | true | [["Dec(9223372036854775807.00)"]] | [["Dec(9223372036854775807.00)"]] | same | [["Dec(9223372036854775807.00)"]] | same |  |
| `kernel/col/bi->21,2/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->21,2/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 6` | true | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/col/bi->21,2/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 7` | true | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/col/bi->5,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->5,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->5,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -9223372036854775808 is too small | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->5,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 9223372036854775807 is too large  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->5,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->5,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/bi->5,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/ii->10,0/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ii->10,0/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 2` | true | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/col/ii->10,0/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 3` | true | [["Dec(-2147483648)"]] | [["Dec(-2147483648)"]] | same | [["Dec(-2147483648)"]] | same |  |
| `kernel/col/ii->10,0/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 4` | true | [["Dec(2147483647)"]] | [["Dec(2147483647)"]] | same | [["Dec(2147483647)"]] | same |  |
| `kernel/col/ii->10,0/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->10,0/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 6` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/ii->10,0/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/ii->10,1/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ii->10,1/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 2` | true | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/col/ii->10,1/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -2147483648.0 is too small to sto | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ii->10,1/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 2147483647.0 is too large to stor | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ii->10,1/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->10,1/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 6` | true | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/col/ii->10,1/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 7` | true | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/col/ii->11,1/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ii->11,1/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 2` | true | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/col/ii->11,1/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 3` | true | [["Dec(-2147483648.0)"]] | [["Dec(-2147483648.0)"]] | same | [["Dec(-2147483648.0)"]] | same |  |
| `kernel/col/ii->11,1/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 4` | true | [["Dec(2147483647.0)"]] | [["Dec(2147483647.0)"]] | same | [["Dec(2147483647.0)"]] | same |  |
| `kernel/col/ii->11,1/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->11,1/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 6` | true | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/col/ii->11,1/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 7` | true | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/col/ii->9,0/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ii->9,0/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 2` | true | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/col/ii->9,0/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -2147483648 is too small to store | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ii->9,0/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 2147483647 is too large to store  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ii->9,0/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->9,0/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 6` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/ii->9,0/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/si->4,0/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/si->4,0/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 2` | true | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/col/si->4,0/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -32768 is too small to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/si->4,0/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 32767 is too large to store in a  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/si->4,0/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->4,0/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 6` | true | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/col/si->4,0/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 7` | true | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/col/si->5,0/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/si->5,0/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/col/si->5,0/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | [["Dec(-32768)"]] | [["Dec(-32768)"]] | same | [["Dec(-32768)"]] | same |  |
| `kernel/col/si->5,0/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | [["Dec(32767)"]] | [["Dec(32767)"]] | same | [["Dec(32767)"]] | same |  |
| `kernel/col/si->5,0/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->5,0/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/col/si->5,0/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/col/si->5,1/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/si->5,1/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 2` | true | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/col/si->5,1/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -32768.0 is too small to store in | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/si->5,1/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 32767.0 is too large to store in  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/si->5,1/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->5,1/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 6` | true | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/col/si->5,1/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 7` | true | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/col/si->6,1/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/si->6,1/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 2` | true | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/col/si->6,1/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 3` | true | [["Dec(-32768.0)"]] | [["Dec(-32768.0)"]] | same | [["Dec(-32768.0)"]] | same |  |
| `kernel/col/si->6,1/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 4` | true | [["Dec(32767.0)"]] | [["Dec(32767.0)"]] | same | [["Dec(32767.0)"]] | same |  |
| `kernel/col/si->6,1/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->6,1/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 6` | true | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/col/si->6,1/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 7` | true | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/col/ti->2,0/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ti->2,0/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 2` | true | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/col/ti->2,0/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -128 is too small to store in a D | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ti->2,0/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 127 is too large to store in a De | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ti->2,0/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->2,0/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 6` | true | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/col/ti->2,0/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 7` | true | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/col/ti->3,0/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ti->3,0/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 2` | true | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/col/ti->3,0/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 3` | true | [["Dec(-128)"]] | [["Dec(-128)"]] | same | [["Dec(-128)"]] | same |  |
| `kernel/col/ti->3,0/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 4` | true | [["Dec(127)"]] | [["Dec(127)"]] | same | [["Dec(127)"]] | same |  |
| `kernel/col/ti->3,0/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->3,0/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 6` | true | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/col/ti->3,0/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 7` | true | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/col/ti->3,1/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ti->3,1/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 2` | true | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/col/ti->3,1/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -128.0 is too small to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ti->3,1/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 127.0 is too large to store in a  | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/col/ti->3,1/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->3,1/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 6` | true | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/col/ti->3,1/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 7` | true | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/col/ti->4,1/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ti->4,1/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 2` | true | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/col/ti->4,1/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 3` | true | [["Dec(-128.0)"]] | [["Dec(-128.0)"]] | same | [["Dec(-128.0)"]] | same |  |
| `kernel/col/ti->4,1/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 4` | true | [["Dec(127.0)"]] | [["Dec(127.0)"]] | same | [["Dec(127.0)"]] | same |  |
| `kernel/col/ti->4,1/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->4,1/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 6` | true | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/col/ti->4,1/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 7` | true | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/lit/bi->18,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(18,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->18,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(18,0))` | true | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/lit/bi->18,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(18,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->18,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(18,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->18,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(18,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->18,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(18,0))` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/bi->18,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(18,0))` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/bi->19,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->19,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/lit/bi->19,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(-9223372036854775808)"]] | [["Dec(-9223372036854775808)"]] | same | [["Dec(-9223372036854775808)"]] | same |  |
| `kernel/lit/bi->19,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(9223372036854775807)"]] | [["Dec(9223372036854775807)"]] | same | [["Dec(9223372036854775807)"]] | same |  |
| `kernel/lit/bi->19,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(19,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->19,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/bi->19,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(19,0))` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/bi->20,2/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(20,2))` | true | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/lit/bi->20,2/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(20,2))` | true | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/lit/bi->20,2/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(20,2))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->20,2/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(20,2))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->20,2/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(20,2))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->20,2/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(20,2))` | true | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/lit/bi->20,2/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(20,2))` | true | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/lit/bi->21,2/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/lit/bi->21,2/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/lit/bi->21,2/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(-9223372036854775808.00)"]] | [["Dec(-9223372036854775808.00)"]] | same | [["Dec(-9223372036854775808.00)"]] | same |  |
| `kernel/lit/bi->21,2/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(9223372036854775807.00)"]] | [["Dec(9223372036854775807.00)"]] | same | [["Dec(9223372036854775807.00)"]] | same |  |
| `kernel/lit/bi->21,2/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(21,2))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->21,2/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/lit/bi->21,2/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(21,2))` | true | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/lit/bi->5,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(5,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->5,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(5,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->5,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(5,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->5,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(5,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->5,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(5,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->5,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(5,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/bi->5,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(5,0))` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/ii->10,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(10,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ii->10,0/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(10,0))` | true | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/lit/ii->10,0/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(10,0))` | true | [["Dec(-2147483648)"]] | [["Dec(-2147483648)"]] | same | [["Dec(-2147483648)"]] | same |  |
| `kernel/lit/ii->10,0/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(10,0))` | true | [["Dec(2147483647)"]] | [["Dec(2147483647)"]] | same | [["Dec(2147483647)"]] | same |  |
| `kernel/lit/ii->10,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(10,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->10,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(10,0))` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/ii->10,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(10,0))` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/ii->10,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(10,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ii->10,1/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(10,1))` | true | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/lit/ii->10,1/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(10,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ii->10,1/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(10,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ii->10,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(10,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->10,1/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(10,1))` | true | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/lit/ii->10,1/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(10,1))` | true | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/lit/ii->11,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(11,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ii->11,1/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(11,1))` | true | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/lit/ii->11,1/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(11,1))` | true | [["Dec(-2147483648.0)"]] | [["Dec(-2147483648.0)"]] | same | [["Dec(-2147483648.0)"]] | same |  |
| `kernel/lit/ii->11,1/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(11,1))` | true | [["Dec(2147483647.0)"]] | [["Dec(2147483647.0)"]] | same | [["Dec(2147483647.0)"]] | same |  |
| `kernel/lit/ii->11,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(11,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->11,1/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(11,1))` | true | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/lit/ii->11,1/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(11,1))` | true | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/lit/ii->9,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(9,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ii->9,0/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(9,0))` | true | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/lit/ii->9,0/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(9,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ii->9,0/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(9,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ii->9,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(9,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->9,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(9,0))` | true | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/ii->9,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(9,0))` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/si->4,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(4,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/si->4,0/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(4,0))` | true | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/lit/si->4,0/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(4,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/si->4,0/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(4,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/si->4,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(4,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->4,0/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(4,0))` | true | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/lit/si->4,0/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(4,0))` | true | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/lit/si->5,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/si->5,0/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/lit/si->5,0/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(-32768)"]] | [["Dec(-32768)"]] | same | [["Dec(-32768)"]] | same |  |
| `kernel/lit/si->5,0/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(32767)"]] | [["Dec(32767)"]] | same | [["Dec(32767)"]] | same |  |
| `kernel/lit/si->5,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(5,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->5,0/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/lit/si->5,0/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(5,0))` | true | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/lit/si->5,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(5,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/si->5,1/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(5,1))` | true | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/lit/si->5,1/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(5,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/si->5,1/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(5,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/si->5,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(5,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->5,1/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(5,1))` | true | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/lit/si->5,1/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(5,1))` | true | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/lit/si->6,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/si->6,1/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/lit/si->6,1/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(-32768.0)"]] | [["Dec(-32768.0)"]] | same | [["Dec(-32768.0)"]] | same |  |
| `kernel/lit/si->6,1/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(32767.0)"]] | [["Dec(32767.0)"]] | same | [["Dec(32767.0)"]] | same |  |
| `kernel/lit/si->6,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(6,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->6,1/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/lit/si->6,1/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(6,1))` | true | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/lit/ti->2,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(2,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ti->2,0/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(2,0))` | true | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/lit/ti->2,0/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(2,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ti->2,0/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(2,0))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ti->2,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(2,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->2,0/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(2,0))` | true | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/lit/ti->2,0/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(2,0))` | true | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/lit/ti->3,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ti->3,0/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/lit/ti->3,0/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(-128)"]] | [["Dec(-128)"]] | same | [["Dec(-128)"]] | same |  |
| `kernel/lit/ti->3,0/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(127)"]] | [["Dec(127)"]] | same | [["Dec(127)"]] | same |  |
| `kernel/lit/ti->3,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(3,0))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->3,0/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/lit/ti->3,0/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(3,0))` | true | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/lit/ti->3,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(3,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ti->3,1/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(3,1))` | true | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/lit/ti->3,1/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(3,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ti->3,1/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(3,1))` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `kernel/lit/ti->3,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(3,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->3,1/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(3,1))` | true | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/lit/ti->3,1/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(3,1))` | true | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/lit/ti->4,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ti->4,1/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/lit/ti->4,1/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(-128.0)"]] | [["Dec(-128.0)"]] | same | [["Dec(-128.0)"]] | same |  |
| `kernel/lit/ti->4,1/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(127.0)"]] | [["Dec(127.0)"]] | same | [["Dec(127.0)"]] | same |  |
| `kernel/lit/ti->4,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(4,1))` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->4,1/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/lit/ti->4,1/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(4,1))` | true | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `merge/empty` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/empty#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/fit` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | true | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/fit#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | true | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/full` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `merge/full#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `neighbour/big_to_int/id1` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/big_to_int/id2` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 2` | true | [[123456]] | [[123456]] | same | [[123456]] | same |  |
| `neighbour/big_to_int/id3` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 3` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/big_to_int/id4` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 4` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/big_to_int/id5` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/big_to_int/id6` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 6` | true | [[-123456]] | [[-123456]] | same | [[-123456]] | same |  |
| `neighbour/big_to_int/id7` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 7` | true | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/big_to_int/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS INT) = 1` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/big_to_int/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS INT) < 1` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/dbl_to_dec/id1` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/dbl_to_dec/id2` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/dbl_to_dec/id3` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | refuse | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | refuse |  |
| `neighbour/dbl_to_dec/id4` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 1000000000000000000000000000000 i | refuse | ERR Arrow error: Invalid argument error: 1000000000000000000000000000000 i | refuse |  |
| `neighbour/dbl_to_dec/id5` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dbl_to_dec/id6` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/dbl_to_dec/id7` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/dbl_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(db AS DECIMAL(5,0)) = 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/dbl_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(db AS DECIMAL(5,0)) < 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/dec_to_dec/id1` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/dec_to_dec/id2` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 2` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same |  |
| `neighbour/dec_to_dec/id3` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same |  |
| `neighbour/dec_to_dec/id4` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 4` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same |  |
| `neighbour/dec_to_dec/id5` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dec_to_dec/id6` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 6` | true | [["Dec(-1.5)"]] | [["Dec(-1.5)"]] | same | [["Dec(-1.5)"]] | same |  |
| `neighbour/dec_to_dec/id7` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 7` | true | [["Dec(123.5)"]] | [["Dec(123.5)"]] | same | [["Dec(123.5)"]] | same |  |
| `neighbour/dec_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(dd AS DECIMAL(5,1)) = 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same |  |
| `neighbour/dec_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(dd AS DECIMAL(5,1)) < 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same |  |
| `neighbour/int_to_small/id1` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/int_to_small/id2` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 2` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/int_to_small/id3` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 3` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/int_to_small/id4` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 4` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/int_to_small/id5` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/int_to_small/id6` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 6` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/int_to_small/id7` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 7` | true | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/int_to_small/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS SMALLINT) = 1` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/int_to_small/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS SMALLINT) < 1` | true | ERR [CAST_OVERFLOW] | ERR [CAST_OVERFLOW] | same | ERR [CAST_OVERFLOW] | same |  |
| `neighbour/str_to_dec/id1` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/str_to_dec/id2` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/str_to_dec/id3` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | refuse | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | refuse |  |
| `neighbour/str_to_dec/id4` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | ERR [CAST_INVALID_INPUT] | ERR Arrow error: Cast error: Cannot cast string 'abc' to value of Decimal1 | refuse | ERR Arrow error: Cast error: Cannot cast string 'abc' to value of Decimal1 | refuse |  |
| `neighbour/str_to_dec/id5` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/str_to_dec/id6` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/str_to_dec/id7` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/str_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ss AS DECIMAL(5,0)) = 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/str_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ss AS DECIMAL(5,0)) < 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse |  |
| `neighbour/try_big_to_dec/id1` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_big_to_dec/id2` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id3` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id4` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id5` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id6` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id7` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_big_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS DECIMAL(5,0)) < 1` | true | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `neighbour/try_big_to_int/id1` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_int/id2` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 2` | true | [[123456]] | [[123456]] | same | [[123456]] | same |  |
| `neighbour/try_big_to_int/id3` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id4` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id5` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id6` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 6` | true | [[-123456]] | [[-123456]] | same | [[-123456]] | same |  |
| `neighbour/try_big_to_int/id7` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 7` | true | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/try_big_to_int/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS INT) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_int/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS INT) < 1` | true | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_dbl_to_dec/id1` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_dbl_to_dec/id2` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id3` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id4` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id5` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id6` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/try_dbl_to_dec/id7` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_dbl_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(db AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_dbl_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(db AS DECIMAL(5,0)) < 1` | true | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_dec_to_dec/id1` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/try_dec_to_dec/id2` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 2` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id3` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id4` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id5` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id6` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 6` | true | [["Dec(-1.5)"]] | [["Dec(-1.5)"]] | same | [["Dec(-1.5)"]] | same |  |
| `neighbour/try_dec_to_dec/id7` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 7` | true | [["Dec(123.5)"]] | [["Dec(123.5)"]] | same | [["Dec(123.5)"]] | same |  |
| `neighbour/try_dec_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(dd AS DECIMAL(5,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_dec_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(dd AS DECIMAL(5,1)) < 1` | true | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_int_to_small/id1` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_int_to_small/id2` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 2` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id3` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id4` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id5` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id6` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 6` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id7` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 7` | true | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/try_int_to_small/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ii AS SMALLINT) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_int_to_small/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ii AS SMALLINT) < 1` | true | [] | [] | same | [] | same |  |
| `neighbour/try_str_to_dec/id1` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 1` | true | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_str_to_dec/id2` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 2` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id3` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id4` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id5` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id6` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 6` | true | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/try_str_to_dec/id7` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 7` | true | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_str_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ss AS DECIMAL(5,0)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_str_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ss AS DECIMAL(5,0)) < 1` | true | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_tiny_to_dec/id1` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 1` | true | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id2` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 2` | true | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id3` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 3` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id4` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 4` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id5` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 5` | true | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id6` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 6` | true | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id7` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 7` | true | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ti AS DECIMAL(3,1)) = 1` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_tiny_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ti AS DECIMAL(3,1)) < 1` | true | [[6]] | [[3], [6]] | WRONG | [[6]] | same | WRONG->same |
| `null/lossless_notnull_source` | spark.sql | `nullability of a cast over a NOT NULL source` | true | [["Dec(1)"], ["Dec(12345)"]] | [["Dec(1)"], ["Dec(12345)"]] | same | [["Dec(1)"], ["Dec(12345)"]] | same |  |
| `null/notnull_source` | spark.sql | `nullability of a cast over a NOT NULL source` | true | [["Dec(1)"], ["Dec(12345)"]] | [["Dec(1)"], ["Dec(12345)"]] | same | [["Dec(1)"], ["Dec(12345)"]] | same |  |
| `place/t/avg` | spark.sql | `SELECT avg(CAST(bi AS DECIMAL(5,0))) AS a FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/group_by` | spark.sql | `SELECT CAST(bi AS DECIMAL(5,0)) AS g, count(*) AS c FROM t GROUP BY 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/having` | spark.sql | `SELECT id FROM t GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/having_sum` | spark.sql | `SELECT id FROM t GROUP BY id HAVING sum(CAST(bi AS DECIMAL(5,0))) > 0 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/int_digit_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS DECIMAL(9,0)) < 0` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/join_key` | spark.sql | `SELECT a.id FROM t a JOIN keys k ON CAST(a.bi AS DECIMAL(5,0)) = k.d ORDER BY a.id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/order_by` | spark.sql | `SELECT id FROM t ORDER BY CAST(bi AS DECIMAL(5,0)) NULLS FIRST, id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/projection` | spark.sql | `SELECT id, CAST(bi AS DECIMAL(5,0)) AS r FROM t ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/select_case` | spark.sql | `SELECT id, CASE WHEN CAST(bi AS DECIMAL(5,0)) > 0 THEN 'p' ELSE 'n' END AS r FROM t ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1, "p"], [2, "p"], [3, "n"], [4, "p"], [5, "n"], [6, "n"], [7, "p"]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/small_digit_in` | spark.sql | `SELECT id FROM t WHERE CAST(si AS DECIMAL(4,0)) IN (1, 123)` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [7]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/sum` | spark.sql | `SELECT sum(CAST(bi AS DECIMAL(5,0))) AS s FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/tiny_scale_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ti AS DECIMAL(3,1)) = 1` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_between` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 99999 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [7]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_case` | spark.sql | `SELECT id FROM t WHERE CASE WHEN CAST(bi AS DECIMAL(5,0)) = 1 THEN true ELSE false END ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_gt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) > 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[2], [4], [7]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_in` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 12345) ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [7]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_is_not_null` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IS NOT NULL ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/where_is_null` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IS NULL ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `place/t/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[3], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_ne` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) <> 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[2], [3], [4], [6], [7]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t/where_not_in` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) NOT IN (1, 12345) ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[2], [3], [4], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `place/t_fit/avg` | spark.sql | `SELECT avg(CAST(bi AS DECIMAL(5,0))) AS a FROM t_fit` | true | [["Dec(6173.0000)"]] | [["Dec(6173.0000)"]] | same | [["Dec(6173.0000)"]] | same |  |
| `place/t_fit/group_by` | spark.sql | `SELECT CAST(bi AS DECIMAL(5,0)) AS g, count(*) AS c FROM t_fit GROUP BY 1` | true | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | same | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | same |  |
| `place/t_fit/having` | spark.sql | `SELECT id FROM t_fit GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/having_sum` | spark.sql | `SELECT id FROM t_fit GROUP BY id HAVING sum(CAST(bi AS DECIMAL(5,0))) > 0 ORDER BY id` | true | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/join_key` | spark.sql | `SELECT a.id FROM t_fit a JOIN keys k ON CAST(a.bi AS DECIMAL(5,0)) = k.d ORDER BY a.id` | true | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/order_by` | spark.sql | `SELECT id FROM t_fit ORDER BY CAST(bi AS DECIMAL(5,0)) NULLS FIRST, id` | true | [[5], [1], [7]] | [[5], [1], [7]] | same | [[5], [1], [7]] | same |  |
| `place/t_fit/projection` | spark.sql | `SELECT id, CAST(bi AS DECIMAL(5,0)) AS r FROM t_fit ORDER BY id` | true | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `place/t_fit/select_case` | spark.sql | `SELECT id, CASE WHEN CAST(bi AS DECIMAL(5,0)) > 0 THEN 'p' ELSE 'n' END AS r FROM t_fit ORDER BY id` | true | [[1, "p"], [5, "n"], [7, "p"]] | [[1, "p"], [5, "n"], [7, "p"]] | same | [[1, "p"], [5, "n"], [7, "p"]] | same |  |
| `place/t_fit/sum` | spark.sql | `SELECT sum(CAST(bi AS DECIMAL(5,0))) AS s FROM t_fit` | true | [["Dec(12346)"]] | [["Dec(12346)"]] | same | [["Dec(12346)"]] | same |  |
| `place/t_fit/where_between` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 99999 ORDER BY id` | true | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/where_case` | spark.sql | `SELECT id FROM t_fit WHERE CASE WHEN CAST(bi AS DECIMAL(5,0)) = 1 THEN true ELSE false END ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/where_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/where_gt` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) > 1 ORDER BY id` | true | [[7]] | [[7]] | same | [[7]] | same |  |
| `place/t_fit/where_in` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 12345) ORDER BY id` | true | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/where_is_not_null` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IS NOT NULL ORDER BY id` | true | [[1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1], [7]] | same | OVER-RAISE->same |
| `place/t_fit/where_is_null` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IS NULL ORDER BY id` | true | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `place/t_fit/where_lt` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | true | [] | [] | same | [] | same |  |
| `place/t_fit/where_ne` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) <> 1 ORDER BY id` | true | [[7]] | [[7]] | same | [[7]] | same |  |
| `place/t_fit/where_not_in` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) NOT IN (1, 12345) ORDER BY id` | true | [] | [] | same | [] | same |  |
| `prune/ice_part/cast_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/ice_part/cast_in` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 2) ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [2]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/ice_part/cast_lt` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[5], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/ice_part/conj_cast_first` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/conj_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/conj_or` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `prune/ice_part/int_lossless` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(ii AS DECIMAL(10,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/int_narrow` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(ii AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/ice_part/lossless_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/plain_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE bi = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/plain_gt` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE bi > 100000 ORDER BY id` | true | [[3], [4]] | [[3], [4]] | same | [[3], [4]] | same |  |
| `prune/pq/cast_eq` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/pq/cast_in` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 2) ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [2]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/pq/cast_lt` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[5], [6]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/pq/conj_cast_first` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/conj_eq` | spark.sql on pq | `SELECT id FROM pq WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/conj_or` | spark.sql on pq | `SELECT id FROM pq WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `prune/pq/int_lossless` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(ii AS DECIMAL(10,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/int_narrow` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(ii AS DECIMAL(5,0)) = 1 ORDER BY id` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1]] | SILENT | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | SILENT->same |
| `prune/pq/lossless_eq` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/plain_eq` | spark.sql on pq | `SELECT id FROM pq WHERE bi = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/plain_gt` | spark.sql on pq | `SELECT id FROM pq WHERE bi > 100000 ORDER BY id` | true | [[3], [4]] | [[3], [4]] | same | [[3], [4]] | same |  |
| `pushdown/accepted` | spark.conf.set | `datafusion.execution.parquet.pushdown_filters = true (RePark only)` | true | [["n/a"]] | [["true"]] | WRONG | [["true"]] | WRONG |  |
| `pushdown/pq/conj_cast_first` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/conj_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/conj_or` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | true | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `pushdown/pq/lossless_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/plain_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE bi = 1 ORDER BY id` | true | [[1]] | [[1]] | same | [[1]] | same |  |
| `update/empty` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `update/empty#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `update/fit` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | true | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | OVER-RAISE | OVER-RAISE->OVER-RAISE |
| `update/fit#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | true | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | WRONG | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | WRONG |  |
| `update/full` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | refuse | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | same | refuse->same |
| `update/full#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | true | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `card/filter` | spark.sql | `WHERE CAST(b AS DECIMAL(5,0)) = 1 over (SELECT CAST(123456 AS BIGINT) AS b)` | false | [] | [] | same | [] | same |  |
| `card/projection` | spark.sql | `SELECT CAST(b AS DECIMAL(5,0)) over (SELECT CAST(123456 AS BIGINT) AS b)` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `conj/big_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(bi AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(bi AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_int/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(bi AS INT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_int/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(bi AS INT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/big_to_int/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS INT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dbl_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(db AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/dbl_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(db AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dbl_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(db AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/dec_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(dd AS DECIMAL(5,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dec_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(dd AS DECIMAL(5,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/dec_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(dd AS DECIMAL(5,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/int_to_small/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ii AS SMALLINT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/int_to_small/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ii AS SMALLINT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/int_to_small/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ii AS SMALLINT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/str_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ss AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/str_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ss AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/str_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ss AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `conj/tiny_to_dec/and_eq` | spark.sql | `SELECT id FROM t WHERE id IN (1, 5, 7) AND CAST(ti AS DECIMAL(3,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/tiny_to_dec/and_one_row` | spark.sql | `SELECT id FROM t WHERE id = 1 AND CAST(ti AS DECIMAL(3,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `conj/tiny_to_dec/view_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(ti AS DECIMAL(3,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `ctas/empty` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | false | [] | [] | same | [] | same |  |
| `ctas/empty#readback` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | false | [] | [] | same | [] | same |  |
| `ctas/fit` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `ctas/fit#readback` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `ctas/full` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `ctas/full#readback` | spark.sql write | `ctas from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | - | n/a | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same |  |
| `door/t/eq/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t/eq/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t/eq/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t/eq/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t/eq/view` | view | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t/is_null/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `door/t/is_null/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `door/t/is_null/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `door/t/is_null/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `door/t/is_null/view` | view | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `door/t/lt/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `door/t/lt/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `door/t/lt/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `door/t/lt/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `door/t/lt/view` | view | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `door/t/projection/col_cast` | col_cast | `CAST(bi AS DECIMAL(5,0)) projected over t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `door/t/projection/expr` | expr | `CAST(bi AS DECIMAL(5,0)) projected over t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `door/t/projection/select_expr` | select_expr | `CAST(bi AS DECIMAL(5,0)) projected over t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `door/t/projection/sql` | sql | `CAST(bi AS DECIMAL(5,0)) projected over t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `door/t/projection/view` | view | `CAST(bi AS DECIMAL(5,0)) projected over t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `door/t_fit/eq/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/eq/view` | view | `filter CAST(bi AS DECIMAL(5,0)) = 1 over t_fit` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `door/t_fit/is_null/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/is_null/view` | view | `filter CAST(bi AS DECIMAL(5,0)) IS NULL over t_fit` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `door/t_fit/lt/col_cast` | col_cast | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | false | [] | [] | same | [] | same |  |
| `door/t_fit/lt/expr` | expr | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | false | [] | [] | same | [] | same |  |
| `door/t_fit/lt/filter_str` | filter_str | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | false | [] | [] | same | [] | same |  |
| `door/t_fit/lt/sql` | sql | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | false | [] | [] | same | [] | same |  |
| `door/t_fit/lt/view` | view | `filter CAST(bi AS DECIMAL(5,0)) < 1 over t_fit` | false | [] | [] | same | [] | same |  |
| `door/t_fit/projection/col_cast` | col_cast | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/expr` | expr | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/select_expr` | select_expr | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/sql` | sql | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `door/t_fit/projection/view` | view | `CAST(bi AS DECIMAL(5,0)) projected over t_fit` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/empty` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | false | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `insert_into/empty#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | false | [[100, "Dec(7)"]] | [[100, "Dec(7)"]] | same | [[100, "Dec(7)"]] | same |  |
| `insert_into/fit` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | false | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/fit#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | false | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [100, "Dec(7)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_into/full` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | false | [[1, "Dec(1)"], [100, "Dec(7)"], [2, null], [3, null], [4, null], [... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, "Dec(1)"], [100, "Dec(7)"], [2, null], [3, null], [4, null], [... | same | OVER-RAISE->same |
| `insert_into/full#readback` | spark.sql write | `insert_into from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | false | [[1, "Dec(1)"], [100, "Dec(7)"], [2, null], [3, null], [4, null], [... | [[100, "Dec(7)"]] | WRONG | [[1, "Dec(1)"], [100, "Dec(7)"], [2, null], [3, null], [4, null], [... | same | WRONG->same |
| `insert_overwrite/empty` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | false | [] | ERR Error during planning: INSERT OVERWRITE empty source evaluates a CAST  | OVER-RAISE | ERR Error during planning: INSERT OVERWRITE empty source evaluates a CAST  | OVER-RAISE |  |
| `insert_overwrite/empty#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | false | [] | [[100, "Dec(7)"]] | WRONG | [[100, "Dec(7)"]] | WRONG |  |
| `insert_overwrite/fit` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_overwrite/fit#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `insert_overwrite/full` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `insert_overwrite/full#readback` | spark.sql write | `insert_overwrite from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | [[100, "Dec(7)"]] | WRONG | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | WRONG->same |
| `kernel/col/bi->18,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->18,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 2` | false | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/col/bi->18,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -9223372036854775808 is too small | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->18,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 9223372036854775807 is too large  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->18,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->18,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 6` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/bi->18,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(18,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/bi->19,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->19,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 2` | false | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/col/bi->19,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 3` | false | [["Dec(-9223372036854775808)"]] | [["Dec(-9223372036854775808)"]] | same | [["Dec(-9223372036854775808)"]] | same |  |
| `kernel/col/bi->19,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 4` | false | [["Dec(9223372036854775807)"]] | [["Dec(9223372036854775807)"]] | same | [["Dec(9223372036854775807)"]] | same |  |
| `kernel/col/bi->19,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->19,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 6` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/bi->19,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(19,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/bi->20,2/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 1` | false | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/col/bi->20,2/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 2` | false | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/col/bi->20,2/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -9223372036854775808.00 is too sm | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->20,2/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 9223372036854775807.00 is too lar | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->20,2/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->20,2/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 6` | false | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/col/bi->20,2/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(20,2)) FROM t WHERE id = 7` | false | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/col/bi->21,2/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 1` | false | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/col/bi->21,2/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 2` | false | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/col/bi->21,2/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 3` | false | [["Dec(-9223372036854775808.00)"]] | [["Dec(-9223372036854775808.00)"]] | same | [["Dec(-9223372036854775808.00)"]] | same |  |
| `kernel/col/bi->21,2/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 4` | false | [["Dec(9223372036854775807.00)"]] | [["Dec(9223372036854775807.00)"]] | same | [["Dec(9223372036854775807.00)"]] | same |  |
| `kernel/col/bi->21,2/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->21,2/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 6` | false | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/col/bi->21,2/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(21,2)) FROM t WHERE id = 7` | false | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/col/bi->5,0/id1` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/bi->5,0/id2` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->5,0/id3` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -9223372036854775808 is too small | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->5,0/id4` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 9223372036854775807 is too large  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->5,0/id5` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/bi->5,0/id6` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [[null]] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/bi->5,0/id7` | spark.sql column | `SELECT CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/ii->10,0/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ii->10,0/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 2` | false | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/col/ii->10,0/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 3` | false | [["Dec(-2147483648)"]] | [["Dec(-2147483648)"]] | same | [["Dec(-2147483648)"]] | same |  |
| `kernel/col/ii->10,0/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 4` | false | [["Dec(2147483647)"]] | [["Dec(2147483647)"]] | same | [["Dec(2147483647)"]] | same |  |
| `kernel/col/ii->10,0/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->10,0/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 6` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/ii->10,0/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/ii->10,1/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ii->10,1/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 2` | false | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/col/ii->10,1/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -2147483648.0 is too small to sto | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ii->10,1/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 2147483647.0 is too large to stor | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ii->10,1/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->10,1/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 6` | false | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/col/ii->10,1/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(10,1)) FROM t WHERE id = 7` | false | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/col/ii->11,1/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ii->11,1/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 2` | false | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/col/ii->11,1/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 3` | false | [["Dec(-2147483648.0)"]] | [["Dec(-2147483648.0)"]] | same | [["Dec(-2147483648.0)"]] | same |  |
| `kernel/col/ii->11,1/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 4` | false | [["Dec(2147483647.0)"]] | [["Dec(2147483647.0)"]] | same | [["Dec(2147483647.0)"]] | same |  |
| `kernel/col/ii->11,1/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->11,1/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 6` | false | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/col/ii->11,1/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(11,1)) FROM t WHERE id = 7` | false | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/col/ii->9,0/id1` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ii->9,0/id2` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 2` | false | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/col/ii->9,0/id3` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -2147483648 is too small to store | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ii->9,0/id4` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 2147483647 is too large to store  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ii->9,0/id5` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ii->9,0/id6` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 6` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/col/ii->9,0/id7` | spark.sql column | `SELECT CAST(ii AS DECIMAL(9,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/col/si->4,0/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/si->4,0/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 2` | false | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/col/si->4,0/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -32768 is too small to store in a | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/si->4,0/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 32767 is too large to store in a  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/si->4,0/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->4,0/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 6` | false | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/col/si->4,0/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(4,0)) FROM t WHERE id = 7` | false | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/col/si->5,0/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/si->5,0/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/col/si->5,0/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [["Dec(-32768)"]] | [["Dec(-32768)"]] | same | [["Dec(-32768)"]] | same |  |
| `kernel/col/si->5,0/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [["Dec(32767)"]] | [["Dec(32767)"]] | same | [["Dec(32767)"]] | same |  |
| `kernel/col/si->5,0/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->5,0/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/col/si->5,0/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/col/si->5,1/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/si->5,1/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 2` | false | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/col/si->5,1/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -32768.0 is too small to store in | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/si->5,1/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 32767.0 is too large to store in  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/si->5,1/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->5,1/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 6` | false | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/col/si->5,1/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(5,1)) FROM t WHERE id = 7` | false | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/col/si->6,1/id1` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/si->6,1/id2` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 2` | false | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/col/si->6,1/id3` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 3` | false | [["Dec(-32768.0)"]] | [["Dec(-32768.0)"]] | same | [["Dec(-32768.0)"]] | same |  |
| `kernel/col/si->6,1/id4` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 4` | false | [["Dec(32767.0)"]] | [["Dec(32767.0)"]] | same | [["Dec(32767.0)"]] | same |  |
| `kernel/col/si->6,1/id5` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/si->6,1/id6` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 6` | false | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/col/si->6,1/id7` | spark.sql column | `SELECT CAST(si AS DECIMAL(6,1)) FROM t WHERE id = 7` | false | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/col/ti->2,0/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ti->2,0/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 2` | false | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/col/ti->2,0/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -128 is too small to store in a D | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ti->2,0/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 127 is too large to store in a De | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ti->2,0/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->2,0/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 6` | false | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/col/ti->2,0/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(2,0)) FROM t WHERE id = 7` | false | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/col/ti->3,0/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/col/ti->3,0/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 2` | false | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/col/ti->3,0/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 3` | false | [["Dec(-128)"]] | [["Dec(-128)"]] | same | [["Dec(-128)"]] | same |  |
| `kernel/col/ti->3,0/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 4` | false | [["Dec(127)"]] | [["Dec(127)"]] | same | [["Dec(127)"]] | same |  |
| `kernel/col/ti->3,0/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->3,0/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 6` | false | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/col/ti->3,0/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,0)) FROM t WHERE id = 7` | false | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/col/ti->3,1/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ti->3,1/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 2` | false | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/col/ti->3,1/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -128.0 is too small to store in a | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ti->3,1/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 127.0 is too large to store in a  | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/col/ti->3,1/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->3,1/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 6` | false | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/col/ti->3,1/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 7` | false | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/col/ti->4,1/id1` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/col/ti->4,1/id2` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 2` | false | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/col/ti->4,1/id3` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 3` | false | [["Dec(-128.0)"]] | [["Dec(-128.0)"]] | same | [["Dec(-128.0)"]] | same |  |
| `kernel/col/ti->4,1/id4` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 4` | false | [["Dec(127.0)"]] | [["Dec(127.0)"]] | same | [["Dec(127.0)"]] | same |  |
| `kernel/col/ti->4,1/id5` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/col/ti->4,1/id6` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 6` | false | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/col/ti->4,1/id7` | spark.sql column | `SELECT CAST(ti AS DECIMAL(4,1)) FROM t WHERE id = 7` | false | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/lit/bi->18,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(18,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->18,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(18,0))` | false | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/lit/bi->18,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(18,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->18,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(18,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->18,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(18,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->18,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(18,0))` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/bi->18,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(18,0))` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/bi->19,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->19,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(123456)"]] | [["Dec(123456)"]] | same | [["Dec(123456)"]] | same |  |
| `kernel/lit/bi->19,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(-9223372036854775808)"]] | [["Dec(-9223372036854775808)"]] | same | [["Dec(-9223372036854775808)"]] | same |  |
| `kernel/lit/bi->19,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(9223372036854775807)"]] | [["Dec(9223372036854775807)"]] | same | [["Dec(9223372036854775807)"]] | same |  |
| `kernel/lit/bi->19,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(19,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->19,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/bi->19,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(19,0))` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/bi->20,2/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(20,2))` | false | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/lit/bi->20,2/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(20,2))` | false | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/lit/bi->20,2/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(20,2))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->20,2/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(20,2))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->20,2/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(20,2))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->20,2/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(20,2))` | false | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/lit/bi->20,2/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(20,2))` | false | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/lit/bi->21,2/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(1.00)"]] | [["Dec(1.00)"]] | same | [["Dec(1.00)"]] | same |  |
| `kernel/lit/bi->21,2/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(123456.00)"]] | [["Dec(123456.00)"]] | same | [["Dec(123456.00)"]] | same |  |
| `kernel/lit/bi->21,2/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(-9223372036854775808.00)"]] | [["Dec(-9223372036854775808.00)"]] | same | [["Dec(-9223372036854775808.00)"]] | same |  |
| `kernel/lit/bi->21,2/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(9223372036854775807.00)"]] | [["Dec(9223372036854775807.00)"]] | same | [["Dec(9223372036854775807.00)"]] | same |  |
| `kernel/lit/bi->21,2/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(21,2))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->21,2/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(-123456.00)"]] | [["Dec(-123456.00)"]] | same | [["Dec(-123456.00)"]] | same |  |
| `kernel/lit/bi->21,2/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(21,2))` | false | [["Dec(12345.00)"]] | [["Dec(12345.00)"]] | same | [["Dec(12345.00)"]] | same |  |
| `kernel/lit/bi->5,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS BIGINT) AS DECIMAL(5,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/bi->5,0/id2` | spark.sql literal | `SELECT CAST(CAST(123456 AS BIGINT) AS DECIMAL(5,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->5,0/id3` | spark.sql literal | `SELECT CAST(CAST(-9223372036854775808 AS BIGINT) AS DECIMAL(5,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->5,0/id4` | spark.sql literal | `SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(5,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->5,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS BIGINT) AS DECIMAL(5,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/bi->5,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS BIGINT) AS DECIMAL(5,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/bi->5,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS BIGINT) AS DECIMAL(5,0))` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/ii->10,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(10,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ii->10,0/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(10,0))` | false | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/lit/ii->10,0/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(10,0))` | false | [["Dec(-2147483648)"]] | [["Dec(-2147483648)"]] | same | [["Dec(-2147483648)"]] | same |  |
| `kernel/lit/ii->10,0/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(10,0))` | false | [["Dec(2147483647)"]] | [["Dec(2147483647)"]] | same | [["Dec(2147483647)"]] | same |  |
| `kernel/lit/ii->10,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(10,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->10,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(10,0))` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/ii->10,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(10,0))` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/ii->10,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(10,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ii->10,1/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(10,1))` | false | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/lit/ii->10,1/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(10,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ii->10,1/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(10,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ii->10,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(10,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->10,1/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(10,1))` | false | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/lit/ii->10,1/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(10,1))` | false | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/lit/ii->11,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(11,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ii->11,1/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(11,1))` | false | [["Dec(99999.0)"]] | [["Dec(99999.0)"]] | same | [["Dec(99999.0)"]] | same |  |
| `kernel/lit/ii->11,1/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(11,1))` | false | [["Dec(-2147483648.0)"]] | [["Dec(-2147483648.0)"]] | same | [["Dec(-2147483648.0)"]] | same |  |
| `kernel/lit/ii->11,1/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(11,1))` | false | [["Dec(2147483647.0)"]] | [["Dec(2147483647.0)"]] | same | [["Dec(2147483647.0)"]] | same |  |
| `kernel/lit/ii->11,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(11,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->11,1/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(11,1))` | false | [["Dec(-123456.0)"]] | [["Dec(-123456.0)"]] | same | [["Dec(-123456.0)"]] | same |  |
| `kernel/lit/ii->11,1/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(11,1))` | false | [["Dec(12345.0)"]] | [["Dec(12345.0)"]] | same | [["Dec(12345.0)"]] | same |  |
| `kernel/lit/ii->9,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS INT) AS DECIMAL(9,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ii->9,0/id2` | spark.sql literal | `SELECT CAST(CAST(99999 AS INT) AS DECIMAL(9,0))` | false | [["Dec(99999)"]] | [["Dec(99999)"]] | same | [["Dec(99999)"]] | same |  |
| `kernel/lit/ii->9,0/id3` | spark.sql literal | `SELECT CAST(CAST(-2147483648 AS INT) AS DECIMAL(9,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ii->9,0/id4` | spark.sql literal | `SELECT CAST(CAST(2147483647 AS INT) AS DECIMAL(9,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ii->9,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS INT) AS DECIMAL(9,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ii->9,0/id6` | spark.sql literal | `SELECT CAST(CAST(-123456 AS INT) AS DECIMAL(9,0))` | false | [["Dec(-123456)"]] | [["Dec(-123456)"]] | same | [["Dec(-123456)"]] | same |  |
| `kernel/lit/ii->9,0/id7` | spark.sql literal | `SELECT CAST(CAST(12345 AS INT) AS DECIMAL(9,0))` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `kernel/lit/si->4,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(4,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/si->4,0/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(4,0))` | false | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/lit/si->4,0/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(4,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/si->4,0/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(4,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/si->4,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(4,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->4,0/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(4,0))` | false | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/lit/si->4,0/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(4,0))` | false | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/lit/si->5,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/si->5,0/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(9999)"]] | [["Dec(9999)"]] | same | [["Dec(9999)"]] | same |  |
| `kernel/lit/si->5,0/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(-32768)"]] | [["Dec(-32768)"]] | same | [["Dec(-32768)"]] | same |  |
| `kernel/lit/si->5,0/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(32767)"]] | [["Dec(32767)"]] | same | [["Dec(32767)"]] | same |  |
| `kernel/lit/si->5,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(5,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->5,0/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(-1234)"]] | [["Dec(-1234)"]] | same | [["Dec(-1234)"]] | same |  |
| `kernel/lit/si->5,0/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(5,0))` | false | [["Dec(123)"]] | [["Dec(123)"]] | same | [["Dec(123)"]] | same |  |
| `kernel/lit/si->5,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(5,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/si->5,1/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(5,1))` | false | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/lit/si->5,1/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(5,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/si->5,1/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(5,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/si->5,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(5,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->5,1/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(5,1))` | false | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/lit/si->5,1/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(5,1))` | false | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/lit/si->6,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/si->6,1/id2` | spark.sql literal | `SELECT CAST(CAST(9999 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(9999.0)"]] | [["Dec(9999.0)"]] | same | [["Dec(9999.0)"]] | same |  |
| `kernel/lit/si->6,1/id3` | spark.sql literal | `SELECT CAST(CAST(-32768 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(-32768.0)"]] | [["Dec(-32768.0)"]] | same | [["Dec(-32768.0)"]] | same |  |
| `kernel/lit/si->6,1/id4` | spark.sql literal | `SELECT CAST(CAST(32767 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(32767.0)"]] | [["Dec(32767.0)"]] | same | [["Dec(32767.0)"]] | same |  |
| `kernel/lit/si->6,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS SMALLINT) AS DECIMAL(6,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/si->6,1/id6` | spark.sql literal | `SELECT CAST(CAST(-1234 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(-1234.0)"]] | [["Dec(-1234.0)"]] | same | [["Dec(-1234.0)"]] | same |  |
| `kernel/lit/si->6,1/id7` | spark.sql literal | `SELECT CAST(CAST(123 AS SMALLINT) AS DECIMAL(6,1))` | false | [["Dec(123.0)"]] | [["Dec(123.0)"]] | same | [["Dec(123.0)"]] | same |  |
| `kernel/lit/ti->2,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(2,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ti->2,0/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(2,0))` | false | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/lit/ti->2,0/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(2,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ti->2,0/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(2,0))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ti->2,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(2,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->2,0/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(2,0))` | false | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/lit/ti->2,0/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(2,0))` | false | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/lit/ti->3,0/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `kernel/lit/ti->3,0/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(99)"]] | [["Dec(99)"]] | same | [["Dec(99)"]] | same |  |
| `kernel/lit/ti->3,0/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(-128)"]] | [["Dec(-128)"]] | same | [["Dec(-128)"]] | same |  |
| `kernel/lit/ti->3,0/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(127)"]] | [["Dec(127)"]] | same | [["Dec(127)"]] | same |  |
| `kernel/lit/ti->3,0/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(3,0))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->3,0/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(-12)"]] | [["Dec(-12)"]] | same | [["Dec(-12)"]] | same |  |
| `kernel/lit/ti->3,0/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(3,0))` | false | [["Dec(12)"]] | [["Dec(12)"]] | same | [["Dec(12)"]] | same |  |
| `kernel/lit/ti->3,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(3,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ti->3,1/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(3,1))` | false | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/lit/ti->3,1/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(3,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ti->3,1/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(3,1))` | false | [[null]] | ERR datafusion engine error: Optimizer rule 'simplify_expressions' failed | OVER-RAISE | [[null]] | same | OVER-RAISE->same |
| `kernel/lit/ti->3,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(3,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->3,1/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(3,1))` | false | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/lit/ti->3,1/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(3,1))` | false | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `kernel/lit/ti->4,1/id1` | spark.sql literal | `SELECT CAST(CAST(1 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `kernel/lit/ti->4,1/id2` | spark.sql literal | `SELECT CAST(CAST(99 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `kernel/lit/ti->4,1/id3` | spark.sql literal | `SELECT CAST(CAST(-128 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(-128.0)"]] | [["Dec(-128.0)"]] | same | [["Dec(-128.0)"]] | same |  |
| `kernel/lit/ti->4,1/id4` | spark.sql literal | `SELECT CAST(CAST(127 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(127.0)"]] | [["Dec(127.0)"]] | same | [["Dec(127.0)"]] | same |  |
| `kernel/lit/ti->4,1/id5` | spark.sql literal | `SELECT CAST(CAST(NULL AS TINYINT) AS DECIMAL(4,1))` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `kernel/lit/ti->4,1/id6` | spark.sql literal | `SELECT CAST(CAST(-12 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `kernel/lit/ti->4,1/id7` | spark.sql literal | `SELECT CAST(CAST(12 AS TINYINT) AS DECIMAL(4,1))` | false | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `merge/empty` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | false | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/empty#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | false | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/fit` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | false | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/fit#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | false | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `merge/full` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | false | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | same | OVER-RAISE->same |
| `merge/full#readback` | spark.sql write | `merge from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | false | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | WRONG | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | same | WRONG->same |
| `neighbour/big_to_int/id1` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/big_to_int/id2` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 2` | false | [[123456]] | [[123456]] | same | [[123456]] | same |  |
| `neighbour/big_to_int/id3` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 3` | false | [[0]] | [[0]] | same | [[0]] | same |  |
| `neighbour/big_to_int/id4` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 4` | false | [[-1]] | [[-1]] | same | [[-1]] | same |  |
| `neighbour/big_to_int/id5` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/big_to_int/id6` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 6` | false | [[-123456]] | [[-123456]] | same | [[-123456]] | same |  |
| `neighbour/big_to_int/id7` | spark.sql | `SELECT CAST(bi AS INT) FROM t WHERE id = 7` | false | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/big_to_int/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS INT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/big_to_int/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS INT) < 1` | false | [[3], [4], [6]] | [[3], [4], [6]] | same | [[3], [4], [6]] | same |  |
| `neighbour/dbl_to_dec/id1` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/dbl_to_dec/id2` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/dbl_to_dec/id3` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | OVER-RAISE | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | OVER-RAISE |  |
| `neighbour/dbl_to_dec/id4` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Invalid argument error: 1000000000000000000000000000000 i | OVER-RAISE | ERR Arrow error: Invalid argument error: 1000000000000000000000000000000 i | OVER-RAISE |  |
| `neighbour/dbl_to_dec/id5` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dbl_to_dec/id6` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/dbl_to_dec/id7` | spark.sql | `SELECT CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/dbl_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(db AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/dbl_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(db AS DECIMAL(5,0)) < 1` | false | [[6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/dec_to_dec/id1` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/dec_to_dec/id2` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dec_to_dec/id3` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dec_to_dec/id4` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dec_to_dec/id5` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/dec_to_dec/id6` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 6` | false | [["Dec(-1.5)"]] | [["Dec(-1.5)"]] | same | [["Dec(-1.5)"]] | same |  |
| `neighbour/dec_to_dec/id7` | spark.sql | `SELECT CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 7` | false | [["Dec(123.5)"]] | [["Dec(123.5)"]] | same | [["Dec(123.5)"]] | same |  |
| `neighbour/dec_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(dd AS DECIMAL(5,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/dec_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(dd AS DECIMAL(5,1)) < 1` | false | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/int_to_small/id1` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/int_to_small/id2` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 2` | false | [[-31073]] | [[-31073]] | same | [[-31073]] | same |  |
| `neighbour/int_to_small/id3` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 3` | false | [[0]] | [[0]] | same | [[0]] | same |  |
| `neighbour/int_to_small/id4` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 4` | false | [[-1]] | [[-1]] | same | [[-1]] | same |  |
| `neighbour/int_to_small/id5` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/int_to_small/id6` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 6` | false | [[7616]] | [[7616]] | same | [[7616]] | same |  |
| `neighbour/int_to_small/id7` | spark.sql | `SELECT CAST(ii AS SMALLINT) FROM t WHERE id = 7` | false | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/int_to_small/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS SMALLINT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/int_to_small/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS SMALLINT) < 1` | false | [[2], [3], [4]] | [[2], [3], [4]] | same | [[2], [3], [4]] | same |  |
| `neighbour/str_to_dec/id1` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/str_to_dec/id2` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/str_to_dec/id3` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | OVER-RAISE | ERR Arrow error: Invalid argument error: -123456 is too small to store in  | OVER-RAISE |  |
| `neighbour/str_to_dec/id4` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | ERR Arrow error: Cast error: Cannot cast string 'abc' to value of Decimal1 | OVER-RAISE | ERR Arrow error: Cast error: Cannot cast string 'abc' to value of Decimal1 | OVER-RAISE |  |
| `neighbour/str_to_dec/id5` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/str_to_dec/id6` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/str_to_dec/id7` | spark.sql | `SELECT CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/str_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ss AS DECIMAL(5,0)) = 1` | false | [[1]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/str_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ss AS DECIMAL(5,0)) < 1` | false | [[6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE |  |
| `neighbour/try_big_to_dec/id1` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_big_to_dec/id2` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id3` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id4` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id5` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id6` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_dec/id7` | spark.sql | `SELECT TRY_CAST(bi AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_big_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS DECIMAL(5,0)) < 1` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `neighbour/try_big_to_int/id1` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_int/id2` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 2` | false | [[123456]] | [[123456]] | same | [[123456]] | same |  |
| `neighbour/try_big_to_int/id3` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id4` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id5` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_big_to_int/id6` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 6` | false | [[-123456]] | [[-123456]] | same | [[-123456]] | same |  |
| `neighbour/try_big_to_int/id7` | spark.sql | `SELECT TRY_CAST(bi AS INT) FROM t WHERE id = 7` | false | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/try_big_to_int/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS INT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_big_to_int/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(bi AS INT) < 1` | false | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_dbl_to_dec/id1` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_dbl_to_dec/id2` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id3` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id4` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id5` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dbl_to_dec/id6` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/try_dbl_to_dec/id7` | spark.sql | `SELECT TRY_CAST(db AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_dbl_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(db AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_dbl_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(db AS DECIMAL(5,0)) < 1` | false | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_dec_to_dec/id1` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/try_dec_to_dec/id2` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id3` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id4` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id5` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_dec_to_dec/id6` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 6` | false | [["Dec(-1.5)"]] | [["Dec(-1.5)"]] | same | [["Dec(-1.5)"]] | same |  |
| `neighbour/try_dec_to_dec/id7` | spark.sql | `SELECT TRY_CAST(dd AS DECIMAL(5,1)) FROM t WHERE id = 7` | false | [["Dec(123.5)"]] | [["Dec(123.5)"]] | same | [["Dec(123.5)"]] | same |  |
| `neighbour/try_dec_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(dd AS DECIMAL(5,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_dec_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(dd AS DECIMAL(5,1)) < 1` | false | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_int_to_small/id1` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_int_to_small/id2` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id3` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id4` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id5` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id6` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 6` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_int_to_small/id7` | spark.sql | `SELECT TRY_CAST(ii AS SMALLINT) FROM t WHERE id = 7` | false | [[12345]] | [[12345]] | same | [[12345]] | same |  |
| `neighbour/try_int_to_small/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ii AS SMALLINT) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_int_to_small/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ii AS SMALLINT) < 1` | false | [] | [] | same | [] | same |  |
| `neighbour/try_str_to_dec/id1` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 1` | false | [["Dec(1)"]] | [["Dec(1)"]] | same | [["Dec(1)"]] | same |  |
| `neighbour/try_str_to_dec/id2` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 2` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id3` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id4` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id5` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_str_to_dec/id6` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 6` | false | [["Dec(-2)"]] | [["Dec(-2)"]] | same | [["Dec(-2)"]] | same |  |
| `neighbour/try_str_to_dec/id7` | spark.sql | `SELECT TRY_CAST(ss AS DECIMAL(5,0)) FROM t WHERE id = 7` | false | [["Dec(12345)"]] | [["Dec(12345)"]] | same | [["Dec(12345)"]] | same |  |
| `neighbour/try_str_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ss AS DECIMAL(5,0)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_str_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ss AS DECIMAL(5,0)) < 1` | false | [[6]] | [[6]] | same | [[6]] | same |  |
| `neighbour/try_tiny_to_dec/id1` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 1` | false | [["Dec(1.0)"]] | [["Dec(1.0)"]] | same | [["Dec(1.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id2` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 2` | false | [["Dec(99.0)"]] | [["Dec(99.0)"]] | same | [["Dec(99.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id3` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 3` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id4` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 4` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id5` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 5` | false | [[null]] | [[null]] | same | [[null]] | same |  |
| `neighbour/try_tiny_to_dec/id6` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 6` | false | [["Dec(-12.0)"]] | [["Dec(-12.0)"]] | same | [["Dec(-12.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/id7` | spark.sql | `SELECT TRY_CAST(ti AS DECIMAL(3,1)) FROM t WHERE id = 7` | false | [["Dec(12.0)"]] | [["Dec(12.0)"]] | same | [["Dec(12.0)"]] | same |  |
| `neighbour/try_tiny_to_dec/where_eq` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ti AS DECIMAL(3,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `neighbour/try_tiny_to_dec/where_lt` | spark.sql | `SELECT id FROM t WHERE TRY_CAST(ti AS DECIMAL(3,1)) < 1` | false | [[6]] | [[3], [6]] | WRONG | [[6]] | same | WRONG->same |
| `null/lossless_notnull_source` | spark.sql | `nullability of a cast over a NOT NULL source` | false | [["Dec(1)"], ["Dec(12345)"]] | [["Dec(1)"], ["Dec(12345)"]] | same | [["Dec(1)"], ["Dec(12345)"]] | same |  |
| `null/notnull_source` | spark.sql | `nullability of a cast over a NOT NULL source` | false | [["Dec(1)"], ["Dec(12345)"]] | [["Dec(1)"], ["Dec(12345)"]] | same | [["Dec(1)"], ["Dec(12345)"]] | same |  |
| `place/t/avg` | spark.sql | `SELECT avg(CAST(bi AS DECIMAL(5,0))) AS a FROM t` | false | [["Dec(6173.0000)"]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [["Dec(6173.0000)"]] | same | OVER-RAISE->same |
| `place/t/group_by` | spark.sql | `SELECT CAST(bi AS DECIMAL(5,0)) AS g, count(*) AS c FROM t GROUP BY 1` | false | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 5]] | same | OVER-RAISE->same |
| `place/t/having` | spark.sql | `SELECT id FROM t GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t/having_sum` | spark.sql | `SELECT id FROM t GROUP BY id HAVING sum(CAST(bi AS DECIMAL(5,0))) > 0 ORDER BY id` | false | [[1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1], [7]] | same | OVER-RAISE->same |
| `place/t/int_digit_lt` | spark.sql | `SELECT id FROM t WHERE CAST(ii AS DECIMAL(9,0)) < 0` | false | [[6]] | [[3], [6]] | WRONG | [[6]] | same | WRONG->same |
| `place/t/join_key` | spark.sql | `SELECT a.id FROM t a JOIN keys k ON CAST(a.bi AS DECIMAL(5,0)) = k.d ORDER BY a.id` | false | [[1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1], [7]] | same | OVER-RAISE->same |
| `place/t/order_by` | spark.sql | `SELECT id FROM t ORDER BY CAST(bi AS DECIMAL(5,0)) NULLS FIRST, id` | false | [[2], [3], [4], [5], [6], [1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6], [1], [7]] | same | OVER-RAISE->same |
| `place/t/projection` | spark.sql | `SELECT id, CAST(bi AS DECIMAL(5,0)) AS r FROM t ORDER BY id` | false | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1, "Dec(1)"], [2, null], [3, null], [4, null], [5, null], [6, nul... | same | OVER-RAISE->same |
| `place/t/select_case` | spark.sql | `SELECT id, CASE WHEN CAST(bi AS DECIMAL(5,0)) > 0 THEN 'p' ELSE 'n' END AS r FROM t ORDER BY id` | false | [[1, "p"], [2, "n"], [3, "n"], [4, "n"], [5, "n"], [6, "n"], [7, "p"]] | [[1, "p"], [2, "p"], [3, "n"], [4, "p"], [5, "n"], [6, "n"], [7, "p"]] | WRONG | [[1, "p"], [2, "n"], [3, "n"], [4, "n"], [5, "n"], [6, "n"], [7, "p"]] | same | WRONG->same |
| `place/t/small_digit_in` | spark.sql | `SELECT id FROM t WHERE CAST(si AS DECIMAL(4,0)) IN (1, 123)` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t/sum` | spark.sql | `SELECT sum(CAST(bi AS DECIMAL(5,0))) AS s FROM t` | false | [["Dec(12346)"]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [["Dec(12346)"]] | same | OVER-RAISE->same |
| `place/t/tiny_scale_eq` | spark.sql | `SELECT id FROM t WHERE CAST(ti AS DECIMAL(3,1)) = 1` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t/where_between` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 99999 ORDER BY id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t/where_case` | spark.sql | `SELECT id FROM t WHERE CASE WHEN CAST(bi AS DECIMAL(5,0)) = 1 THEN true ELSE false END ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t/where_eq` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t/where_gt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) > 1 ORDER BY id` | false | [[7]] | [[2], [4], [7]] | WRONG | [[7]] | same | WRONG->same |
| `place/t/where_in` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 12345) ORDER BY id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t/where_is_not_null` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IS NOT NULL ORDER BY id` | false | [[1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1], [7]] | same | OVER-RAISE->same |
| `place/t/where_is_null` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) IS NULL ORDER BY id` | false | [[2], [3], [4], [5], [6]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[2], [3], [4], [5], [6]] | same | OVER-RAISE->same |
| `place/t/where_lt` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | false | [] | [[3], [6]] | WRONG | [] | same | WRONG->same |
| `place/t/where_ne` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) <> 1 ORDER BY id` | false | [[7]] | [[2], [3], [4], [6], [7]] | WRONG | [[7]] | same | WRONG->same |
| `place/t/where_not_in` | spark.sql | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) NOT IN (1, 12345) ORDER BY id` | false | [] | [[2], [3], [4], [6]] | WRONG | [] | same | WRONG->same |
| `place/t_fit/avg` | spark.sql | `SELECT avg(CAST(bi AS DECIMAL(5,0))) AS a FROM t_fit` | false | [["Dec(6173.0000)"]] | [["Dec(6173.0000)"]] | same | [["Dec(6173.0000)"]] | same |  |
| `place/t_fit/group_by` | spark.sql | `SELECT CAST(bi AS DECIMAL(5,0)) AS g, count(*) AS c FROM t_fit GROUP BY 1` | false | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | same | [["Dec(1)", 1], ["Dec(12345)", 1], [null, 1]] | same |  |
| `place/t_fit/having` | spark.sql | `SELECT id FROM t_fit GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/having_sum` | spark.sql | `SELECT id FROM t_fit GROUP BY id HAVING sum(CAST(bi AS DECIMAL(5,0))) > 0 ORDER BY id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/join_key` | spark.sql | `SELECT a.id FROM t_fit a JOIN keys k ON CAST(a.bi AS DECIMAL(5,0)) = k.d ORDER BY a.id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/order_by` | spark.sql | `SELECT id FROM t_fit ORDER BY CAST(bi AS DECIMAL(5,0)) NULLS FIRST, id` | false | [[5], [1], [7]] | [[5], [1], [7]] | same | [[5], [1], [7]] | same |  |
| `place/t_fit/projection` | spark.sql | `SELECT id, CAST(bi AS DECIMAL(5,0)) AS r FROM t_fit ORDER BY id` | false | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same | [[1, "Dec(1)"], [5, null], [7, "Dec(12345)"]] | same |  |
| `place/t_fit/select_case` | spark.sql | `SELECT id, CASE WHEN CAST(bi AS DECIMAL(5,0)) > 0 THEN 'p' ELSE 'n' END AS r FROM t_fit ORDER BY id` | false | [[1, "p"], [5, "n"], [7, "p"]] | [[1, "p"], [5, "n"], [7, "p"]] | same | [[1, "p"], [5, "n"], [7, "p"]] | same |  |
| `place/t_fit/sum` | spark.sql | `SELECT sum(CAST(bi AS DECIMAL(5,0))) AS s FROM t_fit` | false | [["Dec(12346)"]] | [["Dec(12346)"]] | same | [["Dec(12346)"]] | same |  |
| `place/t_fit/where_between` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 99999 ORDER BY id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/where_case` | spark.sql | `SELECT id FROM t_fit WHERE CASE WHEN CAST(bi AS DECIMAL(5,0)) = 1 THEN true ELSE false END ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/where_eq` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `place/t_fit/where_gt` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) > 1 ORDER BY id` | false | [[7]] | [[7]] | same | [[7]] | same |  |
| `place/t_fit/where_in` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 12345) ORDER BY id` | false | [[1], [7]] | [[1], [7]] | same | [[1], [7]] | same |  |
| `place/t_fit/where_is_not_null` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IS NOT NULL ORDER BY id` | false | [[1], [7]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[1], [7]] | same | OVER-RAISE->same |
| `place/t_fit/where_is_null` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) IS NULL ORDER BY id` | false | [[5]] | ERR Arrow error: Invalid argument error: 123456 is too large to store in a | OVER-RAISE | [[5]] | same | OVER-RAISE->same |
| `place/t_fit/where_lt` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | false | [] | [] | same | [] | same |  |
| `place/t_fit/where_ne` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) <> 1 ORDER BY id` | false | [[7]] | [[7]] | same | [[7]] | same |  |
| `place/t_fit/where_not_in` | spark.sql | `SELECT id FROM t_fit WHERE CAST(bi AS DECIMAL(5,0)) NOT IN (1, 12345) ORDER BY id` | false | [] | [] | same | [] | same |  |
| `prune/ice_part/cast_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/cast_in` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 2) ORDER BY id` | false | [[1], [2]] | [[1], [2]] | same | [[1], [2]] | same |  |
| `prune/ice_part/cast_lt` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | false | [] | [[5], [6]] | WRONG | [] | same | WRONG->same |
| `prune/ice_part/conj_cast_first` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/conj_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/conj_or` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `prune/ice_part/int_lossless` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(ii AS DECIMAL(10,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/int_narrow` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(ii AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/lossless_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/plain_eq` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE bi = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/ice_part/plain_gt` | spark.sql on ice_part | `SELECT id FROM ice_part WHERE bi > 100000 ORDER BY id` | false | [[3], [4]] | [[3], [4]] | same | [[3], [4]] | same |  |
| `prune/pq/cast_eq` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/cast_in` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) IN (1, 2) ORDER BY id` | false | [[1], [2]] | [[1], [2]] | same | [[1], [2]] | same |  |
| `prune/pq/cast_lt` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) < 1 ORDER BY id` | false | [] | [[5], [6]] | WRONG | [] | same | WRONG->same |
| `prune/pq/conj_cast_first` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/conj_eq` | spark.sql on pq | `SELECT id FROM pq WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/conj_or` | spark.sql on pq | `SELECT id FROM pq WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `prune/pq/int_lossless` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(ii AS DECIMAL(10,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/int_narrow` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(ii AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/lossless_eq` | spark.sql on pq | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/plain_eq` | spark.sql on pq | `SELECT id FROM pq WHERE bi = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `prune/pq/plain_gt` | spark.sql on pq | `SELECT id FROM pq WHERE bi > 100000 ORDER BY id` | false | [[3], [4]] | [[3], [4]] | same | [[3], [4]] | same |  |
| `pushdown/accepted` | spark.conf.set | `datafusion.execution.parquet.pushdown_filters = true (RePark only)` | false | [["n/a"]] | [["true"]] | WRONG | [["true"]] | WRONG |  |
| `pushdown/pq/conj_cast_first` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 2, 7, 8) ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/conj_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE id IN (1, 2, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/conj_or` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE id NOT IN (1, 2, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 ORDER BY id` | false | [[1], [3], [4], [5], [6]] | [[1], [3], [4], [5], [6]] | same | [[1], [3], [4], [5], [6]] | same |  |
| `pushdown/pq/lossless_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE CAST(bi AS DECIMAL(20,0)) = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `pushdown/pq/plain_eq` | spark.sql on pq, pushdown_filters on | `SELECT id FROM pq WHERE bi = 1 ORDER BY id` | false | [[1]] | [[1]] | same | [[1]] | same |  |
| `update/empty` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0` | false | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `update/empty#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id < 0 (Spark readback)` | false | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same |  |
| `update/fit` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7)` | false | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | OVER-RAISE->same |
| `update/fit#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t WHERE id IN (1, 5, 7) (Spark readback)` | false | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | WRONG | [[1, 1, "Dec(1)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | same | WRONG->same |
| `update/full` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t` | false | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | ERR datafusion engine error: Arrow error: Invalid argument error: 123456 i | OVER-RAISE | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | same | OVER-RAISE->same |
| `update/full#readback` | spark.sql write | `update from SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t (Spark readback)` | false | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | [[1, 1, "Dec(7)"], [2, 123456, "Dec(7)"], [3, -9223372036854775808,... | WRONG | [[1, 1, "Dec(1)"], [2, 123456, null], [3, -9223372036854775808, nul... | same | WRONG->same |


### Totals

- false/head/OVER-RAISE: 15
- false/head/WRONG: 2
- false/head/same: 493
- false/main/OVER-RAISE: 88
- false/main/WRONG: 22
- false/main/n/a: 1
- false/main/same: 399
- true/head/OVER-RAISE: 12
- true/head/WRONG: 3
- true/head/refuse: 10
- true/head/same: 484
- true/main/OVER-RAISE: 18
- true/main/SILENT: 34
- true/main/WRONG: 5
- true/main/refuse: 76
- true/main/same: 376


## The conjunction and `OR` shapes (114, ANSI on)

From the Fable 5.2 consult of 2026-11-25 (`c*`, `d*`, `g*` cells over an 8-row in-memory frame `t` and the same rows
as a Parquet file `tp`), re-measured on the Ruling 1 head. Spark and head are this run; main is the consult's run of
the same package.

| cell | statement | Spark | main | main vs Spark | head | head vs Spark |
|---|---|---|---|---|---|---|
| `t/c01_and_eq` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c02_cast_first` | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c03_null_left` | `SELECT id FROM t WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `t/c03b_null_cmp` | `SELECT id FROM t WHERE g > 0 AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c03c_null_expr` | `SELECT id FROM t WHERE (nb OR id = 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c04_middle` | `SELECT id FROM t WHERE id > 0 AND CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c05_two_before` | `SELECT id FROM t WHERE id > 0 AND id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c06_or_in_a` | `SELECT id FROM t WHERE (id = 1 OR id IN (5, 7, 8)) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c07_or_right` | `SELECT id FROM t WHERE id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `t/c08_not_or` | `SELECT id FROM t WHERE NOT (id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) <> 1)` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c09_proj_and` | `SELECT id, id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM t` | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same |
| `t/c09b_proj_or` | `SELECT id, id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM t` | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | same | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | same |
| `t/c10_case_bool` | `SELECT id FROM t WHERE CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) = 1 ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c11_guard_inner` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c12_guard_outer` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN (id IN (1, 5, 7, 8)) IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) = 1 ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c13_view` | `SELECT id FROM (SELECT * FROM t WHERE id IN (1, 5, 7, 8)) WHERE CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c14_view_rev` | `SELECT id FROM (SELECT * FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1) WHERE id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c15_join_where` | `SELECT a.id FROM t a JOIN u ON a.id = u.id WHERE u.k = 1 AND CAST(a.bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c16_join_on` | `SELECT a.id FROM t a JOIN u ON a.id = u.id AND a.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c17_if_proj` | `SELECT id, IF(id IN (1, 5, 7, 8), CAST(bi AS DECIMAL(5,0)), NULL) AS r FROM t` | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | same | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | same |
| `t/c18_having` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c19_view_proj` | `SELECT id FROM (SELECT id, bi, id + 0 AS j FROM t WHERE id IN (1, 5, 7, 8)) WHERE CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c20_alias` | `SELECT id FROM (SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM t) WHERE id IN (1, 5, 7, 8) AND d = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c24_nested_or` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND (CAST(bi AS DECIMAL(5,0)) = 1 OR id = 5)` | 1,5,8 | 1,5,8 | same | 1,5,8 | same |
| `t/c25_two_raising` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 AND CAST(bi AS DECIMAL(3,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c26_in` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) IN (1, 12345)` | 1,7,8 | 1,7,8 | same | 1,7,8 | same |
| `t/c27_between` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 5` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c28_isnull` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) IS NULL` | 5 | ERR Arrow | OVER-RAISE | 5 | same |
| `t/c29_bare` | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c30_guard_inner_first` | `SELECT id FROM t WHERE CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 AND id IN (1, 5, 7, 8)` | 1,8 | 1,8 | same | 1,8 | same |
| `t/c31_coalesce` | `SELECT id FROM t WHERE COALESCE(id NOT IN (1, 5, 7, 8), false) OR CAST(bi AS DECIMAL(5,0)) = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `t/c32_join_other` | `SELECT a.id FROM t a JOIN u ON a.id = u.id AND (u.k = 1 AND CAST(a.bi AS DECIMAL(5,0)) = u.k)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR Arrow | refuse | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/c33_semi` | `SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE k = 1) AND CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c01_and_eq` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c02_cast_first` | `SELECT id FROM tp WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c03_null_left` | `SELECT id FROM tp WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `tp/c03b_null_cmp` | `SELECT id FROM tp WHERE g > 0 AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c03c_null_expr` | `SELECT id FROM tp WHERE (nb OR id = 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c04_middle` | `SELECT id FROM tp WHERE id > 0 AND CAST(bi AS DECIMAL(5,0)) = 1 AND id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c05_two_before` | `SELECT id FROM tp WHERE id > 0 AND id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c06_or_in_a` | `SELECT id FROM tp WHERE (id = 1 OR id IN (5, 7, 8)) AND CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c07_or_right` | `SELECT id FROM tp WHERE id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `tp/c08_not_or` | `SELECT id FROM tp WHERE NOT (id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) <> 1)` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c09_proj_and` | `SELECT id, id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM tp` | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same |
| `tp/c09b_proj_or` | `SELECT id, id NOT IN (1, 5, 7, 8) OR CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM tp` | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | same | 1/True,2/True,3/True,4/True,5/N,6/True,7/False,8/True | same |
| `tp/c10_case_bool` | `SELECT id FROM tp WHERE CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) = 1 ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c11_guard_inner` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c12_guard_outer` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN (id IN (1, 5, 7, 8)) IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) = 1 ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c13_view` | `SELECT id FROM (SELECT * FROM tp WHERE id IN (1, 5, 7, 8)) WHERE CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c14_view_rev` | `SELECT id FROM (SELECT * FROM tp WHERE CAST(bi AS DECIMAL(5,0)) = 1) WHERE id IN (1, 5, 7, 8)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c15_join_where` | `SELECT a.id FROM tp a JOIN u ON a.id = u.id WHERE u.k = 1 AND CAST(a.bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c16_join_on` | `SELECT a.id FROM tp a JOIN u ON a.id = u.id AND a.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c17_if_proj` | `SELECT id, IF(id IN (1, 5, 7, 8), CAST(bi AS DECIMAL(5,0)), NULL) AS r FROM tp` | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | same | 1/1,2/N,3/N,4/N,5/N,6/N,7/12345,8/1 | same |
| `tp/c18_having` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) GROUP BY id, bi HAVING CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c19_view_proj` | `SELECT id FROM (SELECT id, bi, id + 0 AS j FROM tp WHERE id IN (1, 5, 7, 8)) WHERE CAST(bi AS DECIMAL(5,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c20_alias` | `SELECT id FROM (SELECT id, CAST(bi AS DECIMAL(5,0)) AS d FROM tp) WHERE id IN (1, 5, 7, 8) AND d = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c24_nested_or` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND (CAST(bi AS DECIMAL(5,0)) = 1 OR id = 5)` | 1,5,8 | 1,5,8 | same | 1,5,8 | same |
| `tp/c25_two_raising` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 AND CAST(bi AS DECIMAL(3,0)) = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c26_in` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) IN (1, 12345)` | 1,7,8 | 1,7,8 | same | 1,7,8 | same |
| `tp/c27_between` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) BETWEEN 0 AND 5` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c28_isnull` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) IS NULL` | 5 | ERR Arrow | OVER-RAISE | 5 | same |
| `tp/c29_bare` | `SELECT id FROM tp WHERE CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c30_guard_inner_first` | `SELECT id FROM tp WHERE CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 AND id IN (1, 5, 7, 8)` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/c31_coalesce` | `SELECT id FROM tp WHERE COALESCE(id NOT IN (1, 5, 7, 8), false) OR CAST(bi AS DECIMAL(5,0)) = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `tp/c32_join_other` | `SELECT a.id FROM tp a JOIN u ON a.id = u.id AND (u.k = 1 AND CAST(a.bi AS DECIMAL(5,0)) = u.k)` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR Arrow | refuse | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/c33_semi` | `SELECT id FROM tp WHERE id IN (SELECT id FROM u WHERE k = 1) AND CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d01_proj_null_and` | `SELECT id, nb AND CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM t` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/False | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d02_proj_null_or` | `SELECT id, nb OR CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM t` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1/True,2/N,3/N,4/N,5/True,6/N,7/True,8/True | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d03_nested_null_and` | `SELECT id FROM t WHERE (nb AND CAST(bi AS DECIMAL(5,0)) = 1) OR id = 99` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d04_cast_first_nullcmp` | `SELECT id FROM t WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND g > 0` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d05_join_inferred` | `SELECT a.id FROM t a JOIN u ON a.id = u.id WHERE u.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d06_values_null_left` | `SELECT id FROM VALUES (1, 1L, true), (2, 123456L, CAST(NULL AS BOOLEAN)) AS v(id, bi, nb) WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/d06b_values_false_left` | `SELECT id FROM VALUES (1, 1L, true), (2, 123456L, false) AS v(id, bi, nb) WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `t/d09_agg_filter` | `SELECT count(*) FILTER (WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1) AS n FROM t` | 2 | 2 | same | 2 | same |
| `t/d09b_sum_case` | `SELECT sum(CASE WHEN id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 THEN 1 ELSE 0 END) AS n FROM t` | 2 | 2 | same | 2 | same |
| `t/d11_if_bool` | `SELECT id FROM t WHERE IF(id IN (1, 5, 7, 8), CAST(bi AS DECIMAL(5,0)) = 1, false)` | 1,8 | 1,8 | same | 1,8 | same |
| `t/d12_case_else` | `SELECT id FROM t WHERE CASE WHEN id NOT IN (1, 5, 7, 8) THEN id = 2 ELSE CAST(bi AS DECIMAL(5,0)) = 1 END` | 1,2,8 | 1,2,8 | same | 1,2,8 | same |
| `t/d13_null_left_ws` | `SELECT id FROM t WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `t/d14_join_filter` | `SELECT a.id FROM t a JOIN u ON a.id = u.id AND (u.k + a.id > 100 OR (a.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = u.k))` | 1,8 | ERR Arrow | OVER-RAISE | 1,8 | same |
| `t/g01_or` | `SELECT id FROM t WHERE id NOT IN (1, 5, 7, 8) OR CASE WHEN (id NOT IN (1, 5, 7, 8)) IS NOT TRUE THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `t/g02_proj_and` | `SELECT id, id IN (1, 5, 7, 8) AND CASE WHEN (id IN (1, 5, 7, 8)) IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) END = 1 AS r FROM t` | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same |
| `t/g03_two_raising` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 AND CASE WHEN id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 THEN CAST(bi AS DECIMAL(3,0)) END = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `t/g05_in` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END IN (1, 12345)` | 1,7,8 | 1,7,8 | same | 1,7,8 | same |
| `t/g06_between` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END BETWEEN 0 AND 5` | 1,8 | 1,8 | same | 1,8 | same |
| `t/g07_null_left` | `SELECT id FROM t WHERE nb AND CASE WHEN nb THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1 | 1 | same | 1 | same |
| `t/g08_isnull` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END IS NULL` | 5 | 5 | same | 5 | same |
| `t/g09_proj_null_and` | `SELECT id, nb AND CASE WHEN nb IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) END = 1 AS r FROM t` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR Arrow | refuse | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `t/g10_case_where` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 THEN true ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `t/g11_ne` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END <> 1` | 7 | 7 | same | 7 | same |
| `t/g12_lit_left` | `SELECT id FROM t WHERE id IN (1, 5, 7, 8) AND 1 = CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/d01_proj_null_and` | `SELECT id, nb AND CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM tp` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/False | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d02_proj_null_or` | `SELECT id, nb OR CAST(bi AS DECIMAL(5,0)) = 1 AS r FROM tp` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1/True,2/N,3/N,4/N,5/True,6/N,7/True,8/True | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d03_nested_null_and` | `SELECT id FROM tp WHERE (nb AND CAST(bi AS DECIMAL(5,0)) = 1) OR id = 99` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d04_cast_first_nullcmp` | `SELECT id FROM tp WHERE CAST(bi AS DECIMAL(5,0)) = 1 AND g > 0` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d05_join_inferred` | `SELECT a.id FROM tp a JOIN u ON a.id = u.id WHERE u.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1,8 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d06_values_null_left` | `SELECT id FROM VALUES (1, 1L, true), (2, 123456L, CAST(NULL AS BOOLEAN)) AS v(id, bi, nb) WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | ERR NUMERIC_VALUE_OUT_OF_RANGE | 1 | SILENT | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/d06b_values_false_left` | `SELECT id FROM VALUES (1, 1L, true), (2, 123456L, false) AS v(id, bi, nb) WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `tp/d09_agg_filter` | `SELECT count(*) FILTER (WHERE id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1) AS n FROM tp` | 2 | 2 | same | 2 | same |
| `tp/d09b_sum_case` | `SELECT sum(CASE WHEN id IN (1, 5, 7, 8) AND CAST(bi AS DECIMAL(5,0)) = 1 THEN 1 ELSE 0 END) AS n FROM tp` | 2 | 2 | same | 2 | same |
| `tp/d11_if_bool` | `SELECT id FROM tp WHERE IF(id IN (1, 5, 7, 8), CAST(bi AS DECIMAL(5,0)) = 1, false)` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/d12_case_else` | `SELECT id FROM tp WHERE CASE WHEN id NOT IN (1, 5, 7, 8) THEN id = 2 ELSE CAST(bi AS DECIMAL(5,0)) = 1 END` | 1,2,8 | 1,2,8 | same | 1,2,8 | same |
| `tp/d13_null_left_ws` | `SELECT id FROM tp WHERE nb AND CAST(bi AS DECIMAL(5,0)) = 1` | 1 | 1 | same | 1 | same |
| `tp/d14_join_filter` | `SELECT a.id FROM tp a JOIN u ON a.id = u.id AND (u.k + a.id > 100 OR (a.id IN (1, 5, 7, 8) AND CAST(a.bi AS DECIMAL(5,0)) = u.k))` | 1,8 | ERR Arrow | OVER-RAISE | 1,8 | same |
| `tp/g01_or` | `SELECT id FROM tp WHERE id NOT IN (1, 5, 7, 8) OR CASE WHEN (id NOT IN (1, 5, 7, 8)) IS NOT TRUE THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1,2,3,4,6,8 | 1,2,3,4,6,8 | same | 1,2,3,4,6,8 | same |
| `tp/g02_proj_and` | `SELECT id, id IN (1, 5, 7, 8) AND CASE WHEN (id IN (1, 5, 7, 8)) IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) END = 1 AS r FROM tp` | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same | 1/True,2/False,3/False,4/False,5/N,6/False,7/False,8/True | same |
| `tp/g03_two_raising` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 AND CASE WHEN id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 THEN CAST(bi AS DECIMAL(3,0)) END = 1` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/g05_in` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END IN (1, 12345)` | 1,7,8 | 1,7,8 | same | 1,7,8 | same |
| `tp/g06_between` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END BETWEEN 0 AND 5` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/g07_null_left` | `SELECT id FROM tp WHERE nb AND CASE WHEN nb THEN CAST(bi AS DECIMAL(5,0)) END = 1` | 1 | 1 | same | 1 | same |
| `tp/g08_isnull` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END IS NULL` | 5 | 5 | same | 5 | same |
| `tp/g09_proj_null_and` | `SELECT id, nb AND CASE WHEN nb IS NOT FALSE THEN CAST(bi AS DECIMAL(5,0)) END = 1 AS r FROM tp` | ERR NUMERIC_VALUE_OUT_OF_RANGE | ERR Arrow | refuse | ERR NUMERIC_VALUE_OUT_OF_RANGE | same |
| `tp/g10_case_where` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END = 1 THEN true ELSE false END` | 1,8 | 1,8 | same | 1,8 | same |
| `tp/g11_ne` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END <> 1` | 7 | 7 | same | 7 | same |
| `tp/g12_lit_left` | `SELECT id FROM tp WHERE id IN (1, 5, 7, 8) AND 1 = CASE WHEN id IN (1, 5, 7, 8) THEN CAST(bi AS DECIMAL(5,0)) END` | 1,8 | 1,8 | same | 1,8 | same |

## Delta 1 (2026-11-25): the guard reworked to copy nothing

Re-measured on the delta-1 head package (`BUILT_FROM 9b83dbaf`; the next commit, `b9c21b17`, changes only the plan shape: one flat lazy `CASE` per predicate, and non-materialized gates).

- **Step-0 table:** identical totals to the first build. ANSI on, 484 of 509 equal Spark (main 376); ANSI off, 493 of 510 (main 399). No cell that equals Spark on main moves.
- **The consult's 114 shapes:** 110 equal Spark (main 82); none moved. Four answer as main where Spark raises: `{t,tp}/d03_nested_null_and` and `{t,tp}/d06_values_null_left`.
- **The verifier's battery** (`vprobe.py`, 59 shapes on 3 sources):
  - ANSI on: 163 of 177 equal Spark (main 149), 0 moved;
  - ANSI off: 170 of 177 (main 155), 0 moved;
  - with `pushdown_filters` on, 0 moved;
  - writes read back with Spark: 15 of 17 (ANSI on) and 18 of 18 (ANSI off), 0 moved.
- **Native door:** byte-identical to main.

### Guard shapes (recorded from Spark 4.1.2; `guard_statements()` in the recorder)

| cell | ANSI | Spark | main | head |
|---|---|---|---|---|
| `chain30_agg_filter` | true | [[2]] | [[2]] | [[2]] |
| `chain30_filter` | true | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `chain30_join_on` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [4]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] |
| `chain30_or_filter` | true | [[2], [3], [5]] | [[2], [3], [5]] | [[2], [3], [5]] |
| `chain30_projection` | true | [[1, true], [2, false], [3, false], [4, true], [5, false]] | [[1, true], [2, false], [3, false], [4, true], [5, false]] | [[1, true], [2, false], [3, false], [4, true], [5, false]] |
| `nested14_filter` | true | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `null_left_agg_filter` | true | [[2]] | [[2]] | [[2]] |
| `null_left_filter` | true | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `null_left_or_filter` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1], [2], [4], [5]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] |
| `null_left_or_projection` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1, true], [2, true], [3, null], [4, true], [5, true]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] |
| `null_left_projection` | true | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] | [[1, true], [2, false], [3, null], [4, true], [5, null]] | ERR [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] |
| `volatile_rand_agg_filter` | true | ERR [INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC] | [[0]] | [[0]] |
| `volatile_rand_filter` | true | [] | [] | [] |
| `volatile_rand_projection` | true | [[1, false], [2, false], [3, false], [4, false], [5, false]] | [[1, false], [2, false], [3, false], [4, false], [5, false]] | [[1, false], [2, false], [3, false], [4, false], [5, false]] |
| `volatile_uuid_filter` | true | [] | [] | [] |
| `chain30_agg_filter` | false | [[2]] | [[2]] | [[2]] |
| `chain30_filter` | false | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `chain30_join_on` | false | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `chain30_or_filter` | false | [[2], [3], [5]] | [[2], [3], [5]] | [[2], [3], [5]] |
| `chain30_projection` | false | [[1, true], [2, false], [3, false], [4, true], [5, false]] | [[1, true], [2, false], [3, false], [4, true], [5, false]] | [[1, true], [2, false], [3, false], [4, true], [5, false]] |
| `nested14_filter` | false | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `null_left_agg_filter` | false | [[2]] | [[2]] | [[2]] |
| `null_left_filter` | false | [[1], [4]] | [[1], [4]] | [[1], [4]] |
| `null_left_or_filter` | false | [[1], [2], [4]] | [[1], [2], [4], [5]] | [[1], [2], [4]] |
| `null_left_or_projection` | false | [[1, true], [2, true], [3, null], [4, true], [5, null]] | [[1, true], [2, true], [3, null], [4, true], [5, true]] | [[1, true], [2, true], [3, null], [4, true], [5, null]] |
| `null_left_projection` | false | [[1, true], [2, false], [3, null], [4, true], [5, null]] | [[1, true], [2, false], [3, null], [4, true], [5, null]] | [[1, true], [2, false], [3, null], [4, true], [5, null]] |
| `volatile_rand_agg_filter` | false | ERR [INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC] | [[0]] | [[0]] |
| `volatile_rand_filter` | false | [] | [] | [] |
| `volatile_rand_projection` | false | [[1, false], [2, false], [3, false], [4, false], [5, false]] | [[1, false], [2, false], [3, false], [4, false], [5, false]] | [[1, false], [2, false], [3, false], [4, false], [5, false]] |
| `volatile_uuid_filter` | false | [] | [] | [] |

### Planning and run time (package `b9c21b17`, the verifier's blow-up shapes; script `timing.py` in the session scratch)

| shape | casts | ANSI | main (s) | head (s) | ratio |
|---|---|---|---|---|---|
| agg_filter | 100 | true | 0.095 | 0.107 | 1.13 |
| agg_filter | 18 | true | 0.021 | 0.023 | 1.10 |
| agg_filter | 500 | true | 0.678 | 0.726 | 1.07 |
| join_on | 100 | true | 0.080 | 0.093 | 1.16 |
| join_on | 18 | true | 0.022 | 0.021 | 0.95 |
| join_on | 500 | true | 0.612 | 0.663 | 1.08 |
| nest_filter | 100 | true | 0.023 | 0.027 | 1.17 |
| nest_filter | 18 | true | 0.006 | 0.007 | 1.17 |
| nest_filter | 500 | true | 0.302 | 0.380 | 1.26 |
| nest_select | 100 | true | 0.232 | 0.243 | 1.05 |
| nest_select | 18 | true | 0.015 | 0.017 | 1.13 |
| nest_select | 500 | true | 5.344 | 5.477 | 1.02 |
| select | 100 | true | 0.327 | 0.384 | 1.17 |
| select | 18 | true | 0.034 | 0.039 | 1.15 |
| select | 500 | true | 5.661 | 6.658 | 1.18 |
| where | 100 | true | 0.076 | 0.085 | 1.12 |
| where | 18 | true | 0.016 | 0.017 | 1.06 |
| where | 500 | true | 0.580 | 0.634 | 1.09 |
| where_or | 100 | true | 0.083 | 0.093 | 1.12 |
| where_or | 18 | true | 0.016 | 0.017 | 1.06 |
| where_or | 500 | true | 0.755 | 0.805 | 1.07 |
| agg_filter | 100 | false | 0.096 | 0.108 | 1.12 |
| agg_filter | 18 | false | 0.022 | 0.025 | 1.14 |
| agg_filter | 500 | false | 0.687 | 0.728 | 1.06 |
| join_on | 100 | false | 0.085 | 0.094 | 1.11 |
| join_on | 18 | false | 0.021 | 0.024 | 1.14 |
| join_on | 500 | false | 0.625 | 0.669 | 1.07 |
| nest_filter | 100 | false | 0.025 | 0.027 | 1.08 |
| nest_filter | 18 | false | 0.006 | 0.007 | 1.17 |
| nest_filter | 500 | false | 0.308 | 0.383 | 1.24 |
| nest_select | 100 | false | 0.234 | 0.242 | 1.03 |
| nest_select | 18 | false | 0.015 | 0.017 | 1.13 |
| nest_select | 500 | false | 5.368 | 5.426 | 1.01 |
| select | 100 | false | 0.337 | 0.382 | 1.13 |
| select | 18 | false | 0.037 | 0.042 | 1.14 |
| select | 500 | false | 5.734 | 6.577 | 1.15 |
| where | 100 | false | 0.078 | 0.085 | 1.09 |
| where | 18 | false | 0.016 | 0.018 | 1.12 |
| where | 500 | false | 0.601 | 0.631 | 1.05 |
| where_or | 100 | false | 0.085 | 0.094 | 1.11 |
| where_or | 18 | false | 0.016 | 0.018 | 1.12 |
| where_or | 500 | false | 0.771 | 0.798 | 1.04 |
