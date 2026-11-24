# ASOF-JOIN-1 — domain table

Every cell ran on Spark 4.1.2 and on main `9a48932f` (product of `a4d8857f`), session zone UTC, in both ANSI modes except
Spark's grid (`G`), which the builder ran with ANSI on only and the critic ran with ANSI off on 2026-11-20 (2,268
records); main ran every group in both modes, and a main cell with no Spark
answer has no verdict ([evidence/](evidence/) has the provenance). Spark's answer is `left._joinAsOf(...)` (surface `D`), the
oracle for every verdict. Main's cells are three surfaces: the same direct call (`main-D`), and the two workarounds
a user can write today, W1 (a range join plus `row_number`) and W2 (a union plus a window fill)
([sketch.md](sketch.md) §2). `merge_asof` (`P`) and the SQL spellings (`S`) have their own rows.

**Classes.** EQUAL: the same sorted rows, names and types (or both raise). TIE: the rows differ only in which of
several tied right rows a left row got, and both answers satisfy Spark's rule (`probes/model.py`'s tie set).
WRONG: main answers different rows, or answers where Spark raises (WRONG-NAMES and WRONG-TYPE when only the names
or types differ). RAISE: main raises where Spark answers. MISSING: RePark has no door (`ATTRIBUTE_NOT_SUPPORTED`,
or no `pyspark.pandas`). `fidelity` rows compare each workaround on Spark with Spark's own `_joinAsOf`; `engine`
rows compare a workaround on main with the same workaround on Spark.

**Groups.** `G` the grid (7 as-of types × 3 directions × `allowExactMatches` × tolerance none/1/2 × `inner`/`left`
× no key/one key/two keys, on 14 left and 11 right rows with NULL keys, NULL as-of values, duplicate right values
and a `nearest` tie); `E` errors; `M` mixed types across the sides; `O` the integer bounds; `N` NaN, signed zero,
infinities; `T` ties under input order, partitioning and shuffle partitions; `C` call forms; `S` SQL and helper
availability; `P` `merge_asof` beside the `_joinAsOf` call it lowers to (`PD`).

## Totals

| pair | cells | verdicts |
|---|---|---|
| main-D | 1280 | MISSING 1280 |
| main-W1 | 1116 | EQUAL 318, TIE 710, WRONG 56, RAISE 32 |
| main-W2 | 972 | EQUAL 564, TIE 368, WRONG 36, RAISE 4 |
| main-P | 135 | MISSING 135 |
| main-SQL | 14 | EQUAL 4, RAISE 10 |
| main-F.expr | 2 | RAISE 2 |
| main-F | 2 | RAISE 2 |
| main-DataFrame | 2 | RAISE 2 |
| **all main cells** | **3523** | the direct call 1280, the workarounds 2088, `merge_asof` 135, SQL and helpers 20 |

All main cells per mode, with the critic's ANSI-off grid (2026-11-20; 5,791 classified main cells = 3,523 + 3 × 756):

| pair | ANSI on | ANSI off |
|---|---|---|
| main-D | 1018: MISSING 1018 | 1018: MISSING 1018 |
| main-W1 | 936: EQUAL 262, TIE 628, WRONG 28, RAISE 18 | 936: EQUAL 266, TIE 628, WRONG 28, RAISE 14 |
| main-W2 | 864: EQUAL 520, TIE 324, WRONG 16, RAISE 4 | 864: EQUAL 520, TIE 324, WRONG 20 |
| main-P | MISSING 68 | MISSING 67 |
| main-SQL / F.expr / F / DataFrame | EQUAL 2, RAISE 8 | EQUAL 2, RAISE 8 |

Spark records 3868 (3,655 unique; `spark-true-EM-` holds its 213 `E` and `M` records twice) and main records 5692
(every surface, both modes, `EXPLAIN` included). Groups by main cells: C 26, E 138, G 2268, M 288, N 72, O 144, P 135,
S 20, T 432.

Spark's `EXPLAIN` for each direction, with and without keys: [evidence/raw/explain-spark.txt](evidence/raw/explain-spark.txt);
main's W1 and W2 plans: [evidence/raw/explain-main.txt](evidence/raw/explain-main.txt).

## The tables (`probes/matrix.py`)

### Totals by group, mode and pair

| group | ansi | pair | EQUAL | TIE | WRONG | WRONG-NAMES | WRONG-TYPE | RAISE | MISSING |
|---|---|---|---|---|---|---|---|---|---|
| C | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 13 |
| C | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 13 |
| E | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 69 |
| E | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 69 |
| G | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 756 |
| G | true | main-W1 | 210 | 546 | 0 | 0 | 0 | 0 | 0 |
| G | true | main-W2 | 476 | 280 | 0 | 0 | 0 | 0 | 0 |
| G | true | spark-W1 | 436 | 320 | 0 | 0 | 0 | 0 | 0 |
| G | true | spark-W2 | 476 | 280 | 0 | 0 | 0 | 0 | 0 |
| G | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 756 |
| G | false | main-W1 | 210 | 546 | 0 | 0 | 0 | 0 | 0 |
| G | false | main-W2 | 476 | 280 | 0 | 0 | 0 | 0 | 0 |
| G | false | spark-W1 | 439 | 317 | 0 | 0 | 0 | 0 | 0 |
| G | false | spark-W2 | 476 | 280 | 0 | 0 | 0 | 0 | 0 |
| M | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 72 |
| M | false | main-W1 | 12 | 30 | 16 | 0 | 0 | 14 | 0 |
| M | false | spark-W1 | 68 | 0 | 4 | 0 | 0 | 0 | 0 |
| M | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 72 |
| M | true | main-W1 | 12 | 30 | 16 | 0 | 0 | 14 | 0 |
| M | true | spark-W1 | 68 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 12 |
| N | false | main-W1 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | false | main-W2 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | false | spark-W1 | 10 | 0 | 2 | 0 | 0 | 0 | 0 |
| N | false | spark-W2 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 12 |
| N | true | main-W1 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | true | main-W2 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | true | spark-W1 | 10 | 0 | 2 | 0 | 0 | 0 | 0 |
| N | true | spark-W2 | 8 | 0 | 4 | 0 | 0 | 0 | 0 |
| O | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 24 |
| O | false | main-W1 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| O | false | main-W2 | 8 | 0 | 16 | 0 | 0 | 0 | 0 |
| O | false | spark-W1 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| O | false | spark-W2 | 8 | 0 | 16 | 0 | 0 | 0 | 0 |
| O | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 24 |
| O | true | main-W1 | 12 | 0 | 8 | 0 | 0 | 4 | 0 |
| O | true | main-W2 | 8 | 0 | 12 | 0 | 0 | 4 | 0 |
| O | true | spark-W1 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| O | true | spark-W2 | 10 | 0 | 14 | 0 | 0 | 0 | 0 |
| P | false | main-P | 0 | 0 | 0 | 0 | 0 | 0 | 67 |
| P | false | spark-PD | 57 | 0 | 0 | 0 | 0 | 0 | 0 |
| P | true | main-P | 0 | 0 | 0 | 0 | 0 | 0 | 68 |
| P | true | spark-PD | 57 | 0 | 0 | 0 | 0 | 0 | 0 |
| S | false | main-DataFrame | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | false | main-F | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | false | main-F.expr | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | false | main-SQL | 2 | 0 | 0 | 0 | 0 | 5 | 0 |
| S | true | main-DataFrame | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | true | main-F | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | true | main-F.expr | 0 | 0 | 0 | 0 | 0 | 1 | 0 |
| S | true | main-SQL | 2 | 0 | 0 | 0 | 0 | 5 | 0 |
| T | false | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 72 |
| T | false | main-W1 | 20 | 52 | 0 | 0 | 0 | 0 | 0 |
| T | false | main-W2 | 28 | 44 | 0 | 0 | 0 | 0 | 0 |
| T | false | spark-W1 | 72 | 0 | 0 | 0 | 0 | 0 | 0 |
| T | false | spark-W2 | 28 | 44 | 0 | 0 | 0 | 0 | 0 |
| T | true | main-D | 0 | 0 | 0 | 0 | 0 | 0 | 72 |
| T | true | main-W1 | 20 | 52 | 0 | 0 | 0 | 0 | 0 |
| T | true | main-W2 | 28 | 44 | 0 | 0 | 0 | 0 | 0 |
| T | true | spark-W1 | 72 | 0 | 0 | 0 | 0 | 0 | 0 |
| T | true | spark-W2 | 28 | 44 | 0 | 0 | 0 | 0 | 0 |

### Grid: main's workarounds against Spark's _joinAsOf, by type and direction (ANSI on)

| type | direction | pair | EQUAL | TIE | WRONG | WRONG-NAMES | WRONG-TYPE | RAISE | MISSING |
|---|---|---|---|---|---|---|---|---|---|
| BIGINT | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| BIGINT | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| BIGINT | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| BIGINT | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| BIGINT | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| BIGINT | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| DATE | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| DATE | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| DATE | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DATE | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DATE | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| DATE | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| DECIMAL(10,2) | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| DOUBLE | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| INT | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| INT | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| INT | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| INT | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| INT | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| INT | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | backward | main-W1 | 10 | 26 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | backward | main-W2 | 36 | 0 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | forward | main-W1 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | forward | main-W2 | 14 | 22 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | nearest | main-W1 | 6 | 30 | 0 | 0 | 0 | 0 | 0 |
| TIMESTAMP_NTZ | nearest | main-W2 | 18 | 18 | 0 | 0 | 0 | 0 | 0 |

### Errors (BIGINT unless named; on=k1, how=left, backward unless named)

| case | Spark ANSI on | Spark ANSI off | main ANSI on |
|---|---|---|---|
| boolean_asof:backward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| boolean_asof:forward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| boolean_asof:nearest | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_day_interval:backward | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_day_interval:forward | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_day_interval:nearest | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_double:backward | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_double:forward | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_double:nearest | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_second_interval:backward | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_second_interval:forward | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| date_tol_second_interval:nearest | 14 rows date,date | 14 rows date,date | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| decimal_tol_double:backward | 14 rows decimal(10,2),decimal(10,2) | 14 rows decimal(10,2),decimal(10,2) | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| decimal_tol_double:forward | 14 rows decimal(10,2),decimal(10,2) | 14 rows decimal(10,2),decimal(10,2) | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| decimal_tol_double:nearest | 14 rows decimal(10,2),decimal(10,2) | 14 rows decimal(10,2),decimal(10,2) | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| dir_mixed_case | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| dir_none | Py4JJavaError An error occurred while calling o78.joinAsOf. : java.lang.NullPointerE | Py4JJavaError An error occurred while calling o11904.joinAsOf. : java.lang.NullPoint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| dir_sideways | AnalysisException AS_OF_JOIN.UNSUPPORTED_DIRECTION | AnalysisException AS_OF_JOIN.UNSUPPORTED_DIRECTION | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| dir_upper | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| double_tol_decimal:backward | 14 rows double,double | 14 rows double,double | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| double_tol_decimal:forward | 14 rows double,double | 14 rows double,double | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| double_tol_decimal:nearest | 14 rows double,double | 14 rows double,double | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| exact_string | Py4JError An error occurred while calling o78.joinAsOf. Trace: py4j.Py4JExceptio | Py4JError An error occurred while calling o11904.joinAsOf. Trace: py4j.Py4JExcep | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_INNER | 10 rows bigint,bigint | 10 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_LEFT | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_anti | IllegalArgumentException requirement failed: Unsupported as-of join type LeftAnti | IllegalArgumentException requirement failed: Unsupported as-of join type LeftAnti | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_bogus | AnalysisException UNSUPPORTED_JOIN_TYPE | AnalysisException UNSUPPORTED_JOIN_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_cross | IllegalArgumentException requirement failed: Unsupported as-of join type Cross | IllegalArgumentException requirement failed: Unsupported as-of join type Cross | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_full | IllegalArgumentException requirement failed: Unsupported as-of join type FullOuter | IllegalArgumentException requirement failed: Unsupported as-of join type FullOuter | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_inner | 10 rows bigint,bigint | 10 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_left_outer | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_leftouter | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_outer | IllegalArgumentException requirement failed: Unsupported as-of join type FullOuter | IllegalArgumentException requirement failed: Unsupported as-of join type FullOuter | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_right | IllegalArgumentException requirement failed: Unsupported as-of join type RightOuter | IllegalArgumentException requirement failed: Unsupported as-of join type RightOuter | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| how_semi | IllegalArgumentException requirement failed: Unsupported as-of join type LeftSemi | IllegalArgumentException requirement failed: Unsupported as-of join type LeftSemi | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| missing_left_col | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| missing_on | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| missing_right_col | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | AnalysisException UNRESOLVED_COLUMN.WITH_SUGGESTION | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ntz_tol_int:backward | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ntz_tol_int:forward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ntz_tol_int:nearest | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_empty_list | IndexError list index out of range | IndexError list index out of range | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof:backward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof:forward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof:nearest | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof_tol:backward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof_tol:forward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| string_asof_tol:nearest | AnalysisException DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE | 14 rows string,string | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_column | Py4JJavaError INTERNAL_ERROR | Py4JJavaError INTERNAL_ERROR | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_double_on_bigint | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_interval_on_bigint | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_negative | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_null | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_null_int | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_python_int | AssertionError tolerance should be Column | AssertionError tolerance should be Column | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_string | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| tol_zero | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_day_interval:backward | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_day_interval:forward | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_day_interval:nearest | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_int:backward | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_int:forward | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_int:nearest | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | AnalysisException DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_month_interval:backward | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_month_interval:forward | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_month_interval:nearest | 14 rows timestamp,timestamp | 14 rows timestamp,timestamp | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_negative_interval:backward | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_negative_interval:forward | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| ts_tol_negative_interval:nearest | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | AnalysisException AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |

### Call forms

| case | Spark ANSI on | Spark ANSI off | main ANSI on |
|---|---|---|---|
| asof_columns | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| asof_expression | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| asof_renamed_right | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| inner_no_on | 12 rows bigint,bigint | 12 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_column | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_columns | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_list_one | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_non_equi | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| on_null_safe | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| select_k1 | AnalysisException AMBIGUOUS_REFERENCE | AnalysisException AMBIGUOUS_REFERENCE | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| select_left_k1 | 14 rows  | 14 rows  | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| select_star | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |
| self_join | 14 rows bigint,bigint | 14 rows bigint,bigint | PySparkAttributeError ATTRIBUTE_NOT_SUPPORTED |

### SQL and helper availability

| case | Spark ANSI on | Spark ANSI off | main ANSI on |
|---|---|---|---|
| asof_as_alias | 8 rows  | 8 rows  | ParseException SQL error: ParserError("Expected: MATCH_CONDITION, found: ON at Line:  |
| correlated_non_equi_scalar | 14 rows  | 14 rows  | AnalysisException Optimizer rule 'scalar_subquery_to_join' failed caused by Schema error |
| left_asof | ParseException PARSE_SYNTAX_ERROR | ParseException PARSE_SYNTAX_ERROR | ParseException SQL error: ParserError("expected OUTER, SEMI, ANTI or JOIN after LEFT" |
| max_by_sql | 1 rows  | 1 rows  | AnalysisException UNRESOLVED_ROUTINE |
| min_by_expr | 3 rows  | 3 rows  | AnalysisException UNRESOLVED_ROUTINE |
| min_by_sql | 1 rows  | 1 rows  | AnalysisException UNRESOLVED_ROUTINE |
| min_by_struct_grouped | 3 rows  | 3 rows  | AnalysisException UNRESOLVED_ROUTINE |
| monotonically_increasing_id | 14 rows  | 14 rows  | UnsupportedOperationException functions.monotonically_increasing_id is not supported yet (single-nod |
| select_list | 14 rows  | 14 rows  | PySparkTypeError expected a column name (str) or Column, got list |
| snowflake_match_condition | ParseException PARSE_SYNTAX_ERROR | ParseException PARSE_SYNTAX_ERROR | UnsupportedOperationException This feature is not implemented: Unsupported JOIN operator AsOf {…} |

### Mixed as-of types (left>right), on=k1, how=left

| cell | Spark ANSI on | Spark ANSI off |
|---|---|---|
| BIGINT>DECIMAL(10,2) backward exact=True tol=2 | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT backward exact=True tol=2 | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING backward exact=True tol=2 | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT backward exact=True tol=2 | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP backward exact=True tol=2 | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) backward exact=True tol=2 | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT backward exact=True tol=2 | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE backward exact=True tol=2 | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT backward exact=True tol=2 | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE backward exact=True tol=2 | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ backward exact=True tol=2 | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP backward exact=True tol=2 | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| BIGINT>DECIMAL(10,2) backward exact=True tol=None | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT backward exact=True tol=None | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING backward exact=True tol=None | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT backward exact=True tol=None | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP backward exact=True tol=None | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) backward exact=True tol=None | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT backward exact=True tol=None | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE backward exact=True tol=None | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT backward exact=True tol=None | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE backward exact=True tol=None | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ backward exact=True tol=None | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP backward exact=True tol=None | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| BIGINT>DECIMAL(10,2) forward exact=True tol=2 | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT forward exact=True tol=2 | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING forward exact=True tol=2 | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT forward exact=True tol=2 | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP forward exact=True tol=2 | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) forward exact=True tol=2 | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT forward exact=True tol=2 | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE forward exact=True tol=2 | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT forward exact=True tol=2 | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE forward exact=True tol=2 | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ forward exact=True tol=2 | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP forward exact=True tol=2 | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| BIGINT>DECIMAL(10,2) forward exact=True tol=None | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT forward exact=True tol=None | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING forward exact=True tol=None | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT forward exact=True tol=None | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP forward exact=True tol=None | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) forward exact=True tol=None | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT forward exact=True tol=None | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE forward exact=True tol=None | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT forward exact=True tol=None | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE forward exact=True tol=None | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ forward exact=True tol=None | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP forward exact=True tol=None | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| BIGINT>DECIMAL(10,2) nearest exact=True tol=2 | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT nearest exact=True tol=2 | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING nearest exact=True tol=2 | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT nearest exact=True tol=2 | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP nearest exact=True tol=2 | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) nearest exact=True tol=2 | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT nearest exact=True tol=2 | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE nearest exact=True tol=2 | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT nearest exact=True tol=2 | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE nearest exact=True tol=2 | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ nearest exact=True tol=2 | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP nearest exact=True tol=2 | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| BIGINT>DECIMAL(10,2) nearest exact=True tol=None | 14 rows bigint,decimal(10,2) | 14 rows bigint,decimal(10,2) |
| BIGINT>INT nearest exact=True tol=None | 14 rows bigint,int | 14 rows bigint,int |
| BIGINT>STRING nearest exact=True tol=None | 14 rows bigint,string | 14 rows bigint,string |
| DATE>BIGINT nearest exact=True tol=None | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES | AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES |
| DATE>TIMESTAMP nearest exact=True tol=None | 14 rows date,timestamp | 14 rows date,timestamp |
| DOUBLE>DECIMAL(10,2) nearest exact=True tol=None | 14 rows double,decimal(10,2) | 14 rows double,decimal(10,2) |
| INT>BIGINT nearest exact=True tol=None | 14 rows int,bigint | 14 rows int,bigint |
| INT>DOUBLE nearest exact=True tol=None | 14 rows int,double | 14 rows int,double |
| STRING>BIGINT nearest exact=True tol=None | 14 rows string,bigint | 14 rows string,bigint |
| TIMESTAMP>DATE nearest exact=True tol=None | 14 rows timestamp,date | 14 rows timestamp,date |
| TIMESTAMP>TIMESTAMP_NTZ nearest exact=True tol=None | 14 rows timestamp,timestamp_ntz | 14 rows timestamp,timestamp_ntz |
| TIMESTAMP_NTZ>TIMESTAMP nearest exact=True tol=None | 14 rows timestamp_ntz,timestamp | 14 rows timestamp_ntz,timestamp |
| main-W2 (critic check 4) | M on: EQ 43, TIE 10, tie-pick-only 4, WRONG 4, RAISE 11 | M off: the same |

### Overflow at the integer bounds, on=k1, how=left

| cell | Spark ANSI on | Spark ANSI off |
|---|---|---|
| BIGINT backward exact=False tol=2 | 3 rows bigint,bigint | 3 rows bigint,bigint |
| INT backward exact=False tol=2 | 3 rows int,int | 3 rows int,int |
| BIGINT backward exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT backward exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT backward exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT backward exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT backward exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT backward exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT forward exact=False tol=2 | 3 rows bigint,bigint | 3 rows bigint,bigint |
| INT forward exact=False tol=2 | 3 rows int,int | 3 rows int,int |
| BIGINT forward exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT forward exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT forward exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT forward exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT forward exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT forward exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT nearest exact=False tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT nearest exact=False tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT nearest exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT nearest exact=False tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT nearest exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT nearest exact=True tol=2 | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |
| BIGINT nearest exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows bigint,bigint |
| INT nearest exact=True tol=None | ArithmeticException ARITHMETIC_OVERFLOW | 3 rows int,int |

### DOUBLE NaN, -0.0 and infinities, on=k1, how=left

| cell | Spark ANSI on | Spark ANSI off |
|---|---|---|
| DOUBLE backward exact=False tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE backward exact=False tol=None | 5 rows double,double | 5 rows double,double |
| DOUBLE backward exact=True tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE backward exact=True tol=None | 5 rows double,double | 5 rows double,double |
| DOUBLE forward exact=False tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE forward exact=False tol=None | 5 rows double,double | 5 rows double,double |
| DOUBLE forward exact=True tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE forward exact=True tol=None | 5 rows double,double | 5 rows double,double |
| DOUBLE nearest exact=False tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE nearest exact=False tol=None | 5 rows double,double | 5 rows double,double |
| DOUBLE nearest exact=True tol=1 | 5 rows double,double | 5 rows double,double |
| DOUBLE nearest exact=True tol=None | 5 rows double,double | 5 rows double,double |

### Non-grid verdicts by pair (both modes)

| group | pair | EQUAL | TIE | WRONG | WRONG-NAMES | WRONG-TYPE | RAISE | MISSING |
|---|---|---|---|---|---|---|---|---|
| M | main-W1 | 24 | 60 | 32 | 0 | 0 | 28 | 0 |
| M | spark-W1 | 136 | 0 | 8 | 0 | 0 | 0 | 0 |
| N | main-W1 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| N | main-W2 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| N | spark-W1 | 20 | 0 | 4 | 0 | 0 | 0 | 0 |
| N | spark-W2 | 16 | 0 | 8 | 0 | 0 | 0 | 0 |
| O | main-W1 | 28 | 0 | 16 | 0 | 0 | 4 | 0 |
| O | main-W2 | 16 | 0 | 28 | 0 | 0 | 4 | 0 |
| O | spark-W1 | 32 | 0 | 16 | 0 | 0 | 0 | 0 |
| O | spark-W2 | 18 | 0 | 30 | 0 | 0 | 0 | 0 |
| P | main-P | 0 | 0 | 0 | 0 | 0 | 0 | 135 |
| P | spark-PD | 114 | 0 | 0 | 0 | 0 | 0 | 0 |
| T | main-W1 | 40 | 104 | 0 | 0 | 0 | 0 | 0 |
| T | main-W2 | 56 | 88 | 0 | 0 | 0 | 0 | 0 |
| T | spark-W1 | 144 | 0 | 0 | 0 | 0 | 0 | 0 |
| T | spark-W2 | 56 | 88 | 0 | 0 | 0 | 0 | 0 |

### Every WRONG, WRONG-NAMES, WRONG-TYPE and RAISE cell

- N main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DATE>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- N main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- O main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- N main-W1 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>DATE"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>DATE"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 RAISE {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- N main-W1 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "false", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- N main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W1 RAISE {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W2 RAISE {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W1 RAISE {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W2 RAISE {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DATE>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- N main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "backward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W1 RAISE {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W2 RAISE {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O main-W1 RAISE {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W2 RAISE {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "forward", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- O main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O main-W2 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W1 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "forward", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- N main-W1 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": 2, "typ": "INT"}
- O main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": false, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "STRING>BIGINT"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>DATE"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": 2, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT>STRING"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DATE>TIMESTAMP"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "STRING>BIGINT"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>DATE"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP>TIMESTAMP_NTZ"}
- M main-W1 RAISE {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "TIMESTAMP_NTZ>TIMESTAMP"}
- N main-W1 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- N spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "DOUBLE"}
- O main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "BIGINT"}
- O main-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}
- O spark-W2 WRONG {"ansi": "true", "dir": "nearest", "exact": true, "how": "left", "on": "k1", "tol": null, "typ": "INT"}

## Ties (`probes/ties.py`)

```text
nearest ties, which side wins (grid, all types and modes)
  spark D after 238
  spark D before 140
  spark W1 after 175
  spark W1 before 203
  spark W2 before 378
duplicate right rows at one as-of value, which rid wins (grid)
  spark D higher-rid 966
  spark W1 higher-rid 245
  spark W1 lowest-rid 721
  spark W2 higher-rid 700
  spark W2 lowest-rid 266
tie group T: picks per (order, partitioning, shuffle partitions, payload) over 2 repeats
  spark D backward lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D backward lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D backward lid2 false {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark D backward lid2 true {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark D forward lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D forward lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D forward lid2 false {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark D forward lid2 true {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark D nearest lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D nearest lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark D nearest lid2 false {'y18': 4, 'z22': 20} by order: {'asis': ['z22'], 'reversed': ['y18', 'z22'], 'rotated': ['z22']}
  spark D nearest lid2 true {'y18': 4, 'z22': 20} by order: {'asis': ['z22'], 'reversed': ['y18', 'z22'], 'rotated': ['z22']}
  spark W1 backward lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 backward lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 backward lid2 false {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark W1 backward lid2 true {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark W1 forward lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 forward lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 forward lid2 false {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark W1 forward lid2 true {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark W1 nearest lid1 false {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 nearest lid1 true {'x1': 4, 'x2': 4, 'x3': 16} by order: {'asis': ['x3'], 'reversed': ['x1', 'x3'], 'rotated': ['x2', 'x3']}
  spark W1 nearest lid2 false {'y18': 4, 'z22': 20} by order: {'asis': ['z22'], 'reversed': ['y18', 'z22'], 'rotated': ['z22']}
  spark W1 nearest lid2 true {'y18': 4, 'z22': 20} by order: {'asis': ['z22'], 'reversed': ['y18', 'z22'], 'rotated': ['z22']}
  spark W2 backward lid1 false {'x1': 4, 'x2': 16, 'x3': 4} by order: {'asis': ['x2', 'x3'], 'reversed': ['x1', 'x2'], 'rotated': ['x2']}
  spark W2 backward lid1 true {'x1': 4, 'x2': 16, 'x3': 4} by order: {'asis': ['x2', 'x3'], 'reversed': ['x1', 'x2'], 'rotated': ['x2']}
  spark W2 backward lid2 false {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark W2 backward lid2 true {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark W2 forward lid1 false {'x1': 4, 'x3': 20} by order: {'asis': ['x1', 'x3'], 'reversed': ['x3'], 'rotated': ['x3']}
  spark W2 forward lid1 true {'x1': 4, 'x3': 20} by order: {'asis': ['x1', 'x3'], 'reversed': ['x3'], 'rotated': ['x3']}
  spark W2 forward lid2 false {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark W2 forward lid2 true {'z22': 24} by order: {'asis': ['z22'], 'reversed': ['z22'], 'rotated': ['z22']}
  spark W2 nearest lid1 false {'x1': 4, 'x2': 16, 'x3': 4} by order: {'asis': ['x2', 'x3'], 'reversed': ['x1', 'x2'], 'rotated': ['x2']}
  spark W2 nearest lid1 true {'x1': 4, 'x2': 16, 'x3': 4} by order: {'asis': ['x2', 'x3'], 'reversed': ['x1', 'x2'], 'rotated': ['x2']}
  spark W2 nearest lid2 false {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
  spark W2 nearest lid2 true {'y18': 24} by order: {'asis': ['y18'], 'reversed': ['y18'], 'rotated': ['y18']}
```

Main W2: a `nearest` tie goes to the earlier row in 756 of 756; at duplicate right values it takes the higher `rid` in
1,400 cells and the lowest in 532.
