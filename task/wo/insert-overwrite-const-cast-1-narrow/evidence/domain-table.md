# INSERT-OVERWRITE-CONST-CAST-1, narrow fix: step 0 domain table

- **Measured:** 2026-11-25, before any product code.
- **Oracle:** live Spark 4.1.2 with Iceberg 1.11.0 on a hadoop catalog, session zone UTC, both ANSI modes.
- **RePark main:** the package built from #1429's tested head `57c92b41` (main has since added Markdown and #1434,
  which changes only the Spark door's SQL text rewrites).
- **Scripts:** `probe_doors_shapes.py` (doors and shapes, 1,040 cells per engine, then a Spark read-back of every table
  RePark wrote) and `probe_decision_table.py` (110 strings x 12 types x 2 ANSI modes, 2,616 cells per engine).
- **Spark read-back of main's tables:** 1,040 cells, every table read. All row counts match RePark's own read. Three
  rows differ only in how PySpark's `collect()` renders a `TIMESTAMP` (it converts to the Python process's local zone);
  the pins read `CAST(x AS STRING)` under UTC so this cannot recur.

## 1. Measured cells

### A. The card's cells and the four further casts (door: `INSERT OVERWRITE t`)

| Cell | ANSI | Spark 4.1.2 | main | main, read back by Spark |
|---|---|---|---|---|
| `DOUBLE` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `TIMESTAMP` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `DOUBLE` | true | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | 0 row(s) |
| `TIMESTAMP` | true | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | 0 row(s) |
| `f1_ntz_subquery` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `f2_ntz_literal` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `f3_dec_from_double_subquery` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `f4_dec_from_double_literal` | false | OK, 0 row(s) | OK, 0 row(s) | 0 row(s) |
| `f1_ntz_subquery` | true | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | 0 row(s) |
| `f2_ntz_literal` | true | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | 0 row(s) |
| `f3_dec_from_double_subquery` | true | `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, 1 row(s) | OK, 0 row(s) | 0 row(s) |
| `f4_dec_from_double_literal` | true | `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, 1 row(s) | OK, 0 row(s) | 0 row(s) |

### B. Each door, the card's shape, value `'x'`

| Door | Type | ANSI on: Spark | ANSI on: main | ANSI off: Spark | ANSI off: main |
|---|---|---|---|---|---|
| `ctl_read` | DOUBLE | `CAST_INVALID_INPUT` | OK | OK | OK |
| `ctl_read` | TIMESTAMP | `CAST_INVALID_INPUT` | OK | OK | OK |
| `ctl_read` | NTZ | `CAST_INVALID_INPUT` | OK | OK | OK |
| `ctl_read` | INT | `CAST_INVALID_INPUT` | OK | OK | OK |
| `ctl_sql_ctas` | DOUBLE | `CAST_INVALID_INPUT` | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_ctas` | TIMESTAMP | `CAST_INVALID_INPUT` | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_ctas` | NTZ | `CAST_INVALID_INPUT` | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_ctas` | INT | `CAST_INVALID_INPUT` | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_insert_into` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_sql_insert_into` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_sql_insert_into` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_sql_insert_into` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_sql_rtas` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_rtas` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_rtas` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_sql_rtas` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_v1_save_as_table_append` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v1_save_as_table_append` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v1_save_as_table_append` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v1_save_as_table_append` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v2_append` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v2_append` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v2_append` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v2_append` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `ctl_v2_create_or_replace` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_v2_create_or_replace` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_v2_create_or_replace` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `ctl_v2_create_or_replace` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow` | INT | `CAST_INVALID_INPUT`, 1 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) | OK, 0 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) |
| `sql_ow_dynamic_mode` | DOUBLE | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `sql_ow_dynamic_mode` | TIMESTAMP | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `sql_ow_dynamic_mode` | NTZ | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `sql_ow_dynamic_mode` | INT | `CAST_INVALID_INPUT`, 2 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 2 row(s) | OK, 2 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 2 row(s) |
| `sql_ow_dynamic_part` | DOUBLE | `CAST_INVALID_INPUT`, 2 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_dynamic_part` | TIMESTAMP | `CAST_INVALID_INPUT`, 2 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_dynamic_part` | NTZ | `CAST_INVALID_INPUT`, 2 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_dynamic_part` | INT | `CAST_INVALID_INPUT`, 2 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_static_part` | DOUBLE | `CAST_INVALID_INPUT`, 2 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `sql_ow_static_part` | TIMESTAMP | `CAST_INVALID_INPUT`, 2 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `sql_ow_static_part` | NTZ | `CAST_INVALID_INPUT`, 2 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `sql_ow_static_part` | INT | `CAST_INVALID_INPUT`, 2 row(s) | OK, 1 row(s) | OK, 1 row(s) | OK, 1 row(s) |
| `sql_ow_table` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_table` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_table` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `sql_ow_table` | INT | `CAST_INVALID_INPUT`, 1 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) | OK, 0 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) |
| `v1_insert_into_overwrite` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_insert_into_overwrite` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_insert_into_overwrite` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_insert_into_overwrite` | INT | `CAST_INVALID_INPUT`, 1 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) | OK, 0 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 1 row(s) |
| `v1_save_as_table_overwrite` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_save_as_table_overwrite` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_save_as_table_overwrite` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v1_save_as_table_overwrite` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v2_overwrite_cond` | DOUBLE | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v2_overwrite_cond` | TIMESTAMP | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v2_overwrite_cond` | NTZ | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v2_overwrite_cond` | INT | `CAST_INVALID_INPUT`, 1 row(s) | OK, 0 row(s) | OK, 0 row(s) | OK, 0 row(s) |
| `v2_overwrite_partitions` | DOUBLE | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `v2_overwrite_partitions` | TIMESTAMP | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `v2_overwrite_partitions` | NTZ | `CAST_INVALID_INPUT`, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) | OK, 2 row(s) |
| `v2_overwrite_partitions` | INT | `CAST_INVALID_INPUT`, 2 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 2 row(s) | OK, 2 row(s) | `Error during planning: INSERT OVERWRITE empty sour`, 2 row(s) |

### C. Shapes, door `INSERT OVERWRITE t`, ANSI on, value `'x'` (`s9`: `'abc'`)

| Shape | DOUBLE | FLOAT | TIMESTAMP | NTZ | INT | BIGINT | DECIMAL | BOOLEAN | DATE |
|---|---|---|---|---|---|---|---|---|---|
| `o1_values_column` | OK / OK | OK / OK | OK / OK | OK / OK | OK / W03 | OK / W03 | OK / W03 | OK / W03 | OK / W03 |
| `o2_limit0` | OK / CII | OK / CII | OK / OK | OK / OK | OK / W03 | OK / W03 | OK / W03 | OK / W03 | OK / W03 |
| `o3_union` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o4_join` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o5_aggregate` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o6_case_dead` | OK / OK | OK / OK | OK / OK | OK / OK | OK / W03 | OK / W03 | OK / W03 | OK / W03 | OK / W03 |
| `o7_try_cast` | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK |
| `o8_scalar_subquery` | CII / CII | CII / CII | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o9_exists` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o10_real_table` | OK / OK | OK / OK | OK / OK | OK / OK | OK / W03 | OK / W03 | OK / W03 | OK / W03 | OK / W03 |
| `o11_temp_view` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o12_distinct` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o13_window` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `o14_valid_value` | OK / OK | OK / OK | CII / OK | CII / OK | OK / W03 | OK / W03 | OK / W03 | OK / W03 | CII / W03 |
| `o15_null_literal` | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK | OK / OK |
| `s1_fromless_literal` | CII / CII | CII / CII | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / GENERIC:datafusion | CII / GENERIC:datafusion | CII / GENERIC:datafusion |
| `s2_subquery` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s3_nested_subquery` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s4_inner_cast_projected` | CII / CII | CII / CII | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s5_inner_filter` | CII / CII | CII / CII | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s6_cte` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s7_values_leaf_literal_cast` | OK / CII | OK / CII | OK / OK | OK / OK | OK / W03 | OK / W03 | OK / GENERIC:datafusion | OK / GENERIC:datafusion | OK / GENERIC:datafusion |
| `s8_renamed_alias` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s9_where_false` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |
| `s10_two_filters` | CII / OK | CII / OK | CII / OK | CII / OK | CII / W03 | CII / W03 | CII / W03 | CII / W03 | CII / W03 |

`CII` = `[CAST_INVALID_INPUT]`, `W03` = main's existing generic refusal "INSERT OVERWRITE empty source evaluates a CAST
that can fail at value level", `NVOR` = `NUMERIC_VALUE_OUT_OF_RANGE`. Each cell is "Spark / main".

**What the shapes show:**
- Over a `FROM`-less `SELECT` (`s1` to `s6`, `s8` to `s10`) Spark raises for every type; main wipes for `DOUBLE`,
  `FLOAT`, `TIMESTAMP` and `TIMESTAMP_NTZ` (except where `SparkFloatStringify` already raises), and refuses with `W03`
  for the integral, `DECIMAL`, `BOOLEAN` and `DATE` types.
- Over `VALUES` (`s7`, `o1`) Spark does **not** raise: a `VALUES` relation is evaluated with its filter first. So
  `VALUES` is not a literal-only source for this guard; it stays as on main.
- `LIMIT 0`, `CASE WHEN false`, `TRY_CAST`, `CAST(NULL ...)` and a real table do not raise in Spark.
- Unions, joins, aggregates, `DISTINCT`, windows, `EXISTS`, scalar subqueries and temp views raise in Spark and wipe or
  refuse on main. They are outside the narrow guard.
- With a valid value (`o14`, `'1'`) Spark still raises for `DATE`, `TIMESTAMP` and `TIMESTAMP_NTZ`: a grammar detail.

**Non-empty sources:** with the row present (no `WHERE 1 = 0`), main's cast raises `[CAST_INVALID_INPUT]` and the
table keeps its row, for `DOUBLE` and `TIMESTAMP` alike. No S1.

**Cast spellings:** Spark 4.1.2 answers `to_timestamp(s)`, `to_date(s)`, `to_timestamp_ntz(s)`, `double(s)`, `int(s)`,
`timestamp(s)` and `s::double` exactly as `CAST(s AS ...)`: `[CAST_INVALID_INPUT]` over `(SELECT 'x' AS s) WHERE 1 = 0`.
RePark plans `CAST(s AS TIMESTAMP)` and one-argument `to_timestamp(s)` to the same expression, so the guard cannot tell
them apart and does not need to.

## 2. The decision table

Each letters-only string was cast through a subquery, `SELECT CAST(s AS T) AS v FROM (SELECT '<v>' AS s) src`, so the
row reaches RePark's runtime cast. ANSI on.

| Target | Letters-only values | Spark raises CII | RePark's cast raises CII | Both raise CII | In the table |
|---|---:|---:|---:|---:|---|
| `TINYINT` | 83 | 83 | 83 | 83 | yes, all |
| `SMALLINT` | 83 | 83 | 83 | 83 | yes, all |
| `INT` | 83 | 83 | 83 | 83 | yes, all |
| `BIGINT` | 83 | 83 | 83 | 83 | yes, all |
| `DECIMAL(10,2)` | 83 | 83 | 0 | 0 | no: RePark raises a generic Arrow error |
| `DECIMAL(38,0)` | 83 | 83 | 0 | 0 | no: RePark raises a generic Arrow error |
| `BOOLEAN` | 83 | 66 | 0 | 0 | no: RePark raises a generic Arrow error (59), accepts 24 |
| `DATE` | 83 | 83 | 0 | 0 | no: RePark raises a generic Arrow error |
| `TIMESTAMP` | 83 | 83 | 83 | 83 | yes, all |
| `TIMESTAMP_NTZ` | 83 | 83 | 83 | 83 | yes, all |
| `FLOAT` | 83 | 72 | 66 | 66 | yes, minus the words below |
| `DOUBLE` | 83 | 72 | 66 | 66 | yes, minus the words below |

Letters-only values where the two engines do not both raise, for `FLOAT` and `DOUBLE` (identical):

| Value | Spark 4.1.2 | RePark |
|---|---|---|
| 'INF' | OK | OK |
| 'INFINITY' | OK | OK |
| 'Inf' | OK | OK |
| 'Infinity' | OK | OK |
| `Infinity` + `d` | CII | OK |
| 'NAN' | OK | OK |
| 'NaN' | OK | OK |
| `NaN` + `d` | CII | OK |
| `NaN` + `f` | CII | OK |
| 'iNfInItY' | OK | OK |
| 'inf' | OK | OK |
| `inf` + `d` | CII | OK |
| 'infinity' | OK | OK |
| `infinity` + `f` | CII | OK |
| 'nAn' | OK | OK |
| 'nan' | OK | OK |
| `nan` + `d` | CII | OK |

**Outside the letters-only class** (blank, padded, signed, digits, `0x1p0`, `2024`, `T00:00:00`, non-ASCII letters and
digits, control characters) the engines disagree on trimming, partial dates, year width and hex floats: `''` and `' '`
raise in Spark but cast to a value in RePark for `DOUBLE`; `'0x1p0'` is a `DOUBLE` in Spark and raises in RePark;
`'2024'` is a `TIMESTAMP_NTZ` in Spark and raises in RePark. None of these is in the table.

**The table the guard implements** (`spark_cast_raises`): a non-empty string of ASCII letters, cast to `TINYINT`,
`SMALLINT`, `INT`, `BIGINT`, `TIMESTAMP` or `TIMESTAMP_NTZ`, or to `FLOAT` or `DOUBLE` unless it is `nan`, `inf` or
`infinity` in any case, alone or with a `d` or `f` suffix. On all 83 measured strings for every listed type, Spark
and RePark's cast both raise `[CAST_INVALID_INPUT]`. The guard also runs RePark's cast on the value and refuses only
with that error, so it never refuses a value RePark's cast accepts. `DATE`, `DECIMAL` and `BOOLEAN` stay out: RePark's
cast raises a generic Arrow error there, not Spark's class.

## 3. Seat inventory

| Seat | `path:line` (head) | Why it exists | Guarded |
|---|---|---|---|
| Spark SQL router, `INSERT` dispatch | `crates/repark-spark/src/router.rs:465` | `D4InsertOverwrite`: every SQL `INSERT OVERWRITE` and the DataFrame writers' `INSERT OVERWRITE` statements (`insertInto` overwrite, `overwritePartitions`, `saveAsTable`'s "overwrite" statement) reach `execute_insert_overwrite` | dispatch only |
| Whole-table empty arm | `crates/repark-spark/src/insert_overwrite.rs:86-139` | probes emptiness (86), runs the existing W03 type check (106), then wipes (135) or, for a dynamic overwrite, returns without a commit (132) | yes, at 115, after W03 |
| Partition overwrite | `crates/repark-spark/src/insert_overwrite.rs:231-268` | static and dynamic `PARTITION` specs stage and commit even an empty source (268) | yes, at 232 |
| `writeTo(t).overwrite(cond)` | `crates/repark-spark/src/router/insert_positional/replace_where.rs:249-266` | `INSERT INTO ... REPLACE WHERE` over the `_repark_writer_v2_` view stages and commits by filter (266) | yes, at 249, only for that view |
| `saveAsTable` overwrite | `crates/repark-spark/src/ctas.rs:205` | an existing table is replaced by RTAS over the `_repark_writer_` view | yes, only for that view and an existing table |
| DataFrame views | `crates/repark-spark/src/view_ddl/frame_settings.rs:225` | `FrameView::get_logical_plan` inlines a DataFrame's own plan when its settings match the reader's; a frame built under other settings is a closed scan | read through, not changed |
| Existing W03 guard | `crates/repark-spark/src/insert_overwrite.rs:717`, `:793` | refuses an empty whole-table overwrite whose plan holds a cast `cast_may_fail_at_runtime` flags (misses the `DOUBLE`, `FLOAT`, `TIMESTAMP` and `TIMESTAMP_NTZ` cast functions) | unchanged |
| BY NAME / any-schema overwrite | `crates/repark-spark/src/insert_by_name.rs:140` | the by-name empty-overwrite wipe | no: not a listed door |
| `INSERT INTO`, CTAS create, SQL RTAS, `writeTo().replace()` / `createOrReplace()`, `MERGE`, `UPDATE`, `DELETE`, `COPY`, reads, `EXPLAIN`, the native door | `append_with_options.rs`, `ctas.rs`, `merge.rs`, `router/delete_update.rs`, `spark_ast.rs:191` | not overwrite doors | no |

Up: [../../map.md](../../map.md).
