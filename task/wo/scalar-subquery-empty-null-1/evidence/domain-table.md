# Step 0 — SCALAR-SUBQUERY-EMPTY-NULL-1 Domain Table, Seat Inventory, and Design

Model: `gemini-4-neon-xhigh`  
Base commit: `fa7d75e446e6f0a3edf48f6538c6a42c376515e7` (`main`)  
Main package: `main release wheel (`fa7d75e4`)` (`BUILT_FROM` = `fa7d75e446e6f0a3edf48f6538c6a42c376515e7`)  

## 1. Main Package and WS3 Probe Confirmation

- **`spark_ansi_false` (121 cells):** moved vs `e80e6bc8` = **0**; breakdown vs Spark 4.1.2: `{'NN_WITH_NULLS': 30, 'WRONG_VAL': 60, 'MATCH': 23, 'NULLABILITY_DIFF': 5, 'RP_ERR': 3}`.
- **`spark_ansi_true` (121 cells):** moved vs `e80e6bc8` = **0**; breakdown vs Spark 4.1.2: `{'NN_WITH_NULLS': 30, 'WRONG_VAL': 60, 'MATCH': 23, 'NULLABILITY_DIFF': 5, 'RP_ERR': 3}`.
- **`native` (`repark.sql`, 121 cells):** breakdown vs Spark 4.1.2 (DuckDB/Trino agree on NULL for empty scalar subqueries): `{'NN_WITH_NULLS': 16, 'WRONG_VAL': 60, 'MATCH': 42, 'RP_ERR': 3}`.
- **Breakdown of the 60 `WRONG_VAL` cells (identical in ANSI=false and ANSI=true):**
  - **57 cells** are caused directly by `SCALAR-SUBQUERY-EMPTY-NULL-1` (DataFusion `Expr::ScalarSubquery::nullable()` / `to_field()` reading `subquery.schema().field(0).is_nullable()` verbatim).
  - **3 cells** (`agg_grouping_sets_empty_isnull`, `agg_rollup_empty_isnull`, `agg_cube_empty_isnull`) return `True` in live Spark 4.1.2 because Spark returns **0 rows** for `GROUP BY ROLLUP/CUBE/GROUPING SETS (())` over an empty table, whereas DataFusion (`datafusion-physical-plan-54.1.0/src/aggregates/row_hash.rs:1150`, `init_empty_grouping_sets`), DuckDB, and Trino follow standard SQL and return **1 row (`0`)** so `0 IS NULL` is `False` even without constant folding. (`agg_groupby_b_rollup_b_isnull`, which has no empty grouping set, returns 0 rows in DataFusion too and **is** fixed by this unit.)
- **3 `RP_ERR` cells** (`corr_nonagg_where_isnull`, `corr_nonagg_select_val`, `corr_nonagg_coalesce`): pre-existing DataFusion unaggregated correlated scalar subquery without `LIMIT` rejection (`AnalysisException: Invalid (non-executable) plan after Analyzer`).
- **44 `NN_WITH_NULLS` cells** (`nonnull_with_nulls` on main, including 9 that already returned wrong values and 35 that returned the right value with an invalid `nullable=false` Arrow/DataFrame schema containing nulls).
- **Extra cells on main:**
  - `inline_utf8view_subq`: ERR: `PySparkException: Invalid argument error: Column 'r' is` (runtime Arrow panic/error: `Column 'r' is declared as non-nullable but contains null values`).
  - `df_api_scalar_isnull`: `[]` (empty instead of `[[1], [2]]`).
- **Physical uncorrelated scalar subquery (`enable_physical_uncorrelated_scalar_subquery=true`):** `tbl_isnull_select` = `[[False]]`, `tbl_isnull_where` = `[]`, `tbl_coalesce` = `[[1, None], [2, None]]`, `agg_count_having_isnull` = `[[False]]`, `agg_count_groupby_isnull` = `[[False]]` — all broken on main.
- **DML cells (11 cells):**
  - `dml_insert_where_isnull`: Spark `[[1, 100], [2, 200]]`, main `spark.sql` (ANSI F/T) `[]`, main `native` `[]`.
  - `dml_ctas_where_coalesce`: Spark `[[1, 42], [2, 42]]`, main `spark.sql` (ANSI F/T) `[]`, main `native` `[]`.
  - `dml_ctas_bare_subq`: Spark `[[1, None], [2, None]]`, main `[[1, None], [2, None]]` (nullability on created column is wrong on main).
  - `dml_delete_*` (3 cells), `dml_update_*` (3 cells): refused on main (`DELETE`/`UPDATE` with subquery unsupported — keep refusal).
  - `dml_merge_on_isnull`, `dml_merge_when_isnull`: Spark refuses with `[UNSUPPORTED_MERGE_CONDITION.SUBQUERY]`; main returns `[[1, 10], [2, 20]]` on Spark door and `UnsupportedOperationException` on native door (explicitly out of scope for this unit; record post-fix behavior without changing MERGE rules).

## 2. Domain Table (Step 0.3)

### 2A. WS3 Read Matrix (121 cells, ANSI=false & ANSI=true identical on main)

| Cell | Spark 4.1.2 (ANSI=false) | Main `spark.sql` (ANSI=false) | Main `spark.sql` (ANSI=true) | Main `repark.sql` (native) | Main status |
|---|---|---|---|---|---|
| `inline_bare_select` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_isnull_select` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_isnotnull_select` | `[[False]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `inline_isnull_where` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `inline_isnotnull_where` | `[]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` | `WRONG_VAL` |
| `inline_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `inline_ifnull` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `inline_nvl` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `inline_nvl2` | `[[1, 2], [2, 2]]` (null=[False, False]) | `[[1, 1], [2, 1]]` (null=[False, False]) | `[[1, 1], [2, 1]]` (null=[False, False]) | `[[1, 1], [2, 1]]` | `WRONG_VAL` |
| `inline_case_isnull` | `[[1, True], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` | `WRONG_VAL` |
| `inline_case_when_bool` | `[[1, 2], [2, 2]]` (null=[False, False]) | `[[1, 2], [2, 2]]` (null=[False, False]) | `[[1, 2], [2, 2]]` (null=[False, False]) | `[[1, 2], [2, 2]]` | `MATCH` |
| `inline_case_when_bool_out` | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `inline_is_distinct_from` | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` | `MATCH` |
| `inline_is_not_distinct_from` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `inline_eq_1` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `inline_not_eq_1` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `inline_eq_self` | `[[None]]` (null=[True]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `inline_or_not_self` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_and_not_self` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_bool_or_not_bool` | `[[None]]` (null=[True]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `inline_bool_and_not_bool` | `[[None]]` (null=[True]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_mul_zero` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_mul_zero_int32` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_mod_one` | `[[None]]` (null=[True]) | `[[0]]` (null=[True]) | `[[0]]` (null=[True]) | `[[0]]` | `WRONG_VAL` |
| `inline_bitand_zero` | `[[None]]` (null=[True]) | `[[0]]` (null=[False]) | `[[0]]` (null=[False]) | `[[0]]` | `WRONG_VAL` |
| `inline_like_pct` | `[[None]]` (null=[True]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `inline_not_like_pct` | `[[None]]` (null=[True]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_in_list` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `inline_not_in_list` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `inline_cast_int` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_cast_smallint` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `inline_cast_smallint_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `inline_abs_outer_proj` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['a']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['a']** | `[[None]]` | `NN_WITH_NULLS` |
| `inline_abs_from_src` | `[[1, None], [2, None]]` (null=[False, True]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` | `NN_WITH_NULLS` |
| `inline_cast_abs_smallint` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `inline_length` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `inline_limit1_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_limit1_where_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `inline_limit1_coalesce` | `[[42]]` (null=[False]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `inline_limit1_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `inline_lit_where_false_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `inline_lit_from_empty_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `tbl_bare_select` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `tbl_isnull_select` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `tbl_isnotnull_select` | `[[False]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `tbl_isnull_where` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `tbl_isnotnull_where` | `[]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` | `WRONG_VAL` |
| `tbl_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `tbl_ifnull` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `tbl_nvl` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `tbl_nvl2` | `[[1, 2], [2, 2]]` (null=[False, False]) | `[[1, 1], [2, 1]]` (null=[False, False]) | `[[1, 1], [2, 1]]` (null=[False, False]) | `[[1, 1], [2, 1]]` | `WRONG_VAL` |
| `tbl_case_isnull` | `[[1, True], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` | `WRONG_VAL` |
| `tbl_is_distinct_from` | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` | `MATCH` |
| `tbl_eq_1` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `tbl_not_eq_1` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `tbl_eq_self` | `[[None]]` (null=[True]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `tbl_mul_zero` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `tbl_like_pct` | `[[None]]` (null=[True]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `WRONG_VAL` |
| `tbl_in_list` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `tbl_cast_int` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `tbl_cast_smallint` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `tbl_cast_big_to_int` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `tbl_cast_big_to_int_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `tbl_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `tbl_abs_outer_proj` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['a']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['a']** | `[[None]]` | `NN_WITH_NULLS` |
| `tbl_abs_from_l` | `[[1, None], [2, None]]` (null=[False, True]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` | `NN_WITH_NULLS` |
| `tbl_length` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `tbl_limit1_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `tbl_limit1_coalesce` | `[[42]]` (null=[False]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `tbl_limit1_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `tbl_one_row_bare` | `[[5]]` (null=[True]) | `[[5]]` (null=[False]) | `[[5]]` (null=[False]) | `[[5]]` | `NULLABILITY_DIFF` |
| `tbl_one_row_isnull` | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `MATCH` |
| `agg_count_val` | `[[0]]` (null=[True]) | `[[0]]` (null=[False]) | `[[0]]` (null=[False]) | `[[0]]` | `NULLABILITY_DIFF` |
| `agg_count_isnull` | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `MATCH` |
| `agg_count_isnotnull` | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `MATCH` |
| `agg_count_coalesce` | `[[0]]` (null=[False]) | `[[0]]` (null=[False]) | `[[0]]` (null=[False]) | `[[0]]` | `MATCH` |
| `agg_count_having_val` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `agg_count_having_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_count_having_coalesce` | `[[42]]` (null=[False]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `agg_count_having_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `agg_count_plus1_having_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_sum_val` | `[[None]]` (null=[True]) | `[[None]]` (null=[True]) | `[[None]]` (null=[True]) | `[[None]]` | `MATCH` |
| `agg_sum_isnull` | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `MATCH` |
| `agg_max_val` | `[[None]]` (null=[True]) | `[[None]]` (null=[True]) | `[[None]]` (null=[True]) | `[[None]]` | `MATCH` |
| `agg_max_isnull` | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` (null=[False]) | `[[True]]` | `MATCH` |
| `agg_count_groupby_val` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `agg_count_groupby_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_count_groupby_where` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `agg_count_groupby_coalesce` | `[[42]]` (null=[False]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `agg_count_groupby_abs` | `[[None]]` (null=[True]) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None]]` | `NN_WITH_NULLS` |
| `agg_count_nonempty_zero_groups_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_count_limit0_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_count_offset1_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_coalesce_max_nohaving_isnull` | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `MATCH` |
| `agg_coalesce_max_having_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_grouping_sets_empty_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_rollup_empty_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_cube_empty_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_groupby_b_rollup_b_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `agg_select_lit_1_bare` | `[[1]]` (null=[True]) | `[[1]]` (null=[False]) | `[[1]]` (null=[False]) | `[[1]]` | `NULLABILITY_DIFF` |
| `corr_nonagg_where_isnull` | `[[1]]` (null=[False]) | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | `RP_ERR` |
| `corr_nonagg_select_val` | `[[1, None], [2, 20]]` (null=[False, True]) | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | `RP_ERR` |
| `corr_nonagg_coalesce` | `[[1, 42], [2, 20]]` (null=[False, False]) | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | ERR: `AnalysisException: Invalid (non-executable) plan after ` | `RP_ERR` |
| `corr_nonagg_limit1_where_isnull` | `[[1]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `corr_nonagg_limit1_select_isnull` | `[[1, True], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` | `WRONG_VAL` |
| `corr_nonagg_limit1_coalesce` | `[[1, 42], [2, 20]]` (null=[False, False]) | `[[1, None], [2, 20]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 20]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 20]]` **NN_WITH_NULLS:['v']** | `WRONG_VAL` |
| `corr_nonagg_limit1_abs` | `[[1, None], [2, 20]]` (null=[False, True]) | `[[1, None], [2, 20]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 20]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 20]]` | `NN_WITH_NULLS` |
| `corr_count_select_val` | `[[1, 0], [2, 1]]` (null=[False, True]) | `[[1, 0], [2, 1]]` (null=[False, False]) | `[[1, 0], [2, 1]]` (null=[False, False]) | `[[1, 0], [2, 1]]` | `NULLABILITY_DIFF` |
| `corr_count_where_isnull` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `corr_count_having_where_isnull` | `[[1]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `corr_count_having_select_val` | `[[1, None], [2, 1]]` (null=[False, True]) | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['c']** | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['c']** | `[[1, None], [2, 1]]` **NN_WITH_NULLS:['c']** | `NN_WITH_NULLS` |
| `corr_count_having_coalesce` | `[[1, 42], [2, 1]]` (null=[False, False]) | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 1]]` **NN_WITH_NULLS:['v']** | `WRONG_VAL` |
| `corr_count_having_eq0_val` | `[[1, 0], [2, None]]` (null=[False, True]) | `[[1, 0], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['c']** | `[[1, 0], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['c']** | `[[1, 0], [2, None]]` **NN_WITH_NULLS:['c']** | `NN_WITH_NULLS` |
| `corr_count_having_eq0_where_isnull` | `[[2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `corr_count_groupby_where_isnull` | `[[1]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `corr_count_groupby_coalesce` | `[[1, 42], [2, 1]]` (null=[False, False]) | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 1]]` (null=[False, False]) **NN_WITH_NULLS:['v']** | `[[1, None], [2, 1]]` **NN_WITH_NULLS:['v']** | `WRONG_VAL` |
| `corr_max_where_isnull` | `[[1]]` (null=[False]) | `[[1]]` (null=[False]) | `[[1]]` (null=[False]) | `[[1]]` | `MATCH` |
| `corr_max_coalesce` | `[[1, 42], [2, 20]]` (null=[False, False]) | `[[1, 42], [2, 20]]` (null=[False, False]) | `[[1, 42], [2, 20]]` (null=[False, False]) | `[[1, 42], [2, 20]]` | `MATCH` |
| `corr_coalesce_max_having_isnull` | `[[1, True], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` | `WRONG_VAL` |
| `corr_coalesce_max_nohaving_val` | `[[1, 0], [2, 20]]` (null=[False, True]) | `[[1, 0], [2, 20]]` (null=[False, False]) | `[[1, 0], [2, 20]]` (null=[False, False]) | `[[1, 0], [2, 20]]` | `NULLABILITY_DIFF` |

### 2B. WS3 DML, Extra, and `enable_physical_uncorrelated_scalar_subquery=true` Cells

| Cell | Spark 4.1.2 | Main `spark.sql` (ANSI=false) | Main `spark.sql` (ANSI=true) | Main `repark.sql` (native) |
|---|---|---|---|---|
| `dml_delete_tbl_isnull` | `[]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` |
| `dml_delete_tbl_isnotnull` | `[[1, 10], [2, 20]]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` |
| `dml_delete_inline_isnull` | `[]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` | ERR: `AnalysisException: Error during planning: DELETE with a` |
| `dml_update_tbl_isnull` | `[[1, 99], [2, 99]]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` |
| `dml_update_tbl_isnotnull` | `[[1, 10], [2, 20]]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` |
| `dml_update_inline_isnull` | `[[1, 99], [2, 99]]` (null=[False, False]) | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` | ERR: `AnalysisException: Error during planning: UPDATE with a` |
| `dml_merge_on_isnull` | ERR: `AnalysisException: [UNSUPPORTED_MERGE_CONDITION.SUBQUER` | `[[1, 10], [2, 20]]` (null=[False, False]) | `[[1, 10], [2, 20]]` (null=[False, False]) | ERR: `UnsupportedOperationException: This feature is not impl` |
| `dml_merge_when_isnull` | ERR: `AnalysisException: [UNSUPPORTED_MERGE_CONDITION.SUBQUER` | `[[1, 10], [2, 20]]` (null=[False, False]) | `[[1, 10], [2, 20]]` (null=[False, False]) | ERR: `UnsupportedOperationException: This feature is not impl` |
| `dml_insert_where_isnull` | `[[1, 100], [2, 200]]` (null=[False, False]) | `[]` (null=[False, False]) | `[]` (null=[False, False]) | `[]` |
| `dml_ctas_where_coalesce` | `[[1, 42], [2, 42]]` (null=[True, True]) | `[]` (null=[True, True]) | `[]` (null=[True, True]) | `[]` |
| `dml_ctas_bare_subq` | `[[1, None], [2, None]]` (null=[True, True]) | `[[1, None], [2, None]]` (null=[True, True]) | `[[1, None], [2, None]]` (null=[True, True]) | `[[1, None], [2, None]]` |
| `extra.inline_utf8view_subq` | (see extended table) | ERR: `PySparkException: Invalid argument error: Column 'r' is` | — | — |
| `extra.df_api_scalar_isnull` | (see extended table) | `[]` | — | — |
| `phys_uncorr.tbl_bare_select` | (matches `spark_ansi_false`) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | — | — |
| `phys_uncorr.tbl_isnull_select` | (matches `spark_ansi_false`) | `[[False]]` (null=[False]) | — | — |
| `phys_uncorr.tbl_isnull_where` | (matches `spark_ansi_false`) | `[]` (null=[False]) | — | — |
| `phys_uncorr.tbl_coalesce` | (matches `spark_ansi_false`) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | — | — |
| `phys_uncorr.tbl_abs` | (matches `spark_ansi_false`) | `[[None]]` (null=[False]) **NN_WITH_NULLS:['r']** | — | — |
| `phys_uncorr.agg_count_isnull` | (matches `spark_ansi_false`) | `[[False]]` (null=[False]) | — | — |
| `phys_uncorr.agg_count_having_isnull` | (matches `spark_ansi_false`) | `[[False]]` (null=[False]) | — | — |
| `phys_uncorr.agg_count_groupby_isnull` | (matches `spark_ansi_false`) | `[[False]]` (null=[False]) | — | — |

### 2C. Extended Step 0.3 SQL, CTE, Prepared, and View Cells (measured on live Spark 4.1.2)

| Cell | Spark 4.1.2 (ANSI=false) | Main `spark.sql` (ANSI=false) | Main `spark.sql` (ANSI=true) | Main `repark.sql` (native) | Main status |
|---|---|---|---|---|---|
| `cte_with_scalar_subq_isnull` | `[[1, True, 42], [2, True, 42]]` (null=[False, False, False]) | `[[1, False, None], [2, False, None]]` (null=[False, False, False]) **NN_WITH_NULLS:['v']** | `[[1, False, None], [2, False, None]]` (null=[False, False, False]) **NN_WITH_NULLS:['v']** | `[[1, False, None], [2, False, None]]` **NN_WITH_NULLS:['v']** | `WRONG_VAL` |
| `cte_with_scalar_subq_where_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `cte_empty_in_scalar_subq_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `cte_empty_in_scalar_subq_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `cte_nested_scalar_subq_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `cte_count_star_control` | `[[0, False]]` (null=[True, False]) | `[[0, False]]` (null=[False, False]) | `[[0, False]]` (null=[False, False]) | `[[0, False]]` | `NULLABILITY_DIFF` |
| `union_with_empty_subq` | `[[None], [1]]` (null=[True]) | `[[None], [1]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None], [1]]` (null=[False]) **NN_WITH_NULLS:['r']** | `[[None], [1]]` **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `cast_date_subq_to_ts_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `cast_date_subq_to_ts_val` | `[[None]]` (null=[True]) | ERR: `PySparkException: Arrow error: Invalid argument error: ` | ERR: `PySparkException: Arrow error: Invalid argument error: ` | `[[None]]` **NN_WITH_NULLS:['r']** | `RP_ERR` |
| `nested_scalar_subq_isnull` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `nested_scalar_subq_outer_empty` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `WRONG_VAL` |
| `in_list_single_scalar_subq` | `[]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `MATCH` |
| `scalar_subq_in_having` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `scalar_subq_in_join_on` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `[]` | `WRONG_VAL` |
| `scalar_subq_lit_control` | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `MATCH` |
| `scalar_subq_values_control` | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` (null=[False]) | `[[False]]` | `MATCH` |
| `prepared.prep_isnull_select` | `[[True]]` (null=[False]) | `[[False]]` (null=[False]) | — | `[[False]]` | `WRONG_VAL` |
| `prepared.prep_isnull_where` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | — | `[]` | `WRONG_VAL` |
| `prepared.prep_coalesce_param` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, True]) | — | `[[1, None], [2, None]]` | `WRONG_VAL` |
| `prepared.prep_bare` | `[[None]]` (null=[True]) | `[[None]]` (null=[True]) | — | `[[None]]` | `MATCH` |
| `native_view.native_view_subq_select` | rows=`[[1, None, True, 42], [2, None, True, 42]]` | — | — | `[[1, None, False, None], [2, None, False, None]]` **NN_WITH_NULLS:['r', 'c']** | `WRONG_VAL/NULLABILITY` |
| `native_view.native_view_subq_where_isnull` | rows=`[[1], [2]]` | — | — | `[]` | `WRONG_VAL/NULLABILITY` |
| `native_view.native_view_describe` | `r` nullable=`YES` (DuckDB/Trino) | — | — | `[['id', 'Int32', 'NO'], ['r', 'Int32', 'NO']]` | `WRONG_VAL/NULLABILITY` |

### 2D. Extended Step 0.3 DataFrame Door Cells (`F.expr`, `filter(str)`, `selectExpr`, `withColumn`, `filter(Column)`, `.scalar()`, Temp Views, Cache)

| Cell | Spark 4.1.2 (ANSI=false) | Main DF Door (ANSI=false) | Main DF Door (ANSI=true) | Main status |
|---|---|---|---|---|
| `fexpr_withcol_isnull` | `[[1, True], [2, True]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `WRONG_VAL` |
| `fexpr_withcol_isnotnull` | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, True], [2, True]]` (null=[False, False]) | `[[1, True], [2, True]]` (null=[False, False]) | `WRONG_VAL` |
| `fexpr_withcol_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `fexpr_withcol_bare` | `[[1, None], [2, None]]` (null=[False, True]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `fexpr_withcol_cast_date_ts` | `[[1, None], [2, None]]` (null=[False, True]) | ERR: `PySparkException: Arrow error: Invalid argument error: ` | ERR: `PySparkException: Arrow error: Invalid argument error: ` | `RP_ERR` |
| `fexpr_withcol_case_isnull` | `[[1, True], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `WRONG_VAL` |
| `fexpr_withcol_coalesce_id` | `[[1, 1], [2, 2]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `fexpr_withcol_count_star_control` | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `MATCH` |
| `fexpr_filter_col_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `WRONG_VAL` |
| `fexpr_filter_col_isnotnull` | `[]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `WRONG_VAL` |
| `fexpr_filter_col_tbl_isnull` | `[[1], [2]]` (null=[False]) | ERR: `AnalysisException: Error during planning: table 'ice.ns` | ERR: `AnalysisException: Error during planning: table 'ice.ns` | `RP_ERR` |
| `filter_str_inline_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `WRONG_VAL` |
| `filter_str_inline_isnotnull` | `[]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `WRONG_VAL` |
| `filter_str_tbl_isnull` | `[[1], [2]]` (null=[False]) | ERR: `AnalysisException: Error during planning: table 'ice.ns` | ERR: `AnalysisException: Error during planning: table 'ice.ns` | `RP_ERR` |
| `filter_str_tbl_isnotnull` | `[]` (null=[False]) | ERR: `AnalysisException: Error during planning: table 'ice.ns` | ERR: `AnalysisException: Error during planning: table 'ice.ns` | `RP_ERR` |
| `filter_str_count_having_isnull` | `[[1], [2]]` (null=[False]) | ERR: `AnalysisException: Error during planning: table 'ice.ns` | ERR: `AnalysisException: Error during planning: table 'ice.ns` | `RP_ERR` |
| `filter_str_count_star_control` | `[]` (null=[False]) | ERR: `AnalysisException: Error during planning: table 'ice.ns` | ERR: `AnalysisException: Error during planning: table 'ice.ns` | `RP_ERR` |
| `selectexpr_inline_isnull` | `[[1, True], [2, True]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `WRONG_VAL` |
| `selectexpr_inline_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `selectexpr_tbl_isnull` | `[[1, True], [2, True]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `WRONG_VAL` |
| `selectexpr_tbl_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `selectexpr_tbl_bare` | `[[1, None], [2, None]]` (null=[False, True]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `withcol_scalar_isnull` | `[[1, True], [2, True]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `[[1, False], [2, False]]` (null=[False, False]) | `WRONG_VAL` |
| `withcol_scalar_coalesce` | `[[1, 42], [2, 42]]` (null=[False, False]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `WRONG_VAL` |
| `withcol_scalar_bare` | `[[1, None], [2, None]]` (null=[False, True]) | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `[[1, None], [2, None]]` (null=[False, False]) **NN_WITH_NULLS:['r']** | `NN_WITH_NULLS` |
| `filter_scalar_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `WRONG_VAL` |
| `filter_scalar_isnotnull` | `[]` (null=[False]) | `[[1], [2]]` (null=[False]) | `[[1], [2]]` (null=[False]) | `WRONG_VAL` |
| `tempview_sql_subq_select` | `[[1, None, True, 42], [2, None, True, 42]]` (null=[False, True, False, False]) | `[[1, None, False, None], [2, None, False, None]]` (null=[False, False, False, False]) **NN_WITH_NULLS:['r', 'c']** | `[[1, None, False, None], [2, None, False, None]]` (null=[False, False, False, False]) **NN_WITH_NULLS:['r', 'c']** | `WRONG_VAL` |
| `tempview_sql_subq_where_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `WRONG_VAL` |
| `tempview_df_subq_select` | `[[1, None, True, 42], [2, None, True, 42]]` (null=[False, True, False, False]) | `[[1, None, False, None], [2, None, False, None]]` (null=[False, False, False, False]) **NN_WITH_NULLS:['r', 'c']** | `[[1, None, False, None], [2, None, False, None]]` (null=[False, False, False, False]) **NN_WITH_NULLS:['r', 'c']** | `WRONG_VAL` |
| `tempview_df_subq_where_isnull` | `[[1], [2]]` (null=[False]) | `[]` (null=[False]) | `[]` (null=[False]) | `WRONG_VAL` |
| `tempview_df_ansi_flip` | `[[1, True, 42], [2, True, 42]]` (null=[False, False, False]) | `[[1, False, None], [2, False, None]]` (null=[False, False, False]) **NN_WITH_NULLS:['c']** | `[[1, False, None], [2, False, None]]` (null=[False, False, False]) **NN_WITH_NULLS:['c']** | `WRONG_VAL` |
| `cached_subq_where_isnull` | `[[1, None], [2, None]]` (null=[False, True]) | `[]` (null=[False, False]) | `[]` (null=[False, False]) | `WRONG_VAL` |
| `spark_explode_empty_isnull` | `[[True]]` (null=[False]) | ERR: `AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve ` | ERR: `AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve ` | `RP_ERR` |

Notes on 5 pre-existing non-S1 errors in Table 2D:
- `fexpr_filter_col_tbl_isnull` and `filter_str_tbl_*` / `filter_str_count_*` (5 cells): fail on main with `table 'ice.ns.empty_nn' not found` because `F.expr` parses inside a static `EXPR_CONTEXT` (no session catalog) and DataFrame `filter(str)` uses DataFusion's `create_logical_expr` empty-table context provider. Inline subqueries in `F.expr`, `filter(Column from F.expr)`, and `filter(str)` parse cleanly, reproduce the S1 defect on main, and are fixed by this unit; `selectExpr` and `.scalar()` work on BOTH inline and catalog-table subqueries.
- `spark_explode_empty_isnull`: fails on main with `[UNRESOLVED_ROUTINE] Cannot resolve routine explode` (bare `explode(...)` in `SELECT` without `LATERAL VIEW` is not registered as a plain SQL scalar routine; `unnest(...)` is, and `Projection` row-guarantee checks both `Expr::Unnest` and generator UDFs).

## 3. Seat Inventory (Step 0.4)

Property under audit: **"an empty scalar subquery's value may be NULL (even when its projected column is `NOT NULL`), unless the subquery provably produces at least one row."**

### Seat 1: Session analyzer list (`crates/repark-core/src/session/df_guards.rs`)
- **Seen at the seat on main:** **No**, because DataFusion 54.1.0's built-in analyzer list (`inline_table_scan`, `expand_wildcard_rule`, `resolve_grouping_function`, `type_coercion`, `function_registry`) has no scalar-subquery nullability rule, and neither `SparkExtension::configure_analyzer_rules` nor `DataFusionDialect` / `SparkDialect` adds one.
- **Seen at the seat after fix:** **Yes**, at index immediately before `type_coercion` (`insert_scalar_subquery_nullability_before_coercion`), wired inside `context_with_df_54_1_rule_guards` **after** `extension.configure_analyzer_rules` and `dialect.configure_analyzer_rules` (and before `FrameViewSeat` is inserted at index 0).
  - Exact rule order on Spark door: `FrameViewSeat` (0), `inline_table_scan`, `expand_wildcard_rule`, `resolve_grouping_function`, `repark_spark_integral_literal`, `repark_decimal_float_widen`, `repark_struct_position`, `repark_map_subscript`, `repark_spark_string_integral_cast`, `repark_spark_higher_order_preparation`, **`repark_scalar_subquery_nullability`**, `type_coercion`, `function_registry`, `repark_spark_nullability`, `repark_spark_cast_eval`, `repark_spark_generator_rewrite`, `repark_spark_lambda_rebind`, `repark_spark_struct_setop`, `repark_spark_type_coercion`.
  - Exact rule order on Native door: `FrameViewSeat` (0), `inline_table_scan`, `expand_wildcard_rule`, `resolve_grouping_function`, **`repark_scalar_subquery_nullability`**, `type_coercion`, `function_registry`.
  - **Why after extension/dialect pre-coercion rules:** `higher_order_preparation` (`crates/repark-functions/src/lambda_rebind.rs:243`) inspects `projection.expr[0]` of scalar subqueries for `make_array` constructor nullability; `spark_integral_literal`, `decimal_float_widen`, `struct_position`, `map_subscript`, and `spark_string_integral_cast` inspect unwrapped expressions and none of them fold on `!nullable`.
  - **Why before `type_coercion`:** `TypeCoercion` computes `UNION` output nullability (`datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs:1247`) and `Utf8View -> LargeUtf8` nullability (`type_coercion.rs:835`) from child fields. All post-coercion rules — especially `SparkNullability::nonnull_spark_cast` (which wraps `CAST(nonnull_date_subq AS TIMESTAMP)` in `__repark_spark_nonnull__` and crashes at runtime on main!) — run after `type_coercion` and must see `nullable = true`.

### Seat 2: Every other production analyzer list (especially `build_expr_context` in `crates/repark-python/src/column/expr_build.rs`)
- Audited the entire repository for `Analyzer::new`, `with_analyzer_rules`, `SessionStateBuilder::new`, `SessionContext::new`, and `context_with_df_54_1_rule_guards`:
  1. `crates/repark-python/src/column/expr_build.rs:325` (`build_expr_context`): constructs a standalone `SessionContext` (`EXPR_CONTEXT`) for `F.expr(...)` with its own analyzer list (including `SparkNullability`).
     - **Seen at the seat on main:** **No**. On main, `fexpr_withcol_cast_date_ts` crashes at runtime because `SparkNullability` inside `build_expr_context` sees `nullable = false` and wraps the cast in `__repark_spark_nonnull__`, and `F.expr("(SELECT 1 WHERE FALSE) IS NULL")` folds to `false`.
     - **Seen at the seat after fix:** **Yes**. `build_expr_context` calls `repark_core::insert_scalar_subquery_nullability_before_coercion(rules)?` immediately before `type_coercion` (right after `insert_string_integral_before_coercion`) and registers `repark_core::scalar_nullable_udf()`.
  2. `crates/repark-spark/src/view_ddl/temp_settings.rs:64` (`SessionStateBuilder::new().with_default_features()`): only parses `SET` / `RESET` session settings; never plans queries or subqueries. **Seen: No, because** it does not plan expressions or subqueries.
  3. `crates/repark-core/src/session/temp_views.rs`, `lazy_cache.rs`, `crates/repark-spark/src/view_ddl/temp_view.rs`, `crates/repark-python/src/session_runtime.rs`: all build sessions through `Session::new` -> `context_with_df_54_1_rule_guards` (covered by Seat 1).

### Seat 3: DataFusion's off-list runs (`apply_type_coercion` in `create_view` and bare optimizer in `PREPARE`)
Verified in pinned DataFusion 54.1.0 (`pinned crate source datafusion-54.1.0/src/execution/context/mod.rs`):
1. **`Statement::Prepare` (`context/mod.rs:734-764`):** calls `self.state().optimizer().optimize(input, ...)` directly **without** running the session analyzer list!
   - **Seen at the seat on main:** **No** on both doors (Spark door `spark_ast.rs:277` calls `analyze_eagerly` before `execute_logical_plan`, which traverses `Prepare.input` via `LogicalPlan::map_children`, so Spark door will see Seat 1; **Native door** in `crates/repark-core/src/pre_execute.rs:96-107` analyzes a **clone** of `prepare.input` and discards it, handing the unanalyzed `Prepare` plan directly to DataFusion's bare optimizer!).
   - **Seen at the seat after fix:** **Yes**, via **two** layers (belt and suspenders):
     - `ReparkScalarSubqueryNullability` implements **both** `AnalyzerRule` and `OptimizerRule`, seated in `unnest_safe_optimizer_rules` immediately before `simplify_expressions` (zero-cost no-op when already wrapped by the analyzer, guarantees even a bare optimizer run like DataFusion's `Prepare` arm never folds an unwrapped scalar subquery).
     - `PreExecute::execute` in `crates/repark-core/src/pre_execute.rs` replaces `prepare.input` with the analyzed plan before calling `ctx.execute_logical_plan`, so Native door `PREPARE` gets full analyzer coverage too.
2. **`DdlStatement::CreateView` (`context/mod.rs:931-973`):** calls `apply_type_coercion` (`Analyzer::with_rules(vec![TypeCoercion::new()])`) off-list before constructing `ViewTable` (`datafusion-catalog-54.1.0/src/view.rs`), bypassing the session analyzer list.
   - **Seen at the seat on main:** **No**. On Native door, `CREATE VIEW v AS SELECT id, (SELECT b FROM empty_nn) AS r FROM l_nn` stores `r` as `nullable=false` in `ViewTable.table_schema` (`DESCRIBE v` prints `NO`), even though querying `v` later inlines `ViewTable.logical_plan` into the outer query (`builder.rs:519`).
   - **Seen at the seat after fix:** **Yes**. `PreExecute::execute` runs `ReparkScalarSubqueryNullability` on `view.input` for `DdlStatement::CreateView` before `ctx.execute_logical_plan`, so `apply_type_coercion` sees the nullable wrapper and `ViewTable.table_schema` (`DESCRIBE v`) records `nullable = true` (`YES`).

### Seat 4: SQL-planner-time coercions (`datafusion-sql-54.1.0`)
- Verified `datafusion-sql-54.1.0/src/expr/subquery.rs:93` (`parse_scalar_subquery`) and `statement.rs` (`cast_to`):
  - **Seen at the seat:** **No (not needed — does not fold)**, because `parse_scalar_subquery` builds raw `Expr::ScalarSubquery(Subquery { subquery, outer_ref_columns, spans })` without querying `.nullable()` or folding predicates, and `statement.rs` only inserts `Expr::Cast` nodes (which preserve their child's nullability once wrapped in Seat 1 before `TypeCoercion`).

### Seat 5: Optimizer rules that fold or move the evidence (`datafusion-optimizer-54.1.0`)
Verified in `crates/repark-core/src/session/df_guards.rs` (`unnest_safe_optimizer_rules`) and DataFusion 54.1.0 sources:
1. **`SimplifyExpressions`** (`simplify_expressions/expr_simplifier.rs:616-619`, pass 0, before `ScalarSubqueryToJoin`):
   - **Seen at the seat on main:** **No** — folds `!nullable` (`IS NULL -> false`, `IS NOT NULL -> true`, drops `coalesce`/`nvl`/`ifnull` fallbacks, folds `CASE`, `x = x`, `LIKE '%'`, `% 1`, `& 0`, etc.). Also const-evaluates `Immutable`/`Stable` scalar UDFs on literals (`(SELECT 1 FROM empty_nn)`).
   - **Seen at the seat after fix:** **Yes** — `ReparkScalarSubqueryNullability` wraps the subquery's output expression in `Volatility::Volatile` UDF `__repark_scalar_nullable__(x)` whose `return_field_from_args` sets `nullable = true`. `Expr::ScalarSubquery::nullable()` and `to_field()` now return `true`, and `Volatile` prevents literal const-evaluation of the wrapper in Pass 0.
2. **`ScalarSubqueryToJoin`** (`scalar_subquery_to_join.rs`, after `SimplifyExpressions`):
   - **Seen at the seat after fix:** **Yes** — lowers scalar subqueries to `JoinType::Left` (which unconditionally calls `nullify_fields(right)` -> `nullable = true`). Audited `PullUpCorrelatedExpr` and `evaluates_to_null` (`decorrelate.rs`): zero volatility checks block decorrelation; both correlated and uncorrelated subqueries decorrelate identically to main.
3. **`StripScalarNullable`** (new rule, immediately after `scalar_subquery_to_join`):
   - **Seen at the seat after fix:** **Yes** — recursively strips `__repark_scalar_nullable__(x) -> x` outside surviving `Expr::ScalarSubquery` / `Exists` / `InSubquery` / `SetComparison` nodes, preserving output column names with `NamePreserver` and calling `recompute_schema()` up every ancestor whenever expressions or children changed.
   - Because `Left Join::recompute_schema()` keeps right-side fields `nullable = true`, every downstream rule sees `nullable = true` natively with **zero** volatile wrappers left in the plan!
   - When `enable_physical_uncorrelated_scalar_subquery = true`, `ScalarSubqueryToJoin` leaves `Expr::ScalarSubquery` un-lowered; `StripScalarNullable` does not descend into surviving `Expr::ScalarSubquery`, so the wrapper survives through Pass 1+ `SimplifyExpressions` and `physical-expr/src/planner.rs:416` (`ScalarSubquery` physical execution), fixing all 8 `phys_uncorr` cells too!
4. **`DecorrelatePredicateSubquery`:**
   - **Seen at the seat after fix:** **Yes** — scalar subqueries nested inside `EXISTS` / `IN` predicates were already wrapped by Seat 1 (and stripped once lowered, or kept wrapped if inside a surviving predicate subquery).
5. **`EliminateOuterJoin`, `PushDownFilter`, `OptimizeProjections`, `CommonSubexprEliminate`:**
   - **Seen at the seat after fix:** **Yes** — because `StripScalarNullable` runs right after `scalar_subquery_to_join`, all downstream optimizer passes see clean, non-volatile expressions with true `nullable = true` join-derived nullability:
     - `EliminateOuterJoin` still converts `Left Join` -> `Inner`/`Cross Join` on null-rejecting predicates like `WHERE (SELECT b FROM empty_nn) = 1`.
     - `PushDownFilter` still pushes filters into Iceberg/Parquet scans unchanged.
     - `OptimizeProjections` still prunes unused columns unchanged.
     - `CommonSubexprEliminate` is not blocked by any lingering `Volatile` UDF.

### Seat 6: Stored plans re-entering (views, prepared plans, cached tables, DataFrame temp views)
1. **Spark SQL temp views (`ReplanningTempView`, `crates/repark-spark/src/view_ddl/temp_view.rs`):** **Seen: Yes**, plans SQL through the session context (`analyze_eagerly` invokes Seat 1), storing `nullable = true` schema.
2. **DataFrame temp views (`FrameView`, `crates/repark-spark/src/view_ddl/frame_settings.rs`):** **Seen: Yes**, same-settings scans inline the logical plan into the outer query (Seat 1 runs on the outer plan), and cross-settings scans (`tempview_df_ansi_flip`) run `found.analyzer` in `closed_scan` (Seat 1).
3. **Cached tables (`LazyCacheTable::try_new`, `crates/repark-core/src/session/temp_views/lazy_cache.rs:235`):** **Seen: Yes**, explicitly runs `state.analyzer().execute_and_check` (Seat 1) before capturing `stored_schema`.
4. **Native `CREATE VIEW` (`ViewTable`):** **Seen: Yes**, wrapped at DDL time via `PreExecute::execute` (Seat 3) AND inlined on read (Seat 1).
5. **Prepared statements (`Statement::Prepare`):** **Seen: Yes**, wrapped via `PreExecute::execute` (Seat 3) AND `ReparkScalarSubqueryNullability` `OptimizerRule` (Seat 5) AND `spark_ast.rs:277` `analyze_eagerly` on Spark door.

## 4. Design Choice (Step 0.5)

### Candidates evaluated against DataFusion 54.1.0 sources

| Candidate | Mechanism | Keeps | Loses | Verdict |
|---|---|---|---|---|
| **Option A+ (refined — chosen)** | `ReparkScalarSubqueryNullability` (`AnalyzerRule` before `type_coercion` + `OptimizerRule` before `simplify_expressions` + `PreExecute` `Prepare`/`CreateView` + `build_expr_context`) wraps single-column non-null scalar subqueries lacking `guaranteed_at_least_one_row` in `Volatile` UDF `__repark_scalar_nullable__` (with `schema_name` passthrough); `StripScalarNullable` right after `scalar_subquery_to_join` removes wrappers outside surviving subqueries and calls `recompute_schema()` up ancestors. | 100% of DataFusion 54.1.0 untouched (no fork); fixes all 57 S1 ws3 cells + `Utf8View` crash + `CAST(DATE AS TIMESTAMP)` crash + `enable_physical_uncorrelated_scalar_subquery=true` + all DF/`F.expr`/tempview/cache/CTE/PREPARE/VIEW/DML cells on both doors; preserves constant folding on `COUNT(*)`, global aggregates, `(SELECT 1)`, non-empty `VALUES`; preserves decorrelation, `EliminateOuterJoin`, filter pushdown, projection pruning, CSE; zero wrapper leaked into physical plans or column names; zero change to non-subquery plans. | Adds one lightweight analyzer/optimizer pass (fast-path O(plan nodes) check with zero allocation when no scalar subquery is present). | **CHOSEN** |
| **Option A (naive — `report.md` §3.2 verbatim without refinements)** | Analyzer-only rule + strip rule without `Sort.fetch`, `Projection` generator/Unnest guard, `Prepare`/`CreateView` seats, or ancestor `recompute_schema`. | Most SQL read cells. | **5 concrete defects found in DataFusion 54.1.0 verification:** (1) `Sort { fetch: Some(0) }` misclassified as >=1 row; (2) `Projection` with `Expr::Unnest` or generator UDF over `EmptyRelation { produce_one_row: true }` misclassified as >=1 row; (3) `GroupingSet::Rollup`/`Cube`/`GroupingSets` exemption unsafe against Spark empty-table semantics; (4) Native `PREPARE` & `CREATE VIEW` (`DESCRIBE`) bypass analyzer; (5) missing `recompute_schema` on ancestor nodes (`SubqueryAlias`/`Join`/`Projection`) after stripping risks stale schema invariants. | Rejected — superseded by refined Option A+ |
| **Option B (no strip; leave `Volatile` wrapper in plan)** | Wrap in `__repark_scalar_nullable__` and never strip. | Simpler code (no strip rule). | Blocks `EliminateOuterJoin`, `PushDownFilter`, and `CommonSubexprEliminate` after `ScalarSubqueryToJoin`, and leaks a synthetic UDF into every `EXPLAIN` and physical plan. | Rejected — violates plan quality |
| **Option C (disable Pass-0 `SimplifyExpressions` or run `ScalarSubqueryToJoin` before `SimplifyExpressions`)** | Reorder DataFusion optimizer passes so `ScalarSubqueryToJoin` precedes `SimplifyExpressions`. | No synthetic UDF. | Does **not** fix `TypeCoercion` (`UNION`, `Utf8View`), does **not** fix post-coercion `SparkNullability` (`CAST(DATE AS TIMESTAMP)` crash!), does **not** fix `enable_physical_uncorrelated_scalar_subquery=true`, does **not** fix `CREATE VIEW` schema nullability, and breaks `SimplifyExpressions` pre-decorrelation pruning on subqueries (`WHERE FALSE`). | Rejected — incomplete & regresses plans |

### Refinements verified against DataFusion 54.1.0 sources
1. **`Sort.fetch` check:** `LogicalPlan::Sort(s)` has `fetch: Option<usize>` (`datafusion-expr-54.1.0/src/logical_plan/plan.rs`). `guaranteed_at_least_one_row` requires `s.fetch.is_none_or(|n| n >= 1) && guaranteed_at_least_one_row(&s.input)`.
2. **`Projection` unnest/generator guard:** `LogicalPlan::Projection(p)` before `GeneratorRewrite` can contain `Expr::Unnest(_)` or generator UDFs (`explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`, `unnest`, `__repark_gen_alias`), which can emit 0 rows on empty arrays even over `EmptyRelation { produce_one_row: true }`. `guaranteed_at_least_one_row` rejects any `Projection` containing those expressions.
3. **Grouping sets conservatism:** `LogicalPlan::Aggregate(a)` returns `true` **only** when `a.group_expr.is_empty()` (ungrouped global aggregates like `COUNT(*)` without `GROUP BY`). `GroupingSet::Rollup`, `Cube`, and `GroupingSets` are conservatively marked nullable.
4. **Full `Limit` literal width support:** `limit_keeps_at_least_one_row` decodes `Int8/16/32/64` and `UInt8/16/32/64` literal expressions for `skip` and `fetch` (requiring `skip == 0` and `fetch.is_none_or(|n| n >= 1)`).
5. **Dual Analyzer + Optimizer seat + `PreExecute` + `recompute_schema`:** Covers `PREPARE`, `CREATE VIEW`, `F.expr`, `enable_physical_uncorrelated_scalar_subquery=true`, and keeps DataFusion plan schema invariants strictly valid across every ancestor node.

### Plan-stability check (no HALT needed)
- Baseline `EXPLAIN` plans captured in `baseline EXPLAIN recording on `fa7d75e44`` for 5 non-subquery queries (`nosubq_1_filter_proj`, `nosubq_2_agg_group`, `nosubq_3_inner_join`, `nosubq_4_window`, `nosubq_5_coalesce_case`), plus `count_star_isnull`, `count_star_where_isnull`, `lit_1_isnull`, and `subq_eq_1_outer_join_elim`.
- All 5 non-subquery queries hit the fast-path `has_candidate_scalar_subquery == false` and undergo zero transformation.
- `count_star_isnull`, `count_star_where_isnull`, and `lit_1_isnull` satisfy `guaranteed_at_least_one_row == true` and keep their exact constant-folded plans (`Projection: Boolean(false)` / `EmptyRelation: rows=0`).
- `subq_eq_1_outer_join_elim` strips the wrapper right after `scalar_subquery_to_join` so `EliminateOuterJoin` still converts `Left Join` -> `Cross Join` with `Filter: b = 1`.
- Conclusion: **Proceed to build (no HALT).**
