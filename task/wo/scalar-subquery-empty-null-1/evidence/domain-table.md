# SCALAR-SUBQUERY-EMPTY-NULL-1 — Domain Table (Main vs Head vs Spark 4.1.2)

Base: `fa7d75e446e6f0a3edf48f6538c6a42c376515e7` (`main`), merged with `ed637ffb7250b4d1ada60cb52cd7e7d8dffa4aa2` (`main`) in Delta 1  
Model: `gemini-4-neon-xhigh`  
Date: 2026-11-23 (Delta 1 updated 2026-11-24)  

## 1. WS3 Read Matrix (121 cells; ANSI=false and ANSI=true identical on main and head)

Summary on HEAD vs Spark 4.1.2: **57/57** S1 cells fixed in each ANSI mode and on native `repark.sql()`, **0 regressions**, **0 `NONNULL_WITH_NULLS`** (down from 44). Remaining 3 value diffs are empty-table `GROUP BY ROLLUP`/`CUBE`/`GROUPING SETS(())` (DataFusion/DuckDB/Trino return 1 row per SQL standard vs Spark 0 rows); remaining 3 errors are pre-existing unaggregated correlated non-LIMIT rejections.

| Cell | Spark 4.1.2 | Main `spark.sql` | Head `spark.sql` (ANSI F/T) | Main `repark.sql` | Head `repark.sql` | Delta |
|---|---|---|---|---|---|---|
| `inline_bare_select` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_isnull_select` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `inline_isnotnull_select` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `FIXED_VAL` |
| `inline_isnull_where` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `FIXED_VAL` |
| `inline_isnotnull_where` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `FIXED_VAL` |
| `inline_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `inline_ifnull` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `inline_nvl` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `inline_nvl2` | `[[1, 2], [2, 2]]` | `[[1, 1], [2, 1]]` | `[[1, 2], [2, 2]]` | `[[1, 1], [2, 1]]` | `[[1, 2], [2, 2]]` | `FIXED_VAL` |
| `inline_case_isnull` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `FIXED_VAL` |
| `inline_case_when_bool` | `[[1, 2], [2, 2]]` | `[[1, 2], [2, 2]]` | `[[1, 2], [2, 2]]` | `[[1, 2], [2, 2]]` | `[[1, 2], [2, 2]]` | `KEPT` |
| `inline_case_when_bool_out` | `[[1, False], [2, False]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, False], [2, False]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, False], [2, False]]` | `FIXED_VAL` |
| `inline_is_distinct_from` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `KEPT` |
| `inline_is_not_distinct_from` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `inline_eq_1` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `inline_not_eq_1` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `inline_eq_self` | `[[None]]` | `[[True]]` | `[[None]]` | `[[True]]` | `[[None]]` | `FIXED_VAL` |
| `inline_or_not_self` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_and_not_self` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_bool_or_not_bool` | `[[None]]` | `[[True]]` | `[[None]]` | `[[True]]` | `[[None]]` | `FIXED_VAL` |
| `inline_bool_and_not_bool` | `[[None]]` | `[[False]]` | `[[None]]` | `[[False]]` | `[[None]]` | `FIXED_VAL` |
| `inline_mul_zero` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_mul_zero_int32` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_mod_one` | `[[None]]` | `[[0]]` | `[[None]]` | `[[0]]` | `[[None]]` | `FIXED_VAL` |
| `inline_bitand_zero` | `[[None]]` | `[[0]]` | `[[None]]` | `[[0]]` | `[[None]]` | `FIXED_VAL` |
| `inline_like_pct` | `[[None]]` | `[[True]]` | `[[None]]` | `[[True]]` | `[[None]]` | `FIXED_VAL` |
| `inline_not_like_pct` | `[[None]]` | `[[False]]` | `[[None]]` | `[[False]]` | `[[None]]` | `FIXED_VAL` |
| `inline_in_list` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `inline_not_in_list` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `inline_cast_int` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_cast_smallint` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `inline_cast_smallint_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `inline_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `inline_abs_outer_proj` | `[[None]]` | `[[None]]` (NN:['a']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `inline_abs_from_src` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `FIXED_SCHEMA` |
| `inline_cast_abs_smallint` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `inline_length` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `inline_limit1_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `inline_limit1_where_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `FIXED_VAL` |
| `inline_limit1_coalesce` | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `FIXED_VAL` |
| `inline_limit1_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `inline_lit_where_false_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `inline_lit_from_empty_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `tbl_bare_select` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_isnull_select` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `tbl_isnotnull_select` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `FIXED_VAL` |
| `tbl_isnull_where` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `FIXED_VAL` |
| `tbl_isnotnull_where` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `FIXED_VAL` |
| `tbl_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `tbl_ifnull` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `tbl_nvl` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `FIXED_VAL` |
| `tbl_nvl2` | `[[1, 2], [2, 2]]` | `[[1, 1], [2, 1]]` | `[[1, 2], [2, 2]]` | `[[1, 1], [2, 1]]` | `[[1, 2], [2, 2]]` | `FIXED_VAL` |
| `tbl_case_isnull` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `FIXED_VAL` |
| `tbl_is_distinct_from` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `[[1], [2]]` | `KEPT` |
| `tbl_eq_1` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `tbl_not_eq_1` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `tbl_eq_self` | `[[None]]` | `[[True]]` | `[[None]]` | `[[True]]` | `[[None]]` | `FIXED_VAL` |
| `tbl_mul_zero` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_like_pct` | `[[None]]` | `[[True]]` | `[[None]]` | `[[True]]` | `[[None]]` | `FIXED_VAL` |
| `tbl_in_list` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `tbl_cast_int` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_cast_smallint` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_cast_big_to_int` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_cast_big_to_int_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `tbl_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_abs_outer_proj` | `[[None]]` | `[[None]]` (NN:['a']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_abs_from_l` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `FIXED_SCHEMA` |
| `tbl_length` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_limit1_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `tbl_limit1_coalesce` | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `FIXED_VAL` |
| `tbl_limit1_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `tbl_one_row_bare` | `[[5]]` | `[[5]]` | `[[5]]` | `[[5]]` | `[[5]]` | `KEPT` |
| `tbl_one_row_isnull` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `KEPT` |
| `agg_count_val` | `[[0]]` | `[[0]]` | `[[0]]` | `[[0]]` | `[[0]]` | `KEPT` |
| `agg_count_isnull` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `KEPT` |
| `agg_count_isnotnull` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `KEPT` |
| `agg_count_coalesce` | `[[0]]` | `[[0]]` | `[[0]]` | `[[0]]` | `[[0]]` | `KEPT` |
| `agg_count_having_val` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `agg_count_having_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_count_having_coalesce` | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `FIXED_VAL` |
| `agg_count_having_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `agg_count_plus1_having_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_sum_val` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` | `KEPT` |
| `agg_sum_isnull` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `KEPT` |
| `agg_max_val` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` | `KEPT` |
| `agg_max_isnull` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `[[True]]` | `KEPT` |
| `agg_count_groupby_val` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `FIXED_SCHEMA` |
| `agg_count_groupby_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_count_groupby_where` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `FIXED_VAL` |
| `agg_count_groupby_coalesce` | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `[[None]]` (NN:['r']) | `[[42]]` | `FIXED_VAL` |
| `agg_count_groupby_abs` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` | `[[None]]` | `[[None]]` | `FIXED_SCHEMA` |
| `agg_count_nonempty_zero_groups_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_count_limit0_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_count_offset1_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_coalesce_max_nohaving_isnull` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `KEPT` |
| `agg_coalesce_max_having_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_grouping_sets_empty_isnull` | `[[True]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `ROLLUP_STD_SQL` |
| `agg_rollup_empty_isnull` | `[[True]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `ROLLUP_STD_SQL` |
| `agg_cube_empty_isnull` | `[[True]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `ROLLUP_STD_SQL` |
| `agg_groupby_b_rollup_b_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` | `FIXED_VAL` |
| `agg_select_lit_1_bare` | `[[1]]` | `[[1]]` | `[[1]]` | `[[1]]` | `[[1]]` | `KEPT` |
| `corr_nonagg_where_isnull` | `[[1]]` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | `PRE_ERR` |
| `corr_nonagg_select_val` | `[[1, None], [2, 20]]` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | `PRE_ERR` |
| `corr_nonagg_coalesce` | `[[1, 42], [2, 20]]` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | ERR: `AnalysisException: Invalid (non-executable) plan` | `PRE_ERR` |
| `corr_nonagg_limit1_where_isnull` | `[[1]]` | `[]` | `[[1]]` | `[]` | `[[1]]` | `FIXED_VAL` |
| `corr_nonagg_limit1_select_isnull` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `FIXED_VAL` |
| `corr_nonagg_limit1_coalesce` | `[[1, 42], [2, 20]]` | `[[1, None], [2, 20]]` (NN:['v']) | `[[1, 42], [2, 20]]` | `[[1, None], [2, 20]]` (NN:['v']) | `[[1, 42], [2, 20]]` | `FIXED_VAL` |
| `corr_nonagg_limit1_abs` | `[[1, None], [2, 20]]` | `[[1, None], [2, 20]]` (NN:['v']) | `[[1, None], [2, 20]]` | `[[1, None], [2, 20]]` | `[[1, None], [2, 20]]` | `FIXED_SCHEMA` |
| `corr_count_select_val` | `[[1, 0], [2, 1]]` | `[[1, 0], [2, 1]]` | `[[1, 0], [2, 1]]` | `[[1, 0], [2, 1]]` | `[[1, 0], [2, 1]]` | `KEPT` |
| `corr_count_where_isnull` | `[]` | `[]` | `[]` | `[]` | `[]` | `KEPT` |
| `corr_count_having_where_isnull` | `[[1]]` | `[]` | `[[1]]` | `[]` | `[[1]]` | `FIXED_VAL` |
| `corr_count_having_select_val` | `[[1, None], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['c']) | `[[1, None], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['c']) | `[[1, None], [2, 1]]` | `FIXED_SCHEMA` |
| `corr_count_having_coalesce` | `[[1, 42], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['v']) | `[[1, 42], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['v']) | `[[1, 42], [2, 1]]` | `FIXED_VAL` |
| `corr_count_having_eq0_val` | `[[1, 0], [2, None]]` | `[[1, 0], [2, None]]` (NN:['c']) | `[[1, 0], [2, None]]` | `[[1, 0], [2, None]]` (NN:['c']) | `[[1, 0], [2, None]]` | `FIXED_SCHEMA` |
| `corr_count_having_eq0_where_isnull` | `[[2]]` | `[]` | `[[2]]` | `[]` | `[[2]]` | `FIXED_VAL` |
| `corr_count_groupby_where_isnull` | `[[1]]` | `[]` | `[[1]]` | `[]` | `[[1]]` | `FIXED_VAL` |
| `corr_count_groupby_coalesce` | `[[1, 42], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['v']) | `[[1, 42], [2, 1]]` | `[[1, None], [2, 1]]` (NN:['v']) | `[[1, 42], [2, 1]]` | `FIXED_VAL` |
| `corr_max_where_isnull` | `[[1]]` | `[[1]]` | `[[1]]` | `[[1]]` | `[[1]]` | `KEPT` |
| `corr_max_coalesce` | `[[1, 42], [2, 20]]` | `[[1, 42], [2, 20]]` | `[[1, 42], [2, 20]]` | `[[1, 42], [2, 20]]` | `[[1, 42], [2, 20]]` | `KEPT` |
| `corr_coalesce_max_having_isnull` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` | `FIXED_VAL` |
| `corr_coalesce_max_nohaving_val` | `[[1, 0], [2, 20]]` | `[[1, 0], [2, 20]]` | `[[1, 0], [2, 20]]` | `[[1, 0], [2, 20]]` | `[[1, 0], [2, 20]]` | `KEPT` |

## 2. WS3 DML, Extra, and `enable_physical_uncorrelated_scalar_subquery=true` Cells

| Cell | Spark 4.1.2 | Main `spark.sql` | Head `spark.sql` (ANSI F/T) | Main `repark.sql` | Head `repark.sql` |
|---|---|---|---|---|---|
| `dml_delete_tbl_isnull` | `[]` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` |
| `dml_delete_tbl_isnotnull` | `[[1, 10], [2, 20]]` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` |
| `dml_delete_inline_isnull` | `[]` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` | ERR: `AnalysisException: Error during planning: DELETE` |
| `dml_update_tbl_isnull` | `[[1, 99], [2, 99]]` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` |
| `dml_update_tbl_isnotnull` | `[[1, 10], [2, 20]]` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` |
| `dml_update_inline_isnull` | `[[1, 99], [2, 99]]` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` | ERR: `AnalysisException: Error during planning: UPDATE` |
| `dml_merge_on_isnull` | ERR: `AnalysisException: [UNSUPPORTED_MERGE_CONDITION.` | `[[1, 10], [2, 20]]` | `[[1, 99], [2, 99]]` | ERR: `UnsupportedOperationException: This feature is n` | ERR: `UnsupportedOperationException: This feature is n` |
| `dml_merge_when_isnull` | ERR: `AnalysisException: [UNSUPPORTED_MERGE_CONDITION.` | `[[1, 10], [2, 20]]` | ERR: `UnsupportedOperationException: This feature is n` | ERR: `UnsupportedOperationException: This feature is n` | ERR: `UnsupportedOperationException: This feature is n` |
| `dml_insert_where_isnull` | `[[1, 100], [2, 200]]` | `[]` | `[[1, 100], [2, 200]]` | `[]` | `[[1, 100], [2, 200]]` |
| `dml_ctas_where_coalesce` | `[[1, 42], [2, 42]]` | `[]` | `[[1, 42], [2, 42]]` | `[]` | `[[1, 42], [2, 42]]` |
| `dml_ctas_bare_subq` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` |
| `extra.inline_utf8view_subq` | (see extended table) | ERR: `PySparkException: Invalid argument error: Column` | `[[None]]` | — | — |
| `extra.df_api_scalar_isnull` | (see extended table) | `[]` | `[[1], [2]]` | — | — |
| `phys_uncorr.tbl_bare_select` | (matches `spark_ansi_false`) | `[[None]]` (NN:['r']) | `[[None]]` | — | — |
| `phys_uncorr.tbl_isnull_select` | (matches `spark_ansi_false`) | `[[False]]` | `[[True]]` | — | — |
| `phys_uncorr.tbl_isnull_where` | (matches `spark_ansi_false`) | `[]` | `[[1], [2]]` | — | — |
| `phys_uncorr.tbl_coalesce` | (matches `spark_ansi_false`) | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | — | — |
| `phys_uncorr.tbl_abs` | (matches `spark_ansi_false`) | `[[None]]` (NN:['r']) | `[[None]]` | — | — |
| `phys_uncorr.agg_count_isnull` | (matches `spark_ansi_false`) | `[[False]]` | `[[False]]` | — | — |
| `phys_uncorr.agg_count_having_isnull` | (matches `spark_ansi_false`) | `[[False]]` | `[[True]]` | — | — |
| `phys_uncorr.agg_count_groupby_isnull` | (matches `spark_ansi_false`) | `[[False]]` | `[[True]]` | — | — |

## 3. Extended Step 0.3 SQL, CTE, Prepared, and Native View Cells

| Cell | Spark 4.1.2 | Main `spark.sql` | Head `spark.sql` (ANSI F/T) | Main `repark.sql` | Head `repark.sql` |
|---|---|---|---|---|---|
| `cte_with_scalar_subq_isnull` | `[[1, True, 42], [2, True, 42]]` | `[[1, False, None], [2, False, None]]` (NN:['v']) | `[[1, True, 42], [2, True, 42]]` | `[[1, False, None], [2, False, None]]` (NN:['v']) | `[[1, True, 42], [2, True, 42]]` |
| `cte_with_scalar_subq_where_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `cte_empty_in_scalar_subq_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `cte_empty_in_scalar_subq_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` |
| `cte_nested_scalar_subq_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `cte_count_star_control` | `[[0, False]]` | `[[0, False]]` | `[[0, False]]` | `[[0, False]]` | `[[0, False]]` |
| `union_with_empty_subq` | `[[None], [1]]` | `[[None], [1]]` (NN:['r']) | `[[None], [1]]` | `[[None], [1]]` (NN:['r']) | `[[None], [1]]` |
| `cast_date_subq_to_ts_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `cast_date_subq_to_ts_val` | `[[None]]` | ERR: `PySparkException: Arrow error: Invalid argument ` | `[[None]]` | `[[None]]` (NN:['r']) | `[[None]]` |
| `nested_scalar_subq_isnull` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `nested_scalar_subq_outer_empty` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `in_list_single_scalar_subq` | `[]` | `[]` | `[]` | `[]` | `[]` |
| `scalar_subq_in_having` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `scalar_subq_in_join_on` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `scalar_subq_lit_control` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` |
| `scalar_subq_values_control` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` | `[[False]]` |
| `prepared.prep_isnull_select` | `[[True]]` | `[[False]]` | `[[True]]` | `[[False]]` | `[[True]]` |
| `prepared.prep_isnull_where` | `[[1], [2]]` | `[]` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `prepared.prep_coalesce_param` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` | `[[1, 42], [2, 42]]` |
| `prepared.prep_bare` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` | `[[None]]` |
| `native_view.native_view_subq_select` | DuckDB/Trino: nullable `YES` / 2 rows | — | — | `[[1, None, False, None], [2, None, False, None]]` (NN:['r', 'c']) | `[[1, None, True, 42], [2, None, True, 42]]` |
| `native_view.native_view_subq_where_isnull` | DuckDB/Trino: nullable `YES` / 2 rows | — | — | `[]` | `[[1], [2]]` |
| `native_view.native_view_describe` | DuckDB/Trino: nullable `YES` / 2 rows | — | — | `[['id', 'Int32', 'NO'], ['r', 'Int32', 'NO']]` | `[['id', 'Int32', 'NO'], ['r', 'Int32', 'YES']]` |

## 4. Extended Step 0.3 DataFrame Door Cells (`F.expr`, `filter`, `selectExpr`, `withColumn`, `.scalar()`, Temp Views, Cache)

| Cell | Spark 4.1.2 | Main DF (ANSI F/T) | Head DF (ANSI F/T) |
|---|---|---|---|
| `fexpr_withcol_isnull` | `[[1, True], [2, True]]` | `[[1, False], [2, False]]` | `[[1, True], [2, True]]` |
| `fexpr_withcol_isnotnull` | `[[1, False], [2, False]]` | `[[1, True], [2, True]]` | `[[1, False], [2, False]]` |
| `fexpr_withcol_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` |
| `fexpr_withcol_bare` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, None], [2, None]]` |
| `fexpr_withcol_cast_date_ts` | `[[1, None], [2, None]]` | ERR: `PySparkException: Arrow error: Invalid argument ` | `[[1, None], [2, None]]` |
| `fexpr_withcol_case_isnull` | `[[1, True], [2, False]]` | `[[1, False], [2, False]]` | `[[1, True], [2, False]]` |
| `fexpr_withcol_coalesce_id` | `[[1, 1], [2, 2]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 1], [2, 2]]` |
| `fexpr_withcol_count_star_control` | `[[1, False], [2, False]]` | `[[1, False], [2, False]]` | `[[1, False], [2, False]]` |
| `fexpr_filter_col_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `fexpr_filter_col_isnotnull` | `[]` | `[[1], [2]]` | `[]` |
| `fexpr_filter_col_tbl_isnull` | `[[1], [2]]` | ERR: `AnalysisException: Error during planning: table ` | ERR: `AnalysisException: Error during planning: table ` |
| `filter_str_inline_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `filter_str_inline_isnotnull` | `[]` | `[[1], [2]]` | `[]` |
| `filter_str_tbl_isnull` | `[[1], [2]]` | ERR: `AnalysisException: Error during planning: table ` | ERR: `AnalysisException: Error during planning: table ` |
| `filter_str_tbl_isnotnull` | `[]` | ERR: `AnalysisException: Error during planning: table ` | ERR: `AnalysisException: Error during planning: table ` |
| `filter_str_count_having_isnull` | `[[1], [2]]` | ERR: `AnalysisException: Error during planning: table ` | ERR: `AnalysisException: Error during planning: table ` |
| `filter_str_count_star_control` | `[]` | ERR: `AnalysisException: Error during planning: table ` | ERR: `AnalysisException: Error during planning: table ` |
| `selectexpr_inline_isnull` | `[[1, True], [2, True]]` | `[[1, False], [2, False]]` | `[[1, True], [2, True]]` |
| `selectexpr_inline_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` |
| `selectexpr_tbl_isnull` | `[[1, True], [2, True]]` | `[[1, False], [2, False]]` | `[[1, True], [2, True]]` |
| `selectexpr_tbl_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` |
| `selectexpr_tbl_bare` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, None], [2, None]]` |
| `withcol_scalar_isnull` | `[[1, True], [2, True]]` | `[[1, False], [2, False]]` | `[[1, True], [2, True]]` |
| `withcol_scalar_coalesce` | `[[1, 42], [2, 42]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, 42], [2, 42]]` |
| `withcol_scalar_bare` | `[[1, None], [2, None]]` | `[[1, None], [2, None]]` (NN:['r']) | `[[1, None], [2, None]]` |
| `filter_scalar_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `filter_scalar_isnotnull` | `[]` | `[[1], [2]]` | `[]` |
| `tempview_sql_subq_select` | `[[1, None, True, 42], [2, None, True, 42]]` | `[[1, None, False, None], [2, None, False, None]]` (NN:['r', 'c']) | `[[1, None, True, 42], [2, None, True, 42]]` |
| `tempview_sql_subq_where_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `tempview_df_subq_select` | `[[1, None, True, 42], [2, None, True, 42]]` | `[[1, None, False, None], [2, None, False, None]]` (NN:['r', 'c']) | `[[1, None, True, 42], [2, None, True, 42]]` |
| `tempview_df_subq_where_isnull` | `[[1], [2]]` | `[]` | `[[1], [2]]` |
| `tempview_df_ansi_flip` | `[[1, True, 42], [2, True, 42]]` | `[[1, False, None], [2, False, None]]` (NN:['c']) | `[[1, True, 42], [2, True, 42]]` |
| `cached_subq_where_isnull` | `[[1, None], [2, None]]` | `[]` | `[[1, None], [2, None]]` |
| `spark_explode_empty_isnull` | `[[True]]` | `[[False]]` (current main `ed637ffb`) | `[[True]]` |

## 5. EXPLAIN Plan-Quality Comparison (Current Main vs Delta 1 Head, 16 cores)

| Query | Logical Plan Equal | Physical Plan Equal | Marker Free |
|---|---|---|---|
| `nosubq_1_filter_proj` | `True` | `True` | `True` |
| `nosubq_2_agg_group` | `True` | `True` | `True` |
| `nosubq_3_inner_join` | `True` | `True` | `True` |
| `nosubq_4_window` | `True` | `True` | `True` |
| `nosubq_5_coalesce_case` | `True` | `True` | `True` |
| `count_star_isnull` | `True` | `True` | `True` |
| `count_star_where_isnull` | `True` | `True` | `True` |
| `lit_1_isnull` | `True` | `True` | `True` |
| `subq_eq_1_outer_join_elim` | `False` | `True` | `True` |
| `subq_isnull_where` | `False` | `False` | `True` |

## 6. Delta 1 Verification Summary (Spark 4.1.2 / Old Main / Current Main / Merged / Delta 1)

| Suite / Section | Cells | Old Main (`fa7d75e4`) | Current Main (`ed637ffb`) | Merged (`19bb3e3b`) | Delta 1 Head | Regressions vs Current Main |
|---|---:|---:|---:|---:|---:|---:|
| WS3 Read Matrix (`spark.sql`, ANSI F/T, value + clean Arrow schema) | 121 | 28 | 33 | 115 | 115 | 0 |
| Verifier `repro_repark.py` (F-001/F-002/F-003, ANSI F/T) | 20 | 6 | 9 | 0 | 20 | 0 |
| Verifier `full_attack.py` SQL (ANSI F/T) | 154 | 55 | 63 | 133 | 148 | 0 |
| Verifier `full_attack.py` Doors (ANSI F / T) | 16 | 3 / 2 | 1 / 0 | 16 / 12 | 16 / 12 | 0 |
| Verifier `full_attack.py` Writes (ANSI F/T) | 6 | 0 | 0 | 5 | 5 | 0 |
| Delta 1 Generator + Iceberg/Parquet SQL (ANSI F/T) | 50 | 32 | 42 | 35 | 50 | 0 |
| Delta 1 `selectExpr` + Temp Views (ANSI F/T) | 14 | 9 | 10 | 8 | 14 | 0 |
| Delta 1 Iceberg/Parquet `INSERT ... SELECT` (ANSI F/T) | 5 | 5 | 5 | 3 | 5 | 0 |
| Delta 1 Native `repark.sql()` (unnest + Iceberg/Parquet casts/INSERT) | 11 | 10 | 10 | 11 | 11 | 0 |
