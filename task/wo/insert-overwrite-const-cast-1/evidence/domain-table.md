# INSERT-OVERWRITE-CONST-CAST-1 — Domain Table (Spark 4.1.2 vs Main vs Head)

- **Date:** 2026-11-24
- **Spark oracle:** Spark 4.1.2 + Iceberg 1.11.0 (local hadoop catalog)
- **Main:** `514281bd4` (`pkg-d1merged-ea94eedc`, tree-equal to `514281bd4`)
- **Head:** `fix/insert-overwrite-const-cast-1` (`896af53a3`)

## Part 1: Cast Shapes × ANSI Modes (Read Control vs `INSERT OVERWRITE`, 204 cells)

| Shape | ANSI | Spark Read | Main Read | Head Read | Spark OW | Main OW | Head OW |
|---|---|---|---|---|---|---|---|
| `cast_str_double_lit` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_lit` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_nested_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_nested_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_cte` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_cte` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_tempview` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_values1` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_values1` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_values2` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_values2` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_union` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_union` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_expr` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_expr` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_limit0` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_limit0` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_inner_false` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_inner_false` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_lit_over_empty_tbl` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_lit_over_empty_tbl` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_subq_over_empty_tbl` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_subq_over_empty_tbl` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_double_unprojected` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_double_unprojected` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_double_named_struct_dead` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_double_named_struct_dead` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_float_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_float_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ts_lit` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ts_lit` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ts_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ts_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ts_cte` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ts_cte` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ts_tempview` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ts_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ntz_lit` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ntz_lit` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ntz_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ntz_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ntz_cte` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ntz_cte` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_ntz_tempview` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_ntz_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_dec_lit` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_dbl_dec_lit` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_dec_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_dbl_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_dec_cte` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_dbl_dec_cte` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_dec_tempview` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_dbl_dec_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_nan_dec_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_dbl_nan_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_flt_dec_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_flt_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_str_int_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_int_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_int_cte` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_int_cte` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_int_tempview` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_int_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_bigint_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_bigint_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_smallint_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_smallint_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_tinyint_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_tinyint_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_bool_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_bool_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_date_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_date_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_date_tempview` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_date_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_dec_bad_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_str_dec_bad_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_str_dec_ovf_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_str_dec_ovf_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_big_int_subq` | `true` | `ERR(CAST_OVERFLOW)` | `OK` | `OK` | `ERR(CAST_OVERFLOW, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_OVERFLOW, r=1)` |
| `cast_big_int_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_big_int_tempview` | `true` | `ERR(CAST_OVERFLOW)` | `OK` | `OK` | `ERR(CAST_OVERFLOW, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_OVERFLOW, r=1)` |
| `cast_big_int_tempview` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_big_dec_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_big_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_dec_dec_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_dec_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_dbl_int_subq` | `true` | `ERR(CAST_OVERFLOW)` | `OK` | `OK` | `ERR(CAST_OVERFLOW, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_OVERFLOW, r=1)` |
| `cast_dbl_int_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_bool_dec_subq` | `true` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION)` | `OK` | `OK` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` | `OK (r=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION, r=1)` |
| `cast_bool_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `cast_map_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_map_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_arr_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_arr_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `cast_struct_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cast_struct_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `illegal_date_to_int_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_date_to_int_subq` | `false` | `OK` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `OK (r=0)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_date_to_int_tv` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_date_to_int_tv` | `false` | `OK` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `OK (r=0)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_try_date_to_int_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_try_date_to_int_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_int_to_date_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_int_to_date_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION, r=1)` |
| `illegal_ntz_to_bigint_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_bigint_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_bigint_tv` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_bigint_tv` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_double_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_double_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_dec_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_ntz_to_dec_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_int_to_ntz_subq` | `true` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `illegal_int_to_ntz_subq` | `false` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` | `ERR(DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION, r=1)` |
| `dead_case_false_lit` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_case_false_lit` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_true_else_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_true_else_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_1eq1_else_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_1eq1_else_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_simple_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_simple_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_live_branch_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_case_live_branch_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_col_cond_empty` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_case_col_cond_empty` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_if_false_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_if_false_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_if_true_else_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_if_true_else_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_if_true_then_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_if_true_then_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_coalesce_1_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_coalesce_1_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_coalesce_null_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `OK` | `OK` | `ERR(CAST_INVALID_INPUT, r=1)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_coalesce_null_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_where_false_and_subq` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_false_and_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_where_cast_and_false_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_cast_and_false_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_where_1eq0_and_subq` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_1eq0_and_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_where_cast_and_1eq0_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_cast_and_1eq0_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_where_true_or_subq` | `true` | `OK` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_true_or_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_where_cast_or_true_subq` | `true` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `dead_where_cast_or_true_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_case_false_ts_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_ts_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_ntz_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_ntz_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_dec_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_dec_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `dead_case_false_int_subq` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `dead_case_false_int_subq` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_try_cast_double` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_double` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_ts` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_ts` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_ntz` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_ntz` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_dbl_dec` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_dbl_dec` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_int` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_try_cast_int` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_double` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_double` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_ts` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_ts` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_ntz` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_ntz` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_dbl_dec` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_dbl_dec` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_valid_int` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_valid_int` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_null_double` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_null_double` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_double` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_double` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_ts` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_ts` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_ntz` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_ntz` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_dbl_dec` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_dbl_dec` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_empty_int` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_real_empty_int` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_real_bad_where_false_double` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_bad_where_false_double` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `OK (r=0)` | `OK (r=0)` |
| `ctrl_real_bad_where_false_int` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_real_bad_where_false_int` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_real_bad_where_false_big_int` | `true` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_real_bad_where_false_big_int` | `false` | `OK` | `OK` | `OK` | `OK (r=0)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_store_implicit_1e19_empty` | `true` | `OK` | `OK` | `OK` | `ERR(CAST_OVERFLOW_IN_TABLE_INSERT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_store_implicit_1e19_empty` | `false` | `OK` | `OK` | `OK` | `ERR(CAST_OVERFLOW_IN_TABLE_INSERT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_store_implicit_1e19_subq_false` | `true` | `OK` | `OK` | `OK` | `ERR(CAST_OVERFLOW_IN_TABLE_INSERT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |
| `ctrl_store_implicit_1e19_subq_false` | `false` | `OK` | `OK` | `OK` | `ERR(CAST_OVERFLOW_IN_TABLE_INSERT, r=1)` | `ERR(AnalysisException, r=1)` | `ERR(AnalysisException, r=1)` |

## Part 2: Write Doors Matrix (28 doors × 7 shapes × 2 ANSI modes + 48 non-empty atomicity cells, 420 cells)

| ANSI | Shape | Door | Spark | Main | Head |
|---|---|---|---|---|---|
| `true` | `str_double` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `insert_overwrite_by_name` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `insert_overwrite_static_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_double` | `insert_overwrite_dyn_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_double` | `ctas` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_double` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `crtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `writeTo_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `writeTo_overwritePartitions` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_double` | `writeTo_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `writeTo_create` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_double` | `writeTo_replace` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `saveAsTable_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `insertInto_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `insertInto_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `merge_in_source` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `merge_in_action_empty_src` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `merge_on_false_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `merge_on_cond` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `update_set_where_false` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `update_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `update_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `update_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `delete_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `delete_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_double` | `delete_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insert_overwrite_by_name` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insert_overwrite_static_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ts` | `insert_overwrite_dyn_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ts` | `ctas` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_ts` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `crtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `writeTo_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `writeTo_overwritePartitions` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ts` | `writeTo_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `writeTo_create` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_ts` | `writeTo_replace` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `saveAsTable_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insertInto_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `insertInto_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `merge_in_source` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `merge_in_action_empty_src` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `merge_on_false_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `merge_on_cond` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `str_ts` | `update_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `update_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `update_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `delete_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `delete_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ts` | `delete_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insert_overwrite_by_name` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insert_overwrite_static_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ntz` | `insert_overwrite_dyn_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ntz` | `ctas` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_ntz` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `crtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `writeTo_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `writeTo_overwritePartitions` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_ntz` | `writeTo_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `writeTo_create` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_ntz` | `writeTo_replace` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `saveAsTable_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insertInto_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `insertInto_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `merge_in_source` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `merge_in_action_empty_src` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `merge_on_false_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `merge_on_cond` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `str_ntz` | `update_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `update_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `update_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `delete_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `delete_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_ntz` | `delete_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dbl_dec` | `insert_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `insert_into` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `insert_overwrite_by_name` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `insert_overwrite_static_part` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` |
| `true` | `dbl_dec` | `insert_overwrite_dyn_part` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` |
| `true` | `dbl_dec` | `ctas` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=False,n=None)` |
| `true` | `dbl_dec` | `rtas` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `crtas` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `writeTo_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `writeTo_overwritePartitions` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` | `OK (ex=True,n=2)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=2)` |
| `true` | `dbl_dec` | `writeTo_append` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `writeTo_create` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=False,n=None)` |
| `true` | `dbl_dec` | `writeTo_replace` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `saveAsTable_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `insertInto_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `insertInto_append` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `parquet_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `merge_in_source` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `merge_in_action_empty_src` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `merge_on_false_and` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `merge_on_cond` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dbl_dec` | `update_where_1eq0_and` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `update_where_cast_and_1eq0` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `update_where_cast` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `delete_where_1eq0_and` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `delete_where_cast_and_1eq0` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `dbl_dec` | `delete_where_cast` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` |
| `true` | `str_int` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `insert_overwrite_by_name` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `insert_overwrite_static_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_int` | `insert_overwrite_dyn_part` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_int` | `ctas` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_int` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `crtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `writeTo_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `writeTo_overwritePartitions` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` | `ERR(CAST_INVALID_INPUT; ex=True,n=2)` |
| `true` | `str_int` | `writeTo_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `writeTo_create` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=False,n=None)` |
| `true` | `str_int` | `writeTo_replace` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `saveAsTable_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `insertInto_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `insertInto_append` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `merge_in_source` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `merge_in_action_empty_src` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `merge_on_false_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `merge_on_cond` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `str_int` | `update_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `update_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `update_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `delete_where_1eq0_and` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `delete_where_cast_and_1eq0` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `str_int` | `delete_where_cast` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dead_case_double` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dead_case_double` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `true` | `dead_case_double` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `true` | `dead_case_double` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dead_case_double` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dead_case_double` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `dead_case_double` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `dead_case_double` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `merge_on_cond` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `update_set_where_false` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `update_where_cast` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `dead_case_double` | `delete_where_cast` | `OK (ex=True,n=0)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `real_empty_double` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `real_empty_double` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `real_empty_double` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `true` | `real_empty_double` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `true` | `real_empty_double` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `real_empty_double` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `real_empty_double` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `true` | `real_empty_double` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `true` | `ne_str_double_2row` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_2row` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_2row` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_2row` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ts_2row` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ts_2row` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ts_2row` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ts_2row` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ntz_2row` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ntz_2row` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ntz_2row` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_ntz_2row` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_dbl_dec_2row` | `insert_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `true` | `ne_dbl_dec_2row` | `insert_into` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `true` | `ne_dbl_dec_2row` | `rtas` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `true` | `ne_dbl_dec_2row` | `parquet_overwrite` | `ERR(NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `true` | `ne_str_int_2row` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_int_2row` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `true` | `ne_str_int_2row` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_int_2row` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_multibatch` | `insert_overwrite` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_multibatch` | `insert_into` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_multibatch` | `rtas` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `true` | `ne_str_double_multibatch` | `parquet_overwrite` | `ERR(CAST_INVALID_INPUT; ex=None,n=None)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` | `ERR(CAST_INVALID_INPUT; ex=True,n=1)` |
| `false` | `str_double` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_double` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_double` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_double` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `merge_on_cond` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `update_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_double` | `delete_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_ts` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_ts` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ts` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `merge_on_cond` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `update_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ts` | `delete_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_ntz` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `str_ntz` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_ntz` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `merge_on_cond` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `update_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_ntz` | `delete_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `dbl_dec` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `dbl_dec` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dbl_dec` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `merge_on_cond` | `OK (ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `dbl_dec` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `update_where_cast` | `OK (ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `dbl_dec` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dbl_dec` | `delete_where_cast` | `OK (ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `str_int` | `insert_overwrite` | `OK (ex=True,n=0)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` |
| `false` | `str_int` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` |
| `false` | `str_int` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` |
| `false` | `str_int` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` | `ERR(AnalysisException; ex=True,n=2)` |
| `false` | `str_int` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `ERR(AnalysisException; ex=True,n=1)` | `ERR(AnalysisException; ex=True,n=1)` |
| `false` | `str_int` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `str_int` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `merge_on_cond` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `update_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `str_int` | `delete_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `dead_case_double` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `dead_case_double` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `merge_in_action_empty_src` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `merge_on_false_and` | `ERR(Py4JJavaError; ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `merge_on_cond` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `dead_case_double` | `update_set_where_false` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `update_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `update_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `update_where_cast` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `delete_where_1eq0_and` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `delete_where_cast_and_1eq0` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `dead_case_double` | `delete_where_cast` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `insert_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `insert_into` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `real_empty_double` | `insert_overwrite_by_name` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `insert_overwrite_static_part` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `real_empty_double` | `insert_overwrite_dyn_part` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `real_empty_double` | `ctas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `rtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `crtas` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `writeTo_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `writeTo_overwritePartitions` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `real_empty_double` | `writeTo_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `real_empty_double` | `writeTo_create` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `writeTo_replace` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `saveAsTable_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `insertInto_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `insertInto_append` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `real_empty_double` | `parquet_overwrite` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` | `OK (ex=True,n=0)` |
| `false` | `real_empty_double` | `merge_in_source` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` | `OK (ex=True,n=1)` |
| `false` | `ne_str_double_2row` | `insert_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_double_2row` | `insert_into` | `OK (ex=True,n=3)` | `OK (ex=True,n=3)` | `OK (ex=True,n=3)` |
| `false` | `ne_str_double_2row` | `rtas` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_double_2row` | `parquet_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ts_2row` | `insert_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ts_2row` | `insert_into` | `OK (ex=True,n=3)` | `OK (ex=True,n=3)` | `OK (ex=True,n=3)` |
| `false` | `ne_str_ts_2row` | `rtas` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ts_2row` | `parquet_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ntz_2row` | `insert_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ntz_2row` | `insert_into` | `OK (ex=True,n=3)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_ntz_2row` | `rtas` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_ntz_2row` | `parquet_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_dbl_dec_2row` | `insert_overwrite` | `OK (ex=True,n=2)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_dbl_dec_2row` | `insert_into` | `OK (ex=True,n=3)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_dbl_dec_2row` | `rtas` | `OK (ex=True,n=2)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_dbl_dec_2row` | `parquet_overwrite` | `OK (ex=True,n=2)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_int_2row` | `insert_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_int_2row` | `insert_into` | `OK (ex=True,n=3)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_int_2row` | `rtas` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_int_2row` | `parquet_overwrite` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` | `OK (ex=True,n=2)` |
| `false` | `ne_str_double_multibatch` | `insert_overwrite` | `OK (ex=True,n=20000)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_double_multibatch` | `insert_into` | `OK (ex=True,n=20001)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_double_multibatch` | `rtas` | `OK (ex=True,n=20000)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |
| `false` | `ne_str_double_multibatch` | `parquet_overwrite` | `OK (ex=True,n=20000)` | `ERR(PySparkException; ex=True,n=1)` | `ERR(PySparkException; ex=True,n=1)` |

## Part 3: Edge Cases & View/DataFrame ANSI Capture (24 cells)

| Cell | Main | Head |
|---|---|---|
| `true|union_real_and_const` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|union_valid_and_bad` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|left_join_const_right` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|groupby_const_having_false` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|try_cast_wrapping_cast` | `ERR(ERR, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|values_cast_where_false` | `ERR(CAST_INVALID_INPUT, r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|update_empty_no_where` | `OK (r=0)` | `OK (r=0)` |
| `true|update_empty_ts_no_where` | `OK (r=0)` | `OK (r=0)` |
| `true|where_false_and_col_ts` | `OK (r=1)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `true|lit_ts_limit0` | `OK (r=0)` | `OK (r=0)` |
| `false|union_real_and_const` | `OK (r=0)` | `OK (r=0)` |
| `false|union_valid_and_bad` | `OK (r=0)` | `OK (r=0)` |
| `false|left_join_const_right` | `OK (r=0)` | `OK (r=0)` |
| `false|groupby_const_having_false` | `OK (r=0)` | `OK (r=0)` |
| `false|try_cast_wrapping_cast` | `ERR(ERR, r=1)` | `ERR(ERR, r=1)` |
| `false|values_cast_where_false` | `OK (r=0)` | `OK (r=0)` |
| `false|update_empty_no_where` | `OK (r=0)` | `OK (r=0)` |
| `false|update_empty_ts_no_where` | `OK (r=0)` | `OK (r=0)` |
| `false|where_false_and_col_ts` | `OK (r=1)` | `OK (r=1)` |
| `false|lit_ts_limit0` | `OK (r=0)` | `OK (r=0)` |
| `cap|tv_created_off_cast_on` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
| `cap|tv_cast_created_off_read_on` | `OK (r=0)` | `OK (r=0)` |
| `cap|df_built_off_written_on` | `OK (r=0)` | `OK (r=0)` |
| `cap|df_built_on_written_off` | `OK (r=0)` | `ERR(CAST_INVALID_INPUT, r=1)` |
