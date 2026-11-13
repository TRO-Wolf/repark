# Step 0 table

Spark leg: `{"engine": "spark", "module_sha256": "", "tree": "", "version": "4.1.2"}`

RePark leg: `{"engine": "repark", "module_sha256": "44d57fa0ec459c02ae058b70249b6fb30bf7279c28e6d0bb8d4e24bad7e7b455", "native_module": "_native.abi3.so", "tree": "a3f8c7d9+fix", "version": "1.5.6"}`

The native door is labelled against Spark's `spark.sql` answer for information only; it keeps its own dialect.

## Totals by door

| Door | Label | Cells |
|---|---|---|
| fexpr | EQUAL | 335 |
| fexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | 4 |
| fexpr_agg | EQUAL | 2 |
| fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 9 |
| fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | 4 |
| fexpr_groupby | EQUAL | 2 |
| fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 9 |
| fexpr_withcolumn | EQUAL | 288 |
| native | ACCEPTED-WHERE-SPARK-REFUSED | 4 |
| native | EQUAL | 108 |
| native | REFUSED-WHERE-SPARK-ACCEPTED | 41 |
| native | WRONG-READ | 150 |
| orderby_fexpr | BOTH-REFUSED | 12 |
| orderby_fexpr | EQUAL | 3 |
| orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| orderby_fexpr_method | EQUAL | 18 |
| selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | 4 |
| selectexpr | EQUAL | 290 |
| selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 9 |
| sort_col | EQUAL | 18 |
| sort_string | BOTH-REFUSED | 12 |
| sort_string | EQUAL | 3 |
| sort_string | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| sparksql | ACCEPTED-WHERE-SPARK-REFUSED | 4 |
| sparksql | EQUAL | 295 |
| sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 10 |
| typed_window | EQUAL | 18 |

## By family and door

| Family | Door | Label | Cells |
|---|---|---|---|
| `A|array_agg_order_ki` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki` | native | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki_desc` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki_desc` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki_desc` | native | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki_desc` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|array_agg_order_ki_desc` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|first_value_agg_order` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|first_value_agg_order` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|first_value_agg_order` | native | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|first_value_agg_order` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|first_value_agg_order` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|listagg_kd` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd_asc_nl` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd_asc_nl` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd_asc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd_asc_nl` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_kd_asc_nl` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc_nf` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc_nf` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc_nf` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|listagg_ki_desc_nf` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|mode_within` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|mode_within` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|mode_within` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|mode_within` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|mode_within` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_cont_asc` | fexpr_agg | EQUAL | 1 |
| `A|percentile_cont_asc` | fexpr_groupby | EQUAL | 1 |
| `A|percentile_cont_asc` | native | EQUAL | 1 |
| `A|percentile_cont_asc` | selectexpr | EQUAL | 1 |
| `A|percentile_cont_asc` | sparksql | EQUAL | 1 |
| `A|percentile_cont_desc` | fexpr_agg | EQUAL | 1 |
| `A|percentile_cont_desc` | fexpr_groupby | EQUAL | 1 |
| `A|percentile_cont_desc` | native | EQUAL | 1 |
| `A|percentile_cont_desc` | selectexpr | EQUAL | 1 |
| `A|percentile_cont_desc` | sparksql | EQUAL | 1 |
| `A|percentile_disc_asc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_asc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_asc` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_asc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_asc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|percentile_disc_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_ki_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_ki_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_ki_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_ki_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_ki_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `A|string_agg_order_ks` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|string_agg_order_ks` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|string_agg_order_ks` | native | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|string_agg_order_ks` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `A|string_agg_order_ks` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | 1 |
| `D|dedup` | fexpr | EQUAL | 21 |
| `D|dedup_part` | fexpr | EQUAL | 21 |
| `S|orderby` | orderby_fexpr | BOTH-REFUSED | 12 |
| `S|orderby` | orderby_fexpr | EQUAL | 3 |
| `S|orderby` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| `S|orderby` | sort_string | BOTH-REFUSED | 12 |
| `S|orderby` | sort_string | EQUAL | 3 |
| `S|orderby` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | 6 |
| `T|typed` | orderby_fexpr_method | EQUAL | 18 |
| `T|typed` | sort_col | EQUAL | 18 |
| `T|typed` | typed_window | EQUAL | 18 |
| `W|collect_list` | fexpr | EQUAL | 32 |
| `W|collect_list` | fexpr_withcolumn | EQUAL | 32 |
| `W|collect_list` | native | REFUSED-WHERE-SPARK-ACCEPTED | 32 |
| `W|collect_list` | selectexpr | EQUAL | 32 |
| `W|collect_list` | sparksql | EQUAL | 32 |
| `W|dense_rank` | fexpr | EQUAL | 32 |
| `W|dense_rank` | fexpr_withcolumn | EQUAL | 32 |
| `W|dense_rank` | native | EQUAL | 13 |
| `W|dense_rank` | native | WRONG-READ | 19 |
| `W|dense_rank` | selectexpr | EQUAL | 32 |
| `W|dense_rank` | sparksql | EQUAL | 32 |
| `W|first_value` | fexpr | EQUAL | 32 |
| `W|first_value` | fexpr_withcolumn | EQUAL | 32 |
| `W|first_value` | native | EQUAL | 14 |
| `W|first_value` | native | WRONG-READ | 18 |
| `W|first_value` | selectexpr | EQUAL | 32 |
| `W|first_value` | sparksql | EQUAL | 32 |
| `W|lag` | fexpr | EQUAL | 32 |
| `W|lag` | fexpr_withcolumn | EQUAL | 32 |
| `W|lag` | native | EQUAL | 13 |
| `W|lag` | native | WRONG-READ | 19 |
| `W|lag` | selectexpr | EQUAL | 32 |
| `W|lag` | sparksql | EQUAL | 32 |
| `W|last_value` | fexpr | EQUAL | 32 |
| `W|last_value` | fexpr_withcolumn | EQUAL | 32 |
| `W|last_value` | native | EQUAL | 14 |
| `W|last_value` | native | WRONG-READ | 18 |
| `W|last_value` | selectexpr | EQUAL | 32 |
| `W|last_value` | sparksql | EQUAL | 32 |
| `W|lead` | fexpr | EQUAL | 32 |
| `W|lead` | fexpr_withcolumn | EQUAL | 32 |
| `W|lead` | native | EQUAL | 13 |
| `W|lead` | native | WRONG-READ | 19 |
| `W|lead` | selectexpr | EQUAL | 32 |
| `W|lead` | sparksql | EQUAL | 32 |
| `W|rank` | fexpr | EQUAL | 32 |
| `W|rank` | fexpr_withcolumn | EQUAL | 32 |
| `W|rank` | native | EQUAL | 13 |
| `W|rank` | native | WRONG-READ | 19 |
| `W|rank` | selectexpr | EQUAL | 32 |
| `W|rank` | sparksql | EQUAL | 32 |
| `W|row_number` | fexpr | EQUAL | 32 |
| `W|row_number` | fexpr_withcolumn | EQUAL | 32 |
| `W|row_number` | native | EQUAL | 13 |
| `W|row_number` | native | WRONG-READ | 19 |
| `W|row_number` | selectexpr | EQUAL | 32 |
| `W|row_number` | sparksql | EQUAL | 32 |
| `W|running_sum` | fexpr | EQUAL | 32 |
| `W|running_sum` | fexpr_withcolumn | EQUAL | 32 |
| `W|running_sum` | native | EQUAL | 13 |
| `W|running_sum` | native | WRONG-READ | 19 |
| `W|running_sum` | selectexpr | EQUAL | 32 |
| `W|running_sum` | sparksql | EQUAL | 32 |
| `X|array_sort_comparator` | fexpr | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `X|array_sort_comparator` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | 1 |
| `X|array_sort_plain` | fexpr | EQUAL | 1 |
| `X|array_sort_plain` | sparksql | EQUAL | 1 |
| `X|sort_array_asc` | fexpr | EQUAL | 1 |
| `X|sort_array_asc` | sparksql | EQUAL | 1 |
| `X|sort_array_desc` | fexpr | EQUAL | 1 |
| `X|sort_array_desc` | sparksql | EQUAL | 1 |
| `X|subquery_order_asc` | fexpr | EQUAL | 1 |
| `X|subquery_order_asc` | sparksql | EQUAL | 1 |
| `X|subquery_order_desc` | fexpr | EQUAL | 1 |
| `X|subquery_order_desc` | sparksql | EQUAL | 1 |

## Every non-EQUAL cell

| Cell | Door | Label | RePark | Spark |
|---|---|---|---|---|
| `A|array_agg_order_ki` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | `[[[2, 1, 4, 3, 5]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [2, 1, 5]], [2, [4, 3]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki` | native | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [1, 5, 2]], [2, [4, 3]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 23)  … |
| `A|array_agg_order_ki` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | `[[[1, 4, 3, 5, 2]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [1, 5, 2]], [2, [4, 3]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 23)  … |
| `A|array_agg_order_ki_desc` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | `[[[5, 3, 4, 1, 2]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki_desc` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [5, 1, 2]], [2, [3, 4]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki_desc` | native | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [2, 5, 1]], [2, [3, 4]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 23)  … |
| `A|array_agg_order_ki_desc` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | `[[[2, 5, 3, 4, 1]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 13)  == SQL == … |
| `A|array_agg_order_ki_desc` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, [2, 5, 1]], [2, [3, 4]]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 23)  … |
| `A|first_value_agg_order` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | `[[5]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 15)  == SQL == … |
| `A|first_value_agg_order` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, 5], [2, 3]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 15)  == SQL == … |
| `A|first_value_agg_order` | native | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, 2], [2, 3]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 25)  … |
| `A|first_value_agg_order` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | `[[2]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER'. SQLSTATE: 42601 (line 1, pos 15)  == SQL == … |
| `A|first_value_agg_order` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, 2], [2, 3]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': missing ')'. SQLSTATE: 42601 (line 1, pos 25)  … |
| `A|listagg_kd` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["a,d,b,e"]]` |
| `A|listagg_kd` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_kd` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_kd` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["a,d,b,e"]]` |
| `A|listagg_kd` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_kd_asc_nl` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["d,b,e,a"]]` |
| `A|listagg_kd_asc_nl` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_kd_asc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_kd_asc_nl` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["d,b,e,a"]]` |
| `A|listagg_kd_asc_nl` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_ki` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["e,b,a,d"]]` |
| `A|listagg_ki` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,b,d"], [2, "a"]]` |
| `A|listagg_ki` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,b,d"], [2, "a"]]` |
| `A|listagg_ki` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["e,b,a,d"]]` |
| `A|listagg_ki` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,b,d"], [2, "a"]]` |
| `A|listagg_ki_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["d,a,b,e"]]` |
| `A|listagg_ki_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_ki_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_ki_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["d,a,b,e"]]` |
| `A|listagg_ki_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "d,b,e"], [2, "a"]]` |
| `A|listagg_ki_desc_nf` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["e,d,a,b"]]` |
| `A|listagg_ki_desc_nf` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,d,b"], [2, "a"]]` |
| `A|listagg_ki_desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,d,b"], [2, "a"]]` |
| `A|listagg_ki_desc_nf` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[["e,d,a,b"]]` |
| `A|listagg_ki_desc_nf` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `listagg` on search path [`system`.`builtin`, … | `[[1, "e,d,b"], [2, "a"]]` |
| `A|mode_within` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `mode` on search path [`system`.`builtin`, … | `[[10]]` |
| `A|mode_within` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `mode` on search path [`system`.`builtin`, … | `[[1, 10], [2, 20]]` |
| `A|mode_within` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `mode` on search path [`system`.`builtin`, … | `[[1, 10], [2, 20]]` |
| `A|mode_within` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `mode` on search path [`system`.`builtin`, … | `[[10]]` |
| `A|mode_within` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `mode` on search path [`system`.`builtin`, … | `[[1, 10], [2, 20]]` |
| `A|percentile_disc_asc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[10.0]]` |
| `A|percentile_disc_asc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 10.0], [2, 20.0]]` |
| `A|percentile_disc_asc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 10.0], [2, 20.0]]` |
| `A|percentile_disc_asc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[10.0]]` |
| `A|percentile_disc_asc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 10.0], [2, 20.0]]` |
| `A|percentile_disc_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[40.0]]` |
| `A|percentile_disc_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 40.0], [2, 30.0]]` |
| `A|percentile_disc_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 40.0], [2, 30.0]]` |
| `A|percentile_disc_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[40.0]]` |
| `A|percentile_disc_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `percentile_disc` on search path [`system`.`builtin`, … | `[[1, 40.0], [2, 30.0]]` |
| `A|string_agg_ki_desc` | fexpr_agg | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: Error during planning: WITHIN GROUP is only supported for ordered-set aggregate functions | `[["d,a,b,e"]]` |
| `A|string_agg_ki_desc` | fexpr_groupby | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: Error during planning: WITHIN GROUP is only supported for ordered-set aggregate functions | `[[1, "d,b,e"], [2, "a"]]` |
| `A|string_agg_ki_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: Error during planning: WITHIN GROUP is only supported for ordered-set aggregate functions | `[[1, "d,b,e"], [2, "a"]]` |
| `A|string_agg_ki_desc` | selectexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: Error during planning: WITHIN GROUP is only supported for ordered-set aggregate functions | `[["d,a,b,e"]]` |
| `A|string_agg_ki_desc` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: Error during planning: WITHIN GROUP is only supported for ordered-set aggregate functions | `[[1, "d,b,e"], [2, "a"]]` |
| `A|string_agg_order_ks` | fexpr_agg | ACCEPTED-WHERE-SPARK-REFUSED | `[["a,b,d,e"]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': extra input 'ORDER'. SQLSTATE: 42601 (line 1, … |
| `A|string_agg_order_ks` | fexpr_groupby | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, "b,d,e"], [2, "a"]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': extra input 'ORDER'. SQLSTATE: 42601 (line 1, … |
| `A|string_agg_order_ks` | native | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, "b,d,e"], [2, "a"]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': extra input 'ORDER'. SQLSTATE: 42601 (line 1, … |
| `A|string_agg_order_ks` | selectexpr | ACCEPTED-WHERE-SPARK-REFUSED | `[["a,b,d,e"]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': extra input 'ORDER'. SQLSTATE: 42601 (line 1, … |
| `A|string_agg_order_ks` | sparksql | ACCEPTED-WHERE-SPARK-REFUSED | `[[1, "b,d,e"], [2, "a"]]` | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'ORDER': extra input 'ORDER'. SQLSTATE: 42601 (line 1, … |
| `S|orderby|kd|asc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[4], [5], [1], [2], [3]]` |
| `S|orderby|kd|asc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[4], [5], [1], [2], [3]]` |
| `S|orderby|kd|asc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == kd … |
| `S|orderby|kd|asc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == kd … |
| `S|orderby|kd|asc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == kd … |
| `S|orderby|kd|asc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == kd … |
| `S|orderby|kd|desc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[4], [5], [1], [2], [3]]` |
| `S|orderby|kd|desc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[4], [5], [1], [2], [3]]` |
| `S|orderby|kd|desc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == kd … |
| `S|orderby|kd|desc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == kd … |
| `S|orderby|kd|desc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == kd … |
| `S|orderby|kd|desc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == kd … |
| `S|orderby|ki|asc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[2], [1], [4], [3], [5]]` |
| `S|orderby|ki|asc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[2], [1], [4], [3], [5]]` |
| `S|orderby|ki|asc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ki … |
| `S|orderby|ki|asc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ki … |
| `S|orderby|ki|asc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ki … |
| `S|orderby|ki|asc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ki … |
| `S|orderby|ki|desc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[2], [1], [4], [3], [5]]` |
| `S|orderby|ki|desc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[2], [1], [4], [3], [5]]` |
| `S|orderby|ki|desc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ki … |
| `S|orderby|ki|desc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ki … |
| `S|orderby|ki|desc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ki … |
| `S|orderby|ki|desc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ki … |
| `S|orderby|ks|asc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[3], [4], [1], [5], [2]]` |
| `S|orderby|ks|asc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | `[[3], [4], [1], [5], [2]]` |
| `S|orderby|ks|asc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ks … |
| `S|orderby|ks|asc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ks … |
| `S|orderby|ks|asc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ks … |
| `S|orderby|ks|asc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: ASC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 7)  == SQL == ks … |
| `S|orderby|ks|desc` | orderby_fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[3], [4], [1], [5], [2]]` |
| `S|orderby|ks|desc` | sort_string | REFUSED-WHERE-SPARK-ACCEPTED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | `[[3], [4], [1], [5], [2]]` |
| `S|orderby|ks|desc_nf` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ks … |
| `S|orderby|ks|desc_nf` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ks … |
| `S|orderby|ks|desc_nl` | orderby_fexpr | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ks … |
| `S|orderby|ks|desc_nl` | sort_string | BOTH-REFUSED | error: ParseException: SQL error: ParserError("Expected: ), found: DESC at Line: 1, Column: 12") | error: ParseException:  [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULLS'. SQLSTATE: 42601 (line 1, pos 8)  == SQL == ks … |
| `W|collect_list|kd|asc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 5, 1]], [2, [4, 5, 1, 2]], [3, [4, 5, 1, 2, 3]], [4, [4]], [5, [4, 5]]]` |
| `W|collect_list|kd|asc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 5, 1]], [2, [4, 5, 1, 2]], [3, [4, 5, 1, 2, 3]], [4, [4]], [5, [4, 5]]]` |
| `W|collect_list|kd|asc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 1]], [2, [5, 1, 2]], [3, [5, 1, 2, 3]], [4, [5, 1, 2, 3, 4]], [5, [5]]]` |
| `W|collect_list|kd|default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 5, 1]], [2, [4, 5, 1, 2]], [3, [4, 5, 1, 2, 3]], [4, [4]], [5, [4, 5]]]` |
| `W|collect_list|kd|desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 2, 1]], [2, [3, 2]], [3, [3]], [4, [3, 2, 1, 5, 4]], [5, [3, 2, 1, 5]]]` |
| `W|collect_list|kd|desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 3, 2, 1]], [2, [4, 3, 2]], [3, [4, 3]], [4, [4]], [5, [4, 3, 2, 1, 5]]]` |
| `W|collect_list|kd|desc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 2, 1]], [2, [3, 2]], [3, [3]], [4, [3, 2, 1, 5, 4]], [5, [3, 2, 1, 5]]]` |
| `W|collect_list|kd|part_default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 1]], [2, [5, 1, 2]], [3, [4, 3]], [4, [4]], [5, [5]]]` |
| `W|collect_list|kd|part_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 1]], [2, [2]], [3, [3]], [4, [3, 4]], [5, [2, 1, 5]]]` |
| `W|collect_list|ki|asc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 1]], [2, [2]], [3, [2, 1, 4, 3]], [4, [2, 1, 4]], [5, [2, 1, 4, 3, 5]]]` |
| `W|collect_list|ki|asc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 1]], [2, [2]], [3, [2, 1, 4, 3]], [4, [2, 1, 4]], [5, [2, 1, 4, 3, 5]]]` |
| `W|collect_list|ki|asc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [1]], [2, [1, 4, 3, 5, 2]], [3, [1, 4, 3]], [4, [1, 4]], [5, [1, 4, 3, 5]]]` |
| `W|collect_list|ki|default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 1]], [2, [2]], [3, [2, 1, 4, 3]], [4, [2, 1, 4]], [5, [2, 1, 4, 3, 5]]]` |
| `W|collect_list|ki|desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 3, 4, 1]], [2, [5, 3, 4, 1, 2]], [3, [5, 3]], [4, [5, 3, 4]], [5, [5]]]` |
| `W|collect_list|ki|desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 5, 3, 4, 1]], [2, [2]], [3, [2, 5, 3]], [4, [2, 5, 3, 4]], [5, [2, 5]]]` |
| `W|collect_list|ki|desc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 3, 4, 1]], [2, [5, 3, 4, 1, 2]], [3, [5, 3]], [4, [5, 3, 4]], [5, [5]]]` |
| `W|collect_list|ki|part_default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 1]], [2, [2]], [3, [4, 3]], [4, [4]], [5, [2, 1, 5]]]` |
| `W|collect_list|ki|part_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 1]], [2, [5, 1, 2]], [3, [3]], [4, [3, 4]], [5, [5]]]` |
| `W|collect_list|ks|asc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 4, 1]], [2, [3, 4, 1, 5, 2]], [3, [3]], [4, [3, 4]], [5, [3, 4, 1, 5]]]` |
| `W|collect_list|ks|asc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 4, 1]], [2, [3, 4, 1, 5, 2]], [3, [3]], [4, [3, 4]], [5, [3, 4, 1, 5]]]` |
| `W|collect_list|ks|asc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 1]], [2, [4, 1, 5, 2]], [3, [4, 1, 5, 2, 3]], [4, [4]], [5, [4, 1, 5]]]` |
| `W|collect_list|ks|default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 4, 1]], [2, [3, 4, 1, 5, 2]], [3, [3]], [4, [3, 4]], [5, [3, 4, 1, 5]]]` |
| `W|collect_list|ks|desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 5, 1]], [2, [2]], [3, [2, 5, 1, 4, 3]], [4, [2, 5, 1, 4]], [5, [2, 5]]]` |
| `W|collect_list|ks|desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 2, 5, 1]], [2, [3, 2]], [3, [3]], [4, [3, 2, 5, 1, 4]], [5, [3, 2, 5]]]` |
| `W|collect_list|ks|desc_nl` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 5, 1]], [2, [2]], [3, [2, 5, 1, 4, 3]], [4, [2, 5, 1, 4]], [5, [2, 5]]]` |
| `W|collect_list|ks|part_default` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [1]], [2, [1, 5, 2]], [3, [3]], [4, [3, 4]], [5, [1, 5]]]` |
| `W|collect_list|ks|part_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 5, 1]], [2, [2]], [3, [4, 3]], [4, [4]], [5, [2, 5]]]` |
| `W|collect_list|multi|g_desc_ki` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [4, 3, 2, 1]], [2, [4, 3, 2]], [3, [4, 3]], [4, [4]], [5, [4, 3, 2, 1, 5]]]` |
| `W|collect_list|multi|g_ki_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [5, 1]], [2, [5, 1, 2]], [3, [5, 1, 2, 3]], [4, [5, 1, 2, 3, 4]], [5, [5]]]` |
| `W|collect_list|multi|g_ks_desc_nf` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [2, 5, 1]], [2, [2]], [3, [2, 5, 1, 3]], [4, [2, 5, 1, 3, 4]], [5, [2, 5]]]` |
| `W|collect_list|multi|kd_desc_g` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 2, 1]], [2, [3, 2]], [3, [3]], [4, [3, 2, 1, 5, 4]], [5, [3, 2, 1, 5]]]` |
| `W|collect_list|multi|ks_g_desc` | native | REFUSED-WHERE-SPARK-ACCEPTED | error: AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `collect_list` on search path [`system`.`builtin`, … | `[[1, [3, 4, 1]], [2, [3, 4, 1, 5, 2]], [3, [3]], [4, [3, 4]], [5, [3, 4, 1, 5]]]` |
| `W|dense_rank|kd|asc` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|dense_rank|kd|default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|dense_rank|kd|desc` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|dense_rank|kd|part_default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` | `[[1, 2], [2, 3], [3, 2], [4, 1], [5, 1]]` |
| `W|dense_rank|kd|part_desc` | native | WRONG-READ | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` | `[[1, 2], [2, 1], [3, 1], [4, 2], [5, 3]]` |
| `W|dense_rank|ki|asc` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|dense_rank|ki|default` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|dense_rank|ki|desc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 3], [4, 4], [5, 2]]` | `[[1, 4], [2, 5], [3, 2], [4, 3], [5, 1]]` |
| `W|dense_rank|ki|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` |
| `W|dense_rank|ki|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` |
| `W|dense_rank|ks|asc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|dense_rank|ks|default` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|dense_rank|ks|desc` | native | WRONG-READ | `[[1, 4], [2, 2], [3, 1], [4, 5], [5, 3]]` | `[[1, 3], [2, 1], [3, 5], [4, 4], [5, 2]]` |
| `W|dense_rank|ks|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 1], [2, 3], [3, 1], [4, 2], [5, 2]]` |
| `W|dense_rank|ks|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 3], [2, 1], [3, 2], [4, 1], [5, 2]]` |
| `W|dense_rank|multi|g_desc_ki` | native | WRONG-READ | `[[1, 3], [2, 5], [3, 2], [4, 1], [5, 4]]` | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` |
| `W|dense_rank|multi|g_ki_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 4], [4, 5], [5, 2]]` | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` |
| `W|dense_rank|multi|kd_desc_g` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|dense_rank|multi|ks_g_desc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|first_value|kd|asc` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` |
| `W|first_value|kd|default` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` |
| `W|first_value|kd|desc` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|first_value|kd|part_default` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 3], [4, 3], [5, 5]]` | `[[1, 5], [2, 5], [3, 4], [4, 4], [5, 5]]` |
| `W|first_value|kd|part_desc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` |
| `W|first_value|ki|asc` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 1], [4, 1], [5, 1]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|first_value|ki|default` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 1], [4, 1], [5, 1]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|first_value|ki|desc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` |
| `W|first_value|ki|part_default` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 4], [4, 4], [5, 1]]` | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` |
| `W|first_value|ki|part_desc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` | `[[1, 5], [2, 5], [3, 3], [4, 3], [5, 5]]` |
| `W|first_value|ks|asc` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|first_value|ks|default` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|first_value|ks|desc` | native | WRONG-READ | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|first_value|ks|part_default` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 4], [4, 4], [5, 1]]` | `[[1, 1], [2, 1], [3, 3], [4, 3], [5, 1]]` |
| `W|first_value|ks|part_desc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` |
| `W|first_value|multi|g_ki_desc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` |
| `W|first_value|multi|kd_desc_g` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|first_value|multi|ks_g_desc` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|lag|kd|asc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 2], [4, null], [5, 4]]` |
| `W|lag|kd|default` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 2], [4, null], [5, 4]]` |
| `W|lag|kd|desc` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, null], [4, 5], [5, 1]]` |
| `W|lag|kd|part_default` | native | WRONG-READ | `[[1, 5], [2, 1], [3, null], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 4], [4, null], [5, null]]` |
| `W|lag|kd|part_desc` | native | WRONG-READ | `[[1, 2], [2, null], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, null], [3, null], [4, 3], [5, 1]]` |
| `W|lag|ki|asc` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, 1], [5, 3]]` | `[[1, 2], [2, null], [3, 4], [4, 1], [5, 3]]` |
| `W|lag|ki|default` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, 1], [5, 3]]` | `[[1, 2], [2, null], [3, 4], [4, 1], [5, 3]]` |
| `W|lag|ki|desc` | native | WRONG-READ | `[[1, 4], [2, null], [3, 5], [4, 3], [5, 2]]` | `[[1, 4], [2, 1], [3, 5], [4, 3], [5, null]]` |
| `W|lag|ki|part_default` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, null], [3, 4], [4, null], [5, 1]]` |
| `W|lag|ki|part_desc` | native | WRONG-READ | `[[1, 5], [2, null], [3, null], [4, 3], [5, 2]]` | `[[1, 5], [2, 1], [3, null], [4, 3], [5, null]]` |
| `W|lag|ks|asc` | native | WRONG-READ | `[[1, 4], [2, 5], [3, 2], [4, null], [5, 1]]` | `[[1, 4], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|lag|ks|default` | native | WRONG-READ | `[[1, 4], [2, 5], [3, 2], [4, null], [5, 1]]` | `[[1, 4], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|lag|ks|desc` | native | WRONG-READ | `[[1, 5], [2, 3], [3, null], [4, 1], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, 1], [5, 2]]` |
| `W|lag|ks|part_default` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, null], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|lag|ks|part_desc` | native | WRONG-READ | `[[1, 5], [2, null], [3, null], [4, 3], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, null], [5, 2]]` |
| `W|lag|multi|g_desc_ki` | native | WRONG-READ | `[[1, 3], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` |
| `W|lag|multi|g_ki_desc` | native | WRONG-READ | `[[1, 5], [2, null], [3, 1], [4, 3], [5, 2]]` | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` |
| `W|lag|multi|kd_desc_g` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, null], [4, 5], [5, 1]]` |
| `W|lag|multi|ks_g_desc` | native | WRONG-READ | `[[1, 4], [2, 5], [3, 2], [4, null], [5, 1]]` | `[[1, 4], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|last_value|kd|asc` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|last_value|kd|default` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|last_value|kd|desc` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` |
| `W|last_value|kd|part_default` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` |
| `W|last_value|kd|part_desc` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 3], [4, 3], [5, 5]]` | `[[1, 5], [2, 5], [3, 4], [4, 4], [5, 5]]` |
| `W|last_value|ki|asc` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` |
| `W|last_value|ki|default` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` |
| `W|last_value|ki|desc` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 1], [4, 1], [5, 1]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|last_value|ki|part_default` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` | `[[1, 5], [2, 5], [3, 3], [4, 3], [5, 5]]` |
| `W|last_value|ki|part_desc` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 4], [4, 4], [5, 1]]` | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` |
| `W|last_value|ks|asc` | native | WRONG-READ | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|last_value|ks|default` | native | WRONG-READ | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|last_value|ks|desc` | native | WRONG-READ | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` |
| `W|last_value|ks|part_default` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 3], [4, 3], [5, 2]]` | `[[1, 2], [2, 2], [3, 4], [4, 4], [5, 2]]` |
| `W|last_value|ks|part_desc` | native | WRONG-READ | `[[1, 1], [2, 1], [3, 4], [4, 4], [5, 1]]` | `[[1, 1], [2, 1], [3, 3], [4, 3], [5, 1]]` |
| `W|last_value|multi|g_desc_ki` | native | WRONG-READ | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` |
| `W|last_value|multi|kd_desc_g` | native | WRONG-READ | `[[1, 5], [2, 5], [3, 5], [4, 5], [5, 5]]` | `[[1, 4], [2, 4], [3, 4], [4, 4], [5, 4]]` |
| `W|last_value|multi|ks_g_desc` | native | WRONG-READ | `[[1, 3], [2, 3], [3, 3], [4, 3], [5, 3]]` | `[[1, 2], [2, 2], [3, 2], [4, 2], [5, 2]]` |
| `W|lead|kd|asc` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, null], [4, 5], [5, 1]]` |
| `W|lead|kd|default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, null], [4, 5], [5, 1]]` |
| `W|lead|kd|desc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 2], [4, null], [5, 4]]` |
| `W|lead|kd|part_default` | native | WRONG-READ | `[[1, 2], [2, null], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, null], [3, null], [4, 3], [5, 1]]` |
| `W|lead|kd|part_desc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, null], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 4], [4, null], [5, null]]` |
| `W|lead|ki|asc` | native | WRONG-READ | `[[1, 4], [2, null], [3, 5], [4, 3], [5, 2]]` | `[[1, 4], [2, 1], [3, 5], [4, 3], [5, null]]` |
| `W|lead|ki|default` | native | WRONG-READ | `[[1, 4], [2, null], [3, 5], [4, 3], [5, 2]]` | `[[1, 4], [2, 1], [3, 5], [4, 3], [5, null]]` |
| `W|lead|ki|desc` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, 1], [5, 3]]` | `[[1, 2], [2, null], [3, 4], [4, 1], [5, 3]]` |
| `W|lead|ki|part_default` | native | WRONG-READ | `[[1, 5], [2, null], [3, null], [4, 3], [5, 2]]` | `[[1, 5], [2, 1], [3, null], [4, 3], [5, null]]` |
| `W|lead|ki|part_desc` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, null], [3, 4], [4, null], [5, 1]]` |
| `W|lead|ks|asc` | native | WRONG-READ | `[[1, 5], [2, 3], [3, null], [4, 1], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, 1], [5, 2]]` |
| `W|lead|ks|default` | native | WRONG-READ | `[[1, 5], [2, 3], [3, null], [4, 1], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, 1], [5, 2]]` |
| `W|lead|ks|desc` | native | WRONG-READ | `[[1, 4], [2, 5], [3, 2], [4, null], [5, 1]]` | `[[1, 4], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|lead|ks|part_default` | native | WRONG-READ | `[[1, 5], [2, null], [3, null], [4, 3], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, null], [5, 2]]` |
| `W|lead|ks|part_desc` | native | WRONG-READ | `[[1, null], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, null], [2, 5], [3, null], [4, 3], [5, 1]]` |
| `W|lead|multi|g_desc_ki` | native | WRONG-READ | `[[1, 5], [2, null], [3, 1], [4, 3], [5, 2]]` | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` |
| `W|lead|multi|g_ki_desc` | native | WRONG-READ | `[[1, 3], [2, 5], [3, 4], [4, null], [5, 1]]` | `[[1, 2], [2, 3], [3, 4], [4, null], [5, 1]]` |
| `W|lead|multi|kd_desc_g` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 2], [4, 3], [5, null]]` | `[[1, 5], [2, 1], [3, 2], [4, null], [5, 4]]` |
| `W|lead|multi|ks_g_desc` | native | WRONG-READ | `[[1, 5], [2, 3], [3, null], [4, 1], [5, 2]]` | `[[1, 5], [2, null], [3, 4], [4, 1], [5, 2]]` |
| `W|rank|kd|asc` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|rank|kd|default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|rank|kd|desc` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|rank|kd|part_default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` | `[[1, 2], [2, 3], [3, 2], [4, 1], [5, 1]]` |
| `W|rank|kd|part_desc` | native | WRONG-READ | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` | `[[1, 2], [2, 1], [3, 1], [4, 2], [5, 3]]` |
| `W|rank|ki|asc` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|rank|ki|default` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|rank|ki|desc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 3], [4, 4], [5, 2]]` | `[[1, 4], [2, 5], [3, 2], [4, 3], [5, 1]]` |
| `W|rank|ki|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` |
| `W|rank|ki|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` |
| `W|rank|ks|asc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|rank|ks|default` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|rank|ks|desc` | native | WRONG-READ | `[[1, 4], [2, 2], [3, 1], [4, 5], [5, 3]]` | `[[1, 3], [2, 1], [3, 5], [4, 4], [5, 2]]` |
| `W|rank|ks|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 1], [2, 3], [3, 1], [4, 2], [5, 2]]` |
| `W|rank|ks|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 3], [2, 1], [3, 2], [4, 1], [5, 2]]` |
| `W|rank|multi|g_desc_ki` | native | WRONG-READ | `[[1, 3], [2, 5], [3, 2], [4, 1], [5, 4]]` | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` |
| `W|rank|multi|g_ki_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 4], [4, 5], [5, 2]]` | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` |
| `W|rank|multi|kd_desc_g` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|rank|multi|ks_g_desc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|row_number|kd|asc` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|row_number|kd|default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` | `[[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]]` |
| `W|row_number|kd|desc` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|row_number|kd|part_default` | native | WRONG-READ | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` | `[[1, 2], [2, 3], [3, 2], [4, 1], [5, 1]]` |
| `W|row_number|kd|part_desc` | native | WRONG-READ | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` | `[[1, 2], [2, 1], [3, 1], [4, 2], [5, 3]]` |
| `W|row_number|ki|asc` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|row_number|ki|default` | native | WRONG-READ | `[[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]]` | `[[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]]` |
| `W|row_number|ki|desc` | native | WRONG-READ | `[[1, 5], [2, 1], [3, 3], [4, 4], [5, 2]]` | `[[1, 4], [2, 5], [3, 2], [4, 3], [5, 1]]` |
| `W|row_number|ki|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 2], [2, 1], [3, 2], [4, 1], [5, 3]]` |
| `W|row_number|ki|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 2], [2, 3], [3, 1], [4, 2], [5, 1]]` |
| `W|row_number|ks|asc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|row_number|ks|default` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|row_number|ks|desc` | native | WRONG-READ | `[[1, 4], [2, 2], [3, 1], [4, 5], [5, 3]]` | `[[1, 3], [2, 1], [3, 5], [4, 4], [5, 2]]` |
| `W|row_number|ks|part_default` | native | WRONG-READ | `[[1, 1], [2, 3], [3, 2], [4, 1], [5, 2]]` | `[[1, 1], [2, 3], [3, 1], [4, 2], [5, 2]]` |
| `W|row_number|ks|part_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 1], [4, 2], [5, 2]]` | `[[1, 3], [2, 1], [3, 2], [4, 1], [5, 2]]` |
| `W|row_number|multi|g_desc_ki` | native | WRONG-READ | `[[1, 3], [2, 5], [3, 2], [4, 1], [5, 4]]` | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` |
| `W|row_number|multi|g_ki_desc` | native | WRONG-READ | `[[1, 3], [2, 1], [3, 4], [4, 5], [5, 2]]` | `[[1, 2], [2, 3], [3, 4], [4, 5], [5, 1]]` |
| `W|row_number|multi|kd_desc_g` | native | WRONG-READ | `[[1, 4], [2, 3], [3, 2], [4, 1], [5, 5]]` | `[[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]]` |
| `W|row_number|multi|ks_g_desc` | native | WRONG-READ | `[[1, 2], [2, 4], [3, 5], [4, 1], [5, 3]]` | `[[1, 3], [2, 5], [3, 1], [4, 2], [5, 4]]` |
| `W|running_sum|kd|asc` | native | WRONG-READ | `[[1, 6], [2, 8], [3, 11], [4, 15], [5, 5]]` | `[[1, 10], [2, 12], [3, 15], [4, 4], [5, 9]]` |
| `W|running_sum|kd|default` | native | WRONG-READ | `[[1, 6], [2, 8], [3, 11], [4, 15], [5, 5]]` | `[[1, 10], [2, 12], [3, 15], [4, 4], [5, 9]]` |
| `W|running_sum|kd|desc` | native | WRONG-READ | `[[1, 10], [2, 9], [3, 7], [4, 4], [5, 15]]` | `[[1, 6], [2, 5], [3, 3], [4, 15], [5, 11]]` |
| `W|running_sum|kd|part_default` | native | WRONG-READ | `[[1, 6], [2, 8], [3, 3], [4, 7], [5, 5]]` | `[[1, 6], [2, 8], [3, 7], [4, 4], [5, 5]]` |
| `W|running_sum|kd|part_desc` | native | WRONG-READ | `[[1, 3], [2, 2], [3, 7], [4, 4], [5, 8]]` | `[[1, 3], [2, 2], [3, 3], [4, 7], [5, 8]]` |
| `W|running_sum|ki|asc` | native | WRONG-READ | `[[1, 1], [2, 15], [3, 8], [4, 5], [5, 13]]` | `[[1, 3], [2, 2], [3, 10], [4, 7], [5, 15]]` |
| `W|running_sum|ki|default` | native | WRONG-READ | `[[1, 1], [2, 15], [3, 8], [4, 5], [5, 13]]` | `[[1, 3], [2, 2], [3, 10], [4, 7], [5, 15]]` |
| `W|running_sum|ki|desc` | native | WRONG-READ | `[[1, 15], [2, 2], [3, 10], [4, 14], [5, 7]]` | `[[1, 13], [2, 15], [3, 8], [4, 12], [5, 5]]` |
| `W|running_sum|ki|part_default` | native | WRONG-READ | `[[1, 1], [2, 8], [3, 7], [4, 4], [5, 6]]` | `[[1, 3], [2, 2], [3, 7], [4, 4], [5, 8]]` |
| `W|running_sum|ki|part_desc` | native | WRONG-READ | `[[1, 8], [2, 2], [3, 3], [4, 7], [5, 7]]` | `[[1, 6], [2, 8], [3, 3], [4, 7], [5, 5]]` |
| `W|running_sum|ks|asc` | native | WRONG-READ | `[[1, 5], [2, 12], [3, 15], [4, 4], [5, 10]]` | `[[1, 8], [2, 15], [3, 3], [4, 7], [5, 13]]` |
| `W|running_sum|ks|default` | native | WRONG-READ | `[[1, 5], [2, 12], [3, 15], [4, 4], [5, 10]]` | `[[1, 8], [2, 15], [3, 3], [4, 7], [5, 13]]` |
| `W|running_sum|ks|desc` | native | WRONG-READ | `[[1, 11], [2, 5], [3, 3], [4, 15], [5, 10]]` | `[[1, 8], [2, 2], [3, 15], [4, 12], [5, 7]]` |
| `W|running_sum|ks|part_default` | native | WRONG-READ | `[[1, 1], [2, 8], [3, 7], [4, 4], [5, 6]]` | `[[1, 1], [2, 8], [3, 3], [4, 7], [5, 6]]` |
| `W|running_sum|ks|part_desc` | native | WRONG-READ | `[[1, 8], [2, 2], [3, 3], [4, 7], [5, 7]]` | `[[1, 8], [2, 2], [3, 7], [4, 4], [5, 7]]` |
| `W|running_sum|multi|g_desc_ki` | native | WRONG-READ | `[[1, 8], [2, 15], [3, 7], [4, 4], [5, 13]]` | `[[1, 10], [2, 9], [3, 7], [4, 4], [5, 15]]` |
| `W|running_sum|multi|g_ki_desc` | native | WRONG-READ | `[[1, 8], [2, 2], [3, 11], [4, 15], [5, 7]]` | `[[1, 6], [2, 8], [3, 11], [4, 15], [5, 5]]` |
| `W|running_sum|multi|kd_desc_g` | native | WRONG-READ | `[[1, 10], [2, 9], [3, 7], [4, 4], [5, 15]]` | `[[1, 6], [2, 5], [3, 3], [4, 15], [5, 11]]` |
| `W|running_sum|multi|ks_g_desc` | native | WRONG-READ | `[[1, 5], [2, 12], [3, 15], [4, 4], [5, 10]]` | `[[1, 8], [2, 15], [3, 3], [4, 7], [5, 13]]` |
| `X|array_sort_comparator` | fexpr | REFUSED-WHERE-SPARK-ACCEPTED | error: UnsupportedOperationException: This feature is not implemented: Unsupported ast node in sqltorel: Lambda(LambdaFunction … | `[[[null, 3, 1]]]` |
| `X|array_sort_comparator` | sparksql | REFUSED-WHERE-SPARK-ACCEPTED | error: UnsupportedOperationException: This feature is not implemented: Unsupported ast node in sqltorel: Lambda(LambdaFunction … | `[[[null, 3, 1]]]` |
