# Step 0 legs

- Spark: `{"engine": "spark", "version": "4.1.2"}`
- Base: `{"engine": "repark", "version": "1.5.6", "native_module": "_native.abi3.so"}`

| Door | Base label | Cells |
|---|---|---|
| control | BASE-EQUAL | 2 |
| control | EQUAL | 10 |
| filter_fexpr | EQUAL | 28 |
| filter_str_case_sensitive | EQUAL | 8 |
| filter_str_case_sensitive | WRONG-READ | 20 |
| filter_str_created | EQUAL | 8 |
| filter_str_created | WRONG-READ | 20 |
| filter_str_derived | EQUAL | 8 |
| filter_str_derived | WRONG-READ | 20 |
| filter_str_sql_frame | EQUAL | 8 |
| filter_str_sql_frame | WRONG-READ | 20 |
| filter_str_table | EQUAL | 8 |
| filter_str_table | WRONG-READ | 20 |
| join_filter_column | WRONG-READ | 3 |
| join_filter_str | WRONG-READ | 3 |
| native_filter_str | BASE-EQUAL | 28 |
| native_sql_where | BASE-EQUAL | 28 |
| selectexpr | EQUAL | 23 |
| selectexpr | REFUSED-WHERE-SPARK-ANSWERS | 5 |
| sparksql_where | EQUAL | 28 |
| where_str_table | EQUAL | 8 |
| where_str_table | WRONG-READ | 20 |

## Every cell

| Cell | Door | Spark | Base | Base label |
|---|---|---|---|---|
| `fexpr_window_string_desc` | control | [[1, 3], [2, 1], [3, 5], [4, 4], [5, 2]] | [[1, 3], [2, 1], [3, 5], [4, 4], [5, 2]] | EQUAL |
| `native_order_by_int` | control | "n/a" | [[1], [4], [3], [5], [2]] | BASE-EQUAL |
| `native_window_int` | control | "n/a" | [[1, 1], [2, 5], [3, 3], [4, 2], [5, 4]] | BASE-EQUAL |
| `selectexpr_window_double` | control | [[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]] | [[1, 3], [2, 4], [3, 5], [4, 1], [5, 2]] | EQUAL |
| `sort_double_asc` | control | [[4], [5], [1], [2], [3]] | [[4], [5], [1], [2], [3]] | EQUAL |
| `sort_int_asc` | control | [[2], [1], [4], [3], [5]] | [[2], [1], [4], [3], [5]] | EQUAL |
| `sort_int_desc` | control | [[5], [3], [4], [1], [2]] | [[5], [3], [4], [1], [2]] | EQUAL |
| `sort_string_desc` | control | [[2], [5], [1], [4], [3]] | [[2], [5], [1], [4], [3]] | EQUAL |
| `sparksql_order_by_string_desc` | control | [[2], [5], [1], [4], [3]] | [[2], [5], [1], [4], [3]] | EQUAL |
| `sparksql_window_int` | control | [[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]] | [[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]] | EQUAL |
| `typed_window_double_desc` | control | [[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]] | [[1, 3], [2, 2], [3, 1], [4, 5], [5, 4]] | EQUAL |
| `typed_window_int_asc` | control | [[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]] | [[1, 2], [2, 1], [3, 4], [4, 3], [5, 5]] | EQUAL |
| `double_asc_is_null` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `double_asc_nulls_last` | filter_fexpr | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_fexpr | [[2]] | [[2]] | EQUAL |
| `double_desc_nan` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `double_in_asc` | filter_fexpr | [[5]] | [[5]] | EQUAL |
| `int_asc_is_null` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_kw_is_null` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_first` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_fexpr | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_desc_eq` | filter_fexpr | [[4]] | [[4]] | EQUAL |
| `int_desc_gt` | filter_fexpr | [[3], [4], [5]] | [[3], [4], [5]] | EQUAL |
| `int_desc_nulls_first` | filter_fexpr | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_fexpr | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_in_asc` | filter_fexpr | [[1]] | [[1]] | EQUAL |
| `int_in_desc` | filter_fexpr | [[1], [4]] | [[1], [4]] | EQUAL |
| `int_multi_key` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_multi_key_explicit` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_in_asc` | filter_fexpr | [] | [] | EQUAL |
| `int_row_number_in_subquery` | filter_fexpr | [[1]] | [[1]] | EQUAL |
| `int_window_explicit` | filter_fexpr | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `string_asc_is_null` | filter_fexpr | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `string_desc_eq` | filter_fexpr | [[1]] | [[1]] | EQUAL |
| `string_desc_nulls_first` | filter_fexpr | [] | [] | EQUAL |
| `string_in_asc` | filter_fexpr | [[4]] | [[4]] | EQUAL |
| `double_asc_is_null` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | filter_str_case_sensitive | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_str_case_sensitive | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | filter_str_case_sensitive | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_str_case_sensitive | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | filter_str_case_sensitive | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | filter_str_case_sensitive | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | filter_str_case_sensitive | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_str_case_sensitive | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | filter_str_case_sensitive | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | filter_str_case_sensitive | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | filter_str_case_sensitive | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | filter_str_case_sensitive | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | filter_str_case_sensitive | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | filter_str_case_sensitive | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | filter_str_case_sensitive | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | filter_str_case_sensitive | [] | [] | EQUAL |
| `string_in_asc` | filter_str_case_sensitive | [[4]] | [[2], [4]] | WRONG-READ |
| `double_asc_is_null` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | filter_str_created | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_str_created | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | filter_str_created | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | filter_str_created | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_str_created | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | filter_str_created | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | filter_str_created | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | filter_str_created | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_str_created | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | filter_str_created | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | filter_str_created | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | filter_str_created | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | filter_str_created | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | filter_str_created | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | filter_str_created | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | filter_str_created | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | filter_str_created | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | filter_str_created | [] | [] | EQUAL |
| `string_in_asc` | filter_str_created | [[4]] | [[2], [4]] | WRONG-READ |
| `double_asc_is_null` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | filter_str_derived | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_str_derived | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | filter_str_derived | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | filter_str_derived | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_str_derived | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | filter_str_derived | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | filter_str_derived | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | filter_str_derived | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_str_derived | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | filter_str_derived | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | filter_str_derived | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | filter_str_derived | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | filter_str_derived | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | filter_str_derived | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | filter_str_derived | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | filter_str_derived | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | filter_str_derived | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | filter_str_derived | [] | [] | EQUAL |
| `string_in_asc` | filter_str_derived | [[4]] | [[2], [4]] | WRONG-READ |
| `double_asc_is_null` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | filter_str_sql_frame | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_str_sql_frame | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | filter_str_sql_frame | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_str_sql_frame | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | filter_str_sql_frame | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | filter_str_sql_frame | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | filter_str_sql_frame | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_str_sql_frame | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | filter_str_sql_frame | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | filter_str_sql_frame | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | filter_str_sql_frame | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | filter_str_sql_frame | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | filter_str_sql_frame | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | filter_str_sql_frame | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | filter_str_sql_frame | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | filter_str_sql_frame | [] | [] | EQUAL |
| `string_in_asc` | filter_str_sql_frame | [[4]] | [[2], [4]] | WRONG-READ |
| `double_asc_is_null` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | filter_str_table | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | filter_str_table | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | filter_str_table | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | filter_str_table | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | filter_str_table | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | filter_str_table | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | filter_str_table | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | filter_str_table | [] | [] | EQUAL |
| `int_desc_nulls_last` | filter_str_table | [[4]] | [[4]] | EQUAL |
| `int_exists` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | filter_str_table | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | filter_str_table | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | filter_str_table | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | filter_str_table | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | filter_str_table | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | filter_str_table | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | filter_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | filter_str_table | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | filter_str_table | [] | [] | EQUAL |
| `string_in_asc` | filter_str_table | [[4]] | [[2], [4]] | WRONG-READ |
| `join_double_desc` | join_filter_column | [[2]] | [] | WRONG-READ |
| `join_int_desc` | join_filter_column | [[4]] | [] | WRONG-READ |
| `join_string_desc` | join_filter_column | [[1]] | [] | WRONG-READ |
| `join_double_desc` | join_filter_str | [[2]] | [] | WRONG-READ |
| `join_int_desc` | join_filter_str | [[4]] | [] | WRONG-READ |
| `join_string_desc` | join_filter_str | [[1]] | [] | WRONG-READ |
| `double_asc_is_null` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `double_asc_nulls_last` | native_filter_str | "n/a" | [[5]] | BASE-EQUAL |
| `double_desc_eq` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `double_desc_nan` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `double_in_asc` | native_filter_str | "n/a" | [[1], [5]] | BASE-EQUAL |
| `int_asc_is_null` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_asc_kw_is_null` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_asc_nulls_first` | native_filter_str | "n/a" | [[1], [2], [3], [4], [5]] | BASE-EQUAL |
| `int_asc_nulls_last` | native_filter_str | "n/a" | [[1]] | BASE-EQUAL |
| `int_count_derived` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_desc_eq` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_desc_gt` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_desc_nulls_first` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_desc_nulls_last` | native_filter_str | "n/a" | [[4]] | BASE-EQUAL |
| `int_exists` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_in_asc` | native_filter_str | "n/a" | [[1], [4]] | BASE-EQUAL |
| `int_in_desc` | native_filter_str | "n/a" | [[4]] | BASE-EQUAL |
| `int_multi_key` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_multi_key_explicit` | native_filter_str | "n/a" | [[1], [2], [3], [4], [5]] | BASE-EQUAL |
| `int_not_exists` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_not_in_asc` | native_filter_str | "n/a" | [[3], [5]] | BASE-EQUAL |
| `int_row_number_in_subquery` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `int_window_explicit` | native_filter_str | "n/a" | [[1]] | BASE-EQUAL |
| `int_window_in_subquery` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `string_asc_is_null` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `string_desc_eq` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `string_desc_nulls_first` | native_filter_str | "n/a" | [] | BASE-EQUAL |
| `string_in_asc` | native_filter_str | "n/a" | [[2], [4]] | BASE-EQUAL |
| `double_asc_is_null` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `double_asc_nulls_last` | native_sql_where | "n/a" | [[5]] | BASE-EQUAL |
| `double_desc_eq` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `double_desc_nan` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `double_in_asc` | native_sql_where | "n/a" | [[1], [5]] | BASE-EQUAL |
| `int_asc_is_null` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_asc_kw_is_null` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_asc_nulls_first` | native_sql_where | "n/a" | [[1], [2], [3], [4], [5]] | BASE-EQUAL |
| `int_asc_nulls_last` | native_sql_where | "n/a" | [[1]] | BASE-EQUAL |
| `int_count_derived` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_desc_eq` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_desc_gt` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_desc_nulls_first` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_desc_nulls_last` | native_sql_where | "n/a" | [[4]] | BASE-EQUAL |
| `int_exists` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_in_asc` | native_sql_where | "n/a" | [[1], [4]] | BASE-EQUAL |
| `int_in_desc` | native_sql_where | "n/a" | [[4]] | BASE-EQUAL |
| `int_multi_key` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_multi_key_explicit` | native_sql_where | "n/a" | [[1], [2], [3], [4], [5]] | BASE-EQUAL |
| `int_not_exists` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_not_in_asc` | native_sql_where | "n/a" | [[3], [5]] | BASE-EQUAL |
| `int_row_number_in_subquery` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `int_window_explicit` | native_sql_where | "n/a" | [[1]] | BASE-EQUAL |
| `int_window_in_subquery` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `string_asc_is_null` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `string_desc_eq` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `string_desc_nulls_first` | native_sql_where | "n/a" | [] | BASE-EQUAL |
| `string_in_asc` | native_sql_where | "n/a" | [[2], [4]] | BASE-EQUAL |
| `double_asc_is_null` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `double_asc_nulls_last` | selectexpr | [[1, false], [2, false], [3, false], [4, null], [5, true]] | [[1, false], [2, false], [3, false], [4, null], [5, true]] | EQUAL |
| `double_desc_eq` | selectexpr | [[1, false], [2, true], [3, false], [4, null], [5, false]] | [[1, false], [2, true], [3, false], [4, null], [5, false]] | EQUAL |
| `double_desc_nan` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `double_in_asc` | selectexpr | [[1, false], [2, false], [3, false], [4, false], [5, true]] | "ERR:UnsupportedOperationException:This feature is not implemented: Physical plan does not support logical expression InSubquery(InSubquery { expr: Column(Column { relation: Some(Full { catalog: \"datafusion\", schema: \"public\", table: \"" | REFUSED-WHERE-SPARK-ANSWERS |
| `int_asc_is_null` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_asc_kw_is_null` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_asc_nulls_first` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_asc_nulls_last` | selectexpr | [[1, true], [2, null], [3, false], [4, false], [5, false]] | [[1, true], [2, null], [3, false], [4, false], [5, false]] | EQUAL |
| `int_count_derived` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_desc_eq` | selectexpr | [[1, false], [2, null], [3, false], [4, true], [5, false]] | [[1, false], [2, null], [3, false], [4, true], [5, false]] | EQUAL |
| `int_desc_gt` | selectexpr | [[1, false], [2, null], [3, true], [4, true], [5, true]] | [[1, false], [2, null], [3, true], [4, true], [5, true]] | EQUAL |
| `int_desc_nulls_first` | selectexpr | [[1, null], [2, null], [3, null], [4, null], [5, null]] | [[1, null], [2, null], [3, null], [4, null], [5, null]] | EQUAL |
| `int_desc_nulls_last` | selectexpr | [[1, false], [2, null], [3, false], [4, true], [5, false]] | [[1, false], [2, null], [3, false], [4, true], [5, false]] | EQUAL |
| `int_exists` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_in_asc` | selectexpr | [[1, true], [2, false], [3, false], [4, false], [5, false]] | "ERR:UnsupportedOperationException:This feature is not implemented: Physical plan does not support logical expression InSubquery(InSubquery { expr: Column(Column { relation: Some(Full { catalog: \"datafusion\", schema: \"public\", table: \"" | REFUSED-WHERE-SPARK-ANSWERS |
| `int_in_desc` | selectexpr | [[1, true], [2, false], [3, false], [4, true], [5, false]] | "ERR:UnsupportedOperationException:This feature is not implemented: Physical plan does not support logical expression InSubquery(InSubquery { expr: Column(Column { relation: Some(Full { catalog: \"datafusion\", schema: \"public\", table: \"" | REFUSED-WHERE-SPARK-ANSWERS |
| `int_multi_key` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_multi_key_explicit` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_not_exists` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `int_not_in_asc` | selectexpr | [[1, false], [2, false], [3, false], [4, false], [5, false]] | "ERR:UnsupportedOperationException:This feature is not implemented: Physical plan does not support logical expression InSubquery(InSubquery { expr: Column(Column { relation: Some(Full { catalog: \"datafusion\", schema: \"public\", table: \"" | REFUSED-WHERE-SPARK-ANSWERS |
| `int_row_number_in_subquery` | selectexpr | [[1, true], [2, null], [3, false], [4, false], [5, false]] | [[1, true], [2, null], [3, false], [4, false], [5, false]] | EQUAL |
| `int_window_explicit` | selectexpr | [[1, true], [2, null], [3, false], [4, false], [5, false]] | [[1, true], [2, null], [3, false], [4, false], [5, false]] | EQUAL |
| `int_window_in_subquery` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `string_asc_is_null` | selectexpr | [[1, true], [2, true], [3, true], [4, true], [5, true]] | [[1, true], [2, true], [3, true], [4, true], [5, true]] | EQUAL |
| `string_desc_eq` | selectexpr | [[1, true], [2, false], [3, null], [4, false], [5, false]] | [[1, true], [2, false], [3, null], [4, false], [5, false]] | EQUAL |
| `string_desc_nulls_first` | selectexpr | [[1, null], [2, null], [3, null], [4, null], [5, null]] | [[1, null], [2, null], [3, null], [4, null], [5, null]] | EQUAL |
| `string_in_asc` | selectexpr | [[1, false], [2, false], [3, false], [4, true], [5, false]] | "ERR:UnsupportedOperationException:This feature is not implemented: Physical plan does not support logical expression InSubquery(InSubquery { expr: Column(Column { relation: Some(Full { catalog: \"datafusion\", schema: \"public\", table: \"" | REFUSED-WHERE-SPARK-ANSWERS |
| `double_asc_is_null` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `double_asc_nulls_last` | sparksql_where | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | sparksql_where | [[2]] | [[2]] | EQUAL |
| `double_desc_nan` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `double_in_asc` | sparksql_where | [[5]] | [[5]] | EQUAL |
| `int_asc_is_null` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_kw_is_null` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_first` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | sparksql_where | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_desc_eq` | sparksql_where | [[4]] | [[4]] | EQUAL |
| `int_desc_gt` | sparksql_where | [[3], [4], [5]] | [[3], [4], [5]] | EQUAL |
| `int_desc_nulls_first` | sparksql_where | [] | [] | EQUAL |
| `int_desc_nulls_last` | sparksql_where | [[4]] | [[4]] | EQUAL |
| `int_exists` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_in_asc` | sparksql_where | [[1]] | [[1]] | EQUAL |
| `int_in_desc` | sparksql_where | [[1], [4]] | [[1], [4]] | EQUAL |
| `int_multi_key` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_multi_key_explicit` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_in_asc` | sparksql_where | [] | [] | EQUAL |
| `int_row_number_in_subquery` | sparksql_where | [[1]] | [[1]] | EQUAL |
| `int_window_explicit` | sparksql_where | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `string_asc_is_null` | sparksql_where | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `string_desc_eq` | sparksql_where | [[1]] | [[1]] | EQUAL |
| `string_desc_nulls_first` | sparksql_where | [] | [] | EQUAL |
| `string_in_asc` | sparksql_where | [[4]] | [[4]] | EQUAL |
| `double_asc_is_null` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_asc_nulls_last` | where_str_table | [[5]] | [[5]] | EQUAL |
| `double_desc_eq` | where_str_table | [[2]] | [] | WRONG-READ |
| `double_desc_nan` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `double_in_asc` | where_str_table | [[5]] | [[1], [5]] | WRONG-READ |
| `int_asc_is_null` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_kw_is_null` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_asc_nulls_first` | where_str_table | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_asc_nulls_last` | where_str_table | [[1]] | [[1]] | EQUAL |
| `int_count_derived` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_desc_eq` | where_str_table | [[4]] | [] | WRONG-READ |
| `int_desc_gt` | where_str_table | [[3], [4], [5]] | [] | WRONG-READ |
| `int_desc_nulls_first` | where_str_table | [] | [] | EQUAL |
| `int_desc_nulls_last` | where_str_table | [[4]] | [[4]] | EQUAL |
| `int_exists` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_in_asc` | where_str_table | [[1]] | [[1], [4]] | WRONG-READ |
| `int_in_desc` | where_str_table | [[1], [4]] | [[4]] | WRONG-READ |
| `int_multi_key` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_multi_key_explicit` | where_str_table | [[1], [2], [3], [4], [5]] | [[1], [2], [3], [4], [5]] | EQUAL |
| `int_not_exists` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `int_not_in_asc` | where_str_table | [] | [[3], [5]] | WRONG-READ |
| `int_row_number_in_subquery` | where_str_table | [[1]] | [] | WRONG-READ |
| `int_window_explicit` | where_str_table | [[1]] | [[1]] | EQUAL |
| `int_window_in_subquery` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_asc_is_null` | where_str_table | [[1], [2], [3], [4], [5]] | [] | WRONG-READ |
| `string_desc_eq` | where_str_table | [[1]] | [] | WRONG-READ |
| `string_desc_nulls_first` | where_str_table | [] | [] | EQUAL |
| `string_in_asc` | where_str_table | [[4]] | [[2], [4]] | WRONG-READ |
