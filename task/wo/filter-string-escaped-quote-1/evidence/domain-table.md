# task/wo/filter-string-escaped-quote-1/evidence/domain-table.md

Measured cell-by-cell against live PySpark 4.1.2, RePark `main` (`01c15ffa2` / `af9ff7251`), and Delta 1 branch head (`1784f185b`; identical across `5327f5cb0` and `755c6e6ee`) in both ANSI modes.

## Core domain table (338 cells per ANSI mode; 676 total)

### ansi_true

| cell | Spark 4.1.2 | main | head | main_vs_spark | head_vs_spark | delta |
|---|---|---|---|---|---|---|
| `dqi_filter_str::dqi_bs_bs_end` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_bs_dq` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_doubled_dq` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_simple` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_bs_bs_end` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_bs_dq` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_doubled_dq` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_simple` | `[[1]]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_quote` | `[[18]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_quote` | `[[18]]` | `[]` | `[[1]]` | DIFF | DIFF | MOVED_TOWARD_SPARK |
| `esc_true_filter_str::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_bs` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[19]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_doubled` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[20]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_quote` | `[[18]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `filter_fexpr::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `filter_fexpr::adj_sq_with_escape` | `[[6]]` | `[[6]]` | `[[6]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_line_quote_eof` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_fexpr::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_line_quote_mid` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_dquote_col_after` | `[[9]]` | `[[9]]` | `[[9]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_fexpr::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_and_id` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_col_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[[8]]` | `[[8]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `filter_str::adj_sq_with_escape` | `[[6]]` | `[]` | `[[6]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_block_dquote` | `[[2]]` | `[]` | `[[2]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_line_quote_eof` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_line_quote_mid` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::dq_bs_dquote_col_after` | `[[9]]` | `[]` | `[[9]]` | DIFF | EQUAL | FIXED |
| `filter_str::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_str::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `filter_str::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_quote_and_id` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_bs_quote_col_after` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[]` | `[[8]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `join_fexpr::adj_sq` | `[[5, 5]]` | `[[5, 5]]` | `[[5, 5]]` | EQUAL | EQUAL | - |
| `join_fexpr::adj_sq_with_escape` | `[[6, 6]]` | `[[6, 6]]` | `[[6, 6]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_dquote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_nested_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_eof` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_lead` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_mid` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_dquote_col_after` | `[[9, 9]]` | `[[9, 9]]` | `[[9, 9]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_dquote_simple` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_n` | `[[4, 4]]` | `[[4, 4]]` | `[[4, 4]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_doubled_quote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_quad_quote` | `[[17, 17]]` | `[[17, 17]]` | `[[17, 17]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_single_bare_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `join_fexpr::raw_sq_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::raw_sq_bs_n` | `[[12, 12]]` | `[[12, 12]]` | `[[12, 12]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_bs_bs_quote` | `[[7, 7]]` | `[[7, 7]]` | `[[7, 7]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_dquote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_n` | `[[4, 4]]` | `[[4, 4]]` | `[[4, 4]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_and_id` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_col_after` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_kw_and_col_after` | `[[8, 8]]` | `[[8, 8]]` | `[[8, 8]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_doubled_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_quad_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_single_escaped_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_bs_end` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_bs_end_with_quote_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_doubled` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_is_not_null` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_column_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_bs_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_dq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_sq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_upper_R` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_x_hex_lit` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::backtick_bs_end` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::backtick_bs_end_with_quote_after` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::backtick_doubled` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_not_is_not_null` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_or_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_column_bare` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_bs_close` | `[[3]]` | `ERR(ParseException)` | `[[3]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_dq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_sq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_upper_R` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_x_hex_lit` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_selectexpr::backtick_bs_end` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::backtick_bs_end_with_quote_after` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::backtick_doubled` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_is_not_null` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_column_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_dq` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_sq` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_upper_R` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_x_hex_lit` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_bs_end` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_bs_end_with_quote_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_doubled` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_is_not_null` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_column_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_bs_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_dq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_sq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_upper_R` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_x_hex_lit` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::backtick_bs_end` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::backtick_bs_end_with_quote_after` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::backtick_doubled` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_not_is_not_null` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_or_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_column_bare` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_bs_close` | `[[3]]` | `ERR(ParseException)` | `[[3]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_dq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_sq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_upper_R` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_x_hex_lit` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `lex_sq_bs_then_adj` | `[["a'b"]]` | `[["a'b"]]` | `[["a'b"]]` | EQUAL | EQUAL | - |
| `lex_sq_bs_then_doubled_adj` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `lex_sq_bs_then_doubled_char` | `[["a''b"]]` | `[["a''b"]]` | `[["a''b"]]` | EQUAL | EQUAL | - |
| `seat_lambda_filter_sq_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_lambda_sql_sq_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_sql_collate_in_bs_sq` | `[[14]]` | `[[14]]` | `[[14]]` | EQUAL | EQUAL | - |
| `seat_sql_from_in_bs_sq` | `[[13]]` | `[]` | `[[13]]` | DIFF | EQUAL | FIXED |
| `seat_sql_from_in_doubled_sq` | `[[13]]` | `[[13]]` | `[[13]]` | EQUAL | EQUAL | - |
| `seat_using_filter_sq_bs` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_using_selectexpr_sq_bs` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | EQUAL | EQUAL | - |
| `selectexpr::adj_sq` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | EQUAL | EQUAL | - |
| `selectexpr::adj_sq_with_escape` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_dquote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_eof` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_lead` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_mid` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_dquote_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_dquote_simple` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | EQUAL | EQUAL | - |
| `selectexpr::dq_doubled_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::dq_quad_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | EQUAL | EQUAL | - |
| `selectexpr::dq_single_bare_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr::raw_sq_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::raw_sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_bs_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_dquote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_and_id` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_kw_and_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | EQUAL | EQUAL | - |
| `selectexpr::sq_doubled_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_quad_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr::sq_single_escaped_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_dq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_after_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_sq_bs_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_line_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_lit_only_dq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_lit_only_sq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_no_alias_sq_bs` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | `cols=['id', "(s = it'`s`)"] dtypes=[['id', 'bigint'], ["(s = it'`s`)", 'boolean']] n=20, true_ids=[]` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `sql_aliased::dotted_after_dq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_sq_bs_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_line_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_lit_only_dq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_lit_only_sq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_no_alias_sq_bs` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | `cols=['id', 't.s = Utf8("it\'t.s")'] dtypes=[['id', 'bigint'], ['t.s = Utf8("it\'t.s")', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 't.s = Utf8("it\'t.s")'] dtypes=[['id', 'bigint'], ['t.s = Utf8("it\'t.s")', 'boolean']] n=20, true_ids=[10]` | DIFF | DIFF | - |
| `sql_control::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `sql_control::adj_sq_with_escape` | `[[6]]` | `[[6]]` | `[[6]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_eof` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_mid` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_dquote_col_after` | `[[9]]` | `[[9]]` | `[[9]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `sql_control::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `sql_control::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `sql_control::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_and_id` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_col_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[[8]]` | `[[8]]` | EQUAL | EQUAL | - |
| `sql_control::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `sql_control::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `where_str::adj_sq_with_escape` | `[[6]]` | `[]` | `[[6]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_block_dquote` | `[[2]]` | `[]` | `[[2]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_line_quote_eof` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_line_quote_mid` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::dq_bs_dquote_col_after` | `[[9]]` | `[]` | `[[9]]` | DIFF | EQUAL | FIXED |
| `where_str::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `where_str::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `where_str::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_quote_and_id` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_bs_quote_col_after` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[]` | `[[8]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::adj_sq` | `[[5, True]]` | `[[5, True]]` | `[[5, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::adj_sq_with_escape` | `[[6, True]]` | `[[6, True]]` | `[[6, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_dquote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_nested_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_line_quote_eof` | `[[1, True]]` | `ERR(ParseException)` | `[[1, True]]` | DIFF | EQUAL | FIXED |
| `withcol_fexpr::comment_line_quote_lead` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_line_quote_mid` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_dquote_col_after` | `[[9, True]]` | `[[9, True]]` | `[[9, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_dquote_simple` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_n` | `[[4, True]]` | `[[4, True]]` | `[[4, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_doubled_quote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_quad_quote` | `[[17, True]]` | `[[17, True]]` | `[[17, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_single_bare_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::raw_sq_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::raw_sq_bs_n` | `[[12, True]]` | `[[12, True]]` | `[[12, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_bs_bs_quote` | `[[7, True]]` | `[[7, True]]` | `[[7, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_dquote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_n` | `[[4, True]]` | `[[4, True]]` | `[[4, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_and_id` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_col_after` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_kw_and_col_after` | `[[8, True]]` | `[[8, True]]` | `[[8, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_doubled_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_quad_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_single_escaped_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |

### ansi_false

| cell | Spark 4.1.2 | main | head | main_vs_spark | head_vs_spark | delta |
|---|---|---|---|---|---|---|
| `dqi_filter_str::dqi_bs_bs_end` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_bs_dq` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_doubled_dq` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_filter_str::dqi_simple` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_bs_bs_end` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_bs_dq` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_doubled_dq` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `dqi_sql::dqi_simple` | `[]` | `ERR(UnsupportedOperationException)` | `ERR(UnsupportedOperationException)` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_bs_quote` | `[[18]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_filter_fexpr::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_filter_str::esc_true_sq_bs_quote` | `[[18]]` | `[]` | `[[1]]` | DIFF | DIFF | MOVED_TOWARD_SPARK |
| `esc_true_filter_str::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_bs` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[19]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | DIFF | DIFF | - |
| `esc_true_selectexpr::esc_true_sq_doubled` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[20]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_bs` | `[[19]]` | `[[3]]` | `[[3]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_n` | `[[12]]` | `[[4]]` | `[[4]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_bs_quote` | `[[18]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `esc_true_sql::esc_true_sq_doubled` | `[[20]]` | `[[1]]` | `[[1]]` | DIFF | DIFF | - |
| `filter_fexpr::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `filter_fexpr::adj_sq_with_escape` | `[[6]]` | `[[6]]` | `[[6]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_line_quote_eof` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_fexpr::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::comment_line_quote_mid` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_dquote_col_after` | `[[9]]` | `[[9]]` | `[[9]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `filter_fexpr::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_fexpr::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_and_id` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_col_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[[8]]` | `[[8]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_fexpr::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `filter_str::adj_sq_with_escape` | `[[6]]` | `[]` | `[[6]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_block_dquote` | `[[2]]` | `[]` | `[[2]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_line_quote_eof` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::comment_line_quote_mid` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::dq_bs_dquote_col_after` | `[[9]]` | `[]` | `[[9]]` | DIFF | EQUAL | FIXED |
| `filter_str::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_str::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `filter_str::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `filter_str::sq_bs_quote_and_id` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_bs_quote_col_after` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[]` | `[[8]]` | DIFF | EQUAL | FIXED |
| `filter_str::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `filter_str::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `filter_str::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `join_fexpr::adj_sq` | `[[5, 5]]` | `[[5, 5]]` | `[[5, 5]]` | EQUAL | EQUAL | - |
| `join_fexpr::adj_sq_with_escape` | `[[6, 6]]` | `[[6, 6]]` | `[[6, 6]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_dquote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_nested_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_block_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_eof` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_lead` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::comment_line_quote_mid` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_dquote_col_after` | `[[9, 9]]` | `[[9, 9]]` | `[[9, 9]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_dquote_simple` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_bs_n` | `[[4, 4]]` | `[[4, 4]]` | `[[4, 4]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_doubled_quote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_quad_quote` | `[[17, 17]]` | `[[17, 17]]` | `[[17, 17]]` | EQUAL | EQUAL | - |
| `join_fexpr::dq_single_bare_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `join_fexpr::raw_sq_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::raw_sq_bs_n` | `[[12, 12]]` | `[[12, 12]]` | `[[12, 12]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_bs_before_close` | `[[3, 3]]` | `[[3, 3]]` | `[[3, 3]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_bs_bs_quote` | `[[7, 7]]` | `[[7, 7]]` | `[[7, 7]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_dquote` | `[[2, 2]]` | `[[2, 2]]` | `[[2, 2]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_n` | `[[4, 4]]` | `[[4, 4]]` | `[[4, 4]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_and_id` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_col_after` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_bs_quote_kw_and_col_after` | `[[8, 8]]` | `[[8, 8]]` | `[[8, 8]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_doubled_quote` | `[[1, 1]]` | `[[1, 1]]` | `[[1, 1]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_quad_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `join_fexpr::sq_single_escaped_quote` | `[[16, 16]]` | `[[16, 16]]` | `[[16, 16]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_bs_end` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_bs_end_with_quote_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::backtick_doubled` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_and_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_is_not_null` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_not_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_or_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_column_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_bs_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_dq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_sq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_r_raw_upper_R` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_filter_fexpr::kw_x_hex_lit` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::backtick_bs_end` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::backtick_bs_end_with_quote_after` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::backtick_doubled` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_not_is_not_null` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_not_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_filter_str::kw_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_or_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_column_bare` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_bs_close` | `[[3]]` | `ERR(ParseException)` | `[[3]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_dq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_sq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_r_raw_upper_R` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_filter_str::kw_x_hex_lit` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_selectexpr::backtick_bs_end` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::backtick_bs_end_with_quote_after` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::backtick_doubled` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_and_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_is_not_null` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_not_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_operator` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_or_quoted` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_column_bare` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[3]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_dq` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_sq` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_r_raw_upper_R` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[12]` | EQUAL | EQUAL | - |
| `kw_selectexpr::kw_x_hex_lit` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'int'], ['m', 'boolean']] n=4, true_ids=[1]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_bs_end` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_bs_end_with_quote_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::backtick_doubled` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_and_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_is_not_null` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_not_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_operator` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_or_quoted` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_column_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_bs_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_dq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_sq` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_r_raw_upper_R` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `kw_sql_control::kw_x_hex_lit` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::backtick_bs_end` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::backtick_bs_end_with_quote_after` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::backtick_doubled` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_not_is_not_null` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_not_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `kw_where_str::kw_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_or_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_column_bare` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_bs_close` | `[[3]]` | `ERR(ParseException)` | `[[3]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_dq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_sq` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_r_raw_upper_R` | `[[12]]` | `ERR(ParseException)` | `[[12]]` | DIFF | EQUAL | FIXED |
| `kw_where_str::kw_x_hex_lit` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `lex_sq_bs_then_adj` | `[["a'b"]]` | `[["a'b"]]` | `[["a'b"]]` | EQUAL | EQUAL | - |
| `lex_sq_bs_then_doubled_adj` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `lex_sq_bs_then_doubled_char` | `[["a''b"]]` | `[["a''b"]]` | `[["a''b"]]` | EQUAL | EQUAL | - |
| `seat_lambda_filter_sq_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_lambda_sql_sq_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_sql_collate_in_bs_sq` | `[[14]]` | `[[14]]` | `[[14]]` | EQUAL | EQUAL | - |
| `seat_sql_from_in_bs_sq` | `[[13]]` | `[]` | `[[13]]` | DIFF | EQUAL | FIXED |
| `seat_sql_from_in_doubled_sq` | `[[13]]` | `[[13]]` | `[[13]]` | EQUAL | EQUAL | - |
| `seat_using_filter_sq_bs` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `seat_using_selectexpr_sq_bs` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | EQUAL | EQUAL | - |
| `selectexpr::adj_sq` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[5]` | EQUAL | EQUAL | - |
| `selectexpr::adj_sq_with_escape` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[6]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_dquote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_eof` | `ERR(ParseException)` | `ERR(ParseException)` | `ERR(ParseException)` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_lead` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::comment_line_quote_mid` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_dquote_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[9]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_dquote_simple` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::dq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | EQUAL | EQUAL | - |
| `selectexpr::dq_doubled_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::dq_quad_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[17]` | EQUAL | EQUAL | - |
| `selectexpr::dq_single_bare_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr::raw_sq_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::raw_sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[12]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_bs_before_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_bs_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[7]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_dquote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[2]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_n` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[4]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_and_id` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_bs_quote_kw_and_col_after` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[8]` | EQUAL | EQUAL | - |
| `selectexpr::sq_doubled_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[1]` | EQUAL | EQUAL | - |
| `selectexpr::sq_quad_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr::sq_single_escaped_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[16]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_dq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_after_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_sq_bs_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `selectexpr_aliased::dotted_after_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_line_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_comment_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_lit_only_dq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_lit_only_sq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | DIFF | EQUAL | FIXED |
| `selectexpr_aliased::dotted_no_alias_sq_bs` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | `cols=['id', "(s = it'`s`)"] dtypes=[['id', 'bigint'], ["(s = it'`s`)", 'boolean']] n=20, true_ids=[]` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | DIFF | EQUAL | FIXED |
| `sql_aliased::dotted_after_dq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[11]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_raw_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_sq_bs_bs_close` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[3]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_after_sq_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_block_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_line_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_comment_nested_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=20, true_ids=[10]` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_lit_only_dq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_lit_only_sq_bs` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | `cols=['id', 'v'] dtypes=[['id', 'bigint'], ['v', 'string']] n=20` | EQUAL | EQUAL | - |
| `sql_aliased::dotted_no_alias_sq_bs` | `cols=['id', "(s = it't.s)"] dtypes=[['id', 'bigint'], ["(s = it't.s)", 'boolean']] n=20, true_ids=[10]` | `cols=['id', 't.s = Utf8("it\'t.s")'] dtypes=[['id', 'bigint'], ['t.s = Utf8("it\'t.s")', 'boolean']] n=20, true_ids=[10]` | `cols=['id', 't.s = Utf8("it\'t.s")'] dtypes=[['id', 'bigint'], ['t.s = Utf8("it\'t.s")', 'boolean']] n=20, true_ids=[10]` | DIFF | DIFF | - |
| `sql_control::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `sql_control::adj_sq_with_escape` | `[[6]]` | `[[6]]` | `[[6]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_eof` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::comment_line_quote_mid` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_dquote_col_after` | `[[9]]` | `[[9]]` | `[[9]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `sql_control::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `sql_control::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `sql_control::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_and_id` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_col_after` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[[8]]` | `[[8]]` | EQUAL | EQUAL | - |
| `sql_control::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `sql_control::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `sql_control::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::adj_sq` | `[[5]]` | `[[5]]` | `[[5]]` | EQUAL | EQUAL | - |
| `where_str::adj_sq_with_escape` | `[[6]]` | `[]` | `[[6]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_block_dquote` | `[[2]]` | `[]` | `[[2]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_block_nested_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_block_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_line_quote_eof` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::comment_line_quote_lead` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::comment_line_quote_mid` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::dq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::dq_bs_dquote_col_after` | `[[9]]` | `[]` | `[[9]]` | DIFF | EQUAL | FIXED |
| `where_str::dq_bs_dquote_simple` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::dq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `where_str::dq_doubled_quote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::dq_quad_quote` | `[[17]]` | `[[17]]` | `[[17]]` | EQUAL | EQUAL | - |
| `where_str::dq_single_bare_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::raw_sq_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::raw_sq_bs_n` | `[[12]]` | `[[12]]` | `[[12]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_bs_before_close` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_bs_bs_quote` | `[[7]]` | `[[7]]` | `[[7]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_dquote` | `[[2]]` | `[[2]]` | `[[2]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_n` | `[[4]]` | `[[4]]` | `[[4]]` | EQUAL | EQUAL | - |
| `where_str::sq_bs_quote_and_id` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_bs_quote_col_after` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_bs_quote_kw_and_col_after` | `[[8]]` | `[]` | `[[8]]` | DIFF | EQUAL | FIXED |
| `where_str::sq_doubled_quote` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `where_str::sq_quad_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `where_str::sq_single_escaped_quote` | `[[16]]` | `[[16]]` | `[[16]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::adj_sq` | `[[5, True]]` | `[[5, True]]` | `[[5, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::adj_sq_with_escape` | `[[6, True]]` | `[[6, True]]` | `[[6, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_dquote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_nested_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_block_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_line_quote_eof` | `[[1, True]]` | `ERR(ParseException)` | `[[1, True]]` | DIFF | EQUAL | FIXED |
| `withcol_fexpr::comment_line_quote_lead` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::comment_line_quote_mid` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_dquote_col_after` | `[[9, True]]` | `[[9, True]]` | `[[9, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_dquote_simple` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_bs_n` | `[[4, True]]` | `[[4, True]]` | `[[4, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_doubled_quote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_quad_quote` | `[[17, True]]` | `[[17, True]]` | `[[17, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::dq_single_bare_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::raw_sq_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::raw_sq_bs_n` | `[[12, True]]` | `[[12, True]]` | `[[12, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_bs_before_close` | `[[3, True]]` | `[[3, True]]` | `[[3, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_bs_bs_quote` | `[[7, True]]` | `[[7, True]]` | `[[7, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_dquote` | `[[2, True]]` | `[[2, True]]` | `[[2, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_n` | `[[4, True]]` | `[[4, True]]` | `[[4, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_and_id` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_col_after` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_bs_quote_kw_and_col_after` | `[[8, True]]` | `[[8, True]]` | `[[8, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_doubled_quote` | `[[1, True]]` | `[[1, True]]` | `[[1, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_quad_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |
| `withcol_fexpr::sq_single_escaped_quote` | `[[16, True]]` | `[[16, True]]` | `[[16, True]]` | EQUAL | EQUAL | - |

## Extra edge-case table (29 cells per ANSI mode; 58 total)

### ansi_True

| cell | Spark 4.1.2 | main | head | main_vs_spark | head_vs_spark | delta |
|---|---|---|---|---|---|---|
| `builder_esc_true_fexpr_bs_quote` | `[[18]]` | `[[18]]` | `[[18]]` | EQUAL | EQUAL | - |
| `builder_esc_true_filter_bs_bs` | `[[19]]` | `[[19]]` | `[[19]]` | EQUAL | EQUAL | - |
| `builder_esc_true_filter_bs_quote` | `[[18]]` | `[]` | `[[18]]` | DIFF | EQUAL | FIXED |
| `builder_esc_true_filter_doubled` | `[[20]]` | `[[20]]` | `[[20]]` | EQUAL | EQUAL | - |
| `builder_esc_true_selectexpr_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | EQUAL | EQUAL | - |
| `builder_esc_true_sql_bs_quote` | `[[18]]` | `[[18]]` | `[[18]]` | EQUAL | EQUAL | - |
| `comment_block_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `comment_line_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `comment_nested_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `lambda_filter_uppercase_X_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `lex_raw_adj_regular_bs` | `[["its'x"]]` | `ERR(ParseException)` | `ERR(ParseException)` | DIFF | DIFF | - |
| `lex_raw_adj_regular_bs_filter` | `[[1]]` | `ERR(ParseException)` | `ERR(ParseException)` | DIFF | DIFF | - |
| `mc_and_and_or` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_and_bare_lower` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_is_not_null_and_not_col` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_r_raw_sq` | `[[2]]` | `ERR(ParseException)` | `[[2]]` | DIFF | EQUAL | FIXED |
| `mc_r_raw_upper_R` | `[[2]]` | `ERR(ParseException)` | `[[2]]` | DIFF | EQUAL | FIXED |
| `selectexpr_trailing_line_comment` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[2]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[2]` | DIFF | EQUAL | FIXED |
| `selectexpr_trailing_line_comment_no_alias` | `cols=['id', "(s = it's)"] dtypes=[['id', 'bigint'], ["(s = it's)", 'boolean']] n=4, true_ids=[2]` | `ERR(AnalysisException)` | `cols=['id', "(s = it's)"] dtypes=[['id', 'bigint'], ["(s = it's)", 'boolean']] n=4, true_ids=[2]` | DIFF | EQUAL | FIXED |
| `sql_type_collate_in_bs_sq` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `using_filter_right_key_in_bs_sq` | `[[4]]` | `[]` | `[[4]]` | DIFF | EQUAL | FIXED |
| `using_selectexpr_right_key_in_bs_sq` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[4]` | DIFF | EQUAL | FIXED |

### ansi_False

| cell | Spark 4.1.2 | main | head | main_vs_spark | head_vs_spark | delta |
|---|---|---|---|---|---|---|
| `builder_esc_true_fexpr_bs_quote` | `[[18]]` | `[[18]]` | `[[18]]` | EQUAL | EQUAL | - |
| `builder_esc_true_filter_bs_bs` | `[[19]]` | `[[19]]` | `[[19]]` | EQUAL | EQUAL | - |
| `builder_esc_true_filter_bs_quote` | `[[18]]` | `[]` | `[[18]]` | DIFF | EQUAL | FIXED |
| `builder_esc_true_filter_doubled` | `[[20]]` | `[[20]]` | `[[20]]` | EQUAL | EQUAL | - |
| `builder_esc_true_selectexpr_bs_quote` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=5, true_ids=[18]` | EQUAL | EQUAL | - |
| `builder_esc_true_sql_bs_quote` | `[[18]]` | `[[18]]` | `[[18]]` | EQUAL | EQUAL | - |
| `comment_block_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `comment_line_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `comment_nested_quote_col_a_in_lit` | `[[1]]` | `[]` | `[[1]]` | DIFF | EQUAL | FIXED |
| `lambda_filter_uppercase_X_bs_paren` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `lex_raw_adj_regular_bs` | `[["its'x"]]` | `ERR(ParseException)` | `ERR(ParseException)` | DIFF | DIFF | - |
| `lex_raw_adj_regular_bs_filter` | `[[1]]` | `ERR(ParseException)` | `ERR(ParseException)` | DIFF | DIFF | - |
| `mc_and_and_or` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_and_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_and_bare_lower` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_and_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_and_quoted` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_is_not_null_and_not_col` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_not_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_not_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_or_bare` | `[[1]]` | `[[1]]` | `[[1]]` | EQUAL | EQUAL | - |
| `mc_or_operator` | `[[1]]` | `ERR(ParseException)` | `[[1]]` | DIFF | EQUAL | FIXED |
| `mc_r_raw_sq` | `[[2]]` | `ERR(ParseException)` | `[[2]]` | DIFF | EQUAL | FIXED |
| `mc_r_raw_upper_R` | `[[2]]` | `ERR(ParseException)` | `[[2]]` | DIFF | EQUAL | FIXED |
| `selectexpr_trailing_line_comment` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[2]` | `ERR(AnalysisException)` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[2]` | DIFF | EQUAL | FIXED |
| `selectexpr_trailing_line_comment_no_alias` | `cols=['id', "(s = it's)"] dtypes=[['id', 'bigint'], ["(s = it's)", 'boolean']] n=4, true_ids=[2]` | `ERR(AnalysisException)` | `cols=['id', "(s = it's)"] dtypes=[['id', 'bigint'], ["(s = it's)", 'boolean']] n=4, true_ids=[2]` | DIFF | EQUAL | FIXED |
| `sql_type_collate_in_bs_sq` | `[[3]]` | `[[3]]` | `[[3]]` | EQUAL | EQUAL | - |
| `using_filter_right_key_in_bs_sq` | `[[4]]` | `[]` | `[[4]]` | DIFF | EQUAL | FIXED |
| `using_selectexpr_right_key_in_bs_sq` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[4]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[]` | `cols=['id', 'm'] dtypes=[['id', 'bigint'], ['m', 'boolean']] n=4, true_ids=[4]` | DIFF | EQUAL | FIXED |
