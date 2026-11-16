# Grids (written by grids.py)

### repark, table `t` (ANSI off; `x/y` where ANSI on differs)

| position | ab | ac | ad | bc | ef | gh | i64h | i32w30 | i16w34 | i8w36 | c0c | i32c | i64c | k28k4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `eq_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `ne` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `lt` | R | W | = | R | = | W | W | W | W | W | = | = | = | = |
| `le` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `gt` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `nseq` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `in1` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `in_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `case` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `case_simple` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `if` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `coalesce` | W | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `nvl` | W | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `nvl2` | R | T |  |  |  | T | T | T |  |  | = |  |  |  |
| `nullif` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `greatest` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `least` | R | W |  |  |  | W | T | W |  |  | = |  |  |  |
| `array` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `map` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `array_contains` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `array_union` | B | M |  |  |  | M | M | M |  |  | M |  |  |  |
| `map_concat` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `lag` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lead` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `union_all` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union_all_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `union3` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `except` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `intersect` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `where_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `where_eq_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_lt` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `join_nseq` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_full_using` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `scalar_subquery` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `exists_corr` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |

### repark, table `tx` (ANSI off; `x/y` where ANSI on differs)

| position | ab | ac | ad | bc | ef | gh | i64h | i32w30 | i16w34 | i8w36 | c0c | i32c | i64c | k28k4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `eq_rev` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `ne` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `lt` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `le` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `gt` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `nseq` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `between` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `between_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `in1` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `in_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `not_in` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `case` | R | R | R | R | R | W | R | R | R | R | = | = | = | = |
| `case_simple` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `if` | R | R |  |  |  | W | R | R |  |  | = |  |  |  |
| `coalesce` | W | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `nvl` | T | W |  |  |  | W | W | T |  |  | T |  |  |  |
| `nvl2` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `nullif` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `greatest` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `least` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `array` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `map` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `array_contains` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `array_union` | B | B |  |  |  | B | B | B |  |  | M |  |  |  |
| `map_concat` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `lag` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lead` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `union_all` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `union_all_rev` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `union` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `union3` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `except` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `intersect` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `where_eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `where_eq_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_lt` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_in` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_between` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `join_eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `join_nseq` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `join_full_using` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `in_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `not_in_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `scalar_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |

### native, table `t` (ANSI off; `x/y` where ANSI on differs)

| position | ab | ac | ad | bc | ef | gh | i64h | i32w30 | i16w34 | i8w36 | c0c | i32c | i64c | k28k4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `eq_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `ne` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `lt` | R | W | = | R | = | W | W | W | W | W | = | = | = | = |
| `le` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `gt` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `nseq` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `in1` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `in_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `case` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `case_simple` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `if` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `coalesce` | W | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `nvl` | W | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `nvl2` | R | T |  |  |  | T | T | T |  |  | = |  |  |  |
| `nullif` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `greatest` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `least` | R | W |  |  |  | W | T | W |  |  | = |  |  |  |
| `array` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `map` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `array_contains` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `array_union` | B | B |  |  |  | B | B | B |  |  | B |  |  |  |
| `map_concat` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lag` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lead` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `union_all` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union_all_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `union3` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `except` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `intersect` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `where_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `where_eq_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_lt` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `join_nseq` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_full_using` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `scalar_subquery` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `exists_corr` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |

### native, table `tx` (ANSI off; `x/y` where ANSI on differs)

| position | ab | ac | ad | bc | ef | gh | i64h | i32w30 | i16w34 | i8w36 | c0c | i32c | i64c | k28k4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `eq_rev` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `ne` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `lt` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `le` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `gt` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `nseq` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `between` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `between_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `in1` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `in_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `not_in` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `case` | R | R | R | R | R | W | R | R | R | R | = | = | = | = |
| `case_simple` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `if` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `coalesce` | W | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `nvl` | T | W |  |  |  | W | W | T |  |  | T |  |  |  |
| `nvl2` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `nullif` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `greatest` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `least` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `array` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `map` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `array_contains` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `array_union` | B | B |  |  |  | B | B | B |  |  | B |  |  |  |
| `map_concat` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lag` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lead` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `union_all` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `union_all_rev` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `union` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `union3` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `except` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `intersect` | R | R |  |  |  | R | R | R |  |  | T |  |  |  |
| `where_eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `where_eq_rev` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_lt` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_in` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `where_between` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `join_eq` | R | R | R | R | R | R | R | R | R | R | = | = | = | = |
| `join_nseq` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `join_full_using` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `in_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `not_in_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |
| `scalar_subquery` | R | R |  |  |  | R | R | R |  |  | = |  |  |  |

### group `three` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `three/array3/t` | W | W | R |
| `three/array3/tx` | R | R | R |
| `three/case3/t` | W | W | W |
| `three/case3/tx` | R | R | R |
| `three/coalesce3/t` | W | W | W |
| `three/coalesce3/tx` | W | W | W |
| `three/coalesce_nested/t` | W | W | W |
| `three/coalesce_nested/tx` | W | W | W |
| `three/greatest3/t` | W | W | W |
| `three/greatest3/tx` | R | R | R |
| `three/in3/t` | W | W | W |
| `three/in3/tx` | R | R | R |
| `three/least3/t` | W | W | W |
| `three/least3/tx` | R | R | R |
| `three/u3_flat/t` | W | W | W |
| `three/u3_flat/tx` | R | R | R |
| `three/u3_int/t` | W | W | W |
| `three/u3_int/tx` | R | R | R |
| `three/u3_left/t` | W | W | W |
| `three/u3_left/tx` | R | R | R |
| `three/u3_right/t` | W | W | W |
| `three/u3_right/tx` | R | R | R |
| `three/u3_sub/t` | W | W | W |
| `three/u3_sub/tx` | R | R | R |

### group `agg` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `agg/having_avg_max/t` | W | W | W |
| `agg/having_avg_max/tx` | R | R | R |
| `agg/having_sum_const/t` | W | W | W |
| `agg/having_sum_const/tx` | = | = | = |
| `agg/having_sum_max/t` | W | W | W |
| `agg/having_sum_max/tx` | = | = | = |
| `agg/sum_coalesce/t` | T | T | T |
| `agg/sum_coalesce/tx` | T | T | T |
| `agg/sum_eq_select/t` | W | W | W |
| `agg/sum_eq_select/tx` | = | = | = |
| `agg/sum_type/t` | W | W | W |
| `agg/sum_type/tx` | = | = | = |
| `agg/sum_union/t` | W | W | W |
| `agg/sum_union/tx` | W | W | W |

### group `values` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `values/v_ac` | W | W | W |
| `values/v_bc` | R | R | R |
| `values/v_big_h` | W | W | W |
| `values/v_ca` | W | W | W |
| `values/v_control` | = | = | = |
| `values/v_int_c` | T | T | T |
| `values/v_lit` | W | W | W |
| `values/v_null` | W | W | W |
| `values/v_three` | W | W | W |
| `values/v_two_col` | W | W | W |
| `values/v_union` | W | W | W |

### group `literal` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `literal/coalesce_overflow_lit/t` | R | R | R |
| `literal/coalesce_overflow_lit/tx` | R | R | R |
| `literal/u_arith/t` | = | = | = |
| `literal/u_arith/tx` | = | B | R |
| `literal/u_col_const/t` | W | W | W |
| `literal/u_col_const/tx` | R | R | R |
| `literal/u_const_ab/t` | R | R | R |
| `literal/u_const_ab/tx` | R | R | R |
| `literal/u_const_ac/t` | W | W | W |
| `literal/u_const_ac/tx` | W | W | W |
| `literal/u_null/t` | W | W | W |
| `literal/u_null/tx` | R | R | R |
| `literal/u_overflow39/t` | R | B | R |
| `literal/u_overflow39/tx` | R | B | R |
| `literal/u_overflow_lit/t` | R | B | R |
| `literal/u_overflow_lit/tx` | R | B | R |
| `literal/u_struct/t` | W | W | W |
| `literal/u_struct/tx` | R | R | R |
| `literal/where_overflow_lit/t` | R | B | R |
| `literal/where_overflow_lit/tx` | R | B | R |

### group `special` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `special/gsets_ac/t` | = | = | = |
| `special/gsets_ac/tx` | = | = | = |
| `special/gsets_coalesce/t` | W | W | W |
| `special/gsets_coalesce/tx` | R | R | R |
| `special/pivot_a/t` | R | R | R |
| `special/pivot_a/tx` | R | R | R |
| `special/pivot_c/t` | R | R | R |
| `special/pivot_c/tx` | R | R | R |
| `special/rollup_ac/t` | W | W | W |
| `special/rollup_ac/tx` | W | W | W |
| `special/w_range_a_dec/t` | R | R | R |
| `special/w_range_a_dec/tx` | R | R | R |
| `special/w_range_a_lit/t` | = | = | = |
| `special/w_range_a_lit/tx` | = | = | = |
| `special/w_range_c_dec/t` | R | R | R |
| `special/w_range_c_dec/tx` | R | R | R |
| `special/w_range_c_lit/t` | = | = | = |
| `special/w_range_c_lit/tx` | = | = | = |
| `special/w_range_h_dec/t` | R | R | R |
| `special/w_range_h_dec/tx` | R | R | R |

### group `const` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `const/a1_between_c/t/select` | W | W | W |
| `const/a1_between_c/t/where` | W | W | W |
| `const/a1_between_c/tx/select` | = | = | = |
| `const/a1_between_c/tx/where` | = | = | = |
| `const/a1_eq_c/t/select` | W | W | W |
| `const/a1_eq_c/t/where` | W | W | W |
| `const/a1_eq_c/tx/select` | = | = | = |
| `const/a1_eq_c/tx/where` | = | = | = |
| `const/a_eq_b05/t/select` | R | R | R |
| `const/a_eq_b05/t/where` | R | R | R |
| `const/a_eq_b05/tx/select` | R | R | R |
| `const/a_eq_b05/tx/where` | R | R | R |
| `const/a_eq_c125/t/select` | W | W | W |
| `const/a_eq_c125/t/where` | W | W | W |
| `const/a_eq_c125/tx/select` | R | R | R |
| `const/a_eq_c125/tx/where` | R | R | R |
| `const/a_eq_lit125/t/select` | W | W | W |
| `const/a_eq_lit125/t/where` | W | W | W |
| `const/a_eq_lit125/tx/select` | R | R | R |
| `const/a_eq_lit125/tx/where` | R | R | R |
| `const/a_eq_lit15/t/select` | W | W | W |
| `const/a_eq_lit15/t/where` | W | W | W |
| `const/a_eq_lit15/tx/select` | R | R | R |
| `const/a_eq_lit15/tx/where` | R | R | R |
| `const/a_gt_lit05/t/select` | W | W | W |
| `const/a_gt_lit05/t/where` | W | W | W |
| `const/a_gt_lit05/tx/select` | R | R | R |
| `const/a_gt_lit05/tx/where` | R | R | R |
| `const/a_in_c/t/select` | W | W | W |
| `const/a_in_c/t/where` | W | W | W |
| `const/a_in_c/tx/select` | R | R | R |
| `const/a_in_c/tx/where` | R | R | R |
| `const/a_in_lit/t/select` | W | W | W |
| `const/a_in_lit/t/where` | W | W | W |
| `const/a_in_lit/tx/select` | R | R | R |
| `const/a_in_lit/tx/where` | R | R | R |
| `const/b_eq_a1/t/select` | R | R | R |
| `const/b_eq_a1/t/where` | R | R | R |
| `const/b_eq_a1/tx/select` | R | R | R |
| `const/b_eq_a1/tx/where` | R | R | R |
| `const/c125_eq_a/t/select` | W | W | W |
| `const/c125_eq_a/t/where` | W | W | W |
| `const/c125_eq_a/tx/select` | R | R | R |
| `const/c125_eq_a/tx/where` | R | R | R |
| `const/c_between_a1/t/select` | W | W | W |
| `const/c_between_a1/t/where` | W | W | W |
| `const/c_between_a1/tx/select` | = | = | = |
| `const/c_between_a1/tx/where` | = | = | = |
| `const/c_eq_a1/t/select` | W | W | W |
| `const/c_eq_a1/t/where` | W | W | W |
| `const/c_eq_a1/tx/select` | = | = | = |
| `const/c_eq_a1/tx/where` | = | = | = |
| `const/c_eq_big1/t/select` | = | = | = |
| `const/c_eq_big1/t/where` | = | = | = |
| `const/c_eq_big1/tx/select` | = | = | = |
| `const/c_eq_big1/tx/where` | = | = | = |
| `const/c_ge_a1/t/select` | W | W | W |
| `const/c_ge_a1/t/where` | W | W | W |
| `const/c_ge_a1/tx/select` | = | = | = |
| `const/c_ge_a1/tx/where` | = | = | = |
| `const/c_in_a12/t/select` | W | W | W |
| `const/c_in_a12/t/where` | W | W | W |
| `const/c_in_a12/tx/select` | = | = | = |
| `const/c_in_a12/tx/where` | = | = | = |
| `const/c_lt_a1/t/select` | W | W | W |
| `const/c_lt_a1/t/where` | W | W | W |
| `const/c_lt_a1/tx/select` | = | = | = |
| `const/c_lt_a1/tx/where` | = | = | = |
| `const/c_notin_a12/t/select` | W | W | W |
| `const/c_notin_a12/t/where` | W | W | W |
| `const/c_notin_a12/tx/select` | = | = | = |
| `const/c_notin_a12/tx/where` | = | = | = |
| `const/c_nseq_a1/t/select` | W | W | W |
| `const/c_nseq_a1/t/where` | W | W | W |
| `const/c_nseq_a1/tx/select` | = | = | = |
| `const/c_nseq_a1/tx/where` | = | = | = |
| `const/d_eq_lit145/t/select` | W | W | W |
| `const/d_eq_lit145/t/where` | W | W | W |
| `const/d_eq_lit145/tx/select` | R | R | R |
| `const/d_eq_lit145/tx/where` | R | R | R |
| `const/gg_eq_hlit/t/select` | W | W | W |
| `const/gg_eq_hlit/t/where` | W | W | W |
| `const/gg_eq_hlit/tx/select` | R | R | R |
| `const/gg_eq_hlit/tx/where` | R | R | R |
| `const/h_eq_big1/t/select` | W | W | W |
| `const/h_eq_big1/t/where` | W | W | W |
| `const/h_eq_big1/tx/select` | = | = | = |
| `const/h_eq_big1/tx/where` | = | = | = |
| `const/h_eq_glit/t/select` | W | W | W |
| `const/h_eq_glit/t/where` | W | W | W |
| `const/h_eq_glit/tx/select` | = | = | = |
| `const/h_eq_glit/tx/where` | = | = | = |
| `const/h_eq_int1/t/select` | = | = | = |
| `const/h_eq_int1/t/where` | = | = | = |
| `const/h_eq_int1/tx/select` | = | = | = |
| `const/h_eq_int1/tx/where` | = | = | = |
| `const/i32_eq_w30lit/t/select` | W | W | W |
| `const/i32_eq_w30lit/t/where` | W | W | W |
| `const/i32_eq_w30lit/tx/select` | R | R | R |
| `const/i32_eq_w30lit/tx/where` | R | R | R |
| `const/i64_eq_hlit/t/select` | W | W | W |
| `const/i64_eq_hlit/t/where` | W | W | W |
| `const/i64_eq_hlit/tx/select` | R | R | R |
| `const/i64_eq_hlit/tx/where` | R | R | R |
| `const/lit125_eq_a/t/select` | W | W | W |
| `const/lit125_eq_a/t/where` | W | W | W |
| `const/lit125_eq_a/tx/select` | R | R | R |
| `const/lit125_eq_a/tx/where` | R | R | R |
| `const/w30_eq_int1/t/select` | = | = | = |
| `const/w30_eq_int1/t/where` | = | = | = |
| `const/w30_eq_int1/tx/select` | = | = | = |
| `const/w30_eq_int1/tx/where` | = | = | = |

### group `dfconst` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `dfconst/df_a1_eq_c/t` | W | W |  |
| `dfconst/df_a1_eq_c/tx` | = | = |  |
| `dfconst/df_a_eq_c125/t` | W | W |  |
| `dfconst/df_a_eq_c125/tx` | R | R |  |
| `dfconst/df_a_eq_pydec/t` | W | W |  |
| `dfconst/df_a_eq_pydec/tx` | R | R |  |
| `dfconst/df_a_isin_pydec/t` | W | W |  |
| `dfconst/df_a_isin_pydec/tx` | R | R |  |
| `dfconst/df_c125_eq_a/t` | W | W |  |
| `dfconst/df_c125_eq_a/tx` | R | R |  |
| `dfconst/df_c_between_a1/t` | W | W |  |
| `dfconst/df_c_between_a1/tx` | = | = |  |
| `dfconst/df_c_eq_a1/t` | W | W |  |
| `dfconst/df_c_eq_a1/tx` | = | = |  |
| `dfconst/df_c_isin_a1/t` | W | W |  |
| `dfconst/df_c_isin_a1/tx` | = | = |  |
| `dfconst/fexpr_a_eq_lit/t` | W | W |  |
| `dfconst/fexpr_a_eq_lit/tx` | = | = |  |
| `dfconst/fexpr_c_eq_a1/t` | W | W |  |
| `dfconst/fexpr_c_eq_a1/tx` | = | = |  |

### group `df` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `df/ab/t/between` | R | R |  |
| `df/ab/t/coalesce` | W | W |  |
| `df/ab/t/fexpr_coalesce` | W | W |  |
| `df/ab/t/fexpr_eq` | R | R |  |
| `df/ab/t/fexpr_filter` | R | R |  |
| `df/ab/t/filter_eq` | R | R |  |
| `df/ab/t/greatest` | R | R |  |
| `df/ab/t/isin` | R | R |  |
| `df/ab/t/join` | R | R |  |
| `df/ab/t/select_eq` | R | R |  |
| `df/ab/t/union` | R | R |  |
| `df/ab/t/unionByName` | R | R |  |
| `df/ab/t/when` | R | R |  |
| `df/ab/tx/between` | R | R |  |
| `df/ab/tx/coalesce` | W | W |  |
| `df/ab/tx/fexpr_coalesce` | W | W |  |
| `df/ab/tx/fexpr_eq` | R | R |  |
| `df/ab/tx/fexpr_filter` | R | R |  |
| `df/ab/tx/filter_eq` | R | R |  |
| `df/ab/tx/greatest` | R | R |  |
| `df/ab/tx/isin` | R | R |  |
| `df/ab/tx/join` | R | R |  |
| `df/ab/tx/select_eq` | R | R |  |
| `df/ab/tx/union` | R | R |  |
| `df/ab/tx/unionByName` | R | R |  |
| `df/ab/tx/when` | R | R |  |
| `df/ac/t/between` | W | W |  |
| `df/ac/t/coalesce` | W | W |  |
| `df/ac/t/fexpr_coalesce` | W | W |  |
| `df/ac/t/fexpr_eq` | W | W |  |
| `df/ac/t/fexpr_filter` | W | W |  |
| `df/ac/t/filter_eq` | W | W |  |
| `df/ac/t/greatest` | W | W |  |
| `df/ac/t/isin` | W | W |  |
| `df/ac/t/join` | W | W |  |
| `df/ac/t/select_eq` | W | W |  |
| `df/ac/t/union` | W | W |  |
| `df/ac/t/unionByName` | W | W |  |
| `df/ac/t/when` | W | W |  |
| `df/ac/tx/between` | R | R |  |
| `df/ac/tx/coalesce` | W | W |  |
| `df/ac/tx/fexpr_coalesce` | W | W |  |
| `df/ac/tx/fexpr_eq` | R | R |  |
| `df/ac/tx/fexpr_filter` | R | R |  |
| `df/ac/tx/filter_eq` | R | R |  |
| `df/ac/tx/greatest` | R | R |  |
| `df/ac/tx/isin` | R | R |  |
| `df/ac/tx/join` | R | R |  |
| `df/ac/tx/select_eq` | R | R |  |
| `df/ac/tx/union` | R | R |  |
| `df/ac/tx/unionByName` | R | R |  |
| `df/ac/tx/when` | R | R |  |
| `df/bc/t/between` | R | R |  |
| `df/bc/t/coalesce` | R | R |  |
| `df/bc/t/fexpr_coalesce` | R | R |  |
| `df/bc/t/fexpr_eq` | R | R |  |
| `df/bc/t/fexpr_filter` | R | R |  |
| `df/bc/t/filter_eq` | R | R |  |
| `df/bc/t/greatest` | R | R |  |
| `df/bc/t/isin` | R | R |  |
| `df/bc/t/join` | R | R |  |
| `df/bc/t/select_eq` | R | R |  |
| `df/bc/t/union` | R | R |  |
| `df/bc/t/unionByName` | R | R |  |
| `df/bc/t/when` | R | R |  |
| `df/bc/tx/between` | R | R |  |
| `df/bc/tx/coalesce` | R | R |  |
| `df/bc/tx/fexpr_coalesce` | R | R |  |
| `df/bc/tx/fexpr_eq` | R | R |  |
| `df/bc/tx/fexpr_filter` | R | R |  |
| `df/bc/tx/filter_eq` | R | R |  |
| `df/bc/tx/greatest` | R | R |  |
| `df/bc/tx/isin` | R | R |  |
| `df/bc/tx/join` | R | R |  |
| `df/bc/tx/select_eq` | R | R |  |
| `df/bc/tx/union` | R | R |  |
| `df/bc/tx/unionByName` | R | R |  |
| `df/bc/tx/when` | R | R |  |
| `df/c0c/t/between` | = | = |  |
| `df/c0c/t/coalesce` | = | = |  |
| `df/c0c/t/fexpr_coalesce` | = | = |  |
| `df/c0c/t/fexpr_eq` | = | = |  |
| `df/c0c/t/fexpr_filter` | = | = |  |
| `df/c0c/t/filter_eq` | = | = |  |
| `df/c0c/t/greatest` | = | = |  |
| `df/c0c/t/isin` | = | = |  |
| `df/c0c/t/join` | = | = |  |
| `df/c0c/t/select_eq` | = | = |  |
| `df/c0c/t/union` | = | = |  |
| `df/c0c/t/unionByName` | = | = |  |
| `df/c0c/t/when` | = | = |  |
| `df/c0c/tx/between` | = | = |  |
| `df/c0c/tx/coalesce` | = | = |  |
| `df/c0c/tx/fexpr_coalesce` | = | = |  |
| `df/c0c/tx/fexpr_eq` | = | = |  |
| `df/c0c/tx/fexpr_filter` | = | = |  |
| `df/c0c/tx/filter_eq` | = | = |  |
| `df/c0c/tx/greatest` | = | = |  |
| `df/c0c/tx/isin` | = | = |  |
| `df/c0c/tx/join` | = | = |  |
| `df/c0c/tx/select_eq` | = | = |  |
| `df/c0c/tx/union` | = | = |  |
| `df/c0c/tx/unionByName` | = | = |  |
| `df/c0c/tx/when` | = | = |  |
| `df/gh/t/between` | W | W |  |
| `df/gh/t/coalesce` | W | W |  |
| `df/gh/t/fexpr_coalesce` | W | W |  |
| `df/gh/t/fexpr_eq` | W | W |  |
| `df/gh/t/fexpr_filter` | W | W |  |
| `df/gh/t/filter_eq` | W | W |  |
| `df/gh/t/greatest` | W | W |  |
| `df/gh/t/isin` | W | W |  |
| `df/gh/t/join` | W | W |  |
| `df/gh/t/select_eq` | W | W |  |
| `df/gh/t/union` | W | W |  |
| `df/gh/t/unionByName` | W | W |  |
| `df/gh/t/when` | W | W |  |
| `df/gh/tx/between` | R | R |  |
| `df/gh/tx/coalesce` | W | W |  |
| `df/gh/tx/fexpr_coalesce` | W | W |  |
| `df/gh/tx/fexpr_eq` | R | R |  |
| `df/gh/tx/fexpr_filter` | R | R |  |
| `df/gh/tx/filter_eq` | R | R |  |
| `df/gh/tx/greatest` | R | R |  |
| `df/gh/tx/isin` | R | R |  |
| `df/gh/tx/join` | R | R |  |
| `df/gh/tx/select_eq` | R | R |  |
| `df/gh/tx/union` | R | R |  |
| `df/gh/tx/unionByName` | R | R |  |
| `df/gh/tx/when` | W | W |  |
| `df/i32w30/t/between` | W | W |  |
| `df/i32w30/t/coalesce` | W | W |  |
| `df/i32w30/t/fexpr_coalesce` | W | W |  |
| `df/i32w30/t/fexpr_eq` | W | W |  |
| `df/i32w30/t/fexpr_filter` | W | W |  |
| `df/i32w30/t/filter_eq` | W | W |  |
| `df/i32w30/t/greatest` | W | W |  |
| `df/i32w30/t/isin` | W | W |  |
| `df/i32w30/t/join` | W | W |  |
| `df/i32w30/t/select_eq` | W | W |  |
| `df/i32w30/t/union` | W | W |  |
| `df/i32w30/t/unionByName` | W | W |  |
| `df/i32w30/t/when` | W | W |  |
| `df/i32w30/tx/between` | R | R |  |
| `df/i32w30/tx/coalesce` | W | W |  |
| `df/i32w30/tx/fexpr_coalesce` | W | W |  |
| `df/i32w30/tx/fexpr_eq` | R | R |  |
| `df/i32w30/tx/fexpr_filter` | R | R |  |
| `df/i32w30/tx/filter_eq` | R | R |  |
| `df/i32w30/tx/greatest` | R | R |  |
| `df/i32w30/tx/isin` | R | R |  |
| `df/i32w30/tx/join` | R | R |  |
| `df/i32w30/tx/select_eq` | R | R |  |
| `df/i32w30/tx/union` | R | R |  |
| `df/i32w30/tx/unionByName` | R | R |  |
| `df/i32w30/tx/when` | R | R |  |
| `df/i64h/t/between` | W | W |  |
| `df/i64h/t/coalesce` | W | W |  |
| `df/i64h/t/fexpr_coalesce` | W | W |  |
| `df/i64h/t/fexpr_eq` | W | W |  |
| `df/i64h/t/fexpr_filter` | W | W |  |
| `df/i64h/t/filter_eq` | W | W |  |
| `df/i64h/t/greatest` | W | W |  |
| `df/i64h/t/isin` | W | W |  |
| `df/i64h/t/join` | W | W |  |
| `df/i64h/t/select_eq` | W | W |  |
| `df/i64h/t/union` | W | W |  |
| `df/i64h/t/unionByName` | W | W |  |
| `df/i64h/t/when` | W | W |  |
| `df/i64h/tx/between` | R | R |  |
| `df/i64h/tx/coalesce` | W | W |  |
| `df/i64h/tx/fexpr_coalesce` | W | W |  |
| `df/i64h/tx/fexpr_eq` | R | R |  |
| `df/i64h/tx/fexpr_filter` | R | R |  |
| `df/i64h/tx/filter_eq` | R | R |  |
| `df/i64h/tx/greatest` | R | R |  |
| `df/i64h/tx/isin` | R | R |  |
| `df/i64h/tx/join` | R | R |  |
| `df/i64h/tx/select_eq` | R | R |  |
| `df/i64h/tx/union` | R | R |  |
| `df/i64h/tx/unionByName` | R | R |  |
| `df/i64h/tx/when` | R | R |  |
| `dfconst/df_a1_eq_c/t` | W | W |  |
| `dfconst/df_a1_eq_c/tx` | = | = |  |
| `dfconst/df_a_eq_c125/t` | W | W |  |
| `dfconst/df_a_eq_c125/tx` | R | R |  |
| `dfconst/df_a_eq_pydec/t` | W | W |  |
| `dfconst/df_a_eq_pydec/tx` | R | R |  |
| `dfconst/df_a_isin_pydec/t` | W | W |  |
| `dfconst/df_a_isin_pydec/tx` | R | R |  |
| `dfconst/df_c125_eq_a/t` | W | W |  |
| `dfconst/df_c125_eq_a/tx` | R | R |  |
| `dfconst/df_c_between_a1/t` | W | W |  |
| `dfconst/df_c_between_a1/tx` | = | = |  |
| `dfconst/df_c_eq_a1/t` | W | W |  |
| `dfconst/df_c_eq_a1/tx` | = | = |  |
| `dfconst/df_c_isin_a1/t` | W | W |  |
| `dfconst/df_c_isin_a1/tx` | = | = |  |
| `dfconst/fexpr_a_eq_lit/t` | W | W |  |
| `dfconst/fexpr_a_eq_lit/tx` | = | = |  |
| `dfconst/fexpr_c_eq_a1/t` | W | W |  |
| `dfconst/fexpr_c_eq_a1/tx` | = | = |  |

### group `ice` (RePark Spark door; ANSI off, then on)

| cell | off | on | native off |
|---|---|---|---|
| `ice/ice_ctas_coalesce` | S | S |  |
| `ice/ice_ctas_union` | S | S |  |
| `ice/ice_ctas_union_h` | S | S |  |
| `ice/ice_insert_case` | S | S |  |
| `ice/ice_insert_coalesce` | S | S |  |
| `ice/ice_insert_union` | S | S |  |
| `ice/ice_insert_union_bc` | S | S |  |
| `ice/ice_insert_union_gh` | S | S |  |
| `ice/ice_insert_union_h` | S | S |  |
| `ice/ice_insert_values` | S | S |  |
| `ice/ice_insert_where` | S | S |  |
| `ice/ice_rtas_union` | S | S |  |
| `icedml/cow/delete_a_c125` | S | S |  |
| `icedml/cow/delete_a_eq_c` | S | S |  |
| `icedml/cow/delete_a_lit` | S | S |  |
| `icedml/cow/delete_c_a1` | S | S |  |
| `icedml/cow/delete_in_subq` | S | S |  |
| `icedml/cow/merge_insert_union` | S | S |  |
| `icedml/cow/merge_on_a_c` | S | S |  |
| `icedml/cow/merge_on_c_a` | S | S |  |
| `icedml/cow/update_c_a1` | S | S |  |
| `icedml/cow/update_c_in` | S | S |  |
| `icedml/cow/update_h_i64` | S | S |  |
| `icedml/cow/update_set_coalesce` | R | R |  |
| `icedml/mor/delete_a_c125` | S | S |  |
| `icedml/mor/delete_a_eq_c` | S | S |  |
| `icedml/mor/delete_a_lit` | S | S |  |
| `icedml/mor/delete_c_a1` | S | S |  |
| `icedml/mor/delete_in_subq` | S | S |  |
| `icedml/mor/merge_insert_union` | S | S |  |
| `icedml/mor/merge_on_a_c` | S | S |  |
| `icedml/mor/merge_on_c_a` | S | S |  |
| `icedml/mor/update_c_a1` | S | S |  |
| `icedml/mor/update_c_in` | S | S |  |
| `icedml/mor/update_h_i64` | S | S |  |
| `icedml/mor/update_set_coalesce` | R | R |  |
| `icepart/cow/part_delete_a_c125` | S | S |  |
| `icepart/cow/part_delete_c_a1` | S | S |  |
| `icepart/mor/part_delete_a_c125` | S | S |  |
| `icepart/mor/part_delete_c_a1` | S | S |  |
| `icepart/read/part_a_c125` | W | W |  |
| `icepart/read/part_a_lit` | W | W |  |
| `icepart/read/part_c_a1` | W | W |  |
| `icepart/read/part_c_in` | W | W |  |
| `icepart/read/part_h_big` | W | W |  |

