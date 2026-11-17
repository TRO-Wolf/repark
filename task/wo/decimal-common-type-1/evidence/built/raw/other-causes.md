# Every non-EQUAL cell outside the common-type cause (head 7bf32859, Spark door)

## arithmetic-control (1)
- ANSI true `literal/u_arith/tx`: both_raise

## avg-round (2)
- ANSI false `agg/sum_type/t`: wrong_answer
- ANSI true `agg/sum_type/t`: wrong_answer

## confounded:array_union (20)
- ANSI false `pair/ab/t/array_union`: both_raise
- ANSI false `pair/ac/t/array_union`: missed_raise
- ANSI false `pair/gh/t/array_union`: missed_raise
- ANSI false `pair/i64h/t/array_union`: missed_raise
- ANSI false `pair/i32w30/t/array_union`: missed_raise
- ANSI false `pair/ab/tx/array_union`: both_raise
- ANSI false `pair/ac/tx/array_union`: both_raise
- ANSI false `pair/gh/tx/array_union`: both_raise
- ANSI false `pair/i64h/tx/array_union`: both_raise
- ANSI false `pair/i32w30/tx/array_union`: both_raise
- ANSI true `pair/ab/t/array_union`: both_raise
- ANSI true `pair/ac/t/array_union`: missed_raise
- ANSI true `pair/gh/t/array_union`: missed_raise
- ANSI true `pair/i64h/t/array_union`: missed_raise
- ANSI true `pair/i32w30/t/array_union`: missed_raise
- ANSI true `pair/ab/tx/array_union`: both_raise
- ANSI true `pair/ac/tx/array_union`: both_raise
- ANSI true `pair/gh/tx/array_union`: both_raise
- ANSI true `pair/i64h/tx/array_union`: both_raise
- ANSI true `pair/i32w30/tx/array_union`: both_raise

## confounded:join_full_using (20)
- ANSI false `pair/ab/t/join_full_using`: over_raise
- ANSI false `pair/ac/t/join_full_using`: over_raise
- ANSI false `pair/gh/t/join_full_using`: over_raise
- ANSI false `pair/i64h/t/join_full_using`: over_raise
- ANSI false `pair/i32w30/t/join_full_using`: over_raise
- ANSI false `pair/ab/tx/join_full_using`: over_raise
- ANSI false `pair/ac/tx/join_full_using`: over_raise
- ANSI false `pair/gh/tx/join_full_using`: over_raise
- ANSI false `pair/i64h/tx/join_full_using`: over_raise
- ANSI false `pair/i32w30/tx/join_full_using`: over_raise
- ANSI true `pair/ab/t/join_full_using`: over_raise
- ANSI true `pair/ac/t/join_full_using`: over_raise
- ANSI true `pair/gh/t/join_full_using`: over_raise
- ANSI true `pair/i64h/t/join_full_using`: over_raise
- ANSI true `pair/i32w30/t/join_full_using`: over_raise
- ANSI true `pair/ab/tx/join_full_using`: over_raise
- ANSI true `pair/ac/tx/join_full_using`: over_raise
- ANSI true `pair/gh/tx/join_full_using`: over_raise
- ANSI true `pair/i64h/tx/join_full_using`: over_raise
- ANSI true `pair/i32w30/tx/join_full_using`: over_raise

## confounded:lag (20)
- ANSI false `pair/ab/t/lag`: over_raise
- ANSI false `pair/ac/t/lag`: over_raise
- ANSI false `pair/gh/t/lag`: over_raise
- ANSI false `pair/i64h/t/lag`: over_raise
- ANSI false `pair/i32w30/t/lag`: over_raise
- ANSI false `pair/ab/tx/lag`: over_raise
- ANSI false `pair/ac/tx/lag`: over_raise
- ANSI false `pair/gh/tx/lag`: over_raise
- ANSI false `pair/i64h/tx/lag`: over_raise
- ANSI false `pair/i32w30/tx/lag`: over_raise
- ANSI true `pair/ab/t/lag`: over_raise
- ANSI true `pair/ac/t/lag`: over_raise
- ANSI true `pair/gh/t/lag`: over_raise
- ANSI true `pair/i64h/t/lag`: over_raise
- ANSI true `pair/i32w30/t/lag`: over_raise
- ANSI true `pair/ab/tx/lag`: over_raise
- ANSI true `pair/ac/tx/lag`: over_raise
- ANSI true `pair/gh/tx/lag`: over_raise
- ANSI true `pair/i64h/tx/lag`: over_raise
- ANSI true `pair/i32w30/tx/lag`: over_raise

## confounded:lead (20)
- ANSI false `pair/ab/t/lead`: over_raise
- ANSI false `pair/ac/t/lead`: over_raise
- ANSI false `pair/gh/t/lead`: over_raise
- ANSI false `pair/i64h/t/lead`: over_raise
- ANSI false `pair/i32w30/t/lead`: over_raise
- ANSI false `pair/ab/tx/lead`: over_raise
- ANSI false `pair/ac/tx/lead`: over_raise
- ANSI false `pair/gh/tx/lead`: over_raise
- ANSI false `pair/i64h/tx/lead`: over_raise
- ANSI false `pair/i32w30/tx/lead`: over_raise
- ANSI true `pair/ab/t/lead`: over_raise
- ANSI true `pair/ac/t/lead`: over_raise
- ANSI true `pair/gh/t/lead`: over_raise
- ANSI true `pair/i64h/t/lead`: over_raise
- ANSI true `pair/i32w30/t/lead`: over_raise
- ANSI true `pair/ab/tx/lead`: over_raise
- ANSI true `pair/ac/tx/lead`: over_raise
- ANSI true `pair/gh/tx/lead`: over_raise
- ANSI true `pair/i64h/tx/lead`: over_raise
- ANSI true `pair/i32w30/tx/lead`: over_raise

## confounded:nvl (20)
- ANSI false `pair/ab/t/nvl`: wrong_type
- ANSI false `pair/ac/t/nvl`: wrong_type
- ANSI false `pair/gh/t/nvl`: wrong_type
- ANSI false `pair/i64h/t/nvl`: wrong_answer
- ANSI false `pair/i32w30/t/nvl`: wrong_answer
- ANSI false `pair/ab/tx/nvl`: wrong_type
- ANSI false `pair/ac/tx/nvl`: wrong_type
- ANSI false `pair/gh/tx/nvl`: wrong_answer
- ANSI false `pair/i64h/tx/nvl`: wrong_answer
- ANSI false `pair/i32w30/tx/nvl`: wrong_type
- ANSI true `pair/ab/t/nvl`: wrong_type
- ANSI true `pair/ac/t/nvl`: wrong_type
- ANSI true `pair/gh/t/nvl`: wrong_type
- ANSI true `pair/i64h/t/nvl`: wrong_answer
- ANSI true `pair/i32w30/t/nvl`: wrong_answer
- ANSI true `pair/ab/tx/nvl`: wrong_type
- ANSI true `pair/ac/tx/nvl`: wrong_type
- ANSI true `pair/gh/tx/nvl`: wrong_answer
- ANSI true `pair/i64h/tx/nvl`: wrong_answer
- ANSI true `pair/i32w30/tx/nvl`: wrong_type

## control:array_union (4)
- ANSI false `pair/c0c/t/array_union`: missed_raise
- ANSI false `pair/c0c/tx/array_union`: missed_raise
- ANSI true `pair/c0c/t/array_union`: missed_raise
- ANSI true `pair/c0c/tx/array_union`: missed_raise

## control:core (8)
- ANSI false `pair/c0c/t/except`: wrong_type
- ANSI false `pair/c0c/t/intersect`: wrong_type
- ANSI false `pair/c0c/tx/except`: wrong_type
- ANSI false `pair/c0c/tx/intersect`: wrong_type
- ANSI true `pair/c0c/t/except`: wrong_type
- ANSI true `pair/c0c/t/intersect`: wrong_type
- ANSI true `pair/c0c/tx/except`: wrong_type
- ANSI true `pair/c0c/tx/intersect`: wrong_type

## control:join_full_using (4)
- ANSI false `pair/c0c/t/join_full_using`: over_raise
- ANSI false `pair/c0c/tx/join_full_using`: over_raise
- ANSI true `pair/c0c/t/join_full_using`: over_raise
- ANSI true `pair/c0c/tx/join_full_using`: over_raise

## control:lag (4)
- ANSI false `pair/c0c/t/lag`: over_raise
- ANSI false `pair/c0c/tx/lag`: over_raise
- ANSI true `pair/c0c/t/lag`: over_raise
- ANSI true `pair/c0c/tx/lag`: over_raise

## control:lead (4)
- ANSI false `pair/c0c/t/lead`: over_raise
- ANSI false `pair/c0c/tx/lead`: over_raise
- ANSI true `pair/c0c/t/lead`: over_raise
- ANSI true `pair/c0c/tx/lead`: over_raise

## control:nullif (4)
- ANSI false `pair/c0c/t/nullif`: wrong_type
- ANSI false `pair/c0c/tx/nullif`: wrong_type
- ANSI true `pair/c0c/t/nullif`: wrong_type
- ANSI true `pair/c0c/tx/nullif`: wrong_type

## control:nvl (4)
- ANSI false `pair/c0c/t/nvl`: wrong_type
- ANSI false `pair/c0c/tx/nvl`: wrong_type
- ANSI true `pair/c0c/t/nvl`: wrong_type
- ANSI true `pair/c0c/tx/nvl`: wrong_type

## legacy (56)
- ANSI false `legacy/pair/ab/t/eq`: over_raise
- ANSI false `legacy/pair/ab/t/union_all`: over_raise
- ANSI false `legacy/pair/bc/t/union_all`: over_raise
- ANSI false `legacy/pair/bc/t/eq`: over_raise
- ANSI false `legacy/pair/bc/t/coalesce`: over_raise
- ANSI false `legacy/pair/ab/tx/eq`: over_raise
- ANSI false `legacy/pair/ab/tx/union_all`: over_raise
- ANSI false `legacy/pair/ac/tx/eq`: over_raise
- ANSI false `legacy/pair/ac/tx/union_all`: over_raise
- ANSI false `legacy/pair/ad/tx/union_all`: over_raise
- ANSI false `legacy/pair/ad/tx/eq`: over_raise
- ANSI false `legacy/pair/bc/tx/union_all`: over_raise
- ANSI false `legacy/pair/bc/tx/eq`: over_raise
- ANSI false `legacy/pair/bc/tx/coalesce`: over_raise
- ANSI false `legacy/pair/ef/tx/union_all`: over_raise
- ANSI false `legacy/pair/ef/tx/eq`: over_raise
- ANSI false `legacy/pair/gh/tx/eq`: over_raise
- ANSI false `legacy/pair/gh/tx/union_all`: over_raise
- ANSI false `legacy/pair/i64h/tx/eq`: over_raise
- ANSI false `legacy/pair/i64h/tx/union_all`: over_raise
- ANSI false `legacy/pair/i32w30/tx/eq`: over_raise
- ANSI false `legacy/pair/i32w30/tx/union_all`: over_raise
- ANSI false `legacy/pair/i16w34/tx/union_all`: over_raise
- ANSI false `legacy/pair/i16w34/tx/eq`: over_raise
- ANSI false `legacy/pair/i8w36/tx/union_all`: over_raise
- ANSI false `legacy/pair/i8w36/tx/eq`: over_raise
- ANSI false `legacy/extra/v_bc`: over_raise
- ANSI false `legacy/extra/u_const_ab`: over_raise
- ANSI true `legacy/pair/ab/t/eq`: both_raise
- ANSI true `legacy/pair/ab/t/union_all`: both_raise
- ANSI true `legacy/pair/bc/t/union_all`: both_raise
- ANSI true `legacy/pair/bc/t/eq`: both_raise
- ANSI true `legacy/pair/bc/t/coalesce`: both_raise
- ANSI true `legacy/pair/ab/tx/eq`: both_raise
- ANSI true `legacy/pair/ab/tx/union_all`: both_raise
- ANSI true `legacy/pair/ac/tx/eq`: both_raise
- ANSI true `legacy/pair/ac/tx/union_all`: both_raise
- ANSI true `legacy/pair/ad/tx/union_all`: both_raise
- ANSI true `legacy/pair/ad/tx/eq`: both_raise
- ANSI true `legacy/pair/bc/tx/union_all`: both_raise
- ANSI true `legacy/pair/bc/tx/eq`: both_raise
- ANSI true `legacy/pair/bc/tx/coalesce`: both_raise
- ANSI true `legacy/pair/ef/tx/union_all`: both_raise
- ANSI true `legacy/pair/ef/tx/eq`: both_raise
- ANSI true `legacy/pair/gh/tx/eq`: both_raise
- ANSI true `legacy/pair/gh/tx/union_all`: both_raise
- ANSI true `legacy/pair/i64h/tx/eq`: both_raise
- ANSI true `legacy/pair/i64h/tx/union_all`: both_raise
- ANSI true `legacy/pair/i32w30/tx/eq`: both_raise
- ANSI true `legacy/pair/i32w30/tx/union_all`: both_raise
- ANSI true `legacy/pair/i16w34/tx/union_all`: both_raise
- ANSI true `legacy/pair/i16w34/tx/eq`: both_raise
- ANSI true `legacy/pair/i8w36/tx/union_all`: both_raise
- ANSI true `legacy/pair/i8w36/tx/eq`: both_raise
- ANSI true `legacy/extra/v_bc`: both_raise
- ANSI true `legacy/extra/u_const_ab`: both_raise

## pivot-unsupported (8)
- ANSI false `special/pivot_a/t`: over_raise
- ANSI false `special/pivot_c/t`: over_raise
- ANSI false `special/pivot_a/tx`: over_raise
- ANSI false `special/pivot_c/tx`: over_raise
- ANSI true `special/pivot_a/t`: over_raise
- ANSI true `special/pivot_c/t`: over_raise
- ANSI true `special/pivot_a/tx`: over_raise
- ANSI true `special/pivot_c/tx`: over_raise

## range-offset (12)
- ANSI false `special/w_range_a_dec/t`: over_raise
- ANSI false `special/w_range_c_dec/t`: over_raise
- ANSI false `special/w_range_h_dec/t`: over_raise
- ANSI false `special/w_range_a_dec/tx`: over_raise
- ANSI false `special/w_range_c_dec/tx`: over_raise
- ANSI false `special/w_range_h_dec/tx`: over_raise
- ANSI true `special/w_range_a_dec/t`: over_raise
- ANSI true `special/w_range_c_dec/t`: over_raise
- ANSI true `special/w_range_h_dec/t`: over_raise
- ANSI true `special/w_range_a_dec/tx`: over_raise
- ANSI true `special/w_range_c_dec/tx`: over_raise
- ANSI true `special/w_range_h_dec/tx`: over_raise

## update-cse (4)
- ANSI false `icedml/cow/update_set_coalesce`: over_raise
- ANSI false `icedml/mor/update_set_coalesce`: over_raise
- ANSI true `icedml/cow/update_set_coalesce`: over_raise
- ANSI true `icedml/mor/update_set_coalesce`: over_raise

## using-unaliased-refusal (14)
- ANSI false `rev/m2/using_inner`: over_raise
- ANSI false `rev/m2/using_left`: over_raise
- ANSI false `rev/m2/using_full_distinct_names`: over_raise
- ANSI false `rev/m2/using_inner_dup_names`: over_raise
- ANSI false `rev/m2/using_full_dup_names`: over_raise
- ANSI false `rev/m2/using_full_dup_names_same_type`: over_raise
- ANSI false `rev/m2/using_inner_h_i64`: over_raise
- ANSI true `rev/m2/using_inner`: over_raise
- ANSI true `rev/m2/using_left`: over_raise
- ANSI true `rev/m2/using_full_distinct_names`: over_raise
- ANSI true `rev/m2/using_inner_dup_names`: over_raise
- ANSI true `rev/m2/using_full_dup_names`: over_raise
- ANSI true `rev/m2/using_full_dup_names_same_type`: over_raise
- ANSI true `rev/m2/using_inner_h_i64`: over_raise
