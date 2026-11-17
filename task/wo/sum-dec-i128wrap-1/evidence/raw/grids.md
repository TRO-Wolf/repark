Letters: `=` equal, `T` type, `F` format, `V` value, `R` RePark raises,
`S` Spark raises and RePark answers, `B` both raise (same condition),
`b` both raise (different condition), `E` read error; for `V`/`S`, the RePark value:
`w` wrapped and inside the type, `W` wrapped and past it, `X` exact and past the type,
`k` exact and inside it (Spark overflowed on its own path).

### native / false / control / native / int_avg

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / control / native / int_distinct

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / control / native / int_grouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / control / native / int_try

| group | i | b | d |
|---|---|---|---|
| 41 fits | R | R | R |
| 42 bigint_over | R | R | R |
| 43 bigint_under | R | R | R |
| 44 bigint_pass_back | R | R | R |
| 45 all_null | R | R | R |
| 46 int_max_pair | R | R | R |
| 47 double_over | R | R | R |

### native / false / control / native / int_ungrouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / control / native / int_win_rows2

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / control / native / int_win_running

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / false / sum / native / as_string

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | F |
| 2 fits_nulls | = | F |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | V | V |
| 6 two_max | V | V |
| 7 three_max | V | V |
| 8 neg_max_minus_unit | V | V |
| 9 two_neg_max | V | V |
| 10 pass_back | V | V |
| 11 pass_back_unit | V | V |
| 12 cancel | = | F |
| 13 max_minus_unit | = | = |
| 14 unit | = | F |

### native / false / sum / native / beside

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | V | V | V |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | R | = |
| 5 max_plus_unit | = | R | X |
| 6 two_max | = | R | W |
| 7 three_max | = | R | w |
| 8 neg_max_minus_unit | = | R | X |
| 9 two_neg_max | = | R | W |
| 10 pass_back | = | k | k |
| 11 pass_back_unit | = | R | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | R | = |
| 14 unit | = | = | = |

### native / false / sum / native / cte

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### native / false / sum / native / distinct

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | X | X |
| 6 two_max | = | = |
| 7 three_max | = | = |
| 8 neg_max_minus_unit | X | X |
| 9 two_neg_max | = | = |
| 10 pass_back | = | = |
| 11 pass_back_unit | k | k |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |
| 14 unit | = | = |

### native / false / sum / native / empty_ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | = | = |

### native / false / sum / native / grouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | X | X |
| 6 two_max | = | W | W |
| 7 three_max | = | w | w |
| 8 neg_max_minus_unit | = | X | X |
| 9 two_neg_max | = | W | W |
| 10 pass_back | = | k | k |
| 11 pass_back_unit | = | k | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### native / false / sum / native / grouped_all

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | w | w |

### native / false / sum / native / having

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | V | V |
| 6 two_max | = | = |
| 7 three_max | = | = |
| 8 neg_max_minus_unit | = | = |
| 9 two_neg_max | V | V |
| 10 pass_back | V | V |
| 11 pass_back_unit | V | V |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |
| 14 unit | = | V |

### native / false / sum / native / linear

| group | c10_2 | c38_0 |
|---|---|---|
| 1 fits | T | = |
| 2 fits_nulls | T | = |
| 3 all_null | T | = |
| 4 max | T | V |
| 5 max_plus_unit | T | V |
| 6 two_max | T | V |
| 7 three_max | T | V |
| 8 neg_max_minus_unit | T | = |
| 9 two_neg_max | T | V |
| 10 pass_back | T | V |
| 11 pass_back_unit | T | V |
| 12 cancel | T | V |
| 13 max_minus_unit | T | V |
| 14 unit | T | = |

### native / false / sum / native / plus1

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | T | = | V |
| 2 fits_nulls | T | = | T |
| 3 all_null | T | = | T |
| 4 max | T | V | R |
| 5 max_plus_unit | T | V | R |
| 6 two_max | T | V | V |
| 7 three_max | T | V | V |
| 8 neg_max_minus_unit | T | V | V |
| 9 two_neg_max | T | V | R |
| 10 pass_back | T | V | V |
| 11 pass_back_unit | T | V | R |
| 12 cancel | T | = | V |
| 13 max_minus_unit | T | = | R |
| 14 unit | T | = | V |

### native / false / sum / native / scalar_sub

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | = |

### native / false / sum / native / temp_view

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### native / false / sum / native / ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | X | X |
| 6 two_max | = | W | W |
| 7 three_max | = | w | w |
| 8 neg_max_minus_unit | = | X | X |
| 9 two_neg_max | = | W | W |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### native / false / sum / native / win_part

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | V | V |
| 6 two_max | V | V |
| 7 three_max | V | V |
| 8 neg_max_minus_unit | V | V |
| 9 two_neg_max | V | V |
| 10 pass_back | = | = |
| 11 pass_back_unit | = | = |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / false / sum / native / win_rows2

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | V | V |
| 6 two_max | V | V |
| 7 three_max | V | V |
| 8 neg_max_minus_unit | V | V |
| 9 two_neg_max | V | V |
| 10 pass_back | V | V |
| 11 pass_back_unit | V | V |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / false / sum / native / win_running

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | V | V |
| 6 two_max | V | V |
| 7 three_max | V | V |
| 8 neg_max_minus_unit | V | V |
| 9 two_neg_max | V | V |
| 10 pass_back | V | V |
| 11 pass_back_unit | V | V |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / false / try_sum / native / try_grouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | R | R | R |
| 2 fits_nulls | R | R | R |
| 3 all_null | R | R | R |
| 4 max | R | R | R |
| 5 max_plus_unit | R | R | R |
| 6 two_max | R | R | R |
| 7 three_max | R | R | R |
| 8 neg_max_minus_unit | R | R | R |
| 9 two_neg_max | R | R | R |
| 10 pass_back | R | R | R |
| 11 pass_back_unit | R | R | R |
| 12 cancel | R | R | R |
| 13 max_minus_unit | R | R | R |
| 14 unit | R | R | R |

### native / false / try_sum / native / try_ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | R | R | R |
| 2 fits_nulls | R | R | R |
| 3 all_null | R | R | R |
| 4 max | R | R | R |
| 5 max_plus_unit | R | R | R |
| 6 two_max | R | R | R |
| 7 three_max | R | R | R |
| 8 neg_max_minus_unit | R | R | R |
| 9 two_neg_max | R | R | R |
| 10 pass_back | R | R | R |
| 11 pass_back_unit | R | R | R |
| 12 cancel | R | R | R |
| 13 max_minus_unit | R | R | R |
| 14 unit | R | R | R |

### native / true / control / native / int_avg

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / control / native / int_distinct

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / control / native / int_grouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / control / native / int_try

| group | i | b | d |
|---|---|---|---|
| 41 fits | R | R | R |
| 42 bigint_over | R | R | R |
| 43 bigint_under | R | R | R |
| 44 bigint_pass_back | R | R | R |
| 45 all_null | R | R | R |
| 46 int_max_pair | R | R | R |
| 47 double_over | R | R | R |

### native / true / control / native / int_ungrouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / control / native / int_win_rows2

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / control / native / int_win_running

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### native / true / sum / native / as_string

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | S | S |
| 6 two_max | S | S |
| 7 three_max | S | S |
| 8 neg_max_minus_unit | S | S |
| 9 two_neg_max | S | S |
| 10 pass_back | S | S |
| 11 pass_back_unit | S | S |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |
| 14 unit | = | = |

### native / true / sum / native / beside

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | V | V | V |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | b | = |
| 5 max_plus_unit | = | b | X |
| 6 two_max | = | b | W |
| 7 three_max | = | b | w |
| 8 neg_max_minus_unit | = | b | X |
| 9 two_neg_max | = | b | W |
| 10 pass_back | = | k | k |
| 11 pass_back_unit | = | b | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | b | = |
| 14 unit | = | = | = |

### native / true / sum / native / cte

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### native / true / sum / native / distinct

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | X | X |
| 6 two_max | = | = |
| 7 three_max | = | = |
| 8 neg_max_minus_unit | X | X |
| 9 two_neg_max | = | = |
| 10 pass_back | = | = |
| 11 pass_back_unit | k | k |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |
| 14 unit | = | = |

### native / true / sum / native / empty_ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | = | = |

### native / true / sum / native / grouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | X | X |
| 6 two_max | = | W | W |
| 7 three_max | = | w | w |
| 8 neg_max_minus_unit | = | X | X |
| 9 two_neg_max | = | W | W |
| 10 pass_back | = | k | k |
| 11 pass_back_unit | = | k | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### native / true / sum / native / grouped_all

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | w | w |

### native / true / sum / native / having

| group | c38_0 | c38_38 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | S | S |
| 6 two_max | S | S |
| 7 three_max | S | S |
| 8 neg_max_minus_unit | S | S |
| 9 two_neg_max | S | S |
| 10 pass_back | S | S |
| 11 pass_back_unit | S | S |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |
| 14 unit | = | V |

### native / true / sum / native / linear

| group | c10_2 | c38_0 |
|---|---|---|
| 1 fits | T | = |
| 2 fits_nulls | T | = |
| 3 all_null | T | = |
| 4 max | T | S |
| 5 max_plus_unit | T | S |
| 6 two_max | T | S |
| 7 three_max | T | S |
| 8 neg_max_minus_unit | T | = |
| 9 two_neg_max | T | S |
| 10 pass_back | T | S |
| 11 pass_back_unit | T | S |
| 12 cancel | T | S |
| 13 max_minus_unit | T | S |
| 14 unit | T | = |

### native / true / sum / native / plus1

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | T | = | V |
| 2 fits_nulls | T | = | T |
| 3 all_null | T | = | T |
| 4 max | T | S | R |
| 5 max_plus_unit | T | S | b |
| 6 two_max | T | S | S |
| 7 three_max | T | S | S |
| 8 neg_max_minus_unit | T | S | S |
| 9 two_neg_max | T | S | b |
| 10 pass_back | T | S | S |
| 11 pass_back_unit | T | S | b |
| 12 cancel | T | = | V |
| 13 max_minus_unit | T | = | R |
| 14 unit | T | = | V |

### native / true / sum / native / scalar_sub

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | = |

### native / true / sum / native / temp_view

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### native / true / sum / native / ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | X | X |
| 6 two_max | = | W | W |
| 7 three_max | = | w | w |
| 8 neg_max_minus_unit | = | X | X |
| 9 two_neg_max | = | W | W |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### native / true / sum / native / win_part

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | S | S |
| 6 two_max | S | S |
| 7 three_max | S | S |
| 8 neg_max_minus_unit | S | S |
| 9 two_neg_max | S | S |
| 10 pass_back | = | = |
| 11 pass_back_unit | = | = |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / true / sum / native / win_rows2

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | S | S |
| 6 two_max | S | S |
| 7 three_max | S | S |
| 8 neg_max_minus_unit | S | S |
| 9 two_neg_max | S | S |
| 10 pass_back | S | S |
| 11 pass_back_unit | S | S |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / true / sum / native / win_running

| group | c38_0 | c38_38 |
|---|---|---|
| 4 max | = | = |
| 5 max_plus_unit | S | S |
| 6 two_max | S | S |
| 7 three_max | S | S |
| 8 neg_max_minus_unit | S | S |
| 9 two_neg_max | S | S |
| 10 pass_back | S | S |
| 11 pass_back_unit | S | S |
| 12 cancel | = | = |
| 13 max_minus_unit | = | = |

### native / true / try_sum / native / try_grouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | R | R | R |
| 2 fits_nulls | R | R | R |
| 3 all_null | R | R | R |
| 4 max | R | R | R |
| 5 max_plus_unit | R | R | R |
| 6 two_max | R | R | R |
| 7 three_max | R | R | R |
| 8 neg_max_minus_unit | R | R | R |
| 9 two_neg_max | R | R | R |
| 10 pass_back | R | R | R |
| 11 pass_back_unit | R | R | R |
| 12 cancel | R | R | R |
| 13 max_minus_unit | R | R | R |
| 14 unit | R | R | R |

### native / true / try_sum / native / try_ungrouped

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 1 fits | R | R | R |
| 2 fits_nulls | R | R | R |
| 3 all_null | R | R | R |
| 4 max | R | R | R |
| 5 max_plus_unit | R | R | R |
| 6 two_max | R | R | R |
| 7 three_max | R | R | R |
| 8 neg_max_minus_unit | R | R | R |
| 9 two_neg_max | R | R | R |
| 10 pass_back | R | R | R |
| 11 pass_back_unit | R | R | R |
| 12 cancel | R | R | R |
| 13 max_minus_unit | R | R | R |
| 14 unit | R | R | R |

### spark_door / false / avg / sql / avg_grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 6 two_max | = | = | = | = | = | = | = | = | = | = | = | = |
| 7 three_max | = | = | = | = | = | = | = | = | = | = | = | = |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | = | = | = |
| 10 pass_back | = | = | = | = | = | = | = | = | = | V | V | V |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | = | = | V |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / avg / sql / avg_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | V | V |
| 24 split_back_inside | = | = | = |

### spark_door / false / avg / sql / avg_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / avg / sql / avg_ungrouped

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / avg / sql / grouped_avg_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | V | V |

### spark_door / false / avg / sql / mp_avg_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | k | k |

### spark_door / false / avg / sql / mp_avg_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / avg / sql / ungrouped_avg_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | = | = |

### spark_door / false / control / dataframe / int_F.sum

| group | b |
|---|---|
| 41 fits | = |
| 42 bigint_over | = |
| 44 bigint_pass_back | = |

### spark_door / false / control / sql / int_avg

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_distinct

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_grouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_try

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_ungrouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_win_rows2

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / control / sql / int_win_running

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / false / side / sql / parquet_stats

| group | b |
|---|---|
| 41 fits | R |
| 42 bigint_over | R |

### spark_door / false / sum / dataframe / F.expr

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / F.sum_distinct

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | R | R | R |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | R | R | R |
| 6 two_max |   |   |   |   |   |   |   |   |   | R | R | R |
| 7 three_max |   |   |   |   |   |   |   |   |   | R | R | R |
| 10 pass_back |   |   |   |   |   |   |   |   |   | R | R | R |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | R | R | R |
| None all | R | R | R | R | R | R | R | R | R | R | R | R |

### spark_door / false / sum / dataframe / GroupedData.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / agg_F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / agg_dict

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / repartition

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / selectExpr

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / select_F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / window_part

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | V | V | V |
| 6 two_max |   |   |   |   |   |   |   |   |   | V | V | V |
| 7 three_max |   |   |   |   |   |   |   |   |   | V | V | V |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / dataframe / window_rows2

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | V | V | V |
| 6 two_max |   |   |   |   |   |   |   |   |   | V | V | V |
| 7 three_max |   |   |   |   |   |   |   |   |   | V | V | V |
| 10 pass_back |   |   |   |   |   |   |   |   |   | V | V | V |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | V | V | V |
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / iceberg / ctas

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / false / sum / iceberg / insert

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / false / sum / iceberg / merge_cow

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / false / sum / iceberg / merge_mor

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / false / sum / sql / as_string

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | F | F |
| 2 fits_nulls | = | F | F |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | F | F |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | F | F |

### spark_door / false / sum / sql / beside

| group | c10_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|
| 1 fits | = | = | = | = |
| 2 fits_nulls | = | = | = | = |
| 3 all_null | = | = | = | = |
| 4 max | = | = | = | = |
| 5 max_plus_unit | = | X | X | X |
| 6 two_max | = | W | W | W |
| 7 three_max | = | w | w | w |
| 8 neg_max_minus_unit | = | X | X | X |
| 9 two_neg_max | = | W | W | W |
| 10 pass_back | = | k | k | k |
| 11 pass_back_unit | = | k | k | k |
| 12 cancel | = | = | = | = |
| 13 max_minus_unit | = | = | = | = |
| 14 unit | = | = | = | = |

### spark_door / false / sum / sql / cte

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### spark_door / false / sum / sql / distinct

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | X | X | X |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | X | X | X |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | k | k | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / sum / sql / distinct_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / distinct_ungrouped

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | X | X | X |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | X | X | X |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / sum / sql / empty_grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / sum / sql / empty_ungrouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / sum / sql / grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 6 two_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 7 three_max | = | = | = | = | = | = | = | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = | = | = | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | k | k | k |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / sum / sql / grouped_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | w | w | w |

### spark_door / false / sum / sql / grouped_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | X | X |
| 32 x17100_max_wraps | W | W |
| 33 x10001_back | k | k |

### spark_door / false / sum / sql / grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | k | k |
| 24 split_back_inside | = | = | = |

### spark_door / false / sum / sql / grouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / sum / sql / having

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / sum / sql / having_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / linear

| group | c10_2 | c38_0 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | = |
| 5 max_plus_unit | = | = |
| 6 two_max | = | = |
| 7 three_max | = | = |
| 8 neg_max_minus_unit | = | = |
| 9 two_neg_max | = | V |
| 10 pass_back | = | V |
| 11 pass_back_unit | = | = |
| 12 cancel | = | V |
| 13 max_minus_unit | = | = |
| 14 unit | = | = |

### spark_door / false / sum / sql / mp_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | k | k |

### spark_door / false / sum / sql / mp_grouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / sum / sql / mp_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / false / sum / sql / mp_ungrouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / sum / sql / plus1

| group | c10_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|
| 1 fits | = | = | = | = |
| 2 fits_nulls | = | = | = | = |
| 3 all_null | = | = | = | = |
| 4 max | = | = | = | R |
| 5 max_plus_unit | = | = | V | R |
| 6 two_max | = | = | V | V |
| 7 three_max | = | V | V | V |
| 8 neg_max_minus_unit | = | V | V | V |
| 9 two_neg_max | = | = | V | R |
| 10 pass_back | = | V | V | V |
| 11 pass_back_unit | = | = | V | R |
| 12 cancel | = | = | = | = |
| 13 max_minus_unit | = | = | = | R |
| 14 unit | = | = | = | = |

### spark_door / false / sum / sql / scalar_sub

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | = |

### spark_door / false / sum / sql / temp_view

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### spark_door / false / sum / sql / ungrouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 6 two_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 7 three_max | = | = | = | = | = | = | = | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = | = | = | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / sum / sql / ungrouped_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | X | X |
| 32 x17100_max_wraps | W | W |
| 33 x10001_back | = | = |

### spark_door / false / sum / sql / ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / false / sum / sql / ungrouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / sum / sql / win_default

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / sum / sql / win_default_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / win_parallel

| group | c38_0 |
|---|---|
| 4 max | = |
| 5 max_plus_unit | V |
| 6 two_max | V |
| 7 three_max | V |
| 8 neg_max_minus_unit | V |
| 9 two_neg_max | V |
| 10 pass_back | V |
| 11 pass_back_unit | V |
| 12 cancel | = |
| 13 max_minus_unit | = |

### spark_door / false / sum / sql / win_part

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / sum / sql / win_part_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / win_range

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / sum / sql / win_range_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / win_rows2

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / sum / sql / win_rows2_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / sum / sql / win_running

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | V | V | V |
| 6 two_max | V | V | V |
| 7 three_max | V | V | V |
| 8 neg_max_minus_unit | V | V | V |
| 9 two_neg_max | V | V | V |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / sum / sql / win_running_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | V | V | V |

### spark_door / false / try_sum / dataframe / F.try_sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| 6 two_max |   |   |   |   |   |   |   |   |   | = | = | = |
| 7 three_max |   |   |   |   |   |   |   |   |   | = | = | = |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / try_sum / sql / grouped_try_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | = | = |

### spark_door / false / try_sum / sql / mp_try_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / false / try_sum / sql / mp_try_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / try_sum / sql / mp_try_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | V | V |

### spark_door / false / try_sum / sql / try_empty

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / try_sum / sql / try_grouped

| group | c10_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = |
| 6 two_max | = | = | = | = | = |
| 7 three_max | = | = | = | = | = |
| 8 neg_max_minus_unit | = | = | = | = | = |
| 9 two_neg_max | = | = | = | = | = |
| 10 pass_back | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / try_sum / sql / try_grouped_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / false / try_sum / sql / try_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | k | k |
| 24 split_back_inside | = | V | V |

### spark_door / false / try_sum / sql / try_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | V | = | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / false / try_sum / sql / try_ungrouped

| group | c10_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = |
| 6 two_max | = | = | = | = | = |
| 7 three_max | = | = | = | = | = |
| 8 neg_max_minus_unit | = | = | = | = | = |
| 9 two_neg_max | = | = | = | = | = |
| 10 pass_back | = | = | V | V | V |
| 11 pass_back_unit | = | = | V | V | V |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / false / try_sum / sql / try_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | V | V |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | V | V |

### spark_door / false / try_sum / sql / try_win_part

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / try_sum / sql / try_win_rows2

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / false / try_sum / sql / ungrouped_try_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | V | V |

### spark_door / true / avg / sql / avg_grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | B | B | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | B | B | B |
| 6 two_max | = | = | = | = | = | = | = | = | = | B | B | B |
| 7 three_max | = | = | = | = | = | = | = | = | = | B | B | B |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | B | B | B |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | B | B | B |
| 10 pass_back | = | = | = | = | = | = | = | = | = | S | S | S |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | b | b | S |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | B | B | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / avg / sql / avg_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | B | B |
| 22 split_total_past | = | B | B |
| 23 split_fits | = | S | S |
| 24 split_back_inside | = | = | = |

### spark_door / true / avg / sql / avg_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | B | B | = |
| 5 max_plus_unit | B | B | B |
| 6 two_max | B | B | B |
| 7 three_max | B | B | B |
| 8 neg_max_minus_unit | B | B | B |
| 9 two_neg_max | B | B | B |
| 10 pass_back | B | B | B |
| 11 pass_back_unit | B | B | = |
| 12 cancel | B | B | B |
| 13 max_minus_unit | B | B | = |
| 14 unit | = | = | = |

### spark_door / true / avg / sql / avg_ungrouped

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | B | B | = |
| 5 max_plus_unit | B | B | B |
| 6 two_max | B | B | B |
| 7 three_max | B | B | B |
| 8 neg_max_minus_unit | B | B | B |
| 9 two_neg_max | B | B | B |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | B | B | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | B | B | = |
| 14 unit | = | = | = |

### spark_door / true / avg / sql / grouped_avg_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | B | B |
| 32 x17100_max_wraps | B | B |
| 33 x10001_back | S | S |

### spark_door / true / avg / sql / mp_avg_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | B | B |
| 22 split_total_past | = | B | B |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | k | k |

### spark_door / true / avg / sql / mp_avg_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | B | B | = |
| 5 max_plus_unit | B | B | B |
| 6 two_max | B | B | B |
| 7 three_max | B | B | B |
| 8 neg_max_minus_unit | B | B | B |
| 9 two_neg_max | B | B | B |
| 10 pass_back | B | B | B |
| 11 pass_back_unit | B | B | = |
| 12 cancel | B | B | B |
| 13 max_minus_unit | B | B | = |
| 14 unit | = | = | = |

### spark_door / true / avg / sql / ungrouped_avg_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | B | B |
| 32 x17100_max_wraps | B | B |
| 33 x10001_back | = | = |

### spark_door / true / control / dataframe / int_F.sum

| group | b |
|---|---|
| 41 fits | = |
| 42 bigint_over | S |
| 44 bigint_pass_back | S |

### spark_door / true / control / sql / int_avg

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_distinct

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_grouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_try

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | = | = |
| 43 bigint_under | = | = | = |
| 44 bigint_pass_back | = | = | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_ungrouped

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_win_rows2

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / control / sql / int_win_running

| group | i | b | d |
|---|---|---|---|
| 41 fits | = | = | = |
| 42 bigint_over | = | S | = |
| 43 bigint_under | = | S | = |
| 44 bigint_pass_back | = | S | = |
| 45 all_null | = | = | = |
| 46 int_max_pair | = | = | = |
| 47 double_over | = | = | = |

### spark_door / true / side / sql / parquet_stats

| group | b |
|---|---|
| 41 fits | R |
| 42 bigint_over | b |

### spark_door / true / sum / dataframe / F.expr

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / F.sum_distinct

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | R | R | R |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | b | b | b |
| 6 two_max |   |   |   |   |   |   |   |   |   | R | R | R |
| 7 three_max |   |   |   |   |   |   |   |   |   | R | R | R |
| 10 pass_back |   |   |   |   |   |   |   |   |   | R | R | R |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | b | b | b |
| None all | R | R | R | R | R | R | R | R | R | b | b | b |

### spark_door / true / sum / dataframe / GroupedData.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / agg_F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / agg_dict

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | k | k | k |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | k | k | k |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / repartition

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / selectExpr

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / select_F.sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | X | X | X |
| 6 two_max |   |   |   |   |   |   |   |   |   | W | W | W |
| 7 three_max |   |   |   |   |   |   |   |   |   | w | w | w |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / window_part

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | S | S | S |
| 6 two_max |   |   |   |   |   |   |   |   |   | S | S | S |
| 7 three_max |   |   |   |   |   |   |   |   |   | S | S | S |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / dataframe / window_rows2

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | S | S | S |
| 6 two_max |   |   |   |   |   |   |   |   |   | S | S | S |
| 7 three_max |   |   |   |   |   |   |   |   |   | S | S | S |
| 10 pass_back |   |   |   |   |   |   |   |   |   | S | S | S |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | S | S | S |
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / iceberg / ctas

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / true / sum / iceberg / insert

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / true / sum / iceberg / merge_cow

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / true / sum / iceberg / merge_mor

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| None all | = | k | k |

### spark_door / true / sum / sql / as_string

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / sum / sql / beside

| group | c10_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|
| 1 fits | = | = | = | = |
| 2 fits_nulls | = | = | = | = |
| 3 all_null | = | = | = | = |
| 4 max | = | B | B | = |
| 5 max_plus_unit | = | B | B | B |
| 6 two_max | = | B | B | B |
| 7 three_max | = | B | B | B |
| 8 neg_max_minus_unit | = | B | B | B |
| 9 two_neg_max | = | B | B | B |
| 10 pass_back | = | k | k | k |
| 11 pass_back_unit | = | b | b | k |
| 12 cancel | = | = | = | = |
| 13 max_minus_unit | = | B | B | = |
| 14 unit | = | = | = | = |

### spark_door / true / sum / sql / cte

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### spark_door / true / sum / sql / distinct

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | X | X | X |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | X | X | X |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | k | k | k |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / sum / sql / distinct_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / distinct_ungrouped

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | X | X | X |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | X | X | X |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / sum / sql / empty_grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / sum / sql / empty_ungrouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / sum / sql / grouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 6 two_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 7 three_max | = | = | = | = | = | = | = | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = | = | = | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | k | k | k |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / sum / sql / grouped_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | w | w | w |

### spark_door / true / sum / sql / grouped_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | X | X |
| 32 x17100_max_wraps | W | W |
| 33 x10001_back | k | k |

### spark_door / true / sum / sql / grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | k | k |
| 24 split_back_inside | = | = | = |

### spark_door / true / sum / sql / grouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / sum / sql / having

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / sum / sql / having_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / linear

| group | c10_2 | c38_0 |
|---|---|---|
| 1 fits | = | = |
| 2 fits_nulls | = | = |
| 3 all_null | = | = |
| 4 max | = | B |
| 5 max_plus_unit | = | B |
| 6 two_max | = | B |
| 7 three_max | = | B |
| 8 neg_max_minus_unit | = | = |
| 9 two_neg_max | = | S |
| 10 pass_back | = | B |
| 11 pass_back_unit | = | B |
| 12 cancel | = | B |
| 13 max_minus_unit | = | B |
| 14 unit | = | = |

### spark_door / true / sum / sql / mp_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | k | k |

### spark_door / true / sum / sql / mp_grouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / sum / sql / mp_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | k | k |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / true / sum / sql / mp_ungrouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | k | k | k |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | k | k | k |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / sum / sql / plus1

| group | c10_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|
| 1 fits | = | = | = | = |
| 2 fits_nulls | = | = | = | = |
| 3 all_null | = | = | = | = |
| 4 max | = | B | = | R |
| 5 max_plus_unit | = | b | S | b |
| 6 two_max | = | b | S | S |
| 7 three_max | = | S | S | S |
| 8 neg_max_minus_unit | = | S | S | S |
| 9 two_neg_max | = | b | S | b |
| 10 pass_back | = | S | S | S |
| 11 pass_back_unit | = | b | S | b |
| 12 cancel | = | = | = | = |
| 13 max_minus_unit | = | = | = | R |
| 14 unit | = | = | = | = |

### spark_door / true / sum / sql / scalar_sub

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | = |

### spark_door / true / sum / sql / temp_view

| group | c38_0 |
|---|---|
| 1 fits | = |
| 5 max_plus_unit | X |
| 6 two_max | W |
| 10 pass_back | k |

### spark_door / true / sum / sql / ungrouped

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = | = | = | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = | = | = | = | = | = | = | = |
| 3 all_null | = | = | = | = | = | = | = | = | = | = | = | = |
| 4 max | = | = | = | = | = | = | = | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 6 two_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 7 three_max | = | = | = | = | = | = | = | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | = | = | = | = | = | = | = | X | X | X |
| 9 two_neg_max | = | = | = | = | = | = | = | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = | = | = | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 12 cancel | = | = | = | = | = | = | = | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = | = | = | = | = | = | = | = |
| 14 unit | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / sum / sql / ungrouped_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | X | X |
| 32 x17100_max_wraps | W | W |
| 33 x10001_back | = | = |

### spark_door / true / sum / sql / ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | X | X |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / true / sum / sql / ungrouped_s4

| group | c10_2 | c28_2 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | X | X | X |
| 6 two_max | = | = | W | W | W |
| 7 three_max | = | = | w | w | w |
| 8 neg_max_minus_unit | = | = | X | X | X |
| 9 two_neg_max | = | = | W | W | W |
| 10 pass_back | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / sum / sql / win_default

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / sum / sql / win_default_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / win_parallel

| group | c38_0 |
|---|---|
| 4 max | = |
| 5 max_plus_unit | S |
| 6 two_max | S |
| 7 three_max | S |
| 8 neg_max_minus_unit | S |
| 9 two_neg_max | S |
| 10 pass_back | S |
| 11 pass_back_unit | S |
| 12 cancel | = |
| 13 max_minus_unit | = |

### spark_door / true / sum / sql / win_part

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / sum / sql / win_part_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / win_range

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / sum / sql / win_range_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / win_rows2

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / sum / sql / win_rows2_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / sum / sql / win_running

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | S | S | S |
| 6 two_max | S | S | S |
| 7 three_max | S | S | S |
| 8 neg_max_minus_unit | S | S | S |
| 9 two_neg_max | S | S | S |
| 10 pass_back | S | S | S |
| 11 pass_back_unit | S | S | S |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / sum / sql / win_running_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | S | S | S |

### spark_door / true / try_sum / dataframe / F.try_sum

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 fits |   |   |   |   |   |   |   |   |   | = | = | = |
| 5 max_plus_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| 6 two_max |   |   |   |   |   |   |   |   |   | = | = | = |
| 7 three_max |   |   |   |   |   |   |   |   |   | = | = | = |
| 10 pass_back |   |   |   |   |   |   |   |   |   | = | = | = |
| 11 pass_back_unit |   |   |   |   |   |   |   |   |   | = | = | = |
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / try_sum / sql / grouped_try_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | = | = |

### spark_door / true / try_sum / sql / mp_try_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | = | = |

### spark_door / true / try_sum / sql / mp_try_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | V | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / try_sum / sql / mp_try_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | V | V |

### spark_door / true / try_sum / sql / try_empty

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / try_sum / sql / try_grouped

| group | c10_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = |
| 6 two_max | = | = | = | = | = |
| 7 three_max | = | = | = | = | = |
| 8 neg_max_minus_unit | = | = | = | = | = |
| 9 two_neg_max | = | = | = | = | = |
| 10 pass_back | = | = | = | = | = |
| 11 pass_back_unit | = | = | = | = | = |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / try_sum / sql / try_grouped_all

| group | c10_0 | c10_2 | c18_0 | c18_4 | c19_0 | c19_9 | c28_0 | c28_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| None all | = | = | = | = | = | = | = | = | = | = | = | = |

### spark_door / true / try_sum / sql / try_grouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | = | = |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | k | k |
| 24 split_back_inside | = | V | V |

### spark_door / true / try_sum / sql / try_grouped_s4

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 1 fits | = | = | = |
| 2 fits_nulls | = | = | = |
| 3 all_null | = | = | = |
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |
| 14 unit | = | = | = |

### spark_door / true / try_sum / sql / try_ungrouped

| group | c10_2 | c34_4 | c38_0 | c38_10 | c38_38 |
|---|---|---|---|---|---|
| 1 fits | = | = | = | = | = |
| 2 fits_nulls | = | = | = | = | = |
| 3 all_null | = | = | = | = | = |
| 4 max | = | = | = | = | = |
| 5 max_plus_unit | = | = | = | = | = |
| 6 two_max | = | = | = | = | = |
| 7 three_max | = | = | = | = | = |
| 8 neg_max_minus_unit | = | = | = | = | = |
| 9 two_neg_max | = | = | = | = | = |
| 10 pass_back | = | = | V | V | V |
| 11 pass_back_unit | = | = | V | V | V |
| 12 cancel | = | = | = | = | = |
| 13 max_minus_unit | = | = | = | = | = |
| 14 unit | = | = | = | = | = |

### spark_door / true / try_sum / sql / try_ungrouped_p2

| group | c10_2 | c38_0 | c38_38 |
|---|---|---|---|
| 21 split_pass_back | = | V | V |
| 22 split_total_past | = | = | = |
| 23 split_fits | = | = | = |
| 24 split_back_inside | = | V | V |

### spark_door / true / try_sum / sql / try_win_part

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | V | V | V |
| 11 pass_back_unit | V | V | V |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / try_sum / sql / try_win_rows2

| group | c38_0 | c38_10 | c38_38 |
|---|---|---|---|
| 4 max | = | = | = |
| 5 max_plus_unit | = | = | = |
| 6 two_max | = | = | = |
| 7 three_max | = | = | = |
| 8 neg_max_minus_unit | = | = | = |
| 9 two_neg_max | = | = | = |
| 10 pass_back | = | = | = |
| 11 pass_back_unit | = | = | = |
| 12 cancel | = | = | = |
| 13 max_minus_unit | = | = | = |

### spark_door / true / try_sum / sql / ungrouped_try_big

| group | c34_0 | c34_4 |
|---|---|---|
| 31 x10001_max | = | = |
| 32 x17100_max_wraps | = | = |
| 33 x10001_back | V | V |

