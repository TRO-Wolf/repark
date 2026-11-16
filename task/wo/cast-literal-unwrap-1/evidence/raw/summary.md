### repark: by kind

| kind | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| bigint_int | false | 154 | 15 | 0 | 129 | 10 | 0 | 0 |
| bigint_int | true | 154 | 0 | 0 | 0 | 0 | 154 | 0 |
| date_ts | false | 116 | 106 | 0 | 10 | 0 | 0 | 0 |
| date_ts | true | 116 | 106 | 0 | 10 | 0 | 0 | 0 |
| dec38_int_overflow | false | 116 | 15 | 0 | 101 | 0 | 0 | 0 |
| dec38_int_overflow | true | 116 | 0 | 0 | 0 | 0 | 116 | 0 |
| dec_bigint | false | 130 | 23 | 0 | 107 | 0 | 0 | 0 |
| dec_bigint | true | 130 | 23 | 0 | 107 | 0 | 0 | 0 |
| dec_dec0 | false | 182 | 33 | 0 | 139 | 10 | 0 | 0 |
| dec_dec0 | true | 182 | 33 | 0 | 139 | 10 | 0 | 0 |
| dec_dec1 | false | 154 | 42 | 0 | 104 | 8 | 0 | 0 |
| dec_dec1 | true | 154 | 42 | 0 | 104 | 8 | 0 | 0 |
| dec_int | false | 196 | 35 | 0 | 151 | 10 | 0 | 0 |
| dec_int | true | 196 | 35 | 0 | 151 | 10 | 0 | 0 |
| dec_prec_overflow | false | 130 | 60 | 0 | 70 | 0 | 0 | 0 |
| dec_prec_overflow | true | 130 | 0 | 0 | 0 | 0 | 130 | 0 |
| dec_round_overflow | false | 116 | 17 | 0 | 95 | 0 | 0 | 4 |
| dec_round_overflow | true | 116 | 0 | 4 | 0 | 0 | 112 | 0 |
| dec_smallint | false | 116 | 19 | 0 | 97 | 0 | 0 | 0 |
| dec_smallint | true | 116 | 19 | 0 | 97 | 0 | 0 | 0 |
| dec_tinyint | false | 116 | 19 | 0 | 97 | 0 | 0 | 0 |
| dec_tinyint | true | 116 | 19 | 0 | 97 | 0 | 0 | 0 |
| dec_tinyint_overflow | false | 116 | 6 | 0 | 110 | 0 | 0 | 0 |
| dec_tinyint_overflow | true | 116 | 0 | 0 | 4 | 0 | 112 | 0 |
| dec_wide | false | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| dec_wide | true | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| double_bigint | false | 102 | 102 | 0 | 0 | 0 | 0 | 0 |
| double_bigint | true | 102 | 102 | 0 | 0 | 0 | 0 | 0 |
| double_dec | false | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| double_dec | true | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| double_dec_overflow | false | 116 | 0 | 0 | 0 | 0 | 0 | 116 |
| double_dec_overflow | true | 116 | 0 | 116 | 0 | 0 | 0 | 0 |
| double_float | false | 118 | 118 | 0 | 0 | 0 | 0 | 0 |
| double_float | true | 118 | 118 | 0 | 0 | 0 | 0 | 0 |
| double_int | false | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| double_int | true | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| double_int_edge | false | 116 | 0 | 0 | 0 | 0 | 0 | 116 |
| double_int_edge | true | 116 | 0 | 116 | 0 | 0 | 0 | 0 |
| float_int | false | 102 | 102 | 0 | 0 | 0 | 0 | 0 |
| float_int | true | 102 | 102 | 0 | 0 | 0 | 0 | 0 |
| int_bigint | false | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| int_bigint | true | 130 | 130 | 0 | 0 | 0 | 0 | 0 |
| int_smallint | false | 116 | 6 | 0 | 110 | 0 | 0 | 0 |
| int_smallint | true | 116 | 0 | 0 | 2 | 0 | 114 | 0 |
| ntz_date | false | 116 | 116 | 0 | 0 | 0 | 0 | 0 |
| ntz_date | true | 116 | 116 | 0 | 0 | 0 | 0 | 0 |
| str_date | false | 126 | 0 | 0 | 0 | 0 | 0 | 126 |
| str_date | true | 126 | 0 | 0 | 0 | 0 | 0 | 126 |
| str_dec | false | 116 | 0 | 0 | 0 | 0 | 0 | 116 |
| str_dec | true | 116 | 0 | 0 | 0 | 0 | 0 | 116 |
| str_double | false | 116 | 116 | 0 | 0 | 0 | 0 | 0 |
| str_double | true | 116 | 116 | 0 | 0 | 0 | 0 | 0 |
| str_int | false | 154 | 0 | 0 | 0 | 0 | 0 | 154 |
| str_int | true | 154 | 0 | 154 | 0 | 0 | 0 | 0 |
| str_int_invalid | false | 102 | 0 | 0 | 0 | 0 | 0 | 102 |
| str_int_invalid | true | 102 | 0 | 102 | 0 | 0 | 0 | 0 |
| ts_date | false | 140 | 140 | 0 | 0 | 0 | 0 | 0 |
| ts_date | true | 140 | 140 | 0 | 0 | 0 | 0 | 0 |

### repark: by family

| family | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| control | false | 376 | 366 | 0 | 10 | 0 | 0 | 0 |
| control | true | 376 | 366 | 0 | 10 | 0 | 0 | 0 |
| decimal-overflow | false | 478 | 98 | 0 | 376 | 0 | 0 | 4 |
| decimal-overflow | true | 478 | 0 | 4 | 4 | 0 | 470 | 0 |
| decimal-to-integer | false | 558 | 96 | 0 | 452 | 10 | 0 | 0 |
| decimal-to-integer | true | 558 | 96 | 0 | 452 | 10 | 0 | 0 |
| decimal-to-smaller-scale | false | 336 | 75 | 0 | 243 | 18 | 0 | 0 |
| decimal-to-smaller-scale | true | 336 | 75 | 0 | 243 | 18 | 0 | 0 |
| double-to-decimal | false | 246 | 130 | 0 | 0 | 0 | 0 | 116 |
| double-to-decimal | true | 246 | 130 | 116 | 0 | 0 | 0 | 0 |
| double-to-float | false | 118 | 118 | 0 | 0 | 0 | 0 | 0 |
| double-to-float | true | 118 | 118 | 0 | 0 | 0 | 0 | 0 |
| float-to-integer | false | 450 | 334 | 0 | 0 | 0 | 0 | 116 |
| float-to-integer | true | 450 | 334 | 116 | 0 | 0 | 0 | 0 |
| integer-overflow | false | 270 | 21 | 0 | 239 | 10 | 0 | 0 |
| integer-overflow | true | 270 | 0 | 0 | 2 | 0 | 268 | 0 |
| string | false | 614 | 116 | 0 | 0 | 0 | 0 | 498 |
| string | true | 614 | 116 | 256 | 0 | 0 | 0 | 242 |
| temporal | false | 256 | 256 | 0 | 0 | 0 | 0 | 0 |
| temporal | true | 256 | 256 | 0 | 0 | 0 | 0 | 0 |

### repark: by storage

| storage | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| ice_cow | false | 35 | 6 | 0 | 0 | 19 | 0 | 10 |
| ice_cow | true | 35 | 6 | 5 | 0 | 14 | 5 | 5 |
| ice_files | false | 435 | 198 | 0 | 147 | 0 | 0 | 90 |
| ice_files | true | 435 | 180 | 60 | 75 | 0 | 90 | 30 |
| ice_mor | false | 35 | 6 | 0 | 0 | 19 | 0 | 10 |
| ice_mor | true | 35 | 6 | 5 | 0 | 14 | 5 | 5 |
| ice_part | false | 719 | 305 | 0 | 262 | 0 | 0 | 152 |
| ice_part | true | 719 | 281 | 102 | 139 | 0 | 147 | 50 |
| parquet | false | 435 | 198 | 0 | 147 | 0 | 0 | 90 |
| parquet | true | 435 | 180 | 60 | 75 | 0 | 90 | 30 |
| view | false | 2043 | 897 | 0 | 764 | 0 | 0 | 382 |
| view | true | 2043 | 838 | 260 | 422 | 0 | 401 | 122 |

### repark: by door

| door | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| dataframe | false | 336 | 120 | 0 | 144 | 0 | 0 | 72 |
| dataframe | true | 336 | 112 | 48 | 82 | 0 | 70 | 24 |
| fexpr | false | 116 | 46 | 0 | 42 | 0 | 0 | 28 |
| fexpr | true | 116 | 44 | 20 | 24 | 0 | 20 | 8 |
| filter_str | false | 116 | 48 | 0 | 44 | 0 | 0 | 24 |
| filter_str | true | 116 | 46 | 16 | 22 | 0 | 24 | 8 |
| sql | false | 3134 | 1396 | 0 | 1090 | 38 | 0 | 610 |
| sql | true | 3134 | 1289 | 408 | 583 | 28 | 624 | 202 |

### repark: by pos

| pos | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| case | false | 29 | 12 | 0 | 11 | 0 | 0 | 6 |
| case | true | 29 | 11 | 4 | 6 | 0 | 6 | 2 |
| delete_eq | false | 14 | 2 | 0 | 0 | 8 | 0 | 4 |
| delete_eq | true | 14 | 2 | 2 | 0 | 6 | 2 | 2 |
| delete_gt | false | 14 | 4 | 0 | 0 | 6 | 0 | 4 |
| delete_gt | true | 14 | 4 | 2 | 0 | 4 | 2 | 2 |
| having | false | 29 | 12 | 0 | 11 | 0 | 0 | 6 |
| having | true | 29 | 11 | 4 | 6 | 0 | 6 | 2 |
| join | false | 116 | 48 | 0 | 44 | 0 | 0 | 24 |
| join | true | 116 | 44 | 16 | 24 | 0 | 24 | 8 |
| merge_and | false | 14 | 2 | 0 | 0 | 8 | 0 | 4 |
| merge_and | true | 14 | 2 | 2 | 0 | 6 | 2 | 2 |
| merge_on | false | 14 | 2 | 0 | 0 | 8 | 0 | 4 |
| merge_on | true | 14 | 2 | 2 | 0 | 6 | 2 | 2 |
| select | false | 259 | 99 | 0 | 104 | 0 | 0 | 56 |
| select | true | 259 | 99 | 38 | 52 | 0 | 52 | 18 |
| update_eq | false | 14 | 2 | 0 | 0 | 8 | 0 | 4 |
| update_eq | true | 14 | 2 | 2 | 0 | 6 | 2 | 2 |
| where | false | 3199 | 1427 | 0 | 1150 | 0 | 0 | 622 |
| where | true | 3199 | 1314 | 420 | 623 | 0 | 640 | 202 |

### native: by kind

| kind | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| bigint_int | false | 79 | 6 | 0 | 73 | 0 | 0 | 0 |
| bigint_int | true | 79 | 0 | 0 | 0 | 0 | 79 | 0 |
| date_ts | false | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| date_ts | true | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| dec38_int_overflow | false | 51 | 6 | 0 | 45 | 0 | 0 | 0 |
| dec38_int_overflow | true | 51 | 0 | 0 | 0 | 0 | 51 | 0 |
| dec_bigint | false | 65 | 27 | 0 | 38 | 0 | 0 | 0 |
| dec_bigint | true | 65 | 27 | 0 | 38 | 0 | 0 | 0 |
| dec_dec0 | false | 107 | 18 | 0 | 89 | 0 | 0 | 0 |
| dec_dec0 | true | 107 | 18 | 0 | 89 | 0 | 0 | 0 |
| dec_dec1 | false | 79 | 30 | 0 | 49 | 0 | 0 | 0 |
| dec_dec1 | true | 79 | 30 | 0 | 49 | 0 | 0 | 0 |
| dec_int | false | 121 | 39 | 0 | 82 | 0 | 0 | 0 |
| dec_int | true | 121 | 39 | 0 | 82 | 0 | 0 | 0 |
| dec_prec_overflow | false | 65 | 21 | 0 | 44 | 0 | 0 | 0 |
| dec_prec_overflow | true | 65 | 0 | 0 | 0 | 0 | 65 | 0 |
| dec_round_overflow | false | 51 | 8 | 0 | 43 | 0 | 0 | 0 |
| dec_round_overflow | true | 51 | 0 | 0 | 0 | 0 | 51 | 0 |
| dec_smallint | false | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_smallint | true | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_tinyint | false | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_tinyint | true | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_tinyint_overflow | false | 51 | 6 | 0 | 45 | 0 | 0 | 0 |
| dec_tinyint_overflow | true | 51 | 0 | 0 | 2 | 0 | 49 | 0 |
| dec_wide | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| dec_wide | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_bigint | false | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| double_bigint | true | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| double_dec | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_dec | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_dec_overflow | false | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| double_dec_overflow | true | 51 | 0 | 51 | 0 | 0 | 0 | 0 |
| double_float | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_float | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_int | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_int | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_int_edge | false | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| double_int_edge | true | 51 | 0 | 51 | 0 | 0 | 0 | 0 |
| float_int | false | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| float_int | true | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| int_bigint | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| int_bigint | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| int_smallint | false | 51 | 6 | 0 | 45 | 0 | 0 | 0 |
| int_smallint | true | 51 | 0 | 0 | 2 | 0 | 49 | 0 |
| ntz_date | false | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| ntz_date | true | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| str_date | false | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_date | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_dec | false | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_dec | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_double | false | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_double | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_int | false | 79 | 0 | 0 | 0 | 0 | 0 | 79 |
| str_int | true | 79 | 0 | 79 | 0 | 0 | 0 | 0 |
| str_int_invalid | false | 37 | 0 | 0 | 0 | 0 | 0 | 37 |
| str_int_invalid | true | 37 | 0 | 37 | 0 | 0 | 0 | 0 |
| ts_date | false | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| ts_date | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |

### native: by storage

| storage | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| native_view | false | 1759 | 759 | 0 | 629 | 0 | 0 | 371 |
| native_view | true | 1759 | 706 | 218 | 338 | 0 | 344 | 153 |

### native: by pos

| pos | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| case | false | 29 | 11 | 0 | 11 | 0 | 0 | 7 |
| case | true | 29 | 10 | 4 | 6 | 0 | 6 | 3 |
| having | false | 29 | 11 | 0 | 11 | 0 | 0 | 7 |
| having | true | 29 | 10 | 4 | 6 | 0 | 6 | 3 |
| join | false | 29 | 11 | 0 | 11 | 0 | 0 | 7 |
| join | true | 29 | 10 | 4 | 6 | 0 | 6 | 3 |
| select | false | 58 | 24 | 0 | 20 | 0 | 0 | 14 |
| select | true | 58 | 24 | 8 | 8 | 0 | 12 | 6 |
| where | false | 1614 | 702 | 0 | 576 | 0 | 0 | 336 |
| where | true | 1614 | 652 | 198 | 312 | 0 | 314 | 138 |

### totals

- native ANSI false agree: 759
- native ANSI false over_raise: 371
- native ANSI false wrong_answer: 629
- native ANSI true agree: 706
- native ANSI true both_raise: 218
- native ANSI true missed_raise: 344
- native ANSI true over_raise: 153
- native ANSI true wrong_answer: 338
- repark ANSI false agree: 1610
- repark ANSI false over_raise: 734
- repark ANSI false wrong_answer: 1320
- repark ANSI false wrong_write: 38
- repark ANSI true agree: 1491
- repark ANSI true both_raise: 492
- repark ANSI true missed_raise: 738
- repark ANSI true over_raise: 242
- repark ANSI true wrong_answer: 711
- repark ANSI true wrong_write: 28

### wrong writes and DML cells

- `bigint_int/dml/cow/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '8:o', '9:o']; RePark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '1:o', '3:o', '4:o', '6:o', '7:o', '9:o']; RePark ['10:o', '1:o', '3:o', '4:o', '8:o', '9:o']
- `bigint_int/dml/cow/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:m', '8:o', '9:o']; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:m', '8:o', '9:o']; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:u', '8:o', '9:o']; RePark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '8:o', '9:o']; RePark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '1:o', '3:o', '4:o', '6:o', '7:o', '9:o']; RePark ['10:o', '1:o', '3:o', '4:o', '8:o', '9:o']
- `bigint_int/dml/mor/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:m', '8:o', '9:o']; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:m', '8:o', '9:o']; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:u', '8:o', '9:o']; RePark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_dec0/dml/cow/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:u', '2:u', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_dec0/dml/mor/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:u', '2:u', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/delete_gt` ANSI false: **agree**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']
- `dec_dec1/dml/cow/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/delete_gt` ANSI false: **agree**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']
- `dec_dec1/dml/mor/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_int/dml/cow/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:u', '4:u', '5:u', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/delete_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/delete_gt` ANSI false: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_int/dml/mor/merge_and` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/merge_on` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/update_eq` ANSI false: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:u', '4:u', '5:u', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `str_date/dml/cow/delete_eq` ANSI false: **over_raise**; Spark ['8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/delete_gt` ANSI false: **over_raise**; Spark ['1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/merge_and` ANSI false: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/merge_on` ANSI false: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/update_eq` ANSI false: **over_raise**; Spark ['1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/delete_eq` ANSI false: **over_raise**; Spark ['8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/delete_gt` ANSI false: **over_raise**; Spark ['1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/merge_and` ANSI false: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/merge_on` ANSI false: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/update_eq` ANSI false: **over_raise**; Spark ['1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_int/dml/cow/delete_eq` ANSI false: **over_raise**; Spark ['10:o', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/delete_gt` ANSI false: **over_raise**; Spark ['10:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/merge_and` ANSI false: **over_raise**; Spark ['10:o', '1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/merge_on` ANSI false: **over_raise**; Spark ['10:o', '1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/update_eq` ANSI false: **over_raise**; Spark ['10:o', '1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/delete_eq` ANSI false: **over_raise**; Spark ['10:o', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/delete_gt` ANSI false: **over_raise**; Spark ['10:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/merge_and` ANSI false: **over_raise**; Spark ['10:o', '1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/merge_on` ANSI false: **over_raise**; Spark ['10:o', '1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/update_eq` ANSI false: **over_raise**; Spark ['10:o', '1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `ts_date/dml/cow/delete_eq` ANSI false: **agree**; Spark ['4:o', '5:o', '6:o']; RePark ['4:o', '5:o', '6:o']
- `ts_date/dml/cow/delete_gt` ANSI false: **agree**; Spark ['1:o', '2:o', '3:o', '5:o', '6:o']; RePark ['1:o', '2:o', '3:o', '5:o', '6:o']
- `ts_date/dml/cow/merge_and` ANSI false: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/cow/merge_on` ANSI false: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/cow/update_eq` ANSI false: **agree**; Spark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']; RePark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/delete_eq` ANSI false: **agree**; Spark ['4:o', '5:o', '6:o']; RePark ['4:o', '5:o', '6:o']
- `ts_date/dml/mor/delete_gt` ANSI false: **agree**; Spark ['1:o', '2:o', '3:o', '5:o', '6:o']; RePark ['1:o', '2:o', '3:o', '5:o', '6:o']
- `ts_date/dml/mor/merge_and` ANSI false: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/merge_on` ANSI false: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/update_eq` ANSI false: **agree**; Spark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']; RePark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']
- `bigint_int/dml/cow/delete_eq` ANSI true: **missed_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/delete_gt` ANSI true: **missed_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ['10:o', '1:o', '3:o', '4:o', '8:o', '9:o']
- `bigint_int/dml/cow/merge_and` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/merge_on` ANSI true: **missed_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/cow/update_eq` ANSI true: **missed_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/delete_eq` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/delete_gt` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '1:o', '3:o', '4:o', '8:o', '9:o']
- `bigint_int/dml/mor/merge_and` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/merge_on` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '1:m', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `bigint_int/dml/mor/update_eq` ANSI true: **missed_raise**; Spark ERR:ArithmeticException:CAST_OVERFLOW:[CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to; RePark ['10:o', '1:u', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/delete_gt` ANSI true: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_dec0/dml/cow/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/cow/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:u', '2:u', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/delete_gt` ANSI true: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_dec0/dml/mor/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:m', '2:m', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec0/dml/mor/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:u', '2:u', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/delete_gt` ANSI true: **agree**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']
- `dec_dec1/dml/cow/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/cow/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/delete_gt` ANSI true: **agree**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '6:o', '7:o', '8:o']
- `dec_dec1/dml/mor/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:m', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_dec1/dml/mor/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:u', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/delete_gt` ANSI true: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_int/dml/cow/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/cow/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:u', '4:u', '5:u', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/delete_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/delete_gt` ANSI true: **wrong_write**; Spark ['10:o', '12:o', '13:o', '1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o']; RePark ['10:o', '12:o', '13:o', '1:o', '2:o', '6:o', '7:o', '8:o']
- `dec_int/dml/mor/merge_and` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/merge_on` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:m', '4:m', '5:m', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:m', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `dec_int/dml/mor/update_eq` ANSI true: **wrong_write**; Spark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:u', '4:u', '5:u', '6:o', '7:o', '8:o', '9:o']; RePark ['10:o', '11:o', '12:o', '13:o', '1:o', '2:u', '3:o', '4:o', '5:o', '6:o', '7:o', '8:o', '9:o']
- `str_date/dml/cow/delete_eq` ANSI true: **over_raise**; Spark ['8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/delete_gt` ANSI true: **over_raise**; Spark ['1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/merge_and` ANSI true: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/merge_on` ANSI true: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/cow/update_eq` ANSI true: **over_raise**; Spark ['1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/delete_eq` ANSI true: **over_raise**; Spark ['8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/delete_gt` ANSI true: **over_raise**; Spark ['1:o', '2:o', '3:o', '4:o', '5:o', '6:o', '7:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/merge_and` ANSI true: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string '2024' to value of Date32
- `str_date/dml/mor/merge_on` ANSI true: **over_raise**; Spark ['1:m', '2:m', '3:m', '4:m', '5:m', '6:m', '7:m', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_date/dml/mor/update_eq` ANSI true: **over_raise**; Spark ['1:u', '2:u', '3:u', '4:u', '5:u', '6:u', '7:u', '8:o', '9:o']; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 2024-01-01' to value of Date32
- `str_int/dml/cow/delete_eq` ANSI true: **both_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/delete_gt` ANSI true: **both_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/merge_and` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/merge_on` ANSI true: **both_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/cow/update_eq` ANSI true: **both_raise**; Spark ERR:Py4JJavaError:None:An error occurred while calling o46.sql. : org.apache.spark.SparkException: Exception thrown in awaitResult: at; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/delete_eq` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/delete_gt` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/merge_and` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string '1 ' to value of Int32
- `str_int/dml/mor/merge_on` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `str_int/dml/mor/update_eq` ANSI true: **both_raise**; Spark ERR:NumberFormatException:CAST_INVALID_INPUT:[CAST_INVALID_INPUT] The value '1.0' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct; RePark ERR:PySparkException:None:datafusion engine error: Arrow error: Cast error: Cannot cast string ' 1' to value of Int32
- `ts_date/dml/cow/delete_eq` ANSI true: **agree**; Spark ['4:o', '5:o', '6:o']; RePark ['4:o', '5:o', '6:o']
- `ts_date/dml/cow/delete_gt` ANSI true: **agree**; Spark ['1:o', '2:o', '3:o', '5:o', '6:o']; RePark ['1:o', '2:o', '3:o', '5:o', '6:o']
- `ts_date/dml/cow/merge_and` ANSI true: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/cow/merge_on` ANSI true: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/cow/update_eq` ANSI true: **agree**; Spark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']; RePark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/delete_eq` ANSI true: **agree**; Spark ['4:o', '5:o', '6:o']; RePark ['4:o', '5:o', '6:o']
- `ts_date/dml/mor/delete_gt` ANSI true: **agree**; Spark ['1:o', '2:o', '3:o', '5:o', '6:o']; RePark ['1:o', '2:o', '3:o', '5:o', '6:o']
- `ts_date/dml/mor/merge_and` ANSI true: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/merge_on` ANSI true: **agree**; Spark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']; RePark ['1:m', '2:m', '3:m', '4:o', '5:o', '6:o']
- `ts_date/dml/mor/update_eq` ANSI true: **agree**; Spark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']; RePark ['1:u', '2:u', '3:u', '4:o', '5:o', '6:o']

### kernel check: the cast compared with a column holding the constant (no unwrap possible)

| kind | ANSI | held cells | RePark held = Spark literal | Spark held = Spark literal |
|---|---|---|---|---|
| bigint_int | false | 8 | 0 | 8 |
| date_ts | false | 8 | 8 | 8 |
| dec38_int_overflow | false | 8 | 0 | 8 |
| dec_bigint | false | 8 | 8 | 8 |
| dec_dec0 | false | 8 | 8 | 8 |
| dec_dec1 | false | 8 | 8 | 8 |
| dec_int | false | 8 | 8 | 8 |
| dec_prec_overflow | false | 8 | 0 | 8 |
| dec_round_overflow | false | 8 | 0 | 8 |
| dec_smallint | false | 8 | 8 | 8 |
| dec_tinyint | false | 8 | 8 | 8 |
| dec_tinyint_overflow | false | 8 | 0 | 8 |
| dec_wide | false | 8 | 8 | 8 |
| double_bigint | false | 8 | 8 | 8 |
| double_dec | false | 8 | 8 | 8 |
| double_dec_overflow | false | 8 | 0 | 8 |
| double_float | false | 8 | 8 | 8 |
| double_int | false | 8 | 8 | 8 |
| double_int_edge | false | 8 | 0 | 8 |
| float_int | false | 8 | 8 | 8 |
| int_bigint | false | 8 | 8 | 8 |
| int_smallint | false | 8 | 0 | 8 |
| ntz_date | false | 8 | 8 | 8 |
| str_date | false | 8 | 0 | 8 |
| str_dec | false | 8 | 0 | 8 |
| str_double | false | 8 | 8 | 8 |
| str_int | false | 8 | 0 | 8 |
| str_int_invalid | false | 8 | 0 | 8 |
| ts_date | false | 8 | 8 | 8 |
| bigint_int | true | 8 | 8 | 8 |
| date_ts | true | 8 | 8 | 8 |
| dec38_int_overflow | true | 8 | 8 | 8 |
| dec_bigint | true | 8 | 8 | 8 |
| dec_dec0 | true | 8 | 8 | 8 |
| dec_dec1 | true | 8 | 8 | 8 |
| dec_int | true | 8 | 8 | 8 |
| dec_prec_overflow | true | 8 | 8 | 8 |
| dec_round_overflow | true | 8 | 8 | 8 |
| dec_smallint | true | 8 | 8 | 8 |
| dec_tinyint | true | 8 | 8 | 8 |
| dec_tinyint_overflow | true | 8 | 7 | 7 |
| dec_wide | true | 8 | 8 | 8 |
| double_bigint | true | 8 | 8 | 8 |
| double_dec | true | 8 | 8 | 8 |
| double_dec_overflow | true | 8 | 8 | 8 |
| double_float | true | 8 | 8 | 8 |
| double_int | true | 8 | 8 | 8 |
| double_int_edge | true | 8 | 8 | 8 |
| float_int | true | 8 | 8 | 8 |
| int_bigint | true | 8 | 8 | 8 |
| int_smallint | true | 8 | 7 | 7 |
| ntz_date | true | 8 | 8 | 8 |
| str_date | true | 8 | 0 | 8 |
| str_dec | true | 8 | 0 | 8 |
| str_double | true | 8 | 8 | 8 |
| str_int | true | 8 | 8 | 8 |
| str_int_invalid | true | 8 | 8 | 8 |
| ts_date | true | 8 | 8 | 8 |

### headline cells (temp view, `CAST(c AS T) = <first constant>`, constant on the right)

| kind | SQL | Spark ANSI off | RePark ANSI off | Spark ANSI on | RePark ANSI on |
|---|---|---|---|---|---|
| dec_int | `CAST(c AS INT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| dec_tinyint | `CAST(c AS TINYINT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| dec_smallint | `CAST(c AS SMALLINT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| dec_bigint | `CAST(c AS BIGINT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| dec_tinyint_overflow | `CAST(c AS TINYINT) = 1` | `[1]` | `[]` | raises ArithmeticException | `[]` |
| dec38_int_overflow | `CAST(c AS INT) = 1` | `[1, 5]` | `[1]` | raises ArithmeticException | `[1]` |
| dec_dec0 | `CAST(c AS DECIMAL(10,0)) = 1` | `[1, 2, 3]` | `[2]` | `[1, 2, 3]` | `[2]` |
| dec_dec1 | `CAST(c AS DECIMAL(10,1)) = 1.3` | `[3]` | `[]` | `[3]` | `[]` |
| dec_prec_overflow | `CAST(c AS DECIMAL(3,2)) = 1.25` | `[2]` | `[2]` | raises ArithmeticException | `[2]` |
| dec_round_overflow | `CAST(c AS DECIMAL(3,2)) = 9.99` | `[2]` | `[]` | raises ArithmeticException | `[]` |
| bigint_int | `CAST(c AS INT) = 1` | `[1, 7]` | `[1]` | raises ArithmeticException | `[1]` |
| int_smallint | `CAST(c AS SMALLINT) = 1` | `[1, 5]` | `[1]` | raises ArithmeticException | `[1]` |
| double_int | `CAST(c AS INT) = 1` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` |
| double_int_edge | `CAST(c AS INT) = 0` | `[2]` | raises PySparkException | raises ArithmeticException | raises PySparkException |
| double_bigint | `CAST(c AS BIGINT) = 1` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` |
| float_int | `CAST(c AS INT) = 1` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` | `[2, 3, 4, 5]` |
| double_float | `CAST(c AS FLOAT) = CAST(0.1 AS FLOAT)` | `[1, 2]` | `[1, 2]` | `[1, 2]` | `[1, 2]` |
| double_dec | `CAST(c AS DECIMAL(10,1)) = 1.3` | `[1, 6]` | `[1, 6]` | `[1, 6]` | `[1, 6]` |
| double_dec_overflow | `CAST(c AS DECIMAL(3,1)) = 1.3` | `[1]` | raises PySparkException | raises ArithmeticException | raises PySparkException |
| str_int | `CAST(c AS INT) = 1` | `[1, 2, 3, 4, 5, 6, 7]` | raises PySparkException | raises NumberFormatException | raises PySparkException |
| str_int_invalid | `CAST(c AS INT) = 1` | `[1]` | raises PySparkException | raises NumberFormatException | raises PySparkException |
| str_dec | `CAST(c AS DECIMAL(10,2)) = 1` | `[1, 2, 3, 4, 5, 6]` | raises PySparkException | `[1, 2, 3, 4, 5, 6]` | raises PySparkException |
| str_double | `CAST(c AS DOUBLE) = 1` | `[1, 2, 3, 4, 5, 6, 10]` | `[1, 2, 3, 4, 5, 6, 10]` | `[1, 2, 3, 4, 5, 6, 10]` | `[1, 2, 3, 4, 5, 6, 10]` |
| str_date | `CAST(c AS DATE) = DATE'2024-01-01'` | `[1, 2, 3, 4, 5, 6, 7]` | raises PySparkException | `[1, 2, 3, 4, 5, 6, 7]` | raises PySparkException |
| ts_date | `CAST(c AS DATE) = DATE'2024-01-01'` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` |
| ntz_date | `CAST(c AS DATE) = DATE'2024-01-01'` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` | `[1, 2, 3]` |
| int_bigint | `CAST(c AS BIGINT) = 1` | `[1]` | `[1]` | `[1]` | `[1]` |
| dec_wide | `CAST(c AS DECIMAL(20,4)) = 1.25` | `[3]` | `[3]` | `[3]` | `[3]` |
| date_ts | `CAST(c AS TIMESTAMP) = TIMESTAMP'2024-01-01 00:00:00'` | `[1]` | `[1]` | `[1]` | `[1]` |

### DML: the ids each statement deleted or retagged


ANSI false.

| kind | statement | Spark CoW | RePark CoW | Spark MoR | RePark MoR | verdict |
|---|---|---|---|---|---|---|
| bigint_int | delete_eq | [1, 7] | [1] | [1, 7] | [1] | **wrong write** |
| bigint_int | delete_gt | [2, 5, 8] | [2, 5, 6, 7] | [2, 5, 8] | [2, 5, 6, 7] | **wrong write** |
| bigint_int | merge_and | [1, 7] | [1] | [1, 7] | [1] | **wrong write** |
| bigint_int | merge_on | [1, 7] | [1] | [1, 7] | [1] | **wrong write** |
| bigint_int | update_eq | [1, 7] | [1] | [1, 7] | [1] | **wrong write** |
| dec_dec0 | delete_eq | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | delete_gt | [4, 5, 9, 11] | [3, 4, 5, 9, 11] | [4, 5, 9, 11] | [3, 4, 5, 9, 11] | **wrong write** |
| dec_dec0 | merge_and | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | merge_on | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | update_eq | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec1 | delete_eq | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | delete_gt | [4, 5, 9, 11] | [4, 5, 9, 11] | [4, 5, 9, 11] | [4, 5, 9, 11] | agree |
| dec_dec1 | merge_and | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | merge_on | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | update_eq | [3] | [] | [3] | [] | **wrong write** |
| dec_int | delete_eq | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | delete_gt | [9, 11] | [3, 4, 5, 9, 11] | [9, 11] | [3, 4, 5, 9, 11] | **wrong write** |
| dec_int | merge_and | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | merge_on | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | update_eq | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| str_date | delete_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | delete_gt | [8] | raises PySparkException | [8] | raises PySparkException | **over-raise** |
| str_date | merge_and | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | merge_on | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | update_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_int | delete_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_int | delete_gt | [9] | raises PySparkException | [9] | raises PySparkException | **over-raise** |
| str_int | merge_and | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_int | merge_on | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_int | update_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| ts_date | delete_eq | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | delete_gt | [4] | [4] | [4] | [4] | agree |
| ts_date | merge_and | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | merge_on | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | update_eq | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |

ANSI true.

| kind | statement | Spark CoW | RePark CoW | Spark MoR | RePark MoR | verdict |
|---|---|---|---|---|---|---|
| bigint_int | delete_eq | raises Py4JJavaError | [1] | raises ArithmeticException | [1] | **missed raise** |
| bigint_int | delete_gt | raises Py4JJavaError | [2, 5, 6, 7] | raises ArithmeticException | [2, 5, 6, 7] | **missed raise** |
| bigint_int | merge_and | raises ArithmeticException | [1] | raises ArithmeticException | [1] | **missed raise** |
| bigint_int | merge_on | raises Py4JJavaError | [1] | raises ArithmeticException | [1] | **missed raise** |
| bigint_int | update_eq | raises Py4JJavaError | [1] | raises ArithmeticException | [1] | **missed raise** |
| dec_dec0 | delete_eq | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | delete_gt | [4, 5, 9, 11] | [3, 4, 5, 9, 11] | [4, 5, 9, 11] | [3, 4, 5, 9, 11] | **wrong write** |
| dec_dec0 | merge_and | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | merge_on | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec0 | update_eq | [1, 2, 3] | [2] | [1, 2, 3] | [2] | **wrong write** |
| dec_dec1 | delete_eq | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | delete_gt | [4, 5, 9, 11] | [4, 5, 9, 11] | [4, 5, 9, 11] | [4, 5, 9, 11] | agree |
| dec_dec1 | merge_and | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | merge_on | [3] | [] | [3] | [] | **wrong write** |
| dec_dec1 | update_eq | [3] | [] | [3] | [] | **wrong write** |
| dec_int | delete_eq | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | delete_gt | [9, 11] | [3, 4, 5, 9, 11] | [9, 11] | [3, 4, 5, 9, 11] | **wrong write** |
| dec_int | merge_and | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | merge_on | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| dec_int | update_eq | [2, 3, 4, 5] | [2] | [2, 3, 4, 5] | [2] | **wrong write** |
| str_date | delete_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | delete_gt | [8] | raises PySparkException | [8] | raises PySparkException | **over-raise** |
| str_date | merge_and | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | merge_on | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_date | update_eq | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | [1, 2, 3, 4, 5, 6, 7] | raises PySparkException | **over-raise** |
| str_int | delete_eq | raises Py4JJavaError | raises PySparkException | raises NumberFormatException | raises PySparkException | both raise |
| str_int | delete_gt | raises Py4JJavaError | raises PySparkException | raises NumberFormatException | raises PySparkException | both raise |
| str_int | merge_and | raises NumberFormatException | raises PySparkException | raises NumberFormatException | raises PySparkException | both raise |
| str_int | merge_on | raises Py4JJavaError | raises PySparkException | raises NumberFormatException | raises PySparkException | both raise |
| str_int | update_eq | raises Py4JJavaError | raises PySparkException | raises NumberFormatException | raises PySparkException | both raise |
| ts_date | delete_eq | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | delete_gt | [4] | [4] | [4] | [4] | agree |
| ts_date | merge_and | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | merge_on | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
| ts_date | update_eq | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | [1, 2, 3] | agree |
