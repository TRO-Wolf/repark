# Summary (written by analyze.py)

## By engine and ANSI

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| native ansi=false | 907 | 176 | 12 | 279 | 20 | 0 | 420 | 0 |
| native ansi=true | 907 | 176 | 19 | 279 | 20 | 0 | 413 | 0 |
| repark ansi=false | 1310 | 1175 | 10 | 10 | 14 | 0 | 95 | 6 |
| repark ansi=true | 1310 | 1174 | 45 | 10 | 14 | 0 | 61 | 6 |

## By cause (RePark Spark door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (native) | 1814 | 352 | 31 | 558 | 40 | 0 | 833 | 0 |
| arithmetic-control ansi=false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| arithmetic-control ansi=true | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| avg-round ansi=false | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| avg-round ansi=true | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| common-type ansi=false | 946 | 925 | 4 | 5 | 0 | 0 | 12 | 0 |
| common-type ansi=true | 946 | 925 | 10 | 5 | 0 | 0 | 6 | 0 |
| confounded:array_union ansi=false | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:array_union ansi=true | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:join_full_using ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:join_full_using ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:nullif ansi=false | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| confounded:nullif ansi=true | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| confounded:nvl ansi=false | 10 | 0 | 0 | 4 | 6 | 0 | 0 | 0 |
| confounded:nvl ansi=true | 10 | 0 | 0 | 4 | 6 | 0 | 0 | 0 |
| control:array_union ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:array_union ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:core ansi=false | 165 | 161 | 0 | 0 | 4 | 0 | 0 | 0 |
| control:core ansi=true | 165 | 161 | 0 | 0 | 4 | 0 | 0 | 0 |
| control:join_full_using ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:join_full_using ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:nullif ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nullif ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:revision ansi=false | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| control:revision ansi=true | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-control ansi=false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-control ansi=true | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| int-literal-type ansi=false | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| int-literal-type ansi=true | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| legacy ansi=false | 94 | 66 | 0 | 0 | 0 | 0 | 28 | 0 |
| legacy ansi=true | 94 | 66 | 28 | 0 | 0 | 0 | 0 | 0 |
| pivot-unsupported ansi=false | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| pivot-unsupported ansi=true | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| range-offset ansi=false | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| range-offset ansi=true | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| update-cse ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| update-cse ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| using-unaliased-refusal ansi=false | 7 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |
| using-unaliased-refusal ansi=true | 7 | 0 | 0 | 0 | 0 | 0 | 7 | 0 |

## By cause (native door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (repark) | 2620 | 2349 | 55 | 20 | 28 | 0 | 156 | 12 |
| arithmetic-control ansi=false | 2 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| arithmetic-control ansi=true | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| avg-round ansi=false | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| avg-round ansi=true | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| common-type ansi=false | 675 | 43 | 0 | 266 | 7 | 0 | 359 | 0 |
| common-type ansi=true | 675 | 43 | 6 | 266 | 7 | 0 | 353 | 0 |
| confounded:array_union ansi=false | 10 | 0 | 10 | 0 | 0 | 0 | 0 | 0 |
| confounded:array_union ansi=true | 10 | 0 | 10 | 0 | 0 | 0 | 0 | 0 |
| confounded:join_full_using ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:join_full_using ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lag ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead ansi=false | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:lead ansi=true | 10 | 0 | 0 | 0 | 0 | 0 | 10 | 0 |
| confounded:nullif ansi=false | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:nullif ansi=true | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:nvl ansi=false | 10 | 0 | 0 | 8 | 2 | 0 | 0 | 0 |
| confounded:nvl ansi=true | 10 | 0 | 0 | 8 | 2 | 0 | 0 | 0 |
| control:array_union ansi=false | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |
| control:array_union ansi=true | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |
| control:core ansi=false | 139 | 125 | 0 | 0 | 6 | 0 | 8 | 0 |
| control:core ansi=true | 139 | 125 | 0 | 0 | 6 | 0 | 8 | 0 |
| control:join_full_using ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:join_full_using ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lag ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:lead ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| control:nullif ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nullif ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:nvl ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| grouping-control ansi=false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-control ansi=true | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| int-literal-type ansi=false | 1 | 0 | 0 | 0 | 1 | 0 | 0 | 0 |
| int-literal-type ansi=true | 1 | 0 | 0 | 0 | 1 | 0 | 0 | 0 |
| pivot-unsupported ansi=false | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| pivot-unsupported ansi=true | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| range-offset ansi=false | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| range-offset ansi=true | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |

## Common-type cells by group (RePark Spark door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 2542 | 851 | 72 | 568 | 68 | 0 | 971 | 12 |
| agg ansi=false | 12 | 11 | 0 | 1 | 0 | 0 | 0 | 0 |
| agg ansi=true | 12 | 11 | 0 | 1 | 0 | 0 | 0 | 0 |
| const ansi=false | 112 | 111 | 0 | 0 | 0 | 0 | 1 | 0 |
| const ansi=true | 112 | 111 | 0 | 0 | 0 | 0 | 1 | 0 |
| df:past38 ansi=false | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| df:past38 ansi=true | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| df:past38-int ansi=false | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| df:past38-int ansi=true | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| dfconst ansi=false | 20 | 19 | 0 | 1 | 0 | 0 | 0 | 0 |
| dfconst ansi=true | 20 | 19 | 0 | 1 | 0 | 0 | 0 | 0 |
| fexpr:past38 ansi=false | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| fexpr:past38 ansi=true | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| fexpr:past38-int ansi=false | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| fexpr:past38-int ansi=true | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| iceberg ansi=false | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| iceberg ansi=true | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| icedml ansi=false | 22 | 22 | 0 | 0 | 0 | 0 | 0 | 0 |
| icedml ansi=true | 22 | 22 | 0 | 0 | 0 | 0 | 0 | 0 |
| icepart ansi=false | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 0 |
| icepart ansi=true | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 0 |
| literal ansi=false | 18 | 8 | 0 | 2 | 0 | 0 | 8 | 0 |
| literal ansi=true | 18 | 8 | 6 | 2 | 0 | 0 | 2 | 0 |
| pair:past38 ansi=false | 297 | 297 | 0 | 0 | 0 | 0 | 0 | 0 |
| pair:past38 ansi=true | 297 | 297 | 0 | 0 | 0 | 0 | 0 | 0 |
| pair:past38-int ansi=false | 198 | 198 | 0 | 0 | 0 | 0 | 0 | 0 |
| pair:past38-int ansi=true | 198 | 198 | 0 | 0 | 0 | 0 | 0 | 0 |
| revision ansi=false | 52 | 44 | 4 | 1 | 0 | 0 | 3 | 0 |
| revision ansi=true | 52 | 44 | 4 | 1 | 0 | 0 | 3 | 0 |
| special ansi=false | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |
| special ansi=true | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |
| three ansi=false | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| three ansi=true | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| values ansi=false | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| values ansi=true | 10 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |

## By group (both ANSI modes, both doors)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| native agg | 28 | 8 | 0 | 14 | 4 | 0 | 2 | 0 |
| native const | 224 | 68 | 0 | 92 | 0 | 0 | 64 | 0 |
| native literal | 40 | 2 | 7 | 10 | 0 | 0 | 21 | 0 |
| native pair:control | 222 | 170 | 4 | 0 | 20 | 0 | 28 | 0 |
| native pair:control-int | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| native pair:past38 | 666 | 4 | 12 | 204 | 6 | 0 | 440 | 0 |
| native pair:past38-int | 444 | 6 | 8 | 190 | 8 | 0 | 232 | 0 |
| native special | 40 | 12 | 0 | 6 | 0 | 0 | 22 | 0 |
| native three | 48 | 0 | 0 | 26 | 0 | 0 | 22 | 0 |
| native values | 22 | 2 | 0 | 16 | 2 | 0 | 2 | 0 |
| repark agg | 28 | 24 | 0 | 4 | 0 | 0 | 0 | 0 |
| repark const | 224 | 222 | 0 | 0 | 0 | 0 | 2 | 0 |
| repark df:control | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark df:past38 | 160 | 160 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark df:past38-int | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark dfconst | 40 | 38 | 0 | 2 | 0 | 0 | 0 | 0 |
| repark fexpr:control | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark fexpr:past38 | 48 | 48 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark fexpr:past38-int | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark iceberg | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark icedml | 48 | 44 | 0 | 0 | 0 | 0 | 4 | 0 |
| repark icepart | 18 | 18 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark legacy | 20 | 16 | 2 | 0 | 0 | 0 | 2 | 0 |
| repark legacy:control | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark legacy:control-int | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark legacy:past38 | 72 | 36 | 18 | 0 | 0 | 0 | 18 | 0 |
| repark legacy:past38-int | 48 | 32 | 8 | 0 | 0 | 0 | 8 | 0 |
| repark literal | 40 | 19 | 7 | 4 | 0 | 0 | 10 | 0 |
| repark pair:control | 222 | 190 | 0 | 0 | 16 | 0 | 12 | 4 |
| repark pair:control-int | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark pair:past38 | 666 | 606 | 8 | 2 | 10 | 0 | 36 | 4 |
| repark pair:past38-int | 444 | 404 | 4 | 6 | 2 | 0 | 24 | 4 |
| repark revision | 124 | 94 | 8 | 2 | 0 | 0 | 20 | 0 |
| repark special | 40 | 20 | 0 | 0 | 0 | 0 | 20 | 0 |
| repark three | 48 | 48 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark values | 22 | 22 | 0 | 0 | 0 | 0 | 0 | 0 |

## By door

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| native native | 1814 | 352 | 31 | 558 | 40 | 0 | 833 | 0 |
| repark dataframe | 322 | 322 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark fexpr | 92 | 90 | 0 | 2 | 0 | 0 | 0 | 0 |
| repark sql | 2206 | 1937 | 55 | 18 | 28 | 0 | 156 | 12 |

## By pair (pair cells, Spark-door SQL)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 3022 | 1421 | 74 | 570 | 40 | 0 | 917 | 0 |
| ab | 182 | 162 | 4 | 0 | 4 | 0 | 12 | 0 |
| ac | 182 | 162 | 2 | 0 | 4 | 0 | 12 | 2 |
| ad | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| bc | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| c0c | 182 | 150 | 0 | 0 | 16 | 0 | 12 | 4 |
| ef | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| gh | 182 | 162 | 2 | 2 | 2 | 0 | 12 | 2 |
| i16w34 | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| i32c | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| i32w30 | 182 | 162 | 2 | 2 | 2 | 0 | 12 | 2 |
| i64c | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| i64h | 182 | 162 | 2 | 4 | 0 | 0 | 12 | 2 |
| i8w36 | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| k28k4 | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |

## By position (pair cells, Spark-door SQL)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 3022 | 1421 | 74 | 570 | 40 | 0 | 917 | 0 |
| array | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| array_contains | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| array_union | 24 | 0 | 12 | 0 | 0 | 0 | 0 | 12 |
| between | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| between_rev | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| case | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| case_simple | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| coalesce | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| eq | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| eq_rev | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| except | 24 | 20 | 0 | 0 | 4 | 0 | 0 | 0 |
| exists_corr | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| greatest | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| gt | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| if | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| in1 | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| in_rev | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| in_subquery | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| intersect | 24 | 20 | 0 | 0 | 4 | 0 | 0 | 0 |
| join_eq | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| join_full_using | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| join_nseq | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| lag | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| le | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| lead | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| least | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| lt | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| map | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| map_concat | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| ne | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| not_in | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| not_in_subquery | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| nseq | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| nullif | 24 | 20 | 0 | 0 | 4 | 0 | 0 | 0 |
| nvl | 24 | 0 | 0 | 8 | 16 | 0 | 0 | 0 |
| nvl2 | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| scalar_subquery | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| union | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| union3 | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| union_all | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| union_all_rev | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| where_between | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| where_eq | 56 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| where_eq_rev | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| where_in | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| where_lt | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |

## By position (pair cells, native door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 3022 | 2441 | 62 | 184 | 34 | 0 | 289 | 12 |
| array | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| array_contains | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| array_union | 24 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |
| between | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| between_rev | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| case | 56 | 16 | 0 | 18 | 0 | 0 | 22 | 0 |
| case_simple | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| coalesce | 56 | 16 | 0 | 36 | 0 | 0 | 4 | 0 |
| eq | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| eq_rev | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| except | 24 | 0 | 0 | 8 | 4 | 0 | 12 | 0 |
| exists_corr | 12 | 2 | 0 | 8 | 0 | 0 | 2 | 0 |
| greatest | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| gt | 24 | 6 | 0 | 6 | 0 | 0 | 12 | 0 |
| if | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| in1 | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| in_rev | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| in_subquery | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| intersect | 24 | 0 | 0 | 8 | 4 | 0 | 12 | 0 |
| join_eq | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| join_full_using | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| join_nseq | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| lag | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| le | 24 | 6 | 0 | 6 | 0 | 0 | 12 | 0 |
| lead | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| least | 24 | 4 | 0 | 6 | 2 | 0 | 12 | 0 |
| lt | 56 | 20 | 0 | 12 | 0 | 0 | 24 | 0 |
| map | 24 | 0 | 0 | 8 | 4 | 0 | 12 | 0 |
| map_concat | 24 | 0 | 0 | 0 | 0 | 0 | 24 | 0 |
| ne | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| not_in | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| not_in_subquery | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| nseq | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| nullif | 24 | 0 | 0 | 8 | 4 | 0 | 12 | 0 |
| nvl | 24 | 0 | 0 | 16 | 8 | 0 | 0 | 0 |
| nvl2 | 24 | 4 | 0 | 0 | 8 | 0 | 12 | 0 |
| scalar_subquery | 24 | 6 | 0 | 6 | 0 | 0 | 12 | 0 |
| union | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| union3 | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| union_all | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| union_all_rev | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| where_between | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| where_eq | 56 | 16 | 0 | 16 | 0 | 0 | 24 | 0 |
| where_eq_rev | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| where_in | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| where_lt | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |

## Every non-agreeing cell outside the pair grid

- `repark` ansi=false `agg/having_avg_max/t`: **wrong_answer**; RePark `{"rows": ["1|0.836666"], "types": ["int",`; Spark `{"rows": ["1|0.836667"], "types": ["int",`
- `repark` ansi=false `agg/sum_type/t`: **wrong_answer**; RePark `{"rows": ["0|2.00|1.000000", "1|2.51|0.836666"], "types": ["int", "decimal(38,2)",`; Spark `{"rows": ["0|2.00|1.000000", "1|2.51|0.836667"], "types": ["int", "decimal(38,2)",`
- `repark` ansi=false `literal/u_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["-2", "0", "1", "1", "2", "null", "null"], "types":`
- `repark` ansi=false `literal/where_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": [], "types":`
- `repark` ansi=false `literal/u_overflow39/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["-2", "0", "1", "1", "1", "2", "null"], "types":`
- `repark` ansi=false `literal/coalesce_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|1.500000000000000000000000000000", "3|-1.500000000000000000000000000000", "4|0.500000000000000000000000000000",`
- `repark` ansi=false `literal/u_struct/t`: **wrong_answer**; RePark `{"rows": ["{f->-2}", "{f->-2}", "{f->0}", "{f->0}", "{f->1}", "{f->1}", "{f->1}", "{f->1}", "{f->1}", "{f->2}", "{f->2}", "{f->null}"], "types":`; Spark `{"rows": ["[-2]", "[-2]", "[0]", "[0]", "[1]", "[1]", "[1]", "[1]", "[1]", "[2]", "[2]", "[null]"], "types":`
- `repark` ansi=false `special/w_range_a_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["1|3", "2|3", "3|1", "4|3", "5|1", "6|1"], "types": ["int",`
- `repark` ansi=false `special/w_range_c_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["1|3", "2|4", "3|1", "4|2", "5|1", "6|4"], "types": ["int",`
- `repark` ansi=false `special/w_range_h_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["1|2", "2|1", "3|1", "4|1", "5|2", "6|1"], "types": ["int",`
- `repark` ansi=false `special/pivot_a/t`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query {`; Spark `{"rows": ["0|1|1", "1|1|null"], "types": ["int", "bigint",`
- `repark` ansi=false `special/pivot_c/t`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query {`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint",`
- `repark` ansi=false `const/i64_eq_hlit/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Internal error: Assertion failed: (0.0..=1.0).contains(&selectivity): Selectivity is out of limit: inf.`; Spark `{"rows": [], "types":`
- `repark` ansi=false `literal/u_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["-99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "null"], "types":`
- `repark` ansi=false `literal/where_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": [], "types":`
- `repark` ansi=false `literal/u_overflow39/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["-100000000", "100000000", "null"], "types":`
- `repark` ansi=false `literal/coalesce_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-99999999.990000000000000000000000000000"], "types": ["int",`
- `repark` ansi=false `literal/u_struct/tx`: **wrong_answer**; RePark `{"rows": ["{f->-100000000}", "{f->-99999999999999999999999999999999999999}", "{f->100000000}", "{f->99999999999999999999999999999999999999}"], "types":`; Spark `{"rows": ["[-100000000]", "[-99999999999999999999999999999999999999]", "[100000000]", "[99999999999999999999999999999999999999]"], "types":`
- `repark` ansi=false `special/w_range_a_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["7|1", "8|1"], "types": ["int",`
- `repark` ansi=false `special/w_range_c_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["7|1", "8|1"], "types": ["int",`
- `repark` ansi=false `special/w_range_h_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value`; Spark `{"rows": ["7|1", "8|1"], "types": ["int",`
- `repark` ansi=false `special/pivot_a/tx`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query {`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint",`
- `repark` ansi=false `special/pivot_c/tx`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query {`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint",`
- `repark` ansi=false `dfconst/fexpr_a_eq_lit/t`: **wrong_answer**; RePark `{"rows": [], "types":`; Spark `{"rows": ["1", "4"], "types":`
- `repark` ansi=false `icedml/cow/update_set_coalesce`: **over_raise**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)",`; Spark `{"rows": ["1|1.00|o", "2|2.00|o", "3|-2.00|o", "4|1.00|o", "5|0.00|o", "6|null|o"], "types": ["int", "decimal(38,2)",`
- `repark` ansi=false `icedml/mor/update_set_coalesce`: **over_raise**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)",`; Spark `{"rows": ["1|1.00|o", "2|2.00|o", "3|-2.00|o", "4|1.00|o", "5|0.00|o", "6|null|o"], "types": ["int", "decimal(38,2)",`
- `repark` ansi=false `legacy/pair/ab/t/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["1|null", "2|null", "3|null", "4|null", "5|false", "6|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/ab/t/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["-0.50000000000000000000000000000000000000", "0.00000000000000000000000000000000000000", "0.12345678901234567890123456789012345678", "0.49999999999999999999999999999999999999",`
- `repark` ansi=false `legacy/pair/bc/t/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["-0.50000000000000000000000000000000000000", "0.12345678901234567890123456789012345678", "0.49000000000000000000000000000000000000", "0.49999999999999999999999999999999999999",`
- `repark` ansi=false `legacy/pair/bc/t/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["1|null", "2|null", "3|null", "4|false", "5|false", "6|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/bc/t/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["1|0.50000000000000000000000000000000000000", "2|-0.50000000000000000000000000000000000000", "3|0.49999999999999999999999999999999999999", "4|0.50000000000000000000000000000000000000",`
- `repark` ansi=false `legacy/pair/ab/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38).`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/ab/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38).`; Spark `{"rows": ["-0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/ac/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2).`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/ac/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2).`; Spark `{"rows": ["-99999999.99", "99999999.99", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/ad/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1).`; Spark `{"rows": ["-0.5", "9999999999999999999999999999999999999.9", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/ad/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1).`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/bc/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38).`; Spark `{"rows": ["-0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/bc/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38).`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/bc/tx/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38).`; Spark `{"rows": ["7|0.99999999999999999999999999999999999999", "8|-0.99999999999999999999999999999999999999"], "types": ["int",`
- `repark` ansi=false `legacy/pair/ef/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20).`; Spark `{"rows": ["-0.50000000000000000000", "0.000000000000000000490", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/ef/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20).`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/gh/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20).`; Spark `{"rows": ["7|null", "8|false"], "types": ["int",`
- `repark` ansi=false `legacy/pair/gh/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20).`; Spark `{"rows": ["-1.00000000000000000000", "0.000000000000000000500", "999999999999999999.99999999999999999999", "null"], "types":`
- `repark` ansi=false `legacy/pair/i64h/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/i64h/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["0.000000000000000000500", "999999999999999999.99999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/i32w30/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/i32w30/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["1.000000000000000000000000000000", "99999999.999999999999999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/i16w34/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["1.0000000000000000000000000000000000", "9999.9999999999999999999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/i16w34/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on:`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/pair/i8w36/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 127.000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["1.000000000000000000000000000000000000", "99.999999999999999999999999999999999999", "null", "null"], "types":`
- `repark` ansi=false `legacy/pair/i8w36/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 127.000000000000000000000000000000000000 is too large to store in a Decimal128 of precision`; Spark `{"rows": ["7|null", "8|null"], "types": ["int",`
- `repark` ansi=false `legacy/extra/v_bc`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["0.50000000000000000000000000000000000000", "null"], "types":`
- `repark` ansi=false `legacy/extra/u_const_ab`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument`; Spark `{"rows": ["0.50000000000000000000000000000000000000", "null"], "types":`
- `repark` ansi=false `rev/m1/map_concat_keys_apart`: **over_raise**; RePark `"ERR:PySparkException:None:Execution error: [NULL_MAP_KEY] Cannot use null as map`; Spark `{"rows": ["1|{1->x,6->y}", "2|{2->x,7->y}", "3|{-2->x,4->y}", "4|{1->x,6->y}", "5|{0->x,5->y}"], "types": ["int",`
- `repark` ansi=false `rev/m1/create_map_keys_apart`: **over_raise**; RePark `"ERR:PySparkException:None:Execution error: [NULL_MAP_KEY] Cannot use null as map`; Spark `{"rows": ["1|{1->x,6->y}", "2|{2->x,7->y}", "3|{-2->x,4->y}", "4|{1->x,6->y}", "5|{0->x,5->y}"], "types": ["int",`
- `repark` ansi=false `rev/m1/map_keys_of_concat`: **over_raise**; RePark `"ERR:PySparkException:None:Execution error: [NULL_MAP_KEY] Cannot use null as map`; Spark `{"rows": ["1|[1,6]", "2|[2,7]", "3|[-2,4]", "4|[1,6]", "5|[0,5]"], "types": ["int",`
- `repark` ansi=false `rev/m2/using_inner`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2|3|3", "0|5|5", "1|1|1", "1|1|4", "1|1|6", "1|4|1", "1|4|4", "1|4|6", "2|2|2"], "types": ["decimal(38,0)", "int",`
- `repark` ansi=false `rev/m2/using_left`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2|3|3", "0|5|5", "1|1|1", "1|1|4", "1|1|6", "1|4|1", "1|4|4", "1|4|6", "2|2|2", "null|6|null"], "types": ["decimal(38,0)", "int",`
- `repark` ansi=false `rev/m2/using_full_distinct_names`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2|3|3", "0|5|5", "1|1|1", "1|1|4", "1|1|6", "1|4|1", "1|4|4", "1|4|6", "2|2|2", "null|6|null"], "types": ["decimal(38,0)", "int",`
- `repark` ansi=false `rev/m2/using_inner_dup_names`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2", "0", "1", "1", "1", "1", "1", "1", "2"], "types":`
- `repark` ansi=false `rev/m2/using_full_dup_names`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2", "0", "1", "1", "1", "1", "1", "1", "2", "null"], "types":`
- `repark` ansi=false `rev/m2/using_full_dup_names_same_type`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["-2", "0", "1", "1", "1", "1", "2", "null", "null"], "types":`
- `repark` ansi=false `rev/m2/using_inner_h_i64`: **over_raise**; RePark `"ERR:AnalysisException:None:Schema error: Schema contains duplicate unqualified`; Spark `{"rows": ["1|1|5", "1|5|5"], "types": ["bigint", "int",`
- `repark` ansi=false `rev/pin/struct_pos`: **wrong_answer**; RePark `{"rows": ["{f->1,g->1}", "{f->2,g->2}"], "types":`; Spark `{"rows": ["[1,1]", "[2,2]"], "types":`

