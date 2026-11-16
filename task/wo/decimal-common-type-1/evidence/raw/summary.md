# Summary (written by analyze.py)

## By engine and ANSI

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| native ansi=false | 907 | 176 | 12 | 279 | 20 | 0 | 420 | 0 |
| native ansi=true | 907 | 176 | 19 | 279 | 20 | 0 | 413 | 0 |
| repark ansi=false | 1248 | 283 | 6 | 379 | 18 | 38 | 518 | 6 |
| repark ansi=true | 1248 | 282 | 41 | 379 | 18 | 38 | 484 | 6 |

## By cause (RePark Spark door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (native) | 1814 | 352 | 31 | 558 | 40 | 0 | 833 | 0 |
| arithmetic-control ansi=false | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| arithmetic-control ansi=true | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| avg-round ansi=false | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| avg-round ansi=true | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| common-type ansi=false | 874 | 49 | 0 | 356 | 7 | 38 | 424 | 0 |
| common-type ansi=true | 874 | 49 | 6 | 356 | 7 | 38 | 418 | 0 |
| confounded:array_union ansi=false | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:array_union ansi=true | 10 | 0 | 6 | 0 | 0 | 0 | 0 | 4 |
| confounded:except ansi=false | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:except ansi=true | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:intersect ansi=false | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:intersect ansi=true | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
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
| control:array_union ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:array_union ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| control:core ansi=false | 161 | 161 | 0 | 0 | 0 | 0 | 0 | 0 |
| control:core ansi=true | 161 | 161 | 0 | 0 | 0 | 0 | 0 | 0 |
| control:except ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:except ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:intersect ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:intersect ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
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
| legacy ansi=false | 94 | 64 | 0 | 2 | 0 | 0 | 28 | 0 |
| legacy ansi=true | 94 | 64 | 28 | 2 | 0 | 0 | 0 | 0 |
| pivot-unsupported ansi=false | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| pivot-unsupported ansi=true | 4 | 0 | 0 | 0 | 0 | 0 | 4 | 0 |
| range-offset ansi=false | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| range-offset ansi=true | 10 | 4 | 0 | 0 | 0 | 0 | 6 | 0 |
| update-cse ansi=false | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| update-cse ansi=true | 2 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |

## By cause (native door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (repark) | 2496 | 565 | 47 | 758 | 36 | 76 | 1002 | 12 |
| arithmetic-control ansi=false | 2 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| arithmetic-control ansi=true | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| avg-round ansi=false | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| avg-round ansi=true | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| common-type ansi=false | 655 | 43 | 0 | 258 | 7 | 0 | 347 | 0 |
| common-type ansi=true | 655 | 43 | 6 | 258 | 7 | 0 | 341 | 0 |
| confounded:array_union ansi=false | 10 | 0 | 10 | 0 | 0 | 0 | 0 | 0 |
| confounded:array_union ansi=true | 10 | 0 | 10 | 0 | 0 | 0 | 0 | 0 |
| confounded:except ansi=false | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:except ansi=true | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:intersect ansi=false | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
| confounded:intersect ansi=true | 10 | 0 | 0 | 4 | 0 | 0 | 6 | 0 |
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
| control:core ansi=false | 135 | 125 | 0 | 0 | 2 | 0 | 8 | 0 |
| control:core ansi=true | 135 | 125 | 0 | 0 | 2 | 0 | 8 | 0 |
| control:except ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:except ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:intersect ansi=false | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| control:intersect ansi=true | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
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
| (other) | 2562 | 819 | 72 | 604 | 62 | 0 | 993 | 12 |
| agg ansi=false | 12 | 3 | 0 | 6 | 2 | 0 | 1 | 0 |
| agg ansi=true | 12 | 3 | 0 | 6 | 2 | 0 | 1 | 0 |
| const ansi=false | 112 | 34 | 0 | 46 | 0 | 0 | 32 | 0 |
| const ansi=true | 112 | 34 | 0 | 46 | 0 | 0 | 32 | 0 |
| df:past38 ansi=false | 80 | 0 | 0 | 25 | 0 | 0 | 55 | 0 |
| df:past38 ansi=true | 80 | 0 | 0 | 25 | 0 | 0 | 55 | 0 |
| df:past38-int ansi=false | 40 | 0 | 0 | 22 | 0 | 0 | 18 | 0 |
| df:past38-int ansi=true | 40 | 0 | 0 | 22 | 0 | 0 | 18 | 0 |
| dfconst ansi=false | 20 | 6 | 0 | 10 | 0 | 0 | 4 | 0 |
| dfconst ansi=true | 20 | 6 | 0 | 10 | 0 | 0 | 4 | 0 |
| fexpr:past38 ansi=false | 24 | 0 | 0 | 10 | 0 | 0 | 14 | 0 |
| fexpr:past38 ansi=true | 24 | 0 | 0 | 10 | 0 | 0 | 14 | 0 |
| fexpr:past38-int ansi=false | 12 | 0 | 0 | 8 | 0 | 0 | 4 | 0 |
| fexpr:past38-int ansi=true | 12 | 0 | 0 | 8 | 0 | 0 | 4 | 0 |
| iceberg ansi=false | 12 | 0 | 0 | 0 | 0 | 12 | 0 | 0 |
| iceberg ansi=true | 12 | 0 | 0 | 0 | 0 | 12 | 0 | 0 |
| icedml ansi=false | 22 | 0 | 0 | 0 | 0 | 22 | 0 | 0 |
| icedml ansi=true | 22 | 0 | 0 | 0 | 0 | 22 | 0 | 0 |
| icepart ansi=false | 9 | 0 | 0 | 5 | 0 | 4 | 0 | 0 |
| icepart ansi=true | 9 | 0 | 0 | 5 | 0 | 4 | 0 | 0 |
| literal ansi=false | 18 | 0 | 0 | 5 | 0 | 0 | 13 | 0 |
| literal ansi=true | 18 | 0 | 6 | 5 | 0 | 0 | 7 | 0 |
| pair:past38 ansi=false | 285 | 2 | 0 | 100 | 2 | 0 | 181 | 0 |
| pair:past38 ansi=true | 285 | 2 | 0 | 100 | 2 | 0 | 181 | 0 |
| pair:past38-int ansi=false | 190 | 3 | 0 | 94 | 3 | 0 | 90 | 0 |
| pair:past38-int ansi=true | 190 | 3 | 0 | 94 | 3 | 0 | 90 | 0 |
| special ansi=false | 4 | 0 | 0 | 3 | 0 | 0 | 1 | 0 |
| special ansi=true | 4 | 0 | 0 | 3 | 0 | 0 | 1 | 0 |
| three ansi=false | 24 | 0 | 0 | 14 | 0 | 0 | 10 | 0 |
| three ansi=true | 24 | 0 | 0 | 14 | 0 | 0 | 10 | 0 |
| values ansi=false | 10 | 1 | 0 | 8 | 0 | 0 | 1 | 0 |
| values ansi=true | 10 | 1 | 0 | 8 | 0 | 0 | 1 | 0 |

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
| repark agg | 28 | 8 | 0 | 14 | 4 | 0 | 2 | 0 |
| repark const | 224 | 68 | 0 | 92 | 0 | 0 | 64 | 0 |
| repark df:control | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark df:past38 | 160 | 0 | 0 | 50 | 0 | 0 | 110 | 0 |
| repark df:past38-int | 80 | 0 | 0 | 44 | 0 | 0 | 36 | 0 |
| repark dfconst | 40 | 12 | 0 | 20 | 0 | 0 | 8 | 0 |
| repark fexpr:control | 12 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark fexpr:past38 | 48 | 0 | 0 | 20 | 0 | 0 | 28 | 0 |
| repark fexpr:past38-int | 24 | 0 | 0 | 16 | 0 | 0 | 8 | 0 |
| repark iceberg | 24 | 0 | 0 | 0 | 0 | 24 | 0 | 0 |
| repark icedml | 48 | 0 | 0 | 0 | 0 | 44 | 4 | 0 |
| repark icepart | 18 | 0 | 0 | 10 | 0 | 8 | 0 | 0 |
| repark legacy | 20 | 12 | 2 | 4 | 0 | 0 | 2 | 0 |
| repark legacy:control | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark legacy:control-int | 24 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark legacy:past38 | 72 | 36 | 18 | 0 | 0 | 0 | 18 | 0 |
| repark legacy:past38-int | 48 | 32 | 8 | 0 | 0 | 0 | 8 | 0 |
| repark literal | 40 | 3 | 7 | 10 | 0 | 0 | 20 | 0 |
| repark pair:control | 222 | 190 | 0 | 0 | 16 | 0 | 12 | 4 |
| repark pair:control-int | 80 | 80 | 0 | 0 | 0 | 0 | 0 | 0 |
| repark pair:past38 | 666 | 4 | 8 | 222 | 6 | 0 | 422 | 4 |
| repark pair:past38-int | 444 | 6 | 4 | 206 | 8 | 0 | 216 | 4 |
| repark special | 40 | 12 | 0 | 6 | 0 | 0 | 22 | 0 |
| repark three | 48 | 0 | 0 | 28 | 0 | 0 | 20 | 0 |
| repark values | 22 | 2 | 0 | 16 | 2 | 0 | 2 | 0 |

## By door

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| native native | 1814 | 352 | 31 | 558 | 40 | 0 | 833 | 0 |
| repark dataframe | 312 | 48 | 0 | 110 | 0 | 0 | 154 | 0 |
| repark fexpr | 92 | 16 | 0 | 40 | 0 | 0 | 36 | 0 |
| repark sql | 2092 | 501 | 47 | 608 | 36 | 76 | 812 | 12 |

## By pair (pair cells, Spark-door SQL)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 2898 | 637 | 66 | 888 | 46 | 76 | 1185 | 0 |
| ab | 182 | 0 | 4 | 6 | 2 | 0 | 170 | 0 |
| ac | 182 | 0 | 2 | 86 | 2 | 0 | 90 | 2 |
| ad | 40 | 2 | 0 | 20 | 0 | 0 | 18 | 0 |
| bc | 40 | 0 | 0 | 0 | 0 | 0 | 40 | 0 |
| c0c | 182 | 150 | 0 | 0 | 16 | 0 | 12 | 4 |
| ef | 40 | 2 | 0 | 20 | 0 | 0 | 18 | 0 |
| gh | 182 | 0 | 2 | 90 | 2 | 0 | 86 | 2 |
| i16w34 | 40 | 0 | 0 | 22 | 0 | 0 | 18 | 0 |
| i32c | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| i32w30 | 182 | 0 | 2 | 84 | 4 | 0 | 90 | 2 |
| i64c | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |
| i64h | 182 | 6 | 2 | 78 | 4 | 0 | 90 | 2 |
| i8w36 | 40 | 0 | 0 | 22 | 0 | 0 | 18 | 0 |
| k28k4 | 40 | 40 | 0 | 0 | 0 | 0 | 0 | 0 |

## By position (pair cells, Spark-door SQL)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 2898 | 637 | 66 | 888 | 46 | 76 | 1185 | 0 |
| array | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| array_contains | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| array_union | 24 | 0 | 12 | 0 | 0 | 0 | 0 | 12 |
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
| if | 24 | 4 | 0 | 10 | 0 | 0 | 10 | 0 |
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
| map | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
| map_concat | 24 | 4 | 0 | 8 | 0 | 0 | 12 | 0 |
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

## By position (pair cells, native door)

| key | cells | agree | both_raise | wrong_answer | wrong_type | wrong_write | over_raise | missed_raise |
|---|---|---|---|---|---|---|---|---|
| (other) | 2898 | 657 | 54 | 922 | 42 | 76 | 1135 | 12 |
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

- `repark` ansi=false `const/a_eq_c125/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_c125/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/c125_eq_a/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c125_eq_a/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/c_eq_a1/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_eq_a1/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/a1_eq_c/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a1_eq_c/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/c_lt_a1/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|true", "4|true", "5|true", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|true", "4|false", "5|true", "6|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_lt_a1/t/where`: **wrong_answer**; RePark `{"rows": ["3", "4", "5"], "types": ["int"]}`; Spark `{"rows": ["3", "5"], "types": ["int"]}`
- `repark` ansi=false `const/c_ge_a1/t/select`: **wrong_answer**; RePark `{"rows": ["1|true", "2|true", "3|false", "4|false", "5|false", "6|true"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_ge_a1/t/where`: **wrong_answer**; RePark `{"rows": ["1", "2", "6"], "types": ["int"]}`; Spark `{"rows": ["1", "2", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/c_nseq_a1/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_nseq_a1/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/c_in_a12/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_in_a12/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "2", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/c_notin_a12/t/select`: **wrong_answer**; RePark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|true"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|true", "4|false", "5|true", "6|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_notin_a12/t/where`: **wrong_answer**; RePark `{"rows": ["1", "2", "3", "4", "5", "6"], "types": ["int"]}`; Spark `{"rows": ["3", "5"], "types": ["int"]}`
- `repark` ansi=false `const/c_between_a1/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c_between_a1/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/a1_between_c/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a1_between_c/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `const/a_in_c/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_in_c/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/b_eq_a1/t/select`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/b_eq_a1/t/where`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_b05/t/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_b05/t/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_lit125/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_lit125/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/lit125_eq_a/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/lit125_eq_a/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_lit15/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_lit15/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["2"], "types": ["int"]}`
- `repark` ansi=false `const/a_gt_lit05/t/select`: **wrong_answer**; RePark `{"rows": ["1|true", "2|true", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_gt_lit05/t/where`: **wrong_answer**; RePark `{"rows": ["1", "2", "4"], "types": ["int"]}`; Spark `{"rows": ["2"], "types": ["int"]}`
- `repark` ansi=false `const/a_in_lit/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_in_lit/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `const/d_eq_lit145/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|false", "5|false", "6|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/d_eq_lit145/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1"], "types": ["int"]}`
- `repark` ansi=false `const/h_eq_big1/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/h_eq_big1/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["5"], "types": ["int"]}`
- `repark` ansi=false `const/i64_eq_hlit/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/i64_eq_hlit/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "5"], "types": ["int"]}`
- `repark` ansi=false `const/i32_eq_w30lit/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/i32_eq_w30lit/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "5"], "types": ["int"]}`
- `repark` ansi=false `const/gg_eq_hlit/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/gg_eq_hlit/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1"], "types": ["int"]}`
- `repark` ansi=false `const/h_eq_glit/t/select`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/h_eq_glit/t/where`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1"], "types": ["int"]}`
- `repark` ansi=false `three/u3_flat/t`: **wrong_answer**; RePark `{"rows": ["-0.50", "-1.50", "-2.00", "-2.50", "0.00", "0.49", "0.50", "0.50", "1.00", "1.00", "1.25", "1.40", "1.45", "1.50", "1.50", "2.00", "2.50", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-1", "-2", "-2", "-3", "0", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "3", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_left/t`: **wrong_answer**; RePark `{"rows": ["-0.50", "-1.50", "-2.00", "-2.50", "0.00", "0.49", "0.50", "0.50", "1.00", "1.00", "1.25", "1.40", "1.45", "1.50", "1.50", "2.00", "2.50", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-1", "-2", "-2", "-3", "0", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "3", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_right/t`: **wrong_answer**; RePark `{"rows": ["-0.50", "-1.50", "-2.00", "-2.50", "0.00", "0.49", "0.50", "0.50", "1.00", "1.00", "1.25", "1.40", "1.45", "1.50", "1.50", "2.00", "2.50", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-1", "-2", "-2", "-3", "0", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "3", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_sub/t`: **wrong_answer**; RePark `{"rows": ["-0.50", "-1.50", "-2.00", "-2.50", "0.00", "0.49", "0.50", "0.50", "1.00", "1.00", "1.25", "1.40", "1.45", "1.50", "1.50", "2.00", "2.50", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-1", "-2", "-2", "-3", "0", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "3", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/coalesce3/t`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|1.50", "3|-1.50", "4|0.50", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/coalesce_nested/t`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|1.50", "3|-1.50", "4|0.50", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|1", "6|2"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/greatest3/t`: **wrong_answer**; RePark `{"rows": ["1|1.50", "2|2.50", "3|-1.50", "4|1.00", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|2", "2|3", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/least3/t`: **wrong_answer**; RePark `{"rows": ["1|1.00", "2|1.50", "3|-2.50", "4|0.50", "5|-0.50", "6|1.40"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-3", "4|1", "5|-1", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/in3/t`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `three/case3/t`: **wrong_answer**; RePark `{"rows": ["1|1.50", "2|2.00", "3|-1.50", "4|0.50", "5|0.00", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|2", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/array3/t`: **wrong_answer**; RePark `{"rows": ["1|[1.25,1.50,1.00]", "2|[1.50,2.50,2.00]", "3|[-1.50,-2.50,-2.00]", "4|[0.50,0.50,1.00]", "5|[0.49,-0.50,0.00]", "6|[1.45,1.40,null]"], "types": ["int", "array<decimal(38,2)>"]}`; Spark `{"rows": ["1|[1,2,1]", "2|[2,3,2]", "3|[-2,-3,-2]", "4|[1,1,1]", "5|[0,-1,0]", "6|[1,1,null]"], "types": ["int", "array<decimal(38,0)>"]}`
- `repark` ansi=false `three/u3_int/t`: **wrong_answer**; RePark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "-1.50000000000000000000", "0.00000000000000000000", "0.00000000004999999999", "0.49000000000000000000", "0.50000000000000000000", "1.00`; Spark `{"rows": ["-1.000000000000000000", "-1.000000000050000000", "-1.500000000000000000", "0.000000000000000000", "0.000000000050000000", "0.490000000000000000", "0.500000000000000000", "1.0000000000000000`
- `repark` ansi=false `agg/having_sum_const/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|2.51"], "types": ["int", "decimal(38,2)"]}`
- `repark` ansi=false `agg/having_sum_max/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["0|2.00", "1|2.51"], "types": ["int", "decimal(38,2)"]}`
- `repark` ansi=false `agg/having_avg_max/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int", "decimal(32,6)"]}`; Spark `{"rows": ["1|0.836667"], "types": ["int", "decimal(32,6)"]}`
- `repark` ansi=false `agg/sum_eq_select/t`: **wrong_answer**; RePark `{"rows": ["0|false", "1|false"], "types": ["int", "boolean"]}`; Spark `{"rows": ["0|true", "1|true"], "types": ["int", "boolean"]}`
- `repark` ansi=false `agg/sum_union/t`: **wrong_answer**; RePark `{"rows": ["0.0000", "0.0001", "1.0000", "1.9950", "2.0000", "2.5050", "2.5100", "null"], "types": ["decimal(38,4)"]}`; Spark `{"rows": ["0.00", "0.00", "1.00", "2.00", "2.00", "2.51", "2.51", "null"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `agg/sum_coalesce/t`: **wrong_type**; RePark `{"rows": ["0|2.0000", "1|2.5100"], "types": ["int", "decimal(38,4)"]}`; Spark `{"rows": ["0|2.00", "1|2.51"], "types": ["int", "decimal(38,2)"]}`
- `repark` ansi=false `agg/sum_type/t`: **wrong_answer**; RePark `{"rows": ["0|2.00|1.000000", "1|2.51|0.836666"], "types": ["int", "decimal(38,2)", "decimal(32,6)"]}`; Spark `{"rows": ["0|2.00|1.000000", "1|2.51|0.836667"], "types": ["int", "decimal(38,2)", "decimal(32,6)"]}`
- `repark` ansi=false `literal/u_const_ab/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_const_ac/t`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_col_const/t`: **wrong_answer**; RePark `{"rows": ["-2.00", "0.00", "1.00", "1.00", "1.25", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "0", "1", "1", "1", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": ["-2", "0", "1", "1", "2", "null", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/where_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `literal/u_overflow39/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 12345678901234567"`; Spark `{"rows": ["-2", "0", "1", "1", "1", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/coalesce_overflow_lit/t`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|1.500000000000000000000000000000", "3|-1.500000000000000000000000000000", "4|0.500000000000000000000000000000", "5|0.490000000000000000000000000000",`
- `repark` ansi=false `literal/u_null/t`: **wrong_answer**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_struct/t`: **wrong_answer**; RePark `{"rows": ["{f->-1.50}", "{f->-2.00}", "{f->0.00}", "{f->0.49}", "{f->0.50}", "{f->1.00}", "{f->1.00}", "{f->1.25}", "{f->1.45}", "{f->1.50}", "{f->2.00}", "{f->null}"], "types": ["struct<f:decimal(38,`; Spark `{"rows": ["[-2]", "[-2]", "[0]", "[0]", "[1]", "[1]", "[1]", "[1]", "[1]", "[2]", "[2]", "[null]"], "types": ["struct<f:decimal(38,0)>"]}`
- `repark` ansi=false `special/w_range_a_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["1|3", "2|3", "3|1", "4|3", "5|1", "6|1"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/w_range_c_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["1|3", "2|4", "3|1", "4|2", "5|1", "6|4"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/w_range_h_dec/t`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["1|2", "2|1", "3|1", "4|1", "5|2", "6|1"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/pivot_a/t`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query { with: No"`; Spark `{"rows": ["0|1|1", "1|1|null"], "types": ["int", "bigint", "bigint"]}`
- `repark` ansi=false `special/pivot_c/t`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query { with: No"`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint", "bigint"]}`
- `repark` ansi=false `special/gsets_coalesce/t`: **wrong_answer**; RePark `{"rows": ["-2.00|1", "0.00|1", "1.00|2", "1.45|1", "2.00|1", "null|6"], "types": ["decimal(38,2)", "bigint"]}`; Spark `{"rows": ["-2|1", "0|1", "1|3", "2|1", "null|6"], "types": ["decimal(38,0)", "bigint"]}`
- `repark` ansi=false `special/rollup_ac/t`: **wrong_answer**; RePark `{"rows": ["-1.50|1", "0.49|1", "0.50|1", "1.25|1", "1.45|1", "1.50|1", "null|6"], "types": ["decimal(38,2)", "bigint"]}`; Spark `{"rows": ["-2|1", "0|1", "1|3", "2|1", "null|6"], "types": ["decimal(38,0)", "bigint"]}`
- `repark` ansi=false `const/a_eq_c125/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_c125/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/c125_eq_a/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/c125_eq_a/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/a_in_c/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_in_c/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/b_eq_a1/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["7|true", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/b_eq_a1/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["7"], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_b05/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_b05/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_lit125/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_lit125/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/lit125_eq_a/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/lit125_eq_a/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/a_eq_lit15/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_eq_lit15/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/a_gt_lit05/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|true", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_gt_lit05/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7"], "types": ["int"]}`
- `repark` ansi=false `const/a_in_lit/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/a_in_lit/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/d_eq_lit145/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/d_eq_lit145/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/i64_eq_hlit/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/i64_eq_hlit/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/i32_eq_w30lit/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/i32_eq_w30lit/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `const/gg_eq_hlit/tx/select`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `const/gg_eq_hlit/tx/where`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `three/u3_flat/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-100000000", "-99999999999999999999999999999999999999", "100000000", "10000000000000000000000000000000000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_left/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-100000000", "-99999999999999999999999999999999999999", "100000000", "10000000000000000000000000000000000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_right/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-100000000", "-99999999999999999999999999999999999999", "100000000", "10000000000000000000000000000000000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/u3_sub/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-100000000", "-99999999999999999999999999999999999999", "100000000", "10000000000000000000000000000000000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `three/coalesce3/tx`: **wrong_answer**; RePark `{"rows": ["7|99999999.99", "8|-99999999.99"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["7|100000000", "8|-100000000"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/coalesce_nested/tx`: **wrong_answer**; RePark `{"rows": ["7|99999999.99", "8|-99999999.99"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["7|100000000", "8|-100000000"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/greatest3/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|99999999999999999999999999999999999999", "8|-1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/least3/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|100000000", "8|-99999999999999999999999999999999999999"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/in3/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `three/case3/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|10000000000000000000000000000000000000", "8|-99999999999999999999999999999999999999"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `three/array3/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|[100000000,10000000000000000000000000000000000000,99999999999999999999999999999999999999]", "8|[-100000000,-1,-99999999999999999999999999999999999999]"], "types": ["int", "array<decimal(3`
- `repark` ansi=false `three/u3_int/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["-9223372036854775808.000000000000000000", "-99999999.990000000000000000", "0.000000000000000001", "1000000000000000000.000000000000000000", "9223372036854775807.000000000000000000", "999999`
- `repark` ansi=false `agg/having_avg_max/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 6). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int", "decimal(32,6)"]}`
- `repark` ansi=false `agg/sum_union/tx`: **wrong_answer**; RePark `{"rows": ["0.0005", "1.0000", "999999.9999", "99999999999999999999999999.9900"], "types": ["decimal(38,4)"]}`; Spark `{"rows": ["0.00", "1.00", "1000000.00", "99999999999999999999999999.99"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `agg/sum_coalesce/tx`: **wrong_type**; RePark `{"rows": ["0|1.0000", "1|99999999999999999999999999.9900"], "types": ["int", "decimal(38,4)"]}`; Spark `{"rows": ["0|1.00", "1|99999999999999999999999999.99"], "types": ["int", "decimal(38,2)"]}`
- `repark` ansi=false `literal/u_const_ab/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_const_ac/tx`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_col_const/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-99999999999999999999999999999999999999", "1", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": ["-99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/where_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `literal/u_overflow39/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 12345678901234567"`; Spark `{"rows": ["-100000000", "100000000", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/coalesce_overflow_lit/tx`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.500000000000000"`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-99999999.990000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`
- `repark` ansi=false `literal/u_null/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-100000000", "-99999999999999999999999999999999999999", "100000000", "99999999999999999999999999999999999999", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `literal/u_struct/tx`: **over_raise**; RePark `"ERR:PySparkException:None:While casting struct field 'f' caused by Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 9999999999999999999"`; Spark `{"rows": ["[-100000000]", "[-99999999999999999999999999999999999999]", "[100000000]", "[99999999999999999999999999999999999999]"], "types": ["struct<f:decimal(38,0)>"]}`
- `repark` ansi=false `special/w_range_a_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["7|1", "8|1"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/w_range_c_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["7|1", "8|1"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/w_range_h_dec/tx`: **over_raise**; RePark `"ERR:AnalysisException:None:Error during planning: Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"`; Spark `{"rows": ["7|1", "8|1"], "types": ["int", "bigint"]}`
- `repark` ansi=false `special/pivot_a/tx`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query { with: No"`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint", "bigint"]}`
- `repark` ansi=false `special/pivot_c/tx`: **over_raise**; RePark `"ERR:UnsupportedOperationException:None:This feature is not implemented: Unsupported ast node Pivot { table: Derived { lateral: false, subquery: Query { with: No"`; Spark `{"rows": ["0|null|null", "1|null|null"], "types": ["int", "bigint", "bigint"]}`
- `repark` ansi=false `special/gsets_coalesce/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-99999999999999999999999999999999999999|1", "99999999999999999999999999999999999999|1", "null|2"], "types": ["decimal(38,0)", "bigint"]}`
- `repark` ansi=false `special/rollup_ac/tx`: **wrong_answer**; RePark `{"rows": ["-99999999.99|1", "99999999.99|1", "null|2"], "types": ["decimal(38,2)", "bigint"]}`; Spark `{"rows": ["-100000000|1", "100000000|1", "null|2"], "types": ["decimal(38,0)", "bigint"]}`
- `repark` ansi=false `values/v_ac`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_ca`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_int_c`: **wrong_type**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(22,2)"]}`; Spark `{"rows": ["1.00", "1.25"], "types": ["decimal(12,2)"]}`
- `repark` ansi=false `values/v_big_h`: **wrong_answer**; RePark `{"rows": ["1.00000000000000000000", "1.00000000000000000049"], "types": ["decimal(38,20)"]}`; Spark `{"rows": ["1.000000000000000000", "1.000000000000000000"], "types": ["decimal(38,18)"]}`
- `repark` ansi=false `values/v_bc`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.250000000000000"`; Spark `{"rows": ["0.500000000000000000000000000000", "1.250000000000000000000000000000"], "types": ["decimal(38,30)"]}`
- `repark` ansi=false `values/v_three`: **wrong_answer**; RePark `{"rows": ["1.00", "1.45", "1.50"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1", "2"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_null`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_lit`: **wrong_answer**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1", "1"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_union`: **wrong_answer**; RePark `{"rows": ["-2.00", "0.00", "1.00", "1.00", "1.25", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "0", "1", "1", "1", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `values/v_two_col`: **wrong_answer**; RePark `{"rows": ["1.00|1", "1.25|2"], "types": ["decimal(38,2)", "int"]}`; Spark `{"rows": ["1|1", "1|2"], "types": ["decimal(38,0)", "int"]}`
- `repark` ansi=false `df/ac/t/union`: **wrong_answer**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/unionByName`: **wrong_answer**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/when`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|2.00", "3|-1.50", "4|1.00", "5|0.49", "6|null"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|null"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/isin`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/t/between`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/t/filter_eq`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "2", "3", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/ac/t/select_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/t/coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|1.50", "3|-1.50", "4|0.50", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/greatest`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|2.00", "3|-1.50", "4|1.00", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/join`: **wrong_answer**; RePark `{"rows": [], "types": ["int", "int"]}`; Spark `{"rows": ["1|1", "1|4", "1|6", "2|2", "3|3", "4|1", "4|4", "4|6", "5|5"], "types": ["int", "int"]}`
- `repark` ansi=false `df/ac/t/fexpr_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/t/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.25", "2|1.50", "3|-1.50", "4|0.50", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/t/fexpr_filter`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "2", "3", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/ab/t/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-1", "-2", "0", "0", "0", "1", "1", "1", "1", "2", "null", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-1", "-2", "0", "0", "0", "1", "1", "1", "1", "2", "null", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 2"`; Spark `{"rows": ["1|1", "2|2", "3|0", "4|1", "5|0", "6|null"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/t/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/t/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/ab/t/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/t/coalesce`: **wrong_answer**; RePark `{"rows": ["1|0.50000000000000000000000000000000000000", "2|-0.50000000000000000000000000000000000000", "3|0.49999999999999999999999999999999999999", "4|0.50000000000000000000000000000000000001", "5|0.`; Spark `{"rows": ["1|1", "2|-1", "3|0", "4|1", "5|0", "6|null"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1", "2|2", "3|0", "4|1", "5|0", "6|null"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1", "1|4", "4|1", "4|4", "5|3", "5|5"], "types": ["int", "int"]}`
- `repark` ansi=false `df/ab/t/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/t/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["1|0.50000000000000000000000000000000000000", "2|-0.50000000000000000000000000000000000000", "3|0.49999999999999999999999999999999999999", "4|0.50000000000000000000000000000000000001", "5|0.`; Spark `{"rows": ["1|1", "2|-1", "3|0", "4|1", "5|0", "6|null"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/t/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/bc/t/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-0.500000000000000000000000000000", "-1.500000000000000000000000000000", "0.123456789012345678901234567890", "0.490000000000000000000000000000", "0.500000000000000000000000000000", "0.50000`
- `repark` ansi=false `df/bc/t/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-0.500000000000000000000000000000", "-1.500000000000000000000000000000", "0.123456789012345678901234567890", "0.490000000000000000000000000000", "0.500000000000000000000000000000", "0.50000`
- `repark` ansi=false `df/bc/t/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|-0.500000000000000000000000000000", "3|-1.500000000000000000000000000000", "4|0.500000000000000000000000000000", "5|0.490000000000000000000000000000"`
- `repark` ansi=false `df/bc/t/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/t/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/t/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["4"], "types": ["int"]}`
- `repark` ansi=false `df/bc/t/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/t/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|1.500000000000000000000000000000", "3|-1.500000000000000000000000000000", "4|0.500000000000000000000000000000", "5|0.490000000000000000000000000000",`
- `repark` ansi=false `df/bc/t/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|1.500000000000000000000000000000", "3|0.500000000000000000000000000000", "4|0.500000000000000000000000000000", "5|0.490000000000000000000000000000", `
- `repark` ansi=false `df/bc/t/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|4", "3|4", "4|4"], "types": ["int", "int"]}`
- `repark` ansi=false `df/bc/t/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/t/fexpr_coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|1.250000000000000000000000000000", "2|1.500000000000000000000000000000", "3|-1.500000000000000000000000000000", "4|0.500000000000000000000000000000", "5|0.490000000000000000000000000000",`
- `repark` ansi=false `df/bc/t/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["4"], "types": ["int"]}`
- `repark` ansi=false `df/gh/t/union`: **wrong_answer**; RePark `{"rows": ["-1.00000000005000000000", "-1.00000000010000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000049", "1.00000000005000000000", "1.000`; Spark `{"rows": ["-1.0000000001", "-1.0000000001", "0.0000000000", "0.0000000000", "1.0000000000", "1.0000000000", "1.0000000001", "1.0000000001", "2.5000000000", "2.5000000000", "null", "null"], "types": ["`
- `repark` ansi=false `df/gh/t/unionByName`: **wrong_answer**; RePark `{"rows": ["-1.00000000005000000000", "-1.00000000010000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000049", "1.00000000005000000000", "1.000`; Spark `{"rows": ["-1.0000000001", "-1.0000000001", "0.0000000000", "0.0000000000", "1.0000000000", "1.0000000000", "1.0000000001", "1.0000000001", "2.5000000000", "2.5000000000", "null", "null"], "types": ["`
- `repark` ansi=false `df/gh/t/when`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000000000000000", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.0000000001", "2|2.5000000000", "3|-1.0000000001", "4|0.0000000000", "5|1.0000000000", "6|null"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/t/isin`: **wrong_answer**; RePark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/t/between`: **wrong_answer**; RePark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/t/filter_eq`: **wrong_answer**; RePark `{"rows": ["2"], "types": ["int"]}`; Spark `{"rows": ["1", "2", "3", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/gh/t/select_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/t/coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.0000000001", "2|2.5000000000", "3|-1.0000000001", "4|0.0000000000", "5|1.0000000000", "6|null"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/t/greatest`: **wrong_answer**; RePark `{"rows": ["1|1.00000000010000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.0000000001", "2|2.5000000000", "3|-1.0000000001", "4|0.0000000000", "5|1.0000000000", "6|null"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/t/join`: **wrong_answer**; RePark `{"rows": ["2|2"], "types": ["int", "int"]}`; Spark `{"rows": ["1|1", "2|2", "3|3", "4|4", "5|5"], "types": ["int", "int"]}`
- `repark` ansi=false `df/gh/t/fexpr_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|true", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/t/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.0000000001", "2|2.5000000000", "3|-1.0000000001", "4|0.0000000000", "5|1.0000000000", "6|null"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/t/fexpr_filter`: **wrong_answer**; RePark `{"rows": ["2"], "types": ["int"]}`; Spark `{"rows": ["1", "2", "3", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/i64h/t/union`: **wrong_answer**; RePark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000000", "1.00000000000000000049", "1.000`; Spark `{"rows": ["-1.000000000000000000", "-1.000000000050000000", "0.000000000000000000", "0.000000000050000000", "1.000000000000000000", "1.000000000000000000", "1.000000000000000000", "1.00000000005000000`
- `repark` ansi=false `df/i64h/t/unionByName`: **wrong_answer**; RePark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000000", "1.00000000000000000049", "1.000`; Spark `{"rows": ["-1.000000000000000000", "-1.000000000050000000", "0.000000000000000000", "0.000000000050000000", "1.000000000000000000", "1.000000000000000000", "1.000000000000000000", "1.00000000005000000`
- `repark` ansi=false `df/i64h/t/when`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.00000000000000000000", "3|-1.00000000005000000000", "4|0.00000000000000000000", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.000000000050000000", "2|2.000000000000000000", "3|-1.000000000050000000", "4|0.000000000000000000", "5|1.000000000000000000", "6|null"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/t/isin`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/t/between`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/t/filter_eq`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["5"], "types": ["int"]}`
- `repark` ansi=false `df/i64h/t/select_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/t/coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.000000000050000000", "2|2.500000000000000000", "3|-1.000000000050000000", "4|0.000000000050000000", "5|1.000000000000000000", "6|null"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/t/greatest`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000000000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.000000000050000000", "2|2.500000000000000000", "3|-1.000000000000000000", "4|0.000000000050000000", "5|1.000000000000000000", "6|null"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/t/join`: **wrong_answer**; RePark `{"rows": [], "types": ["int", "int"]}`; Spark `{"rows": ["1|5", "5|5"], "types": ["int", "int"]}`
- `repark` ansi=false `df/i64h/t/fexpr_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/t/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.00000000005000000000", "2|2.50000000000000000000", "3|-1.00000000005000000000", "4|0.00000000004999999999", "5|1.00000000000000000049", "6|null"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["1|1.000000000050000000", "2|2.500000000000000000", "3|-1.000000000050000000", "4|0.000000000050000000", "5|1.000000000000000000", "6|null"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/t/fexpr_filter`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["5"], "types": ["int"]}`
- `repark` ansi=false `df/i32w30/t/union`: **wrong_answer**; RePark `{"rows": ["-1.000000000000000000000000000000", "-1.000000000000000000000000000050", "0.000000000000000000000000000000", "0.000000000000000000000000000000", "0.999999999999999999999999999950", "1.00000`; Spark `{"rows": ["-1.0000000000000000000000000000", "-1.0000000000000000000000000001", "0.0000000000000000000000000000", "0.0000000000000000000000000000", "1.0000000000000000000000000000", "1.000000000000000`
- `repark` ansi=false `df/i32w30/t/unionByName`: **wrong_answer**; RePark `{"rows": ["-1.000000000000000000000000000000", "-1.000000000000000000000000000050", "0.000000000000000000000000000000", "0.000000000000000000000000000000", "0.999999999999999999999999999950", "1.00000`; Spark `{"rows": ["-1.0000000000000000000000000000", "-1.0000000000000000000000000001", "0.0000000000000000000000000000", "0.0000000000000000000000000000", "1.0000000000000000000000000000", "1.000000000000000`
- `repark` ansi=false `df/i32w30/t/when`: **wrong_answer**; RePark `{"rows": ["1|1.000000000000000000000000000049", "2|2.000000000000000000000000000000", "3|-1.000000000000000000000000000050", "4|0.000000000000000000000000000000", "5|0.999999999999999999999999999950",`; Spark `{"rows": ["1|1.0000000000000000000000000000", "2|2.0000000000000000000000000000", "3|-1.0000000000000000000000000001", "4|0.0000000000000000000000000000", "5|1.0000000000000000000000000000", "6|null"]`
- `repark` ansi=false `df/i32w30/t/isin`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/t/between`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/t/filter_eq`: **wrong_answer**; RePark `{"rows": ["4"], "types": ["int"]}`; Spark `{"rows": ["1", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `df/i32w30/t/select_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/t/coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.000000000000000000000000000049", "2|2.500000000000000000000000000000", "3|-1.000000000000000000000000000050", "4|0.000000000000000000000000000000", "5|0.999999999999999999999999999950",`; Spark `{"rows": ["1|1.0000000000000000000000000000", "2|2.5000000000000000000000000000", "3|-1.0000000000000000000000000001", "4|0.0000000000000000000000000000", "5|1.0000000000000000000000000000", "6|null"]`
- `repark` ansi=false `df/i32w30/t/greatest`: **wrong_answer**; RePark `{"rows": ["1|1.000000000000000000000000000049", "2|2.500000000000000000000000000000", "3|-1.000000000000000000000000000000", "4|0.000000000000000000000000000000", "5|1.000000000000000000000000000000",`; Spark `{"rows": ["1|1.0000000000000000000000000000", "2|2.5000000000000000000000000000", "3|-1.0000000000000000000000000000", "4|0.0000000000000000000000000000", "5|1.0000000000000000000000000000", "6|null"]`
- `repark` ansi=false `df/i32w30/t/join`: **wrong_answer**; RePark `{"rows": ["4|4"], "types": ["int", "int"]}`; Spark `{"rows": ["1|1", "1|5", "4|4", "5|1", "5|5"], "types": ["int", "int"]}`
- `repark` ansi=false `df/i32w30/t/fexpr_eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|true", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|false", "3|false", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/t/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["1|1.000000000000000000000000000049", "2|2.500000000000000000000000000000", "3|-1.000000000000000000000000000050", "4|0.000000000000000000000000000000", "5|0.999999999999999999999999999950",`; Spark `{"rows": ["1|1.0000000000000000000000000000", "2|2.5000000000000000000000000000", "3|-1.0000000000000000000000000001", "4|0.0000000000000000000000000000", "5|1.0000000000000000000000000000", "6|null"]`
- `repark` ansi=false `df/i32w30/t/fexpr_filter`: **wrong_answer**; RePark `{"rows": ["4"], "types": ["int"]}`; Spark `{"rows": ["1", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_c_eq_a1/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a1_eq_c/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_eq_c125/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_c125_eq_a/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_c_isin_a1/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_eq_pydec/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_isin_pydec/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_c_between_a1/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `dfconst/fexpr_c_eq_a1/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `dfconst/fexpr_a_eq_lit/t`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `df/ac/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-100000000", "-99999999999999999999999999999999999999", "100000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-100000000", "-99999999999999999999999999999999999999", "100000000", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on -99999999999999999999999999999999999999"`; Spark `{"rows": ["7|100000000", "8|-99999999999999999999999999999999999999"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/ac/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/tx/coalesce`: **wrong_answer**; RePark `{"rows": ["7|99999999.99", "8|-99999999.99"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["7|100000000", "8|-100000000"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|99999999999999999999999999999999999999", "8|-100000000"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/ac/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ac/tx/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["7|99999999.99", "8|-99999999.99"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["7|100000000", "8|-100000000"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ac/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/ab/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-99999999999999999999999999999999999999", "1", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1", "-99999999999999999999999999999999999999", "1", "99999999999999999999999999999999999999"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on -99999999999999999999999999999999999999"`; Spark `{"rows": ["7|1", "8|-99999999999999999999999999999999999999"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/ab/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/tx/coalesce`: **wrong_answer**; RePark `{"rows": ["7|0.99999999999999999999999999999999999999", "8|-0.99999999999999999999999999999999999999"], "types": ["int", "decimal(38,38)"]}`; Spark `{"rows": ["7|1", "8|-1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|99999999999999999999999999999999999999", "8|-1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/ab/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/ab/tx/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["7|0.99999999999999999999999999999999999999", "8|-0.99999999999999999999999999999999999999"], "types": ["int", "decimal(38,38)"]}`; Spark `{"rows": ["7|1", "8|-1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `df/ab/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/bc/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["-1.000000000000000000000000000000", "-99999999.990000000000000000000000000000", "1.000000000000000000000000000000", "99999999.990000000000000000000000000000"], "types": ["decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["-1.000000000000000000000000000000", "-99999999.990000000000000000000000000000", "1.000000000000000000000000000000", "99999999.990000000000000000000000000000"], "types": ["decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-1.000000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/bc/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/tx/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-99999999.990000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-1.000000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/bc/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/bc/tx/fexpr_coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|99999999.990000000000000000000000000000", "8|-99999999.990000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`
- `repark` ansi=false `df/bc/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/gh/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1.0000000000", "0.0000000000", "1000000000000000000.0000000000", "9999999999999999999999999999.9999999999"], "types": ["decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1.0000000000", "0.0000000000", "1000000000000000000.0000000000", "9999999999999999999999999999.9999999999"], "types": ["decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/when`: **wrong_answer**; RePark `{"rows": ["7|999999999999999999.99999999999999999999", "8|-1.00000000000000000000"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["7|1000000000000000000.0000000000", "8|-1.0000000000"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/gh/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/tx/coalesce`: **wrong_answer**; RePark `{"rows": ["7|999999999999999999.99999999999999999999", "8|0.00000000000000000050"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["7|1000000000000000000.0000000000", "8|0.0000000000"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|9999999999999999999999999999.9999999999", "8|0.0000000000"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/gh/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/gh/tx/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["7|999999999999999999.99999999999999999999", "8|0.00000000000000000050"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["7|1000000000000000000.0000000000", "8|0.0000000000"], "types": ["int", "decimal(38,10)"]}`
- `repark` ansi=false `df/gh/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/i64h/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["-9223372036854775808.000000000000000000", "0.000000000000000001", "1000000000000000000.000000000000000000", "9223372036854775807.000000000000000000"], "types": ["decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["-9223372036854775808.000000000000000000", "0.000000000000000001", "1000000000000000000.000000000000000000", "9223372036854775807.000000000000000000"], "types": ["decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: -9223372036854775808 * 100000000000000000000"`; Spark `{"rows": ["7|1000000000000000000.000000000000000000", "8|-9223372036854775808.000000000000000000"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/i64h/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/tx/coalesce`: **wrong_answer**; RePark `{"rows": ["7|999999999999999999.99999999999999999999", "8|0.00000000000000000050"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["7|1000000000000000000.000000000000000000", "8|0.000000000000000001"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|9223372036854775807.000000000000000000", "8|0.000000000000000001"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/i64h/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i64h/tx/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["7|999999999999999999.99999999999999999999", "8|0.00000000000000000050"], "types": ["int", "decimal(38,20)"]}`; Spark `{"rows": ["7|1000000000000000000.000000000000000000", "8|0.000000000000000001"], "types": ["int", "decimal(38,18)"]}`
- `repark` ansi=false `df/i64h/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/i32w30/tx/union`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["-2147483648.0000000000000000000000000000", "1.0000000000000000000000000000", "100000000.0000000000000000000000000000", "2147483647.0000000000000000000000000000"], "types": ["decimal(38,28)"`
- `repark` ansi=false `df/i32w30/tx/unionByName`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["-2147483648.0000000000000000000000000000", "1.0000000000000000000000000000", "100000000.0000000000000000000000000000", "2147483647.0000000000000000000000000000"], "types": ["decimal(38,28)"`
- `repark` ansi=false `df/i32w30/tx/when`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: -2147483648 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|100000000.0000000000000000000000000000", "8|-2147483648.0000000000000000000000000000"], "types": ["int", "decimal(38,28)"]}`
- `repark` ansi=false `df/i32w30/tx/isin`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/tx/between`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/tx/filter_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `df/i32w30/tx/select_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/tx/coalesce`: **wrong_answer**; RePark `{"rows": ["7|99999999.999999999999999999999999999999", "8|1.000000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`; Spark `{"rows": ["7|100000000.0000000000000000000000000000", "8|1.0000000000000000000000000000"], "types": ["int", "decimal(38,28)"]}`
- `repark` ansi=false `df/i32w30/tx/greatest`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|2147483647.0000000000000000000000000000", "8|1.0000000000000000000000000000"], "types": ["int", "decimal(38,28)"]}`
- `repark` ansi=false `df/i32w30/tx/join`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": [], "types": ["int", "int"]}`
- `repark` ansi=false `df/i32w30/tx/fexpr_eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|false", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `df/i32w30/tx/fexpr_coalesce`: **wrong_answer**; RePark `{"rows": ["7|99999999.999999999999999999999999999999", "8|1.000000000000000000000000000000"], "types": ["int", "decimal(38,30)"]}`; Spark `{"rows": ["7|100000000.0000000000000000000000000000", "8|1.0000000000000000000000000000"], "types": ["int", "decimal(38,28)"]}`
- `repark` ansi=false `df/i32w30/tx/fexpr_filter`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_eq_c125/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_c125_eq_a/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_eq_pydec/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `dfconst/df_a_isin_pydec/tx`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": [], "types": ["int"]}`
- `repark` ansi=false `ice/ice_insert_union`: **wrong_write**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2.00", "-2.00", "0.00", "0.00", "1.00", "1.00", "1.00", "1.00", "1.00", "2.00", "2.00", "null"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `ice/ice_insert_union_h`: **wrong_write**; RePark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000000", "1.00000000000000000049", "1.000`; Spark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "0.00000000000000000000", "0.00000000005000000000", "1.00000000000000000000", "1.00000000000000000000", "1.00000000000000000000", "1.000`
- `repark` ansi=false `ice/ice_insert_union_gh`: **wrong_write**; RePark `{"rows": ["-1.00000000005000000000", "-1.00000000010000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000049", "1.00000000005000000000", "1.000`; Spark `{"rows": ["-1.00000000010000000000", "-1.00000000010000000000", "0.00000000000000000000", "0.00000000000000000000", "1.00000000000000000000", "1.00000000000000000000", "1.00000000010000000000", "1.000`
- `repark` ansi=false `ice/ice_insert_union_bc`: **wrong_write**; RePark `{"rows": ["-0.50000000000000000000000000000000000000", "0.12345678901234567890123456789012345678", "0.49000000000000000000000000000000000000", "0.50000000000000000000000000000000000000", "0.5000000000`; Spark `{"rows": ["-0.50000000000000000000000000000000000000", "0.12345678901234567890123456789000000000", "0.49000000000000000000000000000000000000", "0.50000000000000000000000000000000000000", "0.5000000000`
- `repark` ansi=false `ice/ice_insert_coalesce`: **wrong_write**; RePark `{"rows": ["-1.50", "0.49", "1.00", "1.25", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2.00", "0.00", "1.00", "1.00", "2.00", "null"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `ice/ice_insert_case`: **wrong_write**; RePark `{"rows": ["-1.50", "0.49", "1.00", "1.25", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2.00", "0.00", "1.00", "1.00", "2.00", "null"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `ice/ice_insert_values`: **wrong_write**; RePark `{"rows": ["1.00", "1.25"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["1.00", "1.00"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `ice/ice_insert_where`: **wrong_write**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "2", "3", "4", "5"], "types": ["int"]}`
- `repark` ansi=false `ice/ice_ctas_union`: **wrong_write**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `ice/ice_ctas_coalesce`: **wrong_write**; RePark `{"rows": ["1|1.25", "2|1.50", "3|-1.50", "4|0.50", "5|0.49", "6|1.45"], "types": ["int", "decimal(38,2)"]}`; Spark `{"rows": ["1|1", "2|2", "3|-2", "4|1", "5|0", "6|1"], "types": ["int", "decimal(38,0)"]}`
- `repark` ansi=false `ice/ice_ctas_union_h`: **wrong_write**; RePark `{"rows": ["-1.00000000000000000000", "-1.00000000005000000000", "0.00000000000000000000", "0.00000000004999999999", "1.00000000000000000000", "1.00000000000000000000", "1.00000000000000000049", "1.000`; Spark `{"rows": ["-1.000000000000000000", "-1.000000000050000000", "0.000000000000000000", "0.000000000050000000", "1.000000000000000000", "1.000000000000000000", "1.000000000000000000", "1.00000000005000000`
- `repark` ansi=false `ice/ice_rtas_union`: **wrong_write**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `icedml/cow/delete_a_c125`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/delete_c_a1`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/delete_a_eq_c`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/delete_a_lit`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/delete_in_subq`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/update_c_a1`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|u", "2|null|o", "3|null|o", "4|null|u", "5|null|o", "6|null|u"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/update_c_in`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|u", "2|null|u", "3|null|o", "4|null|u", "5|null|o", "6|null|u"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/update_h_i64`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|u", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/update_set_coalesce`: **over_raise**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|1.00|o", "2|2.00|o", "3|-2.00|o", "4|1.00|o", "5|0.00|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/merge_on_a_c`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|m", "2|null|m", "3|null|o", "4|null|m", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/merge_on_c_a`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|m", "2|null|o", "3|null|o", "4|null|m", "5|null|o", "6|null|m"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/cow/merge_insert_union`: **wrong_write**; RePark `{"rows": ["100|1.00|i", "101|1.25|i", "1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["100|1.00|i", "101|1.00|i", "1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icepart/cow/part_delete_c_a1`: **wrong_write**; RePark `{"rows": ["1", "2", "3", "4", "5", "6"], "types": ["int"]}`; Spark `{"rows": ["2", "3", "5"], "types": ["int"]}`
- `repark` ansi=false `icepart/cow/part_delete_a_c125`: **wrong_write**; RePark `{"rows": ["1", "2", "3", "4", "5", "6"], "types": ["int"]}`; Spark `{"rows": ["2", "3", "5", "6"], "types": ["int"]}`
- `repark` ansi=false `icedml/mor/delete_a_c125`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/delete_c_a1`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/delete_a_eq_c`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/delete_a_lit`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["2|null|o", "3|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/delete_in_subq`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/update_c_a1`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|u", "2|null|o", "3|null|o", "4|null|u", "5|null|o", "6|null|u"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/update_c_in`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|u", "2|null|u", "3|null|o", "4|null|u", "5|null|o", "6|null|u"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/update_h_i64`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|u", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/update_set_coalesce`: **over_raise**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|1.00|o", "2|2.00|o", "3|-2.00|o", "4|1.00|o", "5|0.00|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/merge_on_a_c`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|m", "2|null|m", "3|null|o", "4|null|m", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/merge_on_c_a`: **wrong_write**; RePark `{"rows": ["1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["1|null|m", "2|null|o", "3|null|o", "4|null|m", "5|null|o", "6|null|m"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icedml/mor/merge_insert_union`: **wrong_write**; RePark `{"rows": ["100|1.00|i", "101|1.25|i", "1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`; Spark `{"rows": ["100|1.00|i", "101|1.00|i", "1|null|o", "2|null|o", "3|null|o", "4|null|o", "5|null|o", "6|null|o"], "types": ["int", "decimal(38,2)", "string"]}`
- `repark` ansi=false `icepart/mor/part_delete_c_a1`: **wrong_write**; RePark `{"rows": ["1", "2", "3", "4", "5", "6"], "types": ["int"]}`; Spark `{"rows": ["2", "3", "5"], "types": ["int"]}`
- `repark` ansi=false `icepart/mor/part_delete_a_c125`: **wrong_write**; RePark `{"rows": ["1", "2", "3", "4", "5", "6"], "types": ["int"]}`; Spark `{"rows": ["2", "3", "5", "6"], "types": ["int"]}`
- `repark` ansi=false `icepart/read/part_c_a1`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `icepart/read/part_c_in`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "2", "4", "6"], "types": ["int"]}`
- `repark` ansi=false `icepart/read/part_a_c125`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `icepart/read/part_a_lit`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["1", "4"], "types": ["int"]}`
- `repark` ansi=false `icepart/read/part_h_big`: **wrong_answer**; RePark `{"rows": [], "types": ["int"]}`; Spark `{"rows": ["5"], "types": ["int"]}`
- `repark` ansi=false `legacy/pair/ab/t/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|null", "2|null", "3|null", "4|null", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/ab/t/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.00000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-0.50000000000000000000000000000000000000", "0.00000000000000000000000000000000000000", "0.12345678901234567890123456789012345678", "0.49999999999999999999999999999999999999", "0.5000000000`
- `repark` ansi=false `legacy/pair/bc/t/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["-0.50000000000000000000000000000000000000", "0.12345678901234567890123456789012345678", "0.49000000000000000000000000000000000000", "0.49999999999999999999999999999999999999", "0.5000000000`
- `repark` ansi=false `legacy/pair/bc/t/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|null", "2|null", "3|null", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/bc/t/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 1.25000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1|0.50000000000000000000000000000000000000", "2|-0.50000000000000000000000000000000000000", "3|0.49999999999999999999999999999999999999", "4|0.50000000000000000000000000000000000000", "5|0.`
- `repark` ansi=false `legacy/pair/ab/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/ab/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999", "null", "null"], "types": ["decimal(38,38)"]}`
- `repark` ansi=false `legacy/pair/ac/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/ac/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 2). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-99999999.99", "99999999.99", "null", "null"], "types": ["decimal(38,2)"]}`
- `repark` ansi=false `legacy/pair/ad/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-0.5", "9999999999999999999999999999999999999.9", "null", "null"], "types": ["decimal(38,1)"]}`
- `repark` ansi=false `legacy/pair/ad/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 1). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/bc/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["-0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999", "null", "null"], "types": ["decimal(38,38)"]}`
- `repark` ansi=false `legacy/pair/bc/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/bc/tx/coalesce`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 9999999999"`; Spark `{"rows": ["7|0.99999999999999999999999999999999999999", "8|-0.99999999999999999999999999999999999999"], "types": ["int", "decimal(38,38)"]}`
- `repark` ansi=false `legacy/pair/ef/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999"`; Spark `{"rows": ["-0.50000000000000000000", "0.000000000000000000490", "null", "null"], "types": ["decimal(38,20)"]}`
- `repark` ansi=false `legacy/pair/ef/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/gh/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["7|null", "8|false"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/gh/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Cast error: Cannot cast to Decimal128(38, 20). Overflowing on 99999999999999999999999999999999999999"`; Spark `{"rows": ["-1.00000000000000000000", "0.000000000000000000500", "999999999999999999.99999999999999999999", "null"], "types": ["decimal(38,20)"]}`
- `repark` ansi=false `legacy/pair/i64h/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/i64h/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 9223372036854775807 * 100000000000000000000"`; Spark `{"rows": ["0.000000000000000000500", "999999999999999999.99999999999999999999", "null", "null"], "types": ["decimal(38,20)"]}`
- `repark` ansi=false `legacy/pair/i32w30/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/i32w30/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 2147483647 * 1000000000000000000000000000000"`; Spark `{"rows": ["1.000000000000000000000000000000", "99999999.999999999999999999999999999999", "null", "null"], "types": ["decimal(38,30)"]}`
- `repark` ansi=false `legacy/pair/i16w34/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 32767 * 10000000000000000000000000000000000"`; Spark `{"rows": ["1.0000000000000000000000000000000000", "9999.9999999999999999999999999999999999", "null", "null"], "types": ["decimal(38,34)"]}`
- `repark` ansi=false `legacy/pair/i16w34/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Arithmetic overflow: Overflow happened on: 32767 * 10000000000000000000000000000000000"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/pair/i8w36/tx/union_all`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 127.000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["1.000000000000000000000000000000000000", "99.999999999999999999999999999999999999", "null", "null"], "types": ["decimal(38,36)"]}`
- `repark` ansi=false `legacy/pair/i8w36/tx/eq`: **over_raise**; RePark `"ERR:PySparkException:None:Arrow error: Invalid argument error: 127.000000000000000000000000000000000000 is too large to store in a Decimal128 of precision 38. M"`; Spark `{"rows": ["7|null", "8|null"], "types": ["int", "boolean"]}`
- `repark` ansi=false `legacy/extra/v_bc`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.250000000000000"`; Spark `{"rows": ["0.50000000000000000000000000000000000000", "null"], "types": ["decimal(38,38)"]}`
- `repark` ansi=false `legacy/extra/u_const_ab`: **over_raise**; RePark `"ERR:PySparkException:None:datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Invalid argument error: 1.000000000000000"`; Spark `{"rows": ["0.50000000000000000000000000000000000000", "null"], "types": ["decimal(38,38)"]}`
- `repark` ansi=false `legacy/after_reset/union_all`: **wrong_answer**; RePark `{"rows": ["-1.50", "-2.00", "0.00", "0.49", "0.50", "1.00", "1.00", "1.25", "1.45", "1.50", "2.00", "null"], "types": ["decimal(38,2)"]}`; Spark `{"rows": ["-2", "-2", "0", "0", "1", "1", "1", "1", "1", "2", "2", "null"], "types": ["decimal(38,0)"]}`
- `repark` ansi=false `legacy/after_reset/eq`: **wrong_answer**; RePark `{"rows": ["1|false", "2|false", "3|false", "4|false", "5|false", "6|null"], "types": ["int", "boolean"]}`; Spark `{"rows": ["1|true", "2|true", "3|true", "4|true", "5|true", "6|null"], "types": ["int", "boolean"]}`

