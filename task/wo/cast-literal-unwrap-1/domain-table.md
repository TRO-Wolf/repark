# CAST-LITERAL-UNWRAP-1 — the domain table

Measured 2026-11-16 on main `83df6be1`'s package against Spark 4.1.2, ANSI off and on. Provenance and commands:
[evidence/map.md](evidence/map.md). Every cell, with both answers and its verdict, is in
[evidence/raw/verdicts.jsonl](evidence/raw/verdicts.jsonl); the cell definitions are
[evidence/probes/cells.py](evidence/probes/cells.py).

## The grid and its sampling

- **29 cast kinds** in nine families (the `family` column below): DECIMAL to `TINYINT`, `SMALLINT`, `INT`,
  `BIGINT`; DECIMAL to a smaller scale (`DECIMAL(10,0)`, `DECIMAL(10,1)`) and to a smaller precision (overflow, and
  overflow by rounding); `BIGINT` to `INT` and `INT` to `SMALLINT` across the overflow values; `DOUBLE` and `FLOAT` to
  integers (with NaN, ±Infinity, ±1e20), `DOUBLE` to `FLOAT`, `DOUBLE` to DECIMAL (and its overflow); `STRING` to
  `INT`, DECIMAL, `DOUBLE` and `DATE` over `'01'`, `' 1'`, `'1 '`, `'1.0'`, `'+1'`, `'1e0'`, `'1d'`, invalid and
  empty texts; `TIMESTAMP` and `TIMESTAMP_NTZ` to `DATE`; and three lossless controls (`INT` to `BIGINT`, DECIMAL to
  a wider decimal, `DATE` to `TIMESTAMP`). Values per kind: halves, negatives (truncation toward zero), the type's
  extremes, overflow values in their own kinds (so an ANSI raise does not hide the in-range kinds) and NULL.
- **Constants:** integer, decimal, string and typed-`CAST` literals and a folding expression (`(3 - 2)`), two to
  eight per kind; on the right and on the left; an `IN` list, `NOT IN`, `BETWEEN` and `NOT BETWEEN`.
- **Operators:** `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`.
- **Dense on the temp view:** every constant × every operator × both sides, the lists, and the positions (select
  list `=` and `<`, `CASE WHEN`, `HAVING`, `JOIN … ON a.id = b.id AND <cast term>`).
- **Sampled on Parquet and both Iceberg tables:** the first two constants × `=`, `<`, `>=`, `<=>` on the right and
  `=` on the left, `IN`, `NOT IN`, `BETWEEN`, the select list and `JOIN … ON`. The cast family is uniform across
  storages on the temp view's evidence, and every storage still meets every kind.
- **Storage:** a temp view over `range` (not constant-foldable), one Parquet file, an Iceberg v2 table on a hadoop
  catalog written one row per data file (file pruning by metrics), and one partitioned by identity on the cast
  column (manifest and partition pruning).
- **Doors:** Spark-door SQL on every storage; the DataFrame API (`col.cast(t) == lit`, `>`, `lit == col.cast`,
  `isin`, `between`, a select-list comparison), `F.expr` (filter and select) and `filter(str)` (`=` and `>`) on the
  temp view and the partitioned Iceberg table; the native door `repark.sql()` with the dense grid and positions on
  a view (it refuses local Parquet DDL, so it has no Parquet or catalog cell).
- **Iceberg DML** on seven kinds (`dec_int`, `dec_dec0`, `dec_dec1`, `bigint_int`, `str_int`, `str_date`, `ts_date`):
  `DELETE … WHERE` (`=` and `>`), `UPDATE … WHERE`, `MERGE … ON <cast term>` and `MERGE … WHEN MATCHED AND <cast
  term>`, copy-on-write and merge-on-read, each on a fresh table, the stored rows read back after the statement.
- **Kernel check:** the first two constants × `=`, `<`, `>`, `<=>`, against a column that holds the constant
  (`element_at(array(k, k), id % 2 + 1)`), which no simplifier can unwrap.
- **Totals:** 3,702 RePark cells per ANSI mode (3,632 reads and 70 DML statements), 1,759 native cells compared
  with both ANSI modes, 232 kernel cells per engine and mode.

## Verdicts

`agree`; `both_raise` (both refuse; classes may differ); `wrong_answer` (both answer, differently);
`wrong_write` (both statements succeed, the stored rows differ); `missed_raise` (Spark raises, RePark answers);
`over_raise` (Spark answers, RePark refuses).

## Exclusions

The ten `date_ts` DataFrame cells per mode with a Python `datetime` literal disagree because PySpark reads a naive
`datetime` in the driver's zone (America/New_York) and RePark in the session zone (UTC). That is not this unit; the
same kind agrees on every SQL, `F.expr` and `filter(str)` cell. They are counted below as they fell.


## RePark against Spark, by family

The four numeric families are where DataFusion unwraps ([inventory.md](inventory.md) A6); the others are not
unwrapped, and their non-agreeing cells are kernel refusals ([kernel check](#kernel-check)).

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

## By kind

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

## By storage

`ice_files` is the Iceberg table written one row per file, `ice_part` the one partitioned on the cast column,
`ice_cow` and `ice_mor` the DML tables.

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

## By door

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

## By position

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

## Iceberg DML: every statement, both modes

The ids each statement deleted (`delete_eq`, `delete_gt`) or retagged (`update_eq`, `merge_on`, `merge_and`),
copy-on-write and merge-on-read; the stored rows themselves are in the raw files.


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

## The native door `repark.sql()`

It is an ANSI session; its cells are compared with Spark ANSI on here (the ANSI-off comparison is in
[evidence/raw/summary.md](evidence/raw/summary.md)).

| kind | ANSI | cells | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|---|---|
| bigint_int | true | 79 | 0 | 0 | 0 | 0 | 79 | 0 |
| date_ts | true | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| dec38_int_overflow | true | 51 | 0 | 0 | 0 | 0 | 51 | 0 |
| dec_bigint | true | 65 | 27 | 0 | 38 | 0 | 0 | 0 |
| dec_dec0 | true | 107 | 18 | 0 | 89 | 0 | 0 | 0 |
| dec_dec1 | true | 79 | 30 | 0 | 49 | 0 | 0 | 0 |
| dec_int | true | 121 | 39 | 0 | 82 | 0 | 0 | 0 |
| dec_prec_overflow | true | 65 | 0 | 0 | 0 | 0 | 65 | 0 |
| dec_round_overflow | true | 51 | 0 | 0 | 0 | 0 | 51 | 0 |
| dec_smallint | true | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_tinyint | true | 51 | 13 | 0 | 38 | 0 | 0 | 0 |
| dec_tinyint_overflow | true | 51 | 0 | 0 | 2 | 0 | 49 | 0 |
| dec_wide | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_bigint | true | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| double_dec | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_dec_overflow | true | 51 | 0 | 51 | 0 | 0 | 0 | 0 |
| double_float | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_int | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| double_int_edge | true | 51 | 0 | 51 | 0 | 0 | 0 | 0 |
| float_int | true | 37 | 37 | 0 | 0 | 0 | 0 | 0 |
| int_bigint | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |
| int_smallint | true | 51 | 0 | 0 | 2 | 0 | 49 | 0 |
| ntz_date | true | 51 | 51 | 0 | 0 | 0 | 0 | 0 |
| str_date | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_dec | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_double | true | 51 | 0 | 0 | 0 | 0 | 0 | 51 |
| str_int | true | 79 | 0 | 79 | 0 | 0 | 0 | 0 |
| str_int_invalid | true | 37 | 0 | 37 | 0 | 0 | 0 | 0 |
| ts_date | true | 65 | 65 | 0 | 0 | 0 | 0 | 0 |


## Kernel check

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

## Headline cells

The temp view, `CAST(c AS T) = <first constant>`, constant on the right. Ids index the kind's values in
[evidence/probes/cells.py](evidence/probes/cells.py) (for `dec_int`: 1 is 0.50, 2 is 1.00, 3 is 1.25, 4 is 1.50,
5 is 1.99).

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

## After the critic: the uncovered shapes, re-measured

The Muse inventory critic of `b05846e9` (verdict GAPS: no missed seat, no contradicted cell; its probes and raw
outputs are in the orchestrator's scratch folder `cast-unwrap-critic/`, `shapes.py` and `out/shapes-*.jsonl`) named
seven shapes the grid lacked. Re-measured here on the same fixtures as the grid (`dec_int`, `dec_dec0`,
`bigint_int`, `dec_tinyint_overflow`, `int_smallint` from [evidence/probes/cells.py](evidence/probes/cells.py)),
on main and Spark 4.1.2, both modes, by [evidence/probes/run_shapes.py](evidence/probes/run_shapes.py)
(raw: `evidence/raw/shapes-{spark,repark}-{false,true}.jsonl`; table: `shapes_table.py`). The critic's shape ids
are kept (`s02`, `s03`, …); `spelled/` are the spelled-out operators, `neg_half/` the negative-half edge, `q4/` the
top-of-type cells of owner question 4.

- **Every critic shape reproduces in both modes**, the same direction as the critic measured: nested casts, `TRY_CAST`,
  `::`, a cast column filtered outside its subquery, `withColumn` then `filter`, a non-ASCII column name, the
  spelled-out `IS [NOT] DISTINCT FROM` and `NOT BETWEEN`. `TRY_CAST(b AS INT) = 1` over `BIGINT` agrees (Spark's
  `TRY_CAST` answers NULL on overflow, so no wrapped row matches).
- **Negative halves:** `CAST(c AS DECIMAL(10,0)) = -1` keeps -0.50 and -1.25 on Spark (half away from zero) and
  nothing on main; `< -1` adds -1.25 on main. The same cell is `dec_dec0/view/where/c2/eq/r` in the grid.
- **s16 differs from the critic's report on views over SQL views:** a SQL temp view over a `range` view, over a
  Parquet view, or over a plain SQL view answers on main (with the unwrap's wrong rows). Only a SQL temp view over a
  **`createDataFrame`** view raises `TABLE_OR_VIEW_NOT_FOUND` (`__repark_cdf_…`) where Spark answers.
- **s26:** `EXPLAIN` over a `read.parquet` temp view explains on main. The failing form is the table name created by
  `CREATE TABLE … USING parquet` (sketch §6, item 1), which is what the critic's `pq` was.

| key | shape | Spark off | main off | Spark on | main on |
|---|---|---|---|---|---|
| `s02/nested_int_bigint` | `SELECT id FROM v_dec_int WHERE CAST(CAST(c AS INT) AS BIGINT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s02/nested_dec0_dec20` | `SELECT id FROM v_dec_dec0 WHERE CAST(CAST(c AS DECIMAL(10,0)) AS DECIMAL(20,0)) = 1` | `[1, 2, 3]` | `[2]` | `[1, 2, 3]` | `[2]` |
| `s03/try_cast_int` | `SELECT id FROM v_dec_int WHERE TRY_CAST(c AS INT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s03/try_cast_dec0` | `SELECT id FROM v_dec_dec0 WHERE TRY_CAST(c AS DECIMAL(10,0)) = 1` | `[1, 2, 3]` | `[2]` | `[1, 2, 3]` | `[2]` |
| `s03/try_cast_bigint_int` | `SELECT id FROM v_bigint_int WHERE TRY_CAST(c AS INT) = 1` | `[1]` | `[1]` | `[1]` | `[1]` |
| `s04/colon_int` | `SELECT id FROM v_dec_int WHERE c::int = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s04/colon_dec0` | `SELECT id FROM v_dec_dec0 WHERE c::decimal(10,0) = 1` | `[1, 2, 3]` | `[2]` | `[1, 2, 3]` | `[2]` |
| `s17/subquery` | `SELECT id FROM (SELECT id, CAST(c AS INT) AS k FROM v_dec_int) WHERE k = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s23/non_ascii` | `SELECT id FROM v_cafe WHERE CAST(`café` AS INT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `spelled/is_distinct_c0` | `SELECT id FROM v_dec_int WHERE CAST(c AS INT) IS DISTINCT FROM 1` | `[1, 6, 7, 8, 9, 10, 11, 12, 13]` | `[1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]` | `[1, 6, 7, 8, 9, 10, 11, 12, 13]` | `[1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]` |
| `spelled/is_not_distinct_c0` | `SELECT id FROM v_dec_int WHERE CAST(c AS INT) IS NOT DISTINCT FROM 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `spelled/is_distinct_c1` | `SELECT id FROM v_dec_int WHERE CAST(c AS INT) IS DISTINCT FROM 0` | `[2, 3, 4, 5, 7, 8, 9, 11, 12, 13]` | `[1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13]` | `[2, 3, 4, 5, 7, 8, 9, 11, 12, 13]` | `[1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13]` |
| `spelled/is_not_distinct_c1` | `SELECT id FROM v_dec_int WHERE CAST(c AS INT) IS NOT DISTINCT FROM 0` | `[1, 6, 10]` | `[10]` | `[1, 6, 10]` | `[10]` |
| `spelled/not_between` | `SELECT id FROM v_dec_int WHERE CAST(c AS INT) NOT BETWEEN 1 AND 2` | `[1, 6, 7, 8, 10, 11, 12]` | `[1, 6, 7, 8, 9, 10, 11, 12]` | `[1, 6, 7, 8, 10, 11, 12]` | `[1, 6, 7, 8, 9, 10, 11, 12]` |
| `neg_half/dec0_eq_minus1` | `SELECT id FROM v_dec_dec0 WHERE CAST(c AS DECIMAL(10,0)) = -1` | `[6, 7]` | `[]` | `[6, 7]` | `[]` |
| `neg_half/dec0_lt_minus1` | `SELECT id FROM v_dec_dec0 WHERE CAST(c AS DECIMAL(10,0)) < -1` | `[8, 12]` | `[7, 8, 12]` | `[8, 12]` | `[7, 8, 12]` |
| `s21/with_column_filter_str` | `withColumn("k", col("c").cast("int")).filter("k = 1")` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s22/with_column_filter_col` | `withColumn("k", col("c").cast("int")).filter(col("k") == 1)` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `q4/tinyint/gt_top` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) > 127` | `[]` | `[3, 4, 6]` | `[]` | `[3, 4, 6]` |
| `q4/tinyint/top_lt` | `SELECT id FROM v_dec_tinyint_overflow WHERE 127 < CAST(c AS TINYINT)` | `[]` | `[3, 4, 6]` | `[]` | `[3, 4, 6]` |
| `q4/tinyint/ge_top` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) >= 127` | `[2, 3, 5]` | `[2, 3, 4, 6]` | raises CAST_OVERFLOW | `[2, 3, 4, 6]` |
| `q4/tinyint/le_top` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) <= 127` | `[1, 2, 3, 4, 5, 6]` | `[1, 2, 5]` | raises CAST_OVERFLOW | `[1, 2, 5]` |
| `q4/tinyint/lt_bottom` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) < -128` | `[]` | `[5]` | `[]` | `[5]` |
| `q4/tinyint/ge_bottom` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) >= -128` | `[1, 2, 3, 4, 5, 6]` | `[1, 2, 3, 4, 6]` | raises CAST_OVERFLOW | `[1, 2, 3, 4, 6]` |
| `q4/tinyint/eq_top_plus` | `SELECT id FROM v_dec_tinyint_overflow WHERE CAST(c AS TINYINT) = 128` | `[]` | raises PySparkException | `[]` | raises PySparkException |
| `q4/smallint/gt_top` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) > 32767` | `[]` | `[4, 5]` | `[]` | `[4, 5]` |
| `q4/smallint/top_lt` | `SELECT id FROM v_int_smallint WHERE 32767 < CAST(c AS SMALLINT)` | `[]` | `[4, 5]` | `[]` | `[4, 5]` |
| `q4/smallint/ge_top` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) >= 32767` | `[3, 6]` | `[3, 4, 5]` | raises CAST_OVERFLOW | `[3, 4, 5]` |
| `q4/smallint/le_top` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) <= 32767` | `[1, 2, 3, 4, 5, 6]` | `[1, 2, 3, 6]` | raises CAST_OVERFLOW | `[1, 2, 3, 6]` |
| `q4/smallint/lt_bottom` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) < -32768` | `[]` | `[6]` | `[]` | `[6]` |
| `q4/smallint/ge_bottom` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) >= -32768` | `[1, 2, 3, 4, 5, 6]` | `[1, 2, 3, 4, 5]` | raises CAST_OVERFLOW | `[1, 2, 3, 4, 5]` |
| `q4/smallint/eq_top_plus` | `SELECT id FROM v_int_smallint WHERE CAST(c AS SMALLINT) = 32768` | `[]` | raises PySparkException | `[]` | raises PySparkException |
| `q4/int/gt_top` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) > 2147483647` | `[]` | `[6, 7]` | raises CAST_OVERFLOW | `[6, 7]` |
| `q4/int/top_lt` | `SELECT id FROM v_bigint_int WHERE 2147483647 < CAST(c AS INT)` | `[]` | `[6, 7]` | raises CAST_OVERFLOW | `[6, 7]` |
| `q4/int/ge_top` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) >= 2147483647` | `[5, 8]` | `[5, 6, 7]` | raises CAST_OVERFLOW | `[5, 6, 7]` |
| `q4/int/le_top` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) <= 2147483647` | `[1, 2, 3, 4, 5, 6, 7, 8, 9]` | `[1, 2, 3, 4, 5, 8, 9]` | raises CAST_OVERFLOW | `[1, 2, 3, 4, 5, 8, 9]` |
| `q4/int/lt_bottom` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) < -2147483648` | `[]` | `[8, 9]` | raises CAST_OVERFLOW | `[8, 9]` |
| `q4/int/ge_bottom` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) >= -2147483648` | `[1, 2, 3, 4, 5, 6, 7, 8, 9]` | `[1, 2, 3, 4, 5, 6, 7]` | raises CAST_OVERFLOW | `[1, 2, 3, 4, 5, 6, 7]` |
| `q4/int/eq_top_plus` | `SELECT id FROM v_bigint_int WHERE CAST(c AS INT) = 2147483648` | `[]` | raises PySparkException | `[]` | raises PySparkException |
| `s16/view_over_view` | `CREATE OR REPLACE TEMP VIEW w16 AS SELECT id, CAST(c AS INT) AS k FROM v_dec_int; SELECT id FROM w16 WHERE k = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s16/view_over_view_plain` | `CREATE TEMP VIEW w16b AS SELECT id, c FROM v_dec_int; SELECT id FROM w16b WHERE CAST(c AS INT) = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s16/view_over_base_table` | `CREATE TEMP VIEW w16c AS SELECT id, CAST(c AS INT) AS k FROM p16; SELECT id FROM w16c WHERE k = 1` | `[2, 3, 4, 5]` | `[2]` | `[2, 3, 4, 5]` | `[2]` |
| `s16/view_over_create_dataframe` | `createDataFrame view, then a SQL temp view over it` | `[1, 3]` | raises TABLE_OR_VIEW_NOT_FOUND | `[1, 3]` | raises TABLE_OR_VIEW_NOT_FOUND |
| `s05/int_fn` | `SELECT id FROM v_dec_int WHERE int(c) = 1` | `[2, 3, 4, 5]` | raises UNRESOLVED_ROUTINE | `[2, 3, 4, 5]` | raises UNRESOLVED_ROUTINE |
| `s06/bigint_fn` | `SELECT id FROM v_dec_int WHERE bigint(c) = 1` | `[2, 3, 4, 5]` | raises UNRESOLVED_ROUTINE | `[2, 3, 4, 5]` | raises UNRESOLVED_ROUTINE |
| `s26/explain_parquet_view` | `EXPLAIN SELECT id FROM p16 WHERE CAST(c AS INT) = 1` | plan | plan | plan | plan |

Up: [map.md](map.md).
