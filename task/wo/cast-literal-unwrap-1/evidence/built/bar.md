# CAST-LITERAL-UNWRAP-1 — the build's bar (answers)

**Head:** the package built from `88999454` (the product commit, clean worktree, `BUILT_FROM` holds the full sha).
**Main:** the package whose product tree equals main `cb7d3943`. **Spark:** Spark 4.1.2, the outputs step 0 and its
critic measured (`../raw/spark-*.jsonl`, `../raw/shapes-spark-*.jsonl`, and the critic's `reads`, `dml`, `shapes`
outputs), unchanged, since the oracle and the cells are the same; the build's own pin cells beyond them were measured
on Spark again ([probes/pin_cells.py](probes/pin_cells.py), `raw/pin-cells-spark-*.json`). Every RePark run printed
`repark._native.__file__`, never with `-I`.

**Sets.** `cells`: step 0's full domain table (`../probes/run_cells.py`, 3,702 cells per ANSI mode: reads on the
temp view, Parquet, the Iceberg table one row per file and the one partitioned on the cast column, every door and
position, and the `DELETE` / `UPDATE` / `MERGE` cells copy-on-write and merge-on-read). `native`: the native door
(`../probes/run_native.py`, against Spark ANSI on). `shapes`: step 0's after-critic shapes (`../probes/run_shapes.py`).
`critic-reads`, `critic-dml`, `critic-shapes`, `critic-native`: the critic's four sets (the native set paired by SQL
text with the critic's reads; `n11` and `n12` read a fixture without the NULL row, so their Spark answers are
derived by hand). Verdicts: EQUAL (same rows, or both raise), WRONG, MISSED-RAISE, REFUSE (RePark raises, Spark
answers). Runner and analysis: [probes/run_bar.sh](probes/run_bar.sh), [probes/bar.py](probes/bar.py).

## Result

- **0 EQUAL→WRONG, 0 EQUAL→RAISE outside the Q2 class, 0 MISSED-RAISE on the head.** Every WRONG cell on main is
  EQUAL on the head or falls in a named class.
- **EQUAL→REFUSE (Q2): 122 cells**, every one ANSI off, every one a cast that can overflow for some source value
  (`BIGINT`→`INT`, `INT`→`SMALLINT`, `DECIMAL(10,2)`→`TINYINT`, `DECIMAL(38,0)`→`INT`, `DECIMAL(10,2)`→`DECIMAL(3,2)`,
  `DECIMAL(10,3)`→`DECIMAL(3,2)`) over a column holding an overflowing value, refusing in the kernel (Arrow `Can't
  cast value`, `out of range`, `too large / too small to store`) where Spark wraps or answers NULL; main answered them
  right because the unwrap never cast. That is ruling 2's description, cell for cell (listed below).
- **WRONG→REFUSE (Q2): 648 cells**, the same casts and refusals over cells main answered wrong.
- **WRONG→REFUSE (Q4): 33 cells**, the top-of-type comparisons (`CAST(c AS TINYINT) > 127`, `127 < …`, `< -128`, the
  `SMALLINT` and, ANSI off, `INT` twins) that Spark answers with no rows without casting; the head casts per row and
  refuses (listed below).
- **Unchanged refusals: 1,149 cells**, refused on main and on the head alike: the kernel refusals step 0 carded apart
  (STRING casts, FLOAT and DOUBLE edges with ANSI off, the `= MAX+1` constant), the `int()` / `bigint()` routines and
  the `createDataFrame`-view chain.
- **Excluded: 24 cells**, step 0's ten DataFrame `datetime`-literal cells per mode (a probe artifact of the driver's
  zone), and three `EXPLAIN` texts.
- **The native door** moves as the Spark door: 334 WRONG and 344 MISSED-RAISE cells become EQUAL, 4 become Q4
  refusals, 153 refusals stay.
- **`IN` cap:** no bar cell holds more than 32 items (the domain table's lists hold two); the fallback is pinned
  (`test_an_in_list_past_the_cap_stays_per_row_and_answers_as_spark`) and measured in [cost.md](cost.md) (`in40`).

## Counts

| set | ANSI | cells | main EQUAL / WRONG / MISSED-RAISE / REFUSE | head EQUAL / WRONG / MISSED-RAISE / REFUSE |
|---|---|---|---|---|
| cells | false | 3702 | 1610 / 1358 / 0 / 734 | 2214 / 10 / 0 / 1478 |
| cells | true | 3702 | 1983 / 739 / 738 / 242 | 3444 / 10 / 0 / 248 |
| critic-dml | false | 64 | 6 / 58 / 0 / 0 | 48 / 0 / 0 / 16 |
| critic-dml | true | 64 | 6 / 42 / 16 / 0 | 64 / 0 / 0 / 0 |
| critic-native | native | 8 | 1 / 7 / 0 / 0 | 8 / 0 / 0 / 0 |
| critic-reads | false | 60 | 16 / 44 / 0 / 0 | 50 / 1 / 0 / 9 |
| critic-reads | true | 60 | 13 / 38 / 9 / 0 | 59 / 1 / 0 / 0 |
| critic-shapes | false | 29 | 6 / 19 / 0 / 4 | 25 / 0 / 0 / 4 |
| critic-shapes | true | 29 | 6 / 19 / 0 / 4 | 25 / 0 / 0 / 4 |
| native | native | 1759 | 924 / 338 / 344 / 153 | 1602 / 0 / 0 / 157 |
| shapes | false | 46 | 1 / 39 / 0 / 6 | 21 / 1 / 0 / 24 |
| shapes | true | 46 | 1 / 27 / 12 / 6 | 33 / 1 / 0 / 12 |
| **all** | | 9569 | 4573 / 2728 / 1119 / 1149 | 7593 / 24 / 0 / 1952 |

| head non-EQUAL class | cells |
|---|---|
| EQUAL->REFUSE (Q2) | 122 |
| EXCLUDED (step 0's datetime-literal artifact or plan text) | 24 |
| REFUSE->REFUSE (unchanged refusal) | 1149 |
| WRONG->REFUSE (Q2) | 648 |
| WRONG->REFUSE (Q4) | 33 |

#### EQUAL->REFUSE (Q2), cell by cell

| set | ANSI | cell | head's refusal |
|---|---|---|---|
| cells | false | `dec_tinyint_overflow/view/where/c2/eq/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c2/eq/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c2/ne/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c2/ne/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c2/ns/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c2/ns/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec38_int_overflow/view/where/c1/eq/r` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/view/where/c1/eq/l` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/view/where/c1/ne/r` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/view/where/c1/ne/l` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/view/where/c1/ns/r` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/view/where/c1/ns/l` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/parquet/where/c1/eq/r` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/parquet/where/c1/eq/l` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/parquet/where/c1/ns/r` | Arrow error: Cast error: value of 3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/ice_files/where/c1/eq/r` | Arrow error: Cast error: value of 99999999999999999999999999999999999999 is out of range I |
| cells | false | `dec38_int_overflow/ice_files/where/c1/eq/l` | Arrow error: Cast error: value of 99999999999999999999999999999999999999 is out of range I |
| cells | false | `dec38_int_overflow/ice_files/where/c1/ns/r` | Arrow error: Cast error: value of 99999999999999999999999999999999999999 is out of range I |
| cells | false | `dec38_int_overflow/ice_part/where/c1/eq/r` | Arrow error: Cast error: value of -3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/ice_part/where/c1/eq/l` | Arrow error: Cast error: value of -3000000000 is out of range Int32 |
| cells | false | `dec38_int_overflow/ice_part/where/c1/ns/r` | Arrow error: Cast error: value of -3000000000 is out of range Int32 |
| cells | false | `dec_prec_overflow/view/where/c0/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c0/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c0/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c0/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c1/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c1/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c1/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c1/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c2/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c2/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c2/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c2/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c3/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c3/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c3/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/c3/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/in` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/where/between` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/case/eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/having/eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/join/eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c0/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c0/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c0/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c1/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c1/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/c1/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/in` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/where/between` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/parquet/join/eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/ice_files/where/c0/eq/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/c0/eq/l` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/c0/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/c1/eq/r` | Arrow error: Invalid argument error: 123.45 is too large to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/c1/eq/l` | Arrow error: Invalid argument error: 123.45 is too large to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/c1/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/in` | Arrow error: Invalid argument error: 123.45 is too large to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/where/between` | Arrow error: Invalid argument error: 123.45 is too large to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_files/join/eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c0/eq/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c0/eq/l` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c0/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c1/eq/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c1/eq/l` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/c1/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/where/in` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/ice_part/where/between` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/join/eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/view/df/col_eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/df/lit_eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/df/isin` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/df/between` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/df/fexpr_eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/view/df/filter_str_eq` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_prec_overflow/ice_part/df/col_eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/df/lit_eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/df/isin` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/df/between` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/df/fexpr_eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_prec_overflow/ice_part/df/filter_str_eq` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_round_overflow/view/where/c1/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c1/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c1/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c1/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c2/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c2/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c2/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/view/where/c2/ns/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/parquet/where/c1/eq/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/parquet/where/c1/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/parquet/where/c1/ns/r` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/ice_files/where/c1/eq/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_round_overflow/ice_files/where/c1/eq/l` | Arrow error: Invalid argument error: 10.00 is too large to store in a Decimal128 of precis |
| cells | false | `dec_round_overflow/ice_files/where/c1/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_round_overflow/ice_part/where/c1/eq/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_round_overflow/ice_part/where/c1/eq/l` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `dec_round_overflow/ice_part/where/c1/ns/r` | Arrow error: Invalid argument error: -10.00 is too small to store in a Decimal128 of preci |
| cells | false | `bigint_int/view/where/c1/eq/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c1/eq/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c1/ne/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c1/ne/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c1/ns/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c1/ns/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/parquet/where/c1/eq/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/parquet/where/c1/eq/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/parquet/where/c1/ns/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/ice_files/where/c1/eq/r` | Arrow error: Cast error: Can't cast value -2147483649 to type Int32 |
| cells | false | `bigint_int/ice_files/where/c1/eq/l` | Arrow error: Cast error: Can't cast value -9223372036854775808 to type Int32 |
| cells | false | `bigint_int/ice_files/where/c1/ns/r` | Arrow error: Cast error: Can't cast value -9223372036854775808 to type Int32 |
| cells | false | `bigint_int/ice_part/where/c1/eq/r` | Arrow error: Cast error: Can't cast value -2147483649 to type Int32 |
| cells | false | `bigint_int/ice_part/where/c1/eq/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/ice_part/where/c1/ns/r` | Arrow error: Cast error: Can't cast value -2147483649 to type Int32 |
| cells | false | `int_smallint/view/where/c2/eq/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c2/eq/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c2/ne/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c2/ne/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c2/ns/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c2/ns/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| critic-reads | false | `b02` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| critic-reads | false | `i01` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| critic-reads | false | `i03` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |

#### WRONG->REFUSE (Q4), cell by cell

| set | ANSI | cell | head's refusal |
|---|---|---|---|
| cells | false | `dec_tinyint_overflow/view/where/c1/lt/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/where/c1/gt/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/view/df/col_gt` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | false | `dec_tinyint_overflow/ice_part/df/col_gt` | Arrow error: Cast error: value of -129 is out of range Int8 |
| cells | false | `bigint_int/view/where/c2/lt/l` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `bigint_int/view/where/c2/gt/r` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | false | `int_smallint/view/where/c1/lt/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | false | `int_smallint/view/where/c1/gt/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | false | `q4/tinyint/gt_top` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | false | `q4/tinyint/top_lt` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | false | `q4/tinyint/lt_bottom` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | false | `q4/smallint/gt_top` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | false | `q4/smallint/top_lt` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | false | `q4/smallint/lt_bottom` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | false | `q4/int/gt_top` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| shapes | false | `q4/int/top_lt` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| shapes | false | `q4/int/lt_bottom` | Arrow error: Cast error: Can't cast value 2147483648 to type Int32 |
| cells | true | `dec_tinyint_overflow/view/where/c1/lt/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | true | `dec_tinyint_overflow/view/where/c1/gt/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | true | `dec_tinyint_overflow/view/df/col_gt` | Arrow error: Cast error: value of 128 is out of range Int8 |
| cells | true | `dec_tinyint_overflow/ice_part/df/col_gt` | Arrow error: Cast error: value of -129 is out of range Int8 |
| cells | true | `int_smallint/view/where/c1/lt/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| cells | true | `int_smallint/view/where/c1/gt/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | true | `q4/tinyint/gt_top` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | true | `q4/tinyint/top_lt` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | true | `q4/tinyint/lt_bottom` | Arrow error: Cast error: value of 128 is out of range Int8 |
| shapes | true | `q4/smallint/gt_top` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | true | `q4/smallint/top_lt` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| shapes | true | `q4/smallint/lt_bottom` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| native | native | `dec_tinyint_overflow/native_view/where/c1/lt/l` | Arrow error: Cast error: value of 128 is out of range Int8 |
| native | native | `dec_tinyint_overflow/native_view/where/c1/gt/r` | Arrow error: Cast error: value of 128 is out of range Int8 |
| native | native | `int_smallint/native_view/where/c1/lt/l` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |
| native | native | `int_smallint/native_view/where/c1/gt/r` | Arrow error: Cast error: Can't cast value 32768 to type Int16 |

#### WRONG->REFUSE (Q2), by kind (every cell in `raw/nonequal.tsv`)

| set | ANSI | kind | cells |
|---|---|---|---|
| cells | false | bigint_int | 137 |
| cells | false | dec38_int_overflow | 101 |
| cells | false | dec_prec_overflow | 70 |
| cells | false | dec_round_overflow | 95 |
| cells | false | dec_tinyint_overflow | 106 |
| cells | false | int_smallint | 108 |
| critic-dml | false | critic-dml | 16 |
| critic-reads | false | critic-reads | 6 |
| shapes | false | shapes | 9 |

#### REFUSE->REFUSE (unchanged refusal), by kind (every cell in `raw/nonequal.tsv`)

| set | ANSI | kind | cells |
|---|---|---|---|
| cells | false | dec_round_overflow | 4 |
| cells | false | double_dec_overflow | 116 |
| cells | false | double_int_edge | 116 |
| cells | false | str_date | 126 |
| cells | false | str_dec | 116 |
| cells | false | str_int | 154 |
| cells | false | str_int_invalid | 102 |
| cells | true | str_date | 126 |
| cells | true | str_dec | 116 |
| critic-shapes | false | critic-shapes | 4 |
| critic-shapes | true | critic-shapes | 4 |
| native | native | str_date | 51 |
| native | native | str_dec | 51 |
| native | native | str_double | 51 |
| shapes | false | shapes | 6 |
| shapes | true | shapes | 6 |

#### EXCLUDED (step 0's datetime-literal artifact or plan text), by kind (every cell in `raw/nonequal.tsv`)

| set | ANSI | kind | cells |
|---|---|---|---|
| cells | false | date_ts | 10 |
| cells | true | date_ts | 10 |
| critic-reads | false | critic-reads | 1 |
| critic-reads | true | critic-reads | 1 |
| shapes | false | shapes | 1 |
| shapes | true | shapes | 1 |


Up: [map.md](map.md).
