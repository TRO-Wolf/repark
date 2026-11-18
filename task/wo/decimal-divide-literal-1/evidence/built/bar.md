# DECIMAL-DIVIDE-LITERAL-1 — the bar (built 2026-11-18)

Head: package `pkg-ddl-a30a8d60` (`BUILT_FROM` `a30a8d60`, the product commit, built from a clean worktree). Main:
`pkg-avg-27d6149f` (main `cd3d9ac3`'s product tree). Spark: PySpark 4.1.2, `local[1]`, the session zone UTC, Iceberg
1.11.0. Every run printed `repark._native.__file__` and named the package it meant. Raw data: [raw/](raw/map.md).

## 1. Step 0's whole domain table, re-run on the head

Step 0's probes (`../probes/`: `run_door.py` for `base`, `apl_false`, `pmp_false` in both modes, `run_revision.py` in
both modes, `run_native.py`, and `run_extra.py` ANSI on), compacted with step 0's `compact.py`, compared with step 0's
`classify` against step 0's committed Spark cells. Main's cells are AVG-DECIMAL-ROUND-1's head cells
(`../../../avg-decimal-round-1/evidence/built/raw/head/`, the same package as main's). One row per cell, run and mode:
7,258 comparisons, plus 9 `run_extra` cells.

| main | head | cells |
|---|---|---|
| BOTH_RAISE | BOTH_RAISE | 201 |
| EQUAL | EQUAL | 4807 |
| FORMAT | FORMAT | 4 |
| READ_ERR | READ_ERR | 1 |
| REPARK_RAISES | EQUAL | 4 |
| REPARK_RAISES | REPARK_RAISES | 367 |
| SPARK_RAISES | BOTH_RAISE | 26 |
| SPARK_RAISES | SPARK_RAISES | 59 |
| TYPE | EQUAL | 374 |
| TYPE | TYPE | 94 |
| VALUE | EQUAL | 710 |
| VALUE | TYPE | 12 |
| VALUE | VALUE | 599 |

Head verdicts: `EQUAL` 5,895 (main 4,807), `VALUE` 599 (1,321), `TYPE` 106 (468), `REPARK_RAISES` 367 (371),
`BOTH_RAISE` 227 (201), `SPARK_RAISES` 59 (85), `FORMAT` 4, `READ_ERR` 1. `run_extra`: `extra/mul_smallint_literal`
and `extra/mul_bigint_literal` TYPE → EQUAL; the other seven unchanged.

- **EQUAL → non-EQUAL: 0** (no EQUAL → WRONG, no EQUAL → RAISE).
- **Protected cells moved: 0.** The native door (440 comparisons), every `avg`, `mean`, `sum` and `avgctl` cell other
  than `shape/avg_div` and `shape/mean_div`, every `try_*` cell, the typed operands (`div/typed/*`: `d / bi`,
  `d / bl`, `d / bs`, `d / by`, `CAST(3 AS INT)`, `CAST(3 AS BIGINT)`) and the folded divisor (`shape/fold_div`,
  `moddf/F.expr_fold`: `d / (1 + 2)`) answer exactly as main does.
- **1,088 cells became EQUAL**, by family: `div/lit_right` 176, `div/lit_left` 234, `div/where_round` 24,
  `div/case_round` 24, `div/where_lit` 4, `div/case_lit` 4, `div/literal_only` 4, `divctl/mod` 40, `divctl/mul` 28,
  `divctl/add` 2, `divctl/sub` 2, `divdf/col_div_int` 52, `divdf/col_div_lit` 52, `divdf/col_div_intcol` 48,
  `divdf/int_rdiv_col` 56, `divdf/where_round` 46, `divdf/F.expr` 28, `divdf/selectExpr` 28, `divdf/col_mul_int` 14,
  `hdiv/derived`, `hdiv/cte`, `hdiv/tempview`, `hdiv/flat` 2 each, `ice/ctas_div` 4, `ice/insert_div` 4,
  `ice/update_div` 8, `ice/merge_div` 8, `ice/where_div` 4, `mod/mod`, `mod/pmod`, `mod/pmod_neg`, `mod/pct`,
  `moddf/pct`, `moddf/F.pmod`, `moddf/F.expr_mod`, `moddf/F.expr_pmod` 20 each, `shape/avg_div` 13,
  `shape/mean_div` 13. Every cell, with both verdicts: `raw/compare.jsonl`.
- **The division wrong writes are fixed**: CTAS `d10_2`, `d28_2` (Spark's stored types `decimal(14,6)`,
  `decimal(32,6)`), `INSERT`, `UPDATE` and `MERGE` (copy-on-write and merge-on-read) store Spark's rows
  (`0.06 / 11` stores `0.00546`), both modes.
- **Integer by integer and float division** answer as main does on both doors (`df/guard/*` and `sql/guard/*` in the
  pin file, and the `divctl` controls).

### Cells that moved without reaching EQUAL

- `SPARK_RAISES` → `BOTH_RAISE` (26): the DataFrame `/` at `d38_0` and `d38_2`, row 10, ANSI on
  (`divdf/col_div_int`, `col_div_lit`, `col_div_intcol`, `where_round`, three confs each, and
  `moddf/round_col_div`): RePark's `double` answered `3.33e+35`; it now raises Spark's
  `NUMERIC_VALUE_OUT_OF_RANGE` as Spark does.
- `VALUE` → `TYPE` (12) and `TYPE` → `TYPE` (20): `round(d / 3, 2)` (`round/round_div`, `moddf/round_col_div`): the
  quotient is now Spark's; the `round()` type is DECIMAL-ROUND-RESULT-TYPE-1's (S3).
- `TYPE` → `TYPE` (2): `hdiv/sessview` (below).
- `BOTH_RAISE` → `BOTH_RAISE` (2): `shape/avg_div/d34_4` and `shape/mean_div/d34_4`, ANSI on, both raise the overflow.
- `allowPrecisionLoss=false` (`apl_false`) cells move within their non-EQUAL verdicts (Spark's lossless formula;
  DECIMAL-PRECISION-LOSS-CONF-1, S2).

### Head non-EQUAL cells, by named class (1,363 = 7,258 − 5,895)

| class | cells | verdicts |
|---|---|---|
| native door (NATIVE-DECIMAL-AVG-DIVIDE-1, S2): unchanged from main | 440 | VALUE 315, REPARK_RAISES 72, TYPE 34, BOTH_RAISE 18, SPARK_RAISES 1 |
| `try_*` coercion (TRY-ARITH-DECIMAL-INT-1, S1): unchanged | 236 | REPARK_RAISES 124, VALUE 96, TYPE 16 |
| `allowPrecisionLoss=false` (DECIMAL-PRECISION-LOSS-CONF-1, S2) | 230 | VALUE 142, SPARK_RAISES 58, REPARK_RAISES 30 |
| both raise: the 38-digit row 10 with ANSI on, the same condition | 209 | BOTH_RAISE 209 |
| `round()` type (DECIMAL-ROUND-RESULT-TYPE-1, S3) | 54 | TYPE 54 |
| `avg(DISTINCT x)`, `mean(DISTINCT x)` beside `count(x)` (AVG-DISTINCT-MIXED-1, S3) | 52 | REPARK_RAISES 52 |
| a bare aggregate in `select(F.expr(…))` (FEXPR-COUNT-STAR-AGG-1, S3) | 52 | REPARK_RAISES 52 |
| `avg(c) ± 1`, `avg(c) * 2` at p ≥ 34 (AVG-DECIMAL-ROUND-1's class, the `+ − *` type past 38 digits) | 32 | VALUE 32 |
| the 38-digit multiplication: `d38_10 * 3` (both modes) and `d38_0 * 3`, `d38_2 * 3`, `avg(d38_* * 2)` with ANSI off raise Arrow's overflow (the `d38_10 * 3` S2 card) | 31 | REPARK_RAISES 29, VALUE 2 |
| `describe` / `summary` (not this family) | 12 | REPARK_RAISES 8, FORMAT 4 |
| `WHERE` / `CASE` on `d38_10 / 3 = round(d38_10 / 3, 6)` (the `round()` seat; the DataFrame cell now answers as SQL) | 12 | VALUE 12 |
| `hdiv/sessview`: a session-catalog view over an Iceberg table | 2 | TYPE 2 |
| `ice/where_div/d38_2`, ANSI on: both raise `NUMERIC_VALUE_OUT_OF_RANGE`, Spark's with `.WITH_SUGGESTION` | 1 | READ_ERR 1 |

- **`hdiv/sessview`** (`SELECT h / 2 FROM <session view of d10_2 / 2 over an Iceberg table>`): the literal now
  takes its digits (EXPLAIN: `h / Decimal128(Some(2),1,0)`, and `h` is Spark's `decimal(14,6)`), but the outer `/`
  over the session-catalog view is not wrapped in the division UDF, so Arrow's own decimal division types it
  `decimal(18,10)` (main `decimal(25,17)`, Spark `decimal(16,8)`); values equal. A seat outside this unit's cap
  (the session-catalog view's re-analysis path); **no card yet** — proposed to the orchestrator.

## 2. The pins (`python/repark/tests/types/test_decimal_divide_literal_1.py`)

81 cells per ANSI mode against `decimal_divide_literal_1_spark_oracle.json` (recorded from Spark 4.1.2 by
[scripts/record_oracle.py](scripts/record_oracle.py) with the pin file's own cells module):
`test_a_cell_answers_as_spark` (74 per mode) and `test_a_pick_minimum_precision_false_cell_answers_as_spark` (7 per
mode, a session built with `spark.sql.legacy.literal.pickMinimumPrecision=false`).

| package | passed | failed |
|---|---|---|
| head `pkg-ddl-a30a8d60` | 162 | 0 |
| main `pkg-avg-27d6149f` | 44 | 118 |

Main's 44 green cells are the guards and the `false`-conf cells where main already agrees (`d / 3`, `3 / d28_2`,
`d % 3`, `d * 3L`, which main types by width as Spark then does): `sql/guard/*` (`d / (1 + 2)`, `d / bi`, `i / j`,
`i / 3`, `x / 3`, `d / CAST(3 AS BIGINT)`, `d / CAST(3 AS DECIMAL(20,0))`, `d * CAST(3 AS SMALLINT)`, `d / 3Y`,
`mod(i, 3)`, `x % 3`, `SELECT 3L, 3S, -3L`) and `df/guard/*` (`col(i) / col(j)`, `col(i) / 3`, `col(x) / 3`,
`col(d) / col(x)`, `col(s) / 2`, `F.expr("d / (1 + 2)")`), both modes. Every other cell is red on main in both modes:
`/` with the literal on the right (`d10_2`, `d28_2`, `d10_0`; `3`, `100`, `-3`, `11`) and the left; `%` both sides;
the suffixed `*`, `-`, `/`, `%`; `mod`, `pmod`, `mod(3, d)`, `mod(d, 0.7)`, `mod(d, bi)`; the DataFrame `/` (literal,
`F.lit`, `INT` column, left literal, `d28_2`, decimal column, nested), `%`, `F.pmod`, `F.expr`, `selectExpr`; the four
`h / 2` scopes; `avg(d / 3)` at `d10_2` and `d28_2`; `WHERE` and `CASE`; the Iceberg CTAS (`d10_2`, `d28_2`),
`INSERT` (`d10_2`, `d28_2`), `UPDATE` and `MERGE` (copy-on-write and merge-on-read); and the `false` conf's `d * 3`,
`mod(d, 3)` and `col(d) / 3`.

Rust: `crates/repark-functions/src/decimal_precision/literal_step_tests.rs` (9 tests: the literal widths of `/`, `%`,
`mod`, `pmod`, the folded and cast divisors, the `false` conf, a second analyze, the DataFrame marker, the suffix
spelling).

## 3. Earlier units' pin files and the facade suite

The full facade suite on the first product commit (`02a79edf`, release build, `-n 6`): 35,321 passed, 9 failed.
- Four were a one-line nullability change of the suffix marker in that commit (`SELECT 1e200D` became nullable:
  `test_fnp_4b_literals.py` ×3, `test_java_double_fd_1.py`, and Rust `spark_dialect::suffix_literals_above_decimal38_stay_nonnull_at_analysis`);
  the line was reverted in `a30a8d60`, so the marker in arithmetic is nullable as the other suffix markers are.
- Five asserted main's wrong answer and are **re-pinned to Spark's**: `dataframe/test_columns.py::test_division_is_float`
  (`7.0 / 2.0` as DataFrame columns is `decimal(8,6)` `3.500000`, as `test_sql_float_literal_division_is_decimal`
  already pins for SQL; the integer pair stays `double`), and the four `dec-div0` cells of
  `writes/cast_overflow_insert_1_spark_oracle.json` (`1/0.0` into a `BIGINT`: RePark's
  `CAST_OVERFLOW_IN_TABLE_INSERT` text now names Spark's `DECIMAL(8,6)`, so the RePark-only `DECIMAL(17,6)` head is
  dropped).
- On `a30a8d60` those files, this unit's pins and AVG-DECIMAL-ROUND-1's pins pass (809 passed). DECIMAL-COMMON-TYPE-1,
  CAST-LITERAL-UNWRAP-1, DECIMAL-DOUBLE, the signed-zero files and the INTDIV-1 pins passed in the full run.

Up: [map.md](map.md).
