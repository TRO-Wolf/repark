# DECIMAL-DIVIDE-LITERAL-1 — the bar (built 2026-11-18)

Head: package `pkg-ddl-a30a8d60` (`BUILT_FROM` `a30a8d60`, the product commit, built from a clean worktree). Main:
`pkg-avg-27d6149f` (main `cd3d9ac3`'s product tree). Spark: PySpark 4.1.2, `local[1]`, the session zone UTC, Iceberg
1.11.0. Every run printed `repark._native.__file__` and named the package it meant. Raw data: [raw/](raw/map.md).

> **Delta 1 (2026-11-18).** The delta's head is package `pkg-ddl-8efe8e69` (`BUILT_FROM` `8efe8e69`, product commit
> `8efe8e69` over `a30a8d60`): a decimal `mod()` takes the `%` path. It carries Fable's guidance under the owner's
> overnight delegation (`claude-fable-5-2`, 2026-11-18; not an owner ruling). Section 0 is new; sections 1 to 3 are
> the build's, and re-ran unchanged on the delta's head (marked where the delta changed a count).

## 0. Delta 1: `mod()`, `pmod()` and the zero divisor

### 0.1 Spark first (step 1)

[scripts/remainder_probe.py](scripts/remainder_probe.py), the same cells on Spark 4.1.2, main `pkg-avg-27d6149f`, the
build's head `pkg-ddl-a30a8d60` and the delta's head `pkg-ddl-8efe8e69`, both ANSI modes: Fable's 16 expressions over
`7.25`, `-7.25`, NULL; `mod`, `%`, `pmod` with a zero divisor (literal `0`, `CAST(0 AS DECIMAL(5,2))`, a
`DECIMAL(5,2)` zero column) and with negative operands (`3`, `-3`, `-0.7`, `-3` on the left, `3L`) at `d10_2`,
`d28_2` and `d38_10` over seven rows (`±7.25`, NULL, `±6.00`, and a wide `±` value at each precision); integer
`mod`/`pmod` by zero; and on the DataFrame door `F.expr("mod(…)")`, `F.expr("pmod(…)")`, `F.pmod`, `col % n` and
`selectExpr`. 129 cells per mode. Raw: [raw/delta1/](raw/delta1/) (`rem-*.jsonl`); table:
[scripts/remainder_table.py](scripts/remainder_table.py).

| Spark 4.1.2 | ANSI on | ANSI off |
|---|---|---|
| `mod(d, 0)`, `d % 0`, `pmod(d, 0)`, `mod(d, CAST(0 AS DECIMAL(5,2)))`, a zero column; SQL, `F.expr`, `selectExpr`, `F.pmod`, `col % 0` | raise `[REMAINDER_BY_ZERO]` | NULL, Spark's `Remainder`/`Pmod` type |
| integer `mod(i, 0)`, `i % 0`, `pmod(i, 0)` | raise `[REMAINDER_BY_ZERO]` | NULL, `int` |
| `mod(d, ±3)`, `d % ±3` (negative dividend, divisor, both) | the dividend's sign: `7.25 → 1.25`, `-7.25 → -1.25`; `decimal(3,2)` at `d10_2` and `d28_2`, `decimal(11,10)` at `d38_10` | the same |
| `pmod(d, 3)` | `-7.25 → 1.75` | the same |
| `pmod(d, -3)` | `7.25 → 1.25`, `-7.25 → -1.25` (`r = a % n; r < 0 ? (r + n) % n : r`) | the same |
| `pmod(-3, d)`, the wide row (`12345675.91` does not fit `decimal(3,2)`) | raise `[NUMERIC_VALUE_OUT_OF_RANGE]` | NULL for that row |

Spark's `mod` is `Remainder`, the same expression as `%`, and raises with ANSI on. **Step 2's condition holds:** the
remedy is built.

### 0.2 What the delta built (step 2) and did not build (step 3)

- **`mod()`:** `literal_step.rs` `remainder_node` rewrites a decimal `mod()` call (one whose operands are decimals or
  integers, at least one decimal) onto the `%` operator (`remainder_operator`, `remainder_decimal_operand`), with an
  integer argument cast to its `forType` width; it keeps `%`'s `Remainder` type and every non-zero answer, and takes
  `SparkExprSemantics`' zero-divisor guard. An integer `mod()` and a `mod()` with a float keep `SparkMod`.
- **`pmod()`: not rewritten.** The only allowed target, the spelling `F.pmod` uses (`((a % n) + n) % n`), was measured
  against Spark: it raises for a zero divisor with ANSI on (as Spark does, `[DIVIDE_BY_ZERO]` for
  `[REMAINDER_BY_ZERO]`), but answers a negative divisor unlike Spark (`F.pmod(7.25, -3)` is `-1.75`, Spark `1.25`;
  6 cells below), where the SQL `pmod(d, -3)` answers as Spark. A rewrite would move `mod/pmod_neg` (EQUAL on the
  build's head) to a wrong value, so it is not a target whatever the cost row says (cost.md).

### 0.3 The delta's moves: the class "NULL → RAISE, Spark's answer"

Every cell of the 129 × 2 that moved from the build's head to the delta's head
([scripts/delta_moves.py](scripts/delta_moves.py)) is a decimal `mod()` with a zero divisor, ANSI on: it answered NULL
and now raises, as Spark does. RePark's condition is `[DIVIDE_BY_ZERO]` where Spark's is `[REMAINDER_BY_ZERO]` (the
same as `d % 0` on main; proposed card 4a below). Nothing else moved, in either mode.

| cell (ANSI on) | build's head `a30a8d60` | delta's head `8efe8e69` | Spark |
|---|---|---|---|
| `fable/mod(d, 0)` | `decimal(3,2)` NULL ×3 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `fable/mod(d, CAST(0 AS DECIMAL(5,2)))` | `decimal(5,2)` NULL ×3 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod/d10_2`, `sql/zero/mod/d28_2` | `decimal(3,2)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod/d38_10` | `decimal(11,10)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod_dec/d10_2`, `sql/zero/mod_dec/d28_2` | `decimal(5,2)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod_dec/d38_10` | `decimal(13,10)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod_col/d10_2`, `sql/zero/mod_col/d28_2` | `decimal(5,2)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `sql/zero/mod_col/d38_10` | `decimal(13,10)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `df/expr_mod_zero/d10_2`, `df/expr_mod_zero/d28_2` (`F.expr`) | `decimal(3,2)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `df/expr_mod_zero/d38_10` | `decimal(11,10)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `df/selectExpr_mod_zero/d10_2`, `df/selectExpr_mod_zero/d28_2` | `decimal(3,2)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |
| `df/selectExpr_mod_zero/d38_10` | `decimal(11,10)` NULL ×7 | RAISE `DIVIDE_BY_ZERO` | RAISE `REMAINDER_BY_ZERO` |

17 cells in the probe; the pin file adds 5 more of the class (ANSI on): `sql/call/mod_zero` (`mod(d10_2, 0)`),
`sql/call/mod_zero_decimal`, `sql/call/mod_zero_column` (`mod(d10_2, bi - 3)`), `df/expr_mod_zero` and
`pmp/call_mod_zero` (the `pickMinimumPrecision=false` session). Main answered NULL in all 22. With ANSI off the same
cells answer NULL with Spark's type on both heads, as Spark does.

The delta's head against Spark over the probe: ANSI on, EQUAL 71, both raise 38 (18 `%`/`F.pmod` by zero with the
class name, as on main; the 17 above; 3 `pmod(-3, d)` overflows), Spark raises 17 (proposed card 4b), VALUE 3
(`F.pmod` by `-3`); ANSI off, EQUAL 123, VALUE 3 (`F.pmod` by `-3`), RePark raises 3 (`pmod(-3, d)`). Main: ANSI on
EQUAL 3, TYPE 68; ANSI off EQUAL 15, TYPE 108 (the rest as listed under the cards).

### 0.4 Step 0's bar on the delta's head (step 6)

The build's bar (section 1) re-ran in full on `pkg-ddl-8efe8e69`: **every one of its 7,035 raw cells answers exactly
as on the build's head** (`raw/head/` against the delta's run, field for field), so the 7,258 comparisons keep
section 1's transitions and counts: 0 EQUAL → WRONG, 0 EQUAL → RAISE, 0 protected cells moved. Step 0's probes have no
zero-divisor cell. The class table, reproduced by [scripts/ddl_classes.py](scripts/ddl_classes.py) for both heads
(identical output): [raw/delta1/bar-classes.txt](raw/delta1/bar-classes.txt).

### 0.5 Step 5: a CTAS from a session-catalog view (`hdiv/sessview`)

[scripts/sessview_ctas.py](scripts/sessview_ctas.py): an Iceberg table `lc.ns.vt` of `d10_2`, the view
`CREATE VIEW default.sv_h AS SELECT id, d10_2 / 2 AS h FROM lc.ns.vt`, then
`CREATE TABLE lc.ns.ctas_sessview USING iceberg AS SELECT id, h / 2 AS v FROM default.sv_h`; the stored type is read
from the table's current `metadata.json` schema. A temp view over the same table is the control. Both ANSI modes
answer alike. Raw: `raw/delta1/ctas-*.jsonl`.

| CTAS of `h / 2` from | Spark 4.1.2 | main `pkg-avg-27d6149f` | delta's head `pkg-ddl-8efe8e69` |
|---|---|---|---|
| the session-catalog view | `decimal(16, 8)`: `1.81250000`, `-1.81250000`, NULL, `0.01500000`, `3086419.72750000` | `decimal(25,17)` | `decimal(18,10)` |
| a temp view (control) | `decimal(16, 8)` | `decimal(32,24)` | `decimal(16,8)` (equal to Spark) |

The values are equal at each stored scale (`1.8125000000` on the head); the stored column type is wrong on main and on
the head.

### 0.6 Proposed cards (for the orchestrator's clerk; every severity is a proposal for the owner)

1. **SESSION-VIEW-DECIMAL-DIV-1** (`hdiv/sessview`): the outer `/` over a session-catalog view of an Iceberg table
   misses the division UDF. Cells: `hdiv/sessview` TYPE in both modes (`h / 2` is `decimal(18,10)`, Spark
   `decimal(16,8)`, main `decimal(25,17)`; values equal) and the CTAS cell above (stored `decimal(18,10)` against
   Spark's `decimal(16, 8)`; main `decimal(25,17)`). A2's "`h / 2` scopes" closed at three of the four view-like
   scopes (derived table, CTE, temp view; not the session view), and the flat `d / 2 / 2` agrees. Proposed **S2**; whether it blocks the label is the owner's (Fable's lean:
   S2, not label-blocking if session-catalog views over Iceberg are outside the scoped label).
2. **A written `CAST(3 AS DECIMAL(20,0))` narrowed in `*` and `+`** (U3's default-cast check reads a written
   `DECIMAL(20,0)` cast of an integer literal as the planner's `BIGINT` coercion): `d10_2 * CAST(3 AS DECIMAL(20,0))`
   is `decimal(12,2)` (Spark `decimal(31,2)`), `d10_2 + CAST(3 AS DECIMAL(20,0))` `decimal(11,2)` (Spark
   `decimal(23,2)`); values equal; `/` and a `DECIMAL(10,0)` cast agree; main alike. Raw: `raw/delta1/card-*.jsonl`.
   Proposed **S3**.
3. **A runtime `SET spark.sql.legacy.literal.pickMinimumPrecision=false` is accepted and not read** (it reads back
   `false`; only the builder conf reaches the analyzer; the facade does not route the key to `set_runtime_config`).
   After the `SET`, Spark answers `d10_2 * 3` `decimal(21,2)`, `d10_2 / 3` `decimal(21,13)`, `d10_2 % 3`
   `decimal(10,2)`, `mod(d10_2, 3)` `decimal(10,2)`; main `(12,2)`, `(21,13)`, `(10,2)`, `(12,2)`; the head `(12,2)`,
   `(14,6)`, `(3,2)`, `(3,2)`, values equal, both modes. **`/` and `%` were EQUAL on main and are TYPE on the head**
   (main agreed by never narrowing them): an EQUAL → WRONG cell outside step 0's bar, found by the delta. The fix
   routes the key at runtime (the facade's `builder_conf.py` key list and `session_runtime.rs`
   `apply_runtime_config`): a sixth and seventh product file, outside the cap. Proposed **S2** (the owner's default
   for the other accepted-and-ignored decimal conf, `allowPrecisionLoss`, was S2); the regression is reported as a
   halt in the hand-back, not ruled here.
4. **Zero divisor and `pmod`, left by steps 2 and 3** (all main alike unless stated):
   - a. **The error class of a remainder by zero:** `d % 0`, a decimal `mod(d, 0)`, `F.pmod(d, 0)` and `col % 0`
     raise `[DIVIDE_BY_ZERO] Division by zero. Use try_divide…` where Spark raises `[REMAINDER_BY_ZERO] Remainder by
     zero. Use try_mod…` (ANSI on). Cells: the 18 `%`/`F.pmod` cells (`fable/d % 0`, `fable/d % CAST(0…)`,
     `sql/zero/pct{,_dec,_col}/*`, `sql/int/pct_zero`, `df/F.pmod_zero/*`, `df/pct_zero/*`) and the 22 cells of 0.3.
     The pin file reads the class through `CARDED_CONDITIONS`. Proposed **S3**.
   - b. **ANSI on, `pmod(d, 0)` and an integer `mod`/`pmod` by zero answer NULL** where Spark raises
     `[REMAINDER_BY_ZERO]`: `fable/pmod(d, 0)`, `sql/zero/pmod{,_dec,_col}/{d10_2,d28_2,d38_10}`,
     `df/expr_pmod_zero/{d10_2,d28_2,d38_10}`, `sql/int/{mod_zero,pmod_zero,mod_zero_col,pmod_zero_col}` (17 cells; a
     silent NULL in the label's mode). Proposed **S2**.
   - c. **`F.pmod` with a negative divisor** answers the facade spelling's `((a % n) + n) % n`: `F.pmod(d, F.lit(-3))`
     is `-1.75` for `7.25` and `-2.09` for `12345678.91` where Spark answers `1.25` and `0.91`
     (`df/F.pmod_neg/{d10_2,d28_2,d38_10}`, both modes, 6 cells; a silent wrong value; the SQL `pmod(d, -3)` answers
     as Spark). Proposed **S2**.
   - d. **`pmod()` whose answer overflows Spark's `Pmod` type:** `pmod(-3, d)` at the wide row (`12345675.91` into
     `decimal(3,2)`): Spark NULL (ANSI off) and `[NUMERIC_VALUE_OUT_OF_RANGE]` (ANSI on); the head's cast to the
     `Pmod` type raises Arrow's `Invalid argument error` in both modes; main answered `decimal(12,2)` with the
     value. `sql/neg/pmod_left_neg/{d10_2,d28_2,d38_10}`, 6 cells (ANSI off: main TYPE and VALUE, the head raises; the
     build's cast, not the delta's). Proposed **S3**.

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
| **delta 1:** head `pkg-ddl-8efe8e69` | 182 | 0 |
| **delta 1:** main `pkg-avg-27d6149f` | 45 | 137 |

**Delta 1** re-recorded the oracle from Spark 4.1.2 (every earlier cell's answer unchanged) and added 10 cells per
mode, 91 in all (82 default, 9 `false`-conf): `sql/call/mod_neg` (`mod(d10_2, -3)`), `sql/call/mod_d28_2`,
`sql/call/mod_zero` (`mod(d10_2, 0)`), `sql/call/mod_zero_decimal` (`CAST(0 AS DECIMAL(5,2))`),
`sql/call/mod_zero_column` (`mod(d10_2, bi - 3)`), `sql/mod/zero` (`d10_2 % 0`), `df/expr_mod_neg`,
`df/expr_mod_zero`, `pmp/call_mod_d28_2` (Spark's `decimal(12,2)` with the `INT` width) and `pmp/call_mod_zero`.
With ANSI on, Spark raises `[REMAINDER_BY_ZERO]` for the five zero-divisor `mod` cells and `d10_2 % 0`; the test reads
RePark's `[DIVIDE_BY_ZERO]` for it (`CARDED_CONDITIONS`, proposed card 4a). Every new cell is red on main in both
modes except `d10_2 % 0` with ANSI on (main raises too), the one new green on main; it is red on main with ANSI off
(`decimal(10,2)` against Spark's `decimal(3,2)`). Logs: [raw/delta1/pins/](raw/delta1/pins/).

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
spelling). Delta 1 adds
`a_decimal_mod_call_answers_as_the_remainder_operator` (`mod(d, 3)`, `mod(d, -3)`, `mod(3, d)`, `mod(d, i)` answer
as `%`; `mod(d, 0)` raises `DIVIDE_BY_ZERO` with ANSI on; an integer `mod(i, j)` stays `int`): 10 tests.

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
