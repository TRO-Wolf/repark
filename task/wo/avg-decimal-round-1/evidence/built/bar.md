# AVG-DECIMAL-ROUND-1 — the bar (built 2026-11-17)

Head: package `pkg-avg-27d6149f` (`BUILT_FROM` `27d6149f`, the product commit, built from a clean worktree). Main: `pkg-dct-7bf32859` (main's product tree; `03d1ea8b` differs from it only in documents). Spark: PySpark 4.1.2, `local[1]`, the session zone UTC. Every run printed `repark._native.__file__` and named the package it meant. Raw data in [raw/](raw/).

## 1. Step 0's whole domain table, re-run on the head

Step 0's probes (`evidence/probes/`: `run_door.py` for `base`, `apl_false`, `pmp_false` in both modes, `run_revision.py` in both modes, `run_native.py`), compacted with step 0's `compact.py`, compared with step 0's committed main and Spark cells with step 0's `classify`. One row per cell, run and mode: 7,258 comparisons.

| main | head | cells |
|---|---|---|
| BOTH_RAISE | BOTH_RAISE | 201 |
| EQUAL | EQUAL | 2416 |
| READ_ERR | READ_ERR | 1 |
| REPARK_RAISES | EQUAL | 109 |
| REPARK_RAISES | REPARK_RAISES | 345 |
| REPARK_RAISES | TYPE | 2 |
| REPARK_RAISES | VALUE | 4 |
| SPARK_RAISES | SPARK_RAISES | 85 |
| TYPE | TYPE | 466 |
| VALUE | EQUAL | 2282 |
| VALUE | FORMAT | 4 |
| VALUE | REPARK_RAISES | 26 |
| VALUE | VALUE | 1317 |

- **EQUAL → non-EQUAL: 0.** No answer of another family moved from main (division, `%`, `mod`/`pmod`, `round`, `try_*`, the `h / 2` scopes, controls: 0 moved). **Native door: 0 answers moved** (440 native comparisons, every one equal to main's).
- 2,391 cells became EQUAL (2,282 from VALUE, 109 from REPARK_RAISES).

### Head non-EQUAL cells, by named class

| class | cells | verdicts |
|---|---|---|
| division and other families (DECIMAL-DIVIDE-LITERAL-1, TRY-ARITH-DECIMAL-INT-1, DECIMAL-ROUND-RESULT-TYPE-1, DECIMAL-PRECISION-LOSS-CONF-1, …): unchanged from main | 1641 | READ_ERR 1, REPARK_RAISES 170, SPARK_RAISES 84, TYPE 432, VALUE 954 |
| native door (NATIVE-DECIMAL-AVG-DIVIDE-1, S2): unchanged from main | 440 | BOTH_RAISE 18, REPARK_RAISES 72, SPARK_RAISES 1, TYPE 34, VALUE 315 |
| both raise with ANSI on, the same condition (`ARITHMETIC_OVERFLOW` / Spark's own) | 170 | BOTH_RAISE 170 |
| a bare aggregate in `select(F.expr(…))` refuses (FEXPR-COUNT-STAR-AGG-1, S3) | 52 | REPARK_RAISES 52 |
| `avg(d / 3)`, `mean(d / 3)`: RePark's wider quotient (DECIMAL-DIVIDE-LITERAL-1, ruling 4; not pinned) | 34 | BOTH_RAISE 8, REPARK_RAISES 4, TYPE 2, VALUE 20 |
| `avg(c) - 1`, `avg(c) * 2` and `avg(c) * count(c)` at p ≥ 34: the `+`/`-`/`*` result type past 38 digits (with the `d38_10 * 3` card's family); the `avg` operand itself agrees | 32 | VALUE 32 |
| `avg(DISTINCT x), count(x)` refuses (AVG-DISTINCT-MIXED-1, S3; ruling 2) | 26 | REPARK_RAISES 26 |
| `mean(DISTINCT x), count(x)` (decimal) WRONG → RAISE, the ruled outcome (ruling 2) | 26 | REPARK_RAISES 26 |
| the `d38_10 * 3` overflow (its S2 card): `avg(d38_* * 2)` and `divctl/mul/d38_10` | 18 | BOTH_RAISE 5, REPARK_RAISES 13 |
| `summary()` without percentiles refuses (not this family; main alike) | 8 | REPARK_RAISES 8 |
| `describe`/`summary` of `c38_18`: Spark prints the mean as `4.21667E-17` (not the averager; VALUE → FORMAT, the digits now agree) | 4 | FORMAT 4 |

Every non-EQUAL cell of this unit's family is in a named class; the cells, cell for cell:

- **`mean(DISTINCT x), count(x)` (decimal) WRONG → RAISE, the ruled outcome (ruling 2)** (26): `mixed/mean_distinct_count/c10_0 (rev, ANSI false)`; `mixed/mean_distinct_count/c10_0 (rev, ANSI true)`; `mixed/mean_distinct_count/c10_2 (rev, ANSI false)`; `mixed/mean_distinct_count/c10_2 (rev, ANSI true)`; `mixed/mean_distinct_count/c10_6 (rev, ANSI false)`; `mixed/mean_distinct_count/c10_6 (rev, ANSI true)`; `mixed/mean_distinct_count/c28_0 (rev, ANSI false)`; `mixed/mean_distinct_count/c28_0 (rev, ANSI true)`; `mixed/mean_distinct_count/c28_10 (rev, ANSI false)`; `mixed/mean_distinct_count/c28_10 (rev, ANSI true)`; `mixed/mean_distinct_count/c28_2 (rev, ANSI false)`; `mixed/mean_distinct_count/c28_2 (rev, ANSI true)`; `mixed/mean_distinct_count/c34_0 (rev, ANSI false)`; `mixed/mean_distinct_count/c34_0 (rev, ANSI true)`; `mixed/mean_distinct_count/c34_30 (rev, ANSI false)`; `mixed/mean_distinct_count/c34_30 (rev, ANSI true)`; `mixed/mean_distinct_count/c34_4 (rev, ANSI false)`; `mixed/mean_distinct_count/c34_4 (rev, ANSI true)`; `mixed/mean_distinct_count/c38_0 (rev, ANSI false)`; `mixed/mean_distinct_count/c38_0 (rev, ANSI true)`; `mixed/mean_distinct_count/c38_18 (rev, ANSI false)`; `mixed/mean_distinct_count/c38_18 (rev, ANSI true)`; `mixed/mean_distinct_count/c38_2 (rev, ANSI false)`; `mixed/mean_distinct_count/c38_2 (rev, ANSI true)`; `mixed/mean_distinct_count/c38_36 (rev, ANSI false)`; `mixed/mean_distinct_count/c38_36 (rev, ANSI true)`.
- **`avg(DISTINCT x), count(x)` refuses (AVG-DISTINCT-MIXED-1, S3; ruling 2)** (26): `mixed/avg_distinct_count/c10_0 (rev, ANSI false)`; `mixed/avg_distinct_count/c10_0 (rev, ANSI true)`; `mixed/avg_distinct_count/c10_2 (rev, ANSI false)`; `mixed/avg_distinct_count/c10_2 (rev, ANSI true)`; `mixed/avg_distinct_count/c10_6 (rev, ANSI false)`; `mixed/avg_distinct_count/c10_6 (rev, ANSI true)`; `mixed/avg_distinct_count/c28_0 (rev, ANSI false)`; `mixed/avg_distinct_count/c28_0 (rev, ANSI true)`; `mixed/avg_distinct_count/c28_10 (rev, ANSI false)`; `mixed/avg_distinct_count/c28_10 (rev, ANSI true)`; `mixed/avg_distinct_count/c28_2 (rev, ANSI false)`; `mixed/avg_distinct_count/c28_2 (rev, ANSI true)`; `mixed/avg_distinct_count/c34_0 (rev, ANSI false)`; `mixed/avg_distinct_count/c34_0 (rev, ANSI true)`; `mixed/avg_distinct_count/c34_30 (rev, ANSI false)`; `mixed/avg_distinct_count/c34_30 (rev, ANSI true)`; `mixed/avg_distinct_count/c34_4 (rev, ANSI false)`; `mixed/avg_distinct_count/c34_4 (rev, ANSI true)`; `mixed/avg_distinct_count/c38_0 (rev, ANSI false)`; `mixed/avg_distinct_count/c38_0 (rev, ANSI true)`; `mixed/avg_distinct_count/c38_18 (rev, ANSI false)`; `mixed/avg_distinct_count/c38_18 (rev, ANSI true)`; `mixed/avg_distinct_count/c38_2 (rev, ANSI false)`; `mixed/avg_distinct_count/c38_2 (rev, ANSI true)`; `mixed/avg_distinct_count/c38_36 (rev, ANSI false)`; `mixed/avg_distinct_count/c38_36 (rev, ANSI true)`.
- **`avg(d / 3)`, `mean(d / 3)`: RePark's wider quotient (DECIMAL-DIVIDE-LITERAL-1, ruling 4; not pinned)** (34): `shape/avg_div/d10_0 (rev, ANSI false)`; `shape/avg_div/d10_0 (rev, ANSI true)`; `shape/avg_div/d10_2 (rev, ANSI false)`; `shape/avg_div/d10_2 (rev, ANSI true)`; `shape/avg_div/d18_4 (rev, ANSI false)`; `shape/avg_div/d18_4 (rev, ANSI true)`; `shape/avg_div/d20_2 (rev, ANSI false)`; `shape/avg_div/d20_2 (rev, ANSI true)`; `shape/avg_div/d28_10 (rev, ANSI false)`; `shape/avg_div/d28_10 (rev, ANSI true)`; `shape/avg_div/d28_2 (rev, ANSI false)`; `shape/avg_div/d28_2 (rev, ANSI true)`; `shape/avg_div/d34_4 (rev, ANSI false)`; `shape/avg_div/d34_4 (rev, ANSI true)`; `shape/avg_div/d38_0 (rev, ANSI true)`; `shape/avg_div/d38_10 (rev, ANSI true)`; `shape/avg_div/d38_2 (rev, ANSI true)`; `shape/mean_div/d10_0 (rev, ANSI false)`; `shape/mean_div/d10_0 (rev, ANSI true)`; `shape/mean_div/d10_2 (rev, ANSI false)`; `shape/mean_div/d10_2 (rev, ANSI true)`; `shape/mean_div/d18_4 (rev, ANSI false)`; `shape/mean_div/d18_4 (rev, ANSI true)`; `shape/mean_div/d20_2 (rev, ANSI false)`; `shape/mean_div/d20_2 (rev, ANSI true)`; `shape/mean_div/d28_10 (rev, ANSI false)`; `shape/mean_div/d28_10 (rev, ANSI true)`; `shape/mean_div/d28_2 (rev, ANSI false)`; `shape/mean_div/d28_2 (rev, ANSI true)`; `shape/mean_div/d34_4 (rev, ANSI false)`; `shape/mean_div/d34_4 (rev, ANSI true)`; `shape/mean_div/d38_0 (rev, ANSI true)`; `shape/mean_div/d38_10 (rev, ANSI true)`; `shape/mean_div/d38_2 (rev, ANSI true)`.
- **the `d38_10 * 3` overflow (its S2 card): `avg(d38_* * 2)` and `divctl/mul/d38_10`** (18): `divctl/mul/d38_10/i3 (apl_false, ANSI false)`; `divctl/mul/d38_10/i3 (apl_false, ANSI true)`; `divctl/mul/d38_10/i3 (base, ANSI false)`; `divctl/mul/d38_10/i3 (base, ANSI true)`; `divctl/mul/d38_10/i3 (pmp_false, ANSI false)`; `divctl/mul/d38_10/i3 (pmp_false, ANSI true)`; `divctl/mul/d38_10/neg3 (apl_false, ANSI false)`; `divctl/mul/d38_10/neg3 (apl_false, ANSI true)`; `divctl/mul/d38_10/neg3 (base, ANSI false)`; `divctl/mul/d38_10/neg3 (base, ANSI true)`; `divctl/mul/d38_10/neg3 (pmp_false, ANSI false)`; `divctl/mul/d38_10/neg3 (pmp_false, ANSI true)`; `shape/avg_mul/d38_0 (rev, ANSI false)`; `shape/avg_mul/d38_0 (rev, ANSI true)`; `shape/avg_mul/d38_10 (rev, ANSI false)`; `shape/avg_mul/d38_10 (rev, ANSI true)`; `shape/avg_mul/d38_2 (rev, ANSI false)`; `shape/avg_mul/d38_2 (rev, ANSI true)`.
- **a bare aggregate in `select(F.expr(…))` refuses (FEXPR-COUNT-STAR-AGG-1, S3)** (52): `meandf/select_F.expr_avg/c10_0 (rev, ANSI false)`; `meandf/select_F.expr_avg/c10_0 (rev, ANSI true)`; `meandf/select_F.expr_avg/c10_2 (rev, ANSI false)`; `meandf/select_F.expr_avg/c10_2 (rev, ANSI true)`; `meandf/select_F.expr_avg/c10_6 (rev, ANSI false)`; `meandf/select_F.expr_avg/c10_6 (rev, ANSI true)`; `meandf/select_F.expr_avg/c28_0 (rev, ANSI false)`; `meandf/select_F.expr_avg/c28_0 (rev, ANSI true)`; `meandf/select_F.expr_avg/c28_10 (rev, ANSI false)`; `meandf/select_F.expr_avg/c28_10 (rev, ANSI true)`; `meandf/select_F.expr_avg/c28_2 (rev, ANSI false)`; `meandf/select_F.expr_avg/c28_2 (rev, ANSI true)`; `meandf/select_F.expr_avg/c34_0 (rev, ANSI false)`; `meandf/select_F.expr_avg/c34_0 (rev, ANSI true)`; `meandf/select_F.expr_avg/c34_30 (rev, ANSI false)`; `meandf/select_F.expr_avg/c34_30 (rev, ANSI true)`; `meandf/select_F.expr_avg/c34_4 (rev, ANSI false)`; `meandf/select_F.expr_avg/c34_4 (rev, ANSI true)`; `meandf/select_F.expr_avg/c38_0 (rev, ANSI false)`; `meandf/select_F.expr_avg/c38_0 (rev, ANSI true)`; `meandf/select_F.expr_avg/c38_18 (rev, ANSI false)`; `meandf/select_F.expr_avg/c38_18 (rev, ANSI true)`; `meandf/select_F.expr_avg/c38_2 (rev, ANSI false)`; `meandf/select_F.expr_avg/c38_2 (rev, ANSI true)`; `meandf/select_F.expr_avg/c38_36 (rev, ANSI false)`; `meandf/select_F.expr_avg/c38_36 (rev, ANSI true)`; `meandf/select_F.expr_mean/c10_0 (rev, ANSI false)`; `meandf/select_F.expr_mean/c10_0 (rev, ANSI true)`; `meandf/select_F.expr_mean/c10_2 (rev, ANSI false)`; `meandf/select_F.expr_mean/c10_2 (rev, ANSI true)`; `meandf/select_F.expr_mean/c10_6 (rev, ANSI false)`; `meandf/select_F.expr_mean/c10_6 (rev, ANSI true)`; `meandf/select_F.expr_mean/c28_0 (rev, ANSI false)`; `meandf/select_F.expr_mean/c28_0 (rev, ANSI true)`; `meandf/select_F.expr_mean/c28_10 (rev, ANSI false)`; `meandf/select_F.expr_mean/c28_10 (rev, ANSI true)`; `meandf/select_F.expr_mean/c28_2 (rev, ANSI false)`; `meandf/select_F.expr_mean/c28_2 (rev, ANSI true)`; `meandf/select_F.expr_mean/c34_0 (rev, ANSI false)`; `meandf/select_F.expr_mean/c34_0 (rev, ANSI true)`; `meandf/select_F.expr_mean/c34_30 (rev, ANSI false)`; `meandf/select_F.expr_mean/c34_30 (rev, ANSI true)`; `meandf/select_F.expr_mean/c34_4 (rev, ANSI false)`; `meandf/select_F.expr_mean/c34_4 (rev, ANSI true)`; `meandf/select_F.expr_mean/c38_0 (rev, ANSI false)`; `meandf/select_F.expr_mean/c38_0 (rev, ANSI true)`; `meandf/select_F.expr_mean/c38_18 (rev, ANSI false)`; `meandf/select_F.expr_mean/c38_18 (rev, ANSI true)`; `meandf/select_F.expr_mean/c38_2 (rev, ANSI false)`; `meandf/select_F.expr_mean/c38_2 (rev, ANSI true)`; `meandf/select_F.expr_mean/c38_36 (rev, ANSI false)`; `meandf/select_F.expr_mean/c38_36 (rev, ANSI true)`.
- **both raise with ANSI on, the same condition (`ARITHMETIC_OVERFLOW` / Spark's own)** (170): `avg/ungrouped/c38_0/g20 (apl_false, ANSI true)`; `avg/ungrouped/c38_0/g20 (base, ANSI true)`; `avg/ungrouped/c38_0/g20 (pmp_false, ANSI true)`; `avg/ungrouped/c38_0/g24 (apl_false, ANSI true)`; `avg/ungrouped/c38_0/g24 (base, ANSI true)`; `avg/ungrouped/c38_0/g24 (pmp_false, ANSI true)`; `avg/ungrouped/c38_18/g20 (apl_false, ANSI true)`; `avg/ungrouped/c38_18/g20 (base, ANSI true)`; `avg/ungrouped/c38_18/g20 (pmp_false, ANSI true)`; `avg/ungrouped/c38_18/g24 (apl_false, ANSI true)`; `avg/ungrouped/c38_18/g24 (base, ANSI true)`; `avg/ungrouped/c38_18/g24 (pmp_false, ANSI true)`; `avg/ungrouped/c38_2/g20 (apl_false, ANSI true)`; `avg/ungrouped/c38_2/g20 (base, ANSI true)`; `avg/ungrouped/c38_2/g20 (pmp_false, ANSI true)`; `avg/ungrouped/c38_2/g24 (apl_false, ANSI true)`; `avg/ungrouped/c38_2/g24 (base, ANSI true)`; `avg/ungrouped/c38_2/g24 (pmp_false, ANSI true)`; `avg/ungrouped/c38_36/g20 (apl_false, ANSI true)`; `avg/ungrouped/c38_36/g20 (base, ANSI true)`; `avg/ungrouped/c38_36/g20 (pmp_false, ANSI true)`; `avg/ungrouped/c38_36/g24 (apl_false, ANSI true)`; `avg/ungrouped/c38_36/g24 (base, ANSI true)`; `avg/ungrouped/c38_36/g24 (pmp_false, ANSI true)`; `div/case_round/d38_0 (apl_false, ANSI true)`; `div/case_round/d38_0 (base, ANSI true)`; `div/case_round/d38_0 (pmp_false, ANSI true)`; `div/case_round/d38_2 (apl_false, ANSI true)`; `div/case_round/d38_2 (base, ANSI true)`; `div/case_round/d38_2 (pmp_false, ANSI true)`; `div/lit_right/d38_0/dec3_0 (apl_false, ANSI true)`; `div/lit_right/d38_0/dec3_0 (base, ANSI true)`; `div/lit_right/d38_0/dec3_0 (pmp_false, ANSI true)`; `div/lit_right/d38_0/i100 (base, ANSI true)`; `div/lit_right/d38_0/i100 (pmp_false, ANSI true)`; `div/lit_right/d38_0/i11 (apl_false, ANSI true)`; `div/lit_right/d38_0/i11 (base, ANSI true)`; `div/lit_right/d38_0/i11 (pmp_false, ANSI true)`; `div/lit_right/d38_0/i3 (apl_false, ANSI true)`; `div/lit_right/d38_0/i3 (base, ANSI true)`; `div/lit_right/d38_0/i3 (pmp_false, ANSI true)`; `div/lit_right/d38_0/l3 (apl_false, ANSI true)`; `div/lit_right/d38_0/l3 (base, ANSI true)`; `div/lit_right/d38_0/l3 (pmp_false, ANSI true)`; `div/lit_right/d38_0/neg3 (apl_false, ANSI true)`; `div/lit_right/d38_0/neg3 (base, ANSI true)`; `div/lit_right/d38_0/neg3 (pmp_false, ANSI true)`; `div/lit_right/d38_0/s3 (apl_false, ANSI true)`; `div/lit_right/d38_0/s3 (base, ANSI true)`; `div/lit_right/d38_0/s3 (pmp_false, ANSI true)`; `div/lit_right/d38_0/y3 (apl_false, ANSI true)`; `div/lit_right/d38_0/y3 (base, ANSI true)`; `div/lit_right/d38_0/y3 (pmp_false, ANSI true)`; `div/lit_right/d38_2/dec3_0 (apl_false, ANSI true)`; `div/lit_right/d38_2/dec3_0 (base, ANSI true)`; `div/lit_right/d38_2/dec3_0 (pmp_false, ANSI true)`; `div/lit_right/d38_2/i100 (base, ANSI true)`; `div/lit_right/d38_2/i100 (pmp_false, ANSI true)`; `div/lit_right/d38_2/i11 (base, ANSI true)`; `div/lit_right/d38_2/i11 (pmp_false, ANSI true)`; `div/lit_right/d38_2/i3 (apl_false, ANSI true)`; `div/lit_right/d38_2/i3 (base, ANSI true)`; `div/lit_right/d38_2/i3 (pmp_false, ANSI true)`; `div/lit_right/d38_2/l3 (apl_false, ANSI true)`; `div/lit_right/d38_2/l3 (base, ANSI true)`; `div/lit_right/d38_2/l3 (pmp_false, ANSI true)`; `div/lit_right/d38_2/neg3 (apl_false, ANSI true)`; `div/lit_right/d38_2/neg3 (base, ANSI true)`; `div/lit_right/d38_2/neg3 (pmp_false, ANSI true)`; `div/lit_right/d38_2/s3 (apl_false, ANSI true)`; `div/lit_right/d38_2/s3 (base, ANSI true)`; `div/lit_right/d38_2/s3 (pmp_false, ANSI true)`; `div/lit_right/d38_2/y3 (apl_false, ANSI true)`; `div/lit_right/d38_2/y3 (base, ANSI true)`; `div/lit_right/d38_2/y3 (pmp_false, ANSI true)`; `div/typed/d38_0/cast_bigint (apl_false, ANSI true)`; `div/typed/d38_0/cast_bigint (base, ANSI true)`; `div/typed/d38_0/cast_bigint (pmp_false, ANSI true)`; `div/typed/d38_0/cast_int (apl_false, ANSI true)`; `div/typed/d38_0/cast_int (base, ANSI true)`; `div/typed/d38_0/cast_int (pmp_false, ANSI true)`; `div/typed/d38_0/col_bigint (apl_false, ANSI true)`; `div/typed/d38_0/col_bigint (base, ANSI true)`; `div/typed/d38_0/col_bigint (pmp_false, ANSI true)`; `div/typed/d38_0/col_int (apl_false, ANSI true)`; `div/typed/d38_0/col_int (base, ANSI true)`; `div/typed/d38_0/col_int (pmp_false, ANSI true)`; `div/typed/d38_0/col_smallint (apl_false, ANSI true)`; `div/typed/d38_0/col_smallint (base, ANSI true)`; `div/typed/d38_0/col_smallint (pmp_false, ANSI true)`; `div/typed/d38_0/col_tinyint (apl_false, ANSI true)`; `div/typed/d38_0/col_tinyint (base, ANSI true)`; `div/typed/d38_0/col_tinyint (pmp_false, ANSI true)`; `div/typed/d38_2/cast_bigint (apl_false, ANSI true)`; `div/typed/d38_2/cast_bigint (base, ANSI true)`; `div/typed/d38_2/cast_bigint (pmp_false, ANSI true)`; `div/typed/d38_2/cast_int (apl_false, ANSI true)`; `div/typed/d38_2/cast_int (base, ANSI true)`; `div/typed/d38_2/cast_int (pmp_false, ANSI true)`; `div/typed/d38_2/col_bigint (apl_false, ANSI true)`; `div/typed/d38_2/col_bigint (base, ANSI true)`; `div/typed/d38_2/col_bigint (pmp_false, ANSI true)`; `div/typed/d38_2/col_int (apl_false, ANSI true)`; `div/typed/d38_2/col_int (base, ANSI true)`; `div/typed/d38_2/col_int (pmp_false, ANSI true)`; `div/typed/d38_2/col_smallint (apl_false, ANSI true)`; `div/typed/d38_2/col_smallint (base, ANSI true)`; `div/typed/d38_2/col_smallint (pmp_false, ANSI true)`; `div/typed/d38_2/col_tinyint (apl_false, ANSI true)`; `div/typed/d38_2/col_tinyint (base, ANSI true)`; `div/typed/d38_2/col_tinyint (pmp_false, ANSI true)`; `div/where_round/d38_0 (apl_false, ANSI true)`; `div/where_round/d38_0 (base, ANSI true)`; `div/where_round/d38_0 (pmp_false, ANSI true)`; `div/where_round/d38_2 (apl_false, ANSI true)`; `div/where_round/d38_2 (base, ANSI true)`; `div/where_round/d38_2 (pmp_false, ANSI true)`; `divctl/add/d38_0/i3 (apl_false, ANSI true)`; `divctl/add/d38_0/i3 (base, ANSI true)`; `divctl/add/d38_0/i3 (pmp_false, ANSI true)`; `divctl/add/d38_2/i3 (apl_false, ANSI true)`; `divctl/add/d38_2/i3 (base, ANSI true)`; `divctl/add/d38_2/i3 (pmp_false, ANSI true)`; `divctl/mul/d38_0/i3 (apl_false, ANSI true)`; `divctl/mul/d38_0/i3 (base, ANSI true)`; `divctl/mul/d38_0/i3 (pmp_false, ANSI true)`; `divctl/mul/d38_0/neg3 (apl_false, ANSI true)`; `divctl/mul/d38_0/neg3 (base, ANSI true)`; `divctl/mul/d38_0/neg3 (pmp_false, ANSI true)`; `divctl/mul/d38_2/i3 (apl_false, ANSI true)`; `divctl/mul/d38_2/i3 (base, ANSI true)`; `divctl/mul/d38_2/i3 (pmp_false, ANSI true)`; `divctl/mul/d38_2/neg3 (apl_false, ANSI true)`; `divctl/mul/d38_2/neg3 (base, ANSI true)`; `divctl/mul/d38_2/neg3 (pmp_false, ANSI true)`; `divdf/F.expr/d38_0 (apl_false, ANSI true)`; `divdf/F.expr/d38_0 (base, ANSI true)`; `divdf/F.expr/d38_0 (pmp_false, ANSI true)`; `divdf/F.expr/d38_2 (apl_false, ANSI true)`; `divdf/F.expr/d38_2 (base, ANSI true)`; `divdf/F.expr/d38_2 (pmp_false, ANSI true)`; `divdf/col_mul_int/d38_0 (apl_false, ANSI true)`; `divdf/col_mul_int/d38_0 (base, ANSI true)`; `divdf/col_mul_int/d38_0 (pmp_false, ANSI true)`; `divdf/col_mul_int/d38_10 (apl_false, ANSI true)`; `divdf/col_mul_int/d38_2 (apl_false, ANSI true)`; `divdf/col_mul_int/d38_2 (base, ANSI true)`; `divdf/col_mul_int/d38_2 (pmp_false, ANSI true)`; `divdf/selectExpr/d38_0 (apl_false, ANSI true)`; `divdf/selectExpr/d38_0 (base, ANSI true)`; `divdf/selectExpr/d38_0 (pmp_false, ANSI true)`; `divdf/selectExpr/d38_2 (apl_false, ANSI true)`; `divdf/selectExpr/d38_2 (base, ANSI true)`; `divdf/selectExpr/d38_2 (pmp_false, ANSI true)`; `ice/ctas_div/d38_2 (base, ANSI true)`; `mean/ungrouped/c38_0/g20 (rev, ANSI true)`; `mean/ungrouped/c38_0/g24 (rev, ANSI true)`; `mean/ungrouped/c38_18/g20 (rev, ANSI true)`; `mean/ungrouped/c38_18/g24 (rev, ANSI true)`; `mean/ungrouped/c38_2/g20 (rev, ANSI true)`; `mean/ungrouped/c38_2/g24 (rev, ANSI true)`; `mean/ungrouped/c38_36/g20 (rev, ANSI true)`; `mean/ungrouped/c38_36/g24 (rev, ANSI true)`; `moddf/F.expr_fold/d38_0 (rev, ANSI true)`; `moddf/F.expr_fold/d38_2 (rev, ANSI true)`; `round/round_div/d38_0 (rev, ANSI true)`; `round/round_div/d38_2 (rev, ANSI true)`; `shape/fold_div/d38_0 (rev, ANSI true)`; `shape/fold_div/d38_2 (rev, ANSI true)`; `sum/s04_head (rev, ANSI true)`.
- **`avg(c) - 1`, `avg(c) * 2` and `avg(c) * count(c)` at p ≥ 34: the `+`/`-`/`*` result type past 38 digits (with the `d38_10 * 3` card's family); the `avg` operand itself agrees** (32): `avg/beside/c34_30 (base, ANSI false)`; `avg/beside/c34_30 (base, ANSI true)`; `avg/beside/c34_4 (base, ANSI false)`; `avg/beside/c34_4 (base, ANSI true)`; `avg/beside/c38_18 (base, ANSI false)`; `avg/beside/c38_18 (base, ANSI true)`; `avg/beside/c38_36 (base, ANSI false)`; `avg/beside/c38_36 (base, ANSI true)`; `avg/having_gt/c28_10 (base, ANSI false)`; `avg/having_gt/c28_10 (base, ANSI true)`; `avg/having_gt/c34_30 (base, ANSI false)`; `avg/having_gt/c34_30 (base, ANSI true)`; `avg/having_gt/c38_18 (base, ANSI false)`; `avg/having_gt/c38_18 (base, ANSI true)`; `avg/having_gt/c38_36 (base, ANSI false)`; `avg/having_gt/c38_36 (base, ANSI true)`; `mean/beside/c34_30 (rev, ANSI false)`; `mean/beside/c34_30 (rev, ANSI true)`; `mean/beside/c34_4 (rev, ANSI false)`; `mean/beside/c34_4 (rev, ANSI true)`; `mean/beside/c38_18 (rev, ANSI false)`; `mean/beside/c38_18 (rev, ANSI true)`; `mean/beside/c38_36 (rev, ANSI false)`; `mean/beside/c38_36 (rev, ANSI true)`; `mean/having_gt/c28_10 (rev, ANSI false)`; `mean/having_gt/c28_10 (rev, ANSI true)`; `mean/having_gt/c34_30 (rev, ANSI false)`; `mean/having_gt/c34_30 (rev, ANSI true)`; `mean/having_gt/c38_18 (rev, ANSI false)`; `mean/having_gt/c38_18 (rev, ANSI true)`; `mean/having_gt/c38_36 (rev, ANSI false)`; `mean/having_gt/c38_36 (rev, ANSI true)`.
- **`describe`/`summary` of `c38_18`: Spark prints the mean as `4.21667E-17` (not the averager; VALUE → FORMAT, the digits now agree)** (4): `avgdf/describe/c38_18 (base, ANSI false)`; `avgdf/describe/c38_18 (base, ANSI true)`; `avgdf/summary_five/c38_18 (base, ANSI false)`; `avgdf/summary_five/c38_18 (base, ANSI true)`.
- **`summary()` without percentiles refuses (not this family; main alike)** (8): `avgdf/summary_default/c10_0 (base, ANSI false)`; `avgdf/summary_default/c10_0 (base, ANSI true)`; `avgdf/summary_default/c10_2 (base, ANSI false)`; `avgdf/summary_default/c10_2 (base, ANSI true)`; `avgdf/summary_default/c28_2 (base, ANSI false)`; `avgdf/summary_default/c28_2 (base, ANSI true)`; `avgdf/summary_default/c38_18 (base, ANSI false)`; `avgdf/summary_default/c38_18 (base, ANSI true)`.

Changes between two non-EQUAL verdicts: the 26 ruling-2 cells (VALUE → REPARK_RAISES); `shape/avg_div` and `shape/mean_div` at `d28_2`, `d28_10` (REPARK_RAISES → VALUE) and `d34_4` (REPARK_RAISES → TYPE), ANSI off, ruling 4 (the division's own scale, not the averager, decides them); `avgdf/describe/c38_18` and `avgdf/summary_five/c38_18` (VALUE → FORMAT).

## 2. The critic's cell sets (`avg-critic`: `queries.json`, `queries3.json`, `fix.json`, `rundf.py`, `runice.py`, `n.json`)

| set | main | head | cells |
|---|---|---|---|
| df | EQUAL | EQUAL | 5 |
| df | None | None | 3 |
| df | RAISE | RAISE | 6 |
| df | WRONG | EQUAL | 18 |
| df | WRONG | WRONG | 22 |
| fix | BOTH_RAISE | BOTH_RAISE | 3 |
| fix | RAISE | EQUAL | 7 |
| ice-avg | EQUAL | EQUAL | 8 |
| ice-avg | WRONG | EQUAL | 12 |
| ice-div | EQUAL | EQUAL | 4 |
| ice-div | WRONG | WRONG | 26 |
| q | BOTH_RAISE | BOTH_RAISE | 4 |
| q | EQUAL | EQUAL | 38 |
| q | RAISE | EQUAL | 7 |
| q | RAISE | RAISE | 7 |
| q | WRONG | EQUAL | 39 |
| q | WRONG | RAISE | 2 |
| q | WRONG | WRONG | 55 |
| q3 | BOTH_RAISE | BOTH_RAISE | 1 |
| q3 | EQUAL | EQUAL | 6 |
| q3 | RAISE | EQUAL | 1 |
| q3 | RAISE | RAISE | 2 |
| q3 | WRONG | EQUAL | 8 |

- EQUAL → non-EQUAL: 0. Native (`n01`–`n10`): 10 of 10 equal to main.
- `c10` (`mean(DISTINCT c), count(c)`): WRONG → RAISE (ruling 2). `b08` (`mean(c)`) and every `b*` rounding cell: EQUAL. `f15` (`select(F.expr("mean(c)"))`) still refuses (FEXPR-COUNT-STAR-AGG-1), now naming `SparkAvgWithRetract` (pinned). `s01`: NULL with ANSI off (EQUAL), `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on (both raise). `c09`, `c11`, `c12`, `p01`, `c07`, `c15`, `c16`, `d*`, `f09`–`f24` other than the averager, `where-div-ice` and the division writes stay as on main (their units).

## 3. The pins (`python/repark/tests/types/test_avg_decimal_round_1.py`)

243 cells per ANSI mode against `avg_decimal_round_1_spark_oracle.json` (recorded from Spark 4.1.2 with the same
cells module), plus the ruled tests. Every `avg` cell has its `mean` twin. The cells cover:
- precisions 10, 28, 34 and 38: ungrouped, including the near-limit g20, g22 and g24 cells and the ANSI-off NULLs;
- grouped, `HAVING`, and every window frame: partition, `ROWS 1 PRECEDING` (a control), a sliding `ROWS 2 PRECEDING`,
  running and `RANGE`;
- `DISTINCT`, `FILTER`, `GROUPING SETS`, `ROLLUP`, `CUBE`, `UNION ALL`, an `avg` beside `max`, and a four-file merge;
- `try_avg`, `s04`, `s04_head`, `s01`, the controls (`sum / count`, typed division, `avg` and `mean` of `DOUBLE`,
  `BIGINT` and `INT`);
- the DataFrame spellings, `F.expr` and `selectExpr`, the `describe` and `summary` means, and `repartition(4)`;
- every Iceberg wrong write: `avg` CTAS of `c10_0`, `c10_2`, `c28_2` and `c38_18`, the two `mean` CTAS, `avg` `INSERT`,
  and `avg` `MERGE` copy-on-write and merge-on-read, with stored rows and schema.

| run | result |
|---|---|
| head `pkg-avg-27d6149f` | **500 passed** |
| main `pkg-dct-7bf32859` | **362 failed**, 138 passed |

Red on main: 177 cells per mode; the decimal `mean(DISTINCT x), count(x)` refusal (both modes); the bare `F.expr("mean")`
naming `SparkAvgWithRetract` (both); the two-file partial cell (both functions, both modes). Every position has a cell
that is red on main. The ones green on main are the terminating quotients, the 2-row `ROWS 1 PRECEDING` frame, `s04`,
the one-file grouped `s04`, the error cells and the controls.

## 4. The non-decimal `mean` (now RePark's accumulator)

`mean` and `avg` of `DOUBLE`, `BIGINT`, `INT`, `SMALLINT`, `FLOAT`, `TINYINT`, `BOOLEAN`, `STRING`, a string literal,
`NULL` and three `INTERVAL` kinds were run on Spark, main and the head, in both modes. Each was run grouped, ungrouped,
as a window (`ROWS` and partition), with `DISTINCT`, with `DISTINCT` beside `count`, in `HAVING`, and on the DataFrame
door (174 cells; raw in [raw/nondec/](raw/nondec/)). **EQUAL → non-EQUAL: 0.**

The first head build moved `mean(DISTINCT x), count(x)` of `DOUBLE`, `BIGINT` and `INT` from EQUAL to a refusal: main
answered through DataFusion's distinct accumulator. The product commit keeps that accumulator for a float
`mean(DISTINCT)`, so these six cells are EQUAL again. The cells that still move are these:
- `mean(x), avg(x)`: main raised a duplicate field; the head answers as Spark (EQUAL);
- `mean('1.5')`, `mean(str)`, `mean(t)` and `mean(NULL)`: a refusal on main and on the head, with another text.

## 5. The partial sum past 38 digits (§7's open case)

| shape (`MAX38`, `MAX38`, `−MAX38`, `−MAX38`, `5` at `decimal(38,0)`) | Spark 4.1.2 | main | head |
|---|---|---|---|
| ungrouped, one partition (`s04`) | `1.0000` | `1.0000` | `1.0000` |
| grouped, one partition | raises (ANSI on), NULL (off) | `1.0000` | `1.0000` (ruled unbounded sum; named) |
| grouped, two files (`MAX38`, `MAX38` in one) | raises (ANSI on), NULL (off) | `1.0000` | **as Spark** (pinned) |
| `MAX38`, `MAX38`, `5` (`s04_head`) | raises / NULL | raises its own text / raises | as Spark |

Spark keeps its sum buffer in a row of `sumDataType`: per row in a grouped hash aggregate, and per partial before the
shuffle. The head keeps an exact running sum inside an accumulator and ships a NULL partial sum when a partial leaves
the sum type. Spark's answers: [raw/partition/](raw/partition/). RePark's answers are in the pin file.

## 6. Reachability of the `mean` alias

- SQL `mean(x)` plans as `mean(t.x)` on RePark's averager (no `avg(t.x) AS mean(...)` alias). The DataFusion `Avg`
  registered under `mean` is overwritten by `aggregate::functions()`; `SparkAvg` (datafusion-spark, name `avg`) and
  DataFusion's `Avg` (name `avg`) were already overwritten by `avg_udaf()`.
- `F.mean`, `GroupedData.mean`, `agg({x: "mean"})`, `describe` and `summary` call `F.avg`, which is `avg_udaf()`.
  `F.expr("mean(x)")` and `selectExpr("mean(x)")` resolve through the session registry.
- Proof: the `mean` cells above, the Rust test `mean_is_spark_avg_and_keeps_its_name` (the registry's `mean` downcasts
  to `SparkAvgWithRetract`; `avg(v), mean(v)` plans and answers), and the `f15` refusal naming `SparkAvgWithRetract`.
- The native `repark.sql()` door registers none of these and keeps DataFusion's `Avg` (§1: 0 moves).
