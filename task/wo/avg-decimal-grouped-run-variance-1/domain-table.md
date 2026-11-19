# AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 — domain table (measured)

## Method

- **Builds:** main `6903fb48` as `pkg-main-4c365814` (same product tree); #1360's head `pkg-sum-845766fd` (its
  `avg` files equal main's: `git diff 6903fb48 845766fd` on `avg_decimal.rs`, `avg_groups.rs` is empty); the sizing's
  prototypes `pkg-sumx-898e15e6` and `pkg-sumx-5469a59a` (their `avg_decimal.rs` differs from main's only in
  `pub(crate)` on three helpers). Interpreter `S/lane-nan-write-canonical-1/.venv/bin/python`, no `-I`; every run
  prints `repark._native.__file__` (logs `N/avg-variance-out/logs/`).
- **Tables** ([evidence/tables.py](evidence/tables.py), written once, read by both engines):
  - `t`: four Parquet files (64-row row groups), columns `c10_2`, `c18_0`, `c19_0`, `c20_0`, `c28_4`, `c38_0`, groups
    1 to 12 from the #1360 verifier's `det.py` scenarios with `m = 10^p - 1` (sums that cross 38 digits and come
    back at `c38_0`), group 0 a 1,200-row filler, group 12 all NULL;
  - `big`: the sizing's three files of 300,000 rows (`M`, `5`, `-M` once per group per file, 3,000 groups, exact
    average `0.0167`);
  - `t4`: the sizing's three-file groups 21 to 25 (`c38_0`);
  - the #1360 verifier's over-precision files (`S/verify-sum/overprec/files/`, copied byte for byte, `cmp` clean),
    alone and as a two-file table with a second file of one `0` row (crafted the same way).
- **Queries** ([evidence/cells.py](evidence/cells.py)): per column, `avg` and `try_avg` ungrouped (`WHERE g = k` and
  the whole table) and grouped (`WHERE g = k GROUP BY g` and every group), `HAVING avg(c) > 0`, `GROUP BY ROLLUP(g)`,
  `avg(c) FILTER (WHERE id % 2 = 0)`, `abs(avg(c))` grouped, a whole-partition window, a sliding window
  (`ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`), `avg(DISTINCT c)`, and an Iceberg `CREATE TABLE … AS SELECT g,
  avg(c) … GROUP BY g` read back. The `sum` / `try_sum` twins of the ungrouped and grouped cells ride along.
- **Configurations,** 11 per ANSI mode (22 in all), 20 runs per query each: 2, 4, 8 and 10 cores (`taskset` inside
  lane B's 48-57) with file work stealing on and off, and `SET datafusion.execution.target_partitions` 2, 4 and 8 at
  10 cores, stealing on. Every setting is made before the tables are read.
- **Spark 4.1.2** ([evidence/spark_avg.py](evidence/spark_avg.py)): the same files and queries at `local[1]` (2 runs;
  `t` reads as 1 split) and `local[4]` (3 runs; `t` 4 splits, `big` and `t4` 3), shuffle partitions equal to the
  core count, Iceberg 1.11.0 hadoop catalog.
- **Classes** ([evidence/classify.py](evidence/classify.py)), per cell = query × mode × configuration:
  **VARIES** (more than one answer in 20 runs); **EQUAL** (one answer, Spark's); **WRONG** (both answer, values
  differ; `WRONG*` when RePark's value is not the exact one, [evidence/model.py](evidence/model.py)); **RAISE** (one
  side raises, or both raise different conditions). The exact model adds the values, checks the total once against
  `10^min(p+10, 38)` and the half-up quotient against `10^min(p+4, 38)`.

## Size and the short answer

- **Main:** 1,180 queries per ANSI mode (770 `avg` / `try_avg`, 410 `sum` / `try_sum` twins) × 2 modes × 11
  configurations = **25,960 cells**, 20 runs each: 16,940 `avg` cells (15,356 on `t`, `big` and `t4`; 1,584 on the
  over-precision files) and 9,020 `sum` cells. Spark: 4,720 (query, mode, master) answers, **one answer on every
  run** at both masters.
- **VARIES, `avg`:** **96 cells** (31 query-mode pairs, 13 with ANSI off and 18 with it on): `big` 31, `t` `c38_0`
  46, `t4` 19. **None at precision 10, 18, 19, 20 or 28**, none in a window or `DISTINCT` cell, none on the
  over-precision files. With stealing off every `avg` cell is steady, but `big` and `t4` then follow the core count.
- **Steady but not exact (`WRONG*` and the RAISE cells with a value behind them), `avg`, outside the over-precision
  files:** only at precision 38 (`c38_0`, `big`, `t4`): a partial's NULL marker answers NULL or
  `ARITHMETIC_OVERFLOW` where the exact total fits. At precision 28 and under every steady `avg` cell is the exact
  answer and Spark's.
- **Spark's own layout:** 48 `avg` (query, mode) pairs of 1,396 answer differently at `local[1]` and `local[4]` (20
  with ANSI off, 28 on), all at precision 38. Spark's `avg` follows its split layout as its `sum` does (20 / 23 `sum`
  pairs here).
- **Over-precision files (Spark refuses every read with `NUMERIC_VALUE_OUT_OF_RANGE`):** main answers a value in 48
  (query, mode) pairs, all 11 configurations, both modes: ungrouped `avg` / `try_avg` and the whole-partition window
  on `wrap5` (`1.25` on one file, `1` on two) and `neg` (`-1.25`, `-1`), at precision 19 and 20. Their exact totals
  are past 38 digits, so the answer should be NULL or `ARITHMETIC_OVERFLOW`. The grouped cells and the sliding window
  are exact. #1360's head answers the same (it leaves `avg` alone).
- **Narrow natives (one probe, [evidence/narrow_native.py](evidence/narrow_native.py), main, 20 runs):** a Parquet
  `decimal(9,2)` written as Arrow `decimal32` reads as `Decimal32`. `M, M, M / -M, -M, -M / 5 / 0` (exact average
  `0.625`) answers `0.625000` at `target_partitions` 1, but **NULL** (ANSI off) or `ARITHMETIC_OVERFLOW` (on)
  grouped at 2 and 4, and ungrouped NULL in 5 of 20 at 2: three values near the 9-digit maximum wrap the `i32`
  partial, which ships the marker. `decimal64(18,0)` of the same shape answers `0.6250` (no wrap at three values).
  Spark has one decimal type, `decimal(9,2)`, and answers `0.625`. Not counted in the 16,940.

## The headline cells, per configuration (main, answers × runs of 20)

`N` = NULL groups of 3,000; `AO` = `ARITHMETIC_OVERFLOW`; `NVOOR` = `NUMERIC_VALUE_OUT_OF_RANGE`. Spark at `local[1]` / `local[4]`: `big` grouped all 3,000 NULL / `AO` at both; `big` ungrouped `0.0167` / NULL (`AO`); `t` g1 ungrouped `0.5556` / NULL (`AO`); `t4` g23 `1.6667` / NULL (`AO`); g25 `1.6667` at both. Exact: `0.0167` in every `big` group.

| query | ANSI | 2c on | 4c on | 8c on | 10c on | 2c off | 4c off | 8c off | 10c off | SET 2 | SET 4 | SET 8 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `big/c/g/avg/all` | false | 0N×20 | 398N×16 / 618N×3 / 583N×1 | 437N×14 / 369N×5 / 220N×1 | 220N×15 / 219N×4 / 63N×1 | 3000N×20 | 398N×20 | 437N×20 | 220N×20 | 0N×20 | 398N×8 / 583N×6 / 618N×3 / 645N×3 | 437N×16 / 369N×2 / 220N×2 |
| `big/c/g/avg/all` | true | 0N×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | 0N×20 | AO×20 | AO×20 |
| `big/c/u/avg/all` | false | 0.0167×11 / NULL×9 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | 0.0167×20 | NULL×20 | NULL×20 |
| `big/c/u/avg/all` | true | AO×15 / 0.0167×5 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | 0.0167×11 / AO×9 | AO×20 | AO×20 |
| `t/c38_0/g/avg/all` | true | AO×20 | AO×15 / NVOOR×5 | AO×18 / NVOOR×2 | AO×13 / NVOOR×7 | AO×14 / NVOOR×6 | AO×19 / NVOOR×1 | AO×18 / NVOOR×2 | AO×16 / NVOOR×4 | AO×17 / NVOOR×3 | AO×17 / NVOOR×3 | AO×17 / NVOOR×3 |
| `t/c38_0/u/avg/g1` | false | NULL×20 | NULL×20 | NULL×20 | NULL×20 | 0.5556×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 |
| `t/c38_0/u/avg/g1` | true | AO×19 / 0.5556×1 | AO×20 | AO×20 | AO×20 | 0.5556×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 |
| `t4/c38_0/g/avg/g23` | false | NULL×18 / 1.6667×2 | NULL×20 | NULL×20 | NULL×20 | 1.6667×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 | NULL×20 |
| `t4/c38_0/g/avg/g23` | true | AO×16 / 1.6667×4 | AO×20 | AO×20 | AO×20 | 1.6667×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 | AO×20 |
| `t4/c38_0/g/avg/g25` | false | NULL×17 / 1.6667×3 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | NULL×12 / 1.6667×8 | 1.6667×20 | 1.6667×20 |
| `t4/c38_0/g/avg/g25` | true | AO×17 / 1.6667×3 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | 1.6667×20 | AO×10 / 1.6667×10 | 1.6667×20 | 1.6667×20 |

## Class counts per family (main; families with any cell not EQUAL; `sum` rows are the twins)

Each count is over 11 configurations × the family's queries. `WRONG*`: RePark's value is neither Spark's nor the exact one.

| family | column | ANSI | cells | vs `local[1]` | vs `local[4]` |
|---|---|---|---|---|---|
| big g avg | big | false | 11 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 |
| big g avg | big | true | 11 | EQUAL 9, RAISE 2 | EQUAL 9, RAISE 2 |
| big g sum | big | false | 11 | WRONG 11 | WRONG 11 |
| big g sum | big | true | 11 | RAISE 11 | RAISE 11 |
| big g try_avg | big | false | 11 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 |
| big g try_avg | big | true | 11 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 | EQUAL 1, VARIES 5, WRONG 2, WRONG* 3 |
| big g try_sum | big | false | 11 | EQUAL 1, VARIES 8, WRONG 2 | EQUAL 1, VARIES 8, WRONG 2 |
| big g try_sum | big | true | 11 | EQUAL 1, VARIES 8, WRONG 2 | EQUAL 1, VARIES 8, WRONG 2 |
| big u avg | big | false | 22 | EQUAL 8, VARIES 3, WRONG* 11 | EQUAL 16, VARIES 3, WRONG 1, WRONG* 2 |
| big u avg | big | true | 22 | EQUAL 7, RAISE 11, VARIES 4 | EQUAL 16, RAISE 2, VARIES 4 |
| big u sum | big | false | 22 | EQUAL 22 | EQUAL 11, WRONG 11 |
| big u sum | big | true | 22 | EQUAL 22 | EQUAL 11, RAISE 11 |
| big u try_avg | big | false | 22 | EQUAL 6, VARIES 5, WRONG* 11 | EQUAL 15, VARIES 5, WRONG* 2 |
| big u try_avg | big | true | 22 | EQUAL 7, VARIES 4, WRONG* 11 | EQUAL 16, VARIES 4, WRONG* 2 |
| big u try_sum | big | false | 22 | EQUAL 2, VARIES 7, WRONG* 13 | EQUAL 13, VARIES 7, WRONG* 2 |
| big u try_sum | big | true | 22 | EQUAL 2, VARIES 7, WRONG* 13 | EQUAL 13, VARIES 7, WRONG* 2 |
| t distinct avg | c38_0 | false | 143 | EQUAL 134, WRONG* 9 | EQUAL 134, WRONG* 9 |
| t distinct avg | c38_0 | true | 143 | EQUAL 117, RAISE 26 | EQUAL 128, RAISE 15 |
| t expr avg | c38_0 | false | 143 | EQUAL 131, WRONG 2, WRONG* 10 | EQUAL 140, WRONG 3 |
| t expr avg | c38_0 | true | 143 | EQUAL 130, RAISE 13 | EQUAL 139, RAISE 4 |
| t filter avg | c38_0 | false | 11 | EQUAL 9, VARIES 2 | EQUAL 9, VARIES 2 |
| t filter avg | c38_0 | true | 11 | RAISE 1, VARIES 10 | EQUAL 1, VARIES 10 |
| t g avg | c38_0 | false | 154 | EQUAL 131, WRONG 2, WRONG* 21 | EQUAL 150, WRONG 3, WRONG* 1 |
| t g avg | c38_0 | true | 154 | EQUAL 131, RAISE 13, VARIES 10 | EQUAL 139, RAISE 5, VARIES 10 |
| t g sum | c38_0 | false | 154 | EQUAL 66, WRONG 44, WRONG* 44 | EQUAL 55, WRONG 55, WRONG* 44 |
| t g sum | c38_0 | true | 154 | EQUAL 66, RAISE 88 | EQUAL 55, RAISE 99 |
| t g try_avg | c38_0 | false | 154 | EQUAL 131, WRONG 2, WRONG* 21 | EQUAL 150, WRONG 3, WRONG* 1 |
| t g try_avg | c38_0 | true | 154 | EQUAL 131, WRONG 2, WRONG* 21 | EQUAL 150, WRONG 3, WRONG* 1 |
| t g try_sum | c38_0 | false | 154 | EQUAL 132, WRONG* 22 | EQUAL 154 |
| t g try_sum | c38_0 | true | 154 | EQUAL 132, WRONG* 22 | EQUAL 154 |
| t having avg | c38_0 | false | 11 | WRONG 1, WRONG* 10 | EQUAL 10, WRONG 1 |
| t having avg | c38_0 | true | 11 | EQUAL 1, VARIES 10 | RAISE 1, VARIES 10 |
| t ice avg | c38_0 | false | 11 | WRONG* 11 | EQUAL 10, WRONG* 1 |
| t ice avg | c38_0 | true | 11 | EQUAL 2, VARIES 9 | RAISE 2, VARIES 9 |
| t rollup avg | c38_0 | false | 11 | WRONG* 11 | EQUAL 10, WRONG* 1 |
| t rollup avg | c38_0 | true | 11 | EQUAL 7, VARIES 4 | EQUAL 7, VARIES 4 |
| t u avg | c38_0 | false | 154 | EQUAL 113, WRONG* 41 | EQUAL 151, WRONG 3 |
| t u avg | c38_0 | true | 154 | EQUAL 103, RAISE 50, VARIES 1 | EQUAL 127, RAISE 26, VARIES 1 |
| t u sum | c38_0 | false | 154 | EQUAL 110, WRONG* 44 | EQUAL 55, WRONG 55, WRONG* 44 |
| t u sum | c38_0 | true | 154 | EQUAL 110, RAISE 44 | EQUAL 55, RAISE 99 |
| t u try_avg | c38_0 | false | 154 | EQUAL 113, WRONG* 41 | EQUAL 151, WRONG 3 |
| t u try_avg | c38_0 | true | 154 | EQUAL 113, WRONG* 41 | EQUAL 151, WRONG 3 |
| t u try_sum | c38_0 | false | 154 | EQUAL 99, WRONG* 55 | EQUAL 154 |
| t u try_sum | c38_0 | true | 154 | EQUAL 99, WRONG* 55 | EQUAL 154 |
| t win avg | c38_0 | true | 143 | EQUAL 110, RAISE 33 | EQUAL 110, RAISE 33 |
| t4 g avg | t4 | false | 55 | EQUAL 32, VARIES 3, WRONG 11, WRONG* 9 | EQUAL 51, VARIES 3, WRONG 1 |
| t4 g avg | t4 | true | 55 | EQUAL 22, RAISE 30, VARIES 3 | EQUAL 41, RAISE 11, VARIES 3 |
| t4 g sum | t4 | false | 55 | EQUAL 22, WRONG 22, WRONG* 11 | EQUAL 22, WRONG 22, WRONG* 11 |
| t4 g sum | t4 | true | 55 | EQUAL 22, RAISE 33 | EQUAL 22, RAISE 33 |
| t4 g try_avg | t4 | false | 55 | EQUAL 32, VARIES 3, WRONG 11, WRONG* 9 | EQUAL 51, VARIES 3, WRONG 1 |
| t4 g try_avg | t4 | true | 55 | EQUAL 32, VARIES 3, WRONG 11, WRONG* 9 | EQUAL 51, VARIES 3, WRONG 1 |
| t4 g try_sum | t4 | false | 55 | EQUAL 13, VARIES 19, WRONG 12, WRONG* 11 | EQUAL 35, VARIES 19, WRONG 1 |
| t4 g try_sum | t4 | true | 55 | EQUAL 13, VARIES 19, WRONG 12, WRONG* 11 | EQUAL 35, VARIES 19, WRONG 1 |
| t4 u avg | t4 | false | 55 | EQUAL 43, VARIES 1, WRONG* 11 | EQUAL 52, VARIES 1, WRONG 1, WRONG* 1 |
| t4 u avg | t4 | true | 55 | EQUAL 42, RAISE 11, VARIES 2 | EQUAL 39, RAISE 14, VARIES 2 |
| t4 u sum | t4 | false | 55 | EQUAL 44, WRONG* 11 | EQUAL 33, WRONG 11, WRONG* 11 |
| t4 u sum | t4 | true | 55 | EQUAL 44, RAISE 11 | EQUAL 33, RAISE 22 |
| t4 u try_avg | t4 | false | 55 | EQUAL 43, VARIES 2, WRONG* 10 | EQUAL 52, VARIES 2, WRONG 1 |
| t4 u try_avg | t4 | true | 55 | EQUAL 43, VARIES 2, WRONG* 10 | EQUAL 52, VARIES 2, WRONG 1 |
| t4 u try_sum | t4 | false | 55 | EQUAL 25, VARIES 17, WRONG* 13 | EQUAL 36, VARIES 17, WRONG* 2 |
| t4 u try_sum | t4 | true | 55 | EQUAL 25, VARIES 18, WRONG* 12 | EQUAL 36, VARIES 18, WRONG* 1 |

## Every VARIES pair (main)

| query | ANSI | configurations that vary (answers) | answers seen |
|---|---|---|---|
| `big/c/g/avg/all` | false | 4c on (3), 8c on (3), 10c on (3), SET 4 (4), SET 8 (3) | rows=3000 nulls=437 x50; rows=3000 nulls=398 x44; rows=3000 nulls=0 x40; rows=3000 nulls=220 x38 |
| `big/c/g/try_avg/all` | false | 4c on (3), 8c on (4), 10c on (3), SET 4 (5), SET 8 (3) | rows=3000 nulls=437 x47; rows=3000 nulls=398 x43; rows=3000 nulls=220 x41; rows=3000 nulls=0 x40 |
| `big/c/g/try_avg/all` | true | 4c on (3), 8c on (3), 10c on (3), SET 4 (6), SET 8 (3) | rows=3000 nulls=437 x45; rows=3000 nulls=0 x40; rows=3000 nulls=398 x40; rows=3000 nulls=220 x38 |
| `big/c/g/try_sum/all` | false | 4c on (20), 8c on (19), 10c on (20), 4c off (19), 8c off (20), 10c off (20), SET 4 (20), SET 8 (20) | rows=3000 nulls=0 x40; rows=3000 nulls=3000 x20; rows=3000 nulls=1145 x2; rows=3000 nulls=900 x2 |
| `big/c/g/try_sum/all` | true | 4c on (20), 8c on (20), 10c on (20), 4c off (17), 8c off (19), 10c off (20), SET 4 (18), SET 8 (19) | rows=3000 nulls=0 x40; rows=3000 nulls=3000 x20; rows=3000 nulls=1096 x3; rows=3000 nulls=858 x3 |
| `big/c/u/avg/all` | false | 2c on (2) | NULL x189; 0.0167 x31 |
| `big/c/u/avg/all` | true | 2c on (2), SET 2 (2) | ERR ARITHMETIC_OVERFLOW x204; 0.0167 x16 |
| `big/c/u/avg/g7` | false | 8c on (2), SET 8 (2) | 0.0167 x153; NULL x67 |
| `big/c/u/avg/g7` | true | 8c on (2), SET 8 (2) | 0.0167 x151; ERR ARITHMETIC_OVERFLOW x69 |
| `big/c/u/try_avg/all` | false | 2c on (2), SET 2 (2) | NULL x191; 0.0167 x29 |
| `big/c/u/try_avg/all` | true | 2c on (2), SET 2 (2) | NULL x196; 0.0167 x24 |
| `big/c/u/try_avg/g7` | false | 8c on (2), SET 4 (2), SET 8 (2) | 0.0167 x157; NULL x63 |
| `big/c/u/try_avg/g7` | true | 8c on (2), SET 8 (2) | 0.0167 x151; NULL x69 |
| `big/c/u/try_sum/g7` | false | 4c on (2), 8c on (2), 10c on (2), 4c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 5 x123; NULL x97 |
| `big/c/u/try_sum/g7` | true | 4c on (2), 8c on (2), 10c on (2), 4c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 5 x120; NULL x100 |
| `t/c38_0/filter/avg/all` | false | 2c on (2), SET 2 (2) | [["0", "-0.01"], ["1", null], ["10", null], ["11", "1.25"], ["12", ... x190; [["0", "-0.01"], ["1", null], ["10", null], ["11", "1.25"], ["12", ... x30 |
| `t/c38_0/filter/avg/all` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | ERR ARITHMETIC_OVERFLOW x135; ERR NUMERIC_VALUE_OUT_OF_RANGE x85 |
| `t/c38_0/g/avg/all` | true | 4c on (2), 8c on (2), 10c on (2), 2c off (2), 4c off (2), 8c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | ERR ARITHMETIC_OVERFLOW x184; ERR NUMERIC_VALUE_OUT_OF_RANGE x36 |
| `t/c38_0/having/avg/all` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 2c off (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | ERR ARITHMETIC_OVERFLOW x193; ERR NUMERIC_VALUE_OUT_OF_RANGE x27 |
| `t/c38_0/ice/avg/all` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 2c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | stmt ERR ARITHMETIC_OVERFLOW; ERR x11; stmt ERR NUMERIC_VALUE_OUT_OF_RANGE; ERR x9 |
| `t/c38_0/rollup/avg/all` | true | 8c on (2), 10c on (2), 10c off (2), SET 8 (2) | ERR ARITHMETIC_OVERFLOW x210; ERR NUMERIC_VALUE_OUT_OF_RANGE x10 |
| `t/c38_0/u/avg/g1` | true | 2c on (2) | ERR ARITHMETIC_OVERFLOW x199; 0.5556 x21 |
| `t4/c38_0/g/avg/g23` | false | 2c on (2) | 23|NULL x198; 23|1.6667 x22 |
| `t4/c38_0/g/avg/g23` | true | 2c on (2) | ERR ARITHMETIC_OVERFLOW x196; 23|1.6667 x24 |
| `t4/c38_0/g/avg/g25` | false | 2c on (2), SET 2 (2) | 25|1.6667 x191; 25|NULL x29 |
| `t4/c38_0/g/avg/g25` | true | 2c on (2), SET 2 (2) | 25|1.6667 x193; ERR ARITHMETIC_OVERFLOW x27 |
| `t4/c38_0/g/try_avg/g23` | false | 2c on (2) | 23|NULL x199; 23|1.6667 x21 |
| `t4/c38_0/g/try_avg/g23` | true | 2c on (2) | 23|NULL x194; 23|1.6667 x26 |
| `t4/c38_0/g/try_avg/g25` | false | 2c on (2), SET 2 (2) | 25|1.6667 x193; 25|NULL x27 |
| `t4/c38_0/g/try_avg/g25` | true | 2c on (2), SET 2 (2) | 25|1.6667 x195; 25|NULL x25 |
| `t4/c38_0/g/try_sum/g21` | false | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 21|99999999999999999999999999999999999999 x150; 21|NULL x70 |
| `t4/c38_0/g/try_sum/g21` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 21|99999999999999999999999999999999999999 x146; 21|NULL x74 |
| `t4/c38_0/g/try_sum/g25` | false | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | 25|5 x140; 25|NULL x80 |
| `t4/c38_0/g/try_sum/g25` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | 25|5 x140; 25|NULL x80 |
| `t4/c38_0/u/avg/g23` | true | 2c on (2) | ERR ARITHMETIC_OVERFLOW x199; 1.6667 x21 |
| `t4/c38_0/u/avg/g25` | false | 2c on (2) | 1.6667 x183; NULL x37 |
| `t4/c38_0/u/avg/g25` | true | SET 2 (2) | 1.6667 x183; ERR ARITHMETIC_OVERFLOW x37 |
| `t4/c38_0/u/try_avg/g25` | false | 2c on (2), SET 2 (2) | 1.6667 x185; NULL x35 |
| `t4/c38_0/u/try_avg/g25` | true | 2c on (2), SET 2 (2) | 1.6667 x182; NULL x38 |
| `t4/c38_0/u/try_sum/g21` | false | 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 99999999999999999999999999999999999999 x149; NULL x71 |
| `t4/c38_0/u/try_sum/g21` | true | 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 99999999999999999999999999999999999999 x150; NULL x70 |
| `t4/c38_0/u/try_sum/g25` | false | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 4 (2), SET 8 (2) | 5 x135; NULL x85 |
| `t4/c38_0/u/try_sum/g25` | true | 2c on (2), 4c on (2), 8c on (2), 10c on (2), 4c off (2), 8c off (2), 10c off (2), SET 2 (2), SET 4 (2), SET 8 (2) | 5 x137; NULL x83 |

## The `sum` side (#1360's head `pkg-sum-845766fd`) and the prototypes

- **#1360's head, `sum` / `try_sum` twins,** five configurations (SET 2, 4 and 8 at 10 cores; 2 cores, stealing on;
  4 cores, stealing off), both modes, 20 runs: **4,360 cells, none varies, and every one outside the over-precision
  files is the exact total** ([evidence/verdicts/verdicts-pkg-sum-845766fd.tsv](evidence/verdicts/verdicts-pkg-sum-845766fd.tsv)).
  The `sum` twins on main vary in 103 cells (datafusion-spark's `try_sum` and the grouped race the sizing found).
- **#1360's head, `avg`** (`c38_0`, `big`, `t4` and the over-precision files at SET 4 and 4 cores on): 14 of 840
  cells vary, the same queries as on main; #1360 leaves `avg` alone.
- **Prototypes** `pkg-sumx-898e15e6` (carrier (i)) and `pkg-sumx-5469a59a` (carrier (ii)), the same two
  configurations on `c38_0`, `big` and `t4`: `avg` varies in 14 and 15 of 552 cells. Carrier (ii)'s `sum` is not
  exact in 94 cells (its 128-bit marker); carrier (i)'s is exact in all of them.

## Spark against the exact answer (seat A's kept divergence)

Cell for cell in [evidence/classes.md](evidence/classes.md): **81 `avg` (query, mode) pairs** where the exact answer
differs from Spark at one master or both, every one at precision 38 (`c38_0`, `big`, `t4`):

| class | vs `local[1]` | vs `local[4]` |
|---|---|---|
| exact value, Spark NULL (ANSI off and `try_avg`; includes one `HAVING` set where Spark drops the NULL rows) | 24 | 46 |
| exact value, Spark raises `ARITHMETIC_OVERFLOW` (ANSI on) | 8 | 16 |
| both raise, another condition (`NUMERIC_VALUE_OUT_OF_RANGE` against `ARITHMETIC_OVERFLOW`, either way round) | 8 | 13 |
| the exact answer raises or is NULL where Spark answers | 0 | 0 (on these tables; **20 on r1's**, below) |
| equal to Spark at that master (of the 81) | 41 | 6 |

Against `local[1]` 1,356 of the 1,396 `avg` pairs are equal, against `local[4]` 1,321. No exact value differs from a Spark value.
**Corrected in r1:** the claim "the exact answer never raises or is NULL where Spark answers" holds on step 0's
tables only. None of them held a total past 38 digits with an average under `10^34`; on five tables that do, Spark
`local[4]` answers and the exact answer is NULL / `ARITHMETIC_OVERFLOW` (the r1 section below,
[evidence/classes.md](evidence/classes.md)).

## Revision r1 (critic round 1, 2026-11-19)

Every cell the critic measured is re-measured here, with the critic's tables re-written by
[evidence/r1_tables.py](evidence/r1_tables.py) and five new ones. Rendered tables:
[evidence/raw-r1/](evidence/raw-r1/map.md); raw runs stay in `N/avg-revision-out/` (`raw/`, `logs/`, every
`repark._native.__file__` print).

- **Builds:** `avg` on `pkg-main-4c365814` (main `d9eebe06` has the same `avg` source: `git diff 6903fb48
  d9eebe06` touches only `aggregate.rs`, #1360's `sum` routing); `sum` on #1360's `pkg-sum-845766fd`.
- **RePark configurations:** SET 1, 2, 4 and 8 at 10 cores (stealing on), 2 cores (on), 4 cores (off); both ANSI
  modes; 20 runs a cell (spill: 5). Every setting before the first read. Probes
  [evidence/r1_repark.py](evidence/r1_repark.py), [evidence/r1_door.py](evidence/r1_door.py) (DataFrame door,
  Iceberg), cells [evidence/r1_cells.py](evidence/r1_cells.py), exact answers
  [evidence/r1_exact.py](evidence/r1_exact.py) (seat A as revised: the window checks only the quotient).
- **Spark 4.1.2** ([evidence/r1_spark.py](evidence/r1_spark.py)): `local[1]` and `local[4]` × both modes × whole-stage
  codegen on and off, 2 runs each, shuffle partitions equal to the cores. Every Spark cell was steady.

**Main against the new cells** ([evidence/raw-r1/main.md](evidence/raw-r1/main.md),
[evidence/raw-r1/pins.md](evidence/raw-r1/pins.md)):

| cells | main (`pkg-main-4c365814`) | Spark (both masters) | seat A (revised) |
|---|---|---|---|
| `m_w34`, `m_n33`, `m_s2`, `m_mix`, `m_w8` ungrouped | NULL / AO, steady, all 6 configurations | `local[1]` NULL / AO; `local[4]` the quotient (codegen on) | NULL / AO (the new class) |
| the same five, whole-partition window | NULL / AO, steady | the quotient, both modes, both codegen | the quotient |
| `m_w34`, `m_n33` growing frame and a 15,000-row sliding frame | NULL past row 11,111 ([evidence/raw-r1/frames.md](evidence/raw-r1/frames.md)) | the quotient on every row | the quotient on every row |
| `t` `c38_0` window g2, g3, g5 | ANSI on AO×20 everywhere | NVOOR | NVOOR |
| `m_d32` (`decimal(9,2)` as `Decimal32`), all rows / `id < 3` | all rows `0.625` at SET 1, NULL / AO from SET 2 (SET 2 varies); `id < 3` NULL, NVOOR or AO everywhere, window too (`9999999.99` does not fit); type `decimal(9,6)` | `0.625`, `9999999.99`; type `decimal(13,6)` | Spark's answers and type |
| `m_d64` (`decimal(18,0)` as `Decimal64`), all / `id < 10` | all rows `0.2381` at SET 1, NULL / AO from SET 2; `id < 10` NULL, NVOOR or AO everywhere; type `decimal(18,4)` | `0.2381`, `999999999999999999`; type `decimal(22,4)` | Spark's answers and type |
| `m_d256` (`decimal(76,0)`, exact total `2^256 + 5`) | window `0.4167` steady everywhere; grouped and ungrouped `0.4167` / NULL / AO, VARIES at SET 2, 4, 8 and 2 cores | refuses the read (`DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`) | NULL / AO (Q6 (a)); never `0.4167` |

**The VARIES count is a lower bound.** Step 0's 96 cells, and the critic's 80 of its 1,818, are what 20 runs saw.
Added from the critic's runs (`N/avg-critic/out/rp_main.jsonl`, 2 cores, stealing on, ANSI on):

| query | ANSI | configuration | answers |
|---|---|---|---|
| `t/c38_0/g/try_avg/g6` | true | 2c on | `6|0.2`×5 / `6|NULL`×15 (critic); this revision `6|NULL`×20 |
| `t/c38_0/u/try_avg/g6` | true | 2c on | `0.2`×14 / NULL×6 (critic); this revision NULL×20 |
| `t4/c38_0/g/avg/g21` | true | 2c on | NVOOR×17 / AO×3 (critic); this revision NVOOR×18 / AO×2 |

Up: [map.md](map.md).
