# NAN-COMPARE-SPARK-1: the domain table

Measured 2026-11-13 on three RePark builds against Spark 4.1.2 with Iceberg 1.11.0, in both ANSI modes. The
builds, commands and locks are in [evidence/](evidence/map.md); every cell's answers are in
[evidence/raw/cells.tsv](evidence/raw/cells.tsv).

- **Builds:** **base** = main's product (`1d9d2808`, product-identical to `2707924e`); **1325** =
  NAN-WRITE-CANONICAL-1's head `948d7fb3`; **head3** = SIGNED-ZERO-NORMALIZE-1 part B's head `9d92adf6`.
- **NaN shapes**, two NaN rows per column (ids 2 and 9) beside the controls `1.5`, `+Infinity` (3),
  `-Infinity` (4), `-0.0` (5), `0.0` (6), NULL (7) and `-2.5` (8):
  - **in memory** (`created`, `mem` below): `createDataFrame` with the bits `7ff8000000000000` (canonical),
    `fff8000000000000` (sign-set), `7ff8deadbeef0001` (payload) and `fff8deadbeef0001` (sign-set payload);
    FLOAT `7fc00000`, `ffc00000`, `7fc00123`, `ffc00123`;
  - **computed in the query** (`computed`, `sql` below): `CAST('NaN' AS DOUBLE)`, `sqrt(m)`, `i - i` and `-n`
    over `VALUES` columns holding `-1.0`, `Infinity` and NaN. No SQL expression makes a payload NaN on either
    engine; the in-memory source carries it;
  - **stored:** the same rows in Iceberg tables written by RePark base (`rb_*`) and by Spark (`sp_*`), in
    Parquet, ORC and Avro, as one file (`l1`) or five (`l5`: `{1.5, -2.5}`, `{NaN, -0.0, NULL}`,
    `{Infinity, 0.0}`, `{NaN}`, `{-Infinity}`); tables partitioned by the float column (`*_part`, column `pk`
    holds canonical, sign-set and payload NaNs, `v` the same values unpartitioned); plain Parquet files from
    pyarrow (`pq_pa_*`), RePark (`pq_rb_*`) and Spark (`pq_sp_*`); and `nat_parquet_l5`, an Iceberg table each
    build writes with native `INSERT`.
- **Operations:** 53 SQL templates (the [probes](evidence/probes/map.md)): `max`/`min` ungrouped, grouped and as
  windows (whole, running, sliding); `greatest`/`least`; `ORDER BY` in four null placements, TopK, `rank()`;
  `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `IN`, `NOT IN`, `BETWEEN` against `NaN`, `±Infinity`, `0.5` and `0`;
  the same comparisons in projections, in `CASE`, and against another column; `array_sort`, `sort_array`,
  `array_min`, `array_max`; `count(DISTINCT)`, `approx_count_distinct`, `GROUP BY`, a join on a NaN key; and
  five cells over all four shapes at once (`mix`).
- **Doors:** the Spark door (SQL), the DataFrame API, `F.expr`, and the native door. RePark's cell is compared
  with Spark's on the same door and source. The native door is compared with Spark SQL; `nat_parquet_l5` with
  Spark's in-memory cell.
- **Verdicts:** `=` equal; `W` a different answer (NaN equals NaN whatever the bits; ids in order where order is
  the answer); `z` differs only by the sign of a zero; `E` RePark errors where Spark answers; `x` both error.
  A three-letter code reads base / 1325 / head3.

## 1. Totals

| build | ANSI | cells | EQUAL | WRONG | ZERO | R-ERR | S-ERR | BOTH-ERR | NA |
|---|---|---|---|---|---|---|---|---|---|
| base | true | 15351 | 7635 | 7095 | 262 | 359 | 0 | 0 | 0 |
| 1325 | true | 15351 | 7727 | 7001 | 264 | 359 | 0 | 0 | 0 |
| head3 | true | 15351 | 10045 | 4685 | 262 | 359 | 0 | 0 | 0 |
| base | false | 4061 | 1426 | 2499 | 45 | 91 | 0 | 0 | 0 |
| 1325 | false | 4061 | 1517 | 2406 | 47 | 91 | 0 | 0 | 0 |
| head3 | false | 4061 | 2168 | 1757 | 45 | 91 | 0 | 0 | 0 |

- **Wrong-answer cells per build, ANSI on:** base 7,095, 1325 7,001, head3 4,685 of 15,351. ANSI off (six
  sources): 2,499, 2,406, 1,757 of 4,061.
- **Not all of them are NaN.** The canonical columns are the controls. A `W` on a canonical column with `=` on
  head3 is a signed-zero cell that part B fixes (sort ties and keys between `-0.0` and `0.0`); a `W` on the
  native door's default `ORDER BY` and `array_agg` cells is the native dialect (NULLS LAST by default, NULLs kept
  in the array), present on every shape. What remains on canonical columns is NaN, and it is listed below
  (§2 `grp_*`, §6).
- **`R-ERR` (359):** `approx_count_distinct` on FLOAT and DOUBLE, on every door and build ("Support for
  'approx_distinct' for data type Float64 is not implemented"), and the five `mix` cells on some sources
  ("Physical input schema should be the same as the one converted from logical input schema", a `UNION ALL` of
  four columns of one Iceberg table). Both are outside this unit; the `UNION ALL` error is not carded and is
  recorded in the [sketch](sketch.md) §9.

## 2. Shape by operation, the Spark door, in memory and computed

`mem` columns: the `createDataFrame` source; `sql` columns: the computed source; `F`: FLOAT.

| op | mem canonical | mem sign-set | mem payload | mem sign-payload | mem canonical F | mem sign-set F | mem payload F | mem sign-payload F | sql CAST('NaN') | sql sqrt(-1) | sql inf - inf | sql -x | sql CAST('NaN') F | sql sqrt(-1) F | sql inf - inf F | sql -x F |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| agg_max | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| agg_min | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| grp_max | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| grp_min | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| grp_max_one | === | === | === | === |  |  |  |  | === | === | === | === |  |  |  |  |
| grp_min_one | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| win_max_all | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_max_run | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_min_run | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_max_slide | zzz | WWW | zzz | WWW |  |  |  |  | zzz | WWW | WWW | WWW |  |  |  |  |
| win_min_slide | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| greatest | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| least | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| ord_asc | === | WW= | === | WW= | === | WW= | === | WW= | === | WW= | WW= | WW= | === | WW= | WW= | WW= |
| ord_desc | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= |
| ord_asc_nl | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| ord_desc_nf | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| topk_asc | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| topk_desc | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| win_rank | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| f_eq_nan | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_ne_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_lt_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_le_nan | === | === | WWW | === |  |  |  |  | === | === | === | === |  |  |  |  |
| f_gt_nan | === | === | WWW | === |  |  |  |  | === | === | === | === |  |  |  |  |
| f_ge_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_nseq_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_in_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_notin_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_between | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_gt_inf | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_ge_inf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_gt_half | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_lt_zero | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_le_neginf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| p_eq_nan | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| p_gt_inf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| p_lt_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| case_gt | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| case_eq | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| f_eq_col | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| p_eq_col | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| p_lt_col | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| f_isnan | === | === | === | === |  |  |  |  | === | === | === | === |  |  |  |  |
| arr_sort | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| arr_min | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| arr_max | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| sort_arr_asc | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| sort_arr_desc | zzz | WWW | zzz | WWW |  |  |  |  | zzz | WWW | WWW | WWW |  |  |  |  |
| cnt_distinct | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= |
| approx_distinct | EEE | EEE | EEE | EEE |  |  |  |  | EEE | EEE | EEE | EEE |  |  |  |  |
| group_by | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| join_col | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |

- **Sign-set NaN (`fff8…`), however it is made, is wrong on every build** in `max`/`min`, every window
  `max`/`min`, `greatest`/`least`, every comparison with a literal, the arrays, and (before part B) every sort
  and key.
- **Canonical NaN is right** except: **grouped `max`/`min`** (`grp_max`, `grp_min`, `grp_min_one`: `WWW` on
  every shape, §3), and the signed-zero ties part B fixes (`ord_desc`, `win_rank`, `cnt_distinct`, `group_by`,
  `join_col`: `WW=`).
- **Payload NaN (`7ff8dead…`)** sorts and reduces as canonical (it is positive) but is unequal to `NaN`
  (`f_eq_nan`, `f_in_nan`, `<=>`, `case_eq`) and **greater than `NaN`** (`f_gt_nan`, `f_le_nan`).
- **Part B (head3) fixes** every sort cell (`ord_*`, `topk_*`, `win_rank`), every column-to-column comparison
  (`f_eq_col`, `p_eq_col`, `p_lt_col`), the simple `CASE` (`case_eq`), and every key (`cnt_distinct`,
  `group_by`, `join_col`). **It does not fix** literal comparisons, `max`/`min` in any form, `greatest`/`least`,
  or the array functions.
- **NAN-WRITE-CANONICAL-1 (1325) moves no in-memory cell**, as expected for a write seat.

## 3. Key answers (Spark door, in memory)

| op | shape | Spark | base | 1325 | head3 |
|---|---|---|---|---|---|
| agg_max | canonical DOUBLE | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |
| agg_max | sign-set DOUBLE | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |
| agg_min | canonical DOUBLE | [["-inf"]] | [["-inf"]] | [["-inf"]] | [["-inf"]] |
| agg_min | sign-set DOUBLE | [["-inf"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |
| grp_max | canonical DOUBLE | [[0,"NaN"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] |
| grp_max | sign-set DOUBLE | [[0,"NaN"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] | [[0,"0.0"],[1,"NaN"]] |
| grp_min | canonical DOUBLE | [[0,"-inf"],[1,"-0.0"]] | [[0,"-inf"],[1,"NaN"]] | [[0,"-inf"],[1,"NaN"]] | [[0,"-inf"],[1,"NaN"]] |
| grp_min | sign-set DOUBLE | [[0,"-inf"],[1,"-0.0"]] | [[0,"-inf"],[1,"NaN"]] | [[0,"-inf"],[1,"NaN"]] | [[0,"-inf"],[1,"NaN"]] |
| greatest | canonical DOUBLE | [[1,"100.0"],[2,"NaN"],[3,"inf"],[4,"100.0"],… | [[1,"100.0"],[2,"NaN"],[3,"inf"],[4,"100.0"],… | [[1,"100.0"],[2,"NaN"],[3,"in… | [[1,"100.0"],[2,"NaN"],[3,"inf"],[4,"100.0"],… |
| greatest | sign-set DOUBLE | [[1,"100.0"],[2,"NaN"],[3,"inf"],[4,"100.0"],… | [[1,"100.0"],[2,"100.0"],[3,"inf"],[4,"100.0"… | [[1,"100.0"],[2,"100.0"],[3,"… | [[1,"100.0"],[2,"100.0"],[3,"inf"],[4,"100.0"… |
| least | canonical DOUBLE | [[1,"-100.0"],[2,"-100.0"],[3,"-100.0"],[4,"-… | [[1,"-100.0"],[2,"-100.0"],[3,"-100.0"],[4,"-… | [[1,"-100.0"],[2,"-100.0"],[3… | [[1,"-100.0"],[2,"-100.0"],[3,"-100.0"],[4,"-… |
| least | sign-set DOUBLE | [[1,"-100.0"],[2,"-100.0"],[3,"-100.0"],[4,"-… | [[1,"-100.0"],[2,"NaN"],[3,"-100.0"],[4,"-inf… | [[1,"-100.0"],[2,"NaN"],[3,"-… | [[1,"-100.0"],[2,"NaN"],[3,"-100.0"],[4,"-inf… |
| ord_asc | canonical DOUBLE | [[7],[4],[8],[5],[6],[1],[3],[2],[9]] | [[7],[4],[8],[5],[6],[1],[3],[2],[9]] | [[7],[4],[8],[5],[6],[1],[3],… | [[7],[4],[8],[5],[6],[1],[3],[2],[9]] |
| ord_asc | sign-set DOUBLE | [[7],[4],[8],[5],[6],[1],[3],[2],[9]] | [[7],[2],[9],[4],[8],[5],[6],[1],[3]] | [[7],[2],[9],[4],[8],[5],[6],… | [[7],[4],[8],[5],[6],[1],[3],[2],[9]] |
| ord_desc | canonical DOUBLE | [[2],[9],[3],[1],[5],[6],[8],[4],[7]] | [[2],[9],[3],[1],[6],[5],[8],[4],[7]] | [[2],[9],[3],[1],[6],[5],[8],… | [[2],[9],[3],[1],[5],[6],[8],[4],[7]] |
| ord_desc | sign-set DOUBLE | [[2],[9],[3],[1],[5],[6],[8],[4],[7]] | [[3],[1],[6],[5],[8],[4],[2],[9],[7]] | [[3],[1],[6],[5],[8],[4],[2],… | [[2],[9],[3],[1],[5],[6],[8],[4],[7]] |
| f_eq_nan | canonical DOUBLE | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |
| f_eq_nan | sign-set DOUBLE | [[2],[9]] | [] | [] | [] |
| f_gt_inf | canonical DOUBLE | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |
| f_gt_inf | sign-set DOUBLE | [[2],[9]] | [] | [] | [] |
| f_lt_zero | canonical DOUBLE | [[4],[8]] | [[4],[8]] | [[4],[8]] | [[4],[8]] |
| f_lt_zero | sign-set DOUBLE | [[4],[8]] | [[2],[4],[8],[9]] | [[2],[4],[8],[9]] | [[2],[4],[8],[9]] |
| arr_sort | canonical DOUBLE | [[["-inf","-2.5","-0.0","0.0","1.5","inf","Na… | [[["-inf","-2.5","-0.0","0.0","1.5","inf","Na… | [[["-inf","-2.5","-0.0","0.0"… | [[["-inf","-2.5","-0.0","0.0","1.5","inf","Na… |
| arr_sort | sign-set DOUBLE | [[["-inf","-2.5","-0.0","0.0","1.5","inf","Na… | [[["NaN","NaN","-inf","-2.5","-0.0","0.0","1.… | [[["NaN","NaN","-inf","-2.5",… | [[["NaN","NaN","-inf","-2.5","-0.0","0.0","1.… |
| cnt_distinct | canonical DOUBLE | [[6]] | [[7]] | [[7]] | [[6]] |
| cnt_distinct | sign-set DOUBLE | [[6]] | [[7]] | [[7]] | [[6]] |

**Grouped `max`/`min`, every NaN shape.** `SELECT id % 2, max(d) … GROUP BY id % 2` answers `0.0` for the even
group (`NaN, -Infinity, 0.0, -2.5`) where Spark answers NaN, and `min` answers NaN for the odd group where Spark
answers `-0.0`; `min(d) … GROUP BY id > 0` answers NaN where Spark answers `-Infinity`. The ungrouped `max`/`min`
over the same rows is right for canonical NaN. Inventory B2: DataFusion's grouped accumulator replaces the running
value whenever `partial_cmp` returns `None`, so the answer depends on row order. This is a new finding, outside
both cards, and it reaches canonical NaN in data written by any engine.

## 4. The doors

Cells per family and door, ANSI on, all sources; then the wrong count on base / 1325 / head3.

| family | sql (cells: wrong base/1325/head3) | frame (cells: wrong base/1325/head3) | fexpr (cells: wrong base/1325/head3) | native (cells: wrong base/1325/head3) |
|---|---|---|---|---|
| agg | 436: 186/187/187 | 348: 122/122/122 | 348: 122/122/122 | 220: 117/107/115 |
| window | 470: 157/157/157 | 94: 29/29/29 |  | 190: 79/69/79 |
| greatest/least | 348: 128/128/128 | 348: 128/128/128 | 348: 128/128/128 | 148: 64/56/64 |
| sort | 818: 531/531/0 | 630: 402/402/0 |  | 338: 275/269/148 |
| compare-literal | 2200: 894/894/846 | 1072: 479/479/479 | 2200: 894/894/846 | 904: 416/370/393 |
| compare-column | 240: 113/113/0 | 80: 42/42/0 | 240: 113/113/0 | 108: 57/49/0 |
| control | 94: 0/0/0 |  | 94: 0/0/0 | 38: 0/0/0 |
| array | 550: 198/198/198 | 362: 128/128/128 | 550: 198/198/198 | 226: 161/155/161 |
| keys | 442: 348/348/0 | 268: 174/174/0 | 268: 174/174/0 | 186: 148/148/0 |
| mix | 100: 54/54/18 |  |  | 45: 36/35/11 |

- **The Spark door, the DataFrame API and `F.expr` agree** cell for cell where they run the same operation; the
  families' counts differ because the DataFrame door runs a subset of operations and `F.expr` no windows or sorts.
- **The native door** adds its dialect cells (default null order, `array_agg` keeping NULLs) on every shape;
  with the null order spelled out (`ord_asc_nl`, `ord_desc_nf`, `topk_*`) it matches the Spark door's codes. Grouped `max` with one group
  (`grp_max_one`) is right on every door only because the last row read is a NaN.

The native door's matrix (`mem` and `sql` sources):

| op | mem canonical | mem sign-set | mem payload | mem sign-payload | mem canonical F | mem sign-set F | mem payload F | mem sign-payload F | sql CAST('NaN') | sql sqrt(-1) | sql inf - inf | sql -x | sql CAST('NaN') F | sql sqrt(-1) F | sql inf - inf F | sql -x F |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| agg_max | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| agg_min | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| grp_max | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| grp_min | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| grp_max_one | === | === | === | === |  |  |  |  | === | === | === | === |  |  |  |  |
| grp_min_one | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| win_max_all | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_max_run | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_min_run | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| win_max_slide | zzz | WWW | zzz | WWW |  |  |  |  | zzz | WWW | WWW | WWW |  |  |  |  |
| win_min_slide | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| greatest | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| least | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| ord_asc | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW |
| ord_desc | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW | WWW |
| ord_asc_nl | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| ord_desc_nf | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| topk_asc | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| topk_desc | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| win_rank | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| f_eq_nan | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_ne_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_lt_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_le_nan | === | === | WWW | === |  |  |  |  | === | === | === | === |  |  |  |  |
| f_gt_nan | === | === | WWW | === |  |  |  |  | === | === | === | === |  |  |  |  |
| f_ge_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_nseq_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_in_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_notin_nan | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_between | === | WWW | WWW | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_gt_inf | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_ge_inf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_gt_half | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| f_lt_zero | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| f_le_neginf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| p_eq_nan | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| p_gt_inf | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| p_lt_nan | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| case_gt | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| case_eq | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| f_eq_col | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| p_eq_col | === | WW= | WW= | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| p_lt_col | === | WW= | === | WW= |  |  |  |  | === | WW= | WW= | WW= |  |  |  |  |
| f_isnan | === | === | === | === |  |  |  |  | === | === | === | === |  |  |  |  |
| arr_sort | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| arr_min | === | WWW | === | WWW |  |  |  |  | === | WWW | WWW | WWW |  |  |  |  |
| arr_max | === | WWW | === | WWW | === | WWW | === | WWW | === | WWW | WWW | WWW | === | WWW | WWW | WWW |
| sort_arr_asc | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| sort_arr_desc | WWW | WWW | WWW | WWW |  |  |  |  | WWW | WWW | WWW | WWW |  |  |  |  |
| cnt_distinct | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= | WW= |
| approx_distinct | EEE | EEE | EEE | EEE |  |  |  |  | EEE | EEE | EEE | EEE |  |  |  |  |
| group_by | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |
| join_col | WW= | WW= | WW= | WW= |  |  |  |  | WW= | WW= | WW= | WW= |  |  |  |  |

## 5. Storage: what each engine reads back, and the wrong count per column

Spark door (native for `nat_parquet_l5`), ANSI on, DOUBLE columns. The NaN bits are those of `array_sort`'s
result, so every NaN the read produced.

| source | column | cells | wrong base/1325/head3 | NaN bits Spark reads | base reads | 1325 reads | head3 reads |
|---|---|---|---|---|---|---|---|
| computed | dc | 53 | 9/9/3 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| computed | dp | 53 | 48/48/34 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| computed | dq | 53 | 48/48/34 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| computed | ds | 53 | 48/48/34 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| created | dc | 53 | 9/9/3 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| created | dp | 53 | 21/21/12 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| created | dq | 53 | 48/48/34 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| created | ds | 53 | 48/48/34 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| nat_parquet_l5 | dc | 53 | 16/16/11 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| nat_parquet_l5 | dp | 53 | 29/16/20 | 7ff8…0001 | 7ff8…0001 | 7ff8…0000 | 7ff8…0001 |
| nat_parquet_l5 | dq | 53 | 48/17/36 | fff8…0001 | fff8…0001 | 7ff8…0000 | fff8…0001 |
| nat_parquet_l5 | ds | 53 | 48/16/35 | fff8…0000 | fff8…0000 | 7ff8…0000 | fff8…0000 |
| part_created | pk | 49 | 40/40/30 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| part_created | v | 49 | 40/40/30 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| pq_pa_l1 | dc | 53 | 11/11/5 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_pa_l1 | dp | 53 | 19/19/10 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| pq_pa_l1 | dq | 53 | 43/43/29 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| pq_pa_l1 | ds | 53 | 43/43/29 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| pq_pa_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_pa_l5 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| pq_pa_l5 | dq | 49 | 45/45/31 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| pq_pa_l5 | ds | 49 | 45/45/31 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| pq_rb_l1 | dc | 49 | 8/8/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_rb_l1 | dp | 49 | 16/16/7 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| pq_rb_l1 | dq | 49 | 40/40/26 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| pq_rb_l1 | ds | 49 | 40/40/26 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| pq_rb_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_rb_l5 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| pq_rb_l5 | dq | 49 | 45/45/31 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| pq_rb_l5 | ds | 49 | 45/45/31 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| pq_sp_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l1 | dp | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l1 | dq | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l1 | ds | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l5 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l5 | dp | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l5 | dq | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| pq_sp_l5 | ds | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_avro_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_avro_l1 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_avro_l1 | dq | 49 | 45/45/31 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_avro_l1 | ds | 49 | 45/45/31 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_avro_l5 | dc | 53 | 8/9/4 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_avro_l5 | dp | 53 | 21/21/11 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_avro_l5 | dq | 53 | 49/48/35 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_avro_l5 | ds | 53 | 48/49/34 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_avro_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_avro_part | v | 45 | 38/38/28 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| rb_orc_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_orc_l1 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_orc_l1 | dq | 49 | 44/44/30 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_orc_l1 | ds | 49 | 44/44/30 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_orc_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_orc_l5 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_orc_l5 | dq | 49 | 41/41/27 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_orc_l5 | ds | 49 | 41/41/27 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_orc_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_orc_part | v | 45 | 34/34/24 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| rb_parquet_l1 | dc | 53 | 9/9/3 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_parquet_l1 | dp | 53 | 21/21/12 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_parquet_l1 | dq | 53 | 47/47/33 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_parquet_l1 | ds | 53 | 47/47/33 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_parquet_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_parquet_l5 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| rb_parquet_l5 | dq | 49 | 41/41/27 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| rb_parquet_l5 | ds | 49 | 41/41/27 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| rb_parquet_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| rb_parquet_part | v | 45 | 34/34/24 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| sp_avro_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_avro_l1 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| sp_avro_l1 | dq | 49 | 45/45/31 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| sp_avro_l1 | ds | 49 | 45/45/31 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| sp_avro_l5 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_avro_l5 | dp | 49 | 18/18/9 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 | 7ff8…0001 |
| sp_avro_l5 | dq | 49 | 45/45/31 | fff8…0001 | fff8…0001 | fff8…0001 | fff8…0001 |
| sp_avro_l5 | ds | 49 | 45/45/31 | fff8…0000 | fff8…0000 | fff8…0000 | fff8…0000 |
| sp_avro_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_avro_part | v | 45 | 38/38/28 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 | 7ff8…0000,7ff8…0001,fff8…0000 |
| sp_orc_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l1 | dp | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l1 | dq | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l1 | ds | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l5 | dp | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l5 | dq | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_l5 | ds | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_orc_part | v | 45 | 6/6/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l1 | dc | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l1 | dp | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l1 | dq | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l1 | ds | 49 | 6/6/0 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l5 | dc | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l5 | dp | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l5 | dq | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_l5 | ds | 49 | 7/7/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_part | pk | 45 | 7/7/2 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |
| sp_parquet_part | v | 45 | 6/6/1 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 | 7ff8…0000 |

- **Spark's Parquet and ORC writers store `7ff8…`** for every NaN shape (`sp_parquet_*`, `sp_orc_*`,
  `pq_sp_*`): after part B (head3) those tables have no wrong cell left. Spark's Avro keeps the bits, as base
  does everywhere.
- **Both engines read what is stored**, raw: a sign-set NaN in a base-written table, an Avro table or a
  pyarrow file reaches Spark's operators as `fff8…` too, and Spark still answers it as NaN. So the stored cells
  fail where the in-memory ones do.
- **1325 changes only the table it writes itself** (`nat_parquet_l5`: it reads `7ff8…` back, and its wrong
  count falls from 48 to 16 on the sign-set column). The tables written before it, and every Avro table, keep
  their bits: NWC verifier 2's finding F2 (`= NaN` and `> Infinity` on stored Avro NaNs) is the `rb_avro_*` and
  `sp_avro_*` rows here, on all three builds.
- **The identity partition column reads back canonical** on every engine and build (`*_part` `pk`: one bit
  pattern), while the same values unpartitioned (`v`) keep three.

## 6. Pruning and the statistics shortcut

ANSI on, the Spark door (native for `nat_parquet_l5`). "Spark on the source" reads the same files as RePark;
"Spark in memory" is the same rows in memory (`created`, `part_created`): Spark's row-level answer. "base scan"
is the Iceberg scan's file count (`N=`, of five or nine) or `stats` where `max` was answered from statistics.

| source | col | op | Spark on the source | Spark in memory | base | 1325 | head3 | base scan |
|---|---|---|---|---|---|---|---|---|
| nat_parquet_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| nat_parquet_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| nat_parquet_l5 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| nat_parquet_l5 | dc | f_gt_inf | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=1 |
| nat_parquet_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| nat_parquet_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["NaN"]] | [["inf"]] | N=5 |
| nat_parquet_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [] | [[2],[9]] | [] | N=2 |
| nat_parquet_l5 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| nat_parquet_l5 | ds | f_gt_inf | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=1 |
| nat_parquet_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1],[2],[9]] | [[1]] | N=4 |
| pq_pa_l1 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_pa_l1 | dc | f_eq_nan | [] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l1 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_pa_l1 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l1 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| pq_pa_l1 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_pa_l1 | ds | f_eq_nan | [] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l1 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] |  |
| pq_pa_l1 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l1 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] |  |
| pq_pa_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |  |
| pq_pa_l5 | dc | f_eq_nan | [[9]] | [[2],[9]] | [[9]] | [[9]] | [[9]] |  |
| pq_pa_l5 | dc | f_gt_half | [[1],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3],[9]] | [[1],[3],[9]] | [[1],[3],[9]] |  |
| pq_pa_l5 | dc | f_gt_inf | [[9]] | [[2],[9]] | [[9]] | [[9]] | [[9]] |  |
| pq_pa_l5 | dc | f_in_nan | [[1],[9]] | [[1],[2],[9]] | [[1],[9]] | [[1],[9]] | [[1],[9]] |  |
| pq_pa_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_pa_l5 | ds | f_eq_nan | [[9]] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l5 | ds | f_gt_half | [[1],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] |  |
| pq_pa_l5 | ds | f_gt_inf | [[9]] | [[2],[9]] | [] | [] | [] |  |
| pq_pa_l5 | ds | f_in_nan | [[1],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] |  |
| pq_rb_l1 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_rb_l1 | dc | f_eq_nan | [] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l1 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_rb_l1 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l1 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| pq_rb_l1 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_rb_l1 | ds | f_eq_nan | [] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l1 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] |  |
| pq_rb_l1 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l1 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] |  |
| pq_rb_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |  |
| pq_rb_l5 | dc | f_eq_nan | [[9]] | [[2],[9]] | [[9]] | [[9]] | [[9]] |  |
| pq_rb_l5 | dc | f_gt_half | [[1],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3],[9]] | [[1],[3],[9]] | [[1],[3],[9]] |  |
| pq_rb_l5 | dc | f_gt_inf | [[9]] | [[2],[9]] | [[9]] | [[9]] | [[9]] |  |
| pq_rb_l5 | dc | f_in_nan | [[1],[9]] | [[1],[2],[9]] | [[1],[9]] | [[1],[9]] | [[1],[9]] |  |
| pq_rb_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] |  |
| pq_rb_l5 | ds | f_eq_nan | [[9]] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l5 | ds | f_gt_half | [[1],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] |  |
| pq_rb_l5 | ds | f_gt_inf | [[9]] | [[2],[9]] | [] | [] | [] |  |
| pq_rb_l5 | ds | f_in_nan | [[1],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] |  |
| pq_sp_l1 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |  |
| pq_sp_l1 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l1 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_sp_l1 | dc | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l1 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| pq_sp_l1 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] |  |
| pq_sp_l1 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l1 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_sp_l1 | ds | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l1 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| pq_sp_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | stats |
| pq_sp_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l5 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_sp_l5 | dc | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| pq_sp_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | stats |
| pq_sp_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l5 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] |  |
| pq_sp_l5 | ds | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] |  |
| pq_sp_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] |  |
| rb_avro_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| rb_avro_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=5 |
| rb_avro_l5 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | N=5 |
| rb_avro_l5 | dc | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=5 |
| rb_avro_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=5 |
| rb_avro_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] | N=5 |
| rb_avro_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=5 |
| rb_avro_l5 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=5 |
| rb_avro_l5 | ds | f_gt_inf | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=5 |
| rb_avro_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] | N=5 |
| rb_avro_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=9 |
| rb_avro_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=3 |
| rb_avro_part | pk | f_gt_half | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | N=5 |
| rb_avro_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| rb_avro_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=4 |
| rb_orc_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| rb_orc_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| rb_orc_l5 | dc | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| rb_orc_l5 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| rb_orc_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| rb_orc_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] | N=5 |
| rb_orc_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=2 |
| rb_orc_l5 | ds | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| rb_orc_l5 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| rb_orc_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] | N=4 |
| rb_orc_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=9 |
| rb_orc_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=3 |
| rb_orc_part | pk | f_gt_half | [[1],[5]] | [[1],[2],[3],[4],[5]] | [[1],[5]] | [[1],[5]] | [[1],[5]] | N=2 |
| rb_orc_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| rb_orc_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=4 |
| rb_parquet_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| rb_parquet_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| rb_parquet_l5 | dc | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| rb_parquet_l5 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| rb_parquet_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| rb_parquet_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] | N=5 |
| rb_parquet_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=2 |
| rb_parquet_l5 | ds | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| rb_parquet_l5 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| rb_parquet_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] | N=4 |
| rb_parquet_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=9 |
| rb_parquet_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=3 |
| rb_parquet_part | pk | f_gt_half | [[1],[5]] | [[1],[2],[3],[4],[5]] | [[1],[5]] | [[1],[5]] | [[1],[5]] | N=2 |
| rb_parquet_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| rb_parquet_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=4 |
| sp_avro_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| sp_avro_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=5 |
| sp_avro_l5 | dc | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | N=5 |
| sp_avro_l5 | dc | f_gt_inf | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=5 |
| sp_avro_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=5 |
| sp_avro_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["inf"]] | [["inf"]] | [["inf"]] | N=5 |
| sp_avro_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=5 |
| sp_avro_l5 | ds | f_gt_half | [[1],[2],[3],[9]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=5 |
| sp_avro_l5 | ds | f_gt_inf | [[2],[9]] | [[2],[9]] | [] | [] | [] | N=5 |
| sp_avro_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1]] | [[1]] | [[1]] | N=5 |
| sp_avro_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=7 |
| sp_avro_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=1 |
| sp_avro_part | pk | f_gt_half | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | [[1],[2],[3],[4],[5]] | N=3 |
| sp_avro_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| sp_avro_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=2 |
| sp_orc_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| sp_orc_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| sp_orc_l5 | dc | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| sp_orc_l5 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| sp_orc_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| sp_orc_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| sp_orc_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| sp_orc_l5 | ds | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| sp_orc_l5 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| sp_orc_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| sp_orc_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=7 |
| sp_orc_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=1 |
| sp_orc_part | pk | f_gt_half | [[1],[5]] | [[1],[2],[3],[4],[5]] | [[1],[5]] | [[1],[5]] | [[1],[5]] | N=2 |
| sp_orc_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| sp_orc_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=2 |
| sp_parquet_l5 | dc | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| sp_parquet_l5 | dc | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| sp_parquet_l5 | dc | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| sp_parquet_l5 | dc | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| sp_parquet_l5 | dc | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| sp_parquet_l5 | ds | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=5 |
| sp_parquet_l5 | ds | f_eq_nan | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | [[2],[9]] | N=2 |
| sp_parquet_l5 | ds | f_gt_half | [[1],[3]] | [[1],[2],[3],[9]] | [[1],[3]] | [[1],[3]] | [[1],[3]] | N=2 |
| sp_parquet_l5 | ds | f_gt_inf | [] | [[2],[9]] | [] | [] | [] | N=1 |
| sp_parquet_l5 | ds | f_in_nan | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | [[1],[2],[9]] | N=4 |
| sp_parquet_part | pk | agg_max | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | [["NaN"]] | N=7 |
| sp_parquet_part | pk | f_eq_nan | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | [[2],[3],[4]] | N=1 |
| sp_parquet_part | pk | f_gt_half | [[1],[5]] | [[1],[2],[3],[4],[5]] | [[1],[5]] | [[1],[5]] | [[1],[5]] | N=2 |
| sp_parquet_part | pk | f_gt_inf | [] | [[2],[3],[4]] | [] | [] | [] | N=1 |
| sp_parquet_part | pk | f_in_nan | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | [[1],[2],[3],[4]] | N=2 |

**What the rows show.**

- **On the same files, Spark drops NaN rows too.** On every Parquet and ORC Iceberg table with five files,
  `d > 0.5` keeps `[1, 3]` on Spark and on RePark, though Spark's row semantics keep `[1, 2, 3, 9]`: the file
  `{NaN, -0.0, NULL}` (upper bound `0.0`/`-0.0`) and the NaN-only file are pruned by Iceberg's metrics evaluator,
  which reads `NaN > x` as false (inventory G2). `d > Infinity` keeps nothing on either engine. With a canonical
  NaN, **RePark's pruning is Spark-on-Iceberg's, exactly**, on both writers' layouts.
- **ICEBERG-NAN-FILE-PRUNE-1 is the PRUNED-WRONG class.** Its open point (the two writers' layouts) is answered:
  Spark drops the NaN row on any layout that puts it in a file whose other values are all `<= 0.5`, or alone.
  The card's Spark leg kept the row on Spark's own table, most likely because its `local[2]` writer laid the rows into other files (not re-measured here).
- **Avro data files carry no column metrics** on either writer, so nothing is pruned (`N=5`) and both engines
  keep the NaN rows: Avro is where Spark gives its row-level answer, and where RePark's bit comparison shows.
- **The Avro partition-column oddity is not Avro's.** `pk > Infinity` keeps nothing on Spark for its own
  Parquet, ORC and Avro tables alike, and on RePark's: the manifest's partition summary has upper bound
  `Infinity` (NaN is left out, `contains_nan` is true), and the manifest evaluator skips a manifest whose upper
  bound is not above the literal without reading `contains_nan` (inventory G3). The non-partition column `f`
  in the pin answers `[1..6]` because Avro data files have no bounds. `pk > 0.5` differs by format for the same
  reason: on Parquet and ORC the NaN partitions' data files are NaN-only and pruned by metrics; on Avro they are
  kept.
- **PARQUET-NAN-EQUALITY-1 is a writer's statistics, read the same way by both engines.** pyarrow and RePark
  write Parquet min/max without NaN; Spark's `parquet-mr` 1.16 writes NaN as the max (`stats.jsonl`). On a
  single pyarrow or RePark file, `d = NaN` keeps nothing on Spark and on RePark (`pq_pa_l1`, `pq_rb_l1`), and
  `d > Infinity` likewise; on Spark's own file both keep the NaN rows (`pq_sp_l1`). The card compared RePark on
  RePark's file with Spark on Spark's. RePark's cause is DataFusion's `PruningPredicate` (`min <= NaN AND NaN
  <= max` is false under totalOrder, inventory F2).
- **The statistics shortcut answers `max` without NaN.** On a single pyarrow or RePark Parquet file,
  `max(d)` is `Infinity` on every build (Spark: NaN): the footer's exact max leaves NaN out (inventory B5). On
  `pq_sp_l5` RePark answers `max` from statistics (`stats`) and is right only because `parquet-mr` stored NaN
  as the max. Where one file has no min/max (`pq_*_l5`, the NaN-only file), the shortcut does not fire.
- **Sign-set NaN adds RePark's own bit comparison on top** (`ds` rows): `= NaN` keeps nothing on base-written
  Parquet, ORC and Avro tables where Spark keeps `[2, 9]` (the fork pushes `is_nan`, which keeps the rows, and
  DataFusion's filter above it drops them, inventory G1).

## 7. The NaN producers

Folded (`SELECT sqrt(-1.0)`) and over a column, each engine, door and ANSI mode: the value and its high bits;
`jvm_raw` is Spark's `doubleToRawLongBits` over the column.

| producer | form | spark/sql/on | spark/sql/off | base/sql/on | base/sql/off | base/native/on | base/native/off | 1325/sql/on | 1325/sql/off | 1325/native/on | 1325/native/off | head3/sql/on | head3/sql/off | head3/native/on | head3/native/off |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cast_nan | column | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 |
| cast_nan | folded | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 |
| cast_nan | jvm_raw | 7ff8 | 7ff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| div00 | column | ERR ArithmeticException: [DIVIDE | None | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 |
| div00 | folded | ERR ArithmeticException: [DIVIDE | None | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 | ERR PySparkException: Execution  | None | NaN fff8 | NaN fff8 |
| div00 | jvm_raw | ERR ArithmeticException: [DIVIDE | ERR IllegalArgumentException:  |  |  |  |  |  |  |  |  |  |  |  |  |
| inf_div | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_div | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_div | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| inf_mod | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_mod | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_mod | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| inf_mul0 | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_mul0 | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_mul0 | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| inf_sub | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_sub | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| inf_sub | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| ln_m1 | column | None | None | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| ln_m1 | folded | None | None | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| ln_m1 | jvm_raw | ERR IllegalArgumentException:  | ERR IllegalArgumentException:  |  |  |  |  |  |  |  |  |  |  |  |  |
| log10_m1 | column | None | None | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 |
| log10_m1 | folded | None | None | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 | NaN 7ff8 |
| log10_m1 | jvm_raw | ERR IllegalArgumentException:  | ERR IllegalArgumentException:  |  |  |  |  |  |  |  |  |  |  |  |  |
| neg_nan | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| neg_nan | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| neg_nan | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |
| sqrt_m1 | column | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| sqrt_m1 | folded | NaN 7ff8 | NaN 7ff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 | NaN fff8 |
| sqrt_m1 | jvm_raw | fff8 | fff8 |  |  |  |  |  |  |  |  |  |  |  |  |

- **Over a column, both engines make `fff8…`** for `sqrt(-1)`, `-NaN`, `Infinity - Infinity`, `0 * Infinity`,
  `Infinity / Infinity` and `Infinity % 1` (x86's default NaN). **Folded, Spark gives `7ff8…`, RePark `fff8…`.**
  So "a NaN computed in the same query" is usually sign-set on both engines; Spark's comparisons hide it.
- **`0.0 / 0.0`** is `DIVIDE_BY_ZERO` (ANSI on) or NULL (ANSI off) on Spark and on RePark's Spark door; the
  native door returns NaN in both modes. **`ln(-1)`, `log10(-1)`** are NULL on Spark and NaN on RePark
  (LOG-NONPOSITIVE-NULL-1).

## 8. ANSI

Cells measured in both modes: 4061; verdicts that move with ANSI: base 9, 1325 9, head3 9.

| door | source | col | op | build | ANSI on | ANSI off |
|---|---|---|---|---|---|---|
| fexpr | rb_parquet_l5 | dc | arr_sort | base | ZERO | EQUAL |
| fexpr | rb_parquet_l5 | dc | arr_sort | 1325 | ZERO | EQUAL |
| fexpr | rb_parquet_l5 | dc | arr_sort | head3 | ZERO | EQUAL |
| fexpr | rb_parquet_l5 | dp | sort_arr_desc | base | ZERO | EQUAL |
| fexpr | rb_parquet_l5 | dp | sort_arr_desc | 1325 | ZERO | EQUAL |
| fexpr | rb_parquet_l5 | dp | sort_arr_desc | head3 | ZERO | EQUAL |
| frame | rb_avro_l5 | dc | arr_sort | base | EQUAL | ZERO |
| frame | rb_avro_l5 | dc | arr_sort | 1325 | EQUAL | ZERO |
| frame | rb_avro_l5 | dc | arr_sort | head3 | EQUAL | ZERO |
| frame | rb_parquet_l5 | dp | arr_sort | base | EQUAL | ZERO |
| frame | rb_parquet_l5 | dp | arr_sort | 1325 | EQUAL | ZERO |
| frame | rb_parquet_l5 | dp | arr_sort | head3 | EQUAL | ZERO |

The cells that move are `array_sort` ties between `-0.0` and `0.0` on the five-file sources, whose order
follows the files' read order in both modes on both engines; no NaN cell depends on the ANSI mode.
