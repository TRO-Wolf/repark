# SUM-DEC-I128WRAP-1 — the domain table (measured 2026-11-17)

Main `6f80793b` (#1355 merged; its product tree equals `27d6149f`, package `pkg-avg-27d6149f`) against Spark 4.1.2,
ANSI on and off. Every cell, its SQL or DataFrame form and both answers are in [evidence/raw/](evidence/raw/map.md);
the probes that produced them are in [evidence/probes/](evidence/probes/map.md). Counts below come from
`tables.py` over `raw/verdicts.jsonl`; the per-group letter grids are in `raw/grids.md`.

**Verdicts.** `EQUAL`; **silent** = `SPARK_RAISES` (Spark raises, RePark answers) or `VALUE` (both answer,
differently); **loud other** = `REPARK_RAISES` (RePark raises, Spark answers) or `BOTH_RAISE_OTHER` (both raise,
different condition). For a silent decimal cell the analyzer also tags what RePark answered, against the exact sum
of the fixture: `X` exact and past the result type (a 39-digit value in a 38-digit column), `W` wrapped and past it,
`w` wrapped and inside it (a plausible 38-digit wrong value), `k` exact and inside it (Spark overflowed on its own
path; the sticky class).

## 1. The fixtures

- **`s`** (one Parquet file; also written as **`s4`**, four files by `id % 4`): twelve decimal columns, `(10,0)`,
  `(10,2)`, `(18,0)`, `(18,4)`, `(19,0)`, `(19,9)`, `(28,0)`, `(28,2)`, `(34,4)`, `(38,0)`, `(38,10)`, `(38,38)`, and
  fourteen groups whose rows are unscaled patterns (`M` is the column's largest value, `10^p − 1`):

  | g | shape | rows | exact sum at p = 38, scale 0 | where it lands |
  |---|---|---|---|---|
  | 1 | `fits` | 105, 255, −10 | 350 | fits |
  | 2 | `fits_nulls` | 100, NULL, 200, NULL | 300 | fits |
  | 3 | `all_null` | NULL, NULL | NULL | — |
  | 4 | `max` | M | M | fits |
  | 5 | `max_plus_unit` | M, 1 | 10^38 | past the type, inside `i128` |
  | 6 | `two_max` | M, M | 2·10^38 − 2 | wraps `i128` (39 digits after the wrap) |
  | 7 | `three_max` | M, M, M | 3·10^38 − 3 | wraps to `−40282366920938463463374607431768211459` (38 digits) |
  | 8 | `neg_max_minus_unit` | −M, −1 | −10^38 | past the type |
  | 9 | `two_neg_max` | −M, −M | −2·10^38 + 2 | wraps |
  | 10 | `pass_back` | M, M, −M, −M, 5 | 5 | passes the type and comes back |
  | 11 | `pass_back_unit` | M, 1, −1 | M | passes and comes back |
  | 12 | `cancel` | M, −M, M, −M, 7 | 7 | never passes in row order |
  | 13 | `max_minus_unit` | M, −1 | M − 1 | fits |
  | 14 | `unit` | 1 | 1 | fits |

- **`p2`** (two files; `f` is the file): `split_pass_back` (21: `M, M` | `−M, −M, 5`), `split_total_past` (22: `M` |
  `1`), `split_fits` (23: `M, −M` | `M, −M, 5`), `split_back_inside` (24: `M, M, −M` | `−M`), at `(10,2)`, `(38,0)`,
  `(38,38)`.
- **`big`** (one file, 47,104 rows): `(34,0)` and `(34,4)`; 31 = 10,001 × M (ends at 1.0001·10^38, 39 digits), 32 =
  17,100 × M (wraps `i128`), 33 = 10,001 × M, 10,001 × −M, 5 (passes and comes back).
- **`k`** (in memory; `kp` the same rows as Parquet): `INT`, `BIGINT`, `DOUBLE` controls: fits, `BIGINT` over and
  under (`Long.MaxValue + 1`, `Long.MinValue − 1`), `BIGINT` pass-back, all NULL, two `Int.MaxValue`, two `1e308`.

**Unreachable by construction.** At input precision `p ≤ 28` the sum type is `decimal(p + 10, s)`, so passing it
needs more than 10^10 rows of maximum-magnitude values, and wrapping `i128` more than 1.7·10^10 rows. Those
precisions are measured for "fits" only (every shape above is exact there); `p = 34` needs 10^4 rows (table `big`);
`p = 38` needs two.

**Partitions.** With default confs Spark packed the four `s4` files and the two `p2` files into **one** input split
each (small files share a split; the order is by file size). A second pass (`run_mp.py`, cells keyed `mp/…`) raised
`spark.sql.files.openCostInBytes` to 128 MiB and measured 4 and 2 partitions (`data/s4_partitions`,
`data/p2_partitions`). RePark read `p2` as two file groups into six partitions in both passes (EXPLAIN,
`explain/grouped_p2`).

## 2. Headline counts

**Spark door** (SQL, DataFrame, Iceberg), per ANSI mode; `sum` is the decimal family, `control` the `INT`, `BIGINT`,
`DOUBLE` cells:

| family | cells | ANSI on: EQUAL | both raise | silent | loud other | ANSI off: EQUAL | silent | loud other | format |
|---|---|---|---|---|---|---|---|---|---|
| decimal `sum` | 1,656 | 1,055 | 28 | **522** | 51 | 1,071 | **532** | 45 | 8 |
| `try_sum` (decimal) | 399 | 373 | 0 | 26 | 0 | 371 | 28 | 0 | 0 |
| controls (`INT`, `BIGINT`, `DOUBLE`) | 150 | 133 | 0 | **17** | 0 | 150 | 0 | 0 | 0 |
| `avg` over the same fixtures | 330 | 208 | 110 | 10 | 2 | 320 | 10 | 0 | 0 |

- **Every silent decimal `sum` cell is at p ≥ 34** (p = 38: 512 on, 522 off; p = 34: 10 per mode, all on `big`).
  At p 10, 18, 19 and 28 every cell agrees except the missing `F.sum_distinct` (§5).
- The 51 loud-other cells (on): 30 `F.sum_distinct` (`AttributeError`; 21 where Spark answers, 9 where it raises),
  10 Iceberg statements whose writer refuses a 39-digit value (§6), 9 `sum(x) + 1` cells (§4), 2 `beside`.
  The 45 (off): the same 30, the same 10 Iceberg statements, 5 `sum(x) + 1`.

Decimal `sum` by position, Spark door (silent cells; loud other in brackets):

| position | cells per mode | silent, ANSI on | silent, ANSI off | what RePark answered (tags, ANSI on) |
|---|---|---|---|---|
| aggregate: ungrouped, grouped, all groups, empty, `s4`, `p2`, `big`, the `mp` pass | 712 | 145 | 145 | X 51, W 43, w 21, k 39 |
| `DISTINCT` (grouped, ungrouped, all groups) | 96 | 18 | 18 | X 12, k 3, and 3 all-group cells |
| windows: whole partition, `ROWS` sliding, running, `RANGE`, default frame, parallel | 220 | 121 | 121 | 39-digit or wrapped rows |
| expressions: `beside`, `HAVING`, `+ 1`, `CAST … AS STRING`, `sum(x + 1)`, CTE, scalar subquery, temp view | 248 | 72 (11) | 82 (5) | §4 |
| DataFrame door (12 forms, §5) | 330 | 150 (30) | 150 (30) | X 24, W 24, w 24, k 24 |
| Iceberg statements (§6) | 50 | 16 (10) | 16 (10) | w 8, k 8 |
| **total** | **1,656** | **522 (51)** | **532 (45)** | |

**Native door** (`repark.sql()`, a sample over `VALUES` views), against Spark in each mode: decimal `sum` 358 cells,
silent 142 (on) / 136 (off); `try_sum` 84 cells, all `UNRESOLVED_ROUTINE`; `BIGINT` controls silent 15 (on), 0 (off).
§7.

## 3. Spark's overflow, where it is checked (measured)

The jar's rules are in [sketch.md](sketch.md) §2. What the cells show about **where** the check runs:

| path | Spark 4.1.2 | cells |
|---|---|---|
| grouped (hash aggregate with keys) | the running sum is checked **after every row**; once past `decimal(38, s)` it is NULL for good (sticky): ANSI on `ARITHMETIC_OVERFLOW` "Overflow in sum of decimals. Use 'try_sum' …", off NULL | `grouped/g10`, `g11` (`k`); `grouped_big/g33`; `mp/grouped_p2/g24` |
| ungrouped, one partition | unchecked inside the partial; the partial's final value is checked when it enters the shuffle row | `ungrouped/g10` = 5, `ungrouped/g11` = M (equal); `ungrouped/g5` raises `ARITHMETIC_OVERFLOW` |
| ungrouped, several partitions | each partial checked at the shuffle; the merged total checked at evaluate | `mp/ungrouped_p2/g21` raises (`M, M` in one partial); `mp/ungrouped_p2/g24` = 0; `mp/ungrouped_p2/g22` raises **`NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`** (both partials fit, the total does not) |
| windows | each output row's frame sum is checked at evaluate, not sticky: ANSI on `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, off NULL for that row only | `win_running/g10` off: `M; NULL; M; 0; 5`; `win_part/g10` = 5 on every row (equal) |
| `try_sum` | `Sum` in `TRY` mode: the same buffer, NULL where `sum` would raise | `try/ungrouped/g10` = 5, `try/grouped/g10` = NULL |
| `BIGINT` | `Add` in the eval mode, per row: ANSI on `ARITHMETIC_OVERFLOW` "long overflow. Use 'try_add' …", even when the sum would come back; off wraps | `int/grouped/g42`, `g44` |

Two consequences: the outcome of a pass-and-come-back group depends on row order and partition layout (`p2` group
24 answers 0 in one split and raises in two), and an ungrouped total past 38 digits raises a different condition
with one partition (`ARITHMETIC_OVERFLOW`) than with two (`NUMERIC_VALUE_OUT_OF_RANGE`).

RePark (DataFusion's `Sum`, §inventory): the native `i128` sum wraps silently in every path, and the result is never
checked against `decimal(38, s)`; so it answers `X`, `W` or `w` wherever Spark overflows on the final value, and the
exact value (`k`) wherever Spark overflowed only on its path.

## 4. The positions, cell by cell (p = 38; p = 34 on `big` behaves the same at its row counts)

Representative answers, ANSI on (off: Spark answers NULL where it raises; RePark answers the same as on):

| cell | Spark | RePark | tag |
|---|---|---|---|
| `sum/ungrouped/g5/c38_0` (M + 1) | `ARITHMETIC_OVERFLOW` | `100000000000000000000000000000000000000` in `decimal(38,0)` | X |
| `sum/grouped/g6/c38_0` (M + M) | `ARITHMETIC_OVERFLOW` | `-140282366920938463463374607431768211458` | W |
| `sum/grouped/g7/c38_0` (3 × M) | `ARITHMETIC_OVERFLOW` | `-40282366920938463463374607431768211459` | w |
| `sum/ungrouped/g5/c38_38` | `ARITHMETIC_OVERFLOW` | `1.00000000000000000000000000000000000000` in `decimal(38,38)` | X |
| `sum/grouped_big/g31/c34_4` (10,001 × M) | `ARITHMETIC_OVERFLOW` | `10000999999999999999999999999999998.9999` in `decimal(38,4)` | X |
| `sum/grouped_big/g32/c34_4` (17,100 × M) | `ARITHMETIC_OVERFLOW` | `-16928236692093846346337460743176822.8556` | W |
| `sum/grouped/g10/c38_0` (pass-back) | `ARITHMETIC_OVERFLOW` | `5` | k |
| `sum/win_rows2/g10/c38_0` | `NUMERIC_VALUE_OUT_OF_RANGE` (off: `M; NULL; 0; NULL; −M + 5`) | `M; −1.40·10^38 (wrapped); 0; +1.40·10^38; −M + 5` | W per row |
| `sum/having/g5/c38_0` (`HAVING sum(x) > 0`) | raises (off: no row) | keeps group 5 | — |
| `sum/as_string/g5/c38_0` | raises (off: NULL) | `'10000000000000000000000000000000000000'` — the 39-digit value **printed with 38 digits** (10^37) | — |
| `sum/plus1/g5/c38_0` (`sum(x) + 1`) | `ARITHMETIC_OVERFLOW` (off: NULL) | `NUMERIC_VALUE_OUT_OF_RANGE` (off: NULL, equal by accident: RePark's `+` nulls the out-of-range operand) | loud other |
| `sum/plus1/g7/c38_0` | `ARITHMETIC_OVERFLOW` | `-40282366920938463463374607431768211458` (the wrap, plus one) | silent |
| `sum/distinct/g5/c38_0` | `ARITHMETIC_OVERFLOW` | `10^38` | X |
| `sum/cte/g5`, `sum/scalar_sub/g5`, `sum/temp_view/g5` | raise | the same `10^38` | X |
| `sum/beside/g5/c38_0` (`sum, count, avg, max`) | `ARITHMETIC_OVERFLOW` | `ARITHMETIC_OVERFLOW` from RePark's `avg` (hint `try_avg`) | both raise |
| `sum/beside/g10/c38_0` | `ARITHMETIC_OVERFLOW` | `5`, `avg` exact (AVG-GROUPED-SUM-STICKY-1 for the `avg` column) | k |
| `sum/linear/g9/c38_0` (`sum(x + 1), sum(x + 2)`) | raises (off: NULL) | wrapped 39-digit values: DataFusion's linear rewrite does not fire (RePark's `+` is a UDF), the per-row add is RePark's | W |

- Empty input (`empty_*`), the all-NULL group and NULL rows agree everywhere (NULL; no row when grouped).
- `sum/grouped_all/*` (every group in one query): ANSI on, Spark raises for the whole query at p = 38; RePark answers
  every group, with the `X`, `W`, `w`, `k` values above.
- Windows: 121 of 220 cells per mode. The `ROWS` sliding frame retracts with wrapping arithmetic, so a frame whose
  true sum fits comes back right (`[M, −M]` = 0); every frame or running sum past the type answers a 39-digit or
  wrapped value where Spark raises (on) or answers NULL for that row (off). The whole-partition frame of a pass-back
  group agrees (Spark checks it once). `win_parallel` (two window expressions, no `PARTITION BY`, so
  `ParallelWindowExec`) answers the same wrong rows.

## 5. The DataFrame door

Twelve forms per column over all groups (twelve columns) and per group (groups 1, 5, 6, 7, 10, 11 at the three p = 38
columns): `F.sum`, `GroupedData.sum`, `agg({c: "sum"})`, `F.expr("sum(c)")`, `selectExpr("sum(c)")`,
`select(F.sum(c))`, `agg(F.sum(c))`, `F.try_sum`, `F.sum(c).over(rowsBetween(-1, 0))`, `F.sum(c).over(partitionBy)`,
`repartition(4)` then `groupBy`, `F.sum_distinct`.

- Every `sum` form answers what the SQL door answers: 150 silent cells per mode (24 `X`, 24 `W`, 24 `w`, 24 `k`
  among the per-group cells; the all-group and window cells carry several values each).
- `F.try_sum`: 30 cells, all equal (grouped; RePark's `try_sum` checks per row, as Spark's grouped buffer does).
- `F.sum_distinct` (and `F.sumDistinct`) **do not exist** on RePark's facade: 30 cells raise `AttributeError`.
  Already on the FNP-AGG-1 list (PR #625, a draft, not merged); not this unit's.
- `BIGINT` (`intdf/F.sum/g42`, `g44`): wraps with ANSI on where Spark raises (2 cells).

## 6. Iceberg (local v2 hadoop catalog `lc`)

Per column (`c10_2`, `c38_0`, `c38_38`) and scope (`all` groups; `fit` = groups 1, 2, 3, 4, 12, 13, 14; `wrap` =
groups 1 and 7; `back` = groups 1, 10, 11): `CREATE TABLE … AS SELECT g, sum(c) …`, `INSERT INTO … SELECT`, and
`MERGE` with a `sum` source, copy-on-write and merge-on-read; plus a `big` CTAS (`all`, and groups 32, 33) and a
`try_sum` CTAS. The stored rows and schema are read back, and every data file under the table is read with
`pyarrow`.

| scope | statements per mode | ANSI on | ANSI off |
|---|---|---|---|
| `c10_2`, every scope; `fit` at p = 38; `try_sum` CTAS | 25 | equal | equal |
| `wrap` at `c38_0`, `c38_38` | 8 | Spark raises; **RePark commits the wrapped value** | Spark stores NULL; **RePark commits the wrapped value** |
| `back` at `c38_0`, `c38_38` | 8 | Spark raises; RePark commits the exact 5 and M | Spark stores NULL; RePark commits 5 and M |
| `all` at `c38_0`, `c38_38`; the two `big` CTAS | 10 | both raise: RePark's writer `DataInvalid => Decimal value … is too large for precision 38` | Spark stores NULLs; RePark raises `DataInvalid` |

**The wrong writes, named** (the same in both modes):

1. **Committed, 8 per mode:** `ice/ctas/wrap/c38_0`, `ice/insert/wrap/c38_0`, `ice/merge_cow/wrap/c38_0`,
   `ice/merge_mor/wrap/c38_0`, and the same four at `c38_38`. Group 7's true sum is
   `299999999999999999999999999999999999997`; the table and its data file hold
   `-40282366920938463463374607431768211459` (`-0.40282366920938463463374607431768211459` at scale 38), read back
   by RePark and found in the Parquet file (`file_wrap_38`).
2. **Uncommitted files, 2 per mode:** `ice/insert/all/c38_0` and `ice/insert/all/c38_38`. The statement fails with
   `DataInvalid` and the table reads zero rows, but the data files left under the table's `data/` directory hold
   `100000000000000000000000000000000000000` (39 digits), `±140282366920938463463374607431768211458` (39-digit wraps)
   and the 38-digit wrap (`file_past_precision_orphan`, `file_wrap_39_orphan`, `file_wrap_38_orphan`). The writer
   checks precision from each file's statistics after the file is written (inventory row I-3), so the file stays.
   `CTAS` and `MERGE` of the same source left no file holding such a value.

No 39-digit value reaches a committed file: the writer refuses it. A wrapped value that lands inside 38 digits is
committed. The `back` rows are exact values that Spark refuses (the sticky class); they are not wrong digits.

## 7. The native door (`repark.sql()`)

A sample over `VALUES` views of the same rows: ungrouped, grouped, all groups, empty, `ROWS` sliding, whole
partition, running, `DISTINCT`, `try_sum`, `+ 1`, `CAST … AS STRING`, `HAVING`, `sum(x + 1)`, `beside`, CTE, scalar
subquery, view, at `c10_2`, `c38_0`, `c38_38`, and every control cell.

- Decimal `sum`: the same wraps and 39-digit values (DataFusion's `Sum`): 142 silent (on), 136 (off).
- `try_sum` does not resolve (`UNRESOLVED_ROUTINE`): 84 cells, and 21 control cells.
- 30 `TYPE` cells: `sum(x) + 1` and `sum(x + 1)` take the native door's decimal common type (`decimal(23,2)` where
  Spark has `decimal(21,2)`), the DECIMAL-COMMON-TYPE-NATIVE-1 class, not this unit's.
- `BIGINT` wraps with ANSI on (15 cells), as on the Spark door; the native door has no ANSI flag.

## 8. Controls

- **`BIGINT` overflow**, Spark door: ANSI on, 17 cells where Spark raises `ARITHMETIC_OVERFLOW` ("long overflow")
  and RePark answers the wrapped value: ungrouped, grouped, `DISTINCT`, `ROWS` sliding, running, at groups 42, 43,
  44, and two DataFrame cells. ANSI off: all equal (both wrap). `try_sum` of `BIGINT`: equal (NULL). `INT` and
  `DOUBLE`: equal (`Int.MaxValue × 2` as `BIGINT`; `1e308 + 1e308` = `Infinity`). Their own class; see sketch §5.
- **`avg` over the same fixtures** (main's state after #1355, which a build must not move): 330 cells per mode.
  ANSI on: 208 equal, 110 both raise (same condition), 10 silent, 2 both raise with RePark's
  `NUMERIC_VALUE_OUT_OF_RANGE` for Spark's `ARITHMETIC_OVERFLOW` (`avg/grouped/g11/c38_0`, `c38_10`: the quotient
  `M / 3` leaves `decimal(38,4)`). ANSI off: 320 equal, 10 silent. The 10 silent cells are the ruled
  AVG-GROUPED-SUM-STICKY-1 class (grouped pass-back: `avg/grouped/g10` × 3, `g11/c38_38`, `grouped_p2/g23` × 2,
  `grouped_avg_big/g33` × 2, `mp/avg/grouped_p2/g24` × 2).

## 9. `try_sum` (datafusion-spark's `SparkTrySum`)

26 (on) / 28 (off) cells of 399 differ. Two kinds:

1. **RePark answers NULL where Spark answers the exact sum, 20 per mode.** RePark's `try_sum` checks the precision
   after every row and stays NULL (`try_sum.rs:189-220`), in every path; Spark checks per row only in a grouped
   buffer. So an ungrouped or whole-partition window sum that passes 38 digits and comes back is NULL on RePark:
   `try/ungrouped/g10`, `g11` (× 3 columns), `try/win_part/g10`, `g11` (× 3), `try/ungrouped_p2/g21`, `g24` (× 2),
   `try_sum/ungrouped_try_big/g33` (× 2), `mp/try/ungrouped_p2/g24` (× 2).
2. **Layout-dependent, 6 (on) / 8 (off).** Both engines check per row, on different row orders and partials:
   `try/grouped_p2/g23`, `g24`, `try/s4/g11`, `mp/try/s4/g11`. Neither answer is stable across layouts; pin none.

Grouped `try_sum` of a pass-back group agrees (both NULL), and so does every sliding-frame `try_sum` (DataFusion
re-scans the frame for an accumulator without retract, as WIN-SLIDE-1 recorded).

## 10. Other causes measured beside (not this unit's)

- **A Parquet `BIGINT` column whose values span `Long.MinValue … Long.MaxValue`** makes any filtered read of the
  file fail in DataFusion's statistics: `Internal error: Assertion failed: (0.0..=1.0).contains(&selectivity):
  Selectivity is out of limit: NaN` (`side/parquet_bigint_stats/g41`, `g42`, both modes; Spark answers). The same
  assertion as DECIMAL-CAST-SELECTIVITY-1 (S2), reached here by a plain `WHERE g = 42` with no cast. The controls
  therefore run over an in-memory view.
- `CAST(decimal AS STRING)` with ANSI off: Spark prints small values in scientific notation (`3.50E-8`), RePark in
  plain notation (8 `FORMAT` cells, `sum/as_string/g1`, `g2`, `g12`, `g14` at `c38_10`, `c38_38`).
- `sum(x) + 1` at `c38_38` raises `Arrow error: Arithmetic overflow` where Spark answers `decimal(38,36)` (`plus1/g4`,
  `g13`): the decimal `+` at scale 38, the DECIMAL-MULTIPLY-OVERFLOW-1 family.

## 11. Sampling

- Full grids (every group × every column): `ungrouped`, `grouped`, `avg_grouped`.
- p = 38 columns only (uniform across precisions below, by the arithmetic above): `DISTINCT`, `HAVING`,
  `CAST … AS STRING`, the window forms per group (groups 4 to 13), `try_sum` windows, `s4` `try_sum`, `avg`
  ungrouped and `s4`.
- `c10_2`, `c28_2` and the p = 38 columns: `s4`; `c10_2` with p = 38: `beside`, `+ 1`, `try_sum` grouped and
  ungrouped (with `c34_4`); `c10_2`, `c38_0`: `sum(x + 1), sum(x + 2)`.
- Every column over all groups: `grouped_all`, `empty_*`, `distinct_all`, `try_grouped_all`, `having_all`, the five
  window forms, and every DataFrame form.
- One column (`c38_0`) for the CTE, the scalar subquery and the temp view (groups 1, 5, 6, 10), and `win_parallel`.
