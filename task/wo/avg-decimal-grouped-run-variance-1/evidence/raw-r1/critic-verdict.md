claude-opus-5-5
VERDICT: UNSOUND

# Critic verdict: AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 (PR #1365, `acc58101`, workspace `92be8309`)

I am calling this UNSOUND under the brief's definition, which says a reading or a seat is wrong. Two Spark readings are
wrong, and measurement shows it: the window rule and the ungrouped final merge. Seat A's `Decimal256` row and the
narrow-native return type are also wrong. As a result, the claim "the exact answer never raises or is NULL where Spark
answers" is false, and so is the cell that Q5's default rests on. The repair is narrow. It is a delta to sketch §1, §4
and §6 plus named pins, not a redo. The core of seat A still stands: an exact grouped sum, state chosen by precision,
and a check at `evaluate`. The section "What flips this to GAPS" at the end lists the repair.

Everything below was measured with my own scripts under `N/avg-critic/probe/`. The logs are in `N/avg-critic/logs/` and
the raw runs in `N/avg-critic/out/`. Every RePark run printed `.../pkg-main-4c365814/repark/_native.abi3.so`. Each
`target_partitions` was set before any table was read, and each RePark cell ran 20 times.

## Step 2: Spark's rule, read from the jars myself

I extracted the classes to `N/avg-critic/cls/` and read them with zulu-17 `javap -c -p`.

- `Average.sumDataType`: `DecimalType.bounded(p + 10, s)`. `add()`: for a decimal it builds
  `new DecimalAddNoOverflowCheck(l, r, l.dataType)`. Update: `add(sum, coalesce(child.cast(sumType), 0))`. Merge:
  `add(sum.left, sum.right)`. Evaluate: `If(count = 0, null, DecimalDivideWithOverflowCheck(sum, count.cast(LongDecimal),
  resultType, ctx, evalMode != ANSI))`. There is **no check on the sum at evaluate**. This agrees with step 0's
  `javap.md`.
- `DecimalDivideWithOverflowCheck.eval`: a NULL sum gives NULL, or `overflowInSumOfDecimalError` under ANSI. A
  non-NULL sum goes through `Decimal.$div`, which divides at scale `MAX_SCALE + 1` = 39 with HALF_UP, then through
  `toPrecision(p, s, HALF_UP)`. This agrees with step 0. The scale-39 divide could in principle round twice. I probed
  it with `decimal(38,34)`, 11 rows, sum `5e-34`: Spark and RePark both answer `...4545`, so there is no divergence.
- The only place the sum is checked is the buffer write: `UnsafeRow.setDecimal` (`changePrecision`, then
  `setNullAt` at bytecode 118 to 126) and `UnsafeRowWriter.write(int, Decimal, int, int)` (bytecode 15 to 35).
  `SpecificInternalRow` holds a `Decimal` object and checks nothing. Which buffer an operator uses therefore decides
  Spark's rule. I measured each case at `local[1]` and `local[4]` (`out/sp_cg.jsonl`, `out/sp_hunt2.jsonl`):

| operator | buffer | sum checked | measured |
|---|---|---|---|
| grouped hash aggregate (codegen or not) | `UnsafeRow` | **per row**, sticky NULL | `m_slide` g: NULL / AO at both masters (exact `0.5556`) |
| no-grouping aggregate, whole-stage codegen (the default) | Java locals | **once per partial** at the partial output row | `big` u `local[1]` `0.0167` (running sum crosses 38 digits). Confirms step 0. |
| no-grouping aggregate, **codegen off** | `UnsafeRow` (`TungstenAggregationIterator`) | **per row** | `big` u `local[1]` with `spark.sql.codegen.wholeStage=false`: **NULL**, not `0.0167` |
| no-grouping **final merge**, codegen | Java locals | **never** | `m_w34` u `local[4]`: **`9000000000000000000000000000000000`** (merged total `1.8e38`), where `local[1]` gives AO |
| `ObjectHashAggregate` (a `collect_list` in the same SELECT), under 128 groups | `SpecificInternalRow` | once per partial only | `t4` g24 `1` and `m_slide` `0.5556`, against NULL from the plain hash aggregate |
| `ObjectHashAggregate` past the 128-group fallback | sort-based, `UnsafeRow` | per row | `big` with `collect_list`: all 3,000 groups NULL |
| window (whole partition or sliding) | `SpecificInternalRow` (`AggregateProcessor`) | **never**: only the quotient is checked | `m_w34` window: **`9e33` at both masters, both modes**. `t` g2/g3/g5: NVOOR at both |

**Where step 0 is wrong:**
- **S2-a (sketch §1, "Merge"):** "a merge whose running total leaves the buffer is NULL too" holds for grouped merges
  only. The ungrouped final merge under codegen never checks, so Spark `local[N]` ungrouped can answer a value whose
  exact total is past 38 digits.
- **S2-b (sketch §1, "Windows"; Q5):** "a whole-partition total past 38 digits reaches the divide and raises
  NVOOR". Spark raises only when the quotient also overflows. When the average is under `10^34` it **answers**, steadily
  and at both masters. The windows have a steady, layout-free Spark rule: no sum check, quotient only. Seat A's window
  path, with one sum check at `evaluate`, contradicts it.
- **S2-c (minor):** the per-partial ungrouped rule holds only with whole-stage codegen on, and the grouped per-row rule
  disappears under `ObjectHashAggregate` below 128 groups. Both strengthen Q2's point that Spark has no steady grouped
  rule.

## Step 3: my own inventory against `inventory.md`

I ran `test -e` on every cited DataFusion file: all 8 exist under `~/.cargo/registry/src/*/`. I read the cited lines.
`aggregate.rs:732-741` is `state_fields`, `row_hash.rs:999` and `:1013` are update and merge, `:1241` is `fn spill`,
`no_grouping.rs:469-470`, `window_expr.rs:324-325`, `config.rs:653` is `enable_file_stream_work_stealing`. The
`avg_decimal.rs` cites all hold at `92be8309`: A3 `:303`, A4 `:319`, A5 `:339`, A6 `:406`, A8 `:424`, A9 `:437`, A7
`:455`, A12 `:526`, A15 `:551`, A13 `:576`, A14 `:609`, A16 `:659`, A11 `:219`/`:242`/`:254`.

Differences and missing paths:
1. **W3, return type: wrongly marked "no trust".** At `aggregate.rs:163-189`, `Decimal32` caps the result at
   `Decimal32(9, s+4)` and `Decimal64` at `Decimal64(18, s+4)`. Spark's result is `decimal(p+4, s+4)`, which is
   `decimal(13,6)` for a `decimal(9,2)` input. Measured (`out/rp_narrow.jsonl`, `target_partitions` 1, steady):
   `avg` of three `9999999.99` in a `decimal32(9,2)` column answers **NULL / NVOOR**, where Spark answers
   `9999999.99`. `decimal64(18,0)` × 10 at `999999999999999999` also answers NULL / NVOOR, where Spark answers the value.
   This is a steady wrong answer at one partition. It is not layout, and Q4 does not name it.
2. **`Decimal256`'s add is `wrapping_add` with no count** (`avg_decimal.rs:129-134`), and its `state` still ships the
   marker. Seat A's row, "unchanged (i256 does not wrap at these row counts)", is refuted by
   `m_d256` (`decimal256(76,0)`, 12 rows, exact total `2^256 + 5`): main answers **`0.4167`** in the window (steady)
   and in grouped and ungrouped cells (VARIES with NULL in 5 of 7 configurations). The exact answer is past 76 digits.
   `sum` answers **`5`** on the same file, outside this unit (Decimal256 `sum` is not #1360's carrier).
3. **Spill is a second trigger, independent of layout:** `N/avg-critic/spilldata` holds 200,000 groups as
   `M, M` and then `-M, -M, 5`, exact `1.0`, `target_partitions` 1. No pool: 200,000 of 200,000 groups exact. With a
   **16 MB pool**: **0 of 200,000** non-NULL (ANSI off), AO in 3 of 5 (ANSI on). The inventory lists spill under
   A13 but nothing measures it.
4. **Skip-partial and pools move answers too:** `big` grouped at SET 4 gives 398 NULL groups by default, and **0**
   with `skip_partial_aggregation_probe_rows_threshold = 1, ratio 0.0` (A16 path), or with a 3 MB pool
   (`out/rp_spill.jsonl`).
5. **Not inventoried:** (a) `crates/repark-distributed/src/codec.rs`, which ships aggregate state between workers.
   The new state schema must round-trip there. (b) The analyzer's `Place` (`aggregate.rs:311-369`): avg windows reach
   `legacy_avg_udaf` / `spark_sum::placed`, and avg has no `Place`. Seat A needs one to follow Spark's window rule, and
   that rule now also routes `sum` (#1360). (c) `avg(DISTINCT c), count(DISTINCT id)` **refuses**
   (`DistinctAvgAccumulator`, `aggregate.rs:194-197`) where Spark answers `[1, 8]`. That is a refusal outside the card;
   file it.
6. **Line drift:** the inventory's `aggregate.rs` lines are at `6903fb48`. At the build's base (`77faf69e` merged) `state_fields` is
   `:262-299`, and **`aggregate.rs` is 989 lines against the 1,000 ceiling**. The sketch says 959. Seat A's `state_fields`
   and `Place` changes will not fit in 11 lines, so the build must plan a move.

## Step 4: re-measurement (main `pkg-main-4c365814`, Spark 4.1.2)

The RePark sample is 101 queries × 2 modes × 9 configurations (2/4/8/10 cores with stealing on, 4 and 8 off, SET
2/4/8) = **1,818 cells**, 20 runs each (`out/rp_main.jsonl`, `probe/compare.py`):
- **VARIES: 80 cells** over `big`, `t` and `t4`, both modes, all 9 configurations. 71 of them also vary in step 0's raw
  runs.
- **Against step 0, cell for cell: 1,799 agree and 19 disagree.** Every disagreement is a rare second answer (1 to 5
  of 20) that one side saw and the other did not. There is no conflicting value. 9 cells step 0 calls steady vary
  here, for example `t/c38_0/g/try_avg/g6` 2c on (`0.2`×5 / NULL×15), `t/c38_0/u/try_avg/g6` 2c on, `t4/c38_0/g/avg/g21`
  ANSI on 2c (NVOOR×17 / AO×3), and `big/c/u/avg/all` ANSI off SET 2. **Treat "96 VARIES cells" as a lower bound.**
  `t` g6 belongs in the VARIES list.
- **Steady but not exact, precision 38: 421 cells** (`probe/judge.py`, my own exact model). **Controls at precision 10
  to 28** (`c10_2`, `c18_0`, `c19_0`, `c20_0`, `c28_4`; 4 queries × 2 modes × 9 configurations = 360 cells): every
  one is steady and exact.
- **Over-precision** (`S/verify-sum/overprec/files/` only; single file and the file unioned with itself; 60
  queries × 14 configurations = 840 cells): 224 are not exact, steady and never varying. Those are ungrouped `avg` /
  `try_avg` on `wrap5` and `neg` at p19/p20 (`1.25` / `-1.25` where the exact answer is NULL / AO). The grouped cells
  are exact. This agrees with step 0. The self-union answers `1.25`, not step 0's `1`, because it has no extra `0` row.
- **Narrow natives:** `decimal32(9,2)` (`M,M,M / -M,-M,-M / 5 / 0`, exact `0.625`) varies in grouped and ungrouped
  cells, both modes. **`Decimal64` does wrap:** `decimal64(18,0)` with `10 × (10^18-1)`, `10 × -(10^18-1)` and `5`
  (exact `0.2381`) varies in grouped and ungrouped cells (AO / NULL against `0.2381`). Spark answers `0.625` and `0.2381`.
  Step 0's `Decimal64` probe used three values and so could not wrap. The return-type cap is in step 3 item 1.
- **The window-condition cells** (`t` g2, g3, g5, ANSI on): RePark gives AO×20 in all 9 configurations and Spark gives
  NVOOR at both masters. This agrees with step 0, but see S2-b.
- **Spark** (`out/sp_main.jsonl`): 101 queries × 2 modes × 2 masters = 404 cells, 2 runs each. **All are steady and
  identical to step 0's Spark answers.** 46 of step 0's avg layout pairs (`local[1]` ≠ `local[4]`) are re-measured, and
  every one is reproduced.

## Step 5: shapes step 0 missed (main, 7 configurations, 20 runs, both engines)

| shape | RePark main | Spark `local[1]` / `local[4]` | note |
|---|---|---|---|
| spill, 16 MB pool, `target_partitions` 1 | all groups NULL / AO; exact without the pool | per-row (NULL) | **pin for the wrap column** |
| `GROUPING SETS ((g), ())` on `big` | VARIES in 6 of 7 configurations (up to 11 answers) | all NULL / AO | seat A's accumulators cover it; pin one cell |
| `CUBE(g, h)` on `t` | VARIES | AO | `CUBE(g, id % 2)` with an expression **fails to plan** (`spark_integral_literal … No field named`), outside avg; file it |
| scalar subquery `(SELECT avg(c) FROM big)` | VARIES (`0.0167` / NULL) | `0.0167` / AO | layout class |
| correlated subquery on `t4` | VARIES (AO / NVOOR, NULL rows) | AO | the same |
| join, then `avg` | `t4` VARIES; `big` steady exact (hash-partitioned by `g`) | AO | the same |
| `avg(DISTINCT c)` p38 across 4 files | `1`, steady, exact | `1` | fine |
| `avg(DISTINCT c), count(DISTINCT id)` | **refuses** (`DistinctAvgAccumulator`) | `[1, 8]` | outside the card |
| sliding window, `ROWS 2 PRECEDING`, frames crossing 38 digits | steady NVOOR / NULL rows, Spark's | the same | retract is exact |
| `try_avg` with ANSI on | NULL, never raises | the same | fine |
| nonzero scale at p38 (`decimal(38,34)`) | `...4545`, Spark's | the same | no double-rounding gap |
| Iceberg, RePark-written v2 table, 5 data files + `pos-del` file | steady per setting but **follows `target_partitions`** (`t4` g23 exact at 1, NULL at 2/4/8) | — | RePark cannot list Spark's hadoop table (`SCHEMA_NOT_FOUND`), so the tables are compared on the exact model only |
| DataFrame door, `spark.table(...).groupBy().agg(F.avg)` | VARIES (`big` at SET 4: 6 answers in 20) | — | same kernel (W7) |
| native SQL door `repark.sql()` | not measurable: local-file DDL and file paths are refused | — | no table path is reachable without an opt-in |
| **sum past 38 digits, average under `10^34`** (`m_w34`, 20,000 × `9e33`) | AO / NULL everywhere | **u `local[4]` `9e33`; window `9e33` at both** | breaks the 0/0 claim; see step 6 |
| `Decimal256` near `2^256` (`m_d256`) | **`0.4167`** (silent wrong answer) / VARIES | Spark refuses (`DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`) | seat A row wrong |

Side note, not an avg finding: `big` grouped `sum` with a 3 MB pool at SET 4 sat at about 3 % CPU for over 100 s. I
killed my own process (PID 489635). This may be a hang under a tiny pool. Worth one look.

## Step 6: seat A

- **State by precision with Spark's `p + 10` buffer:** sound for `Decimal128`. At precision 19 and under, one raw
  `i128` holds `n × 10^19 < 2^127` for any n under about `1.7 × 10^19` rows per partial, which no engine reaches with
  declared values. Over-precision values give the narrow refusal in multi-phase plans and NULL / AO in single-phase
  ones. That was accepted for `sum` in #1360; say so for `avg` as well. Declare the state sum type as
  `decimal(min(p+10,38), s)`, as #1360's `SumCheck::data_type` does, not the input type (`aggregate.rs:262-299` ships
  `input_fields[0]`). Today a partial ships totals past the declared type's precision, which is risky for any
  validating consumer such as the distributed codec.
- **The wrap column through spill and `convert_to_state`:** nothing measures it today. Required pins: the spill cell
  above (`target_partitions` 1, 16 MB pool, 200,000 groups, every group `1.0000`), and the skip-partial cell (probe rows
  1, ratio 0.0, at SET 4) for the three-column `convert_to_state`.
- **Reuse by visibility:** yes for `Decimal128`. `exact_total` (`sum_decimal.rs:112`) and `fold_batch` (`:130`) become
  `pub(crate)`. `decimals()` (`:146`), `partial_wraps()` (`:155`) and `unwrapped()` (`:102`) carry the text "sum". If
  the build re-texts them, it must keep the phrase "past its declared precision". #1360's bar pins that phrase at
  `sum_precision_tests.rs:73`, `sum_tests.rs:663` and `:672`, and `python/repark/tests/types/test_sum_dec_i128wrap_1.py:240`.
  Those cells, and #1360's `sum` window cells (the analyzer at `aggregate.rs:311-369` now routes both), are the ones
  this change could move. Re-run #1360's sum pin files unchanged.
- **`Decimal32` / `Decimal64`:** widen to `i128` at update, ship the state sum as `Decimal128(min(p+10,38), s)` with no
  wrap column (`n × 10^18 < 2^127` up to about `1.7 × 10^20` rows), **and fix the return type** to Spark's
  `decimal(min(p+4,38), s+4)` as `Decimal128`. A wrap column on the narrow native is more code and still leaves the
  capped result type.
- **`Decimal256`:** either count wraps (`i256` `overflowing_add`, then refuse or NULL at evaluate) or refuse past
  `i256`. "Unchanged" keeps a silent wrong value and the variance.
- **Cost, K1 `avg(d38)`:** the only measurement of a third avg state column is **1.19x** on K1
  (`task/wo/avg-decimal-round-1/evidence/built/cost.md:35-36`: partial aggregation is skipped at that cardinality, so
  every row's state ships). #1360's second `sum` column landed at 1.04x to 1.085x. So **1.12x is probably not reachable with
  an `Int64` third column.** A cheaper layout that keeps one answer, for the build to measure first: **no third
  column**, with the partial's wrap count packed into the high bits of the `UInt64` count (for example 48 bits of
  count and 16 of signed wraps) and a refusal when either overflows. A state schema cannot add the column only when a
  partial wrapped, because the schema is fixed per plan. Seat B is not one answer (step 0's own reading of `t` g1/g6/g7/g8)
  and should stay the last fallback.
- **The kept divergence:** I re-derived 56 of the 81 pairs in `classes.md` from my own exact model and my own Spark
  runs: all 26 ANSI-off pairs over `big`, `t` g1/g6/g7/g8 and `t4` g23/g24, and 30 ANSI-on pairs including g3, g5, g9,
  g21, g22, the windows, HAVING and ROLLUP. **All 56 agree with the class `classes.md` gives.** But the claim "**no exact
  answer is NULL or raises where Spark answers**" is **false in general.** `m_w34` gives a seat-A AO / NULL where Spark
  answers `9e33`: ungrouped at `local[4]` in both modes and `try_avg`, and in the window at both masters. Step 0's
  tables never held a total past 38 digits with an average under `10^34`, which needs more than `10^4` rows.

## Step 7: the split and the questions

- **Split:** one unit after #1360. That is fine, but the build needs a file plan: `aggregate.rs` is at 989/1,000, so
  move avg's `state_fields` and wiring out, and `avg_decimal.rs` is at 961.
- **Q1** (over-precision here): the default (a) is sound and the cells reproduce.
- **Q2** (accept the kept divergence): the default rests on "0 pairs where exact is NULL / raises and Spark answers",
  which is contradicted (`m_w34`). Restate it with the new class. The windows can avoid that class entirely (see Q5).
- **Q3** (cost): the default (a), halt above 1.12x, will probably halt, given the 1.19x measured. Add option (d): the
  packed-count layout, measured before seat B.
- **Q4** (narrow natives): it must also cover the **return-type cap**, a steady wrong NULL / NVOOR at one partition.
  The default (a) should become "widen to `Decimal128` state and result".
- **Q5** (window condition): the default (a), "a named condition class", rests on a misreading. Spark's window never
  checks the sum and answers whenever the quotient fits. Recommend option (b'): the avg window path skips the sum check
  and checks only the quotient. That is Spark-equal and layout-free. It turns `t` g2/g3/g5 into NVOOR and `m_w34`'s
  window into `9e33`.
- **Missing questions:** Q6, `Decimal256` (count wraps or refuse). Q7, whether the ungrouped `avg` follows Spark's
  unchecked final merge. My recommendation is no: keep the exact total, since Spark's ungrouped answer follows the
  split count. Q8, file the `CUBE(expr)` planner failure and the multi-`DISTINCT` refusal as their own cards.

## What I would require before the build (what flips this to GAPS, then SOUND)

1. Rewrite sketch §1: the ungrouped final merge is unchecked under codegen, and the window checks the quotient only.
   Add the codegen-off and `ObjectHashAggregate` rows.
2. Seat A's window path: no sum check, quotient only. Pins: `m_w34` window `9e33` and `t` g2/g3/g5 NVOOR.
3. Narrow natives: a `Decimal128` widened state and Spark's result type. Pins: `m_d32` `0.625` and `9999999.99`,
   `m_d64` `0.2381` and `999999999999999999`, each at SET 1/2/4/8.
4. `Decimal256`: a stated rule. Pin: `m_d256`, which must not answer `0.4167`.
5. Pins for spill (16 MB pool at `target_partitions` 1, 200,000 groups), skip-partial (`convert_to_state`), one
   `GROUPING SETS` cell, one correlated-subquery cell, the DataFrame door (`spark.table().groupBy().agg(F.avg)`), and an
   Iceberg v2 table with a position-delete file at SET 1/2/4/8.
6. Restate the kept divergence with the `m_w34` ungrouped `local[4]` class. Treat the VARIES count as a lower bound and
   add `t` g6 and `t4` g21.
7. A file plan for `aggregate.rs` (989/1,000). The distributed codec round-trips the new state. #1360's sum pins are
   re-run unchanged.
8. Cost: measure the packed-count layout next to the `Int64` column before Q3 is applied.

Housekeeping: nothing of mine is still running. The warehouses are `wh` (296 KB) and `wh_rp` (104 KB), and the data
is under 4 MB in total, so I deleted nothing. No repository was edited.
