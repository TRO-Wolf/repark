# SUM-DEC-I128WRAP-1 — the bar (head against main and Spark, both ANSI modes)

**Head** `a8016e75` (package `pkg-sum-a8016e75`, `BUILT_FROM` `a8016e75a033906e5bb49fcce9d79bdfa924ad3b`), **main**
the step 0's RePark raw (`pkg-avg-27d6149f`, main's product tree), **Spark 4.1.2** the step 0's Spark raw: the same
probes, fixtures and settings ([../probes/](../probes/map.md)). The head re-ran the step 0's whole domain table:
`run_door.py`, `run_mp.py`, `run_native.py`, `run_revision.py` (RePark, each mode) and `crossread.py` (Spark reading
the head's revision warehouses, each Spark mode); `compact.py`, `trim_revision.py`, `analyze.py` and
`analyze_revision.py` unchanged; [compare_bar.py](compare_bar.py) pairs main's verdicts with the head's. Every run
printed the head package's `_native.abi3.so`; the Iceberg jar's size and modification time were the same before and
after.

## Verdict: the hard bar is **not** met — 46 EQUAL→other cells, all pass-back sums whose answer depends on layout

Everything else holds: 1,329 cells move, avg's 330 cells per mode, the 150 controls per mode (the `BIGINT` ANSI
overflow cells among them) and the native door's 594 records are **byte-for-byte unchanged**, no Iceberg data file
holds a wrapped or 39-digit value, and Spark reads every table the head wrote. The 46 cells (23 per mode) were EQUAL on
main only because main never checks and its wrapping sum came back to the exact value; Spark's own answer there depends
on its row order and split. Three classes ([compare/compare.md](compare/compare.md), [compare/moved.jsonl](compare/moved.jsonl)):

| class | cells (on + off) | what happens | why it cannot be fixed inside the rulings |
|---|---|---|---|
| (a) **one split against several files** | 20: `sum/ungrouped_s4/g10`, `g12` (× 3 columns), `sum/ungrouped_p2/g21`, `sum/grouped_p2/g24` (× 2) | Spark's default pass packed the 4 (`s4`) or 2 (`p2`) files into one split, so its one partial came back to 5, 7 or 0; RePark reads each file as its own partial, one partial passes 38 digits, and the head raises (on) or answers NULL (off) | the **same RePark plan** is EQUAL against Spark's multi-partition pass: every twin `mp/sum/ungrouped_s4/*`, `mp/sum/ungrouped_p2/g21`, `mp/sum/grouped_p2/g24` moved from SPARK_RAISES / VALUE to BOTH_RAISE / EQUAL. RePark can match one Spark layout, not both (Q6; step 0 sketch §7) |
| (b) **partial or merge order** | 14: `sum/distinct/g10` (× 3 off, `c38_10` on), `sum/distinct/g12` (`c38_10` off, `c38_38` on), `sum/grouped_s4/g11/c38_38`, `mp/sum/grouped_s4/g11/c38_0`, `c38_10`, `try/s4/g11/c38_0` (on), `mp/try/s4/g11/c38_10` (on) | a grouped sum whose partials each fit (`M`, `1`, `−1`; the distinct values `M`, `−M`, `5`) passes 38 digits when the final step merges them in the order they arrive, and Q2 (i) checks after every merged partial, as Spark's `UnsafeRow` buffer does | **corrected 2026-11-18 (delta 1):** the answer differs **run to run** on this head (each query repeated 20 times: 17 of 401 queries with ANSI on and 20 with it off give more than one answer), because the final step merges the partials in the order they arrive; Spark gives one answer on every run for a given layout (0 of 172 queries vary at `local[4]`). Two of these cells are **silent with ANSI on**: `try/s4/g11/c38_0` and `mp/try/s4/g11/c38_10`, where `try_sum` answers NULL and Spark the value. Ruled 2026-11-18 (delta 1 below) |
| (c) **DataFrame `repartition(4)`** | 12: `df/repartition/g10`, `g11` (× 3 columns) | Spark's `repartition(4)` deals rows round robin, so no partition holds `M, M`; RePark's is DataFusion's `RoundRobinBatch`, which keeps the whole small batch in one partition, where the per-row check sees `M, M` and sticks | a different repartition (row-level dealing) is not this unit's seat; under ruling Q2 (i) the head answers what Spark answers for the same rows in one partition (`sum/grouped/g10`, `g11`: both raise) |

Classes (a) and (b) are the cells step 0 predicted (sketch §7: "pass-and-come-back cells depend on row order and
layout on both engines, so the build pins only single-batch, single-file cells for them"; Q2 lists `grouped_s4/g12`,
`mp/grouped_p2/g21`, `g24`). By the brief they are still a halt: the bar asks for 0 EQUAL→WRONG and 0 EQUAL→RAISE.
Options for the owner (none taken): accept the three classes as named (a card for each, or one), or a further ruling
on Q2 for the grouped final merge (check the merged total once instead of after every partial: that fixes class (b)'s
grouped and distinct cells, departs from Spark's per-partial check, and leaves (a) and (c)).

## Counts per verdict (Spark door, default pass; main → head)

| ANSI | EQUAL | BOTH_RAISE | SPARK_RAISES | VALUE | REPARK_RAISES | BOTH_RAISE_OTHER | FORMAT |
|---|---|---|---|---|---|---|---|
| on | 1,769 → 1,765 | 138 → 655 | 549 → 32 | 26 → 9 | 24 → 45 | 31 → 31 | 0 → 0 |
| off | 1,912 → 2,439 | 0 → 0 | 0 → 0 | 570 → 56 | 47 → 34 | 0 → 0 | 8 → 8 |

Per family (the default and multi-partition passes, the revision, the native door): [compare/target.md](compare/target.md).
The target sets:

- **decimal `sum`** (1,442 cells per mode): 430 (on) and 449 (off) silent cells move to Spark's answer; 5 (on) and
  36 (off) stay silent, every one in classes (a) to (c) or the `avg` column of `sum/beside` (AVG-GROUPED-SUM-STICKY-1).
  The multi-partition pass: 52 of 52 silent cells per mode move.
- **the shapes** (60 per mode): 47 (on) and 51 (off) of 52 move; on, 4 more raise Spark's condition by message
  (`rev/union/g4_14`, `rev/distinct_mixed_ungrouped/g5`: `NUMERIC_VALUE_OUT_OF_RANGE`, which the revision analyzer
  counts as other because it reads `getCondition`, unset on the facade's exception); `rev/df_try_sum_ungrouped/g10/c38_38`
  answers `5E-38`, numerically Spark's `5.0E-38`.
- **`try_sum`** (332 per mode): 16 (on) and 17 (off) silent cells move, including the 20 over-nulls that are not
  layout cells; 8 (on) and 7 (off) stay, all in classes (a) and (b).
- **Iceberg** (50 statements per mode, plus the revision's 22): every statement EQUAL or BOTH_RAISE in both modes;
  no data file under any table holds a wrapped or 39-digit value (main: 8 committed wraps and 2 orphan files per mode,
  `file_tags` now empty); the revision's 14 committed 39-digit writes per mode are gone (ANSI on: all 22 statements raise
  as Spark does; off: all store `350, NULL, NULL` as Spark does). Read-backs: all EQUAL except 8 (on) CTAS read-backs
  where both engines find no table and RePark's text lacks `TABLE_OR_VIEW_NOT_FOUND`.
- **Spark reads RePark's tables** (`raw/rev-cross-*.jsonl`): 0 `NUMERIC_VALUE_OUT_OF_RANGE` (main: 112); every table
  answers, and the 8 per query that do not exist are the CTAS statements that raised.

## Unchanged, byte for byte

| set | records | equal |
|---|---|---|
| `avg` (default + multi-partition pass), per mode | 276 + 54 = 330 | 330 / 330 both modes |
| controls (`INT`, `BIGINT`, `DOUBLE`; the 17 `BIGINT` ANSI overflow cells among them), per mode | 150 | 150 / 150 both modes |
| native door (`repark.sql()`) | 594 | 594 / 594 |

The AVG-DECIMAL-ROUND-1 pins, DECIMAL-COMMON-TYPE-1, CAST-LITERAL-UNWRAP-1, the DECIMAL-DOUBLE and signed-zero files
pass on the head (3,890 passed, 4 xfailed; the gate table in the hand-back).

## Every non-EQUAL cell left on the head, by named class ([compare/left.jsonl](compare/left.jsonl))

| class | on | off |
|---|---|---|
| native door (NATIVE-DECIMAL-SUM-1, Q3) | 305 | 288 |
| `F.sum_distinct` missing (FNP-AGG-1) | 30 | 30 |
| `BIGINT` ANSI overflow (SUM-BIGINT-ANSI-OVERFLOW-1, Q4) | 17 | 0 |
| `avg` (AVG-GROUPED-SUM-STICKY-1, ruling (a)), with the `avg` column of `sum/beside` | 12 | 14 |
| layout: each file a partial where Spark read one split (class (a) and its Q6 conditions) | 35 | 18 |
| order: partial or merge order (class (b), and class (c), the `repartition(4)` cells) | 17 | 24 |
| views across an ANSI flip (TEMP-VIEW-SESSION-SETTINGS-1) | 5 | 5 |
| seen beside (sketch §8: SQL `PIVOT`, string casts, `summary()`, `describe` notation) | 6 | 7 |
| decimal-to-string notation (domain table §10) | 0 | 8 |
| decimal `+` at scale 38 (DECIMAL-MULTIPLY-OVERFLOW-1 family) | 2 | 2 |
| `BIGINT` Parquet statistics assertion (DECIMAL-CAST-SELECTIVITY-1) | 2 | 2 |
| CTAS read-back: no table on both, RePark's text lacks `TABLE_OR_VIEW_NOT_FOUND` | 8 | 0 |
| same condition by message (`NUMERIC_VALUE_OUT_OF_RANGE`; analyzer reads `getCondition`) | 4 | 0 |
| numerically equal (`5E-38` against `5.0E-38`) | 1 | 1 |
| both raise; Spark's condition wrapped by Py4J (`sum/scalar_sub/g5`, `g6`) | 2 | 0 |

The writer card ICE-DECIMAL-WRITE-PRECISION-1 has no cell left from `sum`: no `sum` path hands the writer an
out-of-precision value; its own hole (bounds-only checks) stays for any other producer.

## The pin files

[python/repark/tests/types/test_sum_dec_i128wrap_1.py](../../../../../python/repark/tests/types/test_sum_dec_i128wrap_1.py):
77 cells × 2 modes plus a data-file scan per mode against a Spark oracle recorded by [record_oracle.py](record_oracle.py)
and [merge_oracle.py](merge_oracle.py). Head (the clone's release build and `pkg-sum-a8016e75`): 156 / 156 green. Main
(`pkg-avg-27d6149f`): 138 red, 18 green, the 18 being the nine control cells per mode
([compare/pins-on-main.txt](compare/pins-on-main.txt)); every position has a red cell. The torture cell
`test_decimal_overflow_sum_refuses_loud` (its strict xfail removed; it pins Spark's `ARITHMETIC_OVERFLOW`): green on
the head, red on main. Rust: `aggregate::sum_tests` (8) and `extension::sum_window_tests` (2).

## Delta 1 (2026-11-18): the grouped final merge checks its total once

**The ruling (owner, 2026-11-18, about 13:40 UTC, verbatim):** "Go with Fable's leans on A, B and C." A is Fable's
lean in its reply on the build's halt (2026-11-18, question 2 and "What to do next"), put to the owner as question A:
"Check the grouped final merge's total once, not after every partial. It amends your Q2 (i) for the final merge only."
The partial step keeps Spark's per-row sticky check. B accepts the layout class, (a) one split against several files
and (c) `repartition(4)`, at S3. C rates the distributed plan codec card S3, filed by the clerk, not in this PR.
Fable's earlier answers (the reply's questions 1 and 3) are Fable's guidance, not rulings.

### Sizing, recorded before any product file of the delta changed

Scripts and raw output: [delta1/](delta1/map.md). Both engines read the same Parquet files (written once by
`pyarrow`: `s` one file, `s4` four, `p2` two, and Fable's probe table `fq` four), each query repeated 20 times in one
session, both ANSI modes. Every RePark run printed its package's `_native.abi3.so`.

**1. Does Spark vary? No.** Spark 4.1.2, `local[4]`, `spark.sql.shuffle.partitions` 4, 172 pass-back and condition
queries (every class (b) query among them), once with Spark's default split packing and once with one split per file:
**0 of 172 queries give more than one answer**, in both ANSI modes and both packings (the two packings answer the same
on every query). Spark's answer depends on the layout, not the run: at `local[4]` `sum(DISTINCT …)` over group 12 (the
distinct values `M`, `−M`, `7`) raises (on) or answers NULL (off) where the step 0's `local[1]` answered 7, and the
ungrouped `DISTINCT` over group 5 raises `ARITHMETIC_OVERFLOW` where `local[1]` raised `NUMERIC_VALUE_OUT_OF_RANGE`.

**RePark before the change** (401 queries: every `s4` and `p2` cell, every `DISTINCT` cell, the `repartition(4)` cells
and Fable's three queries):

| build | ANSI on: queries with more than one answer | ANSI off |
|---|---|---|
| the build's head `pkg-sum-a8016e75` | 17 | 20 |
| main `pkg-view1-c54f5fc7` (`d6f58d7f`) | 4 | 4 |

Every class (b) query varies on the build's head (for example `mp/sum/grouped_s4/g11/c38_0`, ANSI on: 12 raise
`ARITHMETIC_OVERFLOW`, 8 answer `M`; `sum/distinct/g12/c38_10`, ANSI off: 10 NULL, 10 the value). **Main varies too**,
on the grouped `try_sum` over several partials (`mp/try/s4/g11/*` and `fable/grouped_try_sum`: datafusion-spark's
`try_sum` also checks after each merged partial), so the step 0's single main sample of those cells was one draw; main's
`sum` answers one value on every run.

**The condition Spark raises when a grouped total's partials fit and the total does not** is `ARITHMETIC_OVERFLOW`
("Overflow in sum of decimals. Use 'try_sum' …"), not Q6's `NUMERIC_VALUE_OUT_OF_RANGE`, which is the ungrouped
condition: `mp/sum/grouped_p2/g22` (`M` in one file, `1` in the other) raises `ARITHMETIC_OVERFLOW` at `local[1]` (the
step 0's multi-partition pass) and on all 20 runs at `local[4]`, where `mp/sum/ungrouped_p2/g22` raises
`NUMERIC_VALUE_OUT_OF_RANGE`. Spark's grouped buffer is an `UnsafeRow`, so the merged total that passes 38 digits is
stored as NULL and `CheckOverflowInSum` raises the sum overflow. The delta raises Spark's measured condition.

**2. How many cells move, each way (predicted).** [delta1/predict.py](delta1/predict.py) models the change on the 544
cells whose partials the file layout fixes (`s4` and `p2`, grouped `sum` and `try_sum`, both passes, both modes): each
file's partial checked after every row, the merged total once.

| against | into EQUAL or BOTH_RAISE | out of EQUAL or BOTH_RAISE (to SPARK_RAISES or VALUE) |
|---|---|---|
| Spark's default pass (`local[1]`, one split) | 3 on, 2 off | 0 |
| Spark's multi-partition pass (`local[1]`, a split per file) | 3 on, 3 off | 0 |
| Spark `local[4]`, both packings (56 modelled queries per mode) | every modelled query (41 EQUAL and 15 BOTH_RAISE on, 56 EQUAL off); the build's head is right on every run for 137 (on) and 144 (off) of the 172 | 0 |

The 11 predicted moves are `sum/grouped_s4/g11/c38_38` and `mp/sum/grouped_s4/g11/c38_0`, `c38_10` (both modes),
`try/s4/g11/c38_0` (both modes), `try/s4/g11/c38_38` and `mp/try/s4/g11/c38_10` (on), and `mp/try/s4/g11/c38_38`
(off): the eight class (b) cells that are not `DISTINCT`, and three `try_sum` cells that main's own run-to-run
`try_sum` had left non-EQUAL. The model cannot place the six `DISTINCT` cells (their partials are the distinct values'
hash partitions, which depend on the session's partition count); the `repartition(4)` cells keep one partial and do
not move. No modelled cell
leaves EQUAL: on these fixtures Spark's own merge order never passes 38 digits and comes back. The pin file's new
`t4` table builds one that does (group 21, `M`, `1`, `−1` in three files; Spark merges `M` and `1` first and raises
or answers NULL on all 20 runs), the accepted class's pin.

### What was built

One product file, `crates/repark-functions/src/aggregate/sum_decimal.rs` (still five of the cap of six):
`DecimalSumGroups` (`:288`) keeps its per-row check in the partial step (`fold_rows`, `:331`: sticky per group, ANSI
raises at the first overflowing row) and gains a final step of its own (`fold_partials`, `:363`): the merged partials
are added exactly, with a wrap count per group, and an overflowed partial's marker still answers NULL (ANSI off and
`try_sum`) or raises at once (ANSI on). `evaluate` (`:462`) checks each group's total once and raises Spark's grouped
condition, `ARITHMETIC_OVERFLOW` with the `try_sum` hint, or answers NULL. A final step that spills ships its merged
total (`state`, `:494`) as is, or the marker once it no longer fits in 128 bits. The skip path stays
(`convert_to_state`, `:517`): with it off (commit `76295c30`) K1 ran at 1.25x to 1.30x of main, so `6d730107` put it
back; where DataFusion skips the partial step (at least 100,000 rows in a partition with 80 % or more distinct groups)
the rows reach the final step and are totalled once, the final-merge class below. The ungrouped and window
accumulators, `DecimalSumDistinct`, `avg` and the non-decimal delegation are unchanged.

### After the change (head `6d730107`, package `pkg-sum-6d730107`)

The step 0's whole domain table re-ran on the head package and on main's (`pkg-view1-c54f5fc7`), Spark's side the
step 0's; compared with [compare_bar.py](compare_bar.py) against both bases, `pkg-view1-c54f5fc7` and the build's base
`pkg-avg-27d6149f` (the step 0's raw). Compacted output and verdicts: [delta1/raw/](delta1/raw/map.md); comparisons:
[delta1/compare/](delta1/compare/map.md).

**Counts per verdict** (Spark door, default and multi-partition passes; 2,537 cells per mode):

| build | ANSI | EQUAL | BOTH_RAISE | SPARK_RAISES | VALUE | REPARK_RAISES | BOTH_RAISE_OTHER | FORMAT |
|---|---|---|---|---|---|---|---|---|
| base `pkg-avg-27d6149f` | on | 1,769 | 138 | 549 | 26 | 24 | 31 | 0 |
| main `pkg-view1-c54f5fc7` | on | 1,770 | 138 | 549 | 25 | 24 | 31 | 0 |
| the build's head `pkg-sum-a8016e75` | on | 1,765 | 655 | 32 | 9 | 45 | 31 | 0 |
| **head `pkg-sum-6d730107`** | on | **1,773** | **655** | 32 | **6** | **40** | 31 | 0 |
| base `pkg-avg-27d6149f` | off | 1,912 | 0 | 0 | 570 | 47 | 0 | 8 |
| main `pkg-view1-c54f5fc7` | off | 1,914 | 0 | 0 | 568 | 47 | 0 | 8 |
| the build's head `pkg-sum-a8016e75` | off | 2,439 | 0 | 0 | 56 | 34 | 0 | 8 |
| **head `pkg-sum-6d730107`** | off | **2,448** | 0 | 0 | **47** | 34 | 0 | 8 |

**Step 1's two numbers, after.** (1) Spark is unchanged (0 of 172 queries vary, above). RePark's head: **0 of 401
queries give more than one answer** in 20 runs, in both modes (**corrected in delta 2:** this held only for these small
tables at the lane's 16 cores; CI and the sizing step found delta 1 varying at 2, 4 and 8 partitions, see "Delta 2") (the build's head: 17 and 20; main: 4 and 4); Fable's
probe itself, 40 runs per query, answers `M` on every run for all three queries in both modes. (2) The moves, against
the build's head: **17 Spark-door cells into EQUAL** (8 with ANSI on, 9 off: the 11 the model predicted and the six
`DISTINCT` cells it could not place), **0 out of EQUAL or BOTH_RAISE**; the other 6 cells that moved are the views
across an ANSI flip, which main's TEMP-VIEW-SESSION-SETTINGS-1 part 1 fixed. Against Spark's `local[4]` run the head
is right on every run for 148 (on) and 155 (off) of the 172 queries and never varies (the build's head: 137 and 144
right on every run, 17 and 20 on some runs only); the rest are named below.

**All 14 class (b) cells are EQUAL**, both silent ANSI-on cells among them (`try/s4/g11/c38_0` and
`mp/try/s4/g11/c38_10` answer Spark's value on every run).

**The hard bar.** Against either base, **32 cells go from EQUAL to another verdict, the same 32 against both**: the
layout class, owner-accepted 2026-11-18 at S3 (ruling B), cell for cell below. No other cell leaves EQUAL or
BOTH_RAISE. `avg`'s 330 cells per mode, the 150 control cells per mode (the 17 `BIGINT` ANSI cells among them) and the
native door's 594 records are byte-for-byte those of both bases and of the build's head. **The two bases differ on 9
cells:** `rev/view_flip_avg/g5/c38_0` in both modes (main's TEMP-VIEW-SESSION-SETTINGS-1 part 1) and seven grouped
`try_sum` cells over several partials (`try/s4/g11/c38_0`, `c38_38` and `mp/try/s4/g11/c38_0`, `c38_10`, `c38_38`
across the modes), where main's own `try_sum` answers differently from run to run.

#### The cards (listed here; the clerk files them)

**SUM-GROUPED-MERGE-TOTAL-1** (proposed name, beside AVG-GROUPED-SUM-STICKY-1). **Severity: not rated; not
label-blocking**, AVG-GROUPED-SUM-STICKY-1's rating (owner's ruling (a) of 2026-11-17), since Fable's lean put it
beside that card. A grouped decimal `sum` or `try_sum` whose partials each fit and whose total fits, where Spark's own
merge order passes 38 digits and comes back: Spark raises `ARITHMETIC_OVERFLOW` (ANSI on) or answers NULL; RePark
answers the exact total on every run. Cells: none in the domain table (no fixture has that merge order); the pin
file's `t4` group 21 (`M`, `1`, `−1` in three files; Spark merges `M` and `1` first), `final/sum/g21/c38_0`,
`c38_38` and `final/try_sum/g21/c38_0`, `c38_38`, in both modes (8 cells). The same direction, outside the bar's
oracle: where DataFusion skips the partial step (large, high-cardinality inputs), a group's rows reach the final step
and are totalled once, where Spark checks each row (pinned in Rust: `the_overflow_marker_survives_spill_repartition_and_skipped_partials`,
group 20,001); and Spark's own `DISTINCT` partial (`sum/distinct/g11`, three columns per mode, non-EQUAL on both bases
too: the distinct values `M`, `1`, `−1`, Spark summing `M` and `1` first).

**The layout card** (proposed name SUM-PARTIAL-LAYOUT-1). **Severity: S3** (owner, 2026-11-18, ruling B). RePark's
partials follow its plan, not Spark's splits, so a pass-back sum's partial can pass 38 digits on one engine and not the
other; both answer one value per layout. The 32 cells: (a) one split against several files, 20:
`sum/ungrouped_s4/g10` and `g12` at `c38_0`, `c38_10`, `c38_38`, `sum/ungrouped_p2/g21` and `sum/grouped_p2/g24` at
`c38_0` and `c38_38`, in both modes (Spark's default pass reads the files as one split; the `mp/` twins are EQUAL or
BOTH_RAISE); (c) `repartition(4)`, 12: `df/repartition/g10` and `g11` at the three columns, both modes (Spark deals rows
round robin, RePark's `RoundRobinBatch` keeps the small batch whole). Measured outside the bar, against Spark
`local[4]`: the `DISTINCT` cells over groups 11 and 12 and the ungrouped `DISTINCT` over group 5, where Spark's
distinct partials follow its four shuffle partitions (a third kind of layout, not in ruling B's list: the owner may
want to confirm it), and `mp/avg/grouped_p2/g24` (AVG-GROUPED-SUM-STICKY-1).

**Every non-EQUAL cell left on the head, by named class** ([delta1/compare/head-vs-main/compare.md](delta1/compare/head-vs-main/compare.md)):
as the build's table above, except that the "order" row now holds only the `repartition(4)` cells (6 per mode) and
cells that were non-EQUAL on both bases too (`sum/distinct/g11`, 3 per mode, and with ANSI off `sum/distinct_all/*`
and `df/repartition/all/*`, 3 each: Spark's own `DISTINCT` or one-partition order), and the views row holds 2 per mode
after main's fix: native door 305 / 288, `F.sum_distinct` 30 / 30, `BIGINT` ANSI 17 / 0, `avg` 12 / 14, layout 35 / 18, order 9 /
15, views 2 / 2, seen beside 6 / 7, string notation 0 / 8, decimal `+` at scale 38 2 / 2, `BIGINT` Parquet statistics
2 / 2, CTAS read-back not-found text 8 / 0, same condition by message 4 / 0, numerically equal 1 / 1, Py4J-wrapped
condition 2 / 0.

## Delta 2 (2026-11-18, the last): exact in every phase, the state chosen by precision

### The ruling and what it amends

**Owner, 2026-11-18, about 20:05 UTC, verbatim:** "Go with Fable's recommendations on all seven, re-run 1362". The
seven are §9 of Fable's reply on the overnight plan and decision I (Fable's guidance under the owner's delegation,
written about 19:50 UTC; not a ruling until this line). For this unit the line rules:

- **Decision I, option (1):** decimal `sum` and `try_sum` are exact in every phase. Input precision 19 and under keeps
  one raw `i128` state column with no marker; precision 20 and up carries the low word and a wrap count. One check, at
  `evaluate`: `ARITHMETIC_OVERFLOW` with the `try_sum` hint for every non-window total with ANSI on, NULL with ANSI off
  and in `try_sum`. Windows keep their own check; `DISTINCT` and `avg` do not move. **It amends the owner's Q2 (i) of
  2026-11-17 for every phase, and replaces ruling A of 2026-11-18 13:40 UTC** ("Go with Fable's leans on A, B and C"),
  under which delta 1's partial step kept Spark's per-row sticky check.
- **The cost exception:** K1 may reach 1.12x of main for precision 20 and up only; 1.05x stays for precision 19 and
  under and every other held row.
- **One more delta, the last;** a class (a) or (b) finding at the fresh verifier parks the unit.
- **The kept divergence** goes into the label text (the orchestrator files the wording); **aggregate determinism
  runs** at `target_partitions` 2, 4 and 8 on multi-file tables, 20 runs each, the setting made before the table is
  read.

Fable's earlier replies (the build halt, delta 1's CI halt about 17:30 UTC) stay Fable's guidance; the measured
design came from the sizing step on the scratch prototypes `898e15e6` (carrier (i)) and `5469a59a` (carrier (ii)).

**Correction of delta 1's record.** "0 of 401 multi-partial queries varied" (above) held only for the step 0's small
tables at the lane's 16 cores. Delta 1's pin `test_a_multi_partial_query_gives_one_answer_on_every_run` failed PR
#1360's CI, and the sizing step measured delta 1 varying at 2, 4 and 8 cores and at `target_partitions` 2, 4 and 8
(up to 10 answers in 20 runs on a 300,000-row three-file table, and raise-or-answer with ANSI on), because DataFusion
does not fix which rows meet in a partial or their order (file work stealing and round-robin batches).

### What was built (head `fcb0c888`, package `pkg-sum-fcb0c888`)

Two product files, both already among the build's five (cap 6, count unchanged at 5):
`crates/repark-functions/src/aggregate/sum_decimal.rs` (`NARROW_PRECISION` `:16`, `Carrier` `:26`, `unwrapped` `:102`,
`DecimalSum` `:146` with `merge_batch` `:193` and `state` `:210`, `DecimalSumGroups` `:318` with `fold_partials` `:363`,
`shipped` `:409`, `evaluate` `:442`, `state` `:465`, `convert_to_state` `:470`) and
`crates/repark-functions/src/aggregate/spark_sum.rs` (`state_fields` `:143`: one column to precision 19, two from 20).
It is the sizing's prototype (i) with the precision split; the prototype's unused `OVERFLOWED` is gone. A partial of
a precision-19 input that does pass 128 bits (only possible when the input holds values past its declared precision)
raises an error rather than ship a marker.

### The four checks (Fable §3)

1. **K1 for `DECIMAL(10,x)` at 1.05x of main:** see [cost.md](cost.md), "Delta 2".
2. **The state schema can depend on the input precision:** DataFusion 54.1.0 builds `StateFieldsArgs` with the
   aggregate's own `input_fields` (`datafusion-physical-expr-54.1.0/src/aggregate.rs:732-741`), so
   `SparkSum::state_fields` reads the input precision inside the cap. Pinned:
   `the_state_is_one_column_to_precision_19_and_two_from_precision_20` (`sum` and `try_sum` at `(10,2)`, `(18,0)`,
   `(19,0)`, `(19,19)`, `(20,0)`, `(28,2)`, `(38,0)`; and both accumulators and `convert_to_state` at 19 and 20).
3. **One answer in every configuration** (below): yes, and it is the exact total.
4. **No `i128::MIN` marker on a narrow path:** the product holds no marker constant or `i128::MIN` at all; the
   ungrouped `state`, the grouped `state` and `convert_to_state` ship one raw column at precision 19 and under. Pinned:
   `a_narrow_state_ships_the_raw_total_and_never_a_marker` (a partial of `−2 × (10^19 − 1)`, and ten narrow partials
   of `−9 × 10^36` merged, shipped by a final step's spill as `−9 × 10^37`, then NULL or `ARITHMETIC_OVERFLOW` at
   `evaluate`; input past its precision raises an error in both accumulators).

### Determinism (check 3, [delta2/det/](delta2/det/map.md))

The sizing's step 3 on the pin file's tables with columns at precision 10, 18, 19, 20 and 38: `big` (three Parquet
files of 300,000 rows, 3,000 groups; per group `M` in file a, `5` in b, `−M` in c, exact total `5`) and `t4x` (groups
21 to 25 over three files). Each configuration ran every grouped and ungrouped `sum` and `try_sum` (120 queries) 20
times in each ANSI mode; grouped `avg` reported. Lane B has 10 cores, so 10 cores and `SET target_partitions = 16`
stand in for the sizing's 16 cores.

| cores | stealing | `target_partitions` | ANSI | most answers of any `sum` / `try_sum` query | queries not the exact total | grouped `avg` on `big` (reported) |
|---|---|---|---|---|---|---|
| 2, 4, 8, 10 | on | default | off / on | 1 | 0 | 1, 3, 3, 3 / 1 |
| 2, 4, 8, 10 | off | default | off / on | 1 | 0 | 1 / 1 |
| 10 | on | 2, 4, 8, 16 | off / on | 1 | 0 | 1, 4, 3, 1 / 1 |

All 24 configurations: one answer on every run for all 120 queries, each the exact total (`5` per group, `15000` per
table, `t4x` g21 `M`, g22 past 38 digits at precision 38 so NULL or `ARITHMETIC_OVERFLOW`). Only grouped `avg`,
which this unit does not touch, varies (up to 4 answers; `t4x` 2 at 2 cores and `SET 2`).

### The bar (head `pkg-sum-fcb0c888`; main now `pkg-main-6286ba19`)

The step 0's whole domain table re-ran on the head and on main now (raw: [delta2/raw/](delta2/raw/map.md)).

| build | ANSI | EQUAL | BOTH_RAISE | SPARK_RAISES | VALUE | REPARK_RAISES | BOTH_RAISE_OTHER | FORMAT |
|---|---|---|---|---|---|---|---|---|
| main now `pkg-main-6286ba19` | on | 1,770 | 138 | 549 | 25 | 24 | 31 | 0 |
| delta 1 `pkg-sum-6d730107` | on | 1,773 | 655 | 32 | 6 | 40 | 31 | 0 |
| **head `pkg-sum-fcb0c888`** | on | **1,752** | **557** | **120** | **43** | **24** | **41** | 0 |
| main now | off | 1,913 | 0 | 0 | 569 | 47 | 0 | 8 |
| delta 1 | off | 2,448 | 0 | 0 | 47 | 34 | 0 | 8 |
| **head** | off | **2,312** | 0 | 0 | **182** | **35** | 0 | 8 |

- **Cell for cell against the sizing's prototype (i):** 0 cells differ in verdict or answer (2,537 Spark-door cells
  and the revision's 121 per mode), and the 172 `local[4]` queries per mode compare to Spark exactly as (i)'s did.
- **Against delta 1** (the sizing's comparison, [delta2/compare/head-vs-delta1.txt](delta2/compare/head-vs-delta1.txt)):
  388 cells move, the same 388 as the sizing.
- **The two mains differ on 6 cells** ([main-vs-main.jsonl](delta2/compare/main-vs-main.jsonl)): three grouped
  `try_sum` cells over several partials, where main's own `try_sum` varies run to run (`try/s4/g11/c38_38` on,
  `try/s4/g11/c38_10` off, `mp/try/s4/g11/c38_10` on), and three Iceberg `INSERT` statements that both mains refuse, whose error
  names a different first wrapped row (`ice/insert/wide100/c38_0` on, `ice/insert/metrics_full/c38_0` and `c38_38`
  off).
- **Fable's test of its account of Spark: 0 moved cells at precision 28 and under.** The bar holds 766 cells per mode
  at precision 28 or under; every moved cell, against delta 1 and against main, is at precision 34 or 38.
- **No wrong value:** the sizing's `check_exact.py` finds every moved value row the exact total (234 machine-checked,
  0 mismatches; the 121 rows of `HAVING`, `beside`, `plus1` and the Iceberg read-backs are the sizing's hand-checked
  rows, unchanged), and every total past 38 digits raises or answers NULL.
- **Hard bar:** 0 EQUAL→WRONG and 0 EQUAL→RAISE outside the named classes below. `avg`, the controls (`BIGINT`,
  `DOUBLE`), windows and the native door do not move against delta 1 or prototype (i). Against main now, 1,230 cells
  move ([head-vs-main.txt](delta2/compare/head-vs-main.txt)): the build's own fixes (windows and grouped totals into
  EQUAL or BOTH_RAISE), the 45 per mode below, and cells non-EQUAL on main too (Iceberg writes main refused and the
  head commits as the exact total, and two view-flip cells of TEMP-VIEW-SESSION-SETTINGS-1's class).

### The named classes ([delta2/classes.md](delta2/classes.md), every cell listed)

| class | Spark `local[1]`, on / off | Spark `local[4]`, on / off | the sizing measured |
|---|---|---|---|
| **the kept divergence** (decision I): the exact total fits, Spark raises (on) or answers NULL (off) | 133 / 161 | 30 / 30 (each split packing) | 133 / 161 and 30 per mode |
| **the condition class**, out of BOTH_RAISE (both raise, another condition) | 27 / 0 | — | 23 out, plus the 4 expression cells it listed apart |
| the condition class, into BOTH_RAISE | 17 / 0 | — | 17 |
| **the loud expression cell** `sum/plus1/g11/c38_38` | 0 / 1 | — | 1 |
| into EQUAL | 20 / 20 | 9 / 6 into EQUAL or BOTH_RAISE | 20 / 20 and 9 / 6 |

- **The kept divergence, against `local[1]`:** grouped 52 / 58, ungrouped 8 / 8, `try_sum` 31 / 31, DataFrame door
  33 / 45, Iceberg writes 9 / 19 (with ANSI on, 9 writes Spark refuses and RePark commits the exact total; with it off,
  19 store the exact total where Spark stores NULL). Against main now it is 45 cells per mode, since main's wrapping
  sum already answered the exact total where it fits. It comes from a few input shapes (`M, M, −M, −M, 5`; `M, 1, −1`;
  their multi-file layouts; the large-group `M`×10,001 … `5`) over every column and query form.
- **The condition class (ANSI on):** 23 cells where Spark raises `NUMERIC_VALUE_OUT_OF_RANGE` (its partials fit and the
  merged total does not) and RePark `ARITHMETIC_OVERFLOW` (17 multi-split ungrouped cells, 6 ungrouped `DISTINCT` cells
  that DataFusion rewrites to a group-by under a plain `sum`), and 4 expression cells (`sum/beside/g11`, `sum/plus1/g11`
  at `c38_0` and `c38_38`), where the expression's own error now differs; 17 default-pass ungrouped cells join
  BOTH_RAISE. Spark's condition follows its split layout, which no steady rule can copy; Fable's recommendation, adopted
  by the owner's line, is `ARITHMETIC_OVERFLOW` for every non-window total.
- **Into EQUAL:** ruling B's 32 layout cells (all 32) and the 8 `try_sum` cells beside them (all 8) reach Spark's
  answer; so does layout class (c)'s `repartition(4)` at `local[4]`.

### The cards (listed here; the clerk files them)

**SUM-EXACT-TOTAL-DIVERGENCE-1** (proposed; **owner-accepted by decision I**, 2026-11-18). It **supersedes delta 1's
proposed SUM-GROUPED-MERGE-TOTAL-1.** Decimal `sum` and `try_sum` answer the exact total whenever it fits the result
precision; where Spark's own running total passes 38 digits and comes back (per row, per split or in its merge order),
Spark raises `ARITHMETIC_OVERFLOW` (ANSI on) or answers NULL, and RePark answers the exact total on every run, layout
and machine. That includes ANSI-on Iceberg writes Spark refuses and RePark commits, and ANSI-off writes that store the
exact total where Spark stores NULL. Cells: the kept divergence above (133 / 161 against `local[1]`, 30 per mode
against `local[4]`), the pin file's `kept-divergence:` cells and `t4` group 21. **With its condition class:** a
non-window total past 38 digits raises `ARITHMETIC_OVERFLOW` where Spark raises `NUMERIC_VALUE_OUT_OF_RANGE` after a
merge (23 cells, ANSI on; the pin file's `condition-class:` cells). The label wording is the orchestrator's to file.

**The expression card** (proposed name DECIMAL-ADD-SCALE38-OVERFLOW-1; **the rating is the owner's**). `x + 1` on a
`decimal(38,38)` value of `0.99999999999999999999999999999999999999` (a column, not a literal) raises `Arrow error:
Arithmetic overflow` in RePark's decimal addition, on main `6286ba19` and on this head alike, in both ANSI modes;
Spark 4.1.2 answers `2.000000000000000000000000000000000000` (`decimal(38,36)`) in both (measured; a literal `0.5`
answers `1.5` on both engines). The bar's `sum/plus1/g11/c38_38` (ANSI off) shows it now because the total is `M` at
scale 38 instead of NULL; with ANSI on it changes the error of `sum/beside/g11` and `sum/plus1/g11`. Not this unit's
seat (the expression layer).

### Pins (red and green)

- `test_sum_dec_i128wrap_1.py`: the old cells as `test_a_cell_answers_as_spark_or_as_its_named_class`, each id naming
  its class (`spark:`, `kept-divergence:` 16 cells, `condition-class:` 2 cells with ANSI on);
  `test_spark_drops_the_total_of_the_kept_divergence_group_on_every_run` (the oracle's 20 Spark runs of group 21);
  and `test_a_multi_partial_total_is_the_exact_total_on_every_run_at_2_4_and_8_partitions`, which replaces the repeat
  pin that failed CI (36 tests: `big` and `t4x` × `c19_0`, `c20_0`, `c38_0` × `target_partitions` 2, 4, 8 × both
  modes; every grouped and ungrouped `sum` and `try_sum`, 20 runs, one answer, the exact total).
- **Head `pkg-sum-fcb0c888`: 194 passed.** **Delta 1 `pkg-sum-6d730107`: 30 failed** (the 18 class cells, and all 12
  `c38_0` partition pins: `big` grouped `sum` up to 11 answers in 20 runs, the ungrouped total `15000` or NULL, or with
  ANSI on `ARITHMETIC_OVERFLOW` on every run where the total is `5`; `t4x` g21 `M` or NULL); its `c19_0` and `c20_0`
  pins pass, as nothing at those precisions can pass 38 digits. **Main `pkg-main-6286ba19`: 142 failed.**
- Rust, `aggregate::sum_tests` (33 pass): the tests the prototype left failing now assert the exact carrier (the
  marker tests became the exact total through spill, `RepartitionExec`, skipped partials and an IPC round trip, at
  precision 19, 20 and 38), plus the boundary tests named in the checks and
  `a_total_at_the_38_digit_limit_fits_and_one_past_it_does_not` and
  `a_wide_total_wraps_128_bits_and_comes_back_from_precision_20`.
