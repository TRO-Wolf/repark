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
