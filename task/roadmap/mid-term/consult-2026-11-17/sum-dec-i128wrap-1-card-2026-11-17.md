# Card SUM-DEC-I128WRAP-1: a decimal `sum` past 38 digits answers silently; Spark raises

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** Fable's consult reply of 2026-11-17 (evening), section 7, "seen in passing", measured by Fable on #1355's head `8ac1683a` with ANSI on. The reply is named, not linked: "Fable reply — AVG verifier F1 and view settings 2026-11-17.md" (it sits outside the repository). Fable's note says the owner has not seen these cells. The registry already carries this id, so the card points at it rather than repeating it.

**Status:** step 0 PR #1357; S1, label-blocking (owner, 2026-11-17).

**Severity: S1, label-blocking** (owner, 2026-11-17, "defaults on both"; see the dated append below). Fable's section 6, item 5, lists this finding as the owner's decision and gives no rating.

## Finding

A decimal `sum` whose true value passes 38 digits answers a value, silently, where Spark raises. Fable's measured cells, on #1355's head with ANSI on (Fable's measurement):

- `sum` over `MAX38` and `1`, both `DECIMAL(38,0)`, answers `100000000000000000000000000000000000000`: 39 digits in a `decimal(38,0)` column.
- `sum` over two `MAX38` answers `-140282366920938463463374607431768211458`. Main answers the same.
- Spark raises `ARITHMETIC_OVERFLOW` for both cells.
- Sums that fit agree (Fable's note; the note does not itemise the engines).

## Registry

The registry entry is [SUM-DEC-I128WRAP-1](../../../../docs/spark-sql-iceberg-parity.md), BACKLOG 2026-09-10, filed by TORTURE-1 step 2. It records the torture `decimal_overflow` fixture, where `sum` answers a wrapped `decimal128(38, 0)` value on the SQL and DataFrame doors. Its pin, `test_decimal_overflow_sum_refuses_loud`, is xfail(strict) on that fixture. Fable's consult did not read the registry and found no card by file name. This card does not file a second pin; the 2026-11-17 dated bullet in the registry entry carries Fable's two cells.

## Relation

[AVG-DECIMAL-ROUND-1](../decimal-common-type-1-side/avg-decimal-round-1-card-2026-11-16.md)'s exact running sum, on #1355. The relation is the orchestrator's.

## Seat

Not diagnosed. Fable's reply gives no seat for these cells, and its section 8 lists the #1355 diff beyond `aggregate.rs:290-346` as unchecked.

Up: [map.md](map.md).

## Dated append (2026-11-17, later): the step 0 PR #1357 and the owner's rulings

**Owner, 2026-11-17, later the same evening, verbatim "defaults on both".** The owner adopted the step 0's defaults for questions 1 to 6 (the sketch's section 6 on PR #1357's branch, not on this branch). The list is as given for the [rulings record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section, not the owner's words:

- Q1: S1, label-blocking.
- Q2: `sum` and `try_sum` follow Spark's per-row grouped check. With ANSI on they raise at once. With ANSI off and in `try_sum` they carry a marker, chosen by the 100,000-group cost case at 1.05x or less; if it misses, the build stops and reports. `avg` keeps ruling (a).
- Q3: the native door is carded at S2, not label-blocking: [NATIVE-DECIMAL-SUM-1](native-decimal-sum-1-card-2026-11-17.md).
- Q4: the `BIGINT` overflow with ANSI on is a second unit, [SUM-BIGINT-ANSI-OVERFLOW-1](sum-bigint-ansi-overflow-1-card-2026-11-17.md), S1.
- Q5: (a) orphan files after a failed Iceberg write: [ICE-FAILED-WRITE-ORPHAN-FILES-1](ice-failed-write-orphan-files-1-card-2026-11-17.md), S3. (b) [ICE-DECIMAL-WRITE-PRECISION-1](ice-decimal-write-precision-1-card-2026-11-17.md): S1, label-blocking until the writer checks each value.
- Q6: an ungrouped total past 38 digits mirrors Spark: `ARITHMETIC_OVERFLOW` when a partial or a single-phase aggregate overflows; `NUMERIC_VALUE_OUT_OF_RANGE` when the partials fit and the merged total does not.

The step 0 revision's counts behind the severity: 522 silent decimal `sum` cells on the Spark door with ANSI on (step 0), 52 shape cells more (revision), 8 committed wrapped writes and 14 committed 39-digit values in Iceberg statements, and 2 orphan statements. The step 0 sketch says no separate question 7 is needed for views across an ANSI flip: those cells are carried by [TEMP-VIEW-SESSION-SETTINGS-1](../temp-view-session-settings-1-card-2026-11-02.md).

## Built (2026-11-17, Claude Opus 5.5 `claude-opus-5-5`, xhigh) — **HALT** on the bar: 46 layout cells

**Seat A** (sketch §4), branch `fix/sum-dec-i128wrap-1`, product commit `a8016e75`. Five product files (cap 6; the two
`mod` lines in `aggregate.rs` and the test `mod` in `repark-spark/src/extension.rs` do not count):

1. `crates/repark-functions/src/aggregate/spark_sum.rs` (new) — A1/A2: `SparkSum`, `sum` (`Ansi`) and `try_sum`
   (`Try`) over DataFusion's `Sum` / datafusion-spark's `SparkTrySum`; the decimal arm, delegation of every other type,
   `placed` / `legacy` for A3.
2. `crates/repark-functions/src/aggregate/sum_decimal.rs` (new) — the accumulators (`DecimalSum`, `DecimalSumGroups`,
   `DecimalSumDistinct`), Spark's check and conditions, the marker, the magnitude bound.
3. `crates/repark-functions/src/aggregate.rs` — registration in `functions()`, `sum_udaf()`, `try_sum_udaf()`, and A3:
   `SparkAvgAnsiMode` places each `sum` with ANSI on and swaps it to `Legacy` with ANSI off.
4. `crates/repark-functions/src/avg_decimal.rs` — `safe_rows`, `add_batch`, `sub_batch` made `pub(crate)` (shared
   arithmetic only; no behaviour change).
5. `crates/repark-python/src/column/function_dispatch.rs` — the `:938` arm returns `sum_udaf()`.

**The rulings carried (owner, 2026-11-17, verbatim):** "dispatch Opus xhigh effort levels for these fixes, then use
Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out"; "defaults on both"
(Q1 S1 label-blocking; Q2 follow Spark's per-row grouped check, raise at once with ANSI on, a marker with ANSI off and
in `try_sum` chosen by K1 at 1.05x or less, `avg` keeps ruling (a); Q3 the native door carded NATIVE-DECIMAL-SUM-1,
S2; Q4 SUM-BIGINT-ANSI-OVERFLOW-1 a second unit; Q5 ICE-DECIMAL-WRITE-PRECISION-1 its own card, S1, orphans S3; Q6
`ARITHMETIC_OVERFLOW` for an overflowed partial or a single-phase aggregate, `NUMERIC_VALUE_OUT_OF_RANGE` when the
partials fit and the merged total does not); "Defaults, merge 1355" (views read under other settings:
TEMP-VIEW-SESSION-SETTINGS-1, not fixed here).

**The marker:** the **sentinel** `i128::MIN` in the one declared state column (NULL stays "empty"), with ANSI off and
in `try_sum`; with ANSI on the grouped accumulator raises at the first overflowing row and the partial at `state()`,
so its state is the same one column with no marker. K1 (200,000 groups, head / main, five runs): `(10,2)` 0.909 on and 0.954 off, `(38,0)` 0.983 on and 0.950 off ([cost.md](../../../wo/sum-dec-i128wrap-1/evidence/built/cost.md)); the second state column was not needed. Survival pins
(`aggregate::sum_tests`): an Arrow IPC round trip (spill and the distributed data path), a spilling grouped aggregate
(`spill_count > 0`), `RepartitionExec` with and without skipped partial aggregation (`convert_to_state`); no
`validate_decimal_precision` sits between the partial and final steps. The distributed plan codec carries the UDAF by
name (Ballista's codec), so a decoded executor gets the registered `sum` in `Ansi` mode: the mode is lost there, as
`avg`'s already is (behind `cluster`, which no door uses).

**The bar** ([bar.md](../../../wo/sum-dec-i128wrap-1/evidence/built/bar.md)): 1,329 cells move; decimal `sum` 430 / 449 silent cells to Spark's answer, the shapes
47 / 51 of 52, `try_sum` 16 / 17, every Iceberg statement EQUAL or BOTH_RAISE, no wrapped or 39-digit value in any data
file, Spark reads every table; `avg`'s 330 cells per mode, the controls and the native door byte-for-byte unchanged.
**Halt:** 46 cells (23 per mode) go from EQUAL to another verdict, all pass-back sums whose answer depends on the
split or order: (a) 20 where Spark's default pass read several files as one split (their multi-partition twins moved
to Spark's answer), (b) 14 grouped and distinct merges whose answer differed run to run on the build's head (corrected 2026-11-18: Spark gives one answer per layout, and two of them, `try/s4/g11/c38_0` and `mp/try/s4/g11/c38_10`, were silent with ANSI on), (c) 12 DataFrame
`repartition(4)` cells (RoundRobinBatch keeps the batch in one partition). Named classes left: the native door, FNP-AGG-1,
`BIGINT` (Q4), `avg` (ruling (a)), views (TEMP-VIEW-SESSION-SETTINGS-1), the layout and order classes, the observations,
message-text classes. The writer card has no `sum` cell left.

**Cost** ([cost.md](../../../wo/sum-dec-i128wrap-1/evidence/built/cost.md)): every held row at or below 1.05x except K3 `sum(d38)` at 1.067 after ten runs, inside its control band (0.978 to 1.089); K2's 1000-row frame 0.929x / 1.016x of the 3-row frame; C-012 1.001 and 0.995; K5 (reported) 0.110.

## Delta 1 (2026-11-18, Claude Opus 5.5 `claude-opus-5-5`, xhigh): the grouped final merge checks its total once

**The ruling (owner, 2026-11-18, about 13:40 UTC, verbatim):** "Go with Fable's leans on A, B and C." It amends Q2 (i)
of 2026-11-17 **for the grouped final merge only** (A: "Check the grouped final merge's total once, not after every
partial."): the partial step keeps Spark's per-row sticky check. B accepts the layout class at S3; C rates the
distributed plan codec card S3 (the clerk files it, not this PR). Fable's earlier answers (its reply of 2026-11-18,
questions 1 and 3) are Fable's guidance.

**Built:** commits `76295c30` and `6d730107` on `fix/sum-dec-i128wrap-1`, one product file
(`crates/repark-functions/src/aggregate/sum_decimal.rs`; still five of the cap of six). `DecimalSumGroups` adds the
merged partials exactly (a wrap count per group) and checks each total once at `evaluate`, raising Spark's measured
grouped condition, `ARITHMETIC_OVERFLOW` with the `try_sum` hint (not Q6's `NUMERIC_VALUE_OUT_OF_RANGE`, which is the
ungrouped one), or answering NULL; an overflowed partial still answers as Spark does. The partial-skip path stays: with
it off, K1 ran at 1.25x to 1.30x.

**Sized before the change:** Spark 4.1.2 at `local[4]` gives one answer on every one of 20 runs for all 172 pass-back
queries; the build's head varied on 17 (ANSI on) and 20 (off) of 401, main on 4 and 4 (its own grouped `try_sum`).
**After:** the head varies on none (corrected in delta 2: on the step 0's small tables at the lane's 16 cores only;
CI and the sizing step found it varying at 2, 4 and 8 partitions); 17 cells move into EQUAL, none out; all 14 class (b) cells are EQUAL; the 32 cells
left from EQUAL are the layout class, the same against both bases. [bar.md](../../../wo/sum-dec-i128wrap-1/evidence/built/bar.md),
"Delta 1".

**Cards (listed in bar.md, filed by the clerk):** SUM-GROUPED-MERGE-TOTAL-1 (proposed; beside
[AVG-GROUPED-SUM-STICKY-1](avg-grouped-sum-sticky-1-card-2026-11-17.md), not rated, not label-blocking): Spark's own
merge order passes 38 digits and comes back, Spark raises or answers NULL, RePark answers the exact total (pinned on
the `t4` group 21, 8 cells). SUM-PARTIAL-LAYOUT-1 (proposed, S3): the 32 layout cells.

**Cost:** not finished. Delta 1 paused at two of five runs (every K1 row held); Fable's guidance after the CI halt was
not to finish a series for a product that would not merge, and delta 2 replaced it.

## Delta 2 (2026-11-18, Claude Opus 5.5 `claude-opus-5-5`, xhigh, the last): exact in every phase, the state chosen by precision

**The ruling (owner, 2026-11-18, about 20:05 UTC, verbatim):** "Go with Fable's recommendations on all seven, re-run
1362". It adopts Fable's §9 recommendations (Fable's guidance under the owner's delegation until this line). For this
unit: decision I, option (1), **amending Q2 (i) of 2026-11-17 for every phase and replacing ruling A of 13:40 UTC**:
decimal `sum` and `try_sum` add exactly in every phase and check once, at `evaluate` (`ARITHMETIC_OVERFLOW` with the
`try_sum` hint for every non-window total with ANSI on; NULL with ANSI off and in `try_sum`); windows keep their check,
`DISTINCT` and `avg` do not move; the state is one raw `i128` column at input precision 19 and under and the low word
plus a wrap count from 20. K1 may reach 1.12x of main at precision 20 and up only. This is the last delta.

**Built:** commit `fcb0c888` (product and Rust pins) and `62c46161` (facade pins) on `fix/sum-dec-i128wrap-1`; two
product files, both among the build's five (`sum_decimal.rs`, `spark_sum.rs`'s `state_fields`; still five of six).

**Checked:** DataFusion 54.1.0 passes the input fields to `state_fields`, so the schema follows the precision; no
narrow path ships a marker; every grouped and ungrouped `sum` and `try_sum` gives one answer, the exact total, in all
24 determinism configurations (2, 4, 8, 10 cores, stealing on and off, `target_partitions` 2, 4, 8, 16), at precision
10, 18, 19, 20 and 38.

**The bar** ([bar.md](../../../wo/sum-dec-i128wrap-1/evidence/built/bar.md), "Delta 2"): cell for cell the sizing's
prototype (i); no moved cell at precision 28 or under; no wrong value. The kept divergence: 133 (ANSI on) and 161 (off)
cells against Spark `local[1]`, 30 per mode against `local[4]`; the condition class 23 out of BOTH_RAISE (plus 4
expression cells) and 17 in, ANSI on; ruling B's 32 layout cells and 8 `try_sum` cells reach Spark's answer.

**Cards (listed in bar.md, filed by the clerk):** SUM-EXACT-TOTAL-DIVERGENCE-1 (proposed, owner-accepted by decision
I; supersedes SUM-GROUPED-MERGE-TOTAL-1), with its condition class; DECIMAL-ADD-SCALE38-OVERFLOW-1 (proposed; the
rating is the owner's): `x + 1` on a `decimal(38,38)` column of `0.99…9` raises an Arrow overflow on main and head,
where Spark answers `2.000000000000000000000000000000000000`.

**Cost:** [cost.md](../../../wo/sum-dec-i128wrap-1/evidence/built/cost.md), "Delta 2".

## Delta 3 (2026-11-19, Claude Opus 5.5 `claude-opus-5-5`, xhigh): the ungrouped path checks the values it adds

**The ruling (owner, 2026-11-19, about 01:28 UTC, verbatim):** "Go with your recommendations and tonight you don’t even
need to wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck". It
adopts the orchestrator's recommendation for this unit, **one more delta past the last-delta rule**, and so amends the
20:05 UTC rule of 2026-11-18 (delta 2 the last; a class (a) or (b) finding parks the unit). In the same line the owner
rated DECIMAL-ADD-SCALE38-OVERFLOW-1 **S3, not label-blocking**, and C1 **S2, not label-blocking** (below).

**Why:** delta 2's fresh verifier (Opus 5.5 medium) returned FAIL at `36db8dd6`. F1: over a Parquet column whose
values pass its declared precision (`decimal(19,0)` / `decimal(20,0)` over values near 10^38), the ungrouped `sum` and
`try_sum` answered the total modulo 2^128 (`5` for `2^128 + 5`) in both modes, against four records that said such
input raises; Spark refuses the read. F2: the `aggregate::sum_tests` count (13, not 33).

**Built:** commit `845766fd` (product and the Rust pin `aggregate::sum_precision_tests`) and `86f7cdca` (facade pins);
`sum_decimal.rs` only in substance (the ungrouped and window batch add: a four-lane checked `i128` total, or every row
with the carry when it overflows), a test module line in `aggregate.rs`, and `avg_decimal.rs` back to main byte for
byte. Still five product files of six.

**Measured** ([bar.md](../../../wo/sum-dec-i128wrap-1/evidence/built/bar.md), "Delta 3"): on the verifier's files,
every `sum` and `try_sum` total answers NULL or `ARITHMETIC_OVERFLOW`, ungrouped and grouped, at precision 19 and 20,
except that a precision-19 partial past 128 bits in a multi-phase plan raises "past its declared precision" (384 of
384 as expected; delta 2 answered 128 values). The ungrouped `avg` keeps the same over-precision wrap, as on main
(reported; the grouped `avg` unit's seat). Delta 2's whole bar: 6,252 of 6,252 verdicts unchanged; 4 of 242 revision
cells move, all main's #1362 (a DataFrame temp view now answers under its own settings). Determinism: one answer, the
exact total, in the verifier's five configurations.

**Cards (ratings recorded in bar.md; the clerk files them):** DECIMAL-ADD-SCALE38-OVERFLOW-1, S3, not label-blocking;
C1 (RePark's Parquet reader accepts decimal values past the declared precision where Spark refuses the read, main the
same), S2, not label-blocking, sized inside ICE-DECIMAL-WRITE-PRECISION-1's step 0.

**Cost:** [cost.md](../../../wo/sum-dec-i128wrap-1/evidence/built/cost.md), "Delta 3".
