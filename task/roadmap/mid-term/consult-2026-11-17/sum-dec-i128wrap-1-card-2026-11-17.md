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
