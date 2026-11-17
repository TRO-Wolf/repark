# Card AVG-GROUPED-SUM-STICKY-1: a grouped decimal `avg` checks its sum per row on Spark, and once on RePark

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-17, "Merge 1354, option a on the grouped sum" (ruling (a) on the AVG-DECIMAL-ROUND-1 build). **Source:** the [AVG-DECIMAL-ROUND-1 card's Built append](../decimal-common-type-1-side/avg-decimal-round-1-card-2026-11-16.md) (the multi-partition case), [bar.md](../../../wo/avg-decimal-round-1/evidence/built/bar.md) section 5, and the [raw partition probe](../../../wo/avg-decimal-round-1/evidence/built/raw/partition/). The build is on PR #1355 (open, not merged).

**Status:** filed 2026-11-17, not dispatched.

**Severity: not rated; not label-blocking** (owner, 2026-11-17, verbatim "Merge 1354, option a on the grouped sum").

## Finding

In a grouped decimal `avg`, Spark checks the sum per row, because its hash aggregate stores the buffer per row. A group whose sum passes 38 digits inside one partition and then comes back raises `ARITHMETIC_OVERFLOW` with ANSI on, and answers NULL with ANSI off. RePark keeps the exact running sum and checks once at the end, so it answers the exact average (`1.0000` in the measured cell).

- Ungrouped, both engines answer `1.0000` (cell `s04`).
- Across two files, both engines raise or answer NULL (pinned).

## Measured

- Spark 4.1.2, the shape `MAX38`, `MAX38`, `-MAX38`, `-MAX38`, `5` at `decimal(38,0)` ([bar.md](../../../wo/avg-decimal-round-1/evidence/built/bar.md) section 5). One partition, grouped: `[ARITHMETIC_OVERFLOW] Overflow in sum of decimals` with ANSI on, NULL with ANSI off. RePark answers `1.0000` on both (the ruled unbounded running sum). One partition, ungrouped: `1.0000` on both engines.
- The raw probe, [raw/partition/](../../../wo/avg-decimal-round-1/evidence/built/raw/partition/): `mp-spark-true.txt` records `one/grouped` as the overflow and `one/ungrouped` as `1.0000`; `mp-spark-false.txt` records `one/grouped` as NULL. Across two files (`two/grouped`, `two/ungrouped`), Spark raises the overflow with ANSI on and answers NULL with ANSI off.

## Seat (not diagnosed)

- The seat is the grouped accumulator `DecimalAvgGroups` in `crates/repark-functions/src/avg_decimal.rs`. This card names the seat and does not diagnose it.
- Measured in the build: a third state column (a wrap count shipped with each partial state) cost K1 1.19x, so the build kept the declared state types ([built/cost.md](../../../wo/avg-decimal-round-1/evidence/built/cost.md), design history).
- Not measured: a per-row overflow flag held in memory, which would mirror Spark's per-row check.

## Ruling

Owner, 2026-11-17, verbatim: "Merge 1354, option a on the grouped sum". Option (a) means: record the grouped-sum difference on a card, not label-blocking, and merge the unit as it is.

Not in this class: the ANSI-on window `avg` text. Both engines raise the same result-precision condition there, and only the text differs (AVG-DECIMAL-ROUND-1 card, delta 1 append).

Up: [map.md](map.md).
