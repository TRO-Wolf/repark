# Card SUM-DEC-I128WRAP-1: a decimal `sum` past 38 digits answers silently; Spark raises

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** Fable's consult reply of 2026-11-17 (evening), section 7, "seen in passing", measured by Fable on #1355's head `8ac1683a` with ANSI on. The reply is named, not linked: "Fable reply — AVG verifier F1 and view settings 2026-11-17.md" (it sits outside the repository). Fable's note says the owner has not seen these cells. The registry already carries this id, so the card points at it rather than repeating it.

**Status:** filed 2026-11-17, not dispatched. Step 0 dispatched 2026-11-17 (the orchestrator's reading of the owner's words; see the [rulings record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), evening section).

**Severity: not rated** (the owner's to set). Fable's section 6, item 5, lists this finding as the owner's decision; it gives no rating.

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
