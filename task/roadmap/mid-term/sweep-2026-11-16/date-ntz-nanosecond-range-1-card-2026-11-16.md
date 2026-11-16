# Card DATE-NTZ-NANOSECOND-RANGE-1: a DATE after 2262-04-11 beside a TIMESTAMP_NTZ overflows the nanosecond path

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 9). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S3 (owner to rule). A rare shape: a DATE on or after 2262-04-12.

## Finding

The sweep's DATE values include 2262-04-12, which lies past the range of a 64-bit nanosecond count (the range ends on 2262-04-11). When a DATE is compared or combined with a TIMESTAMP_NTZ on main, the DATE is converted through `TIMESTAMP_NS`. With ANSI off the row comes back NULL where Spark answers `false`. With ANSI on main raises `CAST_OVERFLOW` where Spark answers.

## Measured

| cell | ANSI | Spark 4.1.2 | main | class |
|---|---|---|---|---|
| `DATE '2262-04-12'` = TIMESTAMP_NTZ, the 2262 row against four constants | off | `false` | `NULL` | V |
| same, `=` | on | answers `true` and `false` | raises: `The value 106752 of the type "DATE" cannot be cast to "TIMESTAMP_NS" due to an overflow` | R |
| `<`, `<=>`, `IN`, `BETWEEN`, `CASE`, `if`, `coalesce`, `greatest`, `array`, `UNION ALL` | off | answers | same type, different value on the 2262 row | V |
| `NULLIF` | off | `date` | `timestamp_ntz` | T |

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:eq:DATE:TIMESTAMP_NTZ t:eq:DATE:TIMESTAMP_NTZ`

## Not yet known

The seat, and whether the conversion is the nanosecond path that TRANSITION-DAY-ARITHMETIC-1 names in its residue R-3. That card notes the nanosecond range and does not card it. Related: ICE-TSNS-TYPE-PLANNER-1 (out-of-range raises in both ANSI modes, on the timestamp_ns planner), a different seat.

Up: [map.md](map.md).
