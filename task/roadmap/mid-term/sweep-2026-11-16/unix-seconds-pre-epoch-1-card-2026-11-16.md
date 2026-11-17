# Card UNIX-SECONDS-PRE-EPOCH-1: unix_seconds of a pre-1970 timestamp with a fraction answers zero where Spark answers minus one

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 10). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking.

**Severity:** S3, ruled 2026-11-16 (the proposed severity stands). A rare shape: a timestamp before 1970 with a fraction of a second.

Ruled 2026-11-16 ("Merge those and defaults."): the proposed S3 stands; not label-blocking.

## Finding

`unix_seconds(ts)` for `1969-12-31 23:59:59.5` answers `-1` on Spark, which floors the value, and `0` on main, which truncates toward zero. Whole seconds before 1970 and every value after 1970 agree. The rule is FN-UNIX in the audit's inventory (`datafusion-functions` `spark/datetime/unix.rs`, its cast chain, noted there for rounding of pre-epoch fractions).

## Measured

Both ANSI modes give the same result. The values are for the session zone `America/Los_Angeles`, which does not change the epoch count.

| row | Spark 4.1.2 | main |
|---|---|---|
| `1969-12-31 23:59:59.5` | `-1` | `0` |
| `2024-01-01 02:00 UTC` | `1704074400` | `1704074400` |
| `2025-01-01 07:00 UTC` | `1735714800` | `1735714800` |

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark false`, and the same with `spark`; the case `unix_seconds`.

## Not yet known

The seat. The fix is in the epoch cast, not the function name; step 0 confirms.

Up: [map.md](map.md).
