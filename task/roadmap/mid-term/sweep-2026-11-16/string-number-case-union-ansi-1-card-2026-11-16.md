# Card STRING-NUMBER-CASE-UNION-ANSI-1: with ANSI on, CASE and UNION over a STRING beside a number answer a string where Spark picks the number type and raises

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 16). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** ruled 2026-11-16, not dispatched; not label-blocking.

**Severity:** S3, ruled 2026-11-16 (the proposed severity stands). A type-only change on well-formed text; a malformed text answers where Spark raises.

Ruled 2026-11-16 ("Merge those and defaults."): the proposed S3 stands; not label-blocking.

## Finding

With ANSI on, Spark's common type for a STRING beside an INT, TINYINT or SMALLINT in `CASE`, `UNION ALL` and `UNION` is the number type, and a malformed text then raises `CAST_INVALID_INPUT`. Main picks STRING and answers. A column of well-formed text comes back as strings, and a malformed text, which Spark refuses, comes back as a string.

## Measured

Class A on every row: RePark answers and Spark refuses.

| cell, ANSI on | Spark 4.1.2 | main |
|---|---|---|
| `CASE`, STRING `'1.25'` against INT | raises `CAST_INVALID_INPUT`: `The value '1.25' of the type "STRING" cannot be cast to "BIGINT" because it is malformed` | a string column: `1`, `1`, `1`, `100000`, `1` |
| `UNION ALL`, STRING against INT | the same error | a string column, including `1.25` as text |

The sweep's counts of class A with ANSI on are 40 for `CASE`, 48 for `UNION ALL` and 48 for `UNION`. With ANSI off the counts are 6, 6 and 6.

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py t:case:STR_NUM:INT t:union_all:STR_NUM:INT`

## Not yet known

The seat. COALESCE-ANSI-WIDEN-1 names the same common-type seat for bare `coalesce` and `array` over STRING beside DATE, BINARY and DECIMAL, a different set of positions. NVL-TYPE-COERCION-2 and DECIMAL-COMMON-TYPE-1 name the seat too.

Up: [map.md](map.md).
