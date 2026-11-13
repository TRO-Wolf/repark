# Card LOG-NONPOSITIVE-NULL-1: `ln`, `log10` and `log2` of zero or a negative answer -Infinity or NaN where Spark answers NULL

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): item 1's addendum on main `276dc503`, group `log`; Fable's review of 2026-11-12, §3.

**Status:** filed 2026-11-12, not dispatched. Severity **S2** (Fable: a family of functions, not one; the drafter's
S3 `LN-NONPOSITIVE-NULL-1` is this card, widened, and is not filed separately). A silent wrong value on both doors;
it does not block the scoped label. Fable asks for a card and a known-divergences line.

**Measured on main `276dc503` against Spark 4.1.2** (12 WRONG-READ cells; Fable measured five of them on 1.5.6 first):

| cell | Spark | RePark, Spark door and native |
|---|---|---|
| `ln(CAST(-1.0 AS DOUBLE))` | NULL | NaN |
| `ln(CAST(0.0 AS DOUBLE))` | NULL | -Infinity |
| `log10(CAST(-1.0 AS DOUBLE))` | NULL | NaN |
| `log10(CAST(0.0 AS DOUBLE))` | NULL | -Infinity |
| `log2(CAST(-1.0 AS DOUBLE))` | NULL | NaN |
| `log2(CAST(0.0 AS DOUBLE))` | NULL | -Infinity |

- Already matching on the Spark door: `log(x)`, `log(b, x)` and `log1p` (`log1p(-1.0)` and `log1p(-2.0)` are NULL on
  both doors).
- **Native only:** `log(CAST(-1.0 AS DOUBLE))` and `log(CAST(2.0 AS DOUBLE), CAST(-1.0 AS DOUBLE))` answer NaN where
  Spark answers NULL.
- Controls `ln(1.0)`, `log(2.0, 8.0)` and `log1p(1.0)` are EQUAL.

**Fix direction:** Spark's `ln`, `log10` and `log2` return NULL for an input at or below zero, as RePark's `log` and
`log1p` already do on the Spark door. Give the three functions the same guard on both doors, and decide in the same
unit whether native `log` follows. Pin every row above on both doors.

**Close:** fixed with those pins, or declined by a recorded ruling.
