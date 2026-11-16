# Card CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1: with ANSI off, a finite FLOAT or DOUBLE that overflows a DECIMAL cast refuses, where Spark returns NULL

**Date:** 2026-11-15. **Filed by:** the DECIMAL-DOUBLE-UNION-COERCION-1 part 1 builder (`claude-opus-5-5`), from
its step 0′ finding F2. The orchestrator ruled it on 2026-11-15: a card, and part 1 keeps main's and ff34's overflow
behaviour. Beside [CAST-INFINITY-TO-DECIMAL-NULL-1](cast-infinity-to-decimal-null-1-card-2026-11-02.md), which is the
non-finite half.

**Status:** filed; not dispatched. Pre-existing: head equals main.

**Severity: S3, a refusal.** It is not a silent wrong answer. With ANSI off, Spark returns NULL for the row that
overflows. RePark refuses the whole statement (`Arrow error: Invalid argument error: 396191 is too large to store
in a Decimal128 of precision 5. Max is 99999`). With ANSI on, both raise.

## Measured

- **Verifier 2's coercion fuzz with ANSI off**: 19,820 cells, every shape at `DECIMAL(5,0)`, wrong on main `dc6b4c94`,
  on `ff34baf5` and on the part-1 head alike, because the shape fails as a whole
  (`task/wo/decimal-double-union-coercion-1/evidence/step0b/table.md`, section 5).
- **Main, ANSI off:**
  - over the rows (-1.938…, 396191.0, 7.5), `CAST(v AS DECIMAL(5,0))` refuses;
  - without the 396191.0 row it answers -2 and 8;
  - `CAST('396191' AS DECIMAL(5,0))` also refuses.
- **The written-cast kernel the part-1 head restores** (`__repark_float_to_decimal__`) keeps that refusal, as ff34
  had it (`safe` only for `TRY_CAST`).

## Fix direction

The cast kernel's overflow contract would follow Spark: NULL for the row with ANSI off, `NUMERIC_VALUE_OUT_OF_RANGE`
with ANSI on. It is the same seat as CAST-INFINITY-TO-DECIMAL-NULL-1 (non-finite values are NULL in both modes).

## Append (2026-11-16): the message moved at #1339, both texts quoted

Measured by the records clerk on main's packages against Spark 4.1.2, both ANSI modes, both doors. Main's package
is the post-#1339 tree (`a06c1088`; its product tree equals the cited package); the old package is main before #1339
(`dc6b4c94`).

- **Before #1339** (`dc6b4c94`, Spark door, ANSI off and on): `SELECT CAST(CAST(396191 AS DOUBLE) AS DECIMAL(5,0))`
  and `SELECT CAST(CAST(1e30 AS DOUBLE) AS DECIMAL(10,2))` both refuse with
  `datafusion engine error: Optimizer rule 'simplify_expressions' failed`.
- **After #1339** (`a06c1088`, Spark door, ANSI off and on): the same two statements refuse with
  `Arrow error: Invalid argument error: 396191 is too large to store in a Decimal128 of precision 5. Max is 99999`
  and `Arrow error: Invalid argument error: 1000000000000000000000000000000.00 is too large to store in a
  Decimal128 of precision 10. Max is 99999999.99` respectively.
- **Spark 4.1.2:** ANSI off answers NULL (`decimal(5,0)` / `decimal(10,2)`); ANSI on raises
  `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] 396191.0 cannot be represented as Decimal(5, 0)`
  (`ArithmeticException`).
- **Native door** (`a06c1088`): both statements still refuse with the old `simplify_expressions` failure, in both
  modes. The #1339 kernel sits on the Spark door only.

The behaviour class is unchanged, as the brief states: RePark refuses the whole statement where Spark returns NULL
with ANSI off. Severity stays S3.

Up: [map.md](map.md).
