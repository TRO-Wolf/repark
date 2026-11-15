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

Up: [map.md](map.md).
