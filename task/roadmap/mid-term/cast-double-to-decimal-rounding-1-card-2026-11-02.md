# Card CAST-DOUBLE-TO-DECIMAL-ROUNDING-1: `CAST` from DOUBLE to DECIMAL rounds the binary value, where Spark rounds the double's shortest text

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-3 verifier verdict (F2).

**Status:** filed; grade C, P3. **Scalar casts FIXED 2026-11-13 by [DOUBLE-DECIMAL-STORE-ROUND-1](double-decimal-store-round-1-card-2026-11-13.md)** (the same mechanism, measured as one: `CAST(1.005D AS DECIMAL(5,2))` and `CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2))` give `1.01` on the Spark door, both ANSI modes, pinned); the subscript and array-element cells move to [DOUBLE-DECIMAL-NESTED-LEAF-1](double-decimal-nested-leaf-1-card-2026-11-13.md). Originally: Engine-wide: the subscript inherits it, and the subscript fix does not change it.

**Why:** Spark 4.1.2 casts a DOUBLE to DECIMAL through the double's shortest decimal text, so `1.005` rounds half up to `1.01`. RePark rounds the binary value, and `1.00499...` gives `1.00`. The divergence depends on the value: `2.675` agrees in both engines.

**Measured (Spark 4.1.2 and RePark, head and base; copied from the round-3 verifier):**

```text
SELECT CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2)), CAST(CAST('2.675' AS DOUBLE) AS DECIMAL(5,2)), CAST(1.005D AS DECIMAL(5,2))
Spark:    1.01, 2.68, 1.01
RePark:   1.00, 2.68, 1.00
```

The subscript inherits it. A map over `DECIMAL(5,2)` keys with entries `1.00` (7) and `1.01` (8), looked up with `named_struct('k', CAST('1.005' AS DOUBLE), 'j', 2)`, gives Spark `[[1, 8], [2, None]]` and RePark `[[1, 7], [2, None]]` (the base refused). The array forms `array(CAST('1.005' AS DOUBLE))` and `array(1.005D)` behave the same.

**Fix (proposed):** make the DOUBLE-to-DECIMAL cast round the double's shortest decimal text, half up, as Spark does. The fix belongs to the engine's CAST, which the subscript's leaf cast now shares; it is not a subscript change. The subscript cells above become pins once the cast is fixed.

**Retires:** the card when `CAST(1.005D AS DECIMAL(5,2))` gives `1.01` on the native, Spark and ANSI doors, and the subscript cells answer Spark's `[[1, 8], [2, None]]`, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clause C-044 (a kept divergence, no pin asserts it).

Up: [map.md](map.md).
