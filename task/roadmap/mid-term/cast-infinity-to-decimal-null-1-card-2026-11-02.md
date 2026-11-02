# Card CAST-INFINITY-TO-DECIMAL-NULL-1: a non-finite DOUBLE or FLOAT cast to DECIMAL refuses, where Spark gives NULL

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-4 verifier verdict (its F2).

**Status:** filed; grade C, P3; not dispatched. Pre-existing: head equals base. The subscript inherits the cast; the cast itself is outside the subscript.

**Why:** Spark 4.1.2 casts an infinite or NaN DOUBLE or FLOAT to DECIMAL as NULL, in both ANSI modes (its decimal conversion of the non-finite value raises an internal number-format error, which the cast maps to NULL). RePark's CAST refuses the same value with Arrow's overflow text on the head and on the base. A struct-key or array-key lookup whose DECIMAL key field or element receives such a leaf therefore refuses where Spark answers NULL. The cause is the cast, not the subscript: the subscript casts each leaf with the door's own CAST, as Spark's rule does.

**Measured (Spark 4.1.2 against RePark on the head; copied from the round-4 verifier):**

```text
CAST(CAST('Infinity' AS DOUBLE) AS DECIMAL(5,2))              Spark: NULL    RePark: refuses (Overflowing on inf)
CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(5,2))                   Spark: NULL    RePark: refuses (Overflowing on NaN)
CAST(CAST('Infinity' AS FLOAT) AS DECIMAL(5,2))               Spark: NULL    RePark: refuses
SELECT id, m[named_struct('k', CAST('Infinity' AS DOUBLE), 'j', 2)] FROM r   (MAP<STRUCT<k: DECIMAL(5,2), j: INT>, INT>)
                                                              Spark: [[1, None], [2, None]]    RePark: refuses
map(array(CAST(0 AS DECIMAL(5,2))), 7)[array(CAST('Infinity' AS DOUBLE))]   Spark: [[None]]    RePark: refuses
```

A finite overflow (`1e10D` to DECIMAL(5,2)) is not this case: Spark raises `NUMERIC_VALUE_OUT_OF_RANGE`, RePark refuses too, and the card does not cover it.

**Fix (proposed):** a non-finite DOUBLE or FLOAT cast to DECIMAL gives NULL in both ANSI modes, as Spark's CAST does, so the subscript answers as Spark does without change; the cells above are the pins. Finite overflow keeps its refusal.

**Retires:** the card when the four CAST rows and the two subscript rows answer as Spark does, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clause C-051. The same engine cast family as [CAST-DOUBLE-TO-DECIMAL-ROUNDING-1](cast-double-to-decimal-rounding-1-card-2026-11-02.md) (C-044), which is a rounding divergence, not a refusal; this card is the non-finite refusal.

Up: [map.md](map.md).
