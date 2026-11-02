# Card SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1: a scalar DOUBLE map key and RePark's `=` miss `-0.0` and NaN, where Spark matches them

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-3 verifier verdict (out-of-scope observations, the F1 cause).

**Status:** filed; grade C, P3; not dispatched. Pre-existing: head equals base.

**Why:** Spark compares floats with SQL equality in a map lookup: `-0.0 = 0.0`, and every NaN equals every NaN. RePark misses both on a scalar DOUBLE key, which takes DataFusion's scalar path. RePark's general `=` also returns `false` for these pairs. The nested keys of MAP-STRUCT-KEY-SUBSCRIPT-1 normalise floating leaves inside the subscript pipeline only, so the scalar path and `=` remain the gap.

**Measured (Spark 4.1.2 against RePark on the head and the base; copied from the round-3 verifier):**

```text
map(0.0D, 1, ...)[-0.0D]                    Spark: 1    RePark: NULL
map(...NaN key of another bit pattern...)[sqrt(-1D)]   Spark: 2    RePark: NULL
CAST('-0.0' AS DOUBLE) = CAST(0 AS DOUBLE)                   Spark: true     RePark: false
sqrt(CAST(-1 AS DOUBLE)) = CAST('NaN' AS DOUBLE)             Spark: true     RePark: false
```

**Fix (proposed):** normalise floating operands the way Spark's `NormalizeFloatingNumbers` does, in the scalar lookup and in RePark's general `=`, with the cells above as pins. The nested-key normalisation already exists in `key_by_position.rs` and can be reused for the scalar path.

**Retires:** the card when the scalar cells and the four `=` and lookup probes answer Spark's values, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clause C-046; the struct and array keys are C-041.

Up: [map.md](map.md).
