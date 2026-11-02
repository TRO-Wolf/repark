# Card STRUCT-KEY-CAST-OVERFLOW-TEXT-1: an overflowing or invalid cast inside a struct key refuses with DataFusion's text, where Spark gives `CAST_OVERFLOW` or `UNEXPECTED_INPUT_TYPE`

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the round-1 Opus 5.5 verifier's finding F8 on MAP-STRUCT-KEY-SUBSCRIPT-1 (S3: both engines refuse, and only the text and the condition differ).

**Status:** filed; grade C, P3; not dispatched.

**Why:** Both engines refuse the same statements, so no row is wrong. For `m[named_struct('k', 5000000001, 'j', 2)]` on an `INT` key field, Spark 4.1.2 raises `CAST_OVERFLOW` (SQLSTATE 22003); RePark raised DataFusion's optimizer text (`Optimizer rule 'simplify_expressions' failed`, `Can't cast value 5000000001 to type Int32`). For `m[named_struct('k', 'x', 'j', 2)]`, Spark raises `UNEXPECTED_INPUT_TYPE`; the round-1 verifier saw an Arrow cast error before fix round 1. The round-1 leaf rule now refuses a STRING leaf against an `INT` field with `UNEXPECTED_INPUT_TYPE`, but that is not measured on the round-1 head, and the overflow form was measured only before the round.

**Site:** not located.

**Fix (proposed):** refuse an overflowing literal leaf with Spark's `CAST_OVERFLOW` condition (SQLSTATE 22003), and an invalid leaf with `UNEXPECTED_INPUT_TYPE`, naming the types; pin both on the Spark door.

**Measured (Spark 4.1.2, round-1 verifier):** `SELECT id, m[named_struct('k', 5000000001, 'j', 2)] FROM c ORDER BY id` gives `[CAST_OVERFLOW] The value 5000000001L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. SQLSTATE: 22003`. `SELECT id, m[named_struct('k', 'x', 'j', 2)] FROM c ORDER BY id` gives `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "m[named_struct(k, x, j, 2)]"`, with the required type `"STRUCT<k: INT, j: INT>"`.

**Retires:** the card when both forms refuse with Spark's condition, pinned on the Spark door.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (round-1 residue F8; its C-015 and C-028 keep the refusal class).

Up: [map.md](map.md).
