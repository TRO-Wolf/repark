# Card MAP-SUBSCRIPT-PER-ROW-SCALAR-KEY-1: a subscript whose scalar key varies per row refuses loudly, where Spark answers the per-row lookup

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 hand-back (the residue R3 of its records).

**Status:** filed; grade C, P3; not dispatched.

**Why:** On a map `MAP<STRING, INT>` with keys `'1'` and `'2'`, the subscript `ms[CAST(id AS STRING)]` is a per-row key, and Spark 4.1.2 answers the per-row lookup: `SELECT id, ms[CAST(id AS STRING)] FROM sc ORDER BY id` gives `[[1, 70], [2, 9]]` (the round-1 verifier). RePark refuses with `get_field function requires all field_name arguments to be scalars`, a loud refusal and not a wrong value; the round-1 verifier measured the same refusal on the head and the base. The round-0 example (keys `a`, `b` and `c`) answered NULL on every row only because no key of the map matched, so the title's "NULL per row" was wrong (F9). Scalar-key subscripts stay on DataFusion's `get_field` path by design of MAP-STRUCT-KEY-SUBSCRIPT-1; this shape is the one they still refuse.

**Site:** the scalar-key path of the subscript, which DataFusion's `get_field` serves. MAP-STRUCT-KEY-SUBSCRIPT-1's rule `SparkMapSubscript` (`crates/repark-functions/src/collection/map_subscript.rs`) handles nested keys only.

**Fix (proposed):** route a per-row scalar key through the same `element_at` kernel the nested keys use, so the subscript answers each row's lookup as Spark does; pin the Spark answer for a per-row key.

**Measured:** Spark 4.1.2 (the MAP-STRUCT-KEY-SUBSCRIPT-1 builder, measured once, on the table of the example): `SELECT id, m[CAST(id AS STRING)] FROM e ORDER BY id` gives `[[1, None], [2, None]]`. Round-1 verifier, Spark 4.1.2 on keys `'1'` and `'2'`: `[[1, 70], [2, 9]]`; RePark refuses the same way on head and base. RePark (the builder's measurement; the unit leaves scalar keys unchanged, so the same answer holds at the base `aa52bd5f`): refuses with `get_field function requires all field_name arguments to be scalars`.

**Retires:** the card when a per-row scalar key answers Spark's per-row values, pinned on both the Spark door and the ANSI door.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (residue R3; its clause C-012 pins the per-row struct key, which the rule now answers).

Up: [map.md](map.md).
