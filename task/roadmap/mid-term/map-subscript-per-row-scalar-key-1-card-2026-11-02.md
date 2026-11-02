# Card MAP-SUBSCRIPT-PER-ROW-SCALAR-KEY-1: a subscript whose scalar key varies per row refuses loudly, where Spark answers NULL per row

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 hand-back (the residue R3 of its records).

**Status:** filed; grade C, P3; not dispatched.

**Why:** On a map `MAP<STRING, INT>` (a table of two rows, with keys `a` and `b` on row 1 and `c` on row 2), the subscript `m[CAST(id AS STRING)]` is a per-row key. Spark 4.1.2 answers `[[1, None], [2, None]]`. RePark refuses with `get_field function requires all field_name arguments to be scalars`, a loud refusal and not a wrong value, and the same text is what a per-row struct key showed at the base before MAP-STRUCT-KEY-SUBSCRIPT-1. Scalar-key subscripts stay on DataFusion's `get_field` path by design of that unit; this shape is the one they still refuse.

**Site:** the scalar-key path of the subscript, which DataFusion's `get_field` serves. MAP-STRUCT-KEY-SUBSCRIPT-1's rule `SparkMapSubscript` (`crates/repark-functions/src/collection/map_subscript.rs`) handles nested keys only.

**Fix (proposed):** route a per-row scalar key through the same `element_at` kernel the nested keys use, so the subscript answers each row's lookup as Spark does; pin the Spark answer for a per-row key.

**Measured:** Spark 4.1.2 (the MAP-STRUCT-KEY-SUBSCRIPT-1 builder, measured once): `SELECT id, m[CAST(id AS STRING)] FROM e ORDER BY id` gives `[[1, None], [2, None]]`. RePark (the builder's measurement; the unit leaves scalar keys unchanged, so the same answer holds at the base `aa52bd5f`): refuses with `get_field function requires all field_name arguments to be scalars`.

**Retires:** the card when a per-row scalar key answers Spark's per-row values, pinned on both the Spark door and the ANSI door.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (residue R3; its clause C-012 pins the per-row struct key, which the rule now answers).

Up: [map.md](map.md).
