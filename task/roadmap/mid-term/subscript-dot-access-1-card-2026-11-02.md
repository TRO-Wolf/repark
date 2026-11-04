# Card SUBSCRIPT-DOT-ACCESS-1: a field access after a map subscript, or after `element_at`, fails to parse where Spark answers

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the round-1 Opus 5.5 verifier's finding F5 on MAP-STRUCT-KEY-SUBSCRIPT-1 (S2, pre-existing: head equals base).

**Status:** closed 2026-11-04 at `dd0d1c8e` (PR #1259, squashed to main as `3c9f300c`) after the two verifiers. Dated correction, 2026-11-04: the gap was in the lowering, not the parser (sqlparser 0.62 already parses the chain). The site is `crates/repark-core/src/column_resolution/dereference.rs`, called from the Spark door's `plan_statement_with_column_repair`. Ledger: [subscript-dot-access-1-ledger.md](../../ledgers/completed/subscript-dot-access-1-ledger.md) (clauses C-001 to C-006 PROVEN). The leads: the native door, card [SUBSCRIPT-DOT-ACCESS-NATIVE-1](subscript-dot-access-native-1-card-2026-11-04.md); the array, struct and lambda forms, card [DEREFERENCE-FORMS-2](dereference-forms-2-card-2026-11-04.md); the rest are in the ledger. Grade C, P3 as filed.

**Why:** A dot access after a subscript or an `element_at` call fails in the parser. `m[named_struct('k', 1, 'j', 2)].x` answers `This feature is not implemented: Dot access not supported for non-string expr: Identifier(x)`, where Spark 4.1.2 answers the field. The same text comes back for `.arr[0]`, for `(m[...]).x`, for `element_at(m, ...).x` and for the scalar-key `map('a', named_struct('x', 1))['a'].x`. It is a parser gap that does not depend on the key type, so MAP-STRUCT-KEY-SUBSCRIPT-1 did not cause it and its fix does not reach it.

**Site:** not located; the first unit locates it in the parser's dot-access handling.

**Fix (proposed):** accept a field access, and an array subscript after it, on the result of a map subscript or of an `element_at` call, as Spark does; pin the Spark answers on the Spark door.

**Measured (Spark 4.1.2, round-1 verifier):** `SELECT id, m[named_struct('k', 1, 'j', 2)].x FROM sv ORDER BY id` gives `[[1, 5], [2, None]]`; `SELECT id, m[named_struct('k', 1, 'j', 2)].arr[0] FROM sv ORDER BY id` gives `[[1, 10], [2, None]]`; `SELECT map('a', named_struct('x', 1))['a'].x` gives `[[1]]`; `SELECT id, element_at(m, named_struct('k', 1, 'j', 2)).x FROM sv ORDER BY id` gives `[[1, 5], [2, None]]`, and `(m[named_struct('k', 1, 'j', 2)]).x` gives the same. RePark: the error text above, on head and base.

**Retires:** the card when these forms answer Spark's values on the Spark door, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (round-1 residue F5; the subscript rule does not reach the parser).

Up: [map.md](map.md).
