# Card F-IDLESS-TOPLEVEL-POSITIONAL-1: on the no-mapping fallback route, a top-level column of an id-less Parquet file reads a neighbour's values by position (fork card)

**Date:** 2026-11-01. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-64 records round, from the fork ledger of F-IDLESS-STRUCT-CHILD-REMAP-1 (clauses S-20 and S-26).

**Status:** filed; grade B, P3; not dispatched. This is the owner's fork work; the fork is not edited from RePark.

**Why:** On a Parquet data file without embedded field ids, when the table carries no `schema.name-mapping.default` (the fallback-id route), the top-level rule (match by name and position, else `position + 1`) reads a neighbour's values by position. Measured on the fork, pre-existing at `14a93602` and before:

- table `(x, a, b)`, id-less file `(a, b)` with `a = [1, 2]` and `b = [40, 30]`: reads `x = [1, 2]`, `a = [40, 30]`, `b = NULL`;
- a top-level reorder fails with a `Struct -> Int32` cast error (loud, not silent);
- under a mapping that names only an unrelated column, file `(a, c)` reads `a = 1` and `c = 2` (fork verifier round 3, V3-F2);
- a child dropped and then re-added by name reads its old value (`drop s.b`, then `add s.b`, reads `{a: 1, b: 2}`; top-level `drop c`, then `add c`, reads `c = 5`), because a by-name rule without schema history cannot tell a re-added field from the original (fork S-26). On the mapping route the same shapes read NULL, correctly.

**Reachability:** only through a table registered from foreign metadata with no name mapping. Spark's and RePark's `add_files` both install `schema.name-mapping.default` from the table schema, so no RePark door reaches it today. The RP-64 pins do not run this route.

**Site:** the fork's no-mapping fallback route for id-less files, the top-level name-and-position rule with its `position + 1` fallback. The fork ledger names the functions.

**Fix (proposed):** on the no-mapping route a top-level column is read by name only, never by `position + 1`; a top-level reorder is refused loudly or read by name; a re-added field reads NULL or refuses, and never the values of the field it replaces.

**Measured:** fork ledger S-20 and S-26 (the probes of the fork's round-3 verifier, V3-F2, and the fork's own builder), all at `14a93602` or earlier.

**Retires:** the card when the no-mapping route reads a top-level column by name only (never `position + 1`), refuses a top-level reorder loudly, and a re-added field reads NULL or refuses, with fork S-20 and S-26 closing by citation.

**Relation:** [F-IDLESS-STRUCT-CHILD-REMAP-1](f-idless-struct-child-remap-1-card-2026-10-31.md) (closed 2026-11-01 by fork #379 and RP-64; this is its fallback-route residue); [F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1](f-nested-type-mismatch-embedded-ids-1-card-2026-11-01.md) (the other carded residue); RP-64 (the records are in its unit ledger, clause C-017).

Up: [map.md](map.md).
