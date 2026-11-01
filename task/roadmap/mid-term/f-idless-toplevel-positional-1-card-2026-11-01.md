# Card F-IDLESS-TOPLEVEL-POSITIONAL-1: on the no-mapping fallback route, an id-less Parquet top-level column reads a neighbour's values by position or a dropped field's values, reachable through RePark's own name-mapping property (fork card)

**Date:** 2026-11-01. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-64 records round, from the fork ledger of F-IDLESS-STRUCT-CHILD-REMAP-1 (clauses S-20 and S-26). **Rewritten 2026-11-01 (RP-64 fix round 1, verifier F2):** the first filing said no RePark door reaches the route, which was false; the reachability, the diverging shapes and the grade (B, P2, was B, P3) follow the measured answers.

**Status:** filed; grade B, P2; not dispatched. This is the owner's fork work; the fork is not edited from RePark.

**Why:** On a Parquet data file without embedded field ids, when the table carries no `schema.name-mapping.default` (the fallback-id route), the top-level rule (match by name and position, else `position + 1`) reads by position. Measured on the fork at `14a93602` and before, and through RePark's Spark door on both pins, 2026-11-01:

- table `(x, a, b)`, id-less file `(a, b)` with `a = [1, 2]` and `b = [40, 30]`: reads `x = [1, 2]`, `a = [40, 30]`, `b = NULL`. Spark 4.1.2 gives the same positional answer (Java's fallback assigns ids by position), so this shape is Spark-equal and is recorded, not a divergence.
- table `(a, b)` over a file `(b, a)` of the same types: the values swap on both engines (Spark-equal, Java's rule). A top-level reorder that changes a type fails with a `Struct -> Int32` cast error on RePark (loud).
- a struct child or a top-level column dropped and then re-added by the same name: RePark reads the dropped field's old values (`drop s.b`, then `add s.b`, reads `{a: 1, b: 2}`; `drop c`, then `add c`, reads `c = 5`), where Spark reads NULL (`(1, None, None)`). A by-name rule without schema history cannot tell a re-added field from the original (fork S-26). This diverges from Spark.
- a `SET TBLPROPERTIES` of a mapping that names only `zz`, over the file `(a, c)`: RePark reads `(1, 2)`, where Spark reads `(None, None)`. This diverges from Spark.

**Reachability:** through RePark's own door. `ALTER TABLE t UNSET TBLPROPERTIES ('schema.name-mapping.default')` is accepted on the Spark door after `add_files` and leaves the table with no mapping; a `SET TBLPROPERTIES` of a mapping that names other columns gives the partial-mapping shape. RePark does not refuse the UNSET, because Spark accepts it. The route is also reached by a table registered from foreign metadata with no mapping. `add_files` itself installs a mapping, so the id-less file admitted by it does not take this route. The RP-64 pins do not run this route.

**Site:** the fork's no-mapping fallback route for id-less files, the top-level name-and-position rule with its `position + 1` fallback. The fork ledger names the functions.

**Fix (proposed):** on the no-mapping route, a re-added field reads NULL or refuses, and never the values of the field it replaces, and a partial mapping reads NULL for every field the file does not name, as Spark does. The positional read and the same-type reorder follow Java's rule and are Spark-equal; they are recorded here and not proposed for change.

**Measured:** fork ledger S-20 and S-26 (the probes of the fork's round-3 verifier, V3-F2, and the fork's own builder), at `14a93602` or earlier; and the RP-64 Opus 5.5 verifier's F2 oracle of 2026-11-01 (its shapes f1, f3, f4 and f5, on both pins, with Spark's answers).

**Retires:** the card when a re-added field and a partial mapping read NULL for every field the file does not name, as Spark does, with fork S-26 and the partial-mapping shape closing by citation. The positional read and the same-type reorder stay recorded as Java's rule.

**Relation:** [F-IDLESS-STRUCT-CHILD-REMAP-1](f-idless-struct-child-remap-1-card-2026-10-31.md) (closed 2026-11-01 by fork #379, fork #380 and RP-64; this is its fallback-route residue); [F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1](f-nested-type-mismatch-embedded-ids-1-card-2026-11-01.md) (the other carded residue); RP-64 (the records are in its unit ledger, clause C-017).

Up: [map.md](map.md).
