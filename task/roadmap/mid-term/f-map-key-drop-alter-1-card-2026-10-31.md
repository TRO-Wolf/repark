# Card F-MAP-KEY-DROP-ALTER-1: the fork accepts a drop of a map key's struct field, where Spark refuses the ALTER (fork card)

**Date:** 2026-10-31. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-63 records round, from the fork ledger of F-STRUCT-EMPTY-CHILD-PANIC-1, clause S-14.

**Status:** filed, not dispatched. Grade C, P3.

**Why:** `ALTER TABLE ... DROP COLUMN m.key.a` on a `map<struct<a: int>, int>` column succeeds in the fork's `update_schema`. Spark 4.1.2 refuses the same ALTER with `Unsupported table change: Cannot alter map keys`. The read after the drop is correct at `14a93602`: the empty-struct keys, the values and the validity are intact, where the base refused the scan. Loud or correct is met on the read; the schema change itself diverges from Spark.

**Site:** the fork's `update_schema`, on the drop of a field inside a map key struct.

**Fix (either):** the fork refuses the map-key drop with Spark's condition, `Cannot alter map keys`; or the owner records the divergence as a kept divergence, dated, and this card closes on that ruling.

**Measured (fork ledger S-14):** on the head, the read after the drop answers the map correctly (keys as empty structs, values `[7, 8]`, validity `[true, false]`); the schema change succeeds on the fork and is refused by Spark.

**Retires:** the card when the fork refuses the map-key drop with Spark's condition and a pin shows the refusal, or when the owner records the kept divergence with its date.

**Relation:** [F-STRUCT-EMPTY-CHILD-PANIC-1](f-struct-empty-child-panic-1-card-2026-10-29.md) (the parent fork card; its clause S-14); the registry line ICE-NESTED-DDL-1 dated 2026-10-31.

Up: [map.md](map.md).
