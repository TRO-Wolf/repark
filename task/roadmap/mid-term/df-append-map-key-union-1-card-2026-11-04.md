# Card DF-APPEND-MAP-KEY-UNION-1: a DataFrame append whose only change is inside a map key never reaches the schema-evolution union (scope decision)

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L1 (clause C-009) of [MAP-KEY-ALTER-1](../../ledgers/completed/map-key-alter-1-ledger.md): the builder's question 1 and the Opus verifier's findings C6 and C7, re-checked in round two.

**Status:** filed, not dispatched. Verifier class S3. The owner decides the scope.

**Why:** The append lowering in the writer schema turns a DataFrame append into `INSERT ... BY NAME` only when the frame has an extra top-level column. A frame that differs only inside the map-key column (a wider key field, or a new field under the key) is written as an explicit-column `INSERT` that never reaches `union_map_key_refusal`. Measured on the facade: a wider key field (`BIGINT` for `INT`) appends with a down-cast and no schema change, where Java's union refuses `Cannot alter map keys`; a new field under the key, through `writeTo` or `saveAsTable`, is refused by the planner with a cast error rather than Spark's text. No value is stored wrongly: an out-of-range value raises a cast error.

**Decision for the owner:** either (a) lower every merge-schema DataFrame append to `INSERT ... BY NAME` whenever the table accepts any schema, which changes every such append, value-side nested evolution included; or (b) keep the explicit-column path and record its cast and planner texts as an exclusion of the v2 label. The unit did not make either change.

**Retires:** the owner's choice, dated on this card; then a pin for the chosen route.

**Relation:** [MAP-KEY-ALTER-1](map-key-alter-1-card-2026-11-04.md); the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md) D1 and D16.

Up: [map.md](map.md).
