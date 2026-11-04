# Card AVRO-NULL-COLLECTION-ROUNDTRIP-1: RePark's Avro round trip reads a null map or list back as an empty one

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L5 (clause C-011) of [SUBSCRIPT-DOT-ACCESS-1](../../ledgers/completed/subscript-dot-access-1-ledger.md), the Opus verifier's question 3.

**Status:** filed, not dispatched. Verifier class S2.

**Why:** On RePark-written Avro, a null map or list column reads back as empty: `map_keys` answers `[]`, `size` answers 0, and `IS NULL` keeps no row, where Spark keeps the null. The nested-cells replay skips the Avro rows for this cause (part of the 26 Avro rows it skips), so the Spark-door table does not pin them.

**Fix shape:** find which side loses the null (the writer's encoding, the reader's restore, or the schema's nullability); then pin the null and null-parent rows of the table against Spark's values.

**Retires:** when the null and null-parent Avro rows answer Spark's values on RePark-written fixtures, pinned.

**Relation:** [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md) (ledger clause C-011); the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md) D2 (the Avro rows).

Up: [map.md](map.md).
