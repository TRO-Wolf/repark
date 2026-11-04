# Card AVRO-MAP-ENTRY-ORDER-1: RePark reads Spark-written Avro map entries in another order, and the order changes between runs

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L6 (clause C-012) of [SUBSCRIPT-DOT-ACCESS-1](../../ledgers/completed/subscript-dot-access-1-ledger.md), the Opus verifier's finding F6.

**Status:** filed, not dispatched. Verifier class S3.

**Why:** On the Spark-written Avro fixtures (`map_avro_*`), `map_keys(m)` reads [k2, k1] where Spark writes [k1, k2]. The order changes from run to run on the same files, so the six cells of `SELECT id, element_at(m, 'k2').b, map_keys(m) FROM v` flip between runs. The element values agree with Spark. The replay skips these Avro rows, so the pin is not flaky. The split in PR #1259's body (152 value-equal with other names and 7 other) is one cell off the verifier's rerun (153 and 6); the PR body's figures should not be quoted as exact.

**Fix shape:** find whether the order comes from the Avro reader's decoding of the map array or from an unordered collection; then pin one Spark-written fixture's `map_keys` order, and the rows that depend on it.

**Retires:** when `map_keys` on a Spark-written Avro map answers Spark's order on every run, pinned.

**Relation:** [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md) (PR #1259; ledger clauses C-003 and C-012); the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md) D2 and D14.

Up: [map.md](map.md).
