# Card MEMORY-CATALOG-HADOOP-ABA-1: the fork's Hadoop commit check compares a path string, so a commit begun on a dropped table lands on its recreation

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** LOCAL-DROP-RECREATE-STALE-WRITE-1
verifier 2, 2026-11-12, fork PR #382 (findings F1 and F2).

**Status:** filed 2026-11-12, not dispatched. Severity **S2**: a wrong write that resurrects a dropped table's identity.
Pre-existing: base equals head. It needs concurrency, so it is outside LOCAL-DROP-RECREATE-STALE-WRITE-1's sequential
cache claim.

**Builds:** fork #382 head `79b6f19b` and its base `66901f02`; RePark #1313 head `45955e65`. Line numbers are at
`79b6f19b`, paths relative to the fork's root, all under `crates/iceberg/src/catalog/memory/`.

**F1, a staged replace (S2):**

- The Hadoop compare-and-swap in `publish_replace` compares the metadata path as a string (`staged_publish.rs`, lines
  91–92). Hadoop names (`v1`, `v2`, …) come back after a drop, so the same string can name a new incarnation.
- Measured sequence: create, insert (`v2`), begin a staged replace (staged `v3`), drop, recreate, insert (`v2`), then
  commit the staged replace.
- Result: the commit succeeds, the pointer moves to `v3`, and the loaded table carries the dropped table's uuid and
  lineage. The same with the metadata cache on and off, on base and head.
- Spark refuses it: Java's `HadoopTableOperations.commit` refuses a base that is not the refreshed current metadata.

**F2, `update_table` (S3 in the verdict, the same ABA):**

- A property transaction built on the dropped incarnation's `v1` commits onto the recreated table and sets its
  property. The same on base and head.
- The compare is `catalog.rs`, lines 241–260; `update_table` is lines 548–618.

**Reach:** the Python facade releases the GIL around `block_on` (`py.detach`, RePark `session.rs` line 561), so two
threads on one session can drive these catalog calls at once. No sequential statement reaches it.

**Fix direction:** the source gives none beyond Java's behaviour. Make the Hadoop commit refuse a base that is not the
current incarnation, as Java does, in both `publish_replace` and `update_table`, with the two sequences above as pins.

**Close:** both sequences refuse as Spark does, pinned, or the card is declined by a recorded ruling.

**Relation:** [MEMORY-CATALOG-PUT-AFTER-UNLOCK-1](memory-catalog-put-after-unlock-1-card-2026-11-12.md) is the cache-put
race in the same code; the parity entry for LOCAL-DROP-RECREATE-STALE-WRITE-1 is in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

Up: [map.md](map.md).
