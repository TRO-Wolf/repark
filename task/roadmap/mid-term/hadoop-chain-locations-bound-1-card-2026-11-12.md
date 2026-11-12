# Card HADOOP-CHAIN-LOCATIONS-BOUND-1: a Hadoop drop evicts `v1..=version` under the catalog lock with no bound

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** LOCAL-DROP-RECREATE-STALE-WRITE-1
verifier 1, 2026-11-12, fork PR #382 (F2).

**Status:** filed 2026-11-12, not dispatched. Severity **S4**. Introduced by fork PR #382. RePark cannot reach it today.

**What:** `chain_locations` (`crates/iceberg/src/catalog/memory/metadata_naming.rs`, lines 99–109, fork #382 head
`79b6f19b`) loops `1..=version` over the parsed `i32` version, and `drop_table` runs it while it holds the catalog lock
(`catalog.rs`, lines 454–459).

**Effect:** a Hadoop table registered through the fork's `register_table` API from a file named
`v2147483647.metadata.json` would make about 2^31 cache invalidations under the lock. The measured eviction cost is
about 1 to 5 µs per key, so that is tens of minutes or more with the lock held.

**Measured cost at realistic sizes (both verifiers):** a `DROP TABLE` at 1,000 versions took 33 to 36 ms and at 10,000
about 340 to 380 ms on head, within noise of base; the eviction alone was 4.9 ms at 1,000 keys and 484 ms at 100,000.
The deletion of the N metadata files, which the drop already did before #382, dominates.

**Reach:** not from RePark, which has no catalog `register_table` door. Hadoop names written by the catalog start at
`v1`, so only a registered file can carry a huge version.

**Fix direction:** none in the source.

**Close:** the walk is bounded or the case refused, pinned, or the card is declined by a recorded ruling.

Up: [map.md](map.md).
