# Card MEMORY-CATALOG-PUT-AFTER-UNLOCK-1: every memory-catalog write puts its cache entry after it releases the catalog lock

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** LOCAL-DROP-RECREATE-STALE-WRITE-1
verifier 1, 2026-11-12, fork PR #382 (F1), and verifier 2, 2026-11-12, fork PR #382 (F3). Fork PR #382's body says this
follow-up card is filed; this is that card.

**Status:** filed 2026-11-12, not dispatched. Severity **S3**. Pre-existing as a class; #382 adds one more caller of the
same shape.

**Builds:** fork #382 head `79b6f19b` and its base `66901f02`. Line numbers are at `79b6f19b`, paths relative to the
fork's root, under `crates/iceberg/src/catalog/memory/`.

**Where the put follows the unlock:**

- `update_table`: `catalog.rs`, lines 602–616 (the put at line 608);
- `create_table`: `catalog.rs`, line 430;
- `publish_create_hadoop`: `staged_publish.rs`, lines 60–72;
- `publish_replace`, new in #382: `staged_publish.rs`, lines 108–115 (the put at line 113).

**The race (verifier 2's walk):**

1. T1, a replace, publishes `v3` of the old incarnation and releases the lock at `staged_publish.rs` line 108.
2. Before T1 reaches line 113, T2 drops the table and evicts `v1` to `v3` under the lock; `v3` is not cached yet.
3. T3 recreates the table and commits `v2` and `v3`; `update_table` puts the new `v3`.
4. T1 resumes and puts `v3` as the old metadata. The dead entry now sits at the recreated table's current pointer, and
   the next write builds on it.

**Measured:** verifier 1's harness showed that a put landing after a concurrent drop can leave a dead entry, and that
when the recreated table has already reached the same `vN` the entry persists and leads to a wrong write. Its second
case showed the identical race in `update_table`'s unchanged code on base.

**Reach:** only concurrent statements on one table in one session. Verifier 2 judges it practically unreachable: the
window between the unlock and the put has no I/O await point, so it needs OS preemption across five catalog operations
with file I/O. No deterministic harness exists without an injection point between those lines.

**Fix direction (verifier 1):** put the cache entry before releasing the lock, or check the table uuid on cache lookup
(Fable's Q1(c) option).

**Close:** the puts move under the lock or the lookup checks the uuid, pinned, or the card is declined by a recorded
ruling.

**Relation:** [MEMORY-CATALOG-HADOOP-ABA-1](memory-catalog-hadoop-aba-1-card-2026-11-12.md) is the commit-check ABA in
the same code; [CATALOG-REPLACE-CACHE-LEAK-1](catalog-replace-cache-leak-1-card-2026-11-12.md) is the Glue and S3 Tables
cache leak.

Up: [map.md](map.md).
