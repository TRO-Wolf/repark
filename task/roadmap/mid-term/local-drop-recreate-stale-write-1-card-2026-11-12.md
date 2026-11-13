# Card LOCAL-DROP-RECREATE-STALE-WRITE-1: a Hadoop drop and recreate reads and writes on the dropped table's metadata

**Date:** 2026-11-12. **Filed by:** the second builder (`claude-opus-5-5`), on the owner's ruling of 2026-11-12 ("Go with the defaults").

**Status:** S1, a wrong write. It blocks the full format-v2 label and not the scoped Glue and S3 Tables label (ICE-V2-FLAT-AWS-LABEL-1). Built in the second-builder slot; rides v1.5.7. FIXED by fork PR #382 (branch `fix/memory-catalog-replace-cache-1`, head `79b6f19b`, squash-merged 2026-11-13 as `a8bd3a1e` with the same tree) and the RePark pin of this unit, which pins `a8bd3a1e`.

**Why:** On a `type=hadoop` catalog the session metadata cache is keyed by the metadata file path, and Hadoop naming reuses `v1`, `v2`, … when a table is dropped and recreated. The fork's staged replace (`publish_replace`, the publish of `CREATE OR REPLACE` and RTAS) swapped the pointer without touching the cache: it neither evicted the replaced base nor put the location it wrote. A dropped table's entry at a reused `vN` then served the recreated table. Reads returned the dropped table's rows, and later commits built on that base and persisted it: `v3.metadata.json` carried the dropped table's uuid and snapshot chain, and Spark reading the directory read the dropped rows. Measured on 1.5.6 (fork `66901f02`): `C,I7,R,D,C,R` reads id 7; the six writes after it (INSERT, MERGE insert, MERGE update, UPDATE, DELETE, ALTER ADD COLUMN then INSERT) persist wrong data; replace-only history (`R1,R2,R3,D,R4,R5`) reads R2; with PURGE the stale base fails loudly on a purged manifest list.

**Fix (fork, memory catalog only):**
- `publish_replace` mirrors `update_table`: after the swap it releases the lock, evicts the replaced base when it differs, and puts the published location.
- `drop_table` under Hadoop naming evicts `v1` through the current version, computed from the current location; other naming is unchanged.
- No uuid or stat check on lookups (Fable Q1(c)): a note for the Hadoop discovery work, because once a session can see tables written by another process the path key is unsafe again.

**Not in this card:** the Glue and S3 Tables replace paths, which leave an entry (a leak bounded by the cache's byte cap, never a wrong read, since their file names carry a uuid). That is a separate card.

**Workaround on 1.5.6:** `repark.iceberg.metadataCache=false` on Hadoop catalogs.

**Pins:**
- Fork: `crates/iceberg/src/catalog/memory/hadoop_cache_invariant_tests.rs` (a replace moves the entry on both namings; a Hadoop drop evicts every `vN` and keeps other tables' entries) and `hadoop_cache_differential_tests.rs` (cache on against cache off over the measured rows and 96 seeded sequences of 14 steps, failed commits included).
- RePark: `python/repark/tests/iceberg/test_local_drop_recreate_stale_write_1.py` (the four wrong-read rows, the six wrong-write rows, the replace-only row, the PURGE row and seven controls; each cell checks the session read, a path read through `StaticTable`, and the current file's uuid; a live Spark 4.1.2 cell reads the six wrong-write rows by path).
- Evidence: [task/wo/local-drop-recreate-stale-write-1/](../../wo/local-drop-recreate-stale-write-1/map.md).

**Close:** the fork PR merges, RePark re-pins to its merge commit, and the pins stay green.
