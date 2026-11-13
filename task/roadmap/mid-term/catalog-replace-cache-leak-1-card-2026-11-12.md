# Card CATALOG-REPLACE-CACHE-LEAK-1: the fork's Glue and S3 Tables replace publishes leave the replaced metadata location in the cache

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), from Fable's reading of 2026-11-12 and the owner's default of the same day ("card the leak, keep it out of the fix").

**Status:** filed 2026-11-12, not dispatched. Severity **S4**. Kept out of LOCAL-DROP-RECREATE-STALE-WRITE-1, the Hadoop fix that rides v1.5.7.

**What the fork does (read at fork rev `66901f02`, paths relative to the fork's root):**

- **Glue.** `publish_replace_table` (`crates/catalog/glue/src/catalog.rs`, lines 976–982) delegates to `publish` in `crates/catalog/glue/src/catalog/replace_publish.rs`. That function puts the new metadata location into the metadata cache (`cache_put`, lines 100–106) and never invalidates the base location it replaced.
- **S3 Tables.** `publish_replace_table` (`crates/catalog/s3tables/src/catalog.rs`, lines 900–939) puts the new location (`cache_put`, line 935) and never invalidates the base location it replaced.
- **For comparison:** the memory catalog's `update_table` invalidates the location it replaced before it caches the new one. In both AWS catalogs the only `invalidate` calls are in `drop_table` and `invalidate_table` (Glue `catalog.rs` lines 720 and 730; S3 Tables `catalog.rs` lines 798 and 808).

**What the clerk confirmed:** Fable's S3 Tables reading holds as stated. The Glue reading needed one correction: the replace publish does make a cache call, a put of the new location through `replace_publish.rs`. What it lacks is the same as on S3 Tables, the invalidate of the base. A second observation, not part of Fable's reading: on both catalogs `update_table` also caches the new location without invalidating the previous one (Glue `catalog.rs` line 970, S3 Tables `catalog.rs` line 885). Whether that belongs in this card is for whoever charters it.

**Why it is a leak and not a wrong read:** Glue and S3 Tables put a UUID in every metadata file name, so a replaced location is never written again and a stale entry is never hit by a later load. The entry only takes space until the cache's byte cap evicts it. The Hadoop case is different because its names (`v1`, `v2`, …) come back after a drop; that is LOCAL-DROP-RECREATE-STALE-WRITE-1.

**Close:** each replace publish invalidates the base location it replaced, with a cache test per catalog, or the owner declines the card.
