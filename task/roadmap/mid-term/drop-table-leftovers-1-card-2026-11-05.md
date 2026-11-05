# Card DROP-TABLE-LEFTOVERS-1: a DROP TABLE leaves earlier metadata or data files, so a reopen can bring the table back

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the commit corpus.

**Status:** filed 2026-11-05, grade S3, not dispatched. Owner ruling 2026-11-04: one card; no fix before the label. A later fix deletes user data on disk and needs its own owner word.

**Why:** The commit corpus's D3 group ([commit-rule-2026-11-04.md](../../../docs/corpus/commit-rule-2026-11-04.md)). After a successful DROP TABLE on the memory catalog, the earlier metadata files stay, and the reopen brings the table back at its first, empty version. On the hadoop catalog every metadata version is removed but the data files stay, and an extra `v<N+1>` file placed beside the table brings it back. Java's `HadoopCatalog.dropTable` deletes the directory.

**Cells:** 8, the STATE_DIFFERS rows of [commit-cells-2026-11-04.jsonl](../../../docs/corpus/commit-cells-2026-11-04.jsonl), every one a DROP TABLE: 4 on the Spark-door memory catalog, 1 on the Spark-door hadoop catalog, 2 on the native memory catalog, 1 on the native hadoop catalog.

**Label:** excluded. The format-v2 label section lists the card as an exclusion ([format-v2 label](../../../docs/spark-sql-iceberg-parity.md#exclusions)).

**Fix (not chartered):** a DROP TABLE that removes every metadata file and every data file it owns, pinned on both doors and both catalogs, with an owner word before any delete on disk.
