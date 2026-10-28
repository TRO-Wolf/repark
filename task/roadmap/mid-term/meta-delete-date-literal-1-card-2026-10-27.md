# META-DELETE-DATE-LITERAL-1: a whole-partition DATE delete takes the metadata door (P3 card)

**Filed:** 2026-10-27. **Filed by:** the RP-60 records clerk (`claude-haiku-5-5`), from the RP-60 builder's measurement at `a161b999`. **Status:** filed, not dispatched. The owner charters it.

**Grade:** clerk-grade fix, once charted. Nothing changes in the Python facade.

**Severity:** P3. The result is correct today. Only the mechanism differs: a DELETE that covers whole month partitions writes a row-level position-delete file, where Spark's `canDeleteWhere` decides it through the metadata door and removes the data file.

**Why:** RePark's metadata-delete door (`try_metadata_delete` in `crates/repark-iceberg/src/write/meta_delete.rs`) asks the fork's `Table::can_delete_using_metadata` only for a `WHERE` that translates exactly to an Iceberg predicate. The translation runs through `literal_datum` in `crates/repark-iceberg/src/write/conflict_filter.rs`, which turns int, long, float, double, string and bool literals into datums and never a DATE literal. So a predicate on a DATE column declines the door even when its range covers whole partitions of a `months()` table.

**Measured (RP-60 builder, 2026-10-27):** on a format-v2 merge-on-read table partitioned by `months(d)`, the whole-month statement `WHERE d >= DATE '2026-04-01'` took the row-level route and wrote one position-delete file of 20 rows. The partition-value spelling `WHERE m = 4` on an identity column of the same kind of table does take the door (pinned by `whole_partition_delete_still_takes_the_metadata_door` in `crates/repark-spark/tests/posdel_batch_door.rs`).

**Fix as specified:** extend `literal_datum` to date, timestamp and decimal literals, so a whole-partition DATE predicate translates exactly and `can_delete_using_metadata` decides it. The translation stays exact: a literal that cannot be represented exactly still declines and keeps the row-level route.

**Pin:** a whole-month DATE delete on a `months(d)` table leaves `total-delete-files` at 0 and removes the data file.

**Out of scope:** the row-level merge-on-read writer and its delete-file bookkeeping (registry row ICE-META-DELETE-1-D1 in [the registry](../../../docs/spark-sql-iceberg-parity.md)), the copy-on-write delete path, and the Python facade.

**Dispatch:** none. The owner charters it or declines it. It is linked from [the RP-60 card](rp-60-posdel-repin-1-card-2026-10-27.md).

Up: [map.md](map.md).
