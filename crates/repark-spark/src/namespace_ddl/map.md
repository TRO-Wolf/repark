# map — repark-spark/src/namespace_ddl

## Purpose

The table-lifecycle work `../namespace_ddl.rs` delegates rather than inlines.

## Contents

- `purge.rs` — **IPI-21 (2026-09-20):** the reachable-file sweep behind `DROP TABLE … PURGE`.
  The fork's `Catalog::drop_table` has no purge parameter and the memory catalog deletes only the
  `metadata.json`, so the data files must be swept by the fork's `DeleteReachableFiles` action
  **before** the drop — after the drop the metadata location is no longer resolvable.
  `plan_purge` is the gate and the seam: it maps a missing table to Spark's
  `[TABLE_OR_VIEW_NOT_FOUND]`, refuses when `gc.enabled` is `false` with Java
  `SparkCatalog.purgeTable`'s text, and hands back the metadata location and `FileIO` so a test can
  drive the action itself. `purge_table_files` runs the sweep and **returns** the
  `DeleteReachableFilesResult`: per-file delete failures are collected by the fork rather than
  raised, Java suppresses them too, and this crate has no logging facade to route them to — so the
  result is returned rather than discarded and the pin proves the `DROP` still completes.
  The `gc.enabled` read follows Java `PropertyUtil.propertyAsBoolean`: only the literal `true`
  reads as true, and an absent property defaults to `true`
  (`TableProperties::PROPERTY_GC_ENABLED_DEFAULT`). `GC_DISABLED_REFUSAL` is the refusal text,
  shared with the tests so the pin cannot drift from the message. The gate is measured Spark
  parity, not an addition: cell `TP-GC-DISABLED-PURGE` records live Spark refusing the same
  statement, and Java's check sits in `SparkCatalog.purgeTable`, upstream of the action.
  pins: ipi-21-25-42-small-parser/C-008, C-009, C-010

## Pointers

- Up: [../map.md](../map.md). Caller: [../namespace_ddl.rs](../namespace_ddl.rs) `execute_drop_table`.
- Pins: [../tests/purge.rs](../tests/purge.rs),
  [../../../../python/repark/tests/test_ice_small_parser_1.py](../../../../python/repark/tests/test_ice_small_parser_1.py).

## Debug

| Symptom | First check |
|---|---|
| `DROP TABLE … PURGE` left the data files behind | the `purge` flag did not reach `execute_drop_table` — `../router.rs` reads `Statement::Drop.purge` |
| A plain `DROP TABLE` deleted data files | the sweep is gated on the flag alone; `tests/purge.rs` pins both arms of that branch |
| The sweep found nothing | it must run **before** `Catalog::drop_table`; after the drop the metadata location is gone |
| A `gc.enabled=false` table purged anyway | `gc_enabled` reads the table property, not the session; Spark refuses in `SparkCatalog.purgeTable` before the action runs |

First checks: `cargo test -p repark-spark purge`. Escalate to: [../map.md#debug](../map.md).
