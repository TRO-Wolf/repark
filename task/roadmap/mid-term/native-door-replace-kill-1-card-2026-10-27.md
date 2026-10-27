# Card NATIVE-DOOR-REPLACE-KILL-1: the native door's replace has no kill-mid-write cell

**Date:** 2026-10-27. **Filed by:** the ICE-REPLACE-KILL-DOORS-1 clerk round (`claude-haiku-5-5`), from the order's step-0 outcome and ruling RKD-1 ([order](../../wo/ice-replace-kill-doors-1/order.md)).

**Status:** open. Grade C (clerk). Not scheduled; no build is owed until the owner charters it.

**Why:** the native door (`repark.sql()`, the ANSI door) runs `CREATE OR REPLACE TABLE … AS SELECT` and `REPLACE TABLE … AS SELECT` through `crates/repark-sql/src/create_table.rs`. That file calls `StagedTableTransaction::begin_replace` at about line 247 and `publish_replace_table` at about line 332. The Spark door's replace is pinned by ICE-REPLACE-KILL-DOORS-1 (K-8, K-9 and K-10). No kill-mid-write cell holds the native door's publish, so a writer killed there is unpinned.

**Measured:** none yet. The native door's replace is read in the code only. No kill cell has run against it.

**Retires:** a kill-mid-write cell that holds the native door's replace publish on format v2 and v3, and asserts the post-kill reading that RKD-1 (a) fixes for the Spark door. The registry row ICE-LOCAL-COMMIT-POINT-1 then names the cell.

**Relation:** the unit ledger [ICE-REPLACE-KILL-DOORS-1](../../ledgers/staging/ice-replace-kill-doors-1-ledger.md) (its C-012 cites this card) and [ICE-INTEGRITY-PINS-1 C-024](../../ledgers/staging/ice-integrity-pins-1-ledger.md), which named the replace doors unpinned. C-024's native-door half stays open until this card retires. Registry row: [ICE-LOCAL-COMMIT-POINT-1](../../../docs/spark-sql-iceberg-parity.md).
