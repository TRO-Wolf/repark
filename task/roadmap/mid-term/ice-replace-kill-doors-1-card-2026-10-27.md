# Card ICE-REPLACE-KILL-DOORS-1: the RTAS and `CREATE OR REPLACE TABLE … AS SELECT` doors have no kill cell

**Date:** 2026-10-27. **Filed by:** the ICE-V2-LABEL-1 clerk (`claude-haiku-5-5`), from the owner's ruling of 2026-10-27 on the label's third hold: ledger [ice-v2-label-1](../../ledgers/staging/ice-v2-label-1-ledger.md) C-024, superseded by C-029.

**Status:** open. Owner ruled 2026-10-27: a card plus a small pin unit. Not scheduled. Until that unit lands, the replace doors are unpinned, and the registry row [ICE-LOCAL-COMMIT-POINT-1](../../../docs/spark-sql-iceberg-parity.md) says so as a declared line on the format-v2 label.

**Why:** the test-only kill decorator forwards `publish_replace_table` without a hold, so a writer killed inside a replace commit is never held. None of the seven kill cells of `kill_mid_write.rs` calls it. The replace doors are therefore unmeasured, although they are write doors of the integrity family.

## What was measured

Ledger C-024 of [ice-integrity-pins-1](../../ledgers/staging/ice-integrity-pins-1-ledger.md) (verifier S3, 2026-10-22) states the claim. Re-read on the label's main (`59d104d9`) on 2026-10-27:

- `publish_replace_table` is forwarded unheld at `crates/repark-spark/src/tests/kill_mid_write_hold.rs` lines 325 to 331.
- The kill cells of `crates/repark-spark/src/tests/kill_mid_write.rs`: K-1 and K-2 (`INSERT`), K-3 (`INSERT OVERWRITE`), K-4 and K-5 (`MERGE`), K-6 (a `DELETE` statement), K-7 (`CREATE TABLE … AS SELECT`), and the controls K-C0 and K-C1 (`INSERT`).
- No cell names `OR REPLACE`, `REPLACE TABLE` or an RTAS. A case-insensitive search of `kill_mid_write.rs` and `kill_mid_write_hold.rs` finds none.

The pinned catalogs are the ones the integrity family already covers: the memory catalog offline, and Glue and S3 Tables live.

**Retires:** when a kill cell for an RTAS and a kill cell for a `CREATE OR REPLACE TABLE … AS SELECT` both hold `publish_replace_table` on the pinned catalogs, and each leaves the table at its last committed snapshot after a kill. The registry line on ICE-LOCAL-COMMIT-POINT-1 is then withdrawn in the same unit.

**Relation:** ice-integrity-pins-1 C-024 (the claim, OPEN, carried to pin U/X or a later card) and the label ledger [ice-v2-label-1](../../ledgers/staging/ice-v2-label-1-ledger.md), C-024 and C-029. The disposition of the label hold is the owner's, recorded by the 2026-10-27 ruling. This card writes no build order.
