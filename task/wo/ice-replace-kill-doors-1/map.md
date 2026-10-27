# task/wo/ice-replace-kill-doors-1/ — kill-mid-write pins for the replace doors

ICE-REPLACE-KILL-DOORS-1 (grade B, release 1.5.6, 2026-10-27; closes the local-catalog half of
ICE-INTEGRITY-PINS-1 C-024): K-8 and K-9 pin a writer killed at the publish of `CREATE OR REPLACE
TABLE … AS SELECT` and `REPLACE TABLE … AS SELECT` on the local catalog, on format v2 and v3. The
cells are in `crates/repark-spark/src/tests/kill_mid_write.rs` and `kill_mid_write_hold.rs`; the
unit ledger in `task/ledgers/staging/` records the proof.

- [order.md](order.md) — the order of record: the gap, the rulings, the steps, the gates, the halts, the step-0 outcome and ruling RKD-1, and the hand-back.

Up: [../map.md](../map.md).
