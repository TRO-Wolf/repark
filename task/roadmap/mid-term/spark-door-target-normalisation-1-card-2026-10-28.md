# Card SPARK-DOOR-TARGET-NORMALISATION-1: the Spark door's name-based DML and MERGE paths take the target name as written

**Date:** 2026-10-28. **Filed by:** the ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the verifier's round 1 findings and the builder's round-3 notes. Filed on the ICE-DELETE-FORMAT-GUARD-1 branch.

**Status:** open. Priority P3. Not dispatched. Measured only by the builder's round-3 probes (copied, not re-run by the clerk).

**Why:** ICE-DELETE-FORMAT-GUARD-1 (ledger [ice-delete-format-guard-1](../../ledgers/staging/ice-delete-format-guard-1-ledger.md)) resolves the DML target the planner's way for the delete-format guard, the MERGE checks and the encryption guard (`crates/repark-spark/src/dml_target.rs`). Three Spark-door paths still take the name as written, and a planning path under a session write conf refuses a fork-route branch spelling (item 4):

1. The RePark predicate-DML claims in `crates/repark-spark/src/spark_ast.rs` (`try_execute_identity_dml`) and the plain identity claim. `ice.NS.t` and `ice.ns.T` DML fails with `NamespaceNotFound` or `NoSuchTable` on every table, Parquet included, where the planner would resolve the name.
2. MERGE's catalog lookup. `MERGE INTO ICE.NS.T` fails as `unknown catalog ICE`, from `lower` and `catalog_handle` on the name as written. This predates ICE-DELETE-FORMAT-GUARD-1 (measured on the base `321c6095`).
3. The branch loader in `commit_write_on_branch` (`crates/repark-spark/src/write_to_branch.rs`) resolves the target as written, so a case-varied branch DML fails closed instead of running. The pin `case_variant_branch_target_dml_fails_with_the_branch_unchanged` holds that fail-closed behaviour as a control.

4. Under a session write conf (`spark.sql.iceberg.compression-codec`), a fork-route branch DML (`ice.ns.t.branch_b`, a `BETWEEN` DELETE) fails at planning with `Unsupported compound identifier 'ice.ns.t.branch_b'. Expected 1, 2 or 3 parts, got 4`, on Parquet as on Avro; the RePark route commits on the branch. Found by the RP-61 clerk round from the builder's notes (copied, not re-run). The case-variant spellings that also fail on Parquet (items 1 and 2) are differentials under this card, not gaps in the delete-format guard.

**Out of scope:** the native door (`repark-sql`), carded as NATIVE-DOOR-DELETE-FORMAT-1.

**Fix (proposed):** route the Spark door's name-based DML claims, MERGE's catalog lookup and the branch loader through `resolve_dml_target`, so every spelling the planner resolves also runs. Each path gets a red-first pin on a Parquet table. The delete-format refusals that ICE-DELETE-FORMAT-GUARD-1 added are gone (RP-61), so a pin on an inherited Avro or ORC table expects the delete file in the data format. The BUG-001 valve and the encryption check stay as they are.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (C-016); [registry row ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md); [RP-61 revert card](rp-61-delete-format-revert-card-2026-10-28.md).
