# Card NATIVE-DOOR-POST-PUBLISH-1: the native door's post-commit name refresh reports a committed change as failed

**Date:** 2026-10-28. **Filed by:** the ICE-POST-PUBLISH-OUTCOME-1 clerk round (`claude-haiku-5-5`), from the coordinator's ruling Q1 on the builder's round-2 hand-back (the native door is a follow-up unit, not part of AUD-002).

**Status:** open. Grade C (clerk). Not dispatched; no build is owed until the owner charters it.

**Why:** ICE-POST-PUBLISH-OUTCOME-1 (ledger [ice-post-publish-outcome-1](../../ledgers/staging/ice-post-publish-outcome-1-ledger.md), AUD-002) makes a Spark-door statement report the commit's outcome when the session's name refresh fails after a committed publish. The native door (`repark.sql()`, the ANSI door, `crates/repark-sql`) has the same defect class and is not covered. Its post-commit sites call `invalidate_catalog_namespaces` directly, so a refresh failure is returned as the statement's error while the change is committed, and the committed table stays invisible to the next statement until the frozen name directory is rebuilt.

**Sites** (the builder's list, read from the code, not measured): `create_table.rs` `finish` (native CTAS and RTAS, about line 654), `insert_overwrite.rs:115`, `session_insert.rs:70`, `truncate.rs:59`, `schema_ddl.rs:45` and `:117`, `alter.rs:133`, `v3/cow.rs:249`, `:451` and `:732`, `v3/partitioned_equality_deletes.rs:235` and `:496`, and `v3/branch_tag_time_travel.rs:223`. Thirteen sites in all.

**Fix:** switch each site to `repark_iceberg::catalog::invalidate_catalog_namespaces_after_commit`, which the Spark door now uses in `crates/repark-spark/src/catalog_ops.rs`. A pin per site family, shaped like `crates/repark-spark/tests/post_publish_outcome.rs` (a fault catalog armed after the publish, the committed reading, and the session listing after recovery).

**Measured:** none on the native door. The sites are read in the code only; no fault has been injected into the native door's refresh.

**Retires:** the card when every site calls the after-commit variant and each family has its pin. The registry row ICE-POST-PUBLISH-OUTCOME-1 then names the native door as covered.

**Relation:** [ICE-POST-PUBLISH-OUTCOME-1 ledger](../../ledgers/staging/ice-post-publish-outcome-1-ledger.md) (C-007 is this card's open clause); [NATIVE-DOOR-DELETE-FORMAT-1 card](native-door-delete-format-1-card-2026-10-28.md) (the other native-door gap from the same unit, on the delete format rather than the refresh).
