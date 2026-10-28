# Card WAP-BRANCH-TARGET-SILENT-1: under a session WAP branch, a case-varied DELETE on a Parquet table is a silent no-op

**Date:** 2026-10-29. **Filed by:** the ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the verifier's round 2 pre-existing observations (Opus 5.5 verifier at `b9d83f38`). Filed on the ICE-DELETE-FORMAT-GUARD-1 branch.

**Status:** open. Priority P2. Not dispatched. Measured only by the verifier's probe (copied, not re-run by the clerk). Pre-existing on main: the same result on `main`.

**Why:** with `spark.wap.branch` set, a case-varied `DELETE FROM ICE.NS.T` on a Parquet WAP-enabled table returns count 0 and leaves the branch unchanged. The WAP branch target loader (`wap_branch_target` in `crates/repark-spark/src/write_to_branch.rs`) loads the table name as written, the lookup misses, and the statement runs against main. The caller sees a successful zero-row delete and nothing changes on the branch. The delete-format guard of ICE-DELETE-FORMAT-GUARD-1 does not cause this (the table is Parquet), and the same as-written name class is carded for the other Spark-door paths in SPARK-DOOR-TARGET-NORMALISATION-1.

**Fix (proposed):** resolve the name through `resolve_dml_target` (the planner's way, as the delete-format guard does) before the WAP branch target is loaded. Where the loaded name still misses, refuse rather than run against main. Each path gets a red-first pin on a Parquet WAP table: the statement either changes the branch as the spelled table would, or refuses, and main is unchanged in both cases.

**Retires:** the card when the case-varied DELETE under a session WAP branch has a red run on the current head and a green run after the fix.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (the resolver it reuses, C-012); [SPARK-DOOR-TARGET-NORMALISATION-1 card](spark-door-target-normalisation-1-card-2026-10-29.md) (the same as-written name class on the other Spark-door paths); [registry row ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md).
