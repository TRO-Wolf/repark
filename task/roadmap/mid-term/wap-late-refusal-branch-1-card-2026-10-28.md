# Card WAP-LATE-REFUSAL-BRANCH-1: a refusal raised after the WAP branch redirect still creates the branch and writes metadata files

**Date:** 2026-10-28. **Filed by:** the ICE-POST-PUBLISH-OUTCOME-1 and ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the Opus 5.5 verifier's round 4 finding R4-V1 (S3). Filed on the `fix/ice-post-publish-outcome-1` branch; not in that unit pair's scope.

**Status:** open. Priority P3. Not dispatched. Measured only by the round-4 verifier's probe (copied, not re-run by the clerk). Pre-existing on main: the same result on `main`.

**Why:** under a session WAP branch (`spark.wap.branch`), the guard's refusals (the delete-format check, the BUG-001 valve and the encryption check) run before the branch is created, so they leave no branch and no file (ICE-DELETE-FORMAT-GUARD-1, C-020). A refusal that a later check raises on the redirected statement does not: the branch is created and one or two metadata files are written before the statement fails. Four shapes measured: a subquery refusal, an unknown-catalog refusal, the compound-identifier error under a session write conf, and a failed `REPLACE WHERE`. The result is the same on main.

**Fix (proposed):** either the later checks run before `create_wap_branch_from_main`, or the statement removes the branch and the metadata files it wrote when it refuses. Red-first pin per shape: under a session WAP branch with the branch absent, the refused statement leaves no branch and the metadata file count unchanged.

**Retires:** the card when each of the four shapes has a red run on the current head that shows the branch or the file, and a green run that shows neither.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (C-020, the clause this card scopes); [registry row ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md); sibling [WAP-BRANCH-TARGET-SILENT-1 card](wap-branch-target-silent-1-card-2026-10-28.md) (the other WAP branch gap on this route).

Up: [map.md](map.md).
