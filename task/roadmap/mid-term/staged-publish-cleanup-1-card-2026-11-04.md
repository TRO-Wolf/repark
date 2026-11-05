# Card STAGED-PUBLISH-CLEANUP-1: a rejected staged publish removes its files, so no reader takes it for a commit

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`), from the unit's ledger.

**Status:** closed 2026-11-04 at `20389129` (PR #1261, squashed to main as `ca06fec6`) after the two verifiers. Ledger: [staged-publish-cleanup-1-ledger.md](../../ledgers/completed/staged-publish-cleanup-1-ledger.md).

**Why:** The commit-outcome rule's D2 group ([commit-rule-2026-11-04.md](../../../docs/corpus/commit-rule-2026-11-04.md)): a staged CTAS, RTAS or CREATE TABLE whose publish call failed left its staged metadata file on disk, and the reopen read that file as the committed table, on both doors and both catalogs (12 cells, FAILURE_REPORTED_COMMITTED_ON_REOPEN).

**Fix:** After a publish error that is not CommitStateUnknown, a `load_table` probe decides whether the publish landed: the handed location is in the current metadata log, a publish-added snapshot is among the current snapshots, or a create's uuid matches. If it did not land, the attempt's files are removed, except those the current table still references. A walk read failure, an unloadable table or an unknown outcome keeps everything. Site: `crates/repark-iceberg/src/write/staged_publish.rs`, with six call sites in the two doors' CTAS, RTAS and CREATE TABLE paths.

**Result:** D2 is 12 of 12 EQUAL on both doors. Every other group keeps its class across the 364-cell re-run at each verifier round. The kill-at-publish rows are unchanged.

**Leads, not in this card:** [STAGED-PUBLISH-OPTIONS-PATH-1](staged-publish-options-path-1-card-2026-11-04.md) (version numbering on the options path); the walk-failure keep rule is unpinned (ledger C-009); the residue rows C-010 to C-013 of the ledger.

**Relation:** the kill-at-publish rows stay under ICE-LOCAL-COMMIT-POINT-1 (its registry row is in [spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)). The label draft's family-5 row ([v2-label-supported-behaviour-draft-2026-11-03.md](../../../docs/corpus/v2-label-supported-behaviour-draft-2026-11-03.md)) gains the Spark-door sentence.
