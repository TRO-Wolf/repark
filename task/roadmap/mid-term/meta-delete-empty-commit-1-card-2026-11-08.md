# Card META-DELETE-EMPTY-COMMIT-1: a DELETE on a required column that matches no row commits an empty snapshot through the metadata-delete door

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the verdicts of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-013).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter. The verdicts recorded the residue and did not grade it. It is not a wrong write: rows and data files are unchanged.

**Why:** `DELETE FROM ice.ns.t WHERE id IS NULL`, with `id` declared NOT NULL, matches no row. The metadata-delete door answers it before physical planning, so the guard never sees it, and it commits one snapshot with no data change. That holds on the facade for all three table kinds, and on the Rust Spark and ANSI doors. Spark 4.1.2 commits no snapshot for the same statement (the measured cell `u-required-is-null`, DELETE, in [spark.jsonl](../../wo/empty-dml-shared-guard-1/evidence/raw/spark.jsonl)). The wheel test ([test_empty_dml_shared_guard_1.py](../../../python/repark/tests/writes/test_empty_dml_shared_guard_1.py), `METADATA_DELETE_RESIDUE`) and the Rust residue table pin the commit as it is, so a fix reds them on purpose.

**Proposed fix:** either the metadata-delete door answers the zero-count answer without committing, as the guard does for the other empty spellings, or the residue becomes a declared divergence with its own registry row and pin. The owner chooses.

**Close:** the residue pins flip on purpose in the same change that fixes it, or the owner declines the change and the difference is declared.
