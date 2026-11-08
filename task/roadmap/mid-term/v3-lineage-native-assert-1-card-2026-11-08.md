# Card V3-LINEAGE-NATIVE-ASSERT-1: the empty-DML table asserts v3 lineage on the Spark door only

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 verdict of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-015, Opus 5.5 F5).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter. A test-adequacy lead; no behavior is in question.

**Why:** [empty_dml_shared_guard.rs](../../../crates/repark-sql/tests/empty_dml_shared_guard.rs) reads `_row_id` and `_last_updated_sequence_number` only when the door is Spark. On the ANSI and DataFusion-dialect doors the table asserts that snapshot and data files are unchanged, which implies lineage is unchanged for an empty cell, but it does not assert lineage itself. The Opus 5.5 verdict's probe measured the facade's v3 lineage as `none` on every guarded cell. The native Python door cannot create v3 tables. Order §7 asks every v3 cell to assert both columns.

**Proposed fix:** read lineage through the Spark-dialect session on the same catalog for the other doors, or record in the test why they do not.

**Close:** the table asserts lineage on every door and goes red on a deliberate lineage change, or a dated line records why a door is exempt.
