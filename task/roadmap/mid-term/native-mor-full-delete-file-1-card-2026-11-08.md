# Card NATIVE-MOR-FULL-DELETE-FILE-1: a full DELETE on the native door over v2 merge-on-read writes a position-delete file where Spark leaves data/ unchanged

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the verdicts of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-014).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter. Rows and snapshot equal Spark's, so this is not a wrong write.

**Why:** On `repark.sql()` over a v2 merge-on-read table, `DELETE FROM t WHERE true` and a `DELETE FROM t` with no WHERE delete every row and write a position-delete file. The data files stay as they were. On Spark 4.1.2 the same two statements delete every row and leave `data/` unchanged (the measured cells `c-true` and `c-no-where`, DELETE, v2-mor, in [spark.jsonl](../../wo/empty-dml-shared-guard-1/evidence/raw/spark.jsonl)). The inputs are non-empty controls, so the guard's coverage claim does not reach them. The Rust residue table in [empty_dml_shared_guard.rs](../../../crates/repark-sql/tests/empty_dml_shared_guard.rs) pins the file flip. The Opus 5.5 verdict notes the same layout on the facade for a predicate the metadata door does not see as constant true (`0.1 + 0.2 = 0.3`), as a pre-existing observation.

**Proposed fix:** decide whether a whole-table merge-on-read delete on the native door should remove data files as Spark does, or whether the layout is a declared difference with its own registry row and pin.

**Close:** the residue pin flips on purpose in the change that fixes it, or the difference is declared with its row.
