# Card EXPLAIN-EMPTY-DML-SHAPE-1: plain EXPLAIN of an empty DML shows the rewritten plan, and its pin asserts only that the output is non-empty

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 verdict of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-012, Opus 5.5 F1).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter. Advisory: no write either way.

**Why:** After the guard, `EXPLAIN DELETE FROM t WHERE false` shows `Values: (UInt64(0))` and `DataSourceExec` on both doors. Base showed the `Dml` node, with the target table and the `IcebergDeleteExec` operation. The new output is honest about what executes, but the target table and the operation are gone. EXPLAIN text is DataFusion's and was never equal to Spark's, which prints `DeleteFromTable` with the table. The core pin `an_empty_dml_nested_in_a_wider_plan_is_guarded_too` asserts only that the output is non-empty.

**Proposed fix:** record the new text as the intended shape of an empty DML's plan, and pin it on both doors, or pin the existing text in the stack's map row as a dated line.

**Close:** the pin asserts the shape, or a dated ruling accepts it.
