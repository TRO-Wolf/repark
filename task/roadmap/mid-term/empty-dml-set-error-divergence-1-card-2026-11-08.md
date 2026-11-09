# Card EMPTY-DML-SET-ERROR-DIVERGENCE-1: a no-op UPDATE whose SET expression errors answers count 0 where Spark refuses on v2 merge-on-read, and no test on main pins it

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 verdict of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-011, Opus 5.5 F2).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter for the pin. The owner's ruling "All recommendations" (item 8) declares it: ruling D extends to Spark's errors from a no-op DML's SET expression.

**Why:** `UPDATE t SET prim_col = 1 / 0 WHERE false` on a v2 merge-on-read table. Spark 4.1.2 refuses with `ArithmeticException [CAST_OVERFLOW_IN_TABLE_INSERT]` on `prim_col`. RePark answers count 0 and commits nothing (the Opus 5.5 verdict's repro, facade, v2-mor). On v2-cow and v3 Spark answers and commits an empty snapshot, which is the first divergence of ruling D. Base rewrote every file and committed, so the head moves from a wrong write to no write, and the refusal is still missing. No test on main sets an erroring SET expression. The wheel table's assignment is fixed at `flag = 'z'`, and the registry admits no row without a pin ([spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §6), so the declared row cannot land yet.

**Proposed fix:** add a wheel cell with `SET prim_col = 1 / 0` on v2-mor to the table test, with the Spark refusal recorded in the oracle table, and land registry row EMPTY-DML-SHARED-GUARD-1-D3 in the same change as its pin.

**Close:** the cell and the registry row land together, and this card closes.
