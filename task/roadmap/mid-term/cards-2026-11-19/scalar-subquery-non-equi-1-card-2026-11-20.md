# SCALAR-SUBQUERY-NON-EQUI-1 — a correlated scalar subquery with a non-equality predicate fails in DataFusion's scalar_subquery_to_join

**Rating: proposed; the owner rates it.** Proposed S3, pending one probe of `UPDATE` and `DELETE` shapes on a v2 table,
as Fable's section 14 says.

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20.
**Source:** Fable's consult reply (2026-11-20), section 14, kept outside the repository; the step 0 is
[task/wo/asof-join-1/](../../../wo/asof-join-1/map.md), and its guidance is
[guidance.md](../../../wo/asof-join-1/guidance.md).

**Status:** filed 2026-11-20; not dispatched.

**Severity: S3, pending the DML probe.** This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M] The refusal.** A correlated scalar subquery with a non-equality predicate fails in DataFusion's
`scalar_subquery_to_join` with a schema error. Spark answers. Main refuses in every non-equality shape measured, and
never returns a wrong row.

The message is DataFusion's internal one: `Optimizer rule 'scalar_subquery_to_join' failed caused by Schema error`.
The card should ask for a clean refusal at the least.

**The label path is not shown either way.** Every measured shape is a `SELECT`. The same subquery inside
`UPDATE ... SET c = (SELECT ...)` or a `DELETE`/`MERGE` predicate on an Iceberg v2 table was not measured by the
builder or the critic. If those refuse too, it is off the path.

## First step

One probe of the `UPDATE` and `DELETE` shapes on a local v2 table. It should run before the card's rating is final.

Step 0: [task/wo/asof-join-1/](../../../wo/asof-join-1/map.md).

Up: [map.md](map.md).
