# Card OFFSET-NESTED-SORT-1: an OFFSET under a nested ORDER BY answers no rows at one input partition

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the C-3 lane's hand-back (PR #998, not on main yet), its first out-of-scope item.

**Status:** **closed 2026-10-08** by unit OFFSET-NESTED-SORT-1 (branch `fix/offset-nested-sort-1`), pending the product PR's verifier. Confirmed, and wider than filed: the defect is in the pinned DataFusion 54.1.0 (`EnforceSorting`'s sort pushdown ignores a limit's `skip`), it is wrong wherever the scan below the inner sort has one partition (a `createDataFrame` view, a one-file parquet scan and an Iceberg scan at every partition count), and an outer sort over an offset-only limit was wrong at every partition count. RePark guards the rule; the reported statement answers 5 rows at 1, 2 and 16 partitions on all three doors, with a live Spark cell. Ledger: [../../ledgers/staging/offset-nested-sort-1-ledger.md](../../ledgers/staging/offset-nested-sort-1-ledger.md). Registry row: `OFFSET-NESTED-SORT-1` in [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md). The version question below is answered: RePark pins DataFusion 54.1.0.

**Status when filed:** filed, not scheduled.

## Why

**The statement.**

```sql
SELECT v FROM (SELECT v FROM t ORDER BY v LIMIT 5 OFFSET 4990) ORDER BY v
```

**The plan.** The engine plans `GlobalLimitExec: skip=4990, fetch=0` over `SortExec: TopK(fetch=4990)`.

**Observed.** At one input partition the statement returns 0 rows. At 16 partitions it returns 5 rows. The hand-back calls this a wrong answer: the statement should return 5 rows at every partition count.

**Reproduction.** It reproduces with no Postgres scan in the plan: `generate_series` under `target_partitions=1`. It also reproduces on the unpartitioned C-2 scan. The hand-back reads it as an engine defect, not a partition plan changing an answer. The hand-back names DataFusion 54.1. Whether that is the version RePark pins is Not measured.

**Verifier.** The C-3 verifier has been asked to reproduce it on main. Not yet confirmed by the verifier.

**Class.** The hand-back classes it as a wrong answer and says it needs its own card. This is that card.

## The ask

Reproduce it on main. Find whether it is the pinned DataFusion version's defect or RePark's planning. Fix it, or pin it upstream. Add a parity cell.

## Gates

- the statement returns 5 rows at every partition count;
- an Opus verifier on the product PR.

## Pointers

- The C-3 lane's hand-back (PR #998, not on main yet), its first out-of-scope item.
