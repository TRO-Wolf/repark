# Card C-3-PARTITION-TYPES-1: the partition column types the partitioned read takes next

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the C-3 lane's hand-back (PR #998, not on main yet), question Q4, and the orchestrator's C-3 round 1 rulings (2026-10-08) on it.

**Status:** filed, not scheduled. The ruled order is `date` first, then timestamp once the zone rule is ruled.

## Why

**What C-3 delivers.** Partition columns of type `int2`, `int4` and `int8`.

**What C-3 refuses.** The other declared types refuse under the registry row `CONNECT-DECL-pg-partitioned-read`: `date`, `timestamp`, `numeric` and `float`. The hand-back's question names `timestamp/timestamptz`. Whether `timestamptz` is covered separately: Not measured.

**What Spark does.** Spark partitions all of them (C3-T04, T06-T09 in the C-3 ledger, PR #998).

**Why each is not a plain step.** Timestamp strides depend on the JVM zone (D-M2). Numeric and float strides are integer cuts over a non-integer column.

**The ruled order.** `date` first, because it is integer day arithmetic with no zone. Then timestamp, once the zone rule is ruled. Each gets its own recorded Spark grid. The order for numeric and float: Not measured.

## The ask

Deliver `date` first, with its own recorded Spark grid. Then deliver timestamp once the zone rule is ruled, with its own recorded Spark grid.

## Gates

- each type's own recorded Spark grid, before that type is delivered;
- any other gate: Not measured.

## Pointers

- The C-3 lane's hand-back (PR #998, not on main yet), question Q4.
- The orchestrator's C-3 round 1 rulings (2026-10-08), Q4.
- The registry row [`CONNECT-DECL-pg-partitioned-read`](../../../docs/spark-sql-iceberg-parity.md) on main. PR #998 rewrites this row.
