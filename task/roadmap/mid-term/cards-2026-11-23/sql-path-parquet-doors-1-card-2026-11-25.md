# Card SQL-PATH-PARQUET-DOORS-1: SQL reads of a Parquet path refuse where Spark reads them

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from finding C1 of the first verifier of
PARQUET-PARTITION-DISCOVERY-1 (PR #1430). **Severity:** S2 proposed; the owner rates it. Each cell refuses loudly, and
none answers wrongly. **Status:** filed, not dispatched.

## Measured

Over a Spark `partitionBy("p")` directory of `id bigint, s string`, in UTC, with ANSI on. Main and #1430's head refuse
identically:

| Statement | Spark 4.1.2 | Main |
|---|---|---|
| ``SELECT * FROM parquet.`<dir>` `` | `id, s, p int` | refuses: table not found |
| `CREATE TEMPORARY VIEW v USING parquet OPTIONS (path '<dir>')` | a view of `id, s, p int` | refuses: expected `AS` with the view query |
| `CREATE TABLE t USING parquet LOCATION '<dir>'` | a table of `id bigint, s string, p int` | refuses: `CREATE TABLE` without `AS SELECT` requires a column list |

The native door does not change between main and #1430.

## First step

Measure each spelling over flat and partitioned directories, a single file, and the CSV, JSON and ORC formats. Then
route each spelling to the `read.<format>` path, so that it shares partition discovery once #1430 and
CSV-JSON-PARTITION-DISCOVERY-1 land.

Up: [map.md](map.md).
