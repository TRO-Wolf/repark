# Card EXPLAIN-ROOT-SORT-1: `DataFrame.explain()` hides a root `Sort` that the executed plan keeps

**Date:** 2026-10-04 · **Filed by:** Claude (Opus 5.5), orchestrator · **Source:** the TA series design sketch (2026-10-04, Q8). The owner accepted every lean that day, including "file a card, out of scope". No product code in this filing.

## Measured (RePark 1.5.2, PyPI wheel)

```python
df = spark.createDataFrame([(3, "c"), (1, "a"), (2, "b")], ["k", "v"]).sort("k")
df.explain(True)
```

| What | Result |
|---|---|
| `df.collect()` | `[1, 2, 3]`: the sort runs |
| `explain(True)`, optimized logical plan | `SubqueryAlias` over `TableScan`, **no `Sort`** |
| `explain(True)`, physical plan | `DataSourceExec: partitions=1`, **no `SortExec`** |

## Why

- **How the facade explains a frame:** `_explain_text` (`python/repark/src/repark/spark/dataframe/core.py`, around line 2945) registers the frame as a scratch view `__repark_explain_<hex>` and plans `EXPLAIN … SELECT * FROM <view>`.
- **What DataFusion does with that view:** the SQL planner inlines it as a subquery and drops an unlimited `ORDER BY` from it (`datafusion-sql-54.1.0/src/relation/mod.rs:370-399`, gated by `datafusion.sql_parser.enable_subquery_sort_elimination`, default `true`, in `datafusion-common-54.1.0/src/config.rs:322`).
- **The result:** the explained plan is not the plan `collect()` runs. That plan does have the sort. Users reading `explain()` to check sort elision, window sorts or the TA series path (S1/S2a) are misled.

## Scope (proposed)

1. **Explain the frame's own plan.** Replace the scratch-view SQL round trip with a native explain of the frame's logical plan, or keep the view and turn off subquery sort elimination **for the explain statement only**.
2. **Pins:**
   - a root `Sort`, `sort().limit()`, `sort()` under `withColumn`, and a window over a sorted frame each show the `Sort`/`SortExec` that `collect()` executes. Mutation: restore the view round trip;
   - the plan text of every other `explain` mode is unchanged on the existing explain tests.
3. **Out of scope:** changing `enable_subquery_sort_elimination` for user SQL. Spark also ignores `ORDER BY` in subqueries without `LIMIT`.

## Grade and release

Clerk-to-B. The fix is facade plus native plan rendering, with no semantic change to execution. **Lean:** the next patch, independent of the TA series slices.
