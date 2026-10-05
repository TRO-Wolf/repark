# Card CACHE-ORDER-READ-1: a sorted single-partition `eager()` frame loses its order on filter reads

**Date:** 2026-10-05 · **Filed by:** Muse Spark 1.3 (contributor), for the orchestrator · **Source:** TA series S1 (draft PR #944). S1 makes plain reads of a sorted, single-partition `.eager()`/`cache()`/`localCheckpoint` frame keep their order; a filter read still interleaves. No product code in this filing.

## Measured

Probe `/tmp/oc-worker/direct/wo/cards-1005/probe_order.py`: 200,000-row frame, `sort("ts")`, `.eager()`, then plain and `filter(px >= 0.0)` reads checked for ascending `ts`.

| Engine | Plain reads sorted | Filter reads sorted |
|---|---|---|
| S1 head `b1337ead` (`/tmp/xtas1/.venv-head`) | 3 of 3 | 0 of 10 |
| RePark 1.5.2 wheel (`/tmp/oc-worker/direct/wo/ta-single-series/benchenv`) | 3 of 3 | 1 of 10 |
| Spark 4.1.2, `sort().cache().filter()` | 3 of 3 | 3 of 3 |

Spark's row comes from `/tmp/oc-worker/direct/wo/ta-single-series/bench/series/results_spark_order.json` (`sort_cache_then_filter`). The RePark rows are this card's own runs: `/tmp/oc-worker/direct/wo/cards-1005/order_s1.txt` and `/tmp/oc-worker/direct/wo/cards-1005/order_152.txt`.

The S1 lane's option sweep agrees: baseline, `datafusion.optimizer.prefer_existing_sort = true`, and `repartition_file_scans = false` each sort 0 of 10 (`/tmp/oc-worker/direct/wo/ta-series/s1-handback.json`, `filter_read_options`).

Plan text of `sort(ts).eager().filter(...)` on S1's head (from `order_s1.txt`):

```text
FilterExec: px@1 >= 0
  RepartitionExec: partitioning=RoundRobinBatch(26), input_partitions=1, maintains_sort_order=true
    DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=ts@0 ASC
```

1.5.2 shows the same shape minus the ordering claims (`RoundRobinBatch(26)`, no `output_ordering`, `partition_sizes=[4]`).

## Why

The S1 lane's finding, confirmed by the plan above: `BatchSplitStream` splits the single stored batch at execution (invisible in the plan), `RoundRobinBatch` spreads the slices across partitions, and the downstream `CoalescePartitionsExec` merges them in completion order. S1 declares `output_ordering=ts@0 ASC` on the scan and `maintains_sort_order=true` on the repartition, but neither survives the split-and-merge: the filter read interleaves on both engines while Spark keeps the order.

## Scope (proposed)

1. **Option (a): keep an ordered one-batch source unsplit under a filter.** A physical rule that recognizes a filter over a declared-sorted single-partition scan and skips the split/repartition round trip. Cost: a new planner rule plus the proof that skipping the fan-out never changes which rows survive, only their order; risk is a rule that fires too widely and pins other single-partition scans to one partition.
2. **Option (b): a final `SortPreservingMerge` on reads of declared-sorted caches.** Leave the split in place and re-merge on the declared key at the read. Cost: a merge pass over every filtered read of a sorted cache even when the consumer does not need order; risk is a performance regression on the TA loop S1 exists to speed up.
3. **Pins:**
   - `sort().eager()` plain, filter, `withColumn`, and heavy-expression-filter reads each answer ascending `ts` 10 of 10 runs. Mutation: restore the fan-out;
   - the same four reads on `.cache()` and `localCheckpoint` frames answer ascending `ts`;
   - a filter read that narrows to zero rows still answers empty, and the `explain` plan text of the filter read is pinned;
   - the TA single-series bench time does not regress past its S1 gate (option (b)'s merge must prove itself here).
4. **Out of scope:** multi-partition ordering (a repartitioned cache is unordered on Spark too — `sort_cache_then_repartition` is 0 of 3 there); changing `prefer_existing_sort` or `repartition_file_scans` defaults.

## Grade and release

**Lean B, with an Opus sketch** before the unit: the choice between (a) and (b) turns on planner-rule blast radius versus a hot-path merge, and the sketch should measure both on the TA loop first. **Release lean: after S1** — the unit builds on S1's declared ordering and must not rebase under it.
