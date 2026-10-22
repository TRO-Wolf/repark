# Hang samples — what the dumps showed

The raw dumps carry absolute paths of the scratch clone and are not committed; this is their content, summarized. Every
dump below was taken by `faulthandler` (`faulthandler_timeout` 120 to 170 s) or by `pytest-timeout`'s thread method
(120 to 500 s) from a process whose test normally returns in about one second. The card that owns the finding is
[FAIR-POOL-HANG-1](../../../roadmap/mid-term/fair-pool-hang-under-load-1-card-2026-10-22.md).

## Where the main thread was

Of the 31 stacks that contain a frame of `test_t2_spill_reach.py` (step B alone runs, `-n 16` sweep and stress):

| deepest Python frame of the main thread | stacks | tests |
|---|---|---|
| `DataFrame.to_arrow_batches` (`repark/spark/dataframe/core.py`, line 3291), reached through `DataFrame.collect` from the file's `_explain_analyze` | 27 | `test_hash_agg_spills_under_small_fair_pool`, `test_hash_agg_distinct_spills_under_small_fair_pool`, `test_distinct_spills_under_small_fair_pool` |
| `DataFrame.to_arrow` (line 3264) | 4 | `test_hash_join_is_pinned_as_resources_exhausted` |

The other threads in every dump are idle: the `execnet` receiver blocked in `read` (xdist runs only) and one
`threading.Condition.wait`. No native frame is visible in a Python dump, so the blocked or spinning native frame is
unknown; no `gdb` stack was taken.

## Samples

- **Step A, `-n 16`, repeat 2 of the second batch.** Worker gw8, 150 s into
  `test_hash_agg_distinct_spills_under_small_fair_pool` (a `count(DISTINCT md5(...))` plan under `EXPLAIN ANALYZE`, 64M
  pool): main thread in `to_arrow_batches`. The thread-method timeout then killed the worker
  (`node down: Not properly terminated`) and the suite ended at 99 % until the outer 420 s limit.
- **Step A, `-n 16`, repeat 10 of the second batch.** The same test, the same frame, the same ending.
- **Step A, `-n 16`, repeat 10 of the third batch.** Worker, 170 s into `test_hash_join_is_pinned_as_resources_exhausted`
  (16M pool, 1,000,000-row hash join, expects `Resources exhausted`): main thread in `DataFrame.to_arrow`. In the same
  repeat `test_h3_spill_matrix.py::test_a_pool_refusal_is_the_documented_spark_shaped_exception[window_unbounded-64M]`
  failed.
- **Step B, whole file alone, 64M, run 10 of 20.** One serial process, no xdist, box load average about 9. Two tests had
  passed (`test_runtime_set_memory_limit_error_is_fair_not_greedy`, `test_sort_spills_under_small_fair_pool`);
  `test_hash_agg_spills_under_small_fair_pool` sat in `to_arrow_batches` for the 120 s of the `faulthandler` timeout and
  the 250 s of the thread-method timeout, which ended the process. This is the only hang seen without xdist and without
  a second process competing, and the only one seen alone.
- **Step B, stress, 200 processes per pool, 20 at a time.** 30 processes hung (18, 10, 1, 1 at 64M, 72M, 80M, 96M),
  every one inside `to_arrow_batches` or `to_arrow`; the counts per test and pool are in
  [step-b-stress-summary.tsv](step-b-stress-summary.tsv).

## What the dumps do not say

Whether the process was spinning or blocked, what the pool held, and whether a refusal had been raised in another
partition of the same plan. Step 0 of the card asks for exactly that.
