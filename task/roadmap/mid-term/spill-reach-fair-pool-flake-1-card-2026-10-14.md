# Card SPILL-REACH-FAIR-POOL-FLAKE-1: the small-fair-pool sort-merge-join spill probe fails intermittently on CI

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Retires:** when repeated runs settle whether the pool is too tight or the spill path has a gap, and the test or the product changes accordingly with a dated row.

## Why

`python/repark/tests/test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool`
fails intermittently on CI with `repark.errors.PySparkException: Resources exhausted:
Failed to allocate additional 560.3 KB for ExternalSorterMerge[1] with 2.7 MB already
allocated` (CI 2026-10-05 16:23 UTC, facade shard 2; CI 2026-10-13 13:27 UTC, facade
shard 0). Both re-runs passed. The failure is unmeasured: nobody has run it N times to
say whether the pool or the product is at fault.

## Known facts

- The test builds its session in `_small_fair_session` with the default `64M` pool:
  `datafusion.runtime.memory_limit` set to `64M`, 2 target partitions, a 1 MiB sort
  spill reservation, and batch size pinned to 8192 (the module docstring records that
  the recipes were calibrated at 8192-row batches and the larger session default lets
  the sort class finish in memory, which would blunt the probe).
- The test joins two 400,000-row series on `md5` of the id (so the inputs are not
  pre-sorted), with `datafusion.optimizer.prefer_hash_join` set to false, and asserts
  that the `EXPLAIN ANALYZE` plan text shows a spill count above zero.
- The CI failure text is a refusal, not a wrong answer: the sorter asked for 560.3 KB
  more with 2.7 MB already allocated and the fair pool refused.
- Unmeasured: how often the refusal reproduces, whether it depends on the runner's
  memory pressure, and whether a passing run spills by a wide margin or only just.

## The open question

Is the failure a real spill gap under a fair pool (a product issue: the sorter should
spill instead of refusing at this size) or a pool sized too tight for the runner (a
test issue: 64 MiB leaves no room once the shared runner is loaded)? The card records
both readings and rules neither out.

## The ask

- Run the single test N times on CI hardware and record the refusal rate with the
  spill counts of the passing runs.
- If the margin is thin, size the pool up with a dated row that keeps the probe's
  teeth (the 8192-row calibration note above constrains how far it can move).
- If the margin is wide and the refusal still hits, the spill path owns a product
  defect and this card hands it a measured repro.
