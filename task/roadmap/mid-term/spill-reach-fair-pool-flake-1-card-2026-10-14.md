# Card SPILL-REACH-FAIR-POOL-FLAKE-1: the small-fair-pool sort-merge-join spill probe fails intermittently on CI

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Status update (2026-10-22, SPILL-REACH-FAIR-POOL-FLAKE-1, branch `fix/spill-reach-fair-pool-flake-1`):** three CI
hits are now dated: 2026-10-05 16:23 UTC facade shard 2, 2026-10-13 13:27 UTC facade shard 0, 2026-10-22 facade shard 3 on
PR #1142 (run 39763165319, re-run on the owner's word and green). Earlier occurrences are in
[the run 9 report](overnight-report-2026-09-12-run9.md) (2026-09-12, PR #555, `ExternalSorterMerge[0]`, 768.0 KB). Measured
locally at the 64 MiB default, on the ci-profile module: the join probe refused 0 of 30 runs alone and 4 of 67 runs under
`-n 16` beside `python/repark/tests/perf/` (in all seven refusals recorded in full the request exceeded the bytes the pool
quoted as available, and in three of them that figure was zero), and 7 of 280 loaded-type runs in all; a pool of 72 MiB
refused 0 of 221 loaded-type runs and every passing run still spilled (13 to 23 spills for the join, 7 to 10 for the
distinct probe). The default is now 72 MiB, with a dated row in
[the perf map](../../../python/repark/tests/perf/map.md). The runner half was not measured (the push of the diagnostic
branch was denied on 2026-10-22), so the first CI run of the shard that holds this file is the only runner sample. Work
order and evidence: [../../wo/spill-reach-fair-pool-flake-1/map.md](../../wo/spill-reach-fair-pool-flake-1/map.md); ledger:
[../../ledgers/staging/spill-reach-fair-pool-flake-1-ledger.md](../../ledgers/completed/spill-reach-fair-pool-flake-1-ledger.md).
The hangs found on the way are carded apart as [FAIR-POOL-HANG-1](fair-pool-hang-under-load-1-card-2026-10-22.md).

**Status update, later the same day (orchestrator ruling 2026-10-22, under the owner's "go with your recommendations"):** the
default is 80 MiB, not the 72 MiB above; the 72 MiB figures stay as measured. The ordered criterion could not separate 64 MiB
from 72 MiB on a quieter box, so the stress table decides: in CI a hang costs a 45-minute job timeout and a re-run while a
refusal costs one red cell, and 80 MiB cut the hangs in the pool-dependent tests from 16 of 200 stress processes (64 MiB) and 9
(72 MiB) to 1, with 0 refusals in 229 loaded-type runs and the join probe still spilling 12 to 14 times. The `spill_count > 0`
assertions are unchanged. The test step of the first CI run of the shard that holds the file, on the 72 MiB head, was green; the runner half is
still not measured at 80 MiB.

**Retires:** when repeated runs settle whether the pool is too tight or the spill path has a gap, and the test or the product changes accordingly with a dated row.

## Why

`python/repark/tests/perf/test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool`
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
