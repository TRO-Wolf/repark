# task/wo/spill-reach-fair-pool-flake-1/ — SPILL-REACH-FAIR-POOL-FLAKE-1

SPILL-REACH-FAIR-POOL-FLAKE-1 (2026-10-22, owner rulings "Re run" and "go with your recommendations"):
`test_sort_merge_join_spills_under_small_fair_pool` in `python/repark/tests/perf/test_t2_spill_reach.py` refuses
intermittently on CI with `Resources exhausted: Failed to allocate additional ... for ExternalSorterMerge[n]` (2026-10-05
shard 2, 2026-10-13 shard 0, 2026-10-22 shard 3 on PR #1142, run 39763165319, where the owner allowed one re-run). The
card is [spill-reach-fair-pool-flake-1-card-2026-10-14.md](../../roadmap/mid-term/spill-reach-fair-pool-flake-1-card-2026-10-14.md).
The unit built on branch `fix/spill-reach-fair-pool-flake-1` from main `a556289d` (builder Claude Sonnet 5.5, orchestrator
review). The order is one change: the default pool of the file's session helper, `_small_fair_session`, goes from 64M to
72M. No assertion, no bound, no batch size and no 16M pin changes, nothing is skipped or marked xfail, and no CI run was
repeated to find a pass.

**What was measured, and what was not.** The runner half was not measured: the push of a throwaway diagnostic branch and its
draft pull request were denied by the permission system on 2026-10-22 and not retried, so the criterion was adapted by the
owner's word to a local loaded sweep, and the first CI run of this pull request's shard 0 is the only runner sample. Locally,
at 64M, the join probe refused 0 of 30 runs alone and 4 of 67 under `-n 16` beside the rest of `python/repark/tests/perf/`;
in all four the request exceeded the bytes the pool quoted as available. Alone, the same probe refused 20 of 20 at 48M,
9 of 20 at 56M and 0 of 20 at 64M, so 64M sat 8 MiB above a cliff. The ordered sweep (4 pools, 30 loaded repeats and 20
alone runs of the whole file each, taking turns) refused 0 at every pool including the 64M control, so by itself it could not
tell the pools apart; a supplementary stress (200 separate processes of the file per pool, 20 at a time) refused 3 of 184 at
64M and 0 of 191, 199 and 200 at 72M, 80M and 96M. Across everything loaded, 64M refused 7 of 280 and 72M 0 of 221 (one-sided
Fisher p = 0.016, conditions not identical). Every passing run of the two spill-asserting probes still spilled at 72M (join
13 to 23, distinct 7 to 10), so the probe keeps its teeth. 72M is the smallest candidate with zero refusals everywhere, as
ordered; 80M and 96M also had none.

**The hangs.** The measurement found a second symptom: a test of this file sometimes never returns from the native call,
alone once and under contention often (64M, 200 processes: 18 hung; 72M: 10; 80M: 1; 96M: 1, the 16M pin). Moving the
default to 72M lowers that rate and does not remove it. It is a product finding, not a test one, and it is carded apart as
[FAIR-POOL-HANG-1](../../roadmap/mid-term/fair-pool-hang-under-load-1-card-2026-10-22.md); this unit does not absorb it.

**Ruling of 2026-10-22 (orchestrator, under the owner's "go with your recommendations"): the default is 80M, not 72M.** The
figures above are what was measured and stay; the paragraph above names the order as first built. The ordered criterion could
not separate 64M from 72M on a quieter box, so the stress table decides: in CI a hang costs a 45-minute job timeout and a
re-run, a refusal one red cell, and 80M cut the pool-dependent hangs from 16 of 200 stress processes (64M) and 9 (72M) to 1,
with 0 refusals in 229 loaded-type runs and the join still spilling 12 to 14 times. A later commit on the same branch changes
the default argument and the docstring digit that names the pool; the assertions are untouched.

The evidence is in [evidence/](evidence/map.md). The unit's ledger is
[../../ledgers/staging/spill-reach-fair-pool-flake-1-ledger.md](../../ledgers/staging/spill-reach-fair-pool-flake-1-ledger.md);
it stays in staging because clause C-006 (the runner sample) is open.

Up: [../map.md](../map.md).
