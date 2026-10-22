# Card FAIR-POOL-HANG-1: a facade test under a small fair pool sometimes never returns from the native call, more often the tighter the pool and the busier the box

**Date:** 2026-10-22. **Filed by:** Claude Sonnet 5.5 (`claude-sonnet-5-5`), builder of SPILL-REACH-FAIR-POOL-FLAKE-1, for the orchestrator.

**Status:** open, not scheduled. Found while measuring the card
[SPILL-REACH-FAIR-POOL-FLAKE-1](spill-reach-fair-pool-flake-1-card-2026-10-14.md); that unit does not absorb it and
changes no product code.

**Retires:** when the native stack of a stuck worker names the blocked or spinning frame and the cause is fixed or the
affected tests carry a dated row.

## Why

A test of `python/repark/tests/perf/test_t2_spill_reach.py` sometimes sits inside one native call for minutes when it
takes about one second if it returns. It is a hang, not a refusal: no error is raised and no spill count is reported. In
a suite run the progress bar stops at 97 to 99 % and only an outer limit ends it. The same shape is recorded without a
named test in [FACADE-HANG-98-1](facade-hang-98-1-card-2026-10-14.md); whether these are that stall is not established.

## Measured

All on the ci-profile module built from main `a556289d`, under a 40 G scope cap on a 26-core pin, on a box shared with
other builders (load average 9 to 77). The tables are in `task/wo/spill-reach-fair-pool-flake-1/evidence/` of the pull
request that carries this card.

**Whole `python/repark/tests/perf/` under `-n 16`**, the pool of the affected tests at 64 MiB (their default then):

| batch | repeats | repeats that hung in this file | how it was seen |
|---|---|---|---|
| step A 1 | 30 | 3 | the run stalled until the outer 600 s limit killed it; the 120 s signal-method timeout did not end the stall; no test named |
| step A 2 | 14 | 2 | `faulthandler` at 150 s, then the thread-method timeout killed the worker (`node down: Not properly terminated`) |
| step A 3 | 10 | 1 | the same, `faulthandler` at 170 s |
| step A 4 | 14 | 0 | quiet box |

**The same, 30 repeats per pool, the four pools taking turns** (step B sweep), and **the whole file alone**, 20 runs per pool:

| pool | `-n 16` repeats hung | in | alone runs hung | in |
|---|---|---|---|---|
| 64M | 3 of 30 | `test_distinct_spills_under_small_fair_pool` twice, `test_hash_agg_spills_under_small_fair_pool` once | 1 of 20 | `test_hash_agg_spills_under_small_fair_pool` |
| 72M | 0 of 30 | | 0 of 20 | |
| 80M | 1 of 30 | `test_hash_join_is_pinned_as_resources_exhausted` (16M pin) | 0 of 20 | |
| 96M | 1 of 30 | `test_hash_join_is_pinned_as_resources_exhausted` (16M pin) | 0 of 20 | |

**Stress: 200 separate processes of the file per pool, 20 at a time, no xdist** (150 s kill):

| pool | processes hung | tests that use the pool under test | the 16M hash-join pin |
|---|---|---|---|
| 64M | 18 | 16: `test_hash_agg_spills_under_small_fair_pool` 7, `test_hash_agg_distinct_spills_under_small_fair_pool` 5, `test_distinct_spills_under_small_fair_pool` 4 | 2 |
| 72M | 10 | 9: 4, 3, 2 | 1 |
| 80M | 1 | 1: `test_hash_agg_spills_under_small_fair_pool` | 0 |
| 96M | 1 | 0 | 1 |

In every dump with a named test the main thread of one process is inside `DataFrame.to_arrow_batches` (reached through
`DataFrame.collect` from `_explain_analyze`) or `DataFrame.to_arrow` (the 16M pin), at `core.py` lines 3291 and 3264; the
other threads are idle (the `execnet` receiver, or one `threading.Condition` wait). The dump shows Python frames only, so
which native frame is blocked, or whether it is spinning, is not known. No test that asserts the sort or the
sort-merge-join (`test_sort_spills_under_small_fair_pool`, `test_sort_merge_join_spills_under_small_fair_pool`) appears among
the hung ones, and the join probe was the only one to raise refusals. `test_distinct_spills_under_small_fair_pool`, which
the overnight watch of 2026-10-22 saw hang once, hung 2 of 30 repeats and 4 of 200 processes at 64M.

## Known facts and the open reading

- Every hang was under contention or, once, alone at 64M (1 of 20 whole-file runs, one process, no xdist, box load 9).
- The rate falls as the pool grows: of the stress processes, 16, 9, 1 and 0 hung in the tests that use the pool under test at
  64M, 72M, 80M and 96M. Raising the default pool of the file therefore lowers the rate and does not remove it (72M still
  hung 10 of 200). That a refusal in one partition leaves another partition's task waiting, so that the error is lost, is
  a reading that fits the dose and is not measured.
- The 16M pins hung at every pool, 0 to 2 in 200, because they do not take the pool under test.
- The 120 s signal-method timeout of step A batch 1 did not end its stalls; the thread method of the later batches did, by
  killing the worker.

## The ask

Step 0, for an Opus 5.5 agent (`claude-opus-5-5`), on a branch and never on `main`:

- Reproduce on demand with the stress recipe (about 1 process in 20 hangs at 72M, about 1 in 11 at 64M): 200 processes of the
  file, 20 at a time, `pytest-timeout` thread method, `faulthandler_timeout` set.
- While a process is stuck, take its native stacks with `gdb -p <pid> -batch -ex "thread apply all bt"` and record whether
  it is spinning (CPU ticks advancing over ten seconds) or blocked (state `S`, ticks flat), and what the others are doing.
- Hand back the stacks, the state and the pool state at that moment (`FairSpillPool` reserved and the refusal count, if
  readable). The card does not propose a fix; the fix, or a dated mark on the named tests, retires it.
