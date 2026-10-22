# task/wo/spill-reach-fair-pool-flake-1/evidence/ — measurements of SPILL-REACH-FAIR-POOL-FLAKE-1

Everything here was measured on one box, on the ci-profile module built from main `a556289d` (`maturin develop --profile
ci`), every pytest run under a 40 G `systemd-run --scope` cap and `taskset -c 0-25`, with the box load average logged
before each run. The box is shared with other builders, so load swung from 2 to 77. **The runner half is absent:** the push
of the diagnostic branch and its draft pull request were denied on 2026-10-22, so no run of the CI shard was read. The
patch that would have run on the runner is kept as [diag-branch.patch](diag-branch.patch) (not applied).

Step A, the 64 MiB default (the pool before this unit):

- [step-a-summary.tsv](step-a-summary.tsv) — rates per condition and test: 0 of 30 alone, 4 of 67 under `-n 16` for the
  join probe; the runner rows are `NOT RUN`.
- [step-a-local-alone.tsv](step-a-local-alone.tsv), [step-a-local-alone-runs.tsv](step-a-local-alone-runs.tsv) — 30 fresh
  processes per instrumented test: spill counts, operator peaks, `VmHWM`, the box load per run.
- [step-a-local-loaded.tsv](step-a-local-loaded.tsv), [step-a-local-loaded-runs.tsv](step-a-local-loaded-runs.tsv),
  [step-a-local-hang-diag.tsv](step-a-local-hang-diag.tsv), [step-a-local-hang-runs.tsv](step-a-local-hang-runs.tsv) — 68
  repeats of `pytest python/repark/tests/perf -n 16 --dist loadgroup --maxschedchunk=8` in four batches, with the four
  refusals in full (consumer, bytes requested, bytes held, bytes the pool quoted as available).
- [step-a-local-pool-sweep.tsv](step-a-local-pool-sweep.tsv) — the two probes alone, 20 fresh processes per pool of 48M, 56M,
  64M and 72M: the join probe refuses 20, 9, 0 and 0 times.

Step B, the sweep ordered by the coordinator (30 loaded repeats and 20 alone runs of the whole file per pool, the four pools
taking turns so each saw the same box):

- [step-b-sweep-summary.tsv](step-b-sweep-summary.tsv) and [step-b-sweep-spill-counts.tsv](step-b-sweep-spill-counts.tsv) —
  per pool and condition: runs with all nine tests passing, refusals, assertion failures, hangs in this file, runs killed by
  other perf tests, and the spill counts and join peaks of the passing runs of the two spill probes.
- [step-b-runs.tsv](step-b-runs.tsv) — every run: exit code, box load before, wall time, pytest's last line.
- [step-b-spill-records.tsv](step-b-spill-records.tsv) — one row per instrumented test per run.

Step B supplement (the ordered sweep could not separate the pools: the 64M control also refused 0 of 59 there):

- [step-b-stress-summary.tsv](step-b-stress-summary.tsv), [step-b-stress-spill-counts.tsv](step-b-stress-spill-counts.tsv),
  [step-b-stress-spill-records.tsv](step-b-stress-spill-records.tsv) — 200 separate processes of the file per pool, 20 at a
  time, no xdist: the join probe refused 3 of 184 at 64M and 0 of 191, 199 and 200 at 72M, 80M and 96M.

The hangs found on the way: [hang-samples.md](hang-samples.md).

Ruling of 2026-10-22, the default is 80M: no new sweep was run for it, the 80M rows of the sweep and stress above are its
evidence. The final file on the 80M head was run once more:

- [step-b-final-file-gates-80m.tsv](step-b-final-file-gates-80m.tsv) — 10 alone runs of `test_t2_spill_reach.py` (9 passed
  each) and `python/repark/tests/perf` once under `-n 16` (604 passed, 24 skipped in 410 s), at a box load of 45 to 68 from
  other builders.

Up: [../map.md](../map.md).
