# Card FACADE-HANG-98-1: the facade suite sometimes stalls at 98–99 % with idle xdist workers

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Retires:** when the hanging test is named and fixed or marked with a dated row.

## Why

The facade suite (`pytest -n 8` over `python/repark/tests/`) sometimes stops making
progress at 98–99 % with idle xdist workers, both on CI and locally, and the job runs
until its timeout. A re-run passes. The stall is not reproduced on demand, so no test
is named yet and no cause is measured.

## Known facts

- The shape is a stall, not a failure: workers go idle near the end of the run and no
  further test completes before the job timeout.
- It happens on CI and on local runs, so it is not one runner's environment alone.
- A re-run of the same suite passes, so the stall is intermittent, not a fixed red.
- Unmeasured: which test never returns, whether the stall is one test or a worker
  teardown issue, and whether any product code is involved at all.

## The ask

- On a branch (never on `main`), run the suite with `--durations=0` and a per-test
  timeout from pytest-timeout, and record which test never returns.
- Whether a CI-only timeout change lands is the owner's call; this card only asks for
  the branch measurement that names the test.
- The fix or the dated mark for the named test retires this card; the measurement
  alone does not.
