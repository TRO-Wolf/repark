# Card STACK-LINEAR-FLAKE-1: `test_stack_is_linear_in_columns` fails intermittently on CI with exponent 1.128 over a 1.1 bound

**Date:** 2026-10-15. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Retires:** when the bound is re-based with measurements or the test is marked with a dated row.

## Why

`python/repark/tests/test_perf_unpivot_1.py::test_stack_is_linear_in_columns`
failed on CI on 2026-10-15 01:37 UTC, facade shard 1, run `38665297618`
attempt 1, on PR #1064's branch after it merged main; the re-run (attempt 2)
passed and the PR merged.

The CI record quotes the assertion text:

```
stack wall must be linear in columns; exponent=1.128 medians={50: 0.004130164999992303, 250: 0.02440666300000771, 500: 0.05617799899999909}
```

against the bound `exponent <= 1.1` in
`python/repark/tests/test_perf_unpivot_1.py:149`.

## Known facts

- The test (lines 131–153) times one timed shape per width over
  `widths = (50, 250, 500)`: per width it runs one warmup `_time_stack` pass
  and then three timed `_time_stack` passes, and takes the median of the three
  via `_median`. Each `_time_stack` pass builds a one-row frame of `width`
  literal columns, selects `F.stack(5, *frame.columns)`, times one
  `to_arrow()`, and asserts 5 rows out.
- The exponent is a log-log least-squares slope over the three
  (width, median) points (lines 142–148): natural logs of widths and of
  `max(median, 1e-9)`, mean-centred, slope `numerator / denominator`. The pin
  then asserts `exponent <= 1.1` and separately `medians[500] < 21.5`.
- Ledger clause C-002 of `task/ledgers/staging/perf-unpivot-1-ledger.md`
  states the contract: `stack` over a 500-column single-row frame at 5 rows
  out is linear in columns — three reps, medians at 50 / 250 / 500, exponent
  fit at or under 1.1 — against the bridge shape's 21.5 s collect / 0.7 s
  call. The ledger records the landing measurement (2026-09-12, no JVM) at
  exponent 0.91 with medians 0.0321 s / 0.1292 s / 0.2666 s.
- The 1.1 bound exists to catch the return of the old bridge shape the test
  message names: `bridge shape was 21.5s / 0.7s call at 500 columns` — the
  per-cell projection / `mapInArrow` path the unit deleted.
- Distance on this failure: the observed 1.128 clears the bound by 0.028 and
  sits about 0.87 below a quadratic exponent of 2.0. The 500-column median of
  0.056 s sits roughly 380x under the 21.5 s wall, so the companion
  `medians[500] < 21.5` assertion still passes by a wide margin. The CI
  medians (4 ms / 24 ms / 56 ms) are also several times faster than the
  ledger's landing medians, which puts the fit in a millisecond regime where
  small absolute noise moves the slope. Source for every number in this
  paragraph: the CI record quoted above, the test file, and the ledger
  clause; the "millisecond regime" reading is inference, not a measurement.
- Unmeasured: how often the over-bound exponent reproduces, whether it tracks
  loaded shared-runner timing noise, and whether any product slope changed at
  all. Nobody has run the test N times on a quiet machine.

## The open question

Is this a timing test sensitive to a loaded shared runner at millisecond
medians (a test issue: options such as more passes, a larger column count, or
a looser bound that still catches the quadratic shape) or a real slope change
(a product issue)? The card records both readings and rules neither out.

## The ask

- Run `test_stack_is_linear_in_columns` N times on a quiet machine and record
  the exponent distribution with the per-width medians of each run.
- If the over-bound readings recur only under load and the medians stay far
  under the 21.5 s wall, re-base the bound (or the pass count / column count)
  with the measurements in a dated row that keeps the pin's teeth against the
  quadratic bridge shape.
- If the slope holds above 1.1 on quiet hardware, the stack path owns a
  product change and this card hands it a measured repro.
