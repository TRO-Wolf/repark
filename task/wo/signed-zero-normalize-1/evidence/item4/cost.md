# Item 4: cost, base `68b09d18` against head `015fda5ae` (release builds)

**Gate:** ≤ 1.05x median of three. **Result: (b) exceeds it.**

| Case | What | Base runs (s) | Head runs (s) | Base median | Head median | Head / base |
|---|---|---|---|---|---|---|
| (a) | the domain table's 207 `spark.sql` read statements on the facade (after its 21 setups), sum of cell times | 0.798, 0.812, 0.779 | 0.819, 0.801, 0.919 | 0.798 | 0.819 | **1.0256** |
| (b) | 200 `EXPLAIN`s after 10 warm-ups: 300 `DOUBLE` columns, 50 comparisons against zero (7 operators × 4 zero shapes) | 27.530, 27.756, 27.680 | 29.294, 29.050, 30.838 | 27.680 | 29.294 | **1.0583** |

One `repark.slice` unit holding the build lock, interleaved per run (a base, a head, b base, b head), fresh process
and warehouse each, cores 32–47 ([cost_probe.py](cost_probe.py), `cost-raw.jsonl`). Case (a) answered 173 cells on
base and 188 on head (the 15 former refusals now answer, so head does more work).
