# SIGNED-ZERO-CONST-COMPARE-1 — cost

Base: the release build of main `d843d37e`. Head: the release build of the fix. Five alternating runs (orders
base→head, head→base, …), each sample under the exclusive measure lock and both lanes' cargo locks, on cores
48–57, measured 2026-11-14 between 10:07 and 10:45 local time. A sample waits 60 s first when the one-minute load
is above 4.

- `c012p6`, `c012rest`: the C-012 set, the `stamp2-step1` replay (`replay_work.py`), the same split and case lists
  as the earlier cost runs; process wall time.
- `const200` ([const200.py](const200.py)): one session, 40 iterations of plan plus collect, median of the last 35:
  - `folded200`: 200 folded constant comparisons, half of them `(CAST(i AS DOUBLE) * -1) <= CAST('i.0' AS DOUBLE)`,
    which the new seat folds and does not substitute;
  - `column200`: 200 `CAST(id AS DOUBLE) + 0.5 = CAST('i.5' AS DOUBLE)` disjuncts, whose constant side is a
    candidate on every pass.
  Both answers are identical on both builds (`[[1000]]`, `[[200]]`). The committed probe differs from the one
  measured only in opening its output file with `Path.open`, outside the timed loop.

| case | runs | base median | head median | head/base |
|---|---|---|---|---|
| c012p6 (process wall, s) | 5/5 | 37.55 | 37.87 | 1.0085 |
| c012rest (process wall, s) | 5/5 | 40.99 | 41.43 | 1.0107 |
| const200 (process wall, s) | 5/5 | 48.82 | 50.66 | 1.0377 |
| folded200 (plan + collect, ms, median of 35) | 5/5 | 610.98 | 631.44 | 1.0335 |
| column200 (plan + collect, ms, median of 35) | 5/5 | 598.33 | 622.40 | 1.0402 |


Held: every row is at or below 1.05x (largest: `column200`, 1.0402).

Raw samples, in seconds or milliseconds:

```
raw walls: {'const200/base': [49.12, 48.7, 48.77, 48.82, 49.0], 'const200/head': [50.73, 50.41, 50.66, 50.7, 50.51], 'c012p6/base': [38.24, 37.52, 37.8, 37.5, 37.55], 'c012p6/head': [37.87, 38.04, 38.23, 37.73, 37.71], 'c012rest/base': [40.99, 41.21, 41.01, 40.98, 40.8], 'c012rest/head': [41.48, 41.34, 41.66, 41.43, 41.15]}
raw inner: {'folded200/base': [610.98, 609.07, 611.08, 610.01, 613.13], 'column200/base': [598.61, 598.33, 597.49, 597.6, 600.27], 'folded200/head': [627.45, 631.44, 632.48, 631.54, 630.46], 'column200/head': [622.4, 620.94, 624.14, 624.14, 621.88]}
```
