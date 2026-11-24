# TEMP-VIEW-SESSION-SETTINGS-1 part 2 — cost (delta 2, delta 3)

**Run:** 2026-11-18, 18:15–18:58 UTC, then the one row over the bar to 10 runs (19:00 UTC). The runner
`cost/view_cost2.py` (the build's cases, delta 1's `exactjoin` and `crossjoin`), five alternating main, head, main runs
per row (the second main run is the control, `main-aa`), on lane B's cores. Each sample holds the measure lock
exclusively, with the link lock and both cargo locks, and its clock starts inside the lock: `time.perf_counter` inside
the locked Python process for the view rows, `date` inside the locked `bash -c` for C-012. A view row's figure is the median
over runs of each sample's time per timed repetition; C-012's is the median wall time of its replay.

Delta 1's run never produced a row: its `exactjoin` and `crossjoin` frames read `b` from `k2`, whose columns are `a`
and `w`, so every case failed when the script loaded. Delta 2 reads `w AS b`; the first, failed run of this delta was
discarded and the series restarted.

| row | base `pkg-view1-c54f5fc7` (s) | head `pkg-view2-9d71e030` (s) | head/base | control band (main-aa/main) | bar | runs |
|---|---|---|---|---|---|---|
| C-012 c012p6 | 38.9600 | 39.6000 | 1.016 | 0.994 to 1.036 | 1.05x (held) | 5 |
| C-012 c012rest | 42.4000 | 42.4500 | 1.001 | 0.958 to 1.072 | 1.05x (held) | 5 |
| differ_cache | 0.0261 | 0.0267 | 1.022 | 0.964 to 1.032 | reported | 5 |
| differ_filter | 0.0282 | 0.0277 | 0.982 | 0.967 to 1.087 | reported | 5 |
| differ_join | 0.0196 | 0.0207 | 1.054 | 0.909 to 1.133 | reported | 5 |
| differ_nested | 0.0136 | 0.0136 | 1.005 | 0.979 to 1.029 | reported | 5 |
| differ_read | 0.0134 | 0.0138 | 1.026 | 0.959 to 1.064 | reported | 5 |
| match_cache | 0.0230 | 0.0227 | 0.985 | 0.850 to 1.033 | 1.05x (held) | 5 |
| match_condjoin | 0.0122 | 0.0126 | 1.028 | 0.941 to 1.040 | 1.05x (held) | 5 |
| match_crossjoin | 0.1886 | 0.1902 | 1.009 | 0.984 to 1.017 | 1.05x (held) | 5 |
| match_exactjoin | 0.0134 | 0.0138 | 1.029 | 0.971 to 1.045 | 1.05x (held) | 5 |
| match_explode | 0.0117 | 0.0119 | 1.014 | 0.960 to 1.069 | 1.05x (held) | 5 |
| match_filter | 0.0249 | 0.0253 | 1.017 | 0.949 to 1.071 | 1.05x (held) | 5 |
| match_gagg_lit | 0.0074 | 0.0076 | 1.020 | 0.924 to 1.084 | 1.05x (held) | 5 |
| match_insertinto | 0.0317 | 0.0323 | 1.019 | 0.975 to 1.031 | 1.05x (held) | 5 |
| match_join | 0.0185 | 0.0190 | 1.031 | 0.954 to 1.051 | 1.05x (held) | 5 |
| match_limit | 0.0069 | 0.0070 | 1.019 | 0.992 to 1.085 | 1.05x (held) | 5 |
| match_nested | 0.0127 | 0.0128 | 1.007 | 0.974 to 1.081 | 1.05x (held) | 5 |
| match_noview | 0.0119 | 0.0122 | 1.024 | 0.926 to 1.069 | 1.05x (held) | 5 |
| match_parquet | 0.0125 | 0.0127 | 1.023 | 0.973 to 1.054 | 1.05x (held) | 5 |
| match_read | 0.0125 | 0.0124 | 0.986 | 0.991 to 1.103 | 1.05x (held) | 5 |
| match_saveastable | 0.0312 | 0.0306 | 0.982 | 0.870 to 1.026 | 1.05x (held) | 5 |
| match_selectexpr | 0.0094 | 0.0092 | 0.980 | 0.988 to 1.021 | 1.05x (held) | 5 |
| match_setop | 0.0295 | 0.0293 | 0.994 | 0.928 to 1.017 | 1.05x (held) | 5 |
| match_unpivot | 0.0089 | 0.0089 | 1.004 | 0.914 to 1.114 | 1.05x (held) | 10 |
| match_writeto | 0.0305 | 0.0315 | 1.032 | 0.962 to 1.051 | 1.05x (held) | 5 |

**Held rows (the matching path, 1.05x):** all 21 within the bar. `match_unpivot` was 1.056 after 5 runs, above its
control band (0.914 to 1.054), and 1.004 after 10. `match_limit` and `match_setop` answer in an unspecified row order
(`LIMIT` without `ORDER BY`, `intersect`), so their answers differ between main's own runs too. **Differing path
(reported):** 0.982x to 1.054x. The six matching cost cases' plans (`cost/explain-base.json`,
`cost/explain-head.json`) are identical to main's, scratch paths aside.

## Delta 3 (on `pkg-view2-9aa2f182`, against main now)

**Run:** the delta 2 runner (`S/ddu-work/cost-view2/run.sh`, copied with its base package overridable) on lane A's
cores, five alternating main, head, main runs per row, each sample under the exclusive measure lock with its clock
started inside the lock; the base is main now (`pkg-main-6286ba19`, which carries DECIMAL-DIVIDE-LITERAL-1 as the
head does). The held rows the brief names: `match_read`, `match_filter`, `match_join`, `match_insertinto`,
`match_writeto` and C-012. Samples: `cost/delta3/samples.jsonl`. The change adds one uncontended timed read and one
mutex per inline of a frame view.

| row | base `pkg-main-6286ba19` (s) | head `pkg-view2-9aa2f182` (s) | head/base | control band (main-aa/main) | bar | runs |
|---|---|---|---|---|---|---|
| C-012 c012p6 | 39.6600 | 39.7700 | 1.003 | 0.935 to 1.079 | 1.05x (held) | 5 |
| C-012 c012rest | 42.9400 | 43.1800 | 1.006 | 0.919 to 1.012 | 1.05x (held) | 5 |
| match_read | 0.0123 | 0.0124 | 1.008 | 0.936 to 1.057 | 1.05x (held) | 5 |
| match_filter | 0.0259 | 0.0268 | 1.037 | 0.959 to 1.053 | 1.05x (held) | 5 |
| match_join | 0.0191 | 0.0185 | 0.972 | 0.893 to 1.097 | 1.05x (held) | 5 |
| match_insertinto | 0.0319 | 0.0325 | 1.019 | 0.920 to 1.012 | 1.05x (held) | 5 |
| match_writeto | 0.0329 | 0.0321 | 0.976 | 0.976 to 1.092 | 1.05x (held) | 5 |

**Held rows (1.05x):** all seven within the bar (0.972x to 1.037x). The cost cases' plans (`cost/delta3/explain-base.json`, `cost/delta3/explain-head.json`): 6 of 6 identical to main now's, scratch paths aside.

The series ran from 2026-11-18 at about 22:47 UTC to 2026-11-19 at 00:25 UTC on lane A, run by the orchestrator, because the builder's own series got only 2 of 105 samples under the exclusive measure lock before its turn budget ran out. The first attempt was stopped at 23:43 by another lane's process cleanup, so runs 3 to 5 were re-run. All 105 samples exit 0.
