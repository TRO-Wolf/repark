# TEMP-VIEW-SESSION-SETTINGS-1 part 1 — cost

**Builds:** head `pkg-view1-12c0680c` (`BUILT_FROM` `12c0680c`), main `pkg-avg-27d6149f` (main `6f80793b`'s product).
**Method:** 5 alternating runs of main, head and a second main (the control `main-aa`), each sample its own process
under the exclusive measure lock on cores 32-47 (`flock -x measure.lock` with the link and both cargo locks), every
sample at least 200 ms (the shortest 0.209 s). A view sample warms once, then times 8 to 20 repetitions; the table is
the median per repetition. Data: 2,000,000 rows (`k`) and 100,000 rows (`k2`) of Parquet, read as DataFrame views;
`v = SELECT a, b, a % 7 AS m FROM k` and `vv = SELECT a, m FROM v` are SQL temp views created under ANSI on. The
differing rows `SET spark.sql.ansi.enabled=false` before timing. C-012 is the replay set the earlier units held
(`replay_work.py`, the `c012p6` and `c012rest` halves). Scripts: [cost/](cost/).

**`EXPLAIN` identity:** the five matching cases' plans are identical on main and the head (`cost/explain-plans.json`),
as are the pin file's five (`temp_view_session_settings_1_plans.json`: read, filter, `LIMIT`, join, view over view).

| row | base (s) | head (s) | head / base | control (main-aa / main) | runs | bar |
|---|---|---|---|---|---|---|
| 1. a SQL view read, matching settings | 0.0122 | 0.0118 | 0.961 | 0.834 to 1.098 | 5 | held (≤ 1.05) |
| 2. a filter pushed through it | 0.0273 | 0.0266 | 0.973 | 0.967 to 1.016 | 5 | held |
| 3. a join over it | 0.0185 | 0.0189 | 1.023 | 0.961 to 1.106 | 5 | held |
| 4. a view over a view | 0.0128 | 0.0124 | 0.967 | 0.854 to 1.140 | 5 | held |
| 5. a query with no view | 0.0110 | 0.0114 | 1.033 | 0.974 to 1.117 | 5 | held |
| 6. C-012 `c012p6` | 39.88 | 41.11 | 1.031 | 0.489 to 1.014 | 5 | held |
| 6. C-012 `c012rest` | 43.78 | 45.88 | 1.048 | 0.987 to 1.081 | 5 | held |
| 1d. the read, settings differ | 0.0122 | 0.0122 | 0.999 | 0.927 to 1.025 | 5 | reported (ruled) |
| 2d. the filter, settings differ | 0.0290 | 0.0287 | 0.989 | 0.933 to 1.026 | 5 | reported |
| 3d. the join, settings differ | 0.0189 | 0.0198 | 1.046 | 0.887 to 1.131 | 5 | reported |
| 4d. the view over a view, settings differ | 0.0133 | 0.0127 | 0.954 | 0.936 to 1.040 | 5 | reported |

Every held row is at or under 1.05x at five runs, so no row needed the ten-run extension. Answers are equal across
the three builds on every row. On the differing path the base still plans the body under the reader (the wrong
answer for a settings-reading body); the head plans it under the captured settings, a cloned `SessionState` per scan.

## Delta 1 (2026-11-18): every held row re-run on the final head

**Builds:** head `pkg-view1-c54f5fc7` (`BUILT_FROM` `c54f5fc7`, the delta's product commit, the check in its final
place), main `pkg-avg-27d6149f`. Same runner, same data, same method: 5 alternating runs of main, head and `main-aa`,
each sample its own process under the exclusive measure lock on cores 32-47, every sample at least 200 ms. Raw:
`cost/delta1-raw-view.jsonl`, `cost/delta1-samples.jsonl`, `cost/delta1-c012-inlock.jsonl`.

**A flaw in the runner, found and fixed for C-012.** The runner timed a C-012 sample from before it queued for the
locks, so a sample that waited for another lane's build counted the wait: in the first pass three samples did
(`c012p6` head 71.82 s in run 1, `main-aa` 748.62 s in run 4, `c012rest` head 828.08 s in run 4), and part 1's own
`c012p6` control low of 0.489 is one such wait (a main sample of 81.45 s). The view rows time inside the process and
are unaffected. C-012 was re-run, 5 alternating runs, with the clock started and stopped inside the lock; the table's
C-012 rows are that re-run. (From 06:48 to 06:58 UTC a wait loop of the builder's ran unpinned while runs 2 and 3 of
the re-run were sampled; those runs' samples sit inside the other runs' range.)

**`EXPLAIN` identity:** the five matching cases' plans are identical on the head and main, and equal part 1's recorded
`cost/explain-plans.json`.

| row | base (s) | head (s) | head / base | control (main-aa / main) | runs | bar |
|---|---|---|---|---|---|---|
| 1. a SQL view read, matching settings | 0.0116 | 0.0118 | 1.015 | 0.949 to 1.049 | 5 | held (≤ 1.05) |
| 2. a filter pushed through it | 0.0272 | 0.0274 | 1.009 | 0.874 to 1.117 | 5 | held |
| 3. a join over it | 0.0186 | 0.0180 | 0.968 | 0.885 to 1.023 | 5 | held |
| 4. a view over a view | 0.0125 | 0.0124 | 0.990 | 0.917 to 1.083 | 5 | held |
| 5. a query with no view | 0.0112 | 0.0112 | 0.999 | 0.964 to 1.094 | 5 | held |
| 6. C-012 `c012p6` (timed inside the lock) | 39.34 | 39.22 | 0.997 | 0.987 to 1.028 | 5 | held |
| 6. C-012 `c012rest` (timed inside the lock) | 42.76 | 42.52 | 0.994 | 0.952 to 1.008 | 5 | held |
| 1d. the read, settings differ | 0.0127 | 0.0123 | 0.973 | 0.955 to 1.063 | 5 | reported |
| 2d. the filter, settings differ | 0.0289 | 0.0281 | 0.973 | 0.954 to 1.047 | 5 | reported |
| 3d. the join, settings differ | 0.0201 | 0.0193 | 0.958 | 0.912 to 0.988 | 5 | reported |
| 4d. the view over a view, settings differ | 0.0126 | 0.0126 | 1.002 | 0.924 to 1.071 | 5 | reported |

Every held row is at or under 1.05x at five runs; no ten-run extension was needed. Answers are equal across the three
builds on every row. The first pass's C-012 medians, wall clock with the waits, were 1.007 (`c012p6`) and 1.026
(`c012rest`). The check runs only on the differing path, once per scan, over the inlined frames' nodes.
