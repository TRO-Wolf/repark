# TEMP-VIEW-SESSION-SETTINGS-1 part 1 — cost

**Builds:** head `pkg-view1-12c0680c` (`BUILT_FROM` `12c0680c`), main `pkg-avg-27d6149f` (main `6f80793b`'s product).
**Method:** 5 alternating runs of main, head and a second main (the control `main-aa`), each sample its own process
under the exclusive measure lock on cores 32-47 (`flock -x measure.lock` with the link and both cargo locks), every
sample at least 200 ms (the shortest 0.209 s). A view sample warms once, then times 8 to 20 repetitions; the table is
the median per repetition. Data: 2,000,000 rows (`k`) and 100,000 rows (`k2`) of Parquet, read as DataFrame views;
`v = SELECT a, b, a % 7 AS m FROM k` and `vv = SELECT a, m FROM v` are SQL temp views created under ANSI on. The
differing rows `SET spark.sql.ansi.enabled=false` before timing. C-012 is the replay set the earlier units held
(`replay_work.py`, the `c012p6` and `c012rest` halves). Scripts: [cost/](cost/map.md).

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

Up: [map.md](map.md).
