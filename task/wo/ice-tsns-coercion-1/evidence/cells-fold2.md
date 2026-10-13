# Cells moved, slice 1 fold 2 (2026-10-13)

The second verifier's finding G-1 lists 117 probe cells that main answers and the fold-1
head refuses at execution: 95 `sequence` cells and 22 pandas/Arrow door cells. This fold
re-ran the verifier's own probes — `j_seq.py`, `m_udf.py`, and the four `sequence` cases
of `h_retry.py` (`sc_209`, `sc_210`) and `i_cons.py` (`us_209`, `us_210`) — in the
verifier's configurations, on this head's release module and on the verifier's main
module. Counts: [cells-fold2.json](cells-fold2.json).

| Suite | Statement cells run | Refused before, answered now (main answers) |
|---|---|---|
| `j_seq` | 114 | 71 |
| `h_retry` (`sc_209`, `sc_210`) | 16 | 16 |
| `i_cons` (`us_209`, `us_210`) | 8 | 8 |
| `m_udf` | 160 | 18 |
| total | 298 | 113 |

All 95 `sequence` cells answer. Of the 22 door cells 18 answer. Four cells main answers
are still refused:

| Cell | Configurations | Why |
|---|---|---|
| `udf_foreach_batch_like`, `ns` — `createDataFrame(pandas, schema)` | America/New_York ANSI on; UTC ANSI off | Closed by ICE-TSNS-FRAME-DOOR-1 (draft PR #1045); re-measure after it merges. Not fixed here (ruling 4). |
| `udf_map_in_arrow_ddl_ltz`, `ns` — `mapInArrow(…, 'id int, v timestamp')` over the naive site | America/New_York ANSI on; UTC ANSI off | Kind mismatch: the site is a naive nanosecond column and the declared type is the zoned `timestamp`. The door refuses a naive microsecond column the same way on main and on head. Main answered only because it typed the naive site as a zoned microsecond column. Open question for the orchestrator. |

No cell that the fold-1 head answered is refused now. 43 cells that main refuses are
answered now: the plain forms `sequence(ns, ns)`, `sequence(ns, ns + INTERVAL …, INTERVAL …)`,
`typeof(sequence(ns, ns))`, `sequence(ntz, ntz + INTERVAL …, INTERVAL …)`,
`sequence(array(ns, NULL)[0], ns)` and `sequence(ns, coalesce(ns, NULL))` (36 cells), one
`explode(sequence(…))` DataFrame cell, `mapInArrow` over the site with a declared
`timestamp_ntz` (2, source `ns`) and `mapInArrow` over the plain nanosecond column with the
frame's own schema (4).

Facade file `test_ice_tsns_coercion_1_slice1_fold2.py`, 68 cells against main's recorded
answer: 67 answered on main; 65 answered here and 2 pinned as the kind refusal above. Every
one of the 68 equals the same cell over the microsecond twin column once its ticks are
floored to microseconds. Against main's ticks: 39 equal, 26 differ, and the 26 are exactly
the cells where main's own narrowing set the value — 15 where main read the naive `ns`
site as a wall in America/New_York (the `sequence` lists there run 18,001 elements from the
shifted start down to the unshifted stop), and 11 in UTC where main's DataFrame-door site
or Python door cut `-1 ns` toward zero to `0` instead of flooring to `-1 µs`.
