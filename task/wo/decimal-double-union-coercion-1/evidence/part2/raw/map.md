# task/wo/decimal-double-union-coercion-1/evidence/part2/raw/ — raw sets

One JSON object per cell, except the summaries and verdict counts. Paths under the session scratchpad read
`<scratchpad>`.

- `p2b-<probe>-<spark|a06c|dc6b|head>-<true|false>.jsonl` are the cells of `../../part2-step0/probes/p2_cells.py`
  and of `../probes/build_cells.py`, `fam.py`, `bound.py`, `identity.py` and `f3.py`, on Spark 4.1.2, main
  `a06c1088`, main before part 1 (`dc6b4c94`) and the head, in each ANSI mode.
- `p2b-verdicts-vs-a06c.txt` and `p2b-verdicts-vs-dc6b.txt` are `../probes/verdicts.py`'s counts per probe and mode,
  and `p2b-nonequal-vs-a06c.jsonl` lists every cell that is not EQUAL → EQUAL against a06c.
- `preimage-head-ansi-<true|false>.jsonl` are part 1's preimage cells (`../../fix/probes/preimage_cells.py`) on the
  head, beside Spark's and the delta build's in [../../fix/raw/](../../fix/raw/map.md).
- `delta-<spark|a06c|head|new>-<true|false>.jsonl` are `../probes/delta_cells.py`'s cells on Spark 4.1.2, a06c, the
  build's head (`pkg-p2-7906b0e3`) and the delta's head (`pkg-p2-12101e9b`); `delta-verdicts-vs-a06c.txt` is
  `../probes/delta_verdicts.py`'s count for the delta's head. `verifier-sets-vs-a06c.txt` counts both verifiers' sets
  re-run on the delta's head against their own Spark and a06c raw sets, `preimage-delta-verdicts.txt` part 1's
  preimage cells, and `p2b-delta-verdicts-vs-a06c.txt` the bar's six sets.
- `cost-build-table.md` is the build runner's 5-run table on `pkg-p2-6be38a8e` (the halt), and
  `cost-delta-table.md` the delta's table on `pkg-p2-12101e9b`; both are the runner's `analyze.py` output.
- `joined-summary.json` is `../../step0/probes/analyze_step0.py`'s summary of step 0's joined cells, joining the
  Spark raw sets, main's and the head's.

Up: [../map.md](../map.md).
