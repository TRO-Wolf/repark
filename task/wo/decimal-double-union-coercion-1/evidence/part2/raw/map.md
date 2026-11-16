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
- `joined-summary.json` is `../../step0/probes/analyze_step0.py`'s summary of step 0's joined cells, joining the
  Spark raw sets, main's and the head's.

Up: [../map.md](../map.md).
