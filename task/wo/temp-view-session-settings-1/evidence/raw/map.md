# task/wo/temp-view-session-settings-1/evidence/raw/ — raw cells, verdicts, counts, plans

Written by the probes in [../probes/](../probes/map.md). Scratch and home paths are placeholders.

- `spark-true.jsonl`, `main-true.jsonl` — every cell of `run.py`, Spark 4.1.2 and main; `head-avg-true.jsonl` — the
  `avg` body on #1355's package.
- `spark-unset.jsonl`, `main-unset.jsonl` — the unset case.
- `explain-spark.txt`, `explain-main.txt` — `explain.py`'s plans (RePark's show the closed SQL-view scan and the
  DataFrame view's doubled division guard).
- `cs-spark.json`, `cs-main.json` — `run_cs.py`.
- `verdicts.jsonl` — one row per paired cell: build, family, phase, door, position, body, created and read values,
  verdict, whether the directs agree, both answers, and which setting each engine followed.
- `headline.md` — the cross-setting counts by family, phase and door (view gaps silent and loud, direct gaps).
- `summary.md` — the verdicts by build, family and phase; the door-by-direction table; the follows grid; every write,
  nested, reader-side, `now` and unset cell.

Up: [../map.md](../map.md).
