# task/wo/listview-inner-join-key-1/evidence/step0/

The clause: the clerk's 2026-11-11 step-0 table, 326 cells on main `68f70b0f` and 300 on Spark 4.1.2, committed by build item 1 with its absolute paths scrubbed.

- [table.md](table.md) — the rendered table: counts by key type, join form and join type, the wrong and cross-product lists, and both cell tables.
- [explain_failing.md](explain_failing.md) — `explain(True)` for the failing `df_str` inner base cells on main.
- [notes.md](notes.md) — the clerk's judgement calls, oracle observations, run history and gaps.
- [provenance.md](provenance.md) — interpreters, sessions, commands, the placeholder map, and what the committed copy changed.
- [probe_listview_table.py](probe_listview_table.py) — the probe (both legs and `--render`). It takes `--out` and reads the jar from `ICEBERG_JAR`; `--render` over `raw/` reproduces `table.md` and `explain_failing.md` byte for byte.
- [raw/](raw/) — `main.jsonl` and `spark.jsonl`, one JSON line per cell after a meta line.

Up: [../map.md](../map.md).
