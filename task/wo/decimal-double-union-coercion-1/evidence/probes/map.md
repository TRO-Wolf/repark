# task/wo/decimal-double-union-coercion-1/evidence/probes/ — the runners

Comment-free Python; the commands and the environment are in [../map.md](../map.md).

- [cells.py](cells.py) — the operand classes (`LEFT`, `RIGHT`), the 25 construct templates, the extra
  shapes (recursive CTEs, CTE, temp view, filters, joins, the COALESCE-NAN-DOUBLE-1 card's SQL) and the
  one-row `src` frame both engines register.
- [common.py](common.py) — value rendering (a double as `repr|bits`, a decimal as `dec:`), error
  records, the jsonl writer.
- [run_select.py](run_select.py) — every SQL cell on Spark, the RePark Spark door or the native door.
- [run_frames.py](run_frames.py) — the DataFrame API cells: six set operations over two SQL frames, and
  the column functions over `src`.
- [write_cells.py](write_cells.py) — the 30 Iceberg write cells (INSERT, UPDATE, MERGE, CTAS,
  DataFrame append).
- [run_write.py](run_write.py) — runs the write cells into a private hadoop catalog.
- [read_back.py](read_back.py) — Spark reads every table either engine wrote.
- [replay_888.py](replay_888.py) — replays #888's Spark recording on RePark and splits the cells by root.
- [rule_lists.py](rule_lists.py) — `EXPLAIN VERBOSE` on both doors: the analyzer and optimizer lists, the
  plans, and the PREPARE, cache and native-view cells.
- [analyze.py](analyze.py) — the verdict of every Spark/RePark pair.
- [render.py](render.py) — the construct-by-operand matrices.
- [write_table.py](write_table.py) — the write table.

Up: [../map.md](../map.md).
