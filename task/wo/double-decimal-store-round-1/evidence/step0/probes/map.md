# task/wo/double-decimal-store-round-1/evidence/step0/probes/ — the runners

Comment-free Python; the commands and the environment are in [../map.md](../map.md).

- [cells.py](cells.py) — the 21 value texts, the five DECIMAL targets, the two sources, the doors, the
  card cells and the container, subscript and `TRY_CAST` select cells.
- [common.py](common.py) — value rendering (a double as `repr|bits`, a decimal as `dec:`), error records,
  the jsonl writer.
- [run.py](run.py) — one engine and ANSI mode into a private hadoop catalog: two Iceberg source tables
  (DOUBLE, FLOAT), then every door per source and target; a statement that fails is retried one row at a
  time so each row's answer or error is recorded; an optional comma list picks door groups.
- [analyze.py](analyze.py) — the per-row verdict of a Spark/RePark pair (`EQUAL`, `EQUAL-ERR`,
  `WRONG-VALUE`, `ERR-FOR-NULL`, ...).
- [render.py](render.py) — the tables in [../domain-table.md](../domain-table.md).

Up: [../map.md](../map.md).
