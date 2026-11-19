# task/wo/temp-view-frame-reader-parse-1/evidence/raw/ — one JSON object per cell and engine

- `<engine>-<part>.jsonl` — `engine` is `spark` (4.1.2), `main` (`4c365814`) or `part1` (`6286ba19`); `part` as in
  [../probes/map.md](../probes/map.md): `Z0`–`Z3` zone cross-setting passes (`ansi_true`/`ansi_false` × `la_utc`,
  `utc_la`), `Z4`–`Z7` the matching passes, `A` ANSI, `T` timestamp type, `S` SQL file sources, `W0`–`W3` writes,
  `E` plans. A row holds `rows` (sorted `repr` of each row) and `types`, or `error` (the condition, else the first 140
  characters), or `create_error` for a view or frame that could not be made. The cell tag reads
  `<set>/<mode>/<made>_<read>/<reader>/<door>[_<body>]`.
- [cells-verdicts.jsonl](cells-verdicts.jsonl) — 5,732 cells: Spark's, main's and part 1's answers, `v_main`,
  `v_part1`, `moved`, `cross`, the gap class and the emulation twins' verdicts (`probes/cmp.py`).
- [cells-summary.txt](cells-summary.txt) — the counts `cmp.py` prints. [tables.txt](tables.txt) — the tables
  `probes/tables.py` prints, quoted in [../../domain-table.md](../../domain-table.md).

Up: [../map.md](../map.md).
