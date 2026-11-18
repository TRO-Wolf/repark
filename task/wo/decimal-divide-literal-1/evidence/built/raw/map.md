# task/wo/decimal-divide-literal-1/evidence/built/raw/ — raw data (scratch paths scrubbed to `<scratchpad>`)

- `head/` — the head's cells from step 0's probes, in step 0's layout (`repark-<ansi>-<conf>.jsonl`, `native.jsonl`,
  `rev/`, `extra-head-true.jsonl`, and `extra-main-true.jsonl` for main's `run_extra` cells).
- `compare.jsonl` — per cell, run and mode: main's and the head's verdicts against Spark, and whether the head's
  answer equals main's; `compare-extra.jsonl` the same for `run_extra`.
- `pins/` — the pin file's runs on the head and on main's package.
- `cost/` — the cost samples (`samples.jsonl`, `raw-ddl.jsonl`) and `table.md`. Delta 1 replaced them with its
  series on `pkg-ddl-8efe8e69`; the build's two-run samples are kept in `cost/build-2run/`.
- `delta1/` — delta 1's cells (compacted): `rem-<who>-<ansi>.jsonl` (the remainder probe), `ctas-*` (the session-view
  CTAS cell), `card-*` (the proposed cards' cells), for `spark`, main `pkg-avg-27d6149f`, the build's head
  `pkg-ddl-a30a8d60` (not for `ctas`) and the delta's head `pkg-ddl-8efe8e69`; `bar-classes.txt`, the class table of
  step 0's bar on the delta's head; `pins/`, the pin file's runs on the delta's head and on main.
- `delta2/` — delta 2's data on `pkg-ddl-5b34022f`: `cells-<package>.json` (the new `pmod` and runtime `SET` pin
  cells on main, delta 1's head and delta 2's head; Spark's are the pin oracle) and `compare.txt` (each against
  Spark); `rem-*` and `card-*` (delta 1's probes on the head) with `rem-moves.txt`, `card-moves.txt` and
  `rem-table.txt`; `bar-moves.txt` (step 0's raw cells against delta 1's head: none moved); `explain-pmod.txt` (the
  plan of `pmod(d10, 3)`, `pmod(-3, d10)` and `F.pmod`); `pins/`; `cost/`, delta 2's cost series.

Up: [../map.md](../map.md).
