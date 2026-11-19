# task/wo/csv-schema-timestamp-zone-1/evidence/raw/ — cells, verdicts, plans, pins, cost samples

- `cells-verdicts.jsonl` — step 0's 5,732 cells: Spark's, main's and the head's answers, `v_main`, `v_head`,
  `moved`, the gap class and the emulation twins (`cmp_h.py`); `cells-summary.txt`, `tables.txt`, `bar.txt` (`bar.py`).
- `main-E.jsonl`, `head-E.jsonl` — step 0's 16 `EXPLAIN` cells.
- `spark-X.jsonl`, `main-X.jsonl`, `head-X.jsonl`, `xcmp.txt` — the extra shapes; `emu_ab-main.json` — the seat choice.
- `readers-main.jsonl`, `readers-head.jsonl` — the reader sweep; `wsweep-cmp.txt` — the write sweep's comparison.
- `lmt.txt` — the LMT offset-seconds read on Spark, main and the head.
- `pins-main.log`, `pins-head.log` — the pin file on main (red) and on the head package (green).
- `cost-samples.jsonl`, `cost-raw-csv.jsonl` — every cost sample ([../../cost.md](../../cost.md)).
- `delta1-cells.jsonl` — `delta1.py`'s 84 cells on Spark, main, the old head `186c34b0`, the intermediate
  `0b45f6a6` (every `utf8_columns` read named-only) and the delta 1 head `1ee847b7`; row lists over 12 rows kept as a
  count, a hash and the late row.
- `delta1-bar.txt` — delta 1's bar: the verifier's probes, the reader sweep, `x.py`, `delta1.py` and step 0's table,
  each on main, the old head and the delta 1 head, with every moved cell; the write sweep's comparison.
- `delta2-bar.txt` — delta 2's bar: every bar probe's records on the delta 2 package against delta 1's, cell by
  cell (0 differ).
- `delta2-diag.txt` — where delta 1's cost went: the one-process A/B tables, the K2 twin on seven builds and the
  `EXPLAIN ANALYZE` metrics (cost.md "Delta 2").
- `delta2-cost-samples.jsonl`, `delta2-cost-raw-csv.jsonl` — delta 1's runner on the delta 2 package;
  `delta2-costiso-raw.jsonl` — the side-by-side series (main, the first head, delta 1, delta 2).

Up: [../map.md](../map.md).
