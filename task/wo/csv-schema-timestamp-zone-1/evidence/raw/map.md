# task/wo/csv-schema-timestamp-zone-1/evidence/raw/ — cells, verdicts, plans, pins, cost samples

- `cells-verdicts.jsonl` — step 0's 5,732 cells: Spark's, main's and the head's answers, `v_main`, `v_head`,
  `moved`, the gap class and the emulation twins (`cmp_h.py`); `cells-summary.txt`, `tables.txt`, `bar.txt` (`bar.py`).
- `main-E.jsonl`, `head-E.jsonl` — step 0's 16 `EXPLAIN` cells.
- `spark-X.jsonl`, `main-X.jsonl`, `head-X.jsonl`, `xcmp.txt` — the extra shapes; `emu_ab-main.json` — the seat choice.
- `readers-main.jsonl`, `readers-head.jsonl` — the reader sweep; `wsweep-cmp.txt` — the write sweep's comparison.
- `pins-main.log`, `pins-head.log` — the pin file on main (red) and on the head package (green).
- `cost-samples.jsonl`, `cost-raw-csv.jsonl` — every cost sample ([../../cost.md](../../cost.md)).

Up: [../map.md](../map.md).
