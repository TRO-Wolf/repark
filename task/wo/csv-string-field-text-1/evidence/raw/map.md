# task/wo/csv-string-field-text-1/evidence/raw/ — cells and verdicts

- `cells-spark.jsonl.gz`, `cells-main.jsonl.gz`, `cells-unit1.jsonl.gz` — every `domain.py` record (7,704 cells
  each, and RePark's 14 plans).
- `verdicts.jsonl.gz`, `summary.txt`, `tables.md.txt` — `classify.py` and `tables.py` over them.
- `emu-cells.jsonl.gz`, `emu-verdicts.jsonl.gz`, `emu-summary.txt` — the two seat emulations on unit 1's package.
- `kernel-cells.jsonl`, `kernel-summary.txt` — `kernel.py`, Spark and unit 1.
- `castrule-cells.jsonl` — `castrule.py`, both engines.
- `cost-check.jsonl` — the one functional sample per cost case and variant (shared lock; superseded by the series).
- `cost-samples.jsonl`, `cost-table.md.txt` — the five-round exclusive series and its medians.
- `revision-cells-spark.jsonl.gz`, `revision-cells-main.jsonl.gz`, `revision-cells-unit1.jsonl.gz`,
  `revision-tables.md.txt` — `revision.py`'s cells and `rev_tables.py`'s tables.

Up: [../map.md](../map.md).
