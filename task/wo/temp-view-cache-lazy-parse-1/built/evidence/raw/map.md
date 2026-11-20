# task/wo/temp-view-cache-lazy-parse-1/built/evidence/raw/ — A1's recordings and verdicts

- `head.jsonl.gz`, `m-head.jsonl.gz`, [child-head.jsonl](child-head.jsonl), `f8-head.jsonl.gz` — the head's answers
  to step 0's cells, the revision round's, Fable's ten and the `writeTo` cells.
- `f8-spark.jsonl.gz`, `f8-main.jsonl.gz` — Spark's and main's answers to the `writeTo` cells.
- `pin-spark.json.gz`, `pin-main.json.gz`, `pin-head.json.gz` — the pin file's 56 cells per engine (Spark's is the
  committed oracle).
- `verdicts.jsonl.gz`, [summary.txt](summary.txt) — `bar_cmp.py`'s verdict per cell and its counts (the `M` group's
  counts here use this rule; [../../bar.md](../../bar.md) reports them by step 0's verdicts).
- `readers-main.jsonl.gz`, `readers-head.jsonl.gz` — the reader sweep on main and the head.

Up: [../map.md](../map.md).
