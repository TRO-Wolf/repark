# task/wo/decimal-double-union-coercion-1/evidence/part2-step0/raw/ — the raw sets

Each `p2-*.jsonl` file opens with a `provenance` row naming the engine and its loaded library, relative to the
scratchpad.

- `p2-spark-ansi-*.jsonl`, `p2-main-ansi-*.jsonl` and `p2-dc6b-ansi-*.jsonl` are `p2_cells.py`'s rows on Spark,
  main `a06c1088` and main `dc6b4c94`.
- `in-cost-run1.jsonl` to `in-cost-run3.jsonl` are `in_cost.py`'s rows, one file per alternating run.
- `rule-lists-a06c.json` is main's live analyzer and optimizer lists on both doors, from `../../probes/rule_lists.py`.

Up: [../map.md](../map.md).
