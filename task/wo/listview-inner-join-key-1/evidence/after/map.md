# task/wo/listview-inner-join-key-1/evidence/after/

Build item 3 (`claude-opus-5-5`, 2026-11-11): the item-1 table re-run on the item-2 head, the revert record, the struct units' pins and the cost.

- [moves.md](moves.md) — the record: the moves against ruling 1's prediction, the halt checks, the revert record (each seat on its own) and the re-run of the pins of #1283, #1290, #1292 and #1295 in both facade profiles.
- [moves.py](moves.py) — labels base and head against the Spark leg with the probe's own `label_of` and checks every move against the prediction; `python3 -I moves.py` reads `../item1/raw/` and `raw/head/`.
- [table.md](table.md) — the head leg rendered by `../item1/probe_item1.py --render head`.
- [cost.md](cost.md) — ruling 8: the plain List key-name join, base against head, median of three, and the cast path beside it.
- [cost_probe.py](cost_probe.py) — the cost probe: one case per process (`--case execute|build`, `--kind list|listview`); `--report` prints the medians of its JSON lines.
- `raw/head/` — the head leg, one JSONL file per section after a meta line (`plan.jsonl` trimmed to item 1's committed shape).
- `cost-raw.jsonl` — the cost probe's records, in the order they ran.

Up: [../map.md](../map.md).
