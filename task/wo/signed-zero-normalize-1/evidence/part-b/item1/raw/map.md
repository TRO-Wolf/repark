# task/wo/signed-zero-normalize-1/evidence/part-b/item1/raw/ — item 1's raw legs

One JSONL file per leg; the first line is the leg header. `repark-276dc503-*` is main `276dc503`'s release build.
`spark-*` is the Spark 4.1.2 oracle.

- `repark-276dc503-reads.jsonl`, `repark-276dc503-dml.jsonl`, `repark-276dc503-floatkeys.jsonl` — step 0's three parts re-run (`../../step0/run_leg.sh`).
- `repark-276dc503-spill.jsonl`, `spark-spill.jsonl` — D16 (`item1_probe.py --part spill`; RePark with `--memory 96M --partitions 4`, Spark with `--partitions 4`).
- `repark-276dc503-explain.jsonl`, `spark-explain.jsonl` — D14 plan shapes, `Distinct::On` and the FLOAT-to-DOUBLE join (`--part explain`).

Up: [../map.md](../map.md).
