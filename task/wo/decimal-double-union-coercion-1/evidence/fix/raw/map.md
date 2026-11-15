# task/wo/decimal-double-union-coercion-1/evidence/fix/raw/ — the build's raw sets

One JSON object per cell or sample.

- `named-<spark|main|head>-ansi-*.jsonl` are the named cells from `../probes/named.py`.
- `cards-<spark|main|head>-ansi-*.jsonl` are the new cards' cells from `../probes/cards.py`.
- `dml-head-<repark|native>-ansi-*.jsonl` and `fexpr-head-ansi-*.jsonl` are step 0's DML and `F.expr` cells on the
  head, beside step 0′'s main and Spark sets in [../../step0b/raw/](../../step0b/raw/map.md).
- `preimage-<spark|head|head-delta>-ansi-*.jsonl` are the preimage cells from `../probes/preimage_cells.py`: Spark 4.1.2, the part-1 head before the delta, and the delta build.
- `delta-cost-samples.jsonl` and `delta-cost-raw.jsonl` are the delta's cost runner output (the "Delta, 2026-11-15" section of [../cost.md](../cost.md)).
- `cost-samples.jsonl` and `cost-raw.jsonl` are the cost runner's wall times and the in-process timings.

Up: [../map.md](../map.md).
