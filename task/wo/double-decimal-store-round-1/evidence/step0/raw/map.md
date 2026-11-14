# task/wo/double-decimal-store-round-1/evidence/step0/raw/ — one JSON object per cell

Written by [../probes/run.py](../probes/run.py); each file's first line is its `provenance` row (engine,
mode, warehouse name, and for RePark the loaded `_native.abi3.so` relative to the scratchpad).

- spark-ansi-true.jsonl, spark-ansi-false.jsonl, repark-ansi-true.jsonl, repark-ansi-false.jsonl — the
  twelve brief doors, the source readbacks and the card cells.
- extra-spark-ansi-true.jsonl, extra-spark-ansi-false.jsonl, extra-repark-ansi-true.jsonl,
  extra-repark-ansi-false.jsonl — the four doors the inventory added (INSERT OVERWRITE, static-partition
  INSERT OVERWRITE, `insertInto`, `overwritePartitions`).
- selects-spark-ansi-true.jsonl, selects-spark-ansi-false.jsonl, selects-repark-ansi-true.jsonl,
  selects-repark-ansi-false.jsonl — the container, subscript, `TRY_CAST` and literal select cells.

Up: [../map.md](../map.md).
