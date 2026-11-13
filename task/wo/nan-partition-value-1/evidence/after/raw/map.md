# task/wo/nan-partition-value-1/evidence/after/raw/ — the after-fix raw legs

- `pin-red-a3f8c7d9.txt`, `pin-green-945d3326.txt` — the result lines of `test_nan_partition_value_1.py` with `REPARK_PARITY_LIVE=1`, on main's release module and on the fixed release module; the first line names the module.
- `repark-fixed-writes.jsonl`, `repark-fixed-reads-spark.jsonl`, `manifests-repark-fixed.jsonl` — `../../step0/probe/` re-run on the fixed build (fork `0ff9f106`, the same transport code as `945d3326` before the functions moved into `DataFileSerde`).
- `spark-own.jsonl`, `spark-reads-repark-fixed.jsonl` — one Spark leg: its own tables (the oracle the table compares against) and RePark's fixed tables.

Up: [../map.md](../map.md).
