# task/wo/csv-string-field-text-1/built/raw/ — the wiring unit's raw cells (scratch paths normalised to `<tmp>`)

- `pins-spark.jsonl`, `pins-main.jsonl`, `pins-head.jsonl` — `probes/pins.py` on Spark 4.1.2, main's package
  (`9a48932f`) and the head's (`3bde1153`), both ANSI modes.
- `s3-spark.jsonl`, `s3-main.jsonl`, `s3-head.jsonl` — `probes/s3.py`: Spark over byte-identical local files, and
  each package over moto.
- `shapes-spark.jsonl`, `shapes-main.jsonl`, `shapes-head.jsonl` — `probes/shapes.py`.
- `rerun-base.jsonl.gz`, `rerun-head.jsonl.gz` — step 0's domain, revision and kernel probes on each package
  (13,050 records each); `rerun-verdicts.jsonl.gz` and `rerun-table.md.txt` — `probes/rerun.py`'s verdicts and table.
- `cost-samples.jsonl`, `cost-perf.txt` — the short-row cost series ([../cost.md](../cost.md)).

Up: [../map.md](../map.md).
