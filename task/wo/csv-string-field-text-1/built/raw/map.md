# task/wo/csv-string-field-text-1/built/raw/ — the wiring unit's raw cells (scratch paths normalised to `<tmp>`)

- `pins-spark.jsonl`, `pins-main.jsonl`, `pins-head.jsonl` — `probes/pins.py` on Spark 4.1.2, main's package
  (`9a48932f`) and the head's (`3bde1153`), both ANSI modes.
- `s3-spark.jsonl`, `s3-main.jsonl`, `s3-head.jsonl` — `probes/s3.py`: Spark over byte-identical local files, and
  each package over moto.
- `shapes-spark.jsonl`, `shapes-main.jsonl`, `shapes-head.jsonl` — `probes/shapes.py`.
- `rerun-base.jsonl.gz`, `rerun-head.jsonl.gz` — step 0's domain, revision and kernel probes on each package
  (13,050 records each); `rerun-verdicts.jsonl.gz` and `rerun-table.md.txt` — `probes/rerun.py`'s verdicts and table.
- `cost-samples.jsonl`, `cost-perf.txt` — round 0's short-row cost series, ten rounds ([../cost.md](../cost.md));
  `c012-samples.jsonl` — round 0's C-012 series (ten runs).
- `fix1-spark.jsonl`, `fix1-base.jsonl`, `fix1-head.jsonl` — `probes/fix1.py` on Spark 4.1.2, main's package and
  fix round 1's head (`8ac58878`), both ANSI modes, each cell under the `spark.sql.caseSensitive` it records.
- `fix1-verifier-table.md.txt` — the wiring verifier's `probe.py` and `probe2.py` (448 records) re-run on main's
  package and fix round 1's head against Spark's recorded answers: the tables, the moves and every cell not
  EQUAL on the head.
- `fix1-cost-samples.jsonl`, `fix1-cost-perf.txt`, `fix1-c012-samples.jsonl` — fix round 1's cost series.

Up: [../map.md](../map.md).
