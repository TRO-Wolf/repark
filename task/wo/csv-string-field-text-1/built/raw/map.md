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
- `delta2-verifier-table.md.txt` — the round-2 verifier's `mine.py` and V1's `probe.py` and `probe2.py` (816
  records) on Spark 4.1.2 (re-run), the base, the round-2 head `35bc6e3a` (the verifier's run) and delta 2's head
  `ef5abef1`: the counts, the EQUAL cells lost and the moves.
- `delta2-callers-spark.jsonl`, `delta2-callers-base.jsonl`, `delta2-callers-before.jsonl`,
  `delta2-callers-after.jsonl` and `delta2-callers-table.md.txt` — `probes/delta2_callers.py` (every caller of
  `java_case`'s lowering, 72 cells) on Spark, the base, `35bc6e3a` and `ef5abef1`, and the verdict table.
- `delta2-java-lower.txt` — the 76 strings of `java_case.rs`'s final-sigma test
  with Zulu 17's `String.toLowerCase()` answers, from `probes/JavaLower.java`.

Up: [../map.md](../map.md).
