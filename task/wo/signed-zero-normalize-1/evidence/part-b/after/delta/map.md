# task/wo/signed-zero-normalize-1/evidence/part-b/after/delta/ — part B part 1, the verifier delta round

- [delta.md](delta.md) — items D1 to D8 and the gate table: the configuration-key inventory and the refused `SET`s, the `dropDuplicates`
  and `collect_set` measurements and dispositions, the pin changes, the records and the prepared cost run.
- [mkdata.py](mkdata.py) — writes the probe's parquet rows (`-0.0` and `-NaN` first, NaN payloads, FLOAT and DOUBLE;
  the `collect_set` groups over one file and four files).
- [measure.py](measure.py) — the `dropDuplicates`, `distinct`, `writeTo` and `collect_set` cells on Spark or RePark.
- [legs-spark.jsonl](legs-spark.jsonl), [legs-head-66fb7f13.jsonl](legs-head-66fb7f13.jsonl),
  [legs-276dc503.jsonl](legs-276dc503.jsonl) — its legs (Spark under `local[1]` and `local[4]`).

Up: [../map.md](../map.md).
