# Card COLLECTSET-NAN-ELEMENTS-1: DataFrame `F.collect_set` keeps one NaN where Spark keeps every NaN row

**Date:** 2026-11-13. **Filed by:** the lane-1 builder (`claude-opus-5-5`), SIGNED-ZERO-NORMALIZE-1 part B part 1,
delta round item D3 (verifier 2, finding F3).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**. Introduced by part 1: before it, RePark kept one
element per NaN bit pattern.

**Measured** (PySpark 4.1.2 against RePark, 2026-11-13,
[legs-spark.jsonl](../../wo/signed-zero-normalize-1/evidence/part-b/after/delta/legs-spark.jsonl)):

| input to `collect_set` | Spark 4.1.2 | RePark head | RePark `276dc503` |
|---|---|---|---|
| two canonical NaNs | 2 | 1 | 1 |
| canonical NaN, `-NaN` | 2 | 1 | 2 |
| NaN payloads 1 and 2, canonical NaN | 3 | 1 | 3 |
| `-0.0, 0.0, -NaN, NaN, NaN(payload 1), 1.5` | 5 | 3 | 6 |

Spark compares NaNs by IEEE inside `collect_set`, so no NaN row merges with another, while both zeros merge. Its
answer is the same for the DataFrame and SQL forms, under `local[1]` and `local[4]`, over one file and four files,
and after `repartition(4)`.

**Declared:** registry row `SIGNED-ZERO-KEYS-1-COLLECTSET-NAN` in `docs/spark-sql-iceberg-parity.md`; the pin
`test_group_agg.py::test_collect_set_keeps_one_zero_and_its_sign_is_the_declared_residue` asserts RePark's
declared answer (one canonical NaN).

**Close:** `F.collect_set` (both doors) keeps one element per NaN row and one element for both zeros, as Spark
does, with a pin over the four inputs above. The SQL door's zero half is `COLLECTION-FLOAT-EQ-1`.
