# map — python/repark/tests/resolution

## Purpose

Column and attribute resolution: attribute ids and self-joins, duplicate names, case sensitivity, alias qualifiers, temp-view semantics and the attribute-stamp handle.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_attr_id_1_s2.py](test_attr_id_1_s2.py) — ATTR-ID-1 S2: the seam pins.
- [test_attr_id_1_s3a.py](test_attr_id_1_s3a.py) — ATTR-ID-1 S3A: the cutover pins, each under both case rules unless noted.
- [test_attr_id_1_s3b.py](test_attr_id_1_s3b.py) — ATTR-ID-1 S3B: the filter/sort cutover pins, each under both case rules unless noted.
- [test_attr_id_1_s3c.py](test_attr_id_1_s3c.py) — ATTR-ID-1 S3C: the `withColumn(s)`/`withColumn(s)Renamed` cutover pins, each under both case rules unless noted.
- [test_attr_id_1_s3d.py](test_attr_id_1_s3d.py) — ATTR-ID-1 S3D: the `drop`/`dropDuplicates`/`fillna`/`dropna` cutover pins, each under both case rules unless noted.
- [test_attr_id_1_s3e.py](test_attr_id_1_s3e.py) — ATTR-ID-1 S3E: the qualified-name cutover pins, each under both case rules unless noted.
- [test_attr_id_1_s4.py](test_attr_id_1_s4.py) — ATTR-ID-1 S4: the deletion pins, each under both case rules unless noted.
- [test_attr_id_1_sj2.py](test_attr_id_1_sj2.py) — ATTR-ID-1 SJ2: the seam pins.
- [test_attr_id_1_sj3.py](test_attr_id_1_sj3.py) — ATTR-ID-1 SJ3: the join-condition-path pins, each named after its probe cell and equal to live Spark 4.1.2 under both case rules.
- [test_attr_id_1_sj4.py](test_attr_id_1_sj4.py) — ATTR-ID-1 SJ4: the post-join detector pins, each named after its cell and equal to live Spark 4.1.2, plus the uncovered-surface shapes and the strict xfails the root map records.
- [test_attr_id_1_sj5.py](test_attr_id_1_sj5.py) — ATTR-ID-1 SJ5: the cross-join pins equal to live Spark 4.1.2 under both case rules, plus the over-fire guards.
- [test_attr_id_1_sm2_dupexport.py](test_attr_id_1_sm2_dupexport.py) — ATTR-ID-1 SM2: Arrow export of a duplicate-display-name join carries the display names, equal to Spark's `arrow_dup`.
- [test_attr_id_1_sm2_duppandas.py](test_attr_id_1_sm2_duppandas.py) — ATTR-ID-1 SM2: `mapInPandas` / `mapInArrow` over duplicate-display frames refuse `AMBIGUOUS_REFERENCE` with Spark's exact text.
- [test_attr_id_1_sm2_dupviews.py](test_attr_id_1_sm2_dupviews.py) — ATTR-ID-1 SM2: the four temp-view doors over exact-duplicate display frames refuse `COLUMN_ALREADY_EXISTS` and register nothing.
- [test_attr_id_1_sm2_dupwrites.py](test_attr_id_1_sm2_dupwrites.py) — ATTR-ID-1 SM2: duplicate-display-name writes refuse `COLUMN_ALREADY_EXISTS` and create nothing; `insertInto` writes positionally like Spark.
- [test_attr_id_1_sm2_r1.py](test_attr_id_1_sm2_r1.py) — ATTR-ID-1 SM2_R1: the nested `WRITE ORDERED BY` pin.
- [test_attr_id_1_sm2_r6.py](test_attr_id_1_sm2_r6.py) — ATTR-ID-1 SM2 R6: DataFrame `USING` joins keep the left key physically; left-side references answer Spark-exact on every join type.
- [test_attr_id_1_sm2_r7.py](test_attr_id_1_sm2_r7.py) — ATTR-ID-1 SM2 R7: a qualified `DataFrame.__getitem__` repr keeps the qualifier, equal to Spark 4.1.2.
- [test_attr_id_1_sm2c_leak.py](test_attr_id_1_sm2c_leak.py) — ATTR-ID-1 SM2c: a frame with unique display names over twin engine fields writes the display names on every write door.
- [test_attr_id_1_v456.py](test_attr_id_1_v456.py) — ATTR-ID-1 V456: the verifier-fold twin pins, measured against live Spark 4.1.2 `p5_twins`.
- [test_attr_view_semantics_1.py](test_attr_view_semantics_1.py) — A SQL temp view over a stamped frame answers Spark on the parquet-write and groupBy-count doors and their in-scope siblings.
- [test_case_insensitive_conform.py](test_case_insensitive_conform.py) — Case-insensitive by-name column conform through the real facade (WG-4, BUG-007).
- [test_casesens_1.py](test_casesens_1.py) — WO CASESENS-1 slice 1: nested scopes keep the written spelling; MERGE folds its source.
- [test_casesens_2.py](test_casesens_2.py) — WO CASESENS-2 slice 1: bare DataFrame names resolve in Rust.
- [test_cs2p1_alias_qualifier.py](test_cs2p1_alias_qualifier.py) — R-CS2P-1: DataFrame-alias qualifiers keep their case and bind under both case rules.
- [test_fa_5_duplicate_csv.py](test_fa_5_duplicate_csv.py) — FA-5: CSV writes of frames with exact-duplicate display names match the live Spark 4.1.2 cells (header, rows, options).
- [test_perf_attr_stamp_2_o1.py](test_perf_attr_stamp_2_o1.py) — STAMP-2 O1: the facade pins for the per-handle executable twin.
- [test_stamp_2_r5p6_1.py](test_stamp_2_r5p6_1.py) — STAMP-2 R5P6: `fillna` over twin, star, appended, union and self-join frames answers the same on the native route as on the forced SQL route.
- [test_stamp_2_r5p6_2.py](test_stamp_2_r5p6_2.py) — STAMP-2 R5P6: the H1 join door's differential pins.
- [test_u11_edge_case_select.py](test_u11_edge_case_select.py) — E-CASE-SELECT facade pins: SELECT output columns keep the query spelling.
- [_sm2_shared.py](_sm2_shared.py) — Session/join builders plus refusal-shape and no-`__repark_`-bytes helpers for the fold's pin files.
- [casesens_1_spark_oracle.json](casesens_1_spark_oracle.json) — Recorded Spark oracle read by `test_casesens_1.py`, `test_casesens_2.py`.
- [casesens_2_spark_oracle.json](casesens_2_spark_oracle.json) — Recorded Spark oracle read by `test_casesens_2.py`.

## Pointers

- Up: [../map.md](../map.md)
