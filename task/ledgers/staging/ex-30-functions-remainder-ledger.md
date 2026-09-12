# Unit ledger — EX-30 · v1.1 example backfill, the `F.*` remainder (99 names)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-30 merges, or when the owner closes the
slate row.

**Unit:** EX-30 · **Date:** 2026-09-11 · **Model:** swe-2-high · **Branch:** `docs/ex-30-functions-remainder` · **Base:** `f413241b` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-30 lane card (99 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_functions_b.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other
ledger, `briefs/next-sequence.md`.

## Scope

The roster is the 99 `F.*` backlog names of the lane card, re-derived at the base
`f413241b` (all 99 present on `docs/examples/backlog.txt`; C-001 records the exact list).
The oracle is live PySpark 4.1.2 (ANSI on, UTC session zone, zulu-17 JVM,
`PYSPARK_PYTHON` pinned to the lane venv so the pandas arm measures): every asserted
value in every shipped example was measured there first, then matched on repark. Nine
names are covered by two new example scripts. Eighty-nine stay on the backlog — 87
carrying rows their filing batches already wrote (EX-FN-2..20, BL-17,
FNP9-ARRAYS-ZIP-NAMES-1, FNP9-GENERATORS-1, FNP-15 ×6, FNP-16 ×4 families), each
re-measured against the oracle this round, plus `from_xml` / `schema_of_xml` under the
new EX-FN-22. `F.PythonUDFColumn` is measured absent from `pyspark.sql.functions` and
`pyspark.sql.column` on the oracle — a repark-only marker class — and stays pending the
owner ruling it raises (D-4). `F.udf`'s and `F.pandas_udf`'s factory return-type arm is a
measured divergence on covered names: recorded as EX-FN-23 (BACKLOG ARM). No product
file is touched.

**Roster (99):** `F.PandasUDFType`, `F.PythonUDFColumn`, `F.UserDefinedFunction`,
`F.UserDefinedTableFunction`, `F.arrays_zip`, `F.base64`, `F.bitmap_bit_position`,
`F.bitmap_bucket_number`, `F.bitmap_count`, `F.decode`, `F.encode`, `F.expr`,
`F.format_number`, `F.from_csv`, `F.from_xml`, `F.hash`, `F.hll_sketch_agg`,
`F.hll_sketch_estimate`, `F.hll_union`, `F.hll_union_agg`, `F.input_file_block_length`,
`F.input_file_block_start`, `F.input_file_name`, `F.is_variant_null`, `F.java_method`,
`F.json_tuple`, `F.kll_merge_agg_bigint`, `F.kll_merge_agg_double`,
`F.kll_merge_agg_float`, `F.kll_sketch_agg_bigint`, `F.kll_sketch_agg_double`,
`F.kll_sketch_agg_float`, `F.kll_sketch_get_n_bigint`, `F.kll_sketch_get_n_double`,
`F.kll_sketch_get_n_float`, `F.kll_sketch_get_quantile_bigint`,
`F.kll_sketch_get_quantile_double`, `F.kll_sketch_get_quantile_float`,
`F.kll_sketch_get_rank_bigint`, `F.kll_sketch_get_rank_double`,
`F.kll_sketch_get_rank_float`, `F.kll_sketch_merge_bigint`, `F.kll_sketch_merge_double`,
`F.kll_sketch_merge_float`, `F.kll_sketch_to_string_bigint`,
`F.kll_sketch_to_string_double`, `F.kll_sketch_to_string_float`, `F.kurtosis`,
`F.make_timestamp`, `F.mode`, `F.monotonically_increasing_id`, `F.months_between`,
`F.pandas_udf`, `F.parse_json`, `F.posexplode`, `F.posexplode_outer`, `F.raise_error`,
`F.reflect`, `F.replace`, `F.schema_of_csv`, `F.schema_of_variant`,
`F.schema_of_variant_agg`, `F.schema_of_xml`, `F.sentences`, `F.skewness`,
`F.spark_partition_id`, `F.split`, `F.st_asbinary`, `F.st_geogfromwkb`,
`F.st_geomfromwkb`, `F.st_setsrid`, `F.st_srid`, `F.theta_difference`,
`F.theta_intersection`, `F.theta_intersection_agg`, `F.theta_sketch_agg`,
`F.theta_sketch_estimate`, `F.theta_union`, `F.theta_union_agg`, `F.to_csv`,
`F.to_variant_object`, `F.to_xml`, `F.try_parse_json`, `F.try_reflect`,
`F.try_to_timestamp`, `F.try_variant_get`, `F.udf`, `F.udtf`, `F.unwrap_udt`,
`F.variant_get`, `F.xpath`, `F.xpath_boolean`, `F.xpath_double`, `F.xpath_float`,
`F.xpath_int`, `F.xpath_long`, `F.xpath_number`, `F.xpath_short`, `F.xpath_string`.

**Grouping (2 new scripts):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `functions/bitmap.py` | `F.bitmap_bit_position`, `F.bitmap_bucket_number`, `F.bitmap_count` | The bitmap cell split: bucket and bit-position arms over negative, zero, both 32768 bucket edges and NULL, then popcount over raw binaries — all Spark-equal. |
| `functions/udf.py` | `F.udf`, `F.pandas_udf`, `F.udtf`, `F.UserDefinedFunction`, `F.UserDefinedTableFunction`, `F.PandasUDFType` | The Python-UDF door: scalar factory, direct `UserDefinedFunction` construction, `pandas_udf` at `PandasUDFType.SCALAR`, and an `@udtf` table function — all value arms Spark-equal. |

**Stay rows (89 names):**

| Names | Row | Pin |
|---|---|---|
| `F.arrays_zip` | FNP9-ARRAYS-ZIP-NAMES-1 | `test_arrays_zip_names_its_fields_by_position` |
| `F.base64` | BL-17 | `test_bl17_base64_omits_rfc4648_padding_today` |
| `F.decode`, `F.encode` | EX-FN-3 | `test_encode_decode_charset_refuses` |
| `F.expr` | EX-FN-4 | `test_expr_column_reference_refuses` |
| `F.format_number` | EX-FN-5 | `test_format_number_refuses` |
| `F.from_csv` | EX-FN-6 | `test_from_csv_refuses` |
| `F.from_xml`, `F.schema_of_xml` | **EX-FN-22 (new)** | `test_from_xml_refuses`, `test_schema_of_xml_refuses` |
| `F.hash` | EX-FN-7 | `test_hash_refuses` |
| `F.hll_*` (4), `F.theta_*` (7), `F.kll_*` (21) | FNP-16-sketches | `test_sketch_facade_refuses_deferred_by_cost` (+ sql/expr doors) |
| `F.input_file_block_length`, `F.input_file_block_start` | FNP-15-input_file_block_* | `test_facade_attribute_refuses_with_registry_reason[…]` (+ doors) |
| `F.input_file_name` | EX-FN-13 | `test_input_file_name_refuses` |
| `F.is_variant_null`, `F.parse_json`, `F.try_parse_json`, `F.schema_of_variant`, `F.schema_of_variant_agg`, `F.to_variant_object`, `F.try_variant_get`, `F.variant_get` | FNP-16-variant | `test_variant_facade_refuses_deferred_by_cost` (+ doors) |
| `F.java_method` | FNP-15-java_method | `test_facade_attribute_refuses_with_registry_reason[java_method]` (+ doors) |
| `F.json_tuple` | EX-FN-8, FNP9-GENERATORS-1 | `test_json_tuple_refuses`, `test_json_tuple_still_refuses_on_the_facade` |
| `F.kurtosis`, `F.mode`, `F.skewness` | EX-FN-9 | `test_moment_aggregates_refuse` |
| `F.make_timestamp` | EX-FN-10 | `test_make_timestamp_refuses` |
| `F.monotonically_increasing_id`, `F.spark_partition_id` | EX-FN-12 | `test_single_node_ids_refuse` |
| `F.months_between` | EX-FN-11 | `test_months_between_refuses` |
| `F.posexplode`, `F.posexplode_outer` | EX-FN-2, FNP9-GENERATORS-1 | `test_posexplode_pair_refuses` |
| `F.raise_error` | EX-FN-14 | `test_raise_error_refuses` |
| `F.reflect` | FNP-15-reflect | `test_facade_attribute_refuses_with_registry_reason[reflect]` (+ doors) |
| `F.replace` | EX-FN-15 | `test_replace_lit_spelling_refuses`, `test_replace_dollar_arm_answers_backslash` |
| `F.schema_of_csv` | EX-FN-16 | `test_schema_of_csv_refuses` |
| `F.sentences` | EX-FN-17 | `test_sentences_refuses` |
| `F.split` | EX-FN-18 | `test_split_refuses` |
| `F.st_*` (5) | FNP-16-geospatial | `test_geospatial_facade_refuses_deferred_by_cost` (+ doors) |
| `F.to_csv`, `F.to_xml`, `F.xpath`, `F.xpath_boolean`, `F.xpath_double`, `F.xpath_float`, `F.xpath_int`, `F.xpath_long`, `F.xpath_number`, `F.xpath_short`, `F.xpath_string` | FNP-16-csv-xml-xpath | `test_csv_xml_xpath_facade_refuses_deferred_by_cost` (+ doors) |
| `F.try_reflect` | FNP-15-try_reflect | `test_facade_attribute_refuses_with_registry_reason[try_reflect]` (+ doors) |
| `F.try_to_timestamp` | EX-FN-20 | `test_try_to_timestamp_refuses` |
| `F.unwrap_udt` | FNP-15-unwrap_udt | `test_facade_attribute_refuses_with_registry_reason[unwrap_udt]` (+ doors) |

**For the owner (1 name, no §7 row):** `F.PythonUDFColumn` — measured absent from
`pyspark.sql.functions` and `pyspark.sql.column` on the 4.1.2 oracle; on repark it is
the marker class `F.udf(f)("n")` returns (a `PythonUDFColumn`, not a SQL-plan
`Column`). It stays on the backlog as an inventory-narrowing candidate (Q-1).

**Covered names' divergent arm (recorded, not stayed):** `F.udf` / `F.pandas_udf`
factory return type — EX-FN-23.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 99-name roster above is exactly the lane card's list, every name re-derived from `docs/examples/backlog.txt` at the base `f413241b`; 9 names are covered by the two example units in the grouping table, 89 stay on the backlog with the §7 rows in the stay table (87 pre-existing, 2 under the new EX-FN-22), and `F.PythonUDFColumn` stays pending the owner ruling it raises. | The backlog grep at dispatch (all 99 present), the stay table, and the oracle table (99 rows, one per roster name). | **PROVEN** |
| C-002 | `functions/bitmap.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured `bitmap_bit_position` / `bitmap_bucket_number` cells over `[-1, 0, 1, 32767, 32768, 65536, NULL]` (`[1, 0, 0, 32766, 32767, 32767, None]` / `[0, 0, 1, 1, 1, 2, None]`) and `bitmap_count` popcount over raw binaries (`[19, 2, None]`); every `COVERS` name is used in the body. | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its three names. | **PROVEN** |
| C-003 | `functions/udf.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured `F.udf` / direct `F.UserDefinedFunction` / `F.pandas_udf` / `F.udtf` value arms (`u1..u3`, `w1..w3`, `2/4/6`, `(5,)/(6,)`) and the `isinstance(Pair, F.UserDefinedTableFunction)` cell (True on both engines); `F.PandasUDFType.SCALAR` is used as the `functionType` argument; every `COVERS` name is used in the body. | The shipped script (executed standalone and by the `--require-execute` gate) and the oracle rows for its six names. | **PROVEN** |
| C-004 | Every stayed name's §7 row re-measured accurate on live PySpark 4.1.2 (ANSI on, UTC, 2026-09-11) and on repark in this clone: refusals still refuse with the recorded classes and texts, divergent cells (`base64` padding, `arrays_zip` field names, `replace` spelling class) still diverge as recorded. | The oracle table below (99 rows); no row text edits were needed — every recorded repark cell matched the re-measurement. | **PROVEN** |
| C-005 | The two new §7 rows carry pins: EX-FN-22 (`from_xml` / `schema_of_xml` E1 refusals) by `test_from_xml_refuses` + `test_schema_of_xml_refuses`, and EX-FN-23 (the `udf` / `pandas_udf` factory return-type arm on covered names) by `test_udf_factories_answer_typed_wrappers`; `test_examples_functions_b.py` runs green (5 tests). | The pin file's green run and the two registry rows. | **PROVEN** |
| C-006 | The 9 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 9, 128 → 119, with no other `scripts/` change; the backlog delta is exactly the covered set; the gate's static half and its `--require-execute` leg both exit 0 (`806 covered; 119 backlog; 2 exceptions; 214 examples` on this tree — measured before and after). | The gate's own counts line at the base `f413241b` (797/128/212, the unit's before-line) and on the shipped tree (806/119/214), plus the red-first provocation below. | **PROVEN** |
| C-007 | Red-first provocations run and pasted: (1) the nine covered names deleted from `backlog.txt` with their `COVERS` entries stripped reds the static gate with exactly 9 `has no example COVERS row` findings; (2) a wrong expected value injected into `bitmap.py` reds the `--require-execute` leg naming the script and both values. Both restore to 0. | The provocation evidence below. | **PROVEN** |

`LOGIC_SCORE` = **7/7 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Pin red-first.** Each new pin was first asserted as the Spark answer against repark
(`scratch/ex30/pin_redfirst.py` session, exit 1) — the divergence each pin codifies
fails red exactly:

```
RED-FIRST: from_xml refused where Spark answers Row(b=1)
RED-FIRST: schema_of_xml refused where Spark answers STRUCT<b: BIGINT>
RED-FIRST: F.udf answered UserDefinedFunction, not Spark's plain function
RED-FIRST: F.pandas_udf answered PandasUDFFunction, not Spark's plain function
exit=1
```

**P-1 — backlog ratchet.** The nine covered names removed from `docs/examples/backlog.txt`
while `COVERS` was stripped from `bitmap.py` / `udf.py` and `BACKLOG_BASELINE` stood at
119: the static gate exits **1** with exactly **9 findings**, every one
`public name F.<name> has no example COVERS row…` naming exactly `bitmap_bit_position`,
`bitmap_bucket_number`, `bitmap_count`, `pandas_udf`, `udf`, `udtf`, `PandasUDFType`,
`UserDefinedFunction`, `UserDefinedTableFunction`, and no other finding. Restoring the
`COVERS` rows returns the static gate to **0**.

**P-2 — execute-leg control.** A wrong expected value injected into
`functions/bitmap.py` (the bit arm `[1, 0, 0, 32766, 32767, 32767, None]` →
`[1, 0, 0, 32766, 32767, 32767, 9]`), `--require-execute` run, file restored. The leg
reds naming the script and both values:

```
example-coverage execute: 1 finding(s)
  example /tmp/dv-ex30/docs/examples/functions/bitmap.py exited 1: … F.bitmap_bit [1, 0, 0, 32766, 32767, 32767, None] != [1, 0, 0, 32766, 32767, 32767, 9]
exit=1
```

Restored bytes rerun `--require-execute` at exit 0 (the gates table below).

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-11)

One PySpark session (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `.venv/bin/python` as both
`PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` so the pandas arm measures,
`spark.sql.ansi.enabled=true`, `spark.sql.session.timeZone=UTC`, warehouse a tmpdir),
stopped before any gate ran. Repark answers probed on the same session shape
(`scratch/ex30/repark_probe*.py`, gitignored). `pandas` 3.0.5, `pyarrow` 25.0.0 and
`numpy` 2.5.1 are in the lane venv. For refused names one representative call is the
row; answered names carry their full measured cells.

| Name | Spark answer | repark answer | Disposition | Row |
|---|---|---|---|---|
| `F.PandasUDFType` | class, `SCALAR`/`GROUPED_MAP`/`GROUPED_AGG`/`SCALAR_ITER` = 200/201/202/204, no `MAP_ITER`/`COGROUPED_MAP` | identical member values, same two absents | covered (exercised as `functionType`) | `functions/udf.py` |
| `F.PythonUDFColumn` | absent from `pyspark.sql.functions` and `pyspark.sql.column` | marker class `F.udf(f)("n")` returns | stays — not PySpark API; reported for an owner ruling | none (Q-1) |
| `F.UserDefinedFunction` | `UserDefinedFunction(f, returnType="STRING")` answers `u1/u2/u3` on a frame | identical values, identical class name | covered | `functions/udf.py` |
| `F.UserDefinedTableFunction` | `@udtf` decorated class is one; `Explode(lit(5)).collect()` answers `(5,),(6,)` | identical | covered | `functions/udf.py` |
| `F.arrays_zip` | positional field names on expression args (`STRUCT<0,1>`), attribute-named on bare columns | positional on every arm | stays — field-name arm diverges | FNP9-ARRAYS-ZIP-NAMES-1 |
| `F.base64` | `U3Bhcms=`, `QQ==` padded | `U3Bhcms`, `QQ` unpadded | stays — padding divergence | BL-17 |
| `F.bitmap_bit_position` | `[-1,0,1,32767,32768,65536,NULL]` → `[1,0,0,32766,32767,32767,None]` | identical | covered | `functions/bitmap.py` |
| `F.bitmap_bucket_number` | same marks → `[0,0,1,1,1,2,None]` | identical | covered | `functions/bitmap.py` |
| `F.bitmap_count` | `b"Spark"`/`b"A"`/NULL → `[19,2,None]`; INT arm refuses `DATATYPE_MISMATCH` | identical values; INT arm refuses `AnalysisException` | covered | `functions/bitmap.py` |
| `F.decode` | UTF-8 round trip `Spark`/`A`/NULL | refuses: no charset codec | stays | EX-FN-3 |
| `F.encode` | `s` → bytes `a,b,c`/`x,y`/`''` | refuses: no charset codec | stays | EX-FN-3 |
| `F.expr` | `expr("1 + 1")` → 2; `expr("n + 1")` → `[2,3,4]` | literal arm agrees; column ref raises `AnalysisException` | stays | EX-FN-4 |
| `F.format_number` | `'12,332.12'` | refuses R-FN-BATCH3 | stays | EX-FN-5 |
| `F.from_csv` | `(1,'hello')`, `(2,None)`, NULL | refuses E1 | stays | EX-FN-6 |
| `F.from_xml` | `Row(b=1)` on `<a><b>1</b></a>` schema `b INT` | refuses E1 stub | stays | EX-FN-22 |
| `F.hash` | `-559580957`, `1765031574`, `42` | refuses R-FN-BATCH1 | stays | EX-FN-7 |
| `F.hll_sketch_agg`, `F.hll_sketch_estimate`, `F.hll_union`, `F.hll_union_agg` | DataSketches binary blobs / estimates (`hll_sketch_estimate` → 3) | refuses deferred-by-cost | stays | FNP-16-sketches |
| `F.input_file_block_length`, `F.input_file_block_start` | `-1` on a `createDataFrame` frame | refuses unreachable | stays | FNP-15-* |
| `F.input_file_name` | `''` on a `createDataFrame` frame | refuses R-FN-BATCH4 | stays | EX-FN-13 |
| `F.is_variant_null` | `False` | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.java_method` | resolves (probe arg-shape needed `lit`); documented `Math.abs(-5)` → `"5"` | refuses unreachable | stays | FNP-15-java_method |
| `F.json_tuple` | `('1','2')`, `(None,None)` ×2 | refuses E1 | stays | EX-FN-8 |
| `F.kll_*` (21 names) | KLL sketch blobs / `kll_sketch_get_quantile_bigint` → 2 | refuses deferred-by-cost | stays | FNP-16-sketches |
| `F.kurtosis` | `-1.1517159763313605` | refuses R-FN-BATCH4 | stays | EX-FN-9 |
| `F.make_timestamp` | `datetime(2024,1,15,5,30,5)` on `(2024,1,15,10,30,5)` — the measured 4.1.2 cell, hour bound oddly | refuses R-FN-BATCH3 | stays | EX-FN-10 |
| `F.mode` | `2` | refuses R-FN-BATCH4 | stays | EX-FN-9 |
| `F.monotonically_increasing_id` | `0,1,2` | refuses single-node | stays | EX-FN-12 |
| `F.months_between` | `3.0`, `-8.0`, NULL | refuses R-FN-BATCH1 | stays | EX-FN-11 |
| `F.pandas_udf` | `v*2` → `[2,4,6]` (a plain `function`) | `PandasUDFFunction`, identical values | covered; return-type arm is EX-FN-23 | `functions/udf.py` |
| `F.parse_json` | `VariantVal` | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.posexplode` | `(0,10),(1,20)` | refuses | stays | EX-FN-2 |
| `F.posexplode_outer` | `(0,10),(1,20),(None,None) ×2` | refuses | stays | EX-FN-2 |
| `F.raise_error` | `[USER_RAISED_EXCEPTION] boom` P0001 | refuses E1 at build | stays | EX-FN-14 |
| `F.reflect` | resolves (documented `"5"`) | refuses unreachable | stays | FNP-15-reflect |
| `F.replace` | plain `"a"` search refuses `UNRESOLVED_COLUMN`; `lit` search answers `X,b,c` | plain-string search answers `X,b,c`; `lit` search TypeErrors | stays — disjoint spellings | EX-FN-15 |
| `F.schema_of_csv` | `STRUCT<_c0: INT, _c1: STRING>` | refuses E1 | stays | EX-FN-16 |
| `F.schema_of_variant`, `F.schema_of_variant_agg` | `OBJECT<a: BIGINT>` / variant-required refusal | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.schema_of_xml` | `STRUCT<b: BIGINT>` | refuses E1 stub | stays | EX-FN-22 |
| `F.sentences` | `[['Hello','world'],['How','are','you']]`, `[]`, NULL | refuses R-FN-BATCH2 | stays | EX-FN-17 |
| `F.skewness` | `0.3053162697580512` | refuses R-FN-BATCH4 | stays | EX-FN-9 |
| `F.spark_partition_id` | `0,0,0` | refuses single-node | stays | EX-FN-12 |
| `F.split` | `['a','b','c']`, `['x','y']`, `['']` | refuses R-FN-BATCH1 | stays | EX-FN-18 |
| `F.st_*` (5) | `UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED` on stock 4.1.2 (feature gated) | refuses deferred-by-cost | stays | FNP-16-geospatial |
| `F.theta_*` (7 names) | theta blobs / `theta_sketch_estimate` → 3 | refuses deferred-by-cost | stays | FNP-16-sketches |
| `F.to_csv` | `'1,"a,b,c"'`, `'2,"x,y"'`, `'3,""'` | refuses deferred-by-cost | stays | FNP-16-csv-xml-xpath |
| `F.to_variant_object` | `VariantVal` per row | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.to_xml` | `<ROW><n>1</n>…` | refuses deferred-by-cost | stays | FNP-16-csv-xml-xpath |
| `F.try_parse_json` | `VariantVal` | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.try_reflect` | resolves (documented) | refuses unreachable | stays | FNP-15-try_reflect |
| `F.try_to_timestamp` | `datetime(2024,6,15,8,0)` on `"2024-06-15 12:00:00"`, NULL ×2 — the measured 4.1.2 cell | refuses R-FN-BATCH3 | stays | EX-FN-20 |
| `F.try_variant_get`, `F.variant_get` | `1` on `parse_json('{"a":1}')` `$.a` | refuses deferred-by-cost | stays | FNP-16-variant |
| `F.udf` | `u1/u2/u3` (a plain `function`) | `UserDefinedFunction`, identical values | covered; return-type arm is EX-FN-23 | `functions/udf.py` |
| `F.udtf` | `partial` factory; decorated class `UserDefinedTableFunction`; `(5,),(6,)` direct and via registered SQL | `function` factory; identical class and rows | covered; factory wrapper arm is EX-FN-23 | `functions/udf.py` |
| `F.unwrap_udt` | refuses `DATATYPE_MISMATCH` on a STRING arm (needs USERDEFINEDTYPE) | refuses unreachable | stays | FNP-15-unwrap_udt |
| `F.xpath` | `[None,None]` on `//b` over `<a><b>1</b><b>2</b></a>` (lit path) | refuses deferred-by-cost | stays | FNP-16-csv-xml-xpath |
| `F.xpath_boolean` … `F.xpath_string` (8) | resolve with a `lit` path (`xpath_string` → `'hi'`) | refuses deferred-by-cost | stays | FNP-16-csv-xml-xpath |

## Gates (2026-09-11, on this tree)

Each command run independently; no JVM running during any gate (the oracle session
stopped before the first).

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` | 0 — `927 public names; 806 covered; 119 backlog; 2 exceptions; 214 examples` |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | 0 — same counts line; all 214 examples execute green |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_functions_b.py -q` | 0 — 5 passed |
| `make check-docs-links` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-python-conventions` | 0 |
| `make check-docstring-presence` | 0 |
| `make verify` | 0 |

Before/after counts line: `797 covered; 128 backlog; 2 exceptions; 212 examples` →
`806 covered; 119 backlog; 2 exceptions; 214 examples` — exactly the 9 covered names
and 2 new scripts.

## Cost

The Devin (SWE-2) leg ran 2026-09-11: read the contract, the EX-28/EX-29 ledgers, the
coverage gate, the §7 stay rows and the pin files; measured live Spark 4.1.2 cells and
repark probes over all 99 names; wrote two example files, two §7 rows, three pins, the
backlog ratchet, the maps, and this ledger.

## Disk

Pickup: `df -h` free space recorded in the session log. The oracle probes live under
the gitignored `scratch/ex30/` (removable at close). The lane venv carries a debug
native (`repark._native.__debug_assertions__` True) plus `pyspark` 4.1.2, `pandas`
3.0.5, `pyarrow` 25.0.0 for the oracle leg.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python
job (`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged
wheel is installed. EX-30 moves the inventory/backlog ratchet, two example files, two
§7 rows and three pins; it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-30-functions-remainder
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Static gate exit 0 — 927 names, 806 covered, 119 backlog, 2 exceptions, 214 examples; P-1 deleted the nine covered names and the gate red-named each.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/backlog.txt]
    - id: AT-2
      status: ATTACKED
      evidence: Backlog holds exactly the 90 uncovered roster names at 119; baseline moved 128 → 119 matching the 9 covered; P-1 exit 1 proves a removal is caught.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: --require-execute exit 0 over all 214 examples; P-2 wrong-bytes control in bitmap.py red-named the script and both values.
      artifacts: [scripts/check_example_coverage.py, docs/examples/functions/bitmap.py, docs/examples/functions/udf.py]
    - id: AT-4
      status: N/A
      justification: The gate and the pins are read-only over source files; no shared mutable engine state beyond the per-test session fixture.
    - id: AT-5
      status: N/A
      justification: No new execution surface; the gate's child drops AWS_* and PYTHONPATH and touches no network or cloud service; the examples use a memory catalog only.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the unit measures and teaches names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: Every asserted value in both examples and every pin arm was measured on live PySpark 4.1.2 before being written (oracle table above, 99 rows); no guessed cell.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: All nine gate commands ran on the shipped tree with recorded exit codes; no JVM alive during any gate (spark.stop() ran at the end of each oracle probe).
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md updated in the same commits — functions examples, tests, scripts, staging ledger; check-map-sync exit 0.
      artifacts: [docs/examples/functions/map.md, python/repark/tests/map.md, scripts/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_functions_b.py](../../../python/repark/tests/test_examples_functions_b.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-FN-22..23
- Sibling: [ex-29-class-remainder-ledger.md](ex-29-class-remainder-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-30-functions-remainder
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-30-functions-remainder
  artifacts_verified:
    ledger: PASS
    coverage_attestation: PASS
    findings_ledger: PASS
    shipped_flag_register: PASS (count 0)
  done_gate: PASS — all nine gates exit 0; LOGIC_SCORE 7/7 PROVEN; red-first provocations pasted
  status_update: v1.1 example backfill, F.* remainder — 9 covered, 89 stay in 32 family rows, 1 for the owner
  verdict: PASS
  rejection_route: N/A
```
