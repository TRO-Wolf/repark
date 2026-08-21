# PySpark 4.1.2 → RePark function gap

## 0. Sources actually read

| Source | Fact measured |
|---|---|
| `<pyspark-4.1.2-sdist>/src/pyspark/sql/functions/__init__.py` | exactly **one** `__all__`, **506** names (AST-parsed, not grepped) |
| `python/repark/src/repark/spark/functions.py` | exactly one `__all__`, **333** names |
| `task/census/v2-a5be8a7/*/worker-*.json` | `ranked_census` is `[[bucket, count], …]`; NEEDS-JVM = **118** rows across 10 modules, **58** in `test_functions` |
| `python/repark-parity/compat/classify.py` L32–66, L233–258 | the NEEDS-JVM rule (message-first regex list) — decisive for §3 |
| `docs/spark-sql-iceberg-parity.md` | G15 collation refusal; B-TZ-1/2/3/5 SQL-door notes; no JSON/variant/sketch rows |
| `~/.cargo/registry/.../datafusion-spark-54.1.0/src/function/` | **87** registered kernel names |
| `~/.cargo/registry/.../datafusion-functions{,-nested,-aggregate,-window}-54.1.0/src/` | 117 + 53 + 40 + 5 registered names |
| `crates/repark-functions/src/` + its `map.md` | RePark's own shim inventory |

---

## 1. The exact missing set — **181 names**

506 PySpark − 333 RePark = 181 in PySpark's `__all__` and **not** in RePark's `__all__`. (RePark also exports 8 names not in PySpark's `__all__`: `PythonUDFColumn`, `currentDate`, `currentTimestamp` are RePark-only; `bitwiseNOT`, `chr`, `countDistinct`, `random`, `uuid` **do** exist in `builtin.py` but were dropped from `__all__` upstream.)

```
AnalyzeArgument AnalyzeResult ArrowUDFType OrderingColumn PartitioningColumn SelectedColumn
SkipRestOfInputTableException aes_decrypt aes_encrypt aggregate any_value approx_count_distinct
array_insert arrow_udf arrow_udtf asc_nulls_last assert_true bitmap_and_agg bitmap_construct_agg
bitmap_or_agg bround call_function call_udf collate collation column conv convert_timezone
count_min_sketch create_map current_time desc_nulls_first exists filter forall from_json
get_json_object grouping grouping_id histogram_numeric hll_sketch_agg hll_sketch_estimate
hll_union hll_union_agg inline inline_outer input_file_block_length input_file_block_start
is_variant_null java_method json_array_length json_object_keys kll_merge_agg_bigint
kll_merge_agg_double kll_merge_agg_float kll_sketch_agg_bigint kll_sketch_agg_double
kll_sketch_agg_float kll_sketch_get_n_bigint kll_sketch_get_n_double kll_sketch_get_n_float
kll_sketch_get_quantile_bigint kll_sketch_get_quantile_double kll_sketch_get_quantile_float
kll_sketch_get_rank_bigint kll_sketch_get_rank_double kll_sketch_get_rank_float
kll_sketch_merge_bigint kll_sketch_merge_double kll_sketch_merge_float
kll_sketch_to_string_bigint kll_sketch_to_string_double kll_sketch_to_string_float listagg
listagg_distinct localtimestamp make_time make_timestamp_ltz make_timestamp_ntz make_ym_interval
map_concat map_filter map_zip_with mask max_by min_by negate parse_json percentile product
randstr reduce reflect regexp_extract_all regexp_substr regr_avgx regr_avgy regr_count
regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy schema_of_variant
schema_of_variant_agg session_user session_window sha st_asbinary st_geogfromwkb st_geomfromwkb
st_setsrid st_srid stack string_agg string_agg_distinct sum_distinct theta_difference
theta_intersection theta_intersection_agg theta_sketch_agg theta_sketch_estimate theta_union
theta_union_agg time_diff time_trunc timestamp_add timestamp_diff to_binary to_char to_csv
to_json to_number to_time to_timestamp_ltz to_timestamp_ntz to_varchar to_variant_object to_xml
transform transform_keys transform_values try_add try_aes_decrypt try_avg try_divide
try_element_at try_make_interval try_make_timestamp try_make_timestamp_ltz try_make_timestamp_ntz
try_mod try_multiply try_parse_json try_reflect try_subtract try_sum try_to_binary try_to_date
try_to_number try_to_time try_validate_utf8 try_variant_get typeof uniform unwrap_udt
validate_utf8 variant_get window window_time xpath xpath_boolean xpath_double xpath_float
xpath_int xpath_long xpath_number xpath_short xpath_string zip_with
```

7 of the 181 are classes/exceptions, not callables (`AnalyzeArgument`, `AnalyzeResult`, `ArrowUDFType`, `OrderingColumn`, `PartitioningColumn`, `SelectedColumn`, `SkipRestOfInputTableException`) → **174 missing callables + 7 missing symbols**.

---

## 2. Implementation families (25 families, partition verified: no duplicates, no orphans, sums to 181)

| # | Family | n | Members | What it needs |
|---|---|---|---|---|
| F01 | **Ordering completion** | 2 | `asc_nulls_last`, `desc_nulls_first` | **Nothing new.** RePark already exports `asc_nulls_first` + `desc_nulls_last` (imported at `functions.py:1392/1431`); this is the two missing corners of the same 2×2. Facade-only, `SortExpr{nulls_first}` bool flip. |
| F02 | **Pure aliases** | 3 | `column`, `negate`, `sha` | **Nothing new.** `builtin.py:333 column = col`, `builtin.py:3242 negate = negative` — RePark has both `col` and `negative`. `sha` is Spark's SHA-1 spelling → alias onto the `datafusion-spark` `sha1` kernel. |
| F03 | **UDTF analyze protocol + Arrow UDF** | 9 | `AnalyzeArgument`, `AnalyzeResult`, `OrderingColumn`, `PartitioningColumn`, `SelectedColumn`, `SkipRestOfInputTableException`, `ArrowUDFType`, `arrow_udf`, `arrow_udtf` | **Pure Python, no kernel.** The first six are dataclasses/exceptions in `pyspark/sql/udtf.py`; RePark already has `spark/udtf.py` + `functions_udf.py`. `arrow_udf`/`arrow_udtf` are Arrow-batch UDF entry points (Python-side; not JVM). |
| F04 | **By-name dispatch** | 2 | `call_function`, `call_udf` | **Needs a small RePark seam** — resolve a name from the session `FunctionRegistry` and build a `ScalarFunction` Expr. Registry lookup already exists on the DF `SessionContext`. |
| F05 | **Higher-order / lambda** | 11 | `transform`, `filter`, `exists`, `forall`, `aggregate`, `reduce`, `zip_with`, `transform_keys`, `transform_values`, `map_filter`, `map_zip_with` | **Partly a DataFusion kernel, partly new — but the hard part is already done.** DF 54.1 has *first-class lambda support*: `Expr::Lambda` / `Expr::LambdaVariable` (`datafusion-expr-54.1.0/src/expr.rs:431,433,1451`), SQL-planner lambda args (`datafusion-sql-54.1.0/src/expr/function.rs:375-470`), and `datafusion-functions-nested` ships `array_transform` (→`transform`), `array_filter` (→`filter`), `array_any_match` (→`exists`), plus `lambda_utils.rs` / `macros_lambda.rs`. `forall`/`aggregate`/`reduce`/`zip_with`/`transform_keys`/`transform_values`/`map_filter`/`map_zip_with` are **new RePark kernels built on existing DF lambda machinery**, not from scratch. (`datafusion-spark`'s `function/lambda/mod.rs` is `vec![]` — empty.) |
| F06 | **Aggregates with an already-linked DataFusion UDAF** | 16 | `regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`, `regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy`, `sum_distinct`, `grouping`, `approx_count_distinct`, `listagg`, `listagg_distinct`, `string_agg`, `string_agg_distinct` | **DataFusion kernel exists — wire only.** All nine `regr_*` are in `all_default_aggregate_functions()` (`datafusion-functions-aggregate-54.1.0/src/lib.rs:161-169`), as are `sum_distinct`, `grouping`, `approx_distinct` (→`approx_count_distinct`), `string_agg` (→ all four listagg/string_agg spellings, `_distinct` via the DISTINCT modifier). `grouping` is usable because RePark already lowers `cube`/`rollup`/grouping-sets through SQL (`dataframe/core.py:5327-5347`). Residuals: Spark's `approx_count_distinct(rsd)` is HLL++ vs DF's HLL — a value divergence to register, not a build. |
| F07 | **Aggregates needing a new kernel** | 8 | `any_value`, `max_by`, `min_by`, `product`, `percentile`, `grouping_id`, `histogram_numeric`, `count_min_sketch` | **New RePark kernel** (except two near-hits): `any_value` ≈ DF `first_value(ignore_nulls)`; `percentile` ≈ DF `percentile_cont` (exact, but Spark takes a frequency arg). `max_by`/`min_by`/`product`/`grouping_id` are small new UDAFs; `histogram_numeric`/`count_min_sketch` are Spark-specific output shapes. |
| F08 | **`try_*` arithmetic + conversion** | 12 | `try_add`, `try_subtract`, `try_multiply`, `try_divide`, `try_mod`, `try_element_at`, `try_sum`, `try_avg`, `try_to_binary`, `try_to_date`, `try_to_number`, `try_to_time` | **Mostly a RePark rewrite over machinery that exists.** `try_sum` is an exact `datafusion-spark` kernel (`function/aggregate/try_sum.rs`). The rest are "NULL instead of raise" wrappers, and RePark already owns the ANSI seam (`repark-functions/src/ansi.rs` `SparkAnsiConfig` + `__repark_ansi_nonzero_divisor__`, and DEC-6/DEC-7 in the registry) — so this is a systematic pass over one existing rule, not 12 independent builds. `try_element_at` rides RePark's own `SparkElementAt`. |
| F09 | **Timestamp / timezone / TIME type** | 18 | `convert_timezone`, `current_time`, `localtimestamp`, `make_time`, `make_timestamp_ltz`, `make_timestamp_ntz`, `make_ym_interval`, `time_diff`, `time_trunc`, `timestamp_add`, `timestamp_diff`, `to_time`, `to_timestamp_ltz`, `to_timestamp_ntz`, `try_make_interval`, `try_make_timestamp`, `try_make_timestamp_ltz`, `try_make_timestamp_ntz` | **Kernels exist for the TIME half; the LTZ/NTZ half is a RePark job.** DF core has `current_time`, `make_time`, `to_time` (all Time64); `datafusion-spark` has `time_trunc`, `from_utc_timestamp`/`to_utc_timestamp`, `make_interval`/`make_dt_interval`. RePark already has `TimeType` (`types.py:346`) and the whole session-zone carrier (`session_time_zone.rs`, `instant_ts.rs`, `spark.sql.timestampType` Q10). The LTZ/NTZ `make_*`/`to_*` family is a wiring pass on the Q10 carrier. `convert_timezone`/`time_diff`/`timestamp_add`/`timestamp_diff`/`make_ym_interval` are new. |
| F10 | **Time windowing** | 3 | `window`, `session_window`, `window_time` | **New RePark work at plan altitude, not kernel altitude.** Tumbling/sliding `window` requires row expansion (one input row → N windows) and `session_window` requires a stateful sessionization; DataFusion has no equivalent. Deepest item in the list per name. |
| F11 | **JSON** | 5 | `from_json`, `to_json`, `get_json_object`, `json_array_length`, `json_object_keys` | **New RePark kernels.** `datafusion-spark/function/json/` contains only `json_tuple`; DF core has no JSON functions. `serde_json = "1"` is already a workspace dep (`Cargo.toml:65`). Note `from_json` needs the DDL-schema parser, which RePark already has in `types.py` (`_parse_datatype_string` path used at `types.py:1047`). |
| F12 | **CSV / XML / XPath** | 11 | `to_csv`, `to_xml`, `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_number`, `xpath_short`, `xpath_string` | **New RePark kernels + a new XPath dependency.** `datafusion-spark/function/{csv,xml}/mod.rs` are both `vec![]`. The nine `xpath_*` need an XPath 1.0 engine matching Spark's `javax.xml.xpath`; no crate is vendored today. |
| F13 | **VARIANT** | 8 | `parse_json`, `try_parse_json`, `is_variant_null`, `variant_get`, `try_variant_get`, `schema_of_variant`, `schema_of_variant_agg`, `to_variant_object` | **New RePark kernel + a new binary format.** Spark's VARIANT is a specific value/metadata binary encoding; no crate in the registry implements it (no `variant`/`parquet-variant` crate present). RePark already has a `VariantType` shell (`types.py:659`), so the type object exists but nothing backs it. |
| F14 | **Sketches (HLL / theta / KLL)** | 32 | `hll_sketch_agg`, `hll_sketch_estimate`, `hll_union`, `hll_union_agg`; `theta_difference`, `theta_intersection`, `theta_intersection_agg`, `theta_sketch_agg`, `theta_sketch_estimate`, `theta_union`, `theta_union_agg`; the 21 `kll_*` (`{merge_agg,sketch_agg,sketch_get_n,sketch_get_quantile,sketch_get_rank,sketch_merge,sketch_to_string}_{bigint,double,float}`) | **New RePark kernel + a Rust Apache DataSketches port.** Spark's sketch columns are DataSketches *binary blobs*; interop requires byte-exact format compatibility. DF's `hyperloglog.rs` is internal to `approx_distinct` and is **not** the DataSketches HLL format, so it cannot be reused for the blob. Largest family by name count, lowest value per name. |
| F15 | **Bitmap aggregates** | 3 | `bitmap_and_agg`, `bitmap_construct_agg`, `bitmap_or_agg` | **New RePark UDAFs on an existing base.** `datafusion-spark/function/bitmap/` ships the three scalar halves (`bitmap_count`, `bitmap_bit_position`, `bitmap_bucket_number` — all three already exported by RePark); only the aggregate halves are missing. |
| F16 | **Geospatial** | 5 | `st_asbinary`, `st_geogfromwkb`, `st_geomfromwkb`, `st_setsrid`, `st_srid` | **New RePark type + WKB codec.** Spark 4.1's GEOGRAPHY/GEOMETRY types have no Arrow representation and no kernel anywhere in the vendored tree. Effectively a sub-project. |
| F17 | **Crypto + masking** | 4 | `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt`, `mask` | **New RePark kernels.** `datafusion-spark/function/hash/` has crc32/sha1/sha2/xxhash64 only; no AES anywhere. `mask` is trivial string work; the AES trio needs a cipher crate (none vendored). |
| F18 | **Conversion / formatting** | 7 | `to_char`, `to_varchar`, `to_number`, `to_binary`, `conv`, `bround`, `typeof` | **New RePark kernels — the DF `to_char` name is a false friend.** DF core has a `to_char`, but its own doc says *"Unlike the PostgreSQL equivalent … numerical formatting is not supported"* (`datafusion-functions-54.1.0/src/datetime/to_char.rs:40`), while Spark's `to_char`/`to_varchar`/`to_number` are **numeric** format strings (`'$99.99'`). `bround` (half-even) and `conv` (base conversion) have no kernel. `typeof` is facade-only — read the analyzed schema and emit a literal. |
| F19 | **Collection / map constructors** | 3 | `create_map`, `map_concat`, `array_insert` | **`create_map` is nearly free; the other two are new.** DF nested registers `map(keys, values)` (`map.rs:374`), so `create_map(k1,v1,…)` = `map(make_array(...), make_array(...))` in the facade. No `array_insert` and no `map_concat` exist in either registry. |
| F20 | **Generators** | 3 | `inline`, `inline_outer`, `stack` | **Facade/plan work, no kernel.** RePark already ships `explode`/`explode_outer` and the native `dynamicFlatten` plan rewrite (STATUS: v0.5.0 / #183) — `inline` is explode-array-of-struct + field projection, which is exactly that machinery. `stack` is an n-row unpivot. |
| F21 | **Regex** | 2 | `regexp_extract_all`, `regexp_substr` | **New RePark kernel over machinery RePark already wrote.** `crates/repark-functions/src/spark_regexp.rs` already implements the Java `Matcher.find()` loop for `regexp_count`/`regexp_instr`; both of these are the same loop with a different projection. DF's `regexp_match` only returns the *first* match's groups, so it is not sufficient alone. |
| F22 | **Random** | 2 | `randstr`, `uniform` | **New RePark kernel over machinery RePark already wrote.** `random.rs` already implements Spark `XORShiftRandom` + MurmurHash3 `hashSeed` for `rand`/`randn`; these two are two more draws from the same stream. |
| F23 | **Collation** | 2 | `collate`, `collation` | **Policy-blocked, not capability-blocked.** Registry row **G15** explicitly states "`F.collate` / `F.collation` / `Column.collate` are not on the facade (`AttributeError`)" and that collation is refused loudly pending an "implement-or-keep-absent decision". Do not implement without an owner ruling. |
| F24 | **UTF-8 validation** | 2 | `validate_utf8`, `try_validate_utf8` | **Thin new kernel on an existing one.** `datafusion-spark` ships `is_valid_utf8` + `make_valid_utf8` (both already wired into RePark's `expr_fn.rs` per FN-GT1); these two are the raise / NULL-on-invalid variants of the same check. |
| F25 | **JVM reflection + runtime identity** | 8 | `java_method`, `reflect`, `try_reflect`, `unwrap_udt`, `input_file_block_start`, `input_file_block_length`, `session_user`, `assert_true` | **Mixed — see §3.** Four are unreachable, two are deep engine plumbing, two are cheap: `session_user` is the same value RePark already serves via `current_user`/`user`; `assert_true` is a raise-kernel, and RePark already has the raise-kernel pattern in `ansi.rs`. |

---

## 3. JVM-only / structurally unreachable

### 3a. The census's NEEDS-JVM bucket does **not** mean "unreachable" — read it carefully

This is the most important correction to make before planning off that bucket. `compat/classify.py:233-258` decides NEEDS-JVM **message-first** on a regex list (L32-55) that includes `SESSION_OR_CONTEXT_NOT_EXISTS`, `SparkContext._active_spark_context`, and `assert SparkContext._active_spark_context is not None`. Those are exactly the strings PySpark's own `builtin.py` `_invoke_function` emits when the facade did **not** export a name, PySpark's real implementation ran instead, and it tried to reach the (absent) JVM.

Measured split of the 118 NEEDS-JVM rows:

| Cause | Rows |
|---|---|
| `AssertionError` → pyspark builtin `_invoke_function` JVM assert | 43 |
| `[SESSION_OR_CONTEXT_NOT_EXISTS]` fallthrough | 26 |
| `sc.parallelize` (RDD API) | 22 |
| `sc._jvm` (real gateway) | 10 |
| `[JAVA_GATEWAY_EXITED]` | 8 |
| `sc.textFile` (RDD API) | 2 |
| `_jsparkSession` / `_jsc` / `sc.stop` / `sc.broadcast` / `sc.accumulator` / deliberate segfault | 6 |

And in `test_functions` specifically (58 rows): **55 are facade-gap fallthrough** (37 assert + 18 session-not-exists) and only **3** name a real JVM/RDD dependency — `test_function_parity` (`sc._jvm`), `test_basic_functions` (`sc.parallelize`), `test_input_file_name_reset_for_rdd` (`sc.textFile`). So the NEEDS-JVM bucket in the functions module is ~95% a **measurement of this very gap**, not an unreachability verdict. Closing families below will convert those 55 rows into PASS/FAIL rows and move the engine-relevant denominator.

### 3b. Names that genuinely cannot exist in a no-JVM engine

| Name | Reason |
|---|---|
| `java_method` | Loads a Java class by name and invokes a static method by reflection. Requires a live JVM by definition. **Unreachable.** |
| `reflect` | Same expression, different spelling (`CallMethodViaReflection`). **Unreachable.** |
| `try_reflect` | Same, with exception→NULL. **Unreachable.** |
| `unwrap_udt` | Unwraps a Spark `UserDefinedType`. The UDT registry and the SQL-type ↔ object round-trip live on the JVM; PySpark UDTs (e.g. `VectorUDT`) serialize through it. With no JVM there is no UDT system to unwrap from. **Unreachable as Spark defines it.** |
| `input_file_block_start` / `input_file_block_length` | Read Spark's `InputFileBlockHolder` thread-local, populated by the JVM `HadoopRDD`/`FileScanRDD` as it hands a split to a task. Not JVM-only in principle, but there is no DataFusion equivalent surface and RePark's existing `input_file_name` is itself still a disclosed stub. **Reachable only by inventing a different mechanism** — treat as new engine plumbing, and expect a registry row for any divergence. |

Two further families are *not* JVM-blocked but are structurally out of reach at today's cost:

- **F16 geospatial (5)** — needs a GEOGRAPHY/GEOMETRY type with no Arrow representation and no vendored WKB codec.
- **F14 sketches (32)** — needs a byte-compatible Rust port of Apache DataSketches. DF's internal `hyperloglog.rs` is not that format, so it cannot serve the blob even for the HLL subset.

And **F23 collation (2)** is blocked by policy, not capability: G15 is a DECLARED registry row with an owner ruling that these stay absent-and-loud pending an implement-or-keep-absent decision.

---

## 4. Recommended implementation order

The ordering weighs three things: (a) does a kernel already exist in a crate that is *already linked into the wheel*, (b) how many PySpark test rows flip out of the facade-gap bucket, (c) does the work reuse a seam RePark already owns.

**Tier 0 — free, do in one PR (7 names)**
`asc_nulls_last`, `desc_nulls_first` (F01) · `column`, `negate`, `sha` (F02) · `session_user`, `typeof` (from F25/F18)
No kernel, no Rust. Every one of these is an alias or a flag on something RePark already exports. Highest value-per-line in the whole list, and it closes the embarrassing 2-of-4 `asc_nulls_*`/`desc_nulls_*` asymmetry.

**Tier 1 — kernel already compiled in, wire-only (16 names, F06)**
The nine `regr_*` plus `sum_distinct`, `grouping`, `approx_count_distinct`, `listagg`, `listagg_distinct`, `string_agg`, `string_agg_distinct`.
All are in `all_default_aggregate_functions()` — already registered on every RePark session; the only missing piece is the Python wrapper and pins. Sixteen names for roughly the cost of one. `grouping` is unblocked because `cube`/`rollup`/grouping-sets already lower through SQL.

**Tier 2 — highest strategic value: higher-order functions (11 names, F05)**
`transform`/`filter`/`exists` land on DF 54.1's `array_transform`/`array_filter`/`array_any_match`, which are already registered; the DF SQL planner already parses `x -> expr`. The other eight ride the same `Expr::Lambda` plumbing. Put this second because it is the family PySpark users notice most, it converts `test_higher_order_function_failures` / `test_nested_higher_order_function` out of the fallthrough bucket, and the enabling machinery (lambda in the planner + `lambda_utils.rs`) exists *right now* and would otherwise be rediscovered later at full price.

**Tier 3 — reuse RePark's own existing kernels (9 names)**
`regexp_extract_all`, `regexp_substr` (F21 — the `Matcher.find()` loop in `spark_regexp.rs` already exists) · `randstr`, `uniform` (F22 — `XORShiftRandom` already exists) · `validate_utf8`, `try_validate_utf8` (F24 — `is_valid_utf8`/`make_valid_utf8` already wired) · `bitmap_and_agg`, `bitmap_construct_agg`, `bitmap_or_agg` (F15 — the scalar halves already ship) · `assert_true` (the `ansi.rs` raise-kernel pattern).
Cheap because the hard semantics (Java regex dialect, Spark's PRNG, the bitmap layout) were already paid for.

**Tier 4 — the `try_*` sweep (12 names, F08)**
One systematic pass over the ANSI seam RePark already owns (`SparkAnsiConfig`, DEC-6/DEC-7). Twelve names, one rule, one ledger. `try_sum` is a free `datafusion-spark` kernel. Do this as a batch, not name-by-name.

**Tier 5 — small collection/generator wins (6 names)**
`create_map` (facade composition over DF `map`) · `map_concat`, `array_insert` (small new kernels) · `inline`, `inline_outer`, `stack` (reuse `dynamicFlatten`/`explode`) · `call_udf`, `call_function` (registry lookup seam).

**Tier 6 — timestamp/TIME (18 names, F09)**
Half the kernels exist (`current_time`, `make_time`, `to_time`, `time_trunc`); RePark already owns `TimeType`, the session-zone carrier, and the Q10 LTZ/NTZ knob. But this family is entangled with the open TZ rows in the registry (TZ-4 residues, TZ-6, TZ-7, TZ-8, B-TZ-1/2/3/5), so it needs a design pass rather than a wiring pass — which is why it sits below the batches above despite the good kernel coverage.

**Tier 7 — JSON (5 names, F11)**
No kernel, but `serde_json` is already a dep and `types.py` already parses DDL schemas for `from_json`. Genuinely valuable to users (`from_json`/`to_json` are top-20 PySpark functions), but it is a real build. Note this is also where the **exported-but-stubbed** `schema_of_json` sits — do them together.

**Tier 8 — remaining aggregates + conversion/format (15 names, F07 + F18)**
`max_by`/`min_by`/`product`/`grouping_id`/`any_value`/`percentile` are small UDAFs; `to_char`/`to_varchar`/`to_number`/`to_binary`/`conv`/`bround` are new numeric-format kernels (do **not** reuse DF's `to_char` — wrong semantics, documented above).

**Tier 9 — defer, with an owner ruling first**
F23 collation (2, blocked by G15 policy) · F17 crypto/mask (4, needs a cipher crate) · F13 VARIANT (8, new binary format) · F12 CSV/XML/XPath (11, needs an XPath engine) · F10 time windowing (3, plan-altitude) · F16 geospatial (5, new type system) · F14 sketches (32, needs a DataSketches port).
The sketch family is 32 of the 181 names — 18% of the headline gap — for close to the least user value in the list. Explicitly deciding to keep it absent-and-loud (the G15 pattern) would shrink the *actionable* gap from 181 to 149 without writing a line of code.

**Tier 10 — unreachable, register rather than build (4 names)**
`java_method`, `reflect`, `try_reflect`, `unwrap_udt`. Add a divergence-registry row per §6 of `docs/spark-sql-iceberg-parity.md` with the reason from §3b, plus a loud-refusal pin. `input_file_block_start`/`input_file_block_length` go here too until `input_file_name` itself is de-stubbed.

---

## 5. Secondary finding you should fold into the plan — 34 names are exported but **unconditionally raise**

`__all__` membership overstates RePark's real surface. AST-scanning `python/repark/src/repark/spark/functions*.py` for exported functions whose body raises `UnsupportedOperationException` at the top level of the function (no reachable return before it) finds **34** — and 8 more that raise only on some argument paths.

**Unconditional stubs (34):**
```
arrays_zip crc32 datediff format_number format_string from_csv from_utc_timestamp from_xml hash
input_file_name json_tuple kurtosis make_timestamp map_from_arrays mode monotonically_increasing_id
months_between posexplode posexplode_outer raise_error regexp_extract schema_of_csv schema_of_json
schema_of_xml sentences sha1 skewness soundex spark_partition_id split to_utc_timestamp
try_to_timestamp unix_timestamp xxhash64
```
Example (`functions_expr.py:599`):
```python
def regexp_extract(str: Column | str, pattern: str, idx: int) -> Column:
    """Unsupported: engine has no ``regexp_extract``."""

    raise UnsupportedOperationException(
        "functions.regexp_extract is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )
```
No shadowing definition exists — I checked for duplicate `def`s across all `functions*.py` (none) and confirmed `functions.py` imports each of these from `functions_expr`.

**Conditional stubs (8):** `array_join`, `bucket`, `locate`, `pandas_udf`, `percentile_approx`, `sha2`, `to_date`, `to_timestamp`.

**Why this changes the ordering:** cross-matching the 34 against the two kernel registries shows **10 have an exact-name kernel already linked into the wheel** — `crc32`, `format_string`, `from_utc_timestamp`, `json_tuple`, `map_from_arrays`, `sha1`, `soundex`, `to_utc_timestamp`, `xxhash64` (`datafusion-spark`) and `arrays_zip` (`datafusion-functions-nested`) — plus four near-name hits: `datediff` ← `datafusion-spark`'s `date_diff` (which RePark *already exposes* as `date_diff` in `functions_datetime`, so this is arg-order only), `regexp_extract` ← DF `regexp_match`, `try_to_timestamp` ← DF `to_timestamp`, `unix_timestamp` ← `datafusion-spark` `unix_seconds` / DF `to_unixtime`.

**Recommendation: insert this as Tier 0.5, immediately after Tier 0.** ~14 stub removals whose kernels are already compiled into the shipping wheel, on names (`split`, `regexp_extract`, `hash`, `datediff`, `sha1`, `json_tuple`, `format_string`) that are far more heavily used in real PySpark code than anything in the missing-181 list. Fixing a loud stub on `split` buys more than adding `theta_intersection_agg`.

**Honest total:** the real PySpark-4.1.2 function gap is **181 absent + 34 present-but-raising = 215 names**, against a 506-name `__all__` — i.e. RePark meets roughly **58%** of the declared surface today, not the 66% that a raw `__all__` diff suggests.