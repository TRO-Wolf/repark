# RePark function-surface ownership map

Verified against `main @ 24a1e7b`, DataFusion **54.1.0** / `datafusion-spark` **54.1.0**. Read-only; no files written.

---

## 0. The resolution chain (why order is the whole story)

`repark_core::session::df_guards` builds the state with `SessionStateBuilder::…with_default_features()` → **the entire DataFusion default registry is present first**. Then `SparkExtension::register` calls `repark_functions::register_all(ctx)` (`crates/repark-functions/src/lib.rs:71`), which registers in this order — **later registration wins a name clash** (DataFusion's registry overwrites by name):

1. `datafusion_spark::all_default_scalar_functions()` — 83 scalar
2. `datafusion_spark::all_default_aggregate_functions()` — 4 aggregate
3. `aggregate::functions()` — repark `avg`
4. `approx_percentile_cont_udaf().with_aliases(["percentile_approx","approx_percentile"])`
5. `datafusion_spark::all_default_window_functions()` — **empty in 54.1**
6. `datetime::functions()` (18) → `to_date` → `instant_ts::functions()` (3) → `string::functions()` (7) → `collection::functions()` (4) → `url::functions()` (2) → `random::functions()` (2)
7. `decimal_spark::register_spark_decimal_planner(ctx)` (ExprPlanner, not a name)

Then `repark_ta::TaExtension` adds 81 window UDFs.

**435 distinct callable spellings** on the Spark door.

---

## 1. Provenance buckets

### REPARK_OWNED — 123 callable spellings (+11 internal-only)

**A. `repark-functions` registered scalar — 39 spellings / 37 UDF instances**

| module | names |
|---|---|
| `datetime.rs` (18) | `year` `yearofweek` `quarter` `month` `weekofyear` `dayofmonth` `day` `dayofyear` `dayofweek` `weekday` `hour` `minute` `second` `make_date` `add_months` `date_format` `trunc` `date_trunc` |
| `timestamp_cast.rs` (1) | `to_date` |
| `instant_ts.rs` (3) | `now` `current_timestamp` `to_timestamp` |
| `string.rs` (7 +1 alias) | `substring` [alias `substr`] `concat` `bit_length`¹ `octet_length`¹ `regexp_count`² `regexp_instr`² `split_part`³ |
| `collection.rs` (4) | `element_at` `str_to_map`⁴ `shuffle`⁵ `map_from_entries`⁶ |
| `url.rs` (2) | `parse_url` `try_parse_url` (backed by `java_uri.rs`) |
| `random.rs` (2 +1 alias) | `rand` [alias `random`] `randn` |

¹`spark_length.rs` ²`spark_regexp.rs` ³`spark_split_part.rs` ⁴`str_to_map.rs` ⁵`shuffle.rs` ⁶`map_from_entries.rs` — all `#[path]` siblings.

**B. `repark-functions` aggregate — 1 + 2 alias spellings**
`avg` (`SparkAvgWithRetract`); plus `percentile_approx` / `approx_percentile` registered as *aliases over DataFusion-core* `approx_percentile_cont` (Q1 R-ML-QUANTILE — the kernel is DF-core t-digest, the Spark spellings are repark's).

**C. `repark-functions` EMBEDDED — 11, never registered, not user-callable**
Reachable only via analyzer rewrite or facade `expr_fn` embed:
`__repark_array_get__` `__repark_get_item__` (collection.rs) · `__repark_ansi_nonzero_divisor__` (ansi.rs) · `__repark_epoch_seconds_floor__` `__repark_epoch_seconds_real__` `__repark_timestamp_to_string__` `__repark_timestamp_to_date__` (timestamp_cast.rs) · `__repark_spark_decimal_div__` `__repark_spark_decimal_add__` `__repark_spark_decimal_sub__` `__repark_spark_decimal_mul__` (decimal_spark.rs)

**D. `repark-ta` window UDFs — 81** (`SPECS` in `crates/repark-ta/src/udf/mod.rs:243`)
`ta_sma ta_ema ta_rsi ta_adx ta_atr ta_trange ta_var ta_stddev ta_linearreg ta_linearreg_slope ta_linearreg_intercept ta_linearreg_angle ta_tsf ta_correl ta_min ta_max ta_sum ta_wma ta_dema ta_tema ta_trima ta_kama ta_t3 ta_midpoint ta_midprice ta_bbands_upper ta_bbands_middle ta_bbands_lower ta_mom ta_roc ta_rocp ta_rocr ta_rocr100 ta_willr ta_cci ta_cmo ta_bop ta_apo ta_ppo ta_aroon_down ta_aroon_up ta_aroonosc ta_trix ta_ultosc ta_dx ta_adxr ta_plus_di ta_minus_di ta_plus_dm ta_minus_dm ta_macd ta_macd_signal ta_macd_hist ta_macdfix ta_macdfix_signal ta_macdfix_hist ta_macdext ta_macdext_signal ta_macdext_hist ta_ma ta_stoch_slowk ta_stoch_slowd ta_stochf_fastk ta_stochf_fastd ta_stochrsi_fastk ta_stochrsi_fastd ta_natr ta_beta ta_avgprice ta_medprice ta_typprice ta_wclprice ta_mama ta_fama ta_sar ta_sarext ta_mavp ta_ad ta_adosc ta_obv ta_mfi`

**E. `repark-spark` — ZERO owned kernels.** Grep for `ScalarUDFImpl for` / `AggregateUDFImpl for` / `WindowUDFImpl for` across `crates/repark-spark/src/` hits only `tests/common.rs` and `tests/insert_overwrite.rs` (fixtures). The Spark door owns *statement*-level surface, not kernels: 3 CALL procedures (`expire_snapshots`, `rewrite_data_files`, `rollback_to_snapshot` — `call.rs:55`), temporal `RANGE` frame restatement (`window_range.rs`), collation refusal (`collation.rs`), DDL/DML routing, metadata tables. **Cost-of-ownership note for the planner: adding a kernel never touches `repark-spark`.**

### DATAFUSION_SPARK — 87 registered, 73 survive (14 overwritten)

Surviving scalar (70):
`abs array array_contains array_repeat ascii base64 bin bit_count bit_get bitmap_bit_position bitmap_bucket_number bitmap_count bitwise_not ceil char crc32 csc date_add date_diff date_part date_sub elt expm1 factorial floor format_string from_utc_timestamp hex if ilike is_valid_utf8 json_tuple last_day length like luhn_check make_dt_interval make_interval make_valid_utf8 map_from_arrays mod negative next_day pmod rint round sec sha1 sha2 shiftleft shiftright shiftrightunsigned size slice soundex space spark_cast time_trunc to_utc_timestamp try_url_decode unbase64 unhex unix_date unix_micros unix_millis unix_seconds url_decode url_encode width_bucket xxhash64`

Surviving aggregate (3): `collect_list` `collect_set` `try_sum`
Window (0): `all_default_window_functions()` returns `vec![]` in 54.1.

### DATAFUSION_CORE — 269 registered, 239 survive

- scalar surviving **198** (`datafusion-functions` + `datafusion-functions-nested`, minus 14 shadowed by DF-spark and 19 by repark):
`any_match array_any_match array_any_value array_append array_cat array_compact array_concat array_dims array_distance array_distinct array_element array_empty array_except array_extract array_filter array_has array_has_all array_has_any array_indexof array_intersect array_join array_length array_max array_min array_ndims array_normalize array_pop_back array_pop_front array_position array_positions array_prepend array_push_back array_push_front array_remove array_remove_all array_remove_n array_replace array_replace_all array_replace_n array_resize array_reverse array_slice array_sort array_to_string array_transform array_union arrays_overlap arrays_zip arrow_cast arrow_field arrow_metadata arrow_try_cast arrow_typeof btrim cardinality cast_to_type char_length character_length chr coalesce concat_ws contains cosine_distance cot current_date current_time date_bin datepart datetrunc decode digest dot_product empty encode ends_with find_in_set flatten from_unixtime gcd get_field greatest ifnull initcap inner_product instr isnan iszero lcm least left levenshtein list_* (48 list_ aliases) log lower lpad ltrim make_array make_list make_time map map_entries map_extract map_keys map_values md5 named_struct nanvl nullif nvl nvl2 overlay pi position pow power regexp_like regexp_match regexp_replace repeat replace reverse right row rpad rtrim signum starts_with string_to_array string_to_list strpos struct substr_index substring_index to_char to_hex to_local_time to_time to_timestamp_micros to_timestamp_millis to_timestamp_nanos to_timestamp_seconds to_unixtime today translate trim try_cast_to_type union_extract union_tag upper uuid version with_metadata`
- aggregate surviving **30**: `approx_distinct approx_median approx_percentile_cont approx_percentile_cont_with_weight array_agg bool_and bool_or corr count covar covar_pop covar_samp first_value grouping max mean median min nth_value percentile_cont quantile_cont stddev stddev_pop string_agg sum var var_pop var_population var_samp var_sample`
- window **11** (nothing shadows them): `row_number rank dense_rank percent_rank ntile cume_dist lag lead first_value last_value nth_value`

**Counts:** REPARK_OWNED 123 · DATAFUSION_SPARK 73 · DATAFUSION_CORE 239 → **435**.
Facade `repark.spark.functions*` exports 333 `__all__` names (includes `col`/`lit`/`when`/`udf` and other non-kernel helpers); `call_scalar_expr` in `function_dispatch.rs` has **144 match arms** covering ~200 spellings.

---

## 2. Deliberate divergence points — 29 spellings where a repark shim overwrites

### 2a. Overwrites `datafusion-spark` (13 scalar + 1 aggregate)

| name | reason (verbatim from code) |
|---|---|
| `hour` `minute` `second` | `datetime.rs:180` — *"Overwrite datafusion-spark hour/minute/second so TimeType (X1 lit(time)) works (octo C3 / Apache test_hour\|minute\|second)"*; DF-spark's are Timestamp-only |
| `add_months` `trunc` `date_trunc` | H-1a split B session-zone: *"Every calendar field of a `TIMESTAMP` now resolves in `spark.sql.session.timeZone`"*; `TruncDate` — *"invalid fmt → NULL, so `'Q'` is NULL not `QUARTER`"*; `DateTrunc` — format-first arg order, µs output |
| `concat` | `string.rs:17` — DF-spark's `SparkConcat` *"promises `Utf8` at plan time but delegates to DataFusion's kernel, which returns `Utf8View` … Physical evaluation then asserts `result_data_type == promised` and fails loud — TPC-DS Q5 / Q80 / Q84"*. D2 fix: coerce to `Utf8`, any-NULL → NULL, always emit `Utf8` |
| `substring` (+`substr`) | `string.rs:3` — audit AR-1 #6: *"`substr('hello', 0, 3)` returned `'he'` where Spark gives `'hel'`, and a negative position returned `''` where Spark counts from the end"*; Spark `UTF8String.substringSQL` char semantics |
| `shuffle` | `shuffle.rs:3` (X1, **S0**) — *"`general_array_shuffle` writes a placeholder slot for every NULL row with `mutable.extend(0, 0, 1)` … When the child values buffer is empty that read is out of bounds and `arrow-data`'s primitive transform panics"*. `CAST(NULL AS ARRAY<INT>)` panicked at the Python boundary |
| `map_from_entries` | `map_from_entries.rs:1` (X7) — *"`datafusion-spark` 54.1's `map_from_entries` silently keeps the LAST entry … where Spark raises"* `DUPLICATED_MAP_KEY` per `spark.sql.mapKeyDedupPolicy=EXCEPTION`. *"silent-wrong-result divergence on a data-integrity path"* |
| `str_to_map` | `str_to_map.rs:3` (X6) — *"`datafusion-spark` 54.1 splits with `str.split(delim)` (literal). Spark SQL treats `pairDelim` and `keyValueDelim` as Java regex."* Plus `bind_ascii_perl_classes` — Java's `\s\d\w` are ASCII-only, the `regex` crate's are Unicode (NBSP used to split where Spark does not) |
| `parse_url` `try_parse_url` | `url.rs` / map.md (X8) — *"`datafusion-spark` 54.1 extracts with `url::Url`, a WHATWG-URL **normalizer**; Spark uses `java.net.URI`, a **splitter**. Eleven measured divergences closed"* (explicit port kept, case kept, dot segments unresolved, IDN→NULL, opaque `PATH` NULL, `%2e` verbatim, `INVALID_URL` raise) |
| `avg` (UDAF) | `aggregate.rs:3` — R-RETRACT-SHIM X2: *"`SparkAvg` / `AvgAccumulator` never overrides `retract_batch`, so sliding-frame `AVG(...) OVER (ROWS BETWEEN …)` dies"*; DEC-5/Z-3 U1: *"`SparkAvg` … coerced every Numeric — including `DECIMAL` — to `Float64`"*, losing Spark's `(min(38,p+4), min(38,s+4))` type |

### 2b. Overwrites DataFusion-core only (15 spellings)

| name | reason |
|---|---|
| `bit_length` `octet_length` | `spark_length.rs:1` — *"Spark 4.1.2 … accept STRING and BINARY, and stringify every other type … DataFusion's kernels are Utf8/Binary-exact, so an int/bool/float column fails."* Binary passes through so `unhex` payloads stay bytes |
| `regexp_count` `regexp_instr` | `spark_regexp.rs:1` — *"DataFusion's kernels return `0` for NULL inputs (int64) and treat a 3rd `regexp_instr` argument as START POSITION"*; Spark: NULL-in NULL-out INT, idx accepted-and-ignored, 1-based **UTF-16** start |
| `split_part` | `spark_split_part.rs:1` (F-6c) — *"DataFusion's kernel is `split_part(Utf8, Utf8, Int64)` with integer-only coercion … so `split_part('a.b.c', '.', '2')` is a planning error. Spark 4.1.2 casts the STRING"* |
| `element_at` | `collection.rs:3` (audit #15) — *"DataFusion registers `element_at` only as an alias of `map_extract`, so on arrays it fails type coercion for every index, and on maps it returns `map_extract`'s list-wrapped value instead of Spark's plain value"*. Index 0 → `INVALID_INDEX_OF_ZERO` |
| `now` `current_timestamp` `to_timestamp` | `instant_ts.rs:1` (TZ-4 PR-1/PR-2) — overwrite with Arrow `Timestamp(µs, UTC)`; PR-2 localizes zoneless LTZ inputs in the session zone; *"A zone-suffixed string is already an instant (do not localize — H-1a double-shift trap)"* |
| `to_date` | `timestamp_cast.rs:133` (TZ-8) — *"same session-zone kernel as CAST for a TIMESTAMP argument"* |
| `make_date` | `datetime.rs:396` — Spark `make_date(y,m,d) -> DATE`; invalid triple → NULL *"matching Spark with ANSI mode off"* (DF-core `make_date` differs on invalid input) |
| `date_format` | `datetime.rs` — Java-pattern → `Utf8`; DF-core `date_format` is only an alias of `to_char` (strftime patterns). r24 A3 PERF-02 compiles the pattern once per invocation |
| `rand` (+`random`) `randn` | `random.rs:1` (r20 G2) — Spark `XORShiftRandom` + MurmurHash3 `hashSeed` bit-exact; DF-core `random` is unseeded. Partition index pinned at 0 (disclosed residual) |
| `trunc` `date_trunc` `concat` `substring`/`substr` | also shadow DF-core (see 2a). Note: **DataFusion's numeric `trunc(x)` is not reachable** — DF-spark shadowed it, repark re-shadowed with the date form. Spark-correct, but a real behavioural consequence of the chain |

### 2c. Green-field repark names — no collision anywhere (11)
`year` `yearofweek` `quarter` `month` `weekofyear` `dayofmonth` `day` `dayofyear` `dayofweek` `weekday` `randn`
(`lib.rs:5` — *"`datafusion-spark` (52.x) ships the calendar components … but none of the bare calendar extractors Spark SQL exposes"*.)

---

## 3. Line-count economics — `crates/repark-functions/src`

**Totals (27 `.rs` files):** **15,132 lines** — **5,110 test (33.8 %)** / **10,022 non-test**.
(`#[cfg(test)]` block spans, brace-matched; `session_time_zone/tests.rs` counted as test — it is a `#[cfg(test)] mod tests;` file-backed module.)

Largest files: `datetime.rs` 2053 (682 test) · `analyzer.rs` 1407 (896) · `instant_ts.rs` 894 (204) · `decimal_spark.rs` 827 (215) · `timestamp_cast.rs` 827 (232) · `aggregate.rs` 794 (353) · `cardinality.rs` 761 · `string.rs` 724 · `decimal_precision.rs` 705 · `java_uri.rs` 689 · `url.rs` 626 · `collection.rs` 578 · `spark_regexp.rs` 573 · `spark_length.rs` 524 · `str_to_map.rs` 446 · `expr_fn.rs` 388 (0 test) · `random.rs` 356 · `ansi.rs` 322 · `shuffle.rs` 263 · `timestamp_type.rs` 263 · `analyzer/cast_legality.rs` 260 · `spark_split_part.rs` 220 · `map_from_entries.rs` 185 · `lib.rs` 171 (hard 175 ceiling, `scripts/check_lib_rs.py`) · `session_time_zone.rs` 158 · `shim_macros.rs` 26.

**Two ways to price one more kernel:**

**(a) Narrow — the `impl ScalarUDFImpl`/`AggregateUDFImpl` body plus its struct, ctor and helpers, non-test only. n = 31 kernels.**

> **min 52 · median 75 · max 170 · mean 82.3 · total 2,552**

| kernel | file | lines | | kernel | file | lines |
|---|---|---|---|---|---|---|
| `SparkAvgWithRetract` | aggregate.rs | 170 | | `SparkNow` | instant_ts.rs | 73 |
| `SparkArrayGet` | collection.rs | 133 | | `AnsiNonzeroDivisor` | ansi.rs | 71 |
| `SparkSubstring` | string.rs | 121 | | `ReparkShuffle` | shuffle.rs | 70 |
| `DateFormat` | datetime.rs | 117 | | `SparkConcat` | string.rs | 67 |
| `DateTrunc` | datetime.rs | 115 | | `SparkStrToMapRegex` | str_to_map.rs | 64 |
| `SparkParseUrl` | url.rs | 115 | | `SparkTimestampToDate` | timestamp_cast.rs | 60 |
| `SparkEpochSecondsFloor` | timestamp_cast.rs | 108 | | `ReparkMapFromEntries` | map_from_entries.rs | 57 |
| `DatePartUdf` (10 names) | datetime.rs | 98 | | `SparkBitLength` | spark_length.rs | 56 |
| `AddMonths` | datetime.rs | 93 | | `SparkOctetLength` | spark_length.rs | 56 |
| `SparkGetItem` | collection.rs | 89 | | `SparkRand` | random.rs | 54 |
| `SparkElementAt` | collection.rs | 84 | | `SparkRandn` | random.rs | 54 |
| `MakeDate` | datetime.rs | 84 | | `SparkToDate` | timestamp_cast.rs | 54 |
| `SparkTimestampToString` | timestamp_cast.rs | 82 | | `SparkRegexpCount` | spark_regexp.rs | 52 |
| `TruncDate` | datetime.rs | 78 | | `SparkRegexpInstr` | spark_regexp.rs | 52 |
| `SparkEpochSecondsReal` | timestamp_cast.rs | 75 | | `SparkSplitPart` | spark_split_part.rs | 76 |
| `SparkToTimestamp` | instant_ts.rs | 74 | | | | |

**(b) Fully loaded — whole file (kernel + shared helpers + module doc + tests) ÷ kernels in that file. This is the honest planning number.**

> **min 165 · median 287 · max 1,315**

| file | kernels | file lines | per kernel |
|---|---|---|---|
| `url.rs` + `java_uri.rs` | 1 | 1315 | **1315** |
| `aggregate.rs` | 1 | 794 | 794 |
| `instant_ts.rs` | 2 | 894 | 447 |
| `str_to_map.rs` | 1 | 446 | 446 |
| `string.rs` | 2 | 724 | 362 |
| `datetime.rs` | 6 | 2053 | 342 |
| `ansi.rs` | 1 | 322 | 322 |
| `spark_regexp.rs` | 2 | 573 | 287 |
| `shuffle.rs` | 1 | 263 | 263 |
| `spark_length.rs` | 2 | 524 | 262 |
| `spark_split_part.rs` | 1 | 220 | 220 |
| `collection.rs` | 3 | 578 | 193 |
| `map_from_entries.rs` | 1 | 185 | 185 |
| `random.rs` | 2 | 356 | 178 |
| `timestamp_cast.rs` | 5 | 827 | 165 |

**Planner read.** A *thin wrapper* over an upstream kernel (`shuffle`, `map_from_entries`, `split_part`) is **185–263 lines all-in**. A *re-kernel from the Spark/Java source of truth* (`url.rs` on `java.net.URI`, `aggregate.rs` on 4 decimal widths, `instant_ts.rs` on session-zone localization) is **450–1,300**. The **median new kernel is ~287 lines, ~1/3 of it tests**, and it does **not** touch `lib.rs` (at its 175-line ceiling — FN-GT2 X8 paid for `pub mod url;` by moving `shim_macros.rs` out) if it lands as a `#[path]` sibling registered from an existing `functions()` vec. Hidden fixed costs per kernel: one `expr_fn.rs` builder (~6 lines, 0 tests), one `function_dispatch.rs` arm (~4 lines), one `map.md` paragraph, one facade `functions_*.py` export, and — if it diverges — one row in `docs/spark-sql-iceberg-parity.md` with a pin.

---

## 4. Where facade semantics rest on a DataFusion-core function known to differ from Spark

Cross-referenced against `docs/spark-sql-iceberg-parity.md`.

### 4a. Registry-level: pinned divergences whose owner is DATAFUSION_CORE

| parity row | name(s) | provenance | divergence |
|---|---|---|---|
| **G5-RANK-TYPE-1/2/3** | `rank` `row_number` `ntile` | **DATAFUSION_CORE** (`datafusion-functions-window`) | repark yields `uint64`, Spark `int32`. *"SQL door leaves DataFusion UInt64; DF-API door already casts row_number to IntegerType."* Nothing shadows these — DF-spark's window set is empty in 54.1, so **this class is 100 % inherited**. Fixing it = repark taking ownership of 3–11 window names |
| **FLOAT-AGG-1/2** | `sum` `avg` | **DATAFUSION_CORE** (`sum_udaf`, and `avg_udaf` on the facade path — see 4b) | `sum` lands 3.75 where Spark lands 2.25 at `shuffle.partitions=2`; `avg` 0.46875 vs 0.28125. Accumulation-order sensitivity |
| **G12-1 / G12-2** | `<=>` / `eqNullSafe` | **DATAFUSION_CORE** `Operator::IsNotDistinctFrom` (`function_dispatch.rs:244`) | result marked nullable; Spark non-nullable |
| **DEC-9** | binary `*` `+` on DECIMAL | DataFusion-core arithmetic nullability | repark marks overflow-capable results non-null; Spark nullable |
| **G5b-R4** | temporal `RANGE` FOLLOWING→FOLLOWING | DataFusion 54.1.0 window range search | frame includes the current row (120 vs Spark 90). `repark-spark/src/window_range.rs` deliberately fixes R1/R2/R3/R5 and **leaves R4 open** — *"sqlparser 0.62 `WindowFrame` is `// TBD: EXCLUDE`; no dependency bump"* |
| **G18-1 / G18-3 / G10-3** | `make_array`, array construction | DATAFUSION_CORE / arrow-rs | list value field named `item`; Spark `element` |
| **G18-2** | `collect_list` | **DATAFUSION_SPARK** | `list<item: int64>` nullable vs Spark `list<element: int64 not null>` |
| **BL-6** | `bin` `rint` | **DATAFUSION_SPARK** (`spark_math::bin`/`rint` via `repark_functions::expr_fn`) | facade wraps in `CAST(… AS BIGINT/DOUBLE)`, so BOOLEAN is silently accepted; Spark analysis-refuses with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` |
| **B-TZ-3** (surfaced, unpinned) | `date_add` | **DATAFUSION_SPARK** | `date_add(DATE, <int literal>)` refuses in the SQL door (`SparkDateAdd` takes Int8/16/32; a SQL literal is Int64). The facade works **only because** `expr_fn.rs:date_add` inserts an `Int32` cast — a repark-side compensation the SQL door never sees. Same shape for `factorial` (`expr_fn.rs` casts to Int32) |
| **B-TZ-1 / B-TZ-2** | `unix_timestamp` `timestamp_seconds` | **DATAFUSION_CORE**, facade-only | facade lowers to `expr_fn::to_timestamp_seconds`; there is **no SQL-door spelling at all** |

### 4b. Kernel-level: a REPARK_OWNED kernel whose *inner* dependency is DF-core

- **BL-7** — `bit_length` / `octet_length` on DOUBLE. The kernel is repark's (`spark_length.rs`), but the stringify step is the **Arrow `float64 → utf8` cast**: `CAST('Infinity' AS DOUBLE)` → `'inf'` (3) and `1.0E21` → `'1e21'` (4); Spark's `Double.toString` gives `'Infinity'` (8) and `'1.0E21'` (6). *Owning the kernel did not buy ownership of the formatting.*
- **FN-1** — `element_at` OOB. repark's `SparkElementAt` *"delegates to DataFusion's `array_element` kernel"*; under ANSI (repark's default TRUE) Spark raises `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` and repark returns NULL. `ansi.rs`'s implemented scope is only `/` and `%` by zero.
- **FN-2** — `make_date` invalid Y-M-D → NULL, Spark raises `DATETIME_FIELD_OUT_OF_BOUNDS`. Same ANSI-scope root cause; the kernel is repark's.
- **G6-4** — `CAST(TIMESTAMP AS INT)` value/type agree, nullability differs. repark owns the *scale* rewrite (`__repark_epoch_seconds_floor__`) but explicitly *"never the cast-FAILURE surface"* — nullability stays DataFusion's.
- **`url.rs` X8 RESIDUAL** — the `QUERY` key is a `java.util.regex` pattern on Spark and a `regex`-crate pattern here; the `regex` crate is a finite automaton, so `a(?=1)` `(?<=&)b` `(a)\1` `(?>a)` `\Qa\E` compile on Java and **raise** here. Pinned by `url::tests::parse_url_query_key_regex_dialect_residual`. Same class in `str_to_map`.

### 4c. **Two-door asymmetry — the facade lowers to DF-core where the SQL door resolves DF-spark or repark**

Not currently a parity-registry class, and the highest-value finding here for a planner. From `function_dispatch.rs::call_scalar_expr`:

**Facade → DATAFUSION_CORE while SQL door → DATAFUSION_SPARK (17 names):**
`ascii` `base64`(=`encode(x,'base64')`) `unbase64` `ceil`/`ceiling` `floor` `round` `length`/`character_length` `like` `ilike` `elt`(=`make_array`+`array_element`) `size`/`cardinality` `sec`(=`1/cos`) `csc`(=`1/sin`) `slice` `array_repeat` `array_contains`/`array_has` `date_part`/`datepart`

**Facade → DATAFUSION_CORE while SQL door → REPARK_OWNED (2, semantically live):**
- **`to_timestamp`** — `function_dispatch.rs:311` calls `expr_fn::to_timestamp` (DF-core), so the facade **bypasses `instant_ts::SparkToTimestamp`** and its TZ-4 PR-1 `Timestamp(µs, UTC)` typing + PR-2 session-zone localization. `F.to_timestamp(...)` and `spark.sql("SELECT to_timestamp(...)")` are not the same kernel. Directly adjacent to the open **TZ-4 / TZ-7** rows.
- **`avg`** — `unary_aggregate_udaf("avg")` returns `datafusion::functions_aggregate::average::avg_udaf()` (DF-core), so `F.avg()` bypasses `SparkAvgWithRetract`. `aggregate.rs` notes DF-core's `Avg` already carries the `(p+4, s+4)` decimal rule and decimal retract, so the delta is narrower than it looks — but the shim's Spark i64-count / null-on-empty arm is not on the facade path. This is also the kernel behind **FLOAT-AGG-2**.
- Adjacent: **`concat`** is a dedicated `PyColumn::concat` pymethod (`repark-python/src/column/mod.rs:303`) that calls `datafusion::functions::expr_fn::concat` and re-implements the `Utf8` cast + null-propagation `CASE` at the PyO3 layer, rather than embedding `string::concat_udf()`. Same D2 semantics, **two implementations** — a drift risk, not a live divergence.

By contrast the FN-GT1/GT2 rounds deliberately closed this seam for `substr`/`substring`, `element_at`, `shuffle`, `map_from_entries`, `str_to_map`, `parse_url`, `try_parse_url`, `split_part`, `regexp_count`, `regexp_instr`, `bit_length`, `octet_length`, `rand`, `randn`, `to_date`, `make_date` — *"both doors resolve the same UDF"*. `to_timestamp` and `avg` are the two remaining holes in that policy.

---

### Key files (absolute)
- `crates/repark-functions/src/lib.rs` — `register_all` (registration order = precedence)
- `crates/repark-functions/src/expr_fn.rs` — 388 lines, 0 tests; the facade-embed builders and the only place a `datafusion-spark` kernel gets a repark-side argument cast
- `crates/repark-functions/src/map.md` — 26 KB, already the de-facto per-kernel rationale ledger
- `crates/repark-python/src/column/function_dispatch.rs` — 906 lines, 144 arms
- `crates/repark-ta/src/udf/mod.rs:243` — the 81-row `SPECS` table
- `docs/spark-sql-iceberg-parity.md` — 1,581 lines, the divergence registry
- `<cargo-registry>/datafusion-spark-54.1.0/src/lib.rs:176` — `all_default_scalar_functions`