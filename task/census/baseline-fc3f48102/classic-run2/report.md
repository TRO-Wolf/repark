# PySpark-suite compatibility report (C2 / R-PYSPARK-COMPAT)

- **Generated:** 2026-08-08T17:44:35Z
- **pyspark version:** `4.1.2`
- **Spark test source tag:** `v4.1.2` (commit `f0bb2e6a47d0ebda424ffd633fcea8644a597954`)
- **repark version:** `0.0.0`
- **Python:** `3.12.3`

## Denominators (charter — both required)

- **pass / all_collected** = **142 / 345** (41.16%)
- **pass / engine-relevant** = **142 / 257** (55.25%) where engine-relevant = all - SKIP-UPSTREAM - NEEDS-JVM - HARNESS
  - excluded SKIP-UPSTREAM=1, NEEDS-JVM=87, HARNESS=0
  - note: `MODULE-TIMEOUT` stays in engine-relevant (charter formula; wall is ops/harness but not listed among the three exclusions)

## Ranked census (class, count)

| Class | Count |
|---|---:|
| `PASS` | 142 |
| `NEEDS-JVM` | 87 |
| `FAIL-ERROR-CLASS` | 56 |
| `FAIL-VALUE` | 33 |
| `FAIL-MISSING` | 26 |
| `SKIP-UPSTREAM` | 1 |

## Per-module totals

| Module | Tests | PASS | Wall (s) | Timed out |
|---|---:|---:|---:|:---:|
| `test_functions` | 137 | 45 | 1.0 | no |
| `test_dataframe` | 60 | 34 | 0.9 | no |
| `test_types` | 104 | 42 | 0.3 | no |
| `test_column` | 28 | 15 | 0.2 | no |
| `test_readwriter` | 16 | 6 | 0.7 | no |

## Patch map (redirect seam)

| Target | Source | Kind | Notes |
|---|---|---|---|
| `pyspark.sql.SparkSession` | `repark.session.SparkSession (= ReparkSession)` | replace | Builder + session type used by suite factories after ReusedSQLTestCase patch. |
| `pyspark.sql.session.SparkSession` | `repark.session.SparkSession` | replace | Same class object as pyspark.sql.SparkSession. |
| `pyspark.sql.classic.session.SparkSession` | `repark.session.SparkSession` | replace | Spark 4 classic submodule (when importable). |
| `pyspark.sql.DataFrame` | `repark.dataframe.DataFrame` | replace | So `from pyspark.sql import DataFrame` binds repark's type. |
| `pyspark.sql.classic.dataframe.DataFrame` | `repark.dataframe.DataFrame` | replace | Spark 4 classic dataframe submodule (when importable). |
| `pyspark.sql.Row` | `repark.row.Row` | replace | Row identity for createDataFrame / collect comparisons. |
| `pyspark.sql.column.Column` | `repark.column.Column` | replace | Column type shared with repark.functions; submodule + package attribute. |
| `pyspark.sql.dataframe.DataFrame` | `repark.dataframe.DataFrame` | replace | Submodule path for `from pyspark.sql.dataframe import DataFrame`. |
| `pyspark.sql.classic.column.Column` | `repark.column.Column` | replace | Spark 4 classic column submodule (when importable). |
| `pyspark.sql.types.* (overlay)` | `repark.types public names` | overlay | Only names repark implements; missing Spark types stay pyspark (often NEEDS-JVM). |
| `pyspark.sql.functions.* (overlay)` | `repark.functions public names` | overlay | Overlay onto classic functions module + package __init__; submodules (avro, builtin extras) stay pyspark and usually FAIL-MISSING / NEEDS-JVM. |
| `pyspark.errors.* (overlay)` | `repark.errors public exception classes` | overlay | AnalysisException / PySparkTypeError / PySparkAssertionError / … identity for except clauses and check_error isinstance; testing.utils names rebound when already imported (C4 expand2 assert* / assertSchemaEqual). |
| `pyspark.testing.sqlutils.ReusedSQLTestCase.setUpClass` | `compat bootstrap factory → ReparkSession.builder.getOrCreate()` | factory | Does NOT start SparkContext / JVM. cls.sc is repark's minimal SparkContext. |
| `pyspark.testing.sqlutils.ReusedSQLTestCase.tearDownClass` | `compat bootstrap (session.stop + tempdir cleanup)` | factory | Skips JVM sc.stop(). |
| `pyspark.testing.sqlutils.ReusedSQLTestCase.tearDown` | `compat bootstrap no-op (skip _jsparkSession cleanup)` | factory | Original tearDown calls JVM-only cleanupPythonWorkerLogs. |
| `pyspark.testing.utils.ReusedPySparkTestCase.setUpClass` | `compat bootstrap (no SparkContext)` | factory | Prevents parent setUpClass from launching a gateway if a subclass chains super. |
| `pyspark.sql.tests (package)` | `~/.cache/repark-pyspark-tests/<tag>/python/pyspark/sql/tests` | inject-package | Installed pyspark wheel does not ship sql/tests; injected from cache only. |

### Patch log (runtime)

- replace: pyspark.sql.SparkSession → repark.session.SparkSession
- replace: pyspark.sql.{DataFrame,Row,Column,Window} → repark
- replace: pyspark.sql.column.Column → repark
- replace: pyspark.sql.dataframe.DataFrame → repark
- replace: pyspark.sql.classic.dataframe.DataFrame → repark
- replace: pyspark.sql.classic.column.Column → repark
- overlay: pyspark.sql.types ← repark.types (28 names)
- replace: pyspark.sql.types._merge_type → repark.types._merge_type
- replace: pyspark.sql.types._make_type_verifier → repark.types._make_type_verifier
- replace: pyspark.storagelevel.StorageLevel → repark.storage.StorageLevel
- overlay: pyspark.sql.functions ← repark.functions (207 names)
- overlay: pyspark.errors ← repark.errors (11 names)
- overlay: pyspark.errors.exceptions.base ← repark.errors (11 names)
- factory: patched ReusedSQLTestCase + ReusedPySparkTestCase session lifecycle
- inject-package: loaded pyspark.sql.tests from <home>
- factory: ReusedPySparkTestCase.setUpClass skipped JVM for FunctionsTests
- factory: ReusedSQLTestCase.setUpClass → ReparkSession for FunctionsTests

## Non-PASS rows

Every non-PASS: test id, class, one-line cause, first divergent frame.

| Test id | Class | Cause | Frame |
|---|---|---|---|
| `pyspark.sql.tests.test_functions.FunctionsTests.test_assert_true` | `NEEDS-JVM` | AssertionError | `<home> in test_assert_true` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_basic_functions` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_basic_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_binary_math_function` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_binary_math_function` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_bit_length_function` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_bit_length_function` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_collation` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_collation` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_convert_timezone` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_convert_timezone` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_current_time` | `NEEDS-JVM` | AssertionError | `<home> in test_current_time` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_current_timestamp` | `NEEDS-JVM` | AssertionError | `<home> in test_current_timestamp` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_current_user` | `NEEDS-JVM` | AssertionError | `<home> in test_current_user` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_enum_literals` | `FAIL-ERROR-CLASS` | [NOT_INT] arg_name='seed', arg_type='IntEnum' | `<home> in test_enum_literals` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_explode` | `FAIL-VALUE` | Error during planning: unnest() can only be applied to array, struct and null | `<home> in test_explode` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_expr` | `FAIL-VALUE` | Schema error: No field named a. | `<home> in test_expr` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_function_parity` | `NEEDS-JVM` | repark SparkContext has no attribute '_jvm' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_function_parity` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_functions_broadcast` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_functions_broadcast` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_higher_order_function_failures` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_higher_order_function_failures` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_inline` | `NEEDS-JVM` | AssertionError | `<home> in test_inline` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_input_file_name_reset_for_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'textFile' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_input_file_name_reset_for_rdd` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_input_file_name_udf` | `FAIL-MISSING` | 'DataFrameReader' object has no attribute 'text' | `<home> in test_input_file_name_udf` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_merge_agg_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_merge_agg_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_merge_agg_double` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'DOUBLE'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_merge_agg_double` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_merge_agg_float` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'FLOAT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_merge_agg_float` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_merge_agg_with_different_k` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_merge_agg_with_different_k` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_merge_agg_with_nulls` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_merge_agg_with_nulls` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_agg_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_agg_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_agg_double` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'DOUBLE'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_agg_double` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_agg_float` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'FLOAT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_agg_float` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_double_variants` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'DOUBLE'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_double_variants` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_float_variants` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'FLOAT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_float_variants` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_get_n_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_get_n_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_get_quantile_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_get_quantile_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_get_quantile_bigint_array` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_get_quantile_bigint_array` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_get_rank_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_get_rank_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_merge_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_merge_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_to_string_bigint` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'INT'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_kll_sketch_to_string_bigint` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_kll_sketch_with_nulls` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_kll_sketch_with_nulls` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_levenshtein_function` | `FAIL-ERROR-CLASS` | levenshtein() takes 2 positional arguments but 3 were given | `<home> in test_levenshtein_function` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_listagg_distinct_functions` | `NEEDS-JVM` | AssertionError | `<home> in test_listagg_distinct_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_listagg_functions` | `NEEDS-JVM` | AssertionError | `<home> in test_listagg_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_lit_day_time_interval` | `FAIL-ERROR-CLASS` | lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, ndarray, or Enum; got timedelta | `<home> in test_lit_day_time_interval` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_make_date` | `NEEDS-JVM` | AssertionError | `<home> in test_make_date` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_make_time` | `NEEDS-JVM` | AssertionError | `<home> in test_make_time` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_make_timestamp` | `FAIL-MISSING` | functions.make_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3) | `<home> in test_make_timestamp` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_make_timestamp_ntz` | `NEEDS-JVM` | AssertionError | `<home> in test_make_timestamp_ntz` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_map_concat` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_map_concat` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_map_functions` | `FAIL-VALUE` | SQL error: ParserError("Expected: ), found: as at Line: 1, Column: 29") | `<home> in test_map_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_nested_higher_order_function` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_nested_higher_order_function` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_non_deterministic_with_seed` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_non_deterministic_with_seed` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_np_scalar_input` | `FAIL-ERROR-CLASS` | lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, ndarray, or Enum; got int8 | `<home> in test_np_scalar_input` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_nth_value` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_nth_value` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_nullifzero_zeroifnull` | `NEEDS-JVM` | AssertionError | `<home> in test_nullifzero_zeroifnull` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_octet_length_function` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_octet_length_function` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_parse_json` | `NEEDS-JVM` | AssertionError | `<home> in test_parse_json` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_percentile` | `NEEDS-JVM` | AssertionError | `<home> in test_percentile` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_percentile_approx` | `FAIL-ERROR-CLASS` | percentile_approx percentage must be float or sequence of float, got Column | `<home> in test_percentile_approx` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_raise_error` | `FAIL-MISSING` | functions.raise_error evaluation is not supported yet (engine raise kernel deferred; disclosed E1) | `<home> in test_raise_error` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_rand_functions` | `FAIL-ERROR-CLASS` | '<' not supported between instances of 'Row' and 'Row' | `<home> in test_rand_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_randstr_uniform` | `NEEDS-JVM` | AssertionError | `<home> in test_randstr_uniform` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_session_window` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_session_window` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_shiftleft` | `NEEDS-JVM` | AssertionError | `<home> in test_shiftleft` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_shiftright` | `NEEDS-JVM` | AssertionError | `<home> in test_shiftright` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_shiftrightunsigned` | `NEEDS-JVM` | AssertionError | `<home> in test_shiftrightunsigned` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_slice` | `FAIL-VALUE` | Error during planning: Failed to coerce arguments to satisfy a call to 'array_slice' function: coercion from List(Int64), Decimal128(20, 0), Decimal128(22, 0) to the signature OneOf(ArraySignature(array, index, index), ArraySignature(arr... | `<home> in test_slice` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_sort_with_nulls_order` | `NEEDS-JVM` | AssertionError | `<home> in test_sort_with_nulls_order` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_sorting_functions_with_column` | `NEEDS-JVM` | AssertionError | `<home> in test_sorting_functions_with_column` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_st_asbinary` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_st_asbinary` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_st_setsrid` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_st_setsrid` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_st_srid` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_st_srid` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_string_validation` | `NEEDS-JVM` | AssertionError | `<home> in test_string_validation` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_sum_distinct` | `NEEDS-JVM` | AssertionError | `<home> in test_sum_distinct` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_time_diff` | `NEEDS-JVM` | AssertionError | `<home> in test_time_diff` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_time_trunc` | `NEEDS-JVM` | AssertionError | `<home> in test_time_trunc` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_to_time` | `NEEDS-JVM` | AssertionError | `<home> in test_to_time` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_to_timestamp_ltz` | `NEEDS-JVM` | AssertionError | `<home> in test_to_timestamp_ltz` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_to_timestamp_ntz` | `NEEDS-JVM` | AssertionError | `<home> in test_to_timestamp_ntz` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_to_variant_object` | `NEEDS-JVM` | AssertionError | `<home> in test_to_variant_object` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_datetime_functions` | `NEEDS-JVM` | AssertionError | `<home> in test_try_datetime_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_make_interval` | `NEEDS-JVM` | AssertionError | `<home> in test_try_make_interval` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_make_timestamp` | `NEEDS-JVM` | AssertionError | `<home> in test_try_make_timestamp` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_make_timestamp_ltz` | `NEEDS-JVM` | AssertionError | `<home> in test_try_make_timestamp_ltz` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_make_timestamp_ntz` | `NEEDS-JVM` | AssertionError | `<home> in test_try_make_timestamp_ntz` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_parse_json` | `NEEDS-JVM` | AssertionError | `<home> in test_try_parse_json` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_parse_url` | `NEEDS-JVM` | AssertionError | `<home> in test_try_parse_url` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_try_to_time` | `NEEDS-JVM` | AssertionError | `<home> in test_try_to_time` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_variant_expressions` | `NEEDS-JVM` | AssertionError | `<home> in test_variant_expressions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_version` | `NEEDS-JVM` | AssertionError | `<home> in test_version` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_wildcard_import` | `FAIL-VALUE` | Items in the second set but not the first: 'currentTimestamp' 'currentDate' | `<home> in test_wildcard_import` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_window` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window_functions` | `FAIL-ERROR-CLASS` | '<' not supported between instances of 'Row' and 'Row' | `<home> in test_window_functions` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window_functions_cumulative_sum` | `FAIL-ERROR-CLASS` | '<' not supported between instances of 'Row' and 'Row' | `<home> in test_window_functions_cumulative_sum` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window_functions_moving_average` | `FAIL-VALUE` | [DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE] Cannot resolve RANGE window frame due to data type mismatch: The data type of the order key 'date' ('timestamp') does not match the expected data type ("NUMERIC" or "INTERVAL"). ... | `<home> in test_window_functions_moving_average` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window_functions_without_partitionBy` | `FAIL-ERROR-CLASS` | '<' not supported between instances of 'Row' and 'Row' | `<home> in test_window_functions_without_partitionBy` |
| `pyspark.sql.tests.test_functions.FunctionsTests.test_window_time` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_window_time` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_cache_table` | `FAIL-MISSING` | 'Catalog' object has no attribute 'isCached' | `<home> in test_cache_table` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_coalesce_hints_with_string_parameter` | `FAIL-ERROR-CLASS` | createDataFrame expects a list of rows, a pandas DataFrame, or a polars DataFrame, got zip | `<home> in test_coalesce_hints_with_string_parameter` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_create_df_with_collation` | `FAIL-VALUE` | 2 != 1 | `<home> in test_create_df_with_collation` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_dataframe_star` | `FAIL-VALUE` | Schema error: No field named "*". Valid fields are __repark_x_l_2ac7b0a9f20e434ca888cbf60de94002.a, __repark_x_r_de92c5b85da9493ebbdd0eab34411d8c.a, __repark_x_r_de92c5b85da9493ebbdd0eab34411d8c.b, __repark_x_l_2ac7b0a9f20e434ca888cbf60d... | `<home> in test_dataframe_star` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_df_merge_into` | `SKIP-UPSTREAM` | org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog' is not available. Will skip the related tests | `` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_column_name_with_dot` | `FAIL-VALUE` | Schema error: No field named first.name. Column names are case sensitive. You can use double quotes to refer to the "first.name" column or set the datafusion.sql_parser.enable_ident_normalization configuration. Valid fields are id, "firs... | `<home> in test_drop_column_name_with_dot` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_notexistent_col` | `FAIL-VALUE` | Lists differ: ['colA', 'colC', 'colC', 'colD', 'colE'] != ['colA', 'colB', 'colC', 'colC', 'colD', 'colE'] First differing element 1: 'colC' 'colB' Second list contains 1 additional elements. First extra element 5: 'colE' - ['colA', 'col... | `<home> in test_drop_notexistent_col` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_duplicate_field_names` | `FAIL-MISSING` | createDataFrame schema has duplicate names; ambiguous schema bind is not supported | `<home> in test_duplicate_field_names` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_duplicated_column_names` | `FAIL-VALUE` | unique expression names required; createDataFrame schema has duplicate column names | `<home> in test_duplicated_column_names` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_extended_hint_types` | `FAIL-VALUE` | 0 not greater than or equal to 1 | `<home> in test_extended_hint_types` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_generic_hints` | `FAIL-VALUE` | 1 != 0 | `<home> in test_generic_hints` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_help_command` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_help_command` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_input_files` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `inputFiles` is not supported. | `<home> in test_input_files` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_lateral_column_alias` | `FAIL-VALUE` | Schema error: No field named x. Valid fields are id, "generate_series()".value. | `<home> in test_lateral_column_alias` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_local_checkpoint_dataframe` | `FAIL-VALUE` | 'ExistingRDD' not found in "Row(plan_type='logical_plan', plan='SubqueryAlias: __repark_explain_3c00ff495d254177ad50594b4b0564f4\\n TableScan: __repark_ckpt_79d64db9452343aeb9475dd87a5131ff projection=[id]')\nRow(plan_type='physical_plan... | `<home> in test_local_checkpoint_dataframe` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_metadata_column` | `FAIL-VALUE` | NamespaceNotFound => No such namespace: NamespaceIdent(["testcat"]) | `<baseline>/census-venv/lib/python3.12/site-packages/pyspark/testing/sqlutils.py:131 in table` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_pandas_api` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `pandas_api` is not supported. | `<home> in test_pandas_api` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_query_execution_unsupported_in_classic` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `executionInfo` is not supported. | `<home> in test_query_execution_unsupported_in_classic` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_repr_behaviors` | `FAIL-VALUE` | '+---[24 chars]\n+-----+-----+\n\| 1\| 1\|\n\|22222\|22222\|\n+-----+-----+\n' != '+---[24 chars]\n+-----+-----+\n\| 1\| 1\|\n\|22222\|22222\|\n+-----+-----+' +-----+-----+ \| key\|value\| +-----+-----+ \| 1\| 1\| \|22222\|22222\| +-----+-----+ - | `<home> in test_repr_behaviors` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_select_join_keys` | `FAIL-MISSING` | unsupported join type "cross" (supported: 'inner', 'left', 'right', 'full') | `<home> in test_select_join_keys` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_to` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `to` is not supported. | `<home> in test_to` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_toDF_with_schema_string` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_toDF_with_schema_string` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_transpose` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `transpose` is not supported. | `<home> in test_transpose` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_transpose_with_invalid_index_columns` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `transpose` is not supported. | `<home> in test_transpose_with_invalid_index_columns` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_with_column_and_generator` | `FAIL-MISSING` | to_date(format=...) is not supported yet (engine Chrono patterns ≠ Spark Java patterns; use format-less to_date or SQL) | `<home> in test_with_column_and_generator` |
| `pyspark.sql.tests.test_dataframe.DataFrameTests.test_with_columns_renamed_with_duplicated_names` | `FAIL-VALUE` | [AMBIGUOUS_REFERENCE] Reference `value` is ambiguous, could be: [`value`, `value`]. | `<home> in test_with_columns_renamed_with_duplicated_names` |
| `pyspark.sql.tests.test_types.DataTypeVerificationTests.test_verify_type_not_nullable` | `FAIL-ERROR-CLASS` | verify_type((1, 2), ArrayType(IntegerType(), True), nullable=False) | `<home> in test_verify_type_not_nullable` |
| `pyspark.sql.tests.test_types.TypesTests.test_access_nested_types` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_access_nested_types` |
| `pyspark.sql.tests.test_types.TypesTests.test_apply_schema` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_apply_schema` |
| `pyspark.sql.tests.test_types.TypesTests.test_apply_schema_to_dict_and_rows` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_apply_schema_to_dict_and_rows` |
| `pyspark.sql.tests.test_types.TypesTests.test_apply_schema_to_row` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_apply_schema_to_row` |
| `pyspark.sql.tests.test_types.TypesTests.test_apply_schema_with_nullable_udt` | `FAIL-ERROR-CLASS` | dataType ExamplePointUDT() should be an instance of <class 'repark.types.DataType'> | `<home> in test_apply_schema_with_nullable_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_apply_schema_with_udt` | `FAIL-ERROR-CLASS` | dataType ExamplePointUDT() should be an instance of <class 'repark.types.DataType'> | `<home> in test_apply_schema_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_calendar_interval_type` | `FAIL-VALUE` | StringType() != CalendarIntervalType() | `<home> in test_calendar_interval_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_calendar_interval_type_with_sf` | `NEEDS-JVM` | AssertionError | `<home> in test_calendar_interval_type_with_sf` |
| `pyspark.sql.tests.test_types.TypesTests.test_cast_to_string_with_udt` | `FAIL-ERROR-CLASS` | dataType ExamplePointUDT() should be an instance of <class 'repark.types.DataType'> | `<home> in test_cast_to_string_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_cast_to_udt_with_udt` | `FAIL-ERROR-CLASS` | [NOT_DATATYPE_OR_STR] arg_name='dataType', arg_type='PythonOnlyUDT' | `<home> in test_cast_to_udt_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_collated_string` | `FAIL-MISSING` | This feature is not implemented: Unsupported ast node in sqltorel: Collate { expr: Value(ValueWithSpan { value: SingleQuotedString("abc"), span: Span(Location(1,8)..Location(1,13)) }), collation: ObjectName([Identifier(Ident { value: "UT... | `<home> in test_collated_string` |
| `pyspark.sql.tests.test_types.TypesTests.test_complex_nested_udt_in_df` | `FAIL-ERROR-CLASS` | data_type must be str or DataType, got PythonOnlyUDT | `<v1-pin>/python/repark/src/repark/types.py:908 in add` |
| `pyspark.sql.tests.test_types.TypesTests.test_convert_row_to_dict` | `FAIL-MISSING` | 'dict' object has no attribute 'a' | `<home> in test_convert_row_to_dict` |
| `pyspark.sql.tests.test_types.TypesTests.test_create_dataframe_from_dataclasses` | `FAIL-ERROR-CLASS` | createDataFrame expects a list of tuples/lists, dicts, or Row, got element type User | `<home> in test_create_dataframe_from_dataclasses` |
| `pyspark.sql.tests.test_types.TypesTests.test_create_dataframe_from_objects` | `FAIL-ERROR-CLASS` | createDataFrame expects a list of tuples/lists, dicts, or Row, got element type MyObject | `<home> in test_create_dataframe_from_objects` |
| `pyspark.sql.tests.test_types.TypesTests.test_create_dataframe_schema_mismatch` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_create_dataframe_schema_mismatch` |
| `pyspark.sql.tests.test_types.TypesTests.test_daytime_interval_type` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'td interval day to second'; a bare non-DDL string would be character-iterated into column names) | `<home> in test_daytime_interval_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_create_dataframe` | `FAIL-ERROR-CLASS` | dataType GeometryType(0) should be an instance of <class 'repark.types.DataType'> | `<home> in test_geospatial_create_dataframe` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_create_dataframe_rdd` | `FAIL-ERROR-CLASS` | dataType GeometryType(0) should be an instance of <class 'repark.types.DataType'> | `<home> in test_geospatial_create_dataframe_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_encoding` | `NEEDS-JVM` | AssertionError | `<home> in test_geospatial_encoding` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_mixed_check_srid_validity` | `FAIL-ERROR-CLASS` | IllegalArgumentException not raised | `<home> in test_geospatial_mixed_check_srid_validity` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_result_encoding` | `FAIL-VALUE` | Error during planning: Invalid function 'st_geomfromwkb'. Did you mean 'list_pop_front'? | `<home> in test_geospatial_result_encoding` |
| `pyspark.sql.tests.test_types.TypesTests.test_geospatial_schema_inferrence` | `FAIL-ERROR-CLASS` | dataType GeometryType(0) should be an instance of <class 'repark.types.DataType'> | `<home> in test_geospatial_schema_inferrence` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_array_element_type_empty_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_array_element_type_empty_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_array_element_type_with_struct` | `FAIL-VALUE` | Row(f1=[Row(payment=200.5, name=None), Row(payment=None, name='A')]) != Row(f1=[{'payment': 200.5, 'name': None}, {'payment': None, 'name': 'A'}]) | `<home> in test_infer_array_element_type_with_struct` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_array_merge_element_types` | `FAIL-ERROR-CLASS` | ValueError not raised by <lambda> | `<home> in test_infer_array_merge_element_types` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_array_merge_element_types_with_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_array_merge_element_types_with_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_binary_type` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_binary_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_long_type` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_long_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_map_merge_pair_types_with_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_map_merge_pair_types_with_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_map_pair_type_empty_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_map_pair_type_empty_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_nested_dict_as_struct` | `FAIL-VALUE` | Row(f1=[Row(payment=200.5, name='A')], f2=[1, 2]) != Row(f1=[{'payment': 200.5, 'name': 'A'}], f2=[1, 2]) | `<home> in test_infer_nested_dict_as_struct` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_nested_dict_as_struct_with_rdd` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_nested_dict_as_struct_with_rdd` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_nested_schema` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_nested_schema` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_schema` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_specification` | `FAIL-VALUE` | Lists differ: ['boo[54 chars]p', 'string', 'string', 'double', 'array<doubl[170 chars]p>>'] != ['boo[54 chars]p', 'time(6)', 'interval day to second', 'doub[197 chars]p>>'] First differing element 6: 'string' 'time(6)' ['boolean', 'bigin... | `<home> in test_infer_schema_specification` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_to_local` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_schema_to_local` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_upcast_boolean_to_string` | `FAIL-ERROR-CLASS` | createDataFrame cannot build Arrow column 'a': Could not convert 'false' with type str: tried to convert to boolean | `<v1-pin>/python/repark/src/repark/session/_funcs.py:5068 in _arrow_table_from_tuples` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_upcast_float_to_string` | `FAIL-ERROR-CLASS` | createDataFrame cannot build Arrow column 'a': Could not convert '2.1' with type str: tried to convert to double | `<v1-pin>/python/repark/src/repark/session/_funcs.py:5068 in _arrow_table_from_tuples` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_upcast_int_to_string` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home> in test_infer_schema_upcast_int_to_string` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_with_udt` | `FAIL-VALUE` | <class 'repark.types.StringType'> != <class 'pyspark.testing.objects.ExamplePointUDT'> | `<home> in test_infer_schema_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_schema_with_udt_with_column_names` | `FAIL-VALUE` | <class 'repark.types.StringType'> != <class 'pyspark.testing.objects.ExamplePointUDT'> | `<home> in test_infer_schema_with_udt_with_column_names` |
| `pyspark.sql.tests.test_types.TypesTests.test_infer_variant_type` | `FAIL-VALUE` | <class 'repark.types.StringType'> != <class 'repark.types.VariantType'> | `<home> in test_infer_variant_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_negative_decimal` | `FAIL-VALUE` | unknown cast type 'decimal(1,-1)' | `<home> in test_negative_decimal` |
| `pyspark.sql.tests.test_types.TypesTests.test_nested_udt_in_df` | `FAIL-ERROR-CLASS` | elementType PythonOnlyUDT() should be an instance of <class 'repark.types.DataType'> | `<home> in test_nested_udt_in_df` |
| `pyspark.sql.tests.test_types.TypesTests.test_parquet_with_udt` | `FAIL-VALUE` | '(1.0,2.0)' != ExamplePoint(1.0,2.0) | `<home> in test_parquet_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_parse_datatype_string` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_parse_datatype_string` |
| `pyspark.sql.tests.test_types.TypesTests.test_rdd_with_udt` | `FAIL-MISSING` | DataFrame.rdd is not supported: RDD is out of scope for repark (use DataFrame API / Arrow collect) | `<home> in test_rdd_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_schema_with_bad_collations_provider` | `FAIL-ERROR-CLASS` | PySparkValueError not raised by <lambda> | `<home> in test_schema_with_bad_collations_provider` |
| `pyspark.sql.tests.test_types.TypesTests.test_schema_with_collations_json_ser_de` | `NEEDS-JVM` | 'ReparkSession' object has no attribute '_jsparkSession' | `<home> in test_schema_with_collations_json_ser_de` |
| `pyspark.sql.tests.test_types.TypesTests.test_schema_with_collations_on_non_string_types` | `FAIL-ERROR-CLASS` | PySparkTypeError not raised by <lambda> | `<home> in test_schema_with_collations_on_non_string_types` |
| `pyspark.sql.tests.test_types.TypesTests.test_simple_udt_in_df` | `FAIL-ERROR-CLASS` | data_type must be str or DataType, got PythonOnlyUDT | `<v1-pin>/python/repark/src/repark/types.py:908 in add` |
| `pyspark.sql.tests.test_types.TypesTests.test_spark48834_from_ddl_matches_udf_schema_string` | `FAIL-ERROR-CLASS` | udf returnType must be a non-empty type string | `<home> in test_spark48834_from_ddl_matches_udf_schema_string` |
| `pyspark.sql.tests.test_types.TypesTests.test_udf_with_udt` | `FAIL-MISSING` | udf '<lambda>' raised AttributeError: 'str' object has no attribute 'y' Traceback (most recent call last): File "<v1-pin>/pyth... | `<v1-pin>/python/repark/src/repark/dataframe/core.py:2036 in _run_udf_on_batch` |
| `pyspark.sql.tests.test_types.TypesTests.test_udt` | `NEEDS-JVM` | 'ReparkSession' object has no attribute '_jsparkSession' | `<home> in test_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_udt_with_none` | `FAIL-ERROR-CLASS` | udf returnType 'udt' is not a valid type: cannot parse datatype: 'udt' | `<v1-pin>/python/repark/src/repark/functions.py:2043 in _normalize_python_udf_return_type_sql` |
| `pyspark.sql.tests.test_types.TypesTests.test_union_with_udt` | `FAIL-ERROR-CLASS` | dataType ExamplePointUDT() should be an instance of <class 'repark.types.DataType'> | `<home> in test_union_with_udt` |
| `pyspark.sql.tests.test_types.TypesTests.test_variant_to_pandas` | `NEEDS-JVM` | AssertionError | `<home> in test_variant_to_pandas` |
| `pyspark.sql.tests.test_types.TypesTests.test_variant_type` | `NEEDS-JVM` | AssertionError | `<home> in test_variant_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_yearmonth_interval_type` | `FAIL-MISSING` | This feature is not implemented: Unsupported Interval Expression with last_field Some(Month) | `<home> in test_yearmonth_interval_type` |
| `pyspark.sql.tests.test_types.TypesTests.test_ym_interval_in_collect` | `FAIL-MISSING` | This feature is not implemented: Unsupported Interval Expression with last_field Some(Month) | `<home> in test_ym_interval_in_collect` |
| `pyspark.sql.tests.test_column.ColumnTests.test_alias_metadata` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `withMetadata` is not supported. | `<home> in test_alias_metadata` |
| `pyspark.sql.tests.test_column.ColumnTests.test_bitwise_operations` | `NEEDS-JVM` | AssertionError | `<home> in test_bitwise_operations` |
| `pyspark.sql.tests.test_column.ColumnTests.test_col_field_ops_representation` | `FAIL-ERROR-CLASS` | 'Column' object is not callable | `<home> in test_col_field_ops_representation` |
| `pyspark.sql.tests.test_column.ColumnTests.test_column_date_time_op` | `FAIL-VALUE` | Error during planning: Invalid function 'time'. Did you mean 'trim'? | `<home> in test_column_date_time_op` |
| `pyspark.sql.tests.test_column.ColumnTests.test_drop_fields` | `FAIL-ERROR-CLASS` | 'Column' object is not callable | `<home> in test_drop_fields` |
| `pyspark.sql.tests.test_column.ColumnTests.test_enum_literals` | `FAIL-ERROR-CLASS` | [NOT_COLUMN_OR_INT] arg_name='startPos', arg_type='IntEnum' | `<home> in test_enum_literals` |
| `pyspark.sql.tests.test_column.ColumnTests.test_expr_str_representation` | `FAIL-VALUE` | Schema error: No field named foo. | `<home> in test_expr_str_representation` |
| `pyspark.sql.tests.test_column.ColumnTests.test_field_accessor` | `FAIL-VALUE` | A column with name `r.a` cannot be resolved; available columns: ['l', 'r', 'd'] | `<home> in test_field_accessor` |
| `pyspark.sql.tests.test_column.ColumnTests.test_getitem_column` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_getitem_column` |
| `pyspark.sql.tests.test_column.ColumnTests.test_lit_delta_representation` | `FAIL-ERROR-CLASS` | lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, ndarray, or Enum; got timedelta | `<home> in test_lit_delta_representation` |
| `pyspark.sql.tests.test_column.ColumnTests.test_over_negative` | `FAIL-MISSING` | 'int' object has no attribute '_validate_at_over' | `<home> in test_over_negative` |
| `pyspark.sql.tests.test_column.ColumnTests.test_validate_column_types` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_validate_column_types` |
| `pyspark.sql.tests.test_column.ColumnTests.test_with_field` | `FAIL-ERROR-CLASS` | 'Column' object is not callable | `<home> in test_with_field` |
| `pyspark.sql.tests.test_readwriter.ReadwriterTests.test_binary_type` | `FAIL-VALUE` | b'hello' is not an instance of <class 'bytearray'> | `<home> in test_binary_type` |
| `pyspark.sql.tests.test_readwriter.ReadwriterTests.test_bucketed_write` | `FAIL-MISSING` | 'DataFrameWriter' object has no attribute 'bucketBy' | `<home> in test_bucketed_write` |
| `pyspark.sql.tests.test_readwriter.ReadwriterTests.test_cluster_by` | `FAIL-MISSING` | 'DataFrameWriter' object has no attribute 'clusterBy' | `<home> in test_cluster_by` |
| `pyspark.sql.tests.test_readwriter.ReadwriterTests.test_save_and_load` | `FAIL-MISSING` | DataFrameWriter.json option 'noUse' is not supported yet (would silently change write semantics if ignored) | `<home> in test_save_and_load` |
| `pyspark.sql.tests.test_readwriter.ReadwriterTests.test_save_and_load_builder` | `FAIL-MISSING` | DataFrameWriter.json option 'noUse' is not supported yet (would silently change write semantics if ignored) | `<home> in test_save_and_load_builder` |
| `pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_api` | `FAIL-VALUE` | <repark.dataframe.writer_readwriter.DataFrameWriterV2 object at 0x72021014c720> is not an instance of <class 'pyspark.sql.readwriter.DataFrameWriterV2'> | `<home> in test_api` |
| `pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_cluster_by` | `FAIL-ERROR-CLASS` | repark.writeTo supports only using('iceberg'), got 'parquet' | `<home> in test_cluster_by` |
| `pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_create` | `FAIL-ERROR-CLASS` | repark.writeTo supports only using('iceberg'), got 'parquet' | `<home> in test_create` |
| `pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_partitioning_functions` | `NEEDS-JVM` | [SESSION_OR_CONTEXT_NOT_EXISTS] SparkContext or SparkSession should be created first. | `<home> in test_partitioning_functions` |
| `pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_table_overwrite` | `FAIL-MISSING` | DataFrameWriterV2.overwrite(condition) is not supported — no engine path for conditional overwrite (Group I disclosure). Use createOrReplace() for a deliberate full rebuild, or DELETE + append. | `<home> in test_table_overwrite` |

## Findings (engine-relevant; zero mid-unit fixes)

- NEEDS-JVM: 87 test(s) — measurement only (C2 zero-fix)
- FAIL-ERROR-CLASS: 56 test(s) — measurement only (C2 zero-fix)
- FAIL-VALUE: 33 test(s) — measurement only (C2 zero-fix)
- FAIL-MISSING: 26 test(s) — measurement only (C2 zero-fix)

## Provenance

Apache tests loaded from cache tag `v4.1.2` @ `f0bb2e6a47d0ebda424ffd633fcea8644a597954` (runtime fetch; **not** vendored in git). Installed pyspark `4.1.2` provides the runtime package; only `pyspark.sql.tests` is injected from the cache.
