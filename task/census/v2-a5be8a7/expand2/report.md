# PySpark-suite compatibility report (C4 / R-CENSUS-EXPAND2)

- **Generated:** 2026-08-08T23:31:45Z
- **pyspark version:** `4.1.2`
- **Spark test source tag:** `v4.1.2` (commit `f0bb2e6a47d0ebda424ffd633fcea8644a597954`)
- **repark version:** `0.0.0`
- **Python:** `3.12.3`
- **Cohort:** C4 expand2 only (`test_subquery`, `test_collection`, `test_repartition`, `test_utils`, `test_errors`, `test_stat`, `test_creation`, `test_conversion`, `test_serde`) — **own denominators; never blend classic /345 or prior C3 expand**

## Denominators (charter — both required)

- **pass / all_collected** = **87 / 167** (52.10%)
- **pass / engine-relevant** = **87 / 162** (53.70%) where engine-relevant = all - SKIP-UPSTREAM - NEEDS-JVM - HARNESS
  - excluded SKIP-UPSTREAM=2, NEEDS-JVM=3, HARNESS=0
  - note: `MODULE-TIMEOUT` stays in engine-relevant (charter formula; wall is ops/harness but not listed among the three exclusions)

## Ranked census (class, count)

| Class | Count |
|---|---:|
| `PASS` | 87 |
| `FAIL-VALUE` | 40 |
| `FAIL-ERROR-CLASS` | 20 |
| `FAIL-MISSING` | 15 |
| `NEEDS-JVM` | 3 |
| `SKIP-UPSTREAM` | 2 |

## Per-module totals

| Module | Tests | PASS | Wall (s) | Timed out |
|---|---:|---:|---:|:---:|
| `test_subquery` | 28 | 1 | 0.2 | no |
| `test_collection` | 15 | 6 | 0.1 | no |
| `test_repartition` | 10 | 5 | 0.0 | no |
| `test_utils` | 73 | 57 | 0.8 | no |
| `test_errors` | 6 | 1 | 0.1 | no |
| `test_stat` | 6 | 4 | 0.4 | no |
| `test_creation` | 14 | 4 | 0.1 | no |
| `test_conversion` | 2 | 0 | 0.0 | no |
| `test_serde` | 13 | 9 | 0.2 | no |

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
- inject-package: loaded pyspark.sql.tests from <home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests
- factory: ReusedPySparkTestCase.setUpClass skipped JVM for SubqueryTests
- factory: ReusedSQLTestCase.setUpClass → ReparkSession for SubqueryTests

## Non-PASS rows

Every non-PASS: test id, class, one-line cause, first divergent frame.

| Test id | Class | Cause | Frame |
|---|---|---|---|
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_in_between_regular_joins` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:900 in test_lateral_join_in_between_regular_joins` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_inside_subquery` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:980 in test_lateral_join_inside_subquery` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_reference_preceding_from_clause_items` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:858 in test_lateral_join_reference_preceding_from_clause_items` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_aggregation_and_correlated_predicates` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:840 in test_lateral_join_with_aggregation_and_correlated_predicates` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_correlated_predicates` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:818 in test_lateral_join_with_correlated_predicates` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_different_join_types` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:750 in test_lateral_join_with_different_join_types` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_single_column_select` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:710 in test_lateral_join_with_single_column_select` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_star_expansion` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:728 in test_lateral_join_with_star_expansion` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_subquery_alias` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:804 in test_lateral_join_with_subquery_alias` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_table_valued_functions` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1018 in test_lateral_join_with_table_valued_functions` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_lateral_join_with_table_valued_functions_and_join_conditions` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1048 in test_lateral_join_with_table_valued_functions_and_join_conditions` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_multiple_lateral_joins` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:870 in test_multiple_lateral_joins` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_nested_lateral_joins` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:921 in test_nested_lateral_joins` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_noop_outer` | `FAIL-ERROR-CLASS` | 'Column' object is not callable | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:52 in test_noop_outer` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_scalar_subquery_against_local_relations` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `scalar` is not supported. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:166 in test_scalar_subquery_against_local_relations` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_scalar_subquery_inside_lateral_join` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:946 in test_scalar_subquery_inside_lateral_join` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_scalar_subquery_with_missing_outer_reference` | `FAIL-ERROR-CLASS` | `query_context_type` is required when QueryContext exists. QueryContext: []. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:683 in test_scalar_subquery_with_missing_outer_reference` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_simple_uncorrelated_scalar_subquery` | `FAIL-MISSING` | [ATTRIBUTE_NOT_SUPPORTED] Attribute `scalar` is not supported. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:69 in test_simple_uncorrelated_scalar_subquery` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_drop` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1189 in test_subquery_in_drop` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_join_condition` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1093 in test_subquery_in_join_condition` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_repartition` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1195 in test_subquery_in_repartition` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_transpose` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1125 in test_subquery_in_transpose` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_unpivot` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1102 in test_subquery_in_unpivot` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_with_columns` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1138 in test_subquery_in_with_columns` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_in_with_columns_renamed` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1173 in test_subquery_in_with_columns_renamed` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_subquery_with_generator_and_tvf` | `FAIL-VALUE` | SQL error: ParserError("Expected: end of statement, found: AS at Line: 1, Column: 23") | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:1080 in test_subquery_with_generator_and_tvf` |
| `pyspark.sql.tests.test_subquery.SubqueryTests.test_uncorrelated_scalar_subquery_with_view` | `FAIL-VALUE` | Error during planning: table 'datafusion.public.subqueryData' not found | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_subquery.py:106 in test_uncorrelated_scalar_subquery_with_view` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_local_iterator_not_fully_consumed` | `NEEDS-JVM` | repark SparkContext has no attribute '_jvm' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:350 in test_to_local_iterator_not_fully_consumed` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas` | `FAIL-VALUE` | dtype('<M8[us]') != 'datetime64[ns]' | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:107 in test_to_pandas` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_for_array_of_struct` | `FAIL-VALUE` | <class 'numpy.ndarray'> != <class 'list'> | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:277 in test_to_pandas_for_array_of_struct` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_from_empty_dataframe` | `FAIL-MISSING` | This feature is not implemented: Unsupported SQL type TIMESTAMP_NTZ | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:173 in test_to_pandas_from_empty_dataframe` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_from_mixed_dataframe` | `FAIL-VALUE` | Schema error: No field named col1. Did you mean 'column1'?. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:243 in test_to_pandas_from_mixed_dataframe` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_from_null_dataframe` | `FAIL-MISSING` | This feature is not implemented: Unsupported SQL type TIMESTAMP_NTZ | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:203 in test_to_pandas_from_null_dataframe` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_on_cross_join` | `FAIL-VALUE` | Error during planning: Invalid function 'explode'. Did you mean 'encode'? | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:131 in test_to_pandas_on_cross_join` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_required_pandas_not_found` | `SKIP-UPSTREAM` | Required Pandas was found. | `` |
| `pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_pandas_with_duplicated_column_names` | `FAIL-VALUE` | Error during planning: Projections require unique expression names but the expression "Int64(1) AS v" at position 0 and "Int64(1) AS v" at position 1 have the same name. Consider aliasing ("AS") one of them. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_collection.py:115 in test_to_pandas_with_duplicated_column_names` |
| `pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id` | `FAIL-MISSING` | functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_repartition.py:92 in test_repartition_by_id` |
| `pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_negative_values` | `FAIL-MISSING` | functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_repartition.py:100 in test_repartition_by_id_negative_values` |
| `pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_null_values` | `FAIL-MISSING` | functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_repartition.py:118 in test_repartition_by_id_null_values` |
| `pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_string_column_name` | `FAIL-MISSING` | functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_repartition.py:196 in test_repartition_by_id_string_column_name` |
| `pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_range` | `FAIL-MISSING` | functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_repartition.py:63 in test_repartition_by_range` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_data_frame_equal_not_support_streaming` | `FAIL-MISSING` | 'ReparkSession' object has no attribute 'readStream' | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1794 in test_assert_data_frame_equal_not_support_streaming` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_equal_approx_pandas_on_spark_df` | `FAIL-ERROR-CLASS` | Type int64 was not understood. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:938 in test_assert_equal_approx_pandas_on_spark_df` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_equal_duplicate_col` | `FAIL-VALUE` | unique expression names required; createDataFrame schema has duplicate column names | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:567 in test_assert_equal_duplicate_col` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_equal_exact_pandas_on_spark_df` | `FAIL-ERROR-CLASS` | Type int64 was not understood. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:929 in test_assert_equal_exact_pandas_on_spark_df` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_equal_nested_struct_str_duplicate` | `FAIL-MISSING` | Converting to Python dictionary is not supported when duplicate field names are present | `pyarrow/scalar.pxi:1130 in pyarrow.lib.StructScalar.__getitem__` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_error_non_pyspark_df` | `FAIL-ERROR-CLASS` | {'exp[49 chars][Row]]', 'arg_name': 'actual', 'actual_type': <class 'dict'>} != {'exp[49 chars][Row]]', 'arg_name': 'actual', 'actual_type': "<class 'dict'>"} - {'actual_type': <class 'dict'>, + {'actual_type': "<class 'dict'>", ? + + 'a... | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:995 in test_assert_error_non_pyspark_df` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_error_pandas_pyspark_df` | `FAIL-ERROR-CLASS` | Type int64 was not understood. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:949 in test_assert_error_pandas_pyspark_df` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_assert_type_error_pandas_df` | `FAIL-ERROR-CLASS` | Type int64 was not understood. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:882 in test_assert_type_error_pandas_df` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_capture_illegalargument_exception` | `FAIL-MISSING` | datafusion engine error: Invalid or Unsupported Configuration: Could not find config namespace "mapred" | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1743 in test_capture_illegalargument_exception` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_capture_pyspark_value_exception` | `FAIL-MISSING` | functions.sha2(numBits=1024) only 256 is supported (engine sha256; disclosed R-FN-BATCH4) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1751 in test_capture_pyspark_value_exception` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_capture_user_friendly_exception` | `FAIL-VALUE` | Regex didn't match: '.*UNRESOLVED_COLUMN.*`中文字段`.*' not found in 'Schema error: No field named "中文字段".' | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1727 in test_capture_user_friendly_exception` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_dataframe_ignore_column_type` | `FAIL-VALUE` | Schema error: No field named a. Valid fields are __repark_cdf_79e87528f963487c8ab7f58cc0e02ae5."A", __repark_cdf_79e87528f963487c8ab7f58cc0e02ae5."B", __repark_cdf_79e87528f963487c8ab7f58cc0e02ae5."A", __repark_cdf_79e87528f963487c8ab7f5... | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1681 in test_dataframe_ignore_column_type` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_get_error_class_state` | `FAIL-ERROR-CLASS` | None != 'UNRESOLVED_COLUMN.WITHOUT_SUGGESTION' | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1766 in test_get_error_class_state` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_schema_ignore_nullable` | `FAIL-ERROR-CLASS` | PySparkAssertionError not raised | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1258 in test_schema_ignore_nullable` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_spark_upgrade_exception` | `FAIL-MISSING` | functions.unix_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH1) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1735 in test_spark_upgrade_exception` |
| `pyspark.sql.tests.test_utils.UtilsTests.test_special_vals` | `FAIL-ERROR-CLASS` | createDataFrame does not support infinite float values | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_utils.py:1458 in test_special_vals` |
| `pyspark.sql.tests.test_errors.ErrorsTests.test_arithmetic_exception` | `FAIL-ERROR-CLASS` | ArithmeticException not raised | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_errors.py:30 in test_arithmetic_exception` |
| `pyspark.sql.tests.test_errors.ErrorsTests.test_array_index_out_of_bounds_exception` | `FAIL-ERROR-CLASS` | ArrayIndexOutOfBoundsException not raised | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_errors.py:35 in test_array_index_out_of_bounds_exception` |
| `pyspark.sql.tests.test_errors.ErrorsTests.test_date_time_exception` | `FAIL-VALUE` | Error during planning: Invalid function 'unix_timestamp'. Did you mean 'to_timestamp'? | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_errors.py:42 in test_date_time_exception` |
| `pyspark.sql.tests.test_errors.ErrorsTests.test_number_format_exception` | `FAIL-VALUE` | datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Cast error: Cannot cast string 'abc' to value of Float64 type | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_errors.py:47 in test_number_format_exception` |
| `pyspark.sql.tests.test_errors.ErrorsTests.test_spark_runtime_exception` | `FAIL-VALUE` | datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow error: Cast error: Cannot cast value 'abc' to value of Boolean type | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_errors.py:52 in test_spark_runtime_exception` |
| `pyspark.sql.tests.test_stat.DataFrameStatTests.test_freqItems` | `FAIL-MISSING` | DataFrame.stat.freqItems is not supported yet (disclosed R-DF-BATCH2) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_stat.py:40 in test_freqItems` |
| `pyspark.sql.tests.test_stat.DataFrameStatTests.test_replace` | `FAIL-VALUE` | Arrow error: Cast error: Cannot cast string 'Alice' to value of Int64 type | `<repo>/python/repark/src/repark/dataframe/core.py:6771 in to_arrow_batches` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_check_decimal_nan` | `FAIL-ERROR-CLASS` | createDataFrame does not support non-finite Decimal values (got NaN) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:168 in test_check_decimal_nan` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_datetime_time` | `FAIL-VALUE` | StringType() is not an instance of <class 'repark.types.TimeType'> | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:84 in test_create_dataframe_from_datetime_time` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_day_time_interval` | `FAIL-ERROR-CLASS` | createDataFrame does not support pandas timedelta/duration dtypes yet (got dtype timedelta64[ns]) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:154 in test_create_dataframe_from_pandas_with_day_time_interval` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_timestamp` | `FAIL-VALUE` | TimestampType() is not an instance of <class 'repark.types.TimestampNTZType'> | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:105 in test_create_dataframe_from_pandas_with_timestamp` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_required_pandas_not_found` | `SKIP-UPSTREAM` | Required Pandas was found. | `` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_nan_decimal_dataframe` | `FAIL-ERROR-CLASS` | createDataFrame schema string must be a DDL field list like 'a INT, b STRING' (got 'decimal'; a bare non-DDL string would be character-iterated into column names) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:160 in test_create_nan_decimal_dataframe` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_decimal_round` | `FAIL-ERROR-CLASS` | createDataFrame Decimal value 1.2339999999999999857891452847979962825775146484375 is outside DECIMAL(38, 18) scale (fractional digits beyond 18 are not representable without rounding; refuse rather than silent zero/round) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:171 in test_decimal_round` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_invalid_argument_create_dataframe` | `FAIL-ERROR-CLASS` | 'NOT_LIST_OR_NONE_OR_STRUCT' != None : Expected error class was 'NOT_LIST_OR_NONE_OR_STRUCT', got 'None'. | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:179 in test_invalid_argument_create_dataframe` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_partial_inference_failure` | `FAIL-ERROR-CLASS` | PySparkValueError not raised | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:195 in test_partial_inference_failure` |
| `pyspark.sql.tests.test_creation.DataFrameCreationTests.test_schema_inference_from_pandas_with_dict` | `FAIL-VALUE` | Error during planning: Execution error: Function '__repark_get_item__' user-defined coercion failed with: Error during planning: '__repark_get_item__' expects an array or map first argument, got Struct("first": Float64, "second": Float64... | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_creation.py:223 in test_schema_inference_from_pandas_with_dict` |
| `pyspark.sql.tests.test_conversion.ConversionTests.test_binary_as_bytes_conversion` | `FAIL-VALUE` | AssertionError | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_conversion.py:130 in test_binary_as_bytes_conversion` |
| `pyspark.sql.tests.test_conversion.ConversionTests.test_conversion` | `FAIL-ERROR-CLASS` | data_type must be str or DataType, got ExamplePointUDT | `<repo>/python/repark/src/repark/types.py:908 in add` |
| `pyspark.sql.tests.test_serde.SerdeTests.test_int_array_serialization` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_serde.py:140 in test_int_array_serialization` |
| `pyspark.sql.tests.test_serde.SerdeTests.test_serialize_nested_array_and_map` | `NEEDS-JVM` | repark SparkContext has no attribute 'parallelize' (only setLogLevel / applicationId / master are implemented; full SparkContext is out of scope) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_serde.py:33 in test_serialize_nested_array_and_map` |
| `pyspark.sql.tests.test_serde.SerdeTests.test_struct_in_map` | `FAIL-ERROR-CLASS` | unhashable type: 'dict' | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_serde.py:59 in test_struct_in_map` |
| `pyspark.sql.tests.test_serde.SerdeTests.test_time_with_timezone` | `FAIL-VALUE` | datet[18 chars] 8, 8, 19, 31, 45, 230997) != datet[18 chars] 8, 8, 23, 31, 45, 230997, tzinfo=zoneinfo.ZoneInfo(key='UTC')) | `<home>/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/test_serde.py:94 in test_time_with_timezone` |

## Findings (engine-relevant; C4 expand2 cohort — dual denoms own series)

- FAIL-VALUE: 40 test(s) — measurement only (C4 expand2 cohort; own denoms, never /345 or C3)
- FAIL-ERROR-CLASS: 20 test(s) — measurement only (C4 expand2 cohort; own denoms, never /345 or C3)
- FAIL-MISSING: 15 test(s) — measurement only (C4 expand2 cohort; own denoms, never /345 or C3)
- NEEDS-JVM: 3 test(s) — measurement only (C4 expand2 cohort; own denoms, never /345 or C3)

## Provenance

Apache tests loaded from cache tag `v4.1.2` @ `f0bb2e6a47d0ebda424ffd633fcea8644a597954` (runtime fetch; **not** vendored in git). Installed pyspark `4.1.2` provides the runtime package; only `pyspark.sql.tests` is injected from the cache.
