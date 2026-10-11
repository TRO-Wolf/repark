# ICE-NTZ-NESTED-WALL-1 head counts

Counts are computed by `probe.py summary-head` from the JSONL files.
Ticks are microseconds from `toArrow()` cast to int64.

ntz_nested instant cells equal to Spark: 173 of 264.
field-assignment cells identical to main: 324 of 324.
ltz_nested cells identical to main: 396 of 396.
ntz_top cells identical to main: 396 of 396.
instant cells still on the UTC wall outside UTC: 22.

## Instant cells still on the UTC wall, by door and source

- insert_values ts_literal: 2
- merge_update_star ts_column: 4
- merge_update_star ts_literal: 4
- merge_update_whole ts_literal: 4
- merge_update_whole_literal ts_literal: 4
- update_whole_where ts_literal: 4

## Cells that differ from Spark

ntz_nested insert_overwrite_values cow America/New_York ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false
ntz_nested insert_overwrite_values cow America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested insert_overwrite_values cow Asia/Kolkata ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false
ntz_nested insert_overwrite_values cow Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested insert_overwrite_values cow UTC ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false
ntz_nested insert_overwrite_values cow UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested insert_values cow America/New_York ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false
ntz_nested insert_values cow America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested insert_values cow Asia/Kolkata ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false
ntz_nested insert_values cow Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested insert_values cow UTC ts_column head=ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) spark=ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false
ntz_nested merge_update_star cow America/New_York ts_column head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_star cow America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_star cow Asia/Kolkata ts_column head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_star cow Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_star mor America/New_York ts_column head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_star mor America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_star mor Asia/Kolkata ts_column head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_star mor Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_whole cow America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_whole cow Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_whole mor America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_whole mor Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_whole_literal cow America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_whole_literal cow Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested merge_update_whole_literal mor America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested merge_update_whole_literal mor Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested update_col_nowhere cow America/New_York ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_col_nowhere cow America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_col_nowhere cow Asia/Kolkata ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_col_nowhere cow Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_col_nowhere cow UTC ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_col_nowhere cow UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_col_nowhere mor America/New_York ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_col_nowhere mor America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_col_nowhere mor Asia/Kolkata ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_col_nowhere mor Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_col_nowhere mor UTC ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_col_nowhere mor UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_field_col_nowhere cow America/New_York ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_field_col_nowhere cow America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_field_col_nowhere cow Asia/Kolkata ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_field_col_nowhere cow Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_field_col_nowhere cow UTC ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_field_col_nowhere cow UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_field_col_nowhere mor America/New_York ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_field_col_nowhere mor America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_field_col_nowhere mor Asia/Kolkata ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_field_col_nowhere mor Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_field_col_nowhere mor UTC ts_column head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_field_col_nowhere mor UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_field_nowhere cow America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by spark=1767305045123456
ntz_nested update_field_nowhere cow America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_field_nowhere cow Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by spark=1767342845123456
ntz_nested update_field_nowhere cow Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_field_nowhere cow UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by spark=1767323045123456
ntz_nested update_field_nowhere cow UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_field_nowhere mor America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by spark=1767305045123456
ntz_nested update_field_nowhere mor America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_field_nowhere mor Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by spark=1767342845123456
ntz_nested update_field_nowhere mor Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_field_nowhere mor UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by spark=1767323045123456
ntz_nested update_field_nowhere mor UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_field_where cow America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by spark=1767305045123456
ntz_nested update_field_where cow Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by spark=1767342845123456
ntz_nested update_field_where cow UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by spark=1767323045123456
ntz_nested update_field_where mor America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by spark=1767305045123456
ntz_nested update_field_where mor Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by spark=1767342845123456
ntz_nested update_field_where mor UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by spark=1767323045123456
ntz_nested update_whole_nowhere cow America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by spark=1767305045123456
ntz_nested update_whole_nowhere cow America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767305045123456
ntz_nested update_whole_nowhere cow Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by spark=1767342845123456
ntz_nested update_whole_nowhere cow Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767342845123456
ntz_nested update_whole_nowhere cow UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by spark=1767323045123456
ntz_nested update_whole_nowhere cow UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type spark=1767323045123456
ntz_nested update_whole_nowhere mor America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by spark=1767305045123456
ntz_nested update_whole_nowhere mor America/New_York ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767305045123456
ntz_nested update_whole_nowhere mor Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by spark=1767342845123456
ntz_nested update_whole_nowhere mor Asia/Kolkata ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767342845123456
ntz_nested update_whole_nowhere mor UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by spark=1767323045123456
ntz_nested update_whole_nowhere mor UTC ts_literal head=ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 spark=1767323045123456
ntz_nested update_whole_where cow America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by spark=1767305045123456
ntz_nested update_whole_where cow America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested update_whole_where cow Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by spark=1767342845123456
ntz_nested update_whole_where cow Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested update_whole_where cow UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by spark=1767323045123456
ntz_nested update_whole_where mor America/New_York ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by spark=1767305045123456
ntz_nested update_whole_where mor America/New_York ts_literal head=1767323045123456 spark=1767305045123456
ntz_nested update_whole_where mor Asia/Kolkata ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by spark=1767342845123456
ntz_nested update_whole_where mor Asia/Kolkata ts_literal head=1767323045123456 spark=1767342845123456
ntz_nested update_whole_where mor UTC ts_column head=ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by spark=1767323045123456

## Controls that moved

- none
