# ICE-NTZ-NESTED-WALL-1 head counts

Counts are computed by `probe.py summary-head` from the JSONL files.
Ticks are microseconds from `toArrow()` cast to int64.

ntz_nested instant cells equal to Spark: 173 of 264.
field-assignment cells identical to main: 324 of 324.
ltz_nested cells identical to main: 382 of 396.
ntz_top cells identical to main: 396 of 396.
instant cells still on the UTC wall outside UTC: 22.

## Instant cells still on the UTC wall, by door and source

- insert_values ts_literal: 2. R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_star ts_column: 4. SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star ts_literal: 4. SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_whole ts_literal: 4. R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole_literal ts_literal: 4. R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- update_whole_where ts_literal: 4. R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped

## Each UTC-wall cell

- insert_values cow America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- insert_values cow Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_star cow America/New_York ts_column: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star cow America/New_York ts_literal: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star cow Asia/Kolkata ts_column: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star cow Asia/Kolkata ts_literal: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star mor America/New_York ts_column: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star mor America/New_York ts_literal: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star mor Asia/Kolkata ts_column: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_star mor Asia/Kolkata ts_literal: SET * is UpdateAll and does not call the nested walk, so the source struct is copied and a zoned leaf keeps the physical instant
- merge_update_whole cow America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole cow Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole mor America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole mor Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole_literal cow America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole_literal cow Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole_literal mor America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- merge_update_whole_literal mor Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- update_whole_where cow America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- update_whole_where cow Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- update_whole_where mor America/New_York ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped
- update_whole_where mor Asia/Kolkata ts_literal: R4 literal: TIMESTAMP '…+00:00' inside named_struct is planned as a naive timestamp, so the leaf is not wrapped

raw-error doors identical to main: 162 of 162.

- none moved

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

ltz_nested df_append cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested df_append cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested df_insert_into cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested df_insert_into cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested df_overwrite_condition cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested df_overwrite_condition cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested df_save_as_table cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested df_save_as_table cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested insert_replace_where cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested insert_replace_where cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested insert_select cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested insert_select cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
ltz_nested insert_values cow America/New_York ntz_literal head=1767341045123456 main=1767323045123456
ltz_nested insert_values cow Asia/Kolkata ntz_literal head=1767303245123456 main=1767323045123456
