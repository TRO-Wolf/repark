# Nested TIMESTAMP_NTZ doors, RePark at `2c33f289` against Spark 4.1.2

Ticks are microseconds, read by `toArrow()` and cast to int64.
A refusal is the error class and the first line of the text.
`same` means the two ticks, or the two refusals, are equal.

The 28 doors are the nested matrix of ICE-TSNS-MERGE-WALL-1 section 9.2:
`insert_values`, `insert_select`, `insert_by_name`, `insert_overwrite`,
`insert_overwrite_values`, `insert_replace_where`, `merge_insert`,
`merge_insert_star`, `merge_insert_literal`, `merge_update_whole`,
`merge_update_star`, `merge_update_whole_literal`, `update_whole_where`,
`update_whole_nowhere`, `update_col_where`, `update_col_nowhere`,
`df_append`, `df_overwrite_partitions`, `df_overwrite_condition`,
`df_insert_into`, `df_insert_into_overwrite`, `df_save_as_table`,
`merge_update_field`, `merge_update_field_literal`, `update_field_where`,
`update_field_nowhere`, `update_field_col_where`, `update_field_col_nowhere`.
A door whose name starts with `merge_` or `update_` is also run merge-on-read
(`mode` `mor`). Tables are Iceberg format v2.

Schemas: `ntz_nested` is `struct<v: TIMESTAMP_NTZ, n: INT>`, `ltz_nested` is
`struct<v: TIMESTAMP, n: INT>`, `ntz_top` is a top-level `TIMESTAMP_NTZ` column.
Sources: `ts_literal` is `TIMESTAMP '2026-01-02 03:04:05.123456+00:00'`,
`ts_column` is a `TIMESTAMP` column holding that instant, `ntz_literal` is
`TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'`. A column source in a `VALUES`
list or a bare `UPDATE` is the scalar subquery `(SELECT l FROM ice.ns.src)`.

Session walls of the instant: UTC `1767323045123456`, America/New_York
`1767305045123456`, Asia/Kolkata `1767342845123456`. The NTZ literal's wall is
`1767323045123456` in every zone.

On `ntz_nested`, for an instant in a non-UTC zone, Spark stores the session
wall on every door it answers (172 cells) and refuses only the scalar subquery
inside `VALUES` (4 cells). RePark stores the session wall on field assignment
(28 cells) and the UTC wall on the other doors that answer (102 cells).
Unfiltered `UPDATE` and `INSERT OVERWRITE … VALUES` raise a raw Arrow error
on RePark; Spark stores the session wall there.

1188 cells. 374 differ. The rows are the whole inventory, controls included.

| schema | door | mode | zone | source | RePark | Spark | same or differs |
|---|---|---|---|---|---|---|---|
| ltz_nested | df_append | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_append | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_append | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_append | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_insert_into | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_insert_into | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_insert_into_overwrite | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_insert_into_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_overwrite_condition | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_overwrite_condition | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_condition | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_overwrite_partitions | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_overwrite_partitions | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | df_save_as_table | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | df_save_as_table | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | df_save_as_table | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_by_name | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_by_name | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_by_name | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_overwrite | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_overwrite | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_overwrite_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#14511, st#14512, k#14513] ice.ns.t146, true, false, false | differs |
| ltz_nested | insert_overwrite_values | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | insert_overwrite_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_overwrite_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#14511, st#14512, k#14513] ice.ns.t146, true, false, false | differs |
| ltz_nested | insert_overwrite_values | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | insert_overwrite_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_overwrite_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#14511, st#14512, k#14513] ice.ns.t146, true, false, false | differs |
| ltz_nested | insert_overwrite_values | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | insert_replace_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_replace_where | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_replace_where | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_replace_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_select | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_select | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_select | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | insert_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#14142, st#14143, k#14144] ice.ns.t134, false, false, false | differs |
| ltz_nested | insert_values | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | insert_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#14142, st#14143, k#14144] ice.ns.t134, false, false, false | differs |
| ltz_nested | insert_values | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | insert_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#14142, st#14143, k#14144] ice.ns.t134, false, false, false | differs |
| ltz_nested | insert_values | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert_literal | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert_literal | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert_literal | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert_literal | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert_star | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert_star | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_insert_star | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_insert_star | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_insert_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | merge_update_field | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | merge_update_field | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | merge_update_field | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | merge_update_field | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | merge_update_field_literal | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | merge_update_field_literal | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | merge_update_field_literal | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | merge_update_field_literal | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_field_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_star | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_star | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_star | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_star | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_whole | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_whole | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_whole | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_whole | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_whole_literal | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | merge_update_whole_literal | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | merge_update_whole_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767341045123456 | differs |
| ltz_nested | update_col_nowhere | cow | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767303245123456 | differs |
| ltz_nested | update_col_nowhere | cow | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767341045123456 | differs |
| ltz_nested | update_col_nowhere | mor | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767303245123456 | differs |
| ltz_nested | update_col_nowhere | mor | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_col_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | update_col_where | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | update_col_where | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | update_col_where | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | update_col_where | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767341045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767303245123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767341045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767303245123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_col_where | cow | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | update_field_col_where | cow | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | update_field_col_where | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | update_field_col_where | mor | America/New_York | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | update_field_col_where | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767341045123456 | differs |
| ltz_nested | update_field_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t248' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767303245123456 | differs |
| ltz_nested | update_field_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t248' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t248' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767341045123456 | differs |
| ltz_nested | update_field_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t251' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767303245123456 | differs |
| ltz_nested | update_field_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t251' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t251' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_field_where | cow | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | update_field_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t242' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | cow | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | update_field_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t242' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t242' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | mor | America/New_York | ntz_literal | 1767341045123456 | 1767341045123456 | same |
| ltz_nested | update_field_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t245' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | mor | Asia/Kolkata | ntz_literal | 1767303245123456 | 1767303245123456 | same |
| ltz_nested | update_field_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t245' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_field_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t245' / caused by | 1767323045123456 | differs |
| ltz_nested | update_field_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767341045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t194' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767303245123456 | differs |
| ltz_nested | update_whole_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t194' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t194' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767341045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t197' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767303245123456 | differs |
| ltz_nested | update_whole_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t197' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t197' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs, "UTC"), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ltz_nested | update_whole_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | update_whole_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t188' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | update_whole_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t188' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t188' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767341045123456 | differs |
| ltz_nested | update_whole_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t191' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767303245123456 | differs |
| ltz_nested | update_whole_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t191' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ltz_nested | update_whole_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t191' / caused by | 1767323045123456 | differs |
| ltz_nested | update_whole_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_append | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_append | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_append | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_append | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_append | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_append | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_append | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_append | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_append | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_insert_into | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_insert_into | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_insert_into | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_insert_into | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into_overwrite | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_insert_into_overwrite | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_insert_into_overwrite | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_insert_into_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_insert_into_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_condition | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_condition | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_overwrite_condition | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_overwrite_condition | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_condition | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_overwrite_condition | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_overwrite_condition | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_condition | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_condition | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_partitions | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_partitions | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_overwrite_partitions | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_overwrite_partitions | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_overwrite_partitions | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_partitions | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_overwrite_partitions | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_save_as_table | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_save_as_table | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_save_as_table | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | df_save_as_table | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_save_as_table | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_save_as_table | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | df_save_as_table | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_save_as_table | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | df_save_as_table | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_by_name | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_by_name | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_by_name | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_by_name | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_by_name | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_by_name | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_by_name | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_by_name | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_by_name | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_overwrite | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_overwrite | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false | differs |
| ntz_nested | insert_overwrite_values | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | insert_overwrite_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false | differs |
| ntz_nested | insert_overwrite_values | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | insert_overwrite_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_overwrite_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 28; / 'InsertIntoStatement RelationV2[id#441, st#442, k#443] ice.ns.t14, true, false, false | differs |
| ntz_nested | insert_overwrite_values | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but found Struct("v": Timestamp(µs, "UTC"), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | insert_replace_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_replace_where | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_replace_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_replace_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_replace_where | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_replace_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_replace_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_replace_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_replace_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_select | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_select | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_select | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_select | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_select | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_select | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_select | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_select | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_select | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false | differs |
| ntz_nested | insert_values | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | insert_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false | differs |
| ntz_nested | insert_values | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | insert_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | insert_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 22; / 'InsertIntoStatement RelationV2[id#50, st#51, k#52] ice.ns.t2, false, false, false | differs |
| ntz_nested | insert_values | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_literal | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_literal | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_literal | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_literal | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_star | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_star | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_star | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_insert_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_star | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_insert_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_insert_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field_literal | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field_literal | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field_literal | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | merge_update_field_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field_literal | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | merge_update_field_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_field_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_star | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_star | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_star | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_star | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole_literal | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole_literal | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole_literal | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole_literal | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | merge_update_whole_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | merge_update_whole_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | cow | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_col_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_col_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | cow | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_col_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_col_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | cow | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | mor | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_col_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_col_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | mor | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_col_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_col_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | mor | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_col_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_col_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | cow | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_col_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_col_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | cow | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_col_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | mor | America/New_York | ts_column | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_col_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_col_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | mor | Asia/Kolkata | ts_column | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_col_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | America/New_York | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | UTC | ts_column | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_col_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "5"}, "n": Int32, metadata: {"PARQUET:field_id": "6"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_col_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_col_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_col_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_col_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_col_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_col_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_col_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by | 1767305045123456 | differs |
| ntz_nested | update_field_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_field_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by | 1767342845123456 | differs |
| ntz_nested | update_field_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_field_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t116' / caused by | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by | 1767305045123456 | differs |
| ntz_nested | update_field_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_field_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by | 1767342845123456 | differs |
| ntz_nested | update_field_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_field_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t119' / caused by | 1767323045123456 | differs |
| ntz_nested | update_field_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_field_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by | 1767305045123456 | differs |
| ntz_nested | update_field_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by | 1767342845123456 | differs |
| ntz_nested | update_field_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t110' / caused by | 1767323045123456 | differs |
| ntz_nested | update_field_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by | 1767305045123456 | differs |
| ntz_nested | update_field_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_nested | update_field_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by | 1767342845123456 | differs |
| ntz_nested | update_field_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_nested | update_field_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_field_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t113' / caused by | 1767323045123456 | differs |
| ntz_nested | update_field_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_nowhere | cow | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by | 1767305045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767305045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by | 1767342845123456 | differs |
| ntz_nested | update_whole_nowhere | cow | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767342845123456 | differs |
| ntz_nested | update_whole_nowhere | cow | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t62' / caused by | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | cow | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: arguments need to have the same data type | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | America/New_York | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by | 1767305045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | America/New_York | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767305045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | Asia/Kolkata | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by | 1767342845123456 | differs |
| ntz_nested | update_whole_nowhere | mor | Asia/Kolkata | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767342845123456 | differs |
| ntz_nested | update_whole_nowhere | mor | UTC | ntz_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t65' / caused by | 1767323045123456 | differs |
| ntz_nested | update_whole_nowhere | mor | UTC | ts_literal | ERROR PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Struct("v": Timestamp(µs), metadata: {"PARQUET:field_id": "4"}, "n": Int32, metadata: {"PARQUET:field_id": "5"}) but found Struct("v": Timestamp(µs), "n": Int32) at column index 1 | 1767323045123456 | differs |
| ntz_nested | update_whole_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by | 1767305045123456 | differs |
| ntz_nested | update_whole_where | cow | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_whole_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by | 1767342845123456 | differs |
| ntz_nested | update_whole_where | cow | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_whole_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t56' / caused by | 1767323045123456 | differs |
| ntz_nested | update_whole_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by | 1767305045123456 | differs |
| ntz_nested | update_whole_where | mor | America/New_York | ts_literal | 1767323045123456 | 1767305045123456 | differs |
| ntz_nested | update_whole_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by | 1767342845123456 | differs |
| ntz_nested | update_whole_where | mor | Asia/Kolkata | ts_literal | 1767323045123456 | 1767342845123456 | differs |
| ntz_nested | update_whole_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_nested | update_whole_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t59' / caused by | 1767323045123456 | differs |
| ntz_nested | update_whole_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_append | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_append | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_append | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_append | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_append | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_append | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_append | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_append | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_append | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_insert_into | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_insert_into | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_insert_into | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_insert_into | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_insert_into_overwrite | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_insert_into_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_insert_into_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_condition | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_condition | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_overwrite_condition | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_overwrite_condition | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_condition | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_overwrite_condition | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_overwrite_condition | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_condition | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_condition | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_partitions | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_partitions | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_overwrite_partitions | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_overwrite_partitions | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_partitions | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_overwrite_partitions | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_overwrite_partitions | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_partitions | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_overwrite_partitions | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_save_as_table | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_save_as_table | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_save_as_table | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | df_save_as_table | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_save_as_table | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_save_as_table | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | df_save_as_table | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_save_as_table | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | df_save_as_table | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_by_name | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_by_name | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_by_name | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_by_name | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_by_name | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_by_name | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_by_name | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_by_name | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_by_name | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_overwrite | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_overwrite | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_overwrite | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_overwrite | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#28170, v#28171, k#28172] ice.ns.t278, true, false, false | differs |
| ntz_top | insert_overwrite_values | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_overwrite_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#28170, v#28171, k#28172] ice.ns.t278, true, false, false | differs |
| ntz_top | insert_overwrite_values | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_overwrite_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_overwrite_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 29; / 'InsertIntoStatement RelationV2[id#28170, v#28171, k#28172] ice.ns.t278, true, false, false | differs |
| ntz_top | insert_overwrite_values | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_replace_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_replace_where | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_replace_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_replace_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_replace_where | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_replace_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_replace_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_replace_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_replace_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_select | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_select | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_select | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_select | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_select | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_select | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_select | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_select | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_select | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_values | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_values | cow | America/New_York | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#27858, v#27859, k#27860] ice.ns.t266, false, false, false | differs |
| ntz_top | insert_values | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | insert_values | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_values | cow | Asia/Kolkata | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#27858, v#27859, k#27860] ice.ns.t266, false, false, false | differs |
| ntz_top | insert_values | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | insert_values | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | insert_values | cow | UTC | ts_column | ERROR UnsupportedOperationException: This feature is not implemented: Physical plan does not support logical expression ScalarSubquery(<subquery>) | ERROR AnalysisException: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES] Unsupported subquery expression: Scalar subqueries in the VALUES clause. SQLSTATE: 0A000; line 1 pos 24; / 'InsertIntoStatement RelationV2[id#27858, v#27859, k#27860] ice.ns.t266, false, false, false | differs |
| ntz_top | insert_values | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_literal | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_literal | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_literal | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_literal | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_star | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_star | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_star | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_insert_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_star | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_insert_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_insert_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field_literal | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field_literal | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field_literal | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_field_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field_literal | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_field_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_field_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_star | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_star | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_star | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_star | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_star | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_star | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_star | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_star | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_star | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole_literal | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole_literal | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole_literal | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole_literal | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole_literal | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | merge_update_whole_literal | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole_literal | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | merge_update_whole_literal | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | merge_update_whole_literal | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_nowhere | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_nowhere | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_nowhere | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_nowhere | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_nowhere | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_nowhere | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_nowhere | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_nowhere | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_nowhere | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_col_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_nowhere | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_nowhere | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_nowhere | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_nowhere | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_nowhere | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_nowhere | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_nowhere | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_nowhere | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_nowhere | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | cow | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | cow | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | cow | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | mor | America/New_York | ts_column | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_col_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | mor | Asia/Kolkata | ts_column | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_col_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | mor | UTC | ts_column | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_col_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t380' / caused by | 1767305045123456 | differs |
| ntz_top | update_field_nowhere | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_nowhere | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t380' / caused by | 1767342845123456 | differs |
| ntz_top | update_field_nowhere | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_nowhere | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t380' / caused by | 1767323045123456 | differs |
| ntz_top | update_field_nowhere | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t383' / caused by | 1767305045123456 | differs |
| ntz_top | update_field_nowhere | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_nowhere | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t383' / caused by | 1767342845123456 | differs |
| ntz_top | update_field_nowhere | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_nowhere | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t383' / caused by | 1767323045123456 | differs |
| ntz_top | update_field_nowhere | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t374' / caused by | 1767305045123456 | differs |
| ntz_top | update_field_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t374' / caused by | 1767342845123456 | differs |
| ntz_top | update_field_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t374' / caused by | 1767323045123456 | differs |
| ntz_top | update_field_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t377' / caused by | 1767305045123456 | differs |
| ntz_top | update_field_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_field_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t377' / caused by | 1767342845123456 | differs |
| ntz_top | update_field_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_field_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_field_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t377' / caused by | 1767323045123456 | differs |
| ntz_top | update_field_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t326' / caused by | 1767305045123456 | differs |
| ntz_top | update_whole_nowhere | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_whole_nowhere | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t326' / caused by | 1767342845123456 | differs |
| ntz_top | update_whole_nowhere | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_whole_nowhere | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t326' / caused by | 1767323045123456 | differs |
| ntz_top | update_whole_nowhere | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t329' / caused by | 1767305045123456 | differs |
| ntz_top | update_whole_nowhere | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_whole_nowhere | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t329' / caused by | 1767342845123456 | differs |
| ntz_top | update_whole_nowhere | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_whole_nowhere | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_nowhere | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t329' / caused by | 1767323045123456 | differs |
| ntz_top | update_whole_nowhere | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | cow | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | cow | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t320' / caused by | 1767305045123456 | differs |
| ntz_top | update_whole_where | cow | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_whole_where | cow | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | cow | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t320' / caused by | 1767342845123456 | differs |
| ntz_top | update_whole_where | cow | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_whole_where | cow | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | cow | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t320' / caused by | 1767323045123456 | differs |
| ntz_top | update_whole_where | cow | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | mor | America/New_York | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | mor | America/New_York | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t323' / caused by | 1767305045123456 | differs |
| ntz_top | update_whole_where | mor | America/New_York | ts_literal | 1767305045123456 | 1767305045123456 | same |
| ntz_top | update_whole_where | mor | Asia/Kolkata | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | mor | Asia/Kolkata | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t323' / caused by | 1767342845123456 | differs |
| ntz_top | update_whole_where | mor | Asia/Kolkata | ts_literal | 1767342845123456 | 1767342845123456 | same |
| ntz_top | update_whole_where | mor | UTC | ntz_literal | 1767323045123456 | 1767323045123456 | same |
| ntz_top | update_whole_where | mor | UTC | ts_column | ERROR AnalysisException: UPDATE operation on table 'ice.ns.t323' / caused by | 1767323045123456 | differs |
| ntz_top | update_whole_where | mor | UTC | ts_literal | 1767323045123456 | 1767323045123456 | same |

Rows: 1188. Differ: 374.
