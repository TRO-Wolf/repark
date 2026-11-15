# Step 0: the live analyzer lists on main and on the stack base

`EXPLAIN VERBOSE SELECT 0.1 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)` on each door (probe
[../probes/rule_lists.py](../probes/rule_lists.py)), run on main's package (`6f9f2aaa`) and on `ff34baf5`'s. The
rule names and order are identical on both builds; `bool_decimal_cast` keeps its name and gains the float body.

## spark door (45 analyzer rules; identical on both builds: yes)

| idx | rule |
|---|---|
| 0 | `resolve_grouping_function` |
| 1 | `spark_timestamp_unit_widen` |
| 2 | `spark_fractional_division` |
| 3 | `higher_order_preparation` |
| 4 | `spark_float_stringify` |
| 5 | `spark_decimal_precision` |
| 6 | `spark_integral_literal` |
| 7 | `join_condition_refusals` |
| 8 | `spark_map_subscript` |
| 9 | `struct_operands_by_position` |
| 10 | `type_coercion` |
| 11 | `struct_setop_names` |
| 12 | `sliding_frame_rescan` |
| 13 | `repark_insert_store_assignment` |
| 14 | `spark_negate_null_decimal` |
| 15 | `lambda_rebind` |
| 16 | `spark_decimal_precision` |
| 17 | `spark_decimal_rewrite` |
| 18 | `spark_nullability` |
| 19 | `spark_integer_overflow` |
| 20 | `spark_expr_semantics` |
| 21 | `int_to_binary_cast` |
| 22 | `spark_float_stringify` |
| 23 | `array_cardinality_ceiling` |
| 24 | `csv_fold` |
| 25 | `time_cast_guard` |
| 26 | `spark_ltz_timestamp_cast` |
| 27 | `interval_string_cast` |
| 28 | `type_coercion` |
| 29 | `lambda_rebind` |
| 30 | `spark_map_rewrite` |
| 31 | `spark_map_subscript` |
| 32 | `spark_time_window` |
| 33 | `spark_window_time_grouping` |
| 34 | `spark_session_window` |
| 35 | `generator_rewrite` |
| 36 | `resolve_grouping_id` |
| 37 | `fold_spark_numeric_casts` |
| 38 | `spark_projection_display` |
| 39 | `stack_rewrite` |
| 40 | `repark_store_overflow_cast` |
| 41 | `repark_optimizer_seat_config` |
| 42 | `signed_zero_literals` |
| 43 | `spark_integer_overflow` |
| 44 | `bool_decimal_cast` |

## native door (12 analyzer rules; identical on both builds: yes)

| idx | rule |
|---|---|
| 0 | `resolve_grouping_function` |
| 1 | `spark_timestamp_unit_widen` |
| 2 | `spark_map_subscript` |
| 3 | `type_coercion` |
| 4 | `struct_setop_names` |
| 5 | `sliding_frame_rescan` |
| 6 | `spark_map_rewrite` |
| 7 | `repark_optimizer_seat_config` |
| 8 | `signed_zero_literals` |
| 9 | `spark_integer_overflow` |
| 10 | `bool_decimal_cast` |
| 11 | `planned_timestamp_cast` |

## Stored plans and PREPARE

- `prepare_execute` / `spark_door`: decimal(30,15) ['0.100000000000000', '0E-15']
- `prepare_execute` / `native_door`: double ['0.1', '1e-20'] (on `ff34baf5` the same values in the other row order)
- `stored_plans` / `df_cache`: decimal(30,15) ['0.100000000000000', '0E-15']
- `stored_plans` / `cache_table`: UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: CACHE TABLE dd_ct AS SELECT 0.1 AS c UNION ALL SELECT CAST(CAST(0.00000000000000000001 AS DOUBLE) AS DOUBLE)
- `stored_plans` / `native_view`: double ['0.1', '1e-20']

Up: [map.md](map.md).
