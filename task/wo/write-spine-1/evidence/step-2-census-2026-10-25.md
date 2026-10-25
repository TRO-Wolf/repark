# WRITE-SPINE-1 step (2): census evidence (2026-10-25)

Recorded from the builder's hand-back notes for [the step (2) order](../order-step-2.md) at head `d45eb72d`, on the `maturin develop --profile ci` build. The reading (the 288 moved cells, findings F-1 to F-4, the mutant table and the step-4 evidence) is in the [write-spine-1 ledger](../../../ledgers/staging/write-spine-1-ledger.md), step (2) section. This file holds the per-cell data only.

## F-1 table: NESTED-TEXT-LEAF-STORE-1 (102 cells; id, door, main value, Spark's refusal)

| cell | door | main stores | Spark |
|---|---|---|---|
| `insert_by_name.append.array_ltz.str.utc` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ltz.col_str.utc` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.str.utc` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.col_str.utc` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ltz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ntz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.col_str.utc` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.col_str.utc` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ltz.str.new_york` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ltz.col_str.new_york` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.str.new_york` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.col_str.new_york` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ltz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ntz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.col_str.new_york` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.col_str.new_york` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ltz.str.kolkata` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ltz.col_str.kolkata` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.str.kolkata` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_by_name.append.array_ntz.col_str.kolkata` | D3 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ltz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite.append.array_ntz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ltz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `insert_overwrite_values.append.array_ntz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ltz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.cow.array_ntz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ltz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_whole_where.mor.array_ntz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ltz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.cow.array_ntz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ltz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `update_col_where.mor.array_ntz.col_str.kolkata` | D10 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ltz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_overwrite_partitions.append.array_ntz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ltz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| `df_insert_into_overwrite.append.array_ntz.col_str.kolkata` | D4 | `[[1767323045123456]]` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |

## WS2-9 per-cell table, mutant A+B

| cell | family | Spark | mutant | status |
|---|---|---|---|---|
| `insert_values.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_values.append.struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_values.append.array_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_values.append.map_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_values.append.array_struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_select.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_select.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_select.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_select.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_select.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_select.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_select.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_select.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_by_name.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_by_name.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_by_name.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_by_name.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_overwrite.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_overwrite.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_overwrite.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_overwrite_values.append.map_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_overwrite_values.append.map_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_replace_where.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_replace_where.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_replace_where.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_replace_where.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_replace_where.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_replace_where.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_replace_where.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_replace_where.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_star.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_star.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_star.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_star.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_whole.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_whole.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_whole.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_whole.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_whole.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_whole.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_whole.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_whole.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_star.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_star.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_star.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_star.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_star.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_star.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole_literal.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_whole_literal.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole_literal.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `merge_update_whole_literal.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_whole_where.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_whole_where.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_whole_where.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_whole_where.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_whole_nowhere.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_col_where.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_col_where.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_col_where.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_col_where.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_col_where.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `update_col_where.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `update_col_nowhere.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `df_append.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_append.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_append.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_append.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_append.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_append.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_append.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_append.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_overwrite_condition.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_overwrite_condition.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_overwrite_condition.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_overwrite_condition.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_overwrite_condition.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_overwrite_condition.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_overwrite_condition.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_overwrite_condition.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_insert_into.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_insert_into.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_insert_into.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_insert_into.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_insert_into.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_insert_into.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_insert_into.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_insert_into.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_save_as_table.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_save_as_table.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `df_save_as_table.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_save_as_table.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `df_save_as_table.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_save_as_table.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `df_save_as_table.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `df_save_as_table.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `merge_update_field.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `merge_update_field_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_field_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_field_col_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_col_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_col_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_col_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `update_field_col_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_col_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_col_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_field_col_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_options.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_options.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `[{'a': 1767305045123456}]` | EQUAL |
| `insert_options.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_options.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `[[1767305045123456]]` | EQUAL |
| `insert_options.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_options.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `[{'k': 1767305045123456}]` | EQUAL |
| `insert_options.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_options.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `[[{'a': 1767305045123456}]]` | EQUAL |
| `insert_values.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_values.append.struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_values.append.array_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_values.append.map_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_values.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_values.append.array_struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_select.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_select.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_select.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_select.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_select.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_select.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_select.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_select.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_by_name.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_by_name.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_by_name.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_by_name.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_overwrite.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_overwrite.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_overwrite.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_overwrite_values.append.map_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_overwrite_values.append.map_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_replace_where.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_replace_where.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_replace_where.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_replace_where.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_replace_where.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_replace_where.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_replace_where.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_replace_where.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_star.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_star.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_star.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_star.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_whole.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_whole.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_whole.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_whole.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_whole.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_whole.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_whole.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_whole.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_star.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_star.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_star.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_star.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_star.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_star.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_whole_literal.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole_literal.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_whole_literal.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_whole_literal.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole_literal.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `merge_update_whole_literal.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_whole_where.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_whole_where.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_whole_where.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_whole_where.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_whole_nowhere.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_col_where.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_col_where.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_col_where.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_col_where.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_col_where.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `update_col_where.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `update_col_nowhere.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `df_append.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_append.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_append.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_append.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_append.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_append.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_append.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_append.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_overwrite_condition.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_overwrite_condition.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_overwrite_condition.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_overwrite_condition.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_overwrite_condition.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_overwrite_condition.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_overwrite_condition.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_overwrite_condition.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_insert_into.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_insert_into.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_insert_into.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_insert_into.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_insert_into.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_insert_into.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_insert_into.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_insert_into.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_save_as_table.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_save_as_table.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `df_save_as_table.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_save_as_table.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `df_save_as_table.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_save_as_table.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `df_save_as_table.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `df_save_as_table.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `merge_update_field.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `merge_update_field_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_field_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_field_col_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_col_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_col_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_col_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `update_field_col_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_col_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_field_col_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_field_col_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_options.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_options.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `[{'a': 1767342845123456}]` | EQUAL |
| `insert_options.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_options.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `[[1767342845123456]]` | EQUAL |
| `insert_options.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_options.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `[{'k': 1767342845123456}]` | EQUAL |
| `insert_options.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |
| `insert_options.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `[[{'a': 1767342845123456}]]` | EQUAL |

## WS2-9 per-cell table, mutant A (the order's)

| cell | family | Spark | mutant | status |
|---|---|---|---|---|
| `insert_values.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.array_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.map_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.array_struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_select.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_by_name.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.array_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_overwrite_values.append.map_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.map_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ltz.col_wall.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.array_struct_ntz.col_inst.new_york` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_replace_where.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole_literal.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `merge_update_whole_literal.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `merge_update_whole_literal.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.struct_ltz.wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.col_wall.new_york` | F4 | `[{'a': 1767341045123456}]` | `[{'a': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_ltz.wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.col_wall.new_york` | F4 | `[[1767341045123456]]` | `[[1767341045123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.map_ltz.wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.col_wall.new_york` | F4 | `[{'k': 1767341045123456}]` | `[{'k': 1767341045123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_struct_ltz.wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.col_wall.new_york` | F4 | `[[{'a': 1767341045123456}]]` | `[[{'a': 1767341045123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.cow.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.cow.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.mor.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.mor.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.struct_ntz.inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.struct_ntz.col_inst.new_york` | F3 | `[{'a': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_ntz.inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_ntz.col_inst.new_york` | F3 | `[[1767305045123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.map_ntz.inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.map_ntz.col_inst.new_york` | F3 | `[{'k': 1767305045123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_struct_ntz.inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_struct_ntz.col_inst.new_york` | F3 | `[[{'a': 1767305045123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.array_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.map_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_values.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_values.append.array_struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_select.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_select.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_by_name.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_by_name.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_by_name.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_by_name.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_by_name.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `insert_overwrite.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `insert_overwrite_values.append.struct_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `insert_overwrite_values.append.array_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.array_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `insert_overwrite_values.append.map_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.map_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `insert_overwrite_values.append.array_struct_ltz.col_wall.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `This feature is not implemented: Physical plan does not support logical expression ScalarS` | DIFFERENT |
| `insert_overwrite_values.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_overwrite_values.append.array_struct_ntz.col_inst.kolkata` | F1 | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | DIFFERENT |
| `insert_replace_where.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_replace_where.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_star.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_star.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_star.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_star.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_insert_literal.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_insert_literal.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_insert_literal.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_insert_literal.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_star.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_star.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_star.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_star.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole_literal.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole_literal.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767323045123456]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `merge_update_whole_literal.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767323045123456}]` | DIFFERENT |
| `merge_update_whole_literal.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `merge_update_whole_literal.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767323045123456}]]` | DIFFERENT |
| `merge_update_whole_literal.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `merge_update_whole_literal.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_whole_literal.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767323045123456}]` | DIFFERENT |
| `update_whole_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_whole_where.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_whole_where.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_whole_where.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_where.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_whole_nowhere.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_whole_nowhere.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this stat` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_whole_nowhere.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `update_col_where.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `update_col_where.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `update_col_where.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_where.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: arguments need to have the s` | ERROR |
| `update_col_nowhere.cow.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.cow.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `datafusion engine error: Arrow error: Invalid argument error: column types must match sche` | ERROR |
| `update_col_nowhere.mor.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_col_nowhere.mor.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_append.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_overwrite_partitions.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_overwrite_partitions.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_overwrite_partitions.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_partitions.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_overwrite_condition.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.struct_ltz.wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ltz.col_wall.kolkata` | F4 | `[{'a': 1767303245123456}]` | `[{'a': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_ltz.wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ltz.col_wall.kolkata` | F4 | `[[1767303245123456]]` | `[[1767303245123456]]` | EQUAL |
| `df_insert_into_overwrite.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.map_ltz.wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ltz.col_wall.kolkata` | F4 | `[{'k': 1767303245123456}]` | `[{'k': 1767303245123456}]` | EQUAL |
| `df_insert_into_overwrite.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_struct_ltz.wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ltz.col_wall.kolkata` | F4 | `[[{'a': 1767303245123456}]]` | `[[{'a': 1767303245123456}]]` | EQUAL |
| `df_insert_into_overwrite.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_insert_into_overwrite.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `df_save_as_table.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `merge_update_field_literal.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_where.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.cow.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.cow.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.mor.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `update_field_col_nowhere.mor.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.struct_ntz.inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.struct_ntz.col_inst.kolkata` | F3 | `[{'a': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_ntz.inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_ntz.col_inst.kolkata` | F3 | `[[1767342845123456]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.map_ntz.inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.map_ntz.col_inst.kolkata` | F3 | `[{'k': 1767342845123456}]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_struct_ntz.inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |
| `insert_options.append.array_struct_ntz.col_inst.kolkata` | F3 | `[[{'a': 1767342845123456}]]` | `Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incom` | REFUSED |

## Fix round 1: the 106 carve-out cells (C-025, 2026-10-25)

The 106 cells of `test_the_partition_carve_out_refuses_with_the_workaround` (the carve-out of ice-struct-pairing-1 C-031, the `r3_part` cells Spark stores). Measured once on the `d45eb72d` build before fix round 1: 106 of 106 stored rows equal Spark's oracle rows (`ice_struct_pairing_1_spark_oracle.json`), 0 differ, 0 error. Fix round 1 (`c3f22afb`) restores the refusal, because the carve-out ruling stands (WS2-27). Reading: [the step (2) ledger, fix round 1](../../../ledgers/staging/write-spine-1-ledger.md).

- `r3_leaf|part_dynamic|typed_widen_reordered`
- `r3_leaf|part_static|typed_widen_reordered`
- `r3_part|v2|ci|part_dynamic|C_case`
- `r3_part|v2|ci|part_dynamic|C_case_array`
- `r3_part|v2|ci|part_dynamic|C_case_map_key`
- `r3_part|v2|ci|part_dynamic|C_case_mixed_reordered`
- `r3_part|v2|ci|part_dynamic|C_case_reordered`
- `r3_part|v2|ci|part_dynamic|C_case_two_deep`
- `r3_part|v2|ci|part_dynamic|C_twins`
- `r3_part|v2|ci|part_dynamic|case_mixed_reordered`
- `r3_part|v2|ci|part_dynamic|map_in_array_in_struct`
- `r3_part|v2|ci|part_dynamic|map_key_renamed`
- `r3_part|v2|ci|part_dynamic|map_key_reordered`
- `r3_part|v2|ci|part_dynamic|map_value_reordered`
- `r3_part|v2|ci|part_dynamic|null_field_reordered`
- `r3_part|v2|ci|part_dynamic|null_struct`
- `r3_part|v2|ci|part_dynamic|renamed`
- `r3_part|v2|ci|part_dynamic|reordered`
- `r3_part|v2|ci|part_dynamic|two_deep_inner_reordered`
- `r3_part|v2|ci|part_dynamic|two_deep_renamed`
- `r3_part|v2|ci|part_dynamic|typed_widen_reordered`
- `r3_part|v2|ci|part_static|C_case`
- `r3_part|v2|ci|part_static|C_case_array`
- `r3_part|v2|ci|part_static|C_case_map_key`
- `r3_part|v2|ci|part_static|C_case_mixed_reordered`
- `r3_part|v2|ci|part_static|C_case_reordered`
- `r3_part|v2|ci|part_static|C_case_two_deep`
- `r3_part|v2|ci|part_static|C_twins`
- `r3_part|v2|ci|part_static|case_mixed_reordered`
- `r3_part|v2|ci|part_static|map_in_array_in_struct`
- `r3_part|v2|ci|part_static|map_key_renamed`
- `r3_part|v2|ci|part_static|map_key_reordered`
- `r3_part|v2|ci|part_static|map_value_reordered`
- `r3_part|v2|ci|part_static|null_field_reordered`
- `r3_part|v2|ci|part_static|null_struct`
- `r3_part|v2|ci|part_static|renamed`
- `r3_part|v2|ci|part_static|reordered`
- `r3_part|v2|ci|part_static|two_deep_inner_reordered`
- `r3_part|v2|ci|part_static|two_deep_renamed`
- `r3_part|v2|ci|part_static|typed_widen_reordered`
- `r3_part|v2|cs|part_dynamic|C_case`
- `r3_part|v2|cs|part_dynamic|C_case_array`
- `r3_part|v2|cs|part_dynamic|C_case_map_key`
- `r3_part|v2|cs|part_dynamic|C_case_mixed_reordered`
- `r3_part|v2|cs|part_dynamic|C_case_reordered`
- `r3_part|v2|cs|part_dynamic|C_case_two_deep`
- `r3_part|v2|cs|part_dynamic|C_twins`
- `r3_part|v2|cs|part_static|C_case`
- `r3_part|v2|cs|part_static|C_case_array`
- `r3_part|v2|cs|part_static|C_case_map_key`
- `r3_part|v2|cs|part_static|C_case_mixed_reordered`
- `r3_part|v2|cs|part_static|C_case_reordered`
- `r3_part|v2|cs|part_static|C_case_two_deep`
- `r3_part|v2|cs|part_static|C_twins`
- `r3_part|v3|ci|part_dynamic|C_case`
- `r3_part|v3|ci|part_dynamic|C_case_array`
- `r3_part|v3|ci|part_dynamic|C_case_map_key`
- `r3_part|v3|ci|part_dynamic|C_case_mixed_reordered`
- `r3_part|v3|ci|part_dynamic|C_case_reordered`
- `r3_part|v3|ci|part_dynamic|C_case_two_deep`
- `r3_part|v3|ci|part_dynamic|C_twins`
- `r3_part|v3|ci|part_dynamic|case_mixed_reordered`
- `r3_part|v3|ci|part_dynamic|map_in_array_in_struct`
- `r3_part|v3|ci|part_dynamic|map_key_renamed`
- `r3_part|v3|ci|part_dynamic|map_key_reordered`
- `r3_part|v3|ci|part_dynamic|map_value_reordered`
- `r3_part|v3|ci|part_dynamic|null_field_reordered`
- `r3_part|v3|ci|part_dynamic|null_struct`
- `r3_part|v3|ci|part_dynamic|renamed`
- `r3_part|v3|ci|part_dynamic|reordered`
- `r3_part|v3|ci|part_dynamic|two_deep_inner_reordered`
- `r3_part|v3|ci|part_dynamic|two_deep_renamed`
- `r3_part|v3|ci|part_dynamic|typed_widen_reordered`
- `r3_part|v3|ci|part_static|C_case`
- `r3_part|v3|ci|part_static|C_case_array`
- `r3_part|v3|ci|part_static|C_case_map_key`
- `r3_part|v3|ci|part_static|C_case_mixed_reordered`
- `r3_part|v3|ci|part_static|C_case_reordered`
- `r3_part|v3|ci|part_static|C_case_two_deep`
- `r3_part|v3|ci|part_static|C_twins`
- `r3_part|v3|ci|part_static|case_mixed_reordered`
- `r3_part|v3|ci|part_static|map_in_array_in_struct`
- `r3_part|v3|ci|part_static|map_key_renamed`
- `r3_part|v3|ci|part_static|map_key_reordered`
- `r3_part|v3|ci|part_static|map_value_reordered`
- `r3_part|v3|ci|part_static|null_field_reordered`
- `r3_part|v3|ci|part_static|null_struct`
- `r3_part|v3|ci|part_static|renamed`
- `r3_part|v3|ci|part_static|reordered`
- `r3_part|v3|ci|part_static|two_deep_inner_reordered`
- `r3_part|v3|ci|part_static|two_deep_renamed`
- `r3_part|v3|ci|part_static|typed_widen_reordered`
- `r3_part|v3|cs|part_dynamic|C_case`
- `r3_part|v3|cs|part_dynamic|C_case_array`
- `r3_part|v3|cs|part_dynamic|C_case_map_key`
- `r3_part|v3|cs|part_dynamic|C_case_mixed_reordered`
- `r3_part|v3|cs|part_dynamic|C_case_reordered`
- `r3_part|v3|cs|part_dynamic|C_case_two_deep`
- `r3_part|v3|cs|part_dynamic|C_twins`
- `r3_part|v3|cs|part_static|C_case`
- `r3_part|v3|cs|part_static|C_case_array`
- `r3_part|v3|cs|part_static|C_case_map_key`
- `r3_part|v3|cs|part_static|C_case_mixed_reordered`
- `r3_part|v3|cs|part_static|C_case_reordered`
- `r3_part|v3|cs|part_static|C_case_two_deep`
- `r3_part|v3|cs|part_static|C_twins`
