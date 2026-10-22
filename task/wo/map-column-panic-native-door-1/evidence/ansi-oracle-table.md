# MAP-COLUMN-PANIC-NATIVE-DOOR-1 — the oracle replay on the ANSI door

The same 47 statements as [oracle-table.md](oracle-table.md), run on the Rust-embedded ANSI door (`repark_sql::AnsiDialect` on a `ReparkSession`, the door with no Python entry) on main's code and on the head (commit 4), beside the Python native door's and the Spark door's answers on the head. The harness is a throwaway Rust test that read the inlined statements, ran each one on the ANSI door with its `collect` on a spawned task (so a panic is reported, not fatal) and wrote each answer as an Arrow IPC file; the files were read back with the same normaliser as the Python replay, so rows and types compare like for like. The harness is not committed; the raw answers are [replay-main-ansi.json](replay-main-ansi.json) and [replay-head-ansi.json](replay-head-ansi.json).

"equals" means rows and types equal, or both refuse in the same class. "rows only" marks a statement whose rows are equal and whose types differ only by the export coercion the Python boundary applies (`Utf8View` to `Utf8`, list items nullable): the ANSI door hands back Arrow as DataFusion built it. The statements that differ from the Spark door differ for the native-door reasons already named in [oracle-table.md](oracle-table.md) (string-cast spelling, `date_format` pattern, naive instants, `2.5` as double, the Spark-only functions, map equality, `map()`, the literal width of `SELECT 1`).

| shape | ANSI door on main | ANSI door on head | native Python door on head | Spark door (head) | ANSI head equals native Python door | ANSI head equals Spark |
|---|---|---|---|---|---|---|
| `corpus:map.col_inst.cast_ntz.inner` | panic `list array` | value | value | value | yes | yes |
| `corpus:map.col_inst.cast_string.inner` | panic `list array` | value | value | value | rows only | no |
| `corpus:map.col_inst.ccast_ntz` | value | value | value | value | yes | yes |
| `corpus:map.col_inst.ccast_string` | value | value | value | value | yes | no |
| `corpus:map.col_inst.to_json` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | yes | no |
| `corpus:map.col_wall.cast_ltz.inner` | panic `list array` | value | value | value | yes | no |
| `corpus:map.col_wall.ccast_ltz` | value | value | value | value | yes | yes |
| `corpus:map.inst.cast_date.inner` | value | value | value | value | yes | yes |
| `corpus:map.inst.cast_string.inner` | value | value | value | value | rows only | no |
| `corpus:map.inst.date_format.inner` | value | value | value | value | yes | no |
| `corpus:map.inst.to_date.inner` | value | value | value | value | yes | yes |
| `corpus:map.wall.cast_string.inner` | value | value | value | value | rows only | no |
| `pin:array_value` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | yes | no |
| `pin:cast_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:column_key` | value | value | value | value | yes | yes |
| `pin:column_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:column_value_one_row` | panic `list array` | value | value | value | yes | yes |
| `pin:create_map_unresolved` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | yes | yes |
| `pin:duplicate_key` | error `map key must be unique` | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | yes | yes |
| `pin:duplicate_literal` | error `map key must be unique` | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | yes | yes |
| `pin:empty_control` | error `map expected at least one argument` | error `map expected at least one argument` | error `map expected at least one argument` | value | yes | no |
| `pin:function_value` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | yes | no |
| `pin:int_key` | value | value | value | value | yes | yes |
| `pin:literal_control` | value | value | value | value | yes | no |
| `pin:lookup` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:map_keys` | value | value | value | value | yes | yes |
| `pin:map_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:map_values` | error `map requires key and value lists...` | value | value | value | rows only | no |
| `pin:ntz_cast_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:null_key_cast` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | error NULL_MAP_KEY | yes | yes |
| `pin:null_key_column` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | error NULL_MAP_KEY | yes | yes |
| `pin:null_key_literal` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | error NULL_MAP_KEY | yes | yes |
| `pin:null_value` | value | value | value | value | yes | yes |
| `pin:odd_arity_literal` | error `make_map requires an even number` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | yes | no |
| `pin:odd_arity_one` | error `make_map requires an even number` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | yes | no |
| `pin:odd_arity_three` | error `make_map requires an even number` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | yes | no |
| `pin:ordering_refusal` | error `map requires key and value lists...` | value | value | error INVALID_ORDERING_TYPE | yes | no |
| `pin:partial_null_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:predicate` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:size` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | yes | no |
| `pin:struct_value` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:timestamp_value` | error `map requires key and value lists...` | value | value | value | yes | no |
| `pin:two_pairs` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:widen_bigint` | error `map requires key and value lists...` | value | value | value | yes | yes |
| `pin:widen_decimal` | error `map requires key and value lists...` | value | value | value | yes | no |
| `target:range_map` | panic `list array` | value | value | value | yes | yes |
| `target:select_1` | value | value | value | value | yes | no |

Counts: on main the ANSI door panicked `list array` on 5 of the 47 statements and raised the same-length error on 14; on the head it does neither on any. The 14 statements that answered on main answer the identical rows and types on the head (14 of 14); 33 statements answer on the head, the same 33 as the Python native door. 43 of 47 equal the Python native door's answer, 4 more in rows only; 27 equal the Spark door's. The repeated-key and NULL-key refusals now carry Spark's `[DUPLICATED_MAP_KEY]` / `[NULL_MAP_KEY]` text on this door too, as on the Python native door. An odd argument count keeps DataFusion's `make_map requires an even number of arguments`.
