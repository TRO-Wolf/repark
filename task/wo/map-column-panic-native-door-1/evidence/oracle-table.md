# MAP-COLUMN-PANIC-NATIVE-DOOR-1 — the oracle replay on the native door

Each row is one statement run on the native `repark.sql()` door on main (the `main` wheel) and on the head, and on the Spark door (`ReparkSession.sql`, session zone UTC) on the head. The 47 statements are the 12 distinct `map.*` shapes of the MAP-COLUMN-PANIC-1 corpus delta (`task/wo/map-column-panic-1/evidence/corpus-delta.json`: the 49 cells that left the list to EQUAL and the 11 that stayed, each shape counted once across its five zones), the 33 cells of `map_column_panic_1_spark_oracle.json`, and two controls (`SELECT map('a', id) AS m FROM range(1)` and `SELECT 1 AS one`). The brief named eight shapes; the delta holds twelve, and all twelve are here. The native door has no temp views and no `timestamp_micros`, so its statements read the view as an inline subquery with `to_timestamp_micros`; the Spark door reads the temp view. The native door has no session-zone knob, so the zone-sensitive shapes run in its own zone beside the Spark door's UTC.

"equals Spark" means the Arrow rows and the Arrow types are equal, or both doors refuse in the same class. A "no" row carries the reason; each reason is reproduced by a map-free control in [controls-head-native.json](controls-head-native.json) and [controls-head-spark.json](controls-head-spark.json), so it belongs to the native door, not to the map constructor.

| shape | native door on main | native door on head | Spark door (head) | head native equals Spark | note |
|---|---|---|---|---|---|
| `corpus:map.col_inst.cast_ntz.inner` | panic `list array` | value | value | yes |  |
| `corpus:map.col_inst.cast_string.inner` | panic `list array` | value | value | no | string cast spelling (control string_cast_of_instant) |
| `corpus:map.col_inst.ccast_ntz` | value | value | value | yes |  |
| `corpus:map.col_inst.ccast_string` | value | value | value | no | string cast spelling (control string_cast_of_instant) |
| `corpus:map.col_inst.to_json` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | no | to_json is Spark-only (control to_json) |
| `corpus:map.col_wall.cast_ltz.inner` | panic `list array` | value | value | no | native TIMESTAMP carries no zone (control instant_literal) |
| `corpus:map.col_wall.ccast_ltz` | value | value | value | yes |  |
| `corpus:map.inst.cast_date.inner` | value | value | value | yes |  |
| `corpus:map.inst.cast_string.inner` | value | value | value | no | string cast spelling (control string_cast_of_instant) |
| `corpus:map.inst.date_format.inner` | value | value | value | no | date_format pattern not applied on the native door (control date_format_pattern) |
| `corpus:map.inst.to_date.inner` | value | value | value | yes |  |
| `corpus:map.wall.cast_string.inner` | value | value | value | no | string cast spelling (control string_cast_of_instant) |
| `pin:array_value` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | no | array is Spark-only (control array_ctor) |
| `pin:cast_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:column_key` | value | value | value | yes |  |
| `pin:column_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:column_value_one_row` | panic `list array` | value | value | yes |  |
| `pin:create_map_unresolved` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | yes (same refusal) | both refuse create_map as unresolved |
| `pin:duplicate_key` | error `map key must be unique` | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | yes (same refusal) |  |
| `pin:duplicate_literal` | error `map key must be unique` | error DUPLICATED_MAP_KEY | error DUPLICATED_MAP_KEY | yes (same refusal) |  |
| `pin:empty_control` | error `map expected at least one argument` | error `map expected at least one argument` | value | no | map() is refused by the native door (control empty_map) |
| `pin:function_value` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | no | year is Spark-only (control year) |
| `pin:int_key` | value | value | value | yes |  |
| `pin:literal_control` | value | value | value | no | rows equal, integer literal width differs |
| `pin:lookup` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:map_keys` | value | value | value | yes |  |
| `pin:map_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:map_values` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:ntz_cast_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:null_key_cast` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | yes (same refusal) |  |
| `pin:null_key_column` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | yes (same refusal) |  |
| `pin:null_key_literal` | error `map key cannot be null` | error NULL_MAP_KEY | error NULL_MAP_KEY | yes (same refusal) |  |
| `pin:null_value` | value | value | value | yes |  |
| `pin:odd_arity_literal` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | no | native keeps the planner text for an odd count |
| `pin:odd_arity_one` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | no | native keeps the planner text for an odd count |
| `pin:odd_arity_three` | error `make_map requires an even number` | error `make_map requires an even number` | error WRONG_NUM_ARGS | no | native keeps the planner text for an odd count |
| `pin:ordering_refusal` | error `map requires key and value lists...` | value | error INVALID_ORDERING_TYPE | no | native door compares maps, Spark refuses (control map_equality) |
| `pin:partial_null_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:predicate` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:size` | UNRESOLVED_ROUTINE | UNRESOLVED_ROUTINE | value | no | size is Spark-only (control size) |
| `pin:struct_value` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:timestamp_value` | error `map requires key and value lists...` | value | value | no | native TIMESTAMP carries no zone (control instant_literal) |
| `pin:two_pairs` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:widen_bigint` | error `map requires key and value lists...` | value | value | yes |  |
| `pin:widen_decimal` | error `map requires key and value lists...` | value | value | no | native 2.5 is DOUBLE, Spark's is DECIMAL (control decimal_literal) |
| `target:range_map` | panic `list array` | value | value | yes |  |
| `target:select_1` | value | value | value | no | rows equal, integer literal width differs |

Counts: 28 of the 47 statements equal the Spark door; 19 differ, every one for a reason named in the last column. On main the native door panicked `list array` on 5 of the 47 and raised `map requires key and value lists to have the same length` on 14; on the head it does neither on any. Every statement that answered on main answers the same rows and types on the head (14 of 14 identical), and the Spark door's 47 answers are identical on main and on the head. Raw answers: [replay-main-native.json](replay-main-native.json), [replay-head-native.json](replay-head-native.json), [replay-main-spark.json](replay-main-spark.json), [replay-head-spark.json](replay-head-spark.json).

Two changes of text on the native door, both refusals before and after: a repeated key (`map('a', 1, 'a', 2)`, `map('a', id, 'a', id + 1)`) now raises `[DUPLICATED_MAP_KEY] Duplicate map key a was found` where DataFusion's `map key must be unique, duplicate key found: a` came out, and a NULL key (`map(NULL, 1)`, `map(nk, id)`) raises `[NULL_MAP_KEY] Cannot use null as map key` where `map key cannot be null` came out. The rule sends a map with a repeated or NULL key to the per-row kernel by design, so the native door speaks Spark's text for these two refusals. An odd argument count keeps DataFusion's `make_map requires an even number of arguments` on the native door; the Spark door's `WRONG_NUM_ARGS` rewrite is the Spark door's keyword lowering.
