# ntz-store-doors-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/ntz_store.rs::by_name_stores_the_ltz_session_wall, crates/repark-spark/src/tests/ntz_store.rs::column_list_overwrite_stores_the_ltz_session_wall, crates/repark-spark/src/tests/ntz_store.rs::date_stores_the_session_midnight_through_every_door, crates/repark-spark/src/tests/ntz_store.rs::dynamic_overwrite_stores_the_ltz_session_wall, crates/repark-spark/src/tests/ntz_store.rs::overwrite_by_name_stores_the_ltz_session_wall, crates/repark-spark/src/tests/ntz_store.rs::overwrite_partition_stores_the_ltz_session_wall, crates/repark-spark/src/tests/ntz_store.rs::overwrite_stores_the_ltz_session_wall
C-002: crates/repark-spark/src/tests/ntz_store.rs::date_stores_the_session_midnight_through_every_door
C-003: crates/repark-spark/src/tests/ntz_store.rs::date_stores_the_session_midnight_through_every_door, crates/repark-sql/tests/ansi_ntz_wall_cast.rs::ansi_merge_into_an_instant_column_keeps_the_utc_reading
C-004: crates/repark-spark/src/tests/ntz_store.rs::date_stores_the_session_midnight_through_every_door
C-005: none (records)
C-006: crates/repark-spark/src/tests/ntz_values_mix.rs::mixed_values_type_the_column_timestamp_through_the_session_zone, crates/repark-spark/src/tests/ntz_values_mix.rs::unmixed_values_keep_their_timestamp_type
C-007: crates/repark-spark/src/tests/ntz_values_mix.rs::date_ntz_values_type_the_column_timestamp_ntz, crates/repark-spark/src/tests/ntz_values_mix.rs::date_timestamp_mixes_keep_their_timestamp_type
C-008: crates/repark-spark/src/tests/ntz_values_mix.rs::a_written_cast_as_timestamp_beside_null_stays_timestamp, crates/repark-spark/src/tests/ntz_values_mix.rs::a_written_cast_as_timestamp_stores_sparks_walls, crates/repark-spark/src/tests/ntz_values_mix.rs::a_written_cast_as_timestamp_types_the_column_timestamp
C-009: crates/repark-spark/src/tests/ntz_values_mix.rs::an_expression_cell_beside_a_date_keeps_the_timestamp_type, crates/repark-spark/src/tests/ntz_values_mix.rs::an_expression_cell_beside_a_date_stores_sparks_walls
