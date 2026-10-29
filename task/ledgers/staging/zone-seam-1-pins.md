# zone-seam-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: none (records)
C-002: none (records)
C-003: none (records)
C-004: python/repark/tests/timestamps/ntz_9_verify_spark_oracle.json
C-005: none (records)
C-006: python/repark/tests/timestamps/zone_oracle_1_main_difference.json
C-007: python/repark/tests/timestamps/test_zone_seam_1.py
C-008: python/repark/tests/timestamps/test_zone_seam_1.py
C-009: crates/repark-spark/tests/ntz_nested_refuse_casts.rs::CONTAINER_CASTS, crates/repark-spark/tests/ntz_nested_refuse_casts.rs::IN_A_SUBQUERY, crates/repark-spark/tests/ntz_nested_refuse_casts.rs::ON_LINEAGE, crates/repark-spark/tests/ntz_nested_refuse_casts.rs::a_container_cast_of_an_instant_stores_the_session_wall_on_every_door_outside_utc, crates/repark-spark/tests/ntz_nested_refuse_casts.rs::a_scalar_subquery_is_read_by_its_output_and_a_boolean_subquery_is_skipped, crates/repark-spark/tests/ntz_nested_refuse_casts.rs::the_cast_walk_reads_only_what_reaches_the_column, python/repark/tests/timestamps/test_ice_ntz_nested_refuse_1.py
C-010: crates/repark-functions/src/tests/zone_seam_pairs.rs::a_value_under_a_null_struct_never_reaches_an_ansi_leaf
C-011: none (records)
C-012: none (records)
C-013: crates/repark-functions/src/tests/zone_seam_pairs.rs
C-014: python/repark/tests/timestamps/zone_horizon_render_1_residue.json
C-015: crates/repark-spark/tests/ntz_into_ltz_nested.rs, python/repark/tests/timestamps/test_zone_seam_1.py
C-016: crates/repark-iceberg/src/write/nested_ns_gate/tests.rs, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partitioned_overwrite_in_utc_stores_the_utc_read, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partitioned_overwrite_refuses_a_nested_wall_outside_utc_and_writes_no_file
C-017: crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partitioned_overwrite_in_utc_stores_the_utc_read, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partitioned_overwrite_refuses_a_nested_wall_outside_utc_and_writes_no_file
C-018: crates/repark-functions/src/tests/zone_seam_pairs.rs::a_container_cast_converts_every_element_of_every_row_of_a_list, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_plain_update_of_a_struct_with_a_cast_wall_text_stores_spark_instant, crates/repark-spark/tests/ntz_into_ltz_nested.rs::an_update_of_an_inner_struct_with_a_wall_refuses_outside_utc, python/repark/tests/timestamps/test_zone_seam_1.py
C-019: none (records)
C-020: none (records)
C-021: crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partition_column_before_the_struct_still_refuses_outside_utc_and_writes_no_file, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_partition_column_before_the_struct_stores_the_utc_read_in_utc
C-022: none (records)
C-023: crates/repark-spark/tests/timestamp_ns_nested_shapes.rs::a_partitioned_insert_refuses_a_nested_nanosecond_leaf_even_for_a_bare_null
C-024: crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_plain_update_whose_only_wall_is_the_shifted_cast_stores_spark_instant, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_plain_update_with_an_unshifted_wall_beside_the_shifted_cast_refuses_outside_utc, crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_plain_update_with_two_walls_in_utc_stores_the_utc_read
C-025: none (records)
C-026: none (records)
C-027: none (records)
C-028: crates/repark-spark/tests/ntz_into_ltz_nested.rs::a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant
C-029: none (records)
