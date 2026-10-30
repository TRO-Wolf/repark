# ice-orc-avro-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/writes/test_ice_orc_avro_1.py::test_create_format_orc_insert_reads_back
C-002: python/repark/tests/writes/test_ice_orc_avro_1.py::test_create_format_avro_insert_reads_back
C-003: python/repark/tests/writes/test_ice_orc_avro_1.py::test_set_format_then_insert_writes_new_format
C-004: python/repark/tests/writes/test_ice_orc_avro_1.py::test_tp_format_orc_files_row
C-005: python/repark/tests/writes/test_ice_orc_avro_1.py::test_write_format_option_orc
C-006: python/repark/tests/writes/test_ice_orc_avro_1.py::test_write_format_option_avro
C-007: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_delete_on_orc_rewrites_as_orc
C-008: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_update_on_orc_rewrites_as_orc
C-009: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_merge_on_orc_rewrites_as_orc
C-010: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_delete_on_avro_rewrites_as_avro
C-011: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_update_on_avro_rewrites_as_avro
C-012: python/repark/tests/writes/test_ice_orc_avro_1.py::test_cow_merge_on_avro_rewrites_as_avro
C-013: python/repark/tests/writes/test_ice_orc_avro_1.py::test_mor_delete_on_orc_writes_orc_delete_file
C-014: python/repark/tests/writes/test_ice_orc_avro_1.py::test_delete_format_default_overrides
C-015: python/repark/tests/writes/test_ice_orc_avro_1.py::test_v3_delete_side_is_puffin_for_every_data_format
C-016: python/repark/tests/writes/test_ice_orc_avro_1.py::test_read_foreign_orc_table
C-017: python/repark/tests/writes/test_ice_orc_avro_1.py::test_orc_metrics_carry_full_column_metrics
C-018: python/repark/tests/writes/test_ice_orc_avro_1.py::test_avro_metrics_are_empty
C-019: python/repark/tests/writes/test_ice_orc_avro_1.py::test_all_types_round_trip_orc
C-020: python/repark/tests/writes/test_ice_orc_avro_1.py::test_all_types_round_trip_avro
C-021: python/repark/tests/writes/test_ice_orc_avro_1.py::test_unknown_write_format_refuses
C-022: python/repark/tests/writes/test_ice_orc_avro_1.py::test_compaction_keeps_table_format
C-023: python/repark/tests/writes/test_ice_orc_avro_1.py::test_partitioned_v3_cow_delete_rewrites_orc
C-025: python/repark/tests/writes/test_ice_orc_avro_1.py::test_unknown_delete_format_property_refuses
