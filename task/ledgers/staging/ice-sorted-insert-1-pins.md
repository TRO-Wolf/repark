# ice-sorted-insert-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/writes/test_ice_sorted_insert_1.py::test_sql_insert_writes_sorted_stamped_files
C-002: python/repark/tests/writes/test_ice_sorted_insert_1.py::test_dataframe_doors_write_sorted_stamped_files
C-003: python/repark/tests/writes/test_ice_sorted_insert_1.py::test_repark_owned_paths_write_sorted_stamped_files
C-004: python/repark/tests/writes/test_ice_sorted_insert_1.py
C-005: python/repark/tests/writes/test_ice_sorted_insert_1.py
C-006: crates/repark-iceberg/src/write/merge/tests/lineage_stream.rs, python/repark/tests/writes/test_ice_sorted_insert_2.py::_drive
C-007: python/repark/tests/writes/test_ice_sorted_insert_2.py::_drive, python/repark/tests/writes/test_ice_sorted_insert_2.py::test_reordered_table_stamps_the_current_order, python/repark/tests/writes/test_ice_sorted_insert_2.py::test_unpartitioned_overwrite_sorts_and_stamps
C-008: python/repark/tests/writes/test_ice_sorted_insert_2.py::_drive, python/repark/tests/writes/test_ice_sorted_insert_2.py::test_owned_float_overwrite_canonicalises_negative_nan, python/repark/tests/writes/test_ice_sorted_insert_2.py::test_owned_float_overwrite_places_nan_like_spark
C-009: python/repark/tests/writes/test_ice_sorted_insert_2.py::_drive
C-010: crates/repark-iceberg/src/write/merge/tests/lineage_stream.rs, python/repark/tests/writes/test_ice_sorted_insert_2.py::_drive, python/repark/tests/writes/test_ice_sorted_insert_2.py::test_v3_lineage_survives_the_default_order_sort
