# text-write-timestamp-zone-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py
C-002: crates/repark-core/src/session/tests/path_write.rs::temporal_write_options_are_honored_on_csv_path_write, python/repark/tests/io/test_r2_read_formats2.py
C-003: none (records)
C-004: crates/repark-core/src/session/tests/text_write_format.rs
C-005: python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py::test_s3a_csv_write_matches_spark
C-006: none (records)
C-007: crates/repark-core/src/session/tests/path_write.rs::failed_render_append_preserves_existing_objects, crates/repark-core/src/session/tests/path_write.rs::failed_render_overwrite_into_empty_leaves_no_objects, crates/repark-core/src/session/tests/path_write.rs::failed_render_partitioned_overwrite_leaves_no_objects
C-008: python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py::test_failed_local_append_preserves_destination, python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py::test_failed_local_overwrite_leaves_no_destination, python/repark/tests/timestamps/test_text_write_timestamp_zone_1.py::test_failed_local_partitioned_overwrite_leaves_no_destination
C-009: crates/repark-core/src/session/tests/path_write.rs::failed_render_append_leaves_concurrent_objects_alone, crates/repark-core/src/session/tests/path_write.rs::failed_render_root_append_leaves_foreign_prefixes_alone
