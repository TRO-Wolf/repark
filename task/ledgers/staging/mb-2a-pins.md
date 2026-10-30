# mb-2a pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/sink_offsets_tests.rs::dm5_empty_merge_append_commits_one_stamp_only_append_snapshot
C-002: crates/repark-iceberg/src/write/sink_offsets_tests.rs::a_second_stamped_commit_in_one_batch_refuses, crates/repark-iceberg/src/write/sink_offsets_tests.rs::scope_holds_one_stamp_per_sink_and_claims_once
C-003: crates/repark-iceberg/src/write/sink_offsets_tests.rs::append_arm_stamps_summary_and_property_in_one_commit_on_both_doors, crates/repark-iceberg/src/write/sink_offsets_tests.rs::copy_on_write_arm_stamps_both_overwrite_shapes, crates/repark-iceberg/src/write/sink_offsets_tests.rs::merge_on_read_arm_stamps_the_row_delta
C-004: none (records)
C-005: crates/repark-iceberg/src/write/sink_offsets_tests.rs::record_commit_refuses_a_head_without_the_stamp
C-006: none (records)
C-007: crates/repark-iceberg/src/write/sink_offsets_tests.rs::load_table, crates/repark-iceberg/src/write/sink_offsets_tests.rs::resume_reads_the_last_offset_from_one_loaded_table
C-008: none (records)
C-009: crates/repark-iceberg/src/write/sink_offsets_tests.rs::resume_refuses_a_newer_offset_format, crates/repark-iceberg/src/write/sink_offsets_tests.rs::resume_refuses_a_property_whose_stamped_snapshot_expired, crates/repark-iceberg/src/write/sink_offsets_tests.rs::resume_refuses_when_summary_and_property_disagree
C-010: none (records)
C-011: none (records)
C-012: none (records)
C-013: none (records)
C-014: none (records)
C-015: none (records)
C-016: none (records)
C-017: none (records)
C-018: none (records)
C-019: crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::a_bare_empty_merge_append_refuses_on_an_empty_and_a_seeded_sink, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::a_racing_stamp_of_another_query_keeps_both_records, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::an_injected_failure_lands_neither_half_on_every_arm, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::an_unknown_outcome_that_landed_holds_both_halves_on_every_arm, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::interleaved_queries_each_resume_their_own, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::one_catalog_request_carries_both_halves_on_every_arm, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::resume_never_falls_back_to_an_older_stamp_when_the_newest_expired, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::resume_reads_a_stamp_only_chain_and_a_root_stamp, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::resume_refuses_a_newer_format_on_both_halves, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::resume_refuses_a_property_behind_the_summary, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::three_racing_appends_still_stamp_exactly_once_on_every_arm, crates/repark-iceberg/src/write/sink_offsets_probe_tests.rs::update_table
C-020: none (records)
C-021: none (records)
C-022: none (records)
