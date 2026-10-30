# ice-footer-cache-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/session/tests/footer_cache_report.rs::a_bad_footer_budget_fails_the_build_naming_the_key
C-002: crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::disabled_caches_wire_no_handle_and_no_context, crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::each_switch_is_honoured_alone, crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::every_builder_holds_the_session_footer_cache, crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::the_session_handles_reach_the_builder
C-003: crates/repark-core/src/session/tests/footer_cache_report.rs::a_zero_budget_session_reports_nothing_and_rereads_every_footer, crates/repark-core/src/session/tests/footer_cache_report.rs::the_session_reports_its_footer_cache_and_a_warm_scan_reads_no_footer, crates/repark-iceberg/src/catalog/tests/footer_cache.rs::a_warm_rescan_reads_no_data_file_footer_with_the_cache_on
C-004: crates/repark-iceberg/src/catalog/tests/footer_cache.rs::a_filtered_scan_after_an_unfiltered_one_upgrades_the_footer_and_answers_right
C-005: crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::every_builder_holds_the_session_footer_cache, crates/repark-iceberg/src/catalog/tests/cache_wiring.rs::the_counter_sees_every_request_with_the_caches_on, crates/repark-iceberg/src/catalog/tests/footer_cache.rs::the_counter_still_counts_every_page_read_with_the_footer_cache_on
C-006: crates/repark-core/src/session/tests/footer_cache_report.rs::two_sessions_never_share_a_footer_cache
C-007: crates/repark-core/src/session/tests/footer_cache_report.rs::a_zero_budget_session_reports_nothing_and_rereads_every_footer, crates/repark-core/src/session/tests/footer_cache_report.rs::the_session_reports_its_footer_cache_and_a_warm_scan_reads_no_footer
C-008: none (records)
C-009: none (records)
C-010: none (records)
