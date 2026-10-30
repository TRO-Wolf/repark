# mw-8-maintenance-runbook pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_mw8_runbook.py::test_the_runbook_runs_the_documented_procedures_in_order
C-002: python/repark/tests/iceberg/test_mw8_runbook.py::test_position_delete_compaction_folds_the_deletes_to_one_per_partition
C-003: python/repark/tests/iceberg/test_mw8_runbook.py::test_data_compaction_reduces_the_data_file_count
C-004: python/repark/tests/iceberg/test_mw8_runbook.py
C-005: python/repark/tests/iceberg/test_mw8_runbook.py::test_manifest_compaction_drops_the_manifest_count
C-006: python/repark/tests/iceberg/test_mw8_runbook.py::test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held
C-007: python/repark/tests/iceberg/test_maintenance_call.py::test_remove_orphan_files_floor_matches_spark, python/repark/tests/iceberg/test_mw8_runbook.py::test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor
C-008: python/repark/tests/iceberg/test_mw8_runbook.py::test_the_runbook_never_changes_the_row_set
C-009: python/repark/tests/iceberg/test_mw8_runbook.py::test_the_guide_section_links_every_source_it_names
C-010: python/repark/tests/iceberg/test_mw8_runbook.py::test_the_printed_cycle_matches_the_sequence_the_engine_runs
