# map-pr-gate-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark-parity/tests/test_map_pr_gate_1.py::test_branch_mode_passes_when_code_and_map_change, scripts/check_map_md.sh
C-002: none (records)
C-003: none (records)
C-004: python/repark-parity/tests/test_map_pr_gate_1.py::test_branch_mode_ignores_deletion_only_changes
C-005: python/repark-parity/tests/test_map_pr_gate_1.py::test_branch_mode_handles_a_root_level_file
C-006: python/repark-parity/tests/test_map_pr_gate_1.py::test_staged_mode_warns_and_exits_zero
C-007: python/repark-parity/tests/test_map_pr_gate_1.py::test_duplicate_row_rule_fires_on_two_rows, python/repark-parity/tests/test_map_pr_gate_1.py::test_duplicate_row_rule_survives_fix, scripts/sync_map_md.py
C-008: python/repark-parity/tests/test_map_pr_gate_1.py::test_duplicate_row_rule_quiet_when_one_row_mentions_twice
C-009: python/repark-parity/tests/test_map_pr_gate_1.py::test_makefile_runs_branch_mode_over_base, scripts/check_map_md.sh
C-010: python/repark-parity/tests/test_map_pr_gate_1.py::test_both_hook_paths_call_the_staged_mode_identically, scripts/check_map_md.sh
C-011: python/repark-parity/tests/test_map_pr_gate_1.py::test_ci_guard_is_pr_only_and_gitattributes_carries_union, scripts/check_map_md.sh
C-012: scripts/check_map_md.sh, scripts/sync_map_md.py
