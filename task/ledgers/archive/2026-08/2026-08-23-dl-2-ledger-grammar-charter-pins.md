# dl-2-ledger-grammar-charter pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_row_without_evidence_goes_red, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_verdict_cell_and_duplicate_id_go_red
C-002: python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_clean_shape_is_green_and_counts
C-003: python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_archived_and_completed_clauses_can_be_cited, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_unpinned_proven_clause_and_dead_citation_go_red
C-004: python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_attestation_required_once_no_clause_is_open, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_attestation_shape_defects_go_red, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_finding_record_fields_are_checked, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_ledger_without_a_clause_table_goes_red, python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_refuses_to_pass_closed_with_no_ledgers
C-005: python/repark-parity/tests/test_dl_2_ledger_grammar.py::test_exceptions_table_ratchets_down_only
C-006: python/repark-parity/tests/test_dl_2_ledger_grammar.py
