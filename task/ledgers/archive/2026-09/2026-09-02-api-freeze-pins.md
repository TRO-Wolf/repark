# api-freeze pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark-parity/tests/test_api_freeze.py::test_a_changed_decision_is_red, python/repark-parity/tests/test_api_freeze.py::test_every_decision_equals_its_recommendation
C-002: python/repark-parity/tests/test_api_freeze.py::test_the_freeze_pointers_are_in_lockstep, python/repark-parity/tests/test_api_freeze.py::test_the_policy_sentence_is_the_owners_wording
C-003: python/repark-parity/tests/test_api_freeze.py, scripts/build_api_freeze.py
C-004: python/repark-parity/tests/test_api_freeze.py::test_the_freeze_pointers_are_in_lockstep, scripts/ledger_lifecycle.py
