# gate-retire-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-002: crates/repark-iceberg/src/write/nested_ns_gate.rs, crates/repark-spark/tests/ntz_into_ltz_nested.rs
C-003: crates/repark-spark/src/router/nested_ns/tests.rs::a_column_that_resolves_in_no_input_reads_every_input_whole, crates/repark-spark/src/router/nested_ns/tests.rs::an_outer_reference_no_enclosing_join_resolves_fails_closed, crates/repark-spark/src/router/nested_ns/tests.rs::an_unmodelled_node_that_refers_out_reads_the_other_side_whole
C-004: crates/repark-spark/tests/ntz_into_ltz_nested.rs
C-005: crates/repark-spark/src/router/nested_ns/map.md, crates/repark-spark/tests/ntz_into_ltz_nested.rs
