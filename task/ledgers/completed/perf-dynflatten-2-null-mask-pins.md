# perf-dynflatten-2-null-mask pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark-parity/bench/dynflatten/map.md, python/repark/tests/perf/test_dynflatten_null_mask.py::test_every_bed_shape_is_covered
C-002: crates/repark-core/src/dynamic_flatten.rs::dynamic_flatten, crates/repark-core/src/dynamic_flatten/null_mask.rs, python/repark/tests/dataframe/test_dynamic_flatten_divergences.py
C-003: crates/repark-core/src/dynamic_flatten/tests/preserve_nulls.rs, python/repark/tests/perf/test_dynflatten_null_mask.py::test_every_bed_shape_is_covered
C-004: python/repark/tests/parity/test_parity_live_dynflatten.py
C-005: python/repark-parity/bench/dynflatten/map.md
