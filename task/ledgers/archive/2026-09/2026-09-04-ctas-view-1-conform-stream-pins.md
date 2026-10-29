# ctas-view-1-conform-stream pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/ctas.rs::CreatePlan, crates/repark-spark/src/ctas.rs::check_custom_location, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/writes/test_ctas_view_typed.py
C-002: crates/repark-iceberg/src/write/conform.rs::conform_batch, crates/repark-iceberg/src/write/merge/mod.rs::quote_ident, crates/repark-iceberg/src/write/store_assign.rs::ansi_store_assignable, crates/repark-iceberg/src/write/store_assign.rs::normalize_for_assignment, crates/repark-iceberg/src/write/store_assign.rs::refuse_unless_ansi_store_assignable, crates/repark-iceberg/src/write/store_assign.rs::refuse_unless_write_store_assignable, crates/repark-spark/src/ctas.rs::CreatePlan, crates/repark-spark/src/ctas.rs::check_custom_location, crates/repark-sql/src/create_table.rs
C-003: crates/repark-spark/src/tests/show_create.rs
C-004: python/repark/tests/writes/test_ctas_view_typed.py
