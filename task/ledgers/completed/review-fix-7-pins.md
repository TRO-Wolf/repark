# review-fix-7 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/src/repark/config.py::_toml_text, python/repark/tests/conftest.py
C-002: crates/repark-core/src/config_file/map.md, crates/repark-core/src/config_file/tests/wiring.rs
C-003: crates/repark-core/src/config_file/discovery.rs, crates/repark-core/src/config_file/tests/wiring.rs, crates/repark-core/src/config_file/wiring.rs, crates/repark-core/src/session.rs::DATAFUSION_CONFIG_PREFIX, crates/repark-core/src/session.rs::DEFAULT_BATCH_SIZE, crates/repark-core/src/session.rs::apply_datafusion_config_keys, crates/repark-core/src/session.rs::batch_size, crates/repark-core/src/session.rs::build
