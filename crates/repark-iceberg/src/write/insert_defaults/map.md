# map — repark-iceberg/src/write/insert_defaults

## Purpose

Holds the unit-test directory for `../insert_defaults.rs`. The parent module declares
`#[cfg(test)] mod tests;`, which resolves to `tests/mod.rs`.

## Contents

- [tests/](tests/map.md) — the unit tests. A directory rather than `tests.rs` so the
  C-009 setter guard (`python/repark/tests/test_rp3_c009_write_default.py`) reads the
  test-only `with_write_default` builder as test code (run 21b round 2, 2026-09-18).
- `map.md` — this file.
