# map — repark-core/src/config_file/tests

## Purpose

The `repark.toml` loader's pins (CFG-1). Split from the single `tests.rs` in step 3 when
the battery passed the 1,000-line file ceiling — stage pins versus wiring pins. See
[../map.md](../map.md).

## Contents

- `mod.rs` — the 40 stage pins (the seed's three, step-1 discovery/merge/interpolation,
  step-1b `$`-edge flips, step 2's catalog/database/redaction pins) plus the shared
  fixtures (`stub_environment`, `write_file`). Untouched by the split.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011,
  C-012, C-013, C-014, C-015, C-016, C-017
- `wiring.rs` — the 7 step-3 wiring pins (display/session/`conf` translation, the
  builder-profile-default precedence table, the redacted source-column dump, the
  byte-identical catalog done condition, the loud CFG-2 database refusal). Tempdir fixtures
  with stub environments throughout, so no pin mutates the process environment.
  pins: cfg-1/C-018, C-019, C-020, C-021, C-022, C-023, C-025

## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../wiring.rs](../wiring.rs)

## Debug

First checks: `cargo test -p repark-core config_file`.
