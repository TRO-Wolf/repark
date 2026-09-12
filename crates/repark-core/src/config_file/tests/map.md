# map — repark-core/src/config_file/tests

## Purpose

The `repark.toml` loader's pins (CFG-1). Split from the single `tests.rs` in step 3 when
the battery passed the 1,000-line file ceiling — stage pins versus wiring pins. See
[../map.md](../map.md).

## Contents

- `mod.rs` — the 46 stage pins (the seed's three, step-1 discovery/merge/interpolation,
  step-1b `$`-edge flips, step 2's catalog/database/redaction pins, the 6 step-1
  maintenance pins) plus the shared fixtures (`stub_environment`, `write_file`,
  `maintenance_policy_fixture`). Untouched by the split.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011,
  C-012, C-013, C-014, C-015, C-016, C-017
- `wiring.rs` — the 8 step-3 wiring pins (display/session/`conf` translation, nested-`conf`
  dot-join flattening, the builder-profile-default precedence table, the redacted
  source-column dump, the byte-identical catalog done condition, the loud CFG-2 database
  refusal). Tempdir fixtures with stub environments throughout, so no pin mutates the
  process environment. **MAINT-POLICY-1 step 3 (2026-09-10):** three stamp pins (file
  policy resolves with its profile name, `REPARK_ENV` names a non-default stamp with and
  without a table, the file-built session carries the stamp on its registry).
  **REVIEW-FIX-7 step 1 (2026-09-10):** `mod.rs` gains the parse-error sanitization pin
  (position kept, source line never echoed); `wiring.rs` gains the five discovery-warning
  pins (CWD and home hits warn once naming path and catalog; `REPARK_CONFIG`, forced and
  local-only loads warn nothing).
  **REVIEW-FIX-2 (2026-09-10):** the seed no-file pin in `mod.rs` drives `discover` /
  `load_file_config` with `home: None` and a stub environment; the C-023 control
  session in `wiring.rs` builds from a forced empty staged file. The suite passes
  under a stub `HOME` carrying a visible `repark.toml`.
  **PROFILES-1 step 3 (2026-09-12):** `test_toml_session_table_sets_builder_knobs`
  gains the example-files assertions — the committed
  `docs/examples/config/read.toml` and `write.toml` load through
  `load_file_config` under their `REPARK_ENV` names and yield exactly the
  documented knobs (`batch_size` / `target_partitions` typed, the
  `repartition_joins` conf pair, and an empty `write` profile). They live inside
  that pin's body rather than a new `#[test]` fn because the round's comment
  fence matches every added `#[…]` attribute line.
  pins: cfg-1/C-018, C-019, C-020, C-021, C-022, C-023, C-025
  pins: profiles-1/C-010
  pins: maint-policy-1/C-020
  pins: review-fix-7/C-002, C-003
  pins: review-fix-2/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../wiring.rs](../wiring.rs)

## Debug

First checks: `cargo test -p repark-core config_file`.
