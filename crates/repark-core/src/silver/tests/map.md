# map — repark-core/src/silver/tests

## Purpose

Contract pins for the SILVER-S1 typed plan. File-backed so `src/lib.rs` stays a manifest.
See [../map.md](../map.md).

## Contents

- `mod.rs` — fixture loader and the D-6 / D-7 pins.
- `parse.rs` — D-1 / D-2 / D-3 parse and structural-refusal pins. `toml_syntax_refuses`
  uses inline invalid text (A-1).
- `identity.rs` — D-4 canonical bytes, D-5 explain goldens, P2-1 golden bytes pin.
- `measure.rs` — `#[ignore]`d 50/500-column `canonical()` / `explain()` wall and size
  printer (C-009).

pins: silver-s1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-009, C-010

## Pointers

- Up: [../map.md](../map.md)
- Fixtures: [../fixtures/map.md](../fixtures/map.md)

## Debug

First checks: `cargo test -p repark-core silver`.
