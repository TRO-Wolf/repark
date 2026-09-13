# map — repark-core/src/named_sources

## Purpose

File-backed pins for named database sources (`../named_sources.rs`) — CFG-2 step 1: the
declared-source listing, the `source(name)` handle, and the refusing catalog provider
that answers for a registered source name until its connector lands (roadmap 1.10).

## Contents

- `tests.rs` — the seven step-1 pins (`#[cfg(test)] mod tests;` in `../named_sources.rs`):
  a `SELECT` under a registered source name answers the D-1 connector message naming the
  source path, the kind, and `1.10` (never the engine's not-found); registration over an
  unroutable host opens no connection; `ping()` refuses the same message; `sources()`
  lists name / kind spelling / key path / `auto_register` with secret props redacted;
  `auto_register = false` lists without registering (the engine's not-found answers SQL
  under the name); a catalog registered over a source name refuses as a duplicate; an
  unknown handle refuses naming the declared sources. Config fixtures are forced temp
  files, so no pin reads the ambient environment.
  pins: cfg-2/C-003, C-004, C-005, C-006, C-007, C-008, C-009

## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../named_sources.rs](../named_sources.rs)
- The loader stage that parses `SourceSpec`s: [../config_file/sources.rs](../config_file/sources.rs)

## Debug

First checks: `cargo test -p repark-core named_sources`.
