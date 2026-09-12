# map — repark-core/src/silver/fixtures

## Purpose

TOML fixtures for the SILVER-S1 typed plan. Loaded by `src/silver/tests/`. See [../map.md](../map.md).

## Contents

- [positive/](positive/map.md) — six distinct valid plans plus spelling variants of the §8 example.
- [negative/](negative/map.md) — one file per `SilverRefusal` variant, plus a nested unknown-key path.

pins: silver-s1/C-007

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-core silver`.
