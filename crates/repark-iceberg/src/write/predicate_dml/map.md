# map — repark-iceberg/src/write/predicate_dml

CC-3 (2026-08-30): comments condensed to one line; banners removed.

## Purpose

Test modules for [`predicate_dml.rs`](../predicate_dml.rs) (`execute_predicate_dml` — identity
DELETE / UPDATE with a subquery `WHERE`). They are `#[cfg(test)]` children of that module, split
into files because the parent sits near its size ceiling.

Created by **LRS-5 (2026-08-20)**: both had been included from `write/` with `#[path = "…"]`.
Source comments retain predicate and cleanup contracts; implementation narration is omitted.
AGENTS.md allows a test-fixture exception only where the canonical layout cannot work — here it
works, so the attribute is gone rather than documented.

## Contents

- [tests/](tests/map.md) — DELETE and identity UPDATE batteries.

## Pointers

- Up: [`../map.md`](../map.md)
