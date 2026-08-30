# map — repark-functions/src/collection

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

The child modules of [`collection.rs`](../collection.rs). Each holds one Spark collection shim whose
implementation is large enough that keeping it in the parent would push a ceiling; the parent
declares them (`mod str_to_map;` …) and registers their UDFs.

Rust's default module layout keeps these children beside `collection.rs`; no `mod.rs` rename is
needed.

## Contents

- `str_to_map.rs` — regex `str_to_map` (Spark treats both delimiters as regular
  expressions, where the DataFusion kernel splits on literals). Exports
  `bind_ascii_perl_classes`. Depends on workspace `regex`.
- `shuffle.rs` — **X1:** NULL-guarded `shuffle`; the upstream kernel panics on an all-NULL list.
- `map_from_entries.rs` — **X7:** `map_from_entries` under Spark's `EXCEPTION` map-key dedup
  policy (duplicate keys raise rather than last-wins).

## I want to...

| ...do this | go to |
|---|---|
| add a collection shim | a new file here, declared and registered in [`../collection.rs`](../collection.rs) |
| find where these are registered | `collection::functions()` in [`../collection.rs`](../collection.rs) |
| see why the parent is split at all | crate-root and file-size ceilings — [`../../map.md`](../../map.md) |

## Pointers

- Up: [`../../map.md`](../../map.md)
