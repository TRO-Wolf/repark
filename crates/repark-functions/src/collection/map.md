# map — repark-functions/src/collection

## Purpose

The child modules of [`collection.rs`](../collection.rs). Each holds one Spark collection shim whose
implementation is large enough that keeping it in the parent would push a ceiling; the parent
declares them (`mod str_to_map;` …) and registers their UDFs.

Created by **LRS-5 (2026-08-20)**, which moved these three files out of the crate `src/` root and
deleted the `#[path = "…"]` attributes that had been including them from there. AGENTS.md requires
Rust's default module layout; `collection.rs` may sit beside `collection/` in Rust 2018, so nothing
needed renaming to `mod.rs`.

## Contents

- `str_to_map.rs` — **FN-GT2 rework:** regex `str_to_map` (Spark treats both delimiters as regular
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
