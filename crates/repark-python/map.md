# map — repark-python

CC-3 (2026-08-30): comments condensed to one line; banners removed.

## Purpose

PyO3 adapter crate for `repark._native` (crate-DAG **tier 4**, bindings). It exposes session,
DataFrame, Column, exception, streaming Arrow, and ML bindings. Engine work remains in lower
crates; this crate owns the Python boundary and the PyO3/Arrow FFI `unsafe` boundary.

## Contents

- [`src/lib.rs`](src/lib.rs) registers the native module and maps engine errors.
- [`src/session.rs`](src/session.rs) provides synchronous session methods over the shared runtime.
- [`src/dataframe.rs`](src/dataframe.rs) provides immutable plans, actions, transforms, and lazy
  Arrow C Stream export.
- [`src/column/`](src/column/map.md) builds immutable DataFusion expressions and facade functions.
- [`src/fence.rs`](src/fence.rs) converts Rust panics at PyO3 and Arrow callback boundaries.
- [`src/ml.rs`](src/ml.rs) streams batches into native ML estimators; Python does not compute rows.
- [`src/exceptions.rs`](src/exceptions.rs) defines the PySpark-shaped exception taxonomy.
- [`src/allocator.rs`](src/allocator.rs) contains the optional wheel allocator.
- [`tests/`](tests/map.md) contains Rust integration coverage for the binding boundary.

## Contracts

- Spark sessions install both the Spark extension and dialect before build; `native` uses the
  stock DataFusion door.
- DataFrames remain reusable. Arrow export is lazy and bounded to one polled batch at a time.
- Arrow C Stream import drains under the GIL and retains all non-empty batches in the MemTable.
- PyO3 and Arrow callback failures remain typed Python or Arrow errors; no panic crosses FFI.
- ML fits stream Arrow batches into `repark-ml` and return parameter dictionaries.
- `read_excel` and `read_postgres` are loud unsupported operations in this build.

## Change locations

Add a native registration in `src/lib.rs`, a session method in `src/session.rs`, a DataFrame action
in `src/dataframe.rs`, or a Column function in `src/column/`. Update the matching map and tests.

## Verification

Use `cargo fmt --check`, `cargo test -p repark-python`, `make check-rust-file-size`,
`python3 scripts/sync_map_md.py --check`, and `git diff --check`. Keep `extension-module` off for
Rust tests.

## Pointers

- Up: [crates map](../map.md)
- Column navigation: [src/column/map.md](src/column/map.md)
- Test navigation: [tests/map.md](tests/map.md)
