# map — repark-python

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

PyO3 adapter crate for `repark._native` (crate-DAG **tier 4**, bindings). It exposes session,
DataFrame, Column, exception, streaming Arrow, and ML bindings. Engine work remains in lower
crates; this crate owns the Python boundary and the PyO3/Arrow FFI `unsafe` boundary.

## Contents

- [`src/lib.rs`](src/lib.rs) registers the native module and maps engine errors.
- [`src/session.rs`](src/session.rs) provides synchronous session methods over the shared runtime.
- [`src/session_runtime.rs`](src/session_runtime.rs) applies one validated runtime conf value
  to the live session (`set_runtime_config`, strict; `restore_runtime_config`, builder-lenient
  for `RESET`). **SET-ANSI-RUNTIME-1 (2026-09-15).** pins: set-ansi-runtime-1/C-001, C-002, C-003
- [`src/dataframe.rs`](src/dataframe.rs) provides immutable plans, actions, transforms, and lazy
  Arrow C Stream export. **FNP-4B (2026-09-15):** `filter_sql` canonicalizes the predicate with
  the Spark-door lexer and maps downstream locations back; `F.expr` contexts parse Databricks.
- [`src/dataframe_stats.rs`](src/dataframe_stats.rs) — **DF-RUST-3 (2026-09-15):** the
  `freq_items` and `transpose` pyfunctions over `PyDataFrame` (the `dataframe_stack`
  `#[pyfunction]` pattern — one `#[pymethods]` block per class). `transpose` returns the
  display-name overlay with the frame and attaches `_spark_error_class` /
  `_spark_message_parameters` / `_spark_sql_state` on the raised exception.
- [`src/collect_rows.rs`](src/collect_rows.rs) materializes Arrow record batches as Python value
  tuples for `collect`. It imports the batch back through the Arrow C Data Interface
  (`__arrow_c_array__`) and converts only the cell kinds whose `to_pylist` mapping is
  unambiguous — null, boolean, every integer width, `f32`/`f64`, the three UTF-8 layouts and
  the three binary layouts. Every other kind is either supplied pre-converted by the facade or
  declined (`None`), so the facade's own converter keeps decimals, dates, times, timestamps,
  intervals and nested values bit-for-bit. pins: perf-facade-1/C-002
- [`src/logical_names.rs`](src/logical_names.rs) answers `DataFrame.columns` from the plan's
  logical schema without an analyzer pass. Sound because every analyzer rule in
  `repark_functions::analyzer_rules` rewrites through `NamePreserver` and none adds, drops or
  reorders a projection expression; held by a byte-equality pin against the analyzed names.
  pins: perf-facade-1/C-004
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
- Row materialization converts cells, never rows: the binding emits value tuples and the facade
  builds every `Row`, so `Row` semantics have exactly one implementation.
- `column_names` stays analyzer-backed and is the oracle the logical-name pin measures against;
  only `logical_column_names` skips analysis.
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
- Column navigation: `src/column/map.md`
- Test navigation: `tests/map.md`
