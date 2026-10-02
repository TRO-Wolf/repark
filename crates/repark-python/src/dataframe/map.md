# map — repark-python/src/dataframe

## Purpose

`PyDataFrame` is the Python-facing immutable plan plus its shared runtime: lazy
transforms, terminal actions, schema introspection, and Arrow C Stream export.

## Modules

- [`mod.rs`](mod.rs) — the binding: plan access, terminals, builders, schema, and
  stream export.
  **DEEP-FILTER-CHAIN-CRASH-1 verifier fold (2026-09-29):** the inline test
  module moved here to `tests.rs` untouched (the file would otherwise pass its
  ceiling); terminals and `analyzed_arrow_schema_native` drive through
  `frame_drive_segment` plus `block_on_grown_sized`, and the column-taking
  builders (`select`, `filter`, `with_column`, `sort`, `join_on_condition`,
  `aggregate`) refuse past the expression cap and grow past the nesting depth
  (or on a small calling stack) through `drive_columns`. The relocation shed
  every doc comment per the comment ban, so the `PyResult`-returning fns carry
  per-item `#[allow(clippy::missing_errors_doc)]` (repo precedent:
  `repark-distributed`).
  **Limits fold (2026-09-29):** `filter_sql` keeps only the 4 KiB growth gate
  (the text refusal is gone); terminals count non-`Union` nodes toward the
  8192 plan cap, so union spines answer past it and filter-led plans refuse.
  pins: deep-filter-chain-crash-1/C-001, C-010
  **Re-verify fold (2026-09-30, C-013..C-018, CI perf):** `PyDataFrame` caches
  its `PlanDepths`: `new` surveys once, single-node builders compose O(1)
  through `new_with_depths`, conditional-shape builders re-survey. Terminals
  and `analyzed_arrow_schema_native` size from the cache; the analyzed plan
  drops inside the grown future. Every builder clone runs through
  `grown_sync`/`grown_clone_frame`/`drive_columns`, and `drive_columns` grows
  on frame depth too. Builders that attach a column carrying a subquery plan
  (`select`, `filter`, `with_column`, `sort`, `aggregate`, `join_on_condition`,
  SQL-text `filter` with a subquery predicate) re-survey the built plan instead
  of composing O(1): the O(1) rule dropped the carried plan levels and the
  terminal undersized its segment (scalar over a 1000-deep chain SIGSEGVs
  without it). `drive_columns` also grows on carried plan depth, and the
  builder-internal schema walks (`distinct_on`, the names helpers,
  `logical_column_names`) run inside the grown region. `Drop` tears the plan
  down on a grown segment sized from the cache. The `df` field is `ManuallyDrop<DataFrame>` so `Drop` can run the
  teardown on a grown stack: a replace-with-placeholder needs an owned spare
  `SessionState`, whose clone is HashMap-plus-String churn (~100us, paid per
  frame), while `ManuallyDrop::drop` costs nothing. The one `unsafe` call runs
  at most once per handle because `Drop::drop` runs at most once and
  `ManuallyDrop`'s own drop is a no-op, so no double-drop exists; the binding
  crate already allows `unsafe` for PyO3 macros. pins: deep-filter-chain-crash-1/C-013, C-016
  **CAST-OVERFLOW-INSERT-1 re-verify VO2-2 (2026-09-29):** `limit_with_skip`
  takes an optional fetch, so DataFrame `.offset` plans a fetch-less `Limit`
  matching Spark's pure Offset; the facade's large-fetch encoding is retired.
  pins: cast-overflow-insert-1/C-001
  **Merge-main fold (2026-10-01, PR #883):** `filter` / `select` route through
  `is_duplicated::filter_frame` / `select_frame` inside the `drive_columns`
  closure, so a marker rewrite runs on the grown stack under the same verdict
  as the plain clone; the builders re-survey levels when the marker path ran
  (it adds index, helper, sort, and drop levels the O(1) rule cannot count)
  and compose O(1) otherwise.
  pins: polars-is-duplicated-1/C-002, deep-filter-chain-crash-1/C-013
- [`tests.rs`](tests.rs) — **DEEP-FILTER-CHAIN-CRASH-1 verifier fold
  (2026-09-29):** the `dataframe` unit tests, moved verbatim from the inline
  module (Arrow export values, types, laziness, errors, schema caching).
  **Re-verify fold (2026-09-30):** plus the frame exactness battery (every O(1)
  builder rule asserts cached levels equal a fresh `plan_depths`; the
  subquery-carrying shapes pin in `subquery.rs`, next to the constructors).
  pins: deep-filter-chain-crash-1/C-013
