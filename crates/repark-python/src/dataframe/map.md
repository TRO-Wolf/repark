# map — repark-python/src/dataframe

SOURCE-URL-REDACT-1-FN fold 2 (2026-10-06): dataframe `PyValueError` messages pass through `exceptions::mask_user_visible`.

## Purpose

`PyDataFrame` is the Python-facing immutable plan plus its shared runtime: lazy
transforms, terminal actions, schema introspection, and Arrow C Stream export.

## Modules

- [`mod.rs`](mod.rs) — the binding: plan access, terminals, builders, schema, and
  stream export.
  **STAMP-2-R5P6-2 (2026-10-07):** `join_type_from_str` widens to `pub(crate)`: the
  native join door in `../frame_lineage.rs` maps the facade's join type through it.
  pins: stamp-2-r5p6-2/C-002
  **STAMP-2-R5P6-2 step 3 (2026-10-07):** `PyDataFrame` reads its name rule once per
  handle (`rule()`, a `OnceLock<NameRule>`) and passes it to `bound_column` and
  `bound_projection`. Why: `dataframe_names::frame_rule` reads the rule through
  `DataFrame::task_ctx()`, DataFusion 54.1's only borrowed door to a frame's configuration,
  and that builds a `TaskContext`: a clone of the session configuration and of the four
  function registries, about 12 µs. It ran once per bound column, and `perf` put 72 % of a
  native `select` in building and dropping it. A frame's session state never changes after
  the handle is made, so the cached value is the value every call would read. `derived`
  builds a child of `with_column`, `filter`, `filter_sql`, `select`, `sort` and `aggregate`
  and hands it the parent's rule when the parent has read it: those six make the child from
  a clone of the parent through the DataFrame API, which keeps the parent's state.
  `inherit_rule` asserts in debug builds that the inherited rule is the child's own. A
  frame from another state (`sql`, `sql_built`, the native join) starts unread. Measured:
  a three-column native `select` 51.3 → 22.3 µs. pins: stamp-2-r5p6-2/C-010
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
  and compose O(1) otherwise. The helpers take the frame depths and clone
  through `grown_clone_frame` / `grown_clone_expr`, so the grown-stack guard
  holds with no new allow-list entry.
  pins: polars-is-duplicated-1/C-002, deep-filter-chain-crash-1/C-013
  **Stack merge of main d0c50405+ (2026-10-04):** the ATTR-ID-1 stack's
  `dataframe.rs` changes land here. `executable()` builds the O-1 twin
  (`frame_names::strip_for_execution`) on a grown segment sized from the
  cached levels (the larger of the drive segment and the clone need), keeps
  it in the `OnceLock`, and hands out a `grown_clone_frame` of it; a twin
  that loses the first-writer race drops on the grown segment. `Drop` takes
  the twin and tears it down inside the same grown region as `df`. The
  terminals (`count`, `show`, `__arrow_c_stream__`) and
  `analyzed_arrow_schema_native` drive the twin through the cached segment,
  and the schema still passes through `frame_names::strip_schema_ids`.
  `sort` maps a missing key through `unresolved_sort_key` inside
  `drive_columns`; `join_on_names` takes the two lineage nodes and returns
  `(frame, Join node)` from inside `grown_sync`.
  pins: attr-id-1/C-050, C-051, C-053, deep-filter-chain-crash-1/C-013
  **Fold SM-2b item 3 (2026-10-06):** `__arrow_c_stream__` takes an optional
  `display_names` vector alongside `requested_schema` (protocol-compatible:
  consumers pass at most one argument); a length-matched vector renames the
  export schema and batches, anything else keeps engine names.
  pins: attr-id-1/C-062
  **MB-4 round 2b (2026-10-08):** `count`, `show` and `__arrow_c_stream__` call
  `streaming_errors::refuse_streaming_action` first, so every facade batch action on a
  streaming frame refuses DM-3 before execution. pins: mb-4/C-023
- [`tests.rs`](tests.rs) — **STAMP-2-R5P6-2 step 3 (2026-10-07):**
  `name_rule_is_read_once_per_handle_and_children_inherit_their_parents` holds the rule
  cache: a handle starts unread, a frame made before and one made after a
  `spark.sql.caseSensitive` change each keep their own rule, their `select` and
  `filter_sql` children inherit it without a read, and a child of an unread parent starts
  unread. pins: stamp-2-r5p6-2/C-010
  **DEEP-FILTER-CHAIN-CRASH-1 verifier fold
  (2026-09-29):** the `dataframe` unit tests, moved verbatim from the inline
  module (Arrow export values, types, laziness, errors, schema caching).
  **Re-verify fold (2026-09-30):** plus the frame exactness battery (every O(1)
  builder rule asserts cached levels equal a fresh `plan_depths`; the
  subquery-carrying shapes pin in `subquery.rs`, next to the constructors).
  pins: deep-filter-chain-crash-1/C-013
  **Stack merge of main d0c50405+ (2026-10-04):** the battery's
  `join_on_names` arm stamps both sides and passes their root nodes through
  `key_join`, since the join door now requires lineage nodes.
