# map — repark/spark/dataframe/

## Purpose

DataFrame facade package under `repark.spark.dataframe` (Q1 re-home, 2026-08-14).
r26 T1 package-split the former monolith; **r27 T0** made the region split real
(technique A: nested-class extract + owned helpers); **T0b** (SE-1 PR-B) moved the
module-level plan-collapse / show-format / qcol-rewrite helper block out to
`plan_collapse.py`, which is where `core.py`'s remaining headroom came from.

## Contents

- `core.py` — `DataFrame` class + plan/export helpers; re-exports nested classes and
  moved private helpers (Q7 freeze: package + `core` + `_` binds). **G4b:** `DataFrame.join`'s
  `how_aliases` carries the semi family (`semi`/`leftsemi`, `anti`/`leftanti` — `left_semi` /
  `left_anti` fold in via the existing `.replace("_", "")`), routed to the engine tokens listed
  in the module-level `_SEMI_JOIN_HOWS`. Those tokens take two side paths: `_join_on_condition_h1`
  emits the LEFT side only and spells `LEFT SEMI` / `LEFT ANTI` (a semi join contributes no
  right-hand columns, so projecting them would be an unresolvable reference), and a conditionless
  semi/anti join (`on=None` or `on=[]`) refuses loud instead of falling through to the Cartesian
  path — a cross join is a different result set. **G4b-R2 / Y-5:** after a semi/anti join
  the origin map records the right side's plan ids as not-emitted (`_origin_not_emitted`,
  copied by `_spawn` so descendants still raise — Q-002). A later emitting join of
  that same right subtracts those ids (Q-001) so `semi.join(right, …, "inner")` can
  `select(right["k"])` again. `select` / `filter` / `withColumn` of a still-unemitted
  right-parent Column raise Spark 4.1.2's `MISSING_ATTRIBUTES` class instead of
  name-falling back to the left column; `drop` of that Column is a Spark no-op.
  Self-semi is exclusive-set empty (Q-003). **Z-4:** `F.abs` / other `functions.py`
  wrappers thread origin (Y-5 SAF-001); **W-4 / Q-002** extends the thread to the
  aggregate builders. `core.py` `_rebind` is unchanged.
  See `task/y5-origin-map-ledger.md`, `task/z4-residuals-ledger.md`,
  `task/w4-z-residuals-ledger.md`.
  **TZ-4 PR-2:** collect converts tz-aware timestamps to a naive session-zone wall
  (`_arrow_cell_to_spark_python` + `_arrow_type_needs_spark_python_convert`).
  **S-1 R3:** OOM suffix — RAM-relative default, cap 8 GiB (net-zero rewrite).
  **U-DF-1:** `_select_with_generator` mid-project binds a single-ident generator
  through `_bound_generator_array` (`column.py`) so string-form `explode` /
  `explode_outer` keep createDataFrame case (`Legs`); compounds and unresolved
  names keep `generator._inner`. **DF-2:** `dynamicFlatten(empty_as_null=True)`
  uses `explode_outer` on typed **and** void lists (False uses private
  `explode_keep_null`); void NULL-guard is untyped `make_array(NULL)` (no CAST).
  **DF-2 W-1:** the `schema` flat-column type mapper also accepts the Arrow Debug
  spelling `Null` (every flat void column carries it — a plain `SELECT NULL`
  literal included, not just an exploded void column), so `.schema` / `.dtypes`
  report `NullType` / `void` instead of falling open to `StringType`.
  **SE-1 PR-B (2026-08-17):** `DataFrame.declareSorted` / `declare_sorted` — the
  disclosed repark extension (no PySpark twin) that declares a `createDataFrame`
  source frame pre-sorted so DataFusion elides the window `SortExec`. It refuses
  loud on a transformed frame (the `_source_view_name` slot is set only by the
  session's `__repark_cdf_*` materializers and never copied by `_spawn`), refuses
  on a cached/persisted/checkpointed handle (caching redirects `_inner` to a cache
  view in place — declare first, cache afterwards; SQM finding), resolves
  keys through the same `_resolve_getitem_column_name` + `_engine_field_for_display`
  pair `select` uses, hands ENGINE field names to the native
  `declare_temp_view_sorted` (which ALWAYS verifies and refuses loud on
  out-of-order data), and then **re-plans its own `_inner`** — the logical plan
  captured the pre-declaration table source, so without the re-plan the declaring
  frame would be the one frame that never sees the elision.
  **SE-1 PR-D1:** keyword-only `tightenNulls: bool = False` (one name, both
  spellings) unlocks full elision by flipping verified-null-free keys to
  non-nullable; a NULL key refuses; a later default-flag call restores.
  `_tighten_derived` is OR'd across every parent in `_spawn` (R-C; union/join/
  intersect/subtract/crossJoin right-side + `mapInArrow` via `_spawn`). Writer
  CREATE paths refuse on that marker when the output has a non-nullable field
  (R-D). See `task/se1-declared-sorted-ledger.md`.
- `plan_collapse.py` — module-level helper block moved VERBATIM out of `core.py` (T0b,
  move-only): the r23b N2 plan-collapse helpers (alias-chain squash + adjacent
  same-spec window merge), the G2 range-order gate, the `show` / eager-eval / polars /
  duckdb formatters, the Arrow→display and Arrow→SQL type mappers, the r24 DF1
  `dynamicFlatten` struct expander, Spark-simpleString struct-element CAST spelling
  for `explode_outer` (void / `Null` → `_UNTYPED_NULL_ELEMENT` / `make_array(NULL)`),
  and the r20 H1 join-qcol token rewriters.
  **G3b:** nested arrays inside that CAST spelling go through `_sql_array_of`, which emits
  the **angle** form `array<inner>` — never postfix `inner[]`. Measured: postfix migrates the
  `[]` onto the innermost field once `inner` ends in `>`, which is what made GA4's real
  `items[].item_params[]` (array-of-struct inside an array-element struct) refuse with
  `type_coercion` / "Failed to coerce … CASE WHEN". Angle round-trips exactly for scalar
  inners too, so it is the single uniform spelling.
  Imports nothing from `core` at module scope (annotations only, under `TYPE_CHECKING`);
  `core.py` re-exports every name callers use, so `repark.spark.dataframe.core` and
  `repark.spark.dataframe` import paths are unchanged (Q7 freeze).
  **SE-1 R-3:** `_strip_internal_tighten_metadata` lives here so `to_arrow()` /
  `to_arrow_batches` drop the internal `repark.tighten_nulls` tag.
  **DEFECT-2 (2026-08-18):** `_dynamic_flatten_unnest_structs`'s always-quoted
  selectExpr spelling is no longer load-bearing against the DF-54.1
  `push_down_leaf_projections` trip (the Unnest-safe guard in
  `crates/repark-core/src/session/df_guards.rs` owns that now); the spelling
  stays for mixed-case / hostile field-name resolution — its rationale note in
  the helper says so.
  **Round 3:** `_output_field_would_persist_required` (R-D nested Struct/Array/Map)
  lives here so `core.py` stays under its ceiling.
  **CEIL-1 (D1 #173, move-only):** the six remaining `core.py` tail helpers moved here
  VERBATIM — `_is_native_pure_global_aggregate`, `_parse_count_distinct_simple_names`,
  `_global_agg_sql_parts`, `_pandas_udf_window_frame_bounds`, `_reject_partition_transform`,
  `_reject_aggregate_in_with_column`. D1 + DF-2 each fit the 7350 ceiling alone and
  together did not; the extract restored headroom without raising the ceiling. `core.py`
  re-exports all six from its tail bind block, so `joins_columns`'s
  `from …core import _global_agg_sql_parts` (and every other import path) is unchanged
  (Q7 freeze).
- `joins_columns.py` — `GroupedData` + pivot helpers (real body; technique A).
- `writer_readwriter.py` — `DataFrameWriter`, `DataFrameWriterV2`, `DataFrameStatFunctions`
  + write helpers (real body; technique A). **SE-1 PR-D1 SQM F1:** CREATE paths
  (`saveAsTable` create, `writeTo().create()` / `createOrReplace` / `replace`) refuse
  a `_tighten_derived` frame with a non-nullable output field (R-D).
  **F-3 (2026-08-17):** the six undocumented `DataFrameStatFunctions` methods gained
  docstrings — the five delegating ones point at their `DataFrame` twin (which holds the
  real semantics, so there is one truth), and `freqItems` says plainly that it refuses.
  `core.py` was the one file F-3 left alone: its 11 remaining names were deferred because
  the file sat at 8199 of its then 8200-line ceiling. **Closed in F-4 (2026-08-17)** on the
  headroom the T0b extract below freed: all 11 landed (4 in `core.py`, 7 in
  `plan_collapse.py` — the extract moved seven of them with their bodies), so the facade
  census is 1211/1211 by the ledger's own AST rule. Nine of the eleven are nested rendering
  closures and two are `@overload` typing stubs; none is a user-facing API name.
  No ceiling was raised (`core.py` 7253 of 7350, `plan_collapse.py` 1103 of 2500
  at F-4 close; DF-2/D1 live sizes are in the Debug note below).
- `actions_export.py` — `DataFrameNaFunctions` (real body; technique A).
- `__init__.py` — frozen public imports (star-bind of core for private parity).

## I want to…

| Task | Go to |
|---|---|
| Change DataFrame methods / plan glue | `core.py` |
| Change the declared-sorted door (SE-1) | `core.py` (`declare_sorted`) + `../session/_funcs.py` (`_source_view_name`) |
| Change show/eager-eval formatting, Arrow type labels, plan-collapse or qcol rewrite | `plan_collapse.py` |
| Change global-agg routing, the partition-transform gate, or pandas-UDF window frames | `plan_collapse.py` (CEIL-1 moved them out of `core.py`) |
| Change generator mid-project name bind | `../column.py` (`_bound_generator_array`) + `core.py` (`_select_with_generator`) |
| Change `join` how-aliases / semi-family routing | `core.py` (`DataFrame.join` + `_SEMI_JOIN_HOWS`) |
| Change semi/anti origin-map join-type awareness | `core.py` (`_origin_not_emitted` + `_remember_unemitted_right_origins`) |
| Change groupBy / pivot / agg grouping | `joins_columns.py` |
| Change write / save / V2 writer / stat | `writer_readwriter.py` |
| Change na.fill / drop / replace | `actions_export.py` |
| Public import surface | `__init__.py` |

## Pointers

Up: [../map.md](../map.md). Tests: `python/repark/tests/`. MOVE MAP: `task/t0-df-regions-ledger.md`.

## Debug

- Live file sizes (D1 #173, post-CEIL-1 extract + octo-remediation integration):
  `core.py` 7250 of 7350, `plan_collapse.py` 1432 of 2500. (Was 7380 of 7350 — RED —
  when D1 and DF-2 landed together; the CEIL-1 move-only extract below bought the
  room. Ceiling unchanged.)
- Import path breaks → check core re-exports (Q7) and package `__init__` star-bind.
- Circular import → region modules import `DataFrame`/helpers from `core`; `core` imports
  classes only at file end (after helpers defined).
- Census / collect identity regressions → move-only regression; restore from base and re-slice.
**SQM round 7 (R7-1):** every internal scratch view in `core.py` / `joins_columns.py` /
`writer_readwriter.py` is named through `repark.spark._temp_views.scratch_view_name`, so the name
is the home-qualified SQL reference and the mint→scan pair cannot be split by a raw `SET
datafusion.catalog.default_catalog`. Consequence for call sites: the returned string is ALREADY
quoted — `core.py`'s join/select paths and `plan_collapse.py`'s qualifier tokens no longer wrap it
in `quote_ident`. `DataFrame.alias` keeps the user's one-part NAME and reads it via
`home_view_ref`; `_cache_view` now holds the home-qualified spelling (use `local_view_name` to
compare against `list_temp_view_names`).
