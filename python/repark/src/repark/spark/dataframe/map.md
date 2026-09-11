# `repark.spark.dataframe`

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).
CC-2 close: `dynamicFlatten` docstring again names ``repark_core::dynamic_flatten`` and ``Unnest``.

## Purpose

The Spark DataFrame facade. It builds lazy native plans and exposes Spark-compatible actions,
joins, grouping, exports, UDF bridges, and writers. Engine computation stays in Rust; Python
callbacks run only where the API accepts user UDFs and receive Arrow batches.

## Modules

- `core.py` owns `DataFrame`, plan construction, joins, actions, schema/type conversion, cache,
  checkpoint, temp-view registration, declared sorting, `dynamicFlatten`, and public re-exports.
  DFCORE-1 (2026-09-07): the leaf helpers above the class moved out — Arrow cell conversion to
  `rows_export.py`, export-error mapping to `export_errors.py`, mapInArrow schema checks to
  `udf_schema.py`, grouped-UDF assembly to `grouped_udf.py` (6302 → 5954, mirrored in the CAP-1
  test). `core.py` re-imports every moved private name, so the package export surface is
  unchanged; the `PySparkNotImplementedError` import stays because it is part of that surface.
  pins: dfcore-1/C-001, C-002, C-004, C-005, C-006
  DML-A: `mergeInto` `whenNotMatchedBySource` DELETE/UPDATE execute.
  NULLABILITY-2 (2026-09-05): the `schema` property maps the `timestamp`/`timestamp_ntz`
  type keys through `ReparkDataType.fromDDL` — `fromDDL("timestamp")` equals the old
  `TimestampType()` arm, and `timestamp_ntz` now lands on `TimestampNTZType` instead of
  the `StringType` fallback. One import line removed (6303 → 6302, mirrored in the CAP-1
  test). Registry `READ-TSNTZ-DTYPE-1`.
  pins: nullability-2/C-006
  TYPES-1 (2026-09-05): `sample`/`randomSplit` hash arithmetic wraps `__repark_rn` in
  `CAST(__repark_rn AS BIGINT)` (+2 lines, absorbed back in round 4).
  DFCORE-2 (2026-09-07): the four UDF select rewrites moved out — scalar and
  classic to `udf_projection.py`, the window variants to `udf_window_projection.py`
  (5954 → 5263, mirrored in the CAP-1 test). `select` keeps two one-line
  delegations; `core` binds the two modules, so the package and core surfaces
  gain exactly those two names and the class loses exactly the four methods.
  pins: dfcore-2/C-001, C-002, C-003, C-006
  DFCORE-3 (2026-09-07): the six statistics bodies moved to `statistics.py`
  (5263 → 5060, mirrored in the CAP-1 test). The public methods stay as one-line
  wrappers, so the class dir is unchanged; `summary`'s wrapper imports its helper
  locally because its `*statistics` parameter shadows the module binding. `core`
  binds the module, so the package and core surfaces gain exactly that one name.
  pins: dfcore-3/C-001, C-002, C-003, C-004, C-006
  DFCORE-4a (2026-09-07): the three sampling bodies plus argument normalization
  and seed coercion moved to `sampling.py` (5060 → 4819, mirrored in the CAP-1
  test). The public methods stay as one-line wrappers; `_prepare_sample_args`
  leaves the class and `_coerce_sample_seed` is re-imported by identity, so the
  package and core surfaces gain exactly the one module name. The move took
  `core`'s last module-scope `IllegalArgumentException` use with it, so the
  now-redundant function-local re-import in staying code is gone (same object).
  pins: dfcore-4a/C-001, C-002, C-003, C-004
  DFCORE-4b (2026-09-07): the ten display bodies moved to `display.py`
  (4819 → 4539, mirrored in the CAP-1 test). `show`, `__repr__`,
  `_repr_html_`, and `_preview_tail_rows` stay as one-line wrappers; the six
  private helpers leave the class, so the package and core surfaces gain
  exactly the one module name. `printSchema` and `__str__` stay: they share no
  helper with the moved code.   The new binding shadows a join-side loop
  variable, so that local is renamed `display_name` (ruff F402,
  behavior-neutral, +2 lines for the wrapped f-string).
  pins: dfcore-4b/C-001, C-002, C-003, C-004
  DF-EXPLAIN-1 (2026-09-08): `explain` renders Spark section headers over verbatim DataFusion
  plan text through the new `_explain_text` (same `(extended, mode)` shape, returns `str`,
  prints and returns `None`); rows come through `toLocalIterator`, which the spy pin holds out
  of `DataFrame.collect`. The rendering support lives in `explain.py` per the D-5 ruling and
  the exact baseline ratchets 4539 → 4536 in the same commit. The CAP-1 freeze pins absorb the
  split: `EXPECTED_DATAFRAME_DIR` gains `_explain_text`, the package gains the `explain`
  submodule, and the frozen core/package surfaces gain the two private imports.
  pins: df-explain-1/C-003
  DF-EAGER-1 step 2 (2026-09-09): `eager` / `lazy` stay as one-line wrappers with the
  `compute` = `eager` alias; the cache-guard trio (`_CACHE_MAX_BYTES_KEY`,
  `_cache_conf_lookup`, `_resolve_cache_max_bytes`) moves to `eager.py`, re-imported by
  `core` by identity, so the frozen core/package surfaces keep every name and the package
  gains exactly the `eager` submodule. The exact baseline ratchets 4525 → 4487 in the same
  commit. pins: df-eager-1/C-001, C-002, C-003, C-004, C-005, C-006
  REVIEW-FIX-6 (2026-09-10): `_explain_text` refuses the both-set shape first —
  `extended` and `mode` together raise `PySparkValueError` `CANNOT_SET_TOGETHER` (the
  `Row(1, a=2)` message shape) — above the untouched string-`extended` remap. The
  four-line guard is funded under the no-comments ruling by deleting five moved comment
  lines, whose facts live here now: grouping sets reject nested generators because the
  grouping would land on the array placeholder, which Spark rejects; `unpivot`
  snapshots its input into a plan-stable scratch view so the UNION ALL reads see one
  plan even over a deferred Arrow bridge; `create_temp_view` simulates fail-if-exists
  by listing first because the engine only offers createOrReplace, and v1 disclosed
  behavior does not fail on replace. The exact baseline ratchets 4487 → 4486 with the
  CAP-1 mirror in the same commit.
  pins: review-fix-6/C-001, C-002, C-004
  REVIEW-FIX-3 (2026-09-10): `show`'s docstring states the probe-first count D-5
  introduced, line-neutral at the exact baseline.
  pins: review-fix-3/C-004
- `actions_export.py` owns `DataFrameNaFunctions.fill` and `drop`; `DataFrame.replace` stays in
  `core.py`.
- `rows_export.py` owns Arrow-to-`Row` materialization for `collect` / `take` / `head` /
  `toLocalIterator`. Two converters live here: `rows_from_arrow_table_python` is the unchanged
  pure-Python path and stays the correctness oracle, and `rows_from_arrow_table` adds the
  binding fast path in front of it. The fast path is declined — whole batch, straight to the
  Python converter — whenever a column may hold a calendar interval or the object cannot export
  `__arrow_c_array__`; map and tz-aware-timestamp columns are converted in Python and handed to
  the binding as supplied columns, so `_arrow_cell_to_spark_python` keeps its refusal table.
  Two costs the old loop paid per row are gone: the names tuple is built once per batch instead
  of once per `Row`, and the cyclic collector is suspended across the batch, restored in a
  `finally`, because a million freshly tracked `Row` objects otherwise make every generation-2
  pass rescan the whole result. pins: perf-facade-1/C-001, C-002, C-003
  DFCORE-1 (2026-09-07): also owns `_arrow_map_pairs`, `_arrow_cell_to_spark_python`, and
  `_refuse_calendar_interval_python_value`, moved here from `core.py` with no behaviour change;
  its `core` imports are gone. An empty map arrives as `[]` from `to_pylist` and must become
  `{}` — never an empty array. pins: dfcore-1/C-004
- `export_errors.py` owns mid-stream Arrow export failure mapping (DFCORE-1, moved from
  `core.py`). Sort failures surface through DataFusion or PyArrow. The marker list names
  memory / ExternalSorter / FairSpillPool texts, and matching is case-insensitive. The
  sort-preserving merge shares the ExternalSorter pool, so its markers ride along. PyArrow
  sometimes wraps capsule failures in `dynamically evaluated source` noise instead of the
  engine message. The extractor prefers the longest non-noise candidate and strips the
  leading `External error: ` shell DataFusion adds on the Arrow boundary. Error classes,
  chaining, and the memory advice text are unchanged. pins: dfcore-1/C-005
- `udf_schema.py` owns mapInArrow schema coercion and batch validation (DFCORE-1, moved from
  `core.py`). Arrow widths match the session `createDataFrame` path, so `SMALLINT` / `TINYINT`
  / `FLOAT` stay narrow. pins: dfcore-1/C-006
- `grouped_udf.py` owns contiguous-group assembly for the applyInPandas bridge (DFCORE-1,
  moved from `core.py`). The key-missing sentinel marks "no group seen yet" in the single-pass
  boundary scan; it is never a real key. An empty returned frame with no columns is an empty
  group result, not a mismatch. A group that continues past a batch edge is stitched onto the
  pending segments, never closed. pins: dfcore-1/C-006
- `udf_projection.py` owns the scalar pandas and classic UDF select rewrites (DFCORE-2,
  moved from `core.py`). Windowed GROUPED_AGG markers never enter the scalar bridge; they
  route to `udf_window_projection.py`. Partition-transform inputs are refused (they project
  all-null dummies the UDF would silently read); generator and aggregate inputs are refused
  (unnest or aggregate first). Stable bare refs rebind to the frame; compounds keep their
  plan expression. The intermediate projection is plan-only: no UDF runs, no rows move.
  Pass-through types come from the intermediate analyzed Arrow schema (metadata only, never
  a `limit(0)` action); physical types stay as-is so zoned timestamps survive.
  `return_type_sql` is revalidated at the bridge (markers can be mutated after
  construction). Both map-bridge halves are patched with the built identity: `mapInArrow`
  coercion drops timestamp timezones and collapses `timestamp_ntz` / `varchar(n)` /
  `char(n)`. Classic UDFs run once per row: slower than `pandas_udf` by design.
  pins: dfcore-2/C-004
- `udf_window_projection.py` owns the windowed GROUPED_AGG select rewrites (DFCORE-2,
  moved from `core.py`). One select's markers share partition keys, order keys, and frame
  bounds; order is all-or-none. The unbounded path strips the window, aggregates per
  partition, and joins back with `IS NOT DISTINCT FROM` so NULL keys match. Both sides
  materialize first (bridge plans qualify keys and trip ambiguity). The final projection
  follows caller order with last-wins duplicates, so `withColumn` overwrite keeps the
  window result. The ordered path carries partition, order, and UDF inputs plus every
  source column on the group frame, overwrites same-name sources, and projects caller
  order last-wins. pins: dfcore-2/C-005
- `statistics.py` owns the seven statistics bodies behind the public wrappers (DFCORE-3,
  moved from `core.py` and `DataFrameStatFunctions.freqItems`). `summary` builds one row
  per statistic with SQL aggregations joined by UNION ALL; bare `summary()` refuses
  because Spark percentile rows are an engine gap. Multi-name frames aggregate on unique
  engine fields — a display name can be ambiguous or absent from the view schema. Engine
  aliases stay unique; the facade overlays Spark-legal display names afterwards.
  `approxQuantile` validates `relativeError` first (non-numeric is a type error, NaN or
  negative is a value error — NaN is not `< 0` in IEEE so it needs an explicit check)
  and treats out-of-range probabilities as value errors, not type errors. DFCORE-5
  (2026-09-07) batched the per-probability loop to one collect per frame: one
  aggregation projects the list form of `percentile_approx` per column under
  positional aliases and unpacks per column; empty probs/cols short-circuit with no
  collect, a
  NULL cell answers NaN per probability, and the engine reports the first failing
  column so mixed good/bad errors keep their order.
  pins: dfcore-3/C-004, C-005
  pins: dfcore-5/C-001, C-002
  `crosstab` casts both strata to string for Spark's
  string-key pivot form, feeds `pivot` simple-name aggregate inputs, and fills absent
  pairs with 0. pins: dfcore-3/C-004, C-005
- `sampling.py` owns the five sampling bodies behind the public wrappers (DFCORE-4a,
  moved from `core.py`). `sample` resolves its three overloads in
  `_prepare_sample_args`: a bool first positional takes the bool/fraction/seed form,
  a float first positional is the fraction (PySpark quirk — the `seed=` keyword is
  ignored on that form), else the keyword form; anything else refuses with
  `NOT_BOOL_OR_FLOAT_OR_INT`. An omitted seed bakes in 42 so unseeded samples stay
  action-stable; a bool or non-numeric seed refuses in `_coerce_sample_seed`.
  Out-of-range fractions refuse with `IllegalArgumentException` (Spark's class).
  Seeded draws use one deterministic LCG over ordered `row_number()`: the seed mixes
  into the multiplier term because a pure offset left adjacent seeds identical, and
  the ORDER BY uses unique engine field names on multi-name frames. Fraction 1.0 and
  0.0 short-circuit to full and empty scans. `randomSplit` normalizes weights, scores
  every row once into one shared uniform column so the parts partition the frame
  (the last bucket stays open-ended), and answers `random()` when unseeded
  (non-deterministic like Spark). `sampleBy` validates the fractions map (bool keys
  refuse; NaN or out-of-range fractions refuse because engine `rand() < nan` is
  true), drops absent strata, refuses bool seeds (Spark's seed is Long), and filters
  each stratum against one shared `rand(seed)` column so the sequence advances once
  per row. All three carry display names, engine names, and the origin map to each
  child, which keeps the `_repr_html_` hook. pins: dfcore-4a/C-004
- `display.py` owns the ten display bodies behind the public wrappers (DFCORE-4b,
  moved from `core.py`). DISPLAY-POLARS-1 departure (2026-09-09): `_resolve_display_style`'s
  one-line docstring said "default spark"; the default has been `polars` since step 1, so the
  token is corrected here — the resolved default itself comes from
  `session/session_configuration.py`'s `default_display_style()`, not from this module.
  pins: display-polars-1/C-001 Every display door still opens with `_ensure_alive`,
  which validates the window, random, and stratified-sampling markers; the
  narration above those calls was the audit-named removable and is gone.
  The styled-vertical warning carries `stacklevel=3` since the move put a wrapper frame
  between the caller and the body (critic r1 F1, 2026-09-07); the pin asserts the caller's file.
  `show` peeks `mapInArrow` bridges with a bounded materialize (no full IPC
  table, no multiset count on the peek path). DFCORE-6 (2026-09-07) extends
  the same peek to eager `__repr__` / `_repr_html_`, which used to run the
  whole bridge twice (once for the rows, once for the footer count). The
  Spark vertical door fetches one row past the limit and reads the footer
  from it instead of counting. The INFO log keeps a row-count
  breadcrumb; the rendered table stays DEBUG-only because row data is PII.
  REVIEW-FIX-3/9 (2026-09-10): in `_render_styled_show` the polars keep-set is
  `min(n, max_rows)` split `(keep + 1) // 2` head / `keep - head` tail — the
  `edge = max_rows // 2` cap dropped a row at every odd `max_rows` and emptied the
  body at `max_rows = 1`; the ellipsis row shows when a tail exists or the budget
  itself bound (`n >= max_rows`), which is how `max_rows = 1` earns `1, …` without
  reviving the bare-ellipsis `show(1)` regression (C8-Q-001).
  pins: review-fix-3/C-001, review-fix-3/C-002, review-fix-9/C-001
  `truncate` validation refuses bool `n` (an int subclass would silently
  shrink the window), accepts digit strings as width caps, and labels other
  shapes NOT_BOOL per the live oracle. Eager `__repr__` / `_repr_html_` read
  the three `eagerEval` conf keys (runtime then builder), pack Spark
  showString form, fetching one row past the cap and reading the footer from
  it — no `count()` on the plain preview paths since DFCORE-6 (2026-09-07).
  The footer text, plural, and cap-edge shapes are unchanged. DISPLAY-POLARS-1
  step 3 (2026-09-09): `__repr__` and `_repr_html_` resolve the display style
  first. Under `polars` / `duckdb` the repr is the `_render_styled_show` text
  for show()'s own defaults (`n=20`, `truncate=True` → cap 20), returned
  instead of printed regardless of the eagerEval keys, and `_repr_html_`
  returns `None` so Jupyter shows the text repr; the peek path is not
  consulted for the styled modes. Under `spark` both doors stay byte-identical
  to before, peek branches included, and the spark HTML door still escapes
  header and cell text (truncate first, then escape — Spark `Dataset.html`
  ordering) so hostile column names cannot inject markup. DISPLAY-BRIDGE-1 (2026-09-09):
  `_show` resolves the display style before the bridge peek, so an uncached
  `mapInArrow` frame's `show()` follows the style. Under `spark` the peek path is
  byte-identical to before. Under `polars` / `duckdb` the branch peeks the bridge once
  (`_consume_map_in_arrow_batches(max_output_rows=n)`) and hands the peeked table to
  `_render_styled_show` via its `peeked` argument: a short peek (fewer rows returned than
  asked) fixes the shape exactly with no `count()`, a full peek pays one `count()` the way
  the styled repr already does, the head window and (when the peek holds the whole frame)
  the tail window slice off the peeked table, and `show()` and `repr` render a bridged
  frame identically — DISPLAY-POLARS-1-S3-Q-001 closed. The styled-vertical warning moved
  above the style dispatch (one warn site, same message and `stacklevel=3`), so it fires on
  the styled bridge path too.
  pins: display-bridge-1/C-001, C-002, C-003
  Styled previews
  collect head and tail windows only. R-11 (2026-09-09): the polars door
  probes `2 * edge + 1` = 11 rows first. A shorter probe renders the frame
  whole with no `count()` and no tail fetch. A full probe pays one count and
  one tail fetch, and reuses the probe as its head window. Polars renders a
  frame of ten or fewer rows whole, with edges capped at five and ellipsis
  only before a non-empty tail. The duckdb door counts first, keeps at least
  one head row, and keeps its fetch pattern until step 4; the tail preview
  engine-skips and never lets a negative skip reach the native `usize`.
  Type labels come from the head Arrow schema. The
  styled renderer calls the tail preview through the frame (not module-local)
  so the class-level collect spy keeps firing.   The module carries its own
  logger; record names move `core` → `display` while message text and levels
  stay identical. pins: dfcore-4b/C-004
  pins: dfcore-6/C-001, C-002, C-003, C-004
  pins: display-polars-1/C-004
  DISPLAY-POLARS-1 step 4 (2026-09-09): `_display_session_ints` reads
  `(max_rows, max_cols, str_len)` from the token (builder map, then defaults)
  at render time. The polars probe is `limit(max_rows + 1)` with edges
  `max_rows // 2`; a styled `show(truncate=True)` caps cells at the session
  `str_len` (spark keeps its own 20), and the styled repr uses the same cap so
  it stays byte-identical to `show()` defaults. The duckdb door keeps its
  count-first fetch and its `n`-based keep-set; the protected
  no-full-collect pin pins that shape, so unifying the doors needs its own
  card. pins: display-polars-1/C-005
  DISPLAY-LAZY-1 step 1 (2026-09-10, R-22): `_repr` renders the schema header for
  a lazy frame (no stored shape, no materialised cache view) without running the
  plan — header labels come from `_analyzed_arrow_schema` through the same
  `_arrow_pa_type_label` the data table uses, so the box is byte-identical to the
  data header; materialised frames keep the data render, and eagerEval renders
  rows through the `show(maxNumRows)` path. RF-5 rides here: the duckdb door now
  probes `limit(max_rows + 1)` first like polars (the awaited unification card),
  slicing short probes with no `count()`. pins: display-lazy-1/C-001, C-002,
  C-003, C-004, C-005, C-006
  DISPLAY-LAZY-1 step 2 (2026-09-10): the checkpoint arm of
  `_materialize_cache_if_needed` records `_eager_shape` (one count over the pinned
  view, skipped when a shape is stored), so a plain `localCheckpoint()` classifies as
  materialised with zero plan re-runs at `repr`. The `_cache_view`-set early return
  carries a `not self._checkpoint_lazy` term because returning on the view alone left
  a pending checkpoint sticky with lineage untruncated — checkpoint-after-cache must
  still run. pins: display-lazy-1/C-007
- `eager.py` owns the eager materialization bodies behind the public wrappers (DF-EAGER-1
  step 2, moved from `core.py`): the `repark.cache.max_bytes` guard pair plus the key, and
  the frame-first `_eager_materialize` / `_to_lazy` / `_count_rows`. `eager()` materializes
  the plan through the existing cache-view call on an `_identity_child` sibling (the source
  frame is untouched), then fills `_eager_shape` once with a count over the built MemTable
  and the column count from the schema — no Arrow copy crosses to Python (D-7 ruling
  2026-09-09: the count runs over the view, not the source plan); the guard refusal is
  wrapped to name `.eager()`. `lazy()` answers `self` on lazy frames and a shape-less view
  scan on eager ones (no re-execution, no drop). `count()` answers a known shape with no
  query. `unpersist()` clears the shape with the view, so a shape never outlives its
  materialization. pins: df-eager-1/C-001, C-002, C-003, C-004
  REVIEW-FIX-4 (2026-09-10, closes Q-12, Q-13, Q-50): `lazy()` on an eager
  frame is `_spawn_preserving_identity(frame._inner)` with no `_cache_view`
  interpolation — a set `_eager_shape` with no `_cache_view` (the checkpoint
  path leaves exactly that) is not a cache-owned frame, so a temp view named
  `none` stays unreachable. `_count_rows` and `_styled_total_rows` run the
  pending-checkpoint materialize first and then reuse the shape, so
  `localCheckpoint(eager=False)` discharges on the next action with no count
  query. pins: review-fix-4/C-001, C-002, C-003
- `explain.py` owns the explain rendering support (DF-EXPLAIN-1, D-5 ruling 2026-09-08): the
  section headers `_LOGICAL_PLAN_HEADER` / `_PHYSICAL_PLAN_HEADER`, the `_EXPLAIN_CODEGEN_NOTE`
  line, the `_EXPLAIN_SECTION_PLAN` mode map (mode → SQL prefix + section keys), and the
  `_render_explain_sections` helper. Plan text stays verbatim — the measured `logical_plan`
  text does not end in a newline while the physical, tree, and metrics texts do, so the blank
  line between sections is added conditionally. `formatted` takes every returned row under the
  physical header (`EXPLAIN FORMAT TREE` measured one `physical_plan` row), and `cost` /
  any mode containing `analyze` runs `EXPLAIN ANALYZE`, whose measured rows carry
  `plan_type='Plan with Metrics'`. Unknown modes raise `PySparkValueError` naming the five
  modes. pins: df-explain-1/C-001, C-002, C-004, C-005
- `joins_columns.py` owns `GroupedData`, grouping sets, pivot, and pandas UDF grouping bridges.
  DFCORE-1 (2026-09-07): imports the moved schema/group helpers directly from `udf_schema.py`
  and `grouped_udf.py`, not through `core`. The grouped-UDF names arrive via a module import
  with qualified call sites: the canonical two-name from-import costs two lines the exact
  ceiling cannot spare, and sibling ceilings never rise (1239 → 1238, mirrored in the CAP-1
  test). pins: dfcore-1/C-006, C-007
- `plan_collapse.py` owns plan simplification, window structural keys, show formatting, Arrow
  display/type conversion, SQL literal quoting, identifier rewrites, and writer safety helpers.
  DISPLAY-POLARS-1 step 4 (2026-09-09, follow-up): the module keeps the show
  control flow (`_format_polars_show`, `_display_type_labels_from_arrow`) and
  re-exports the spelling helpers; the spellings themselves live in
  `polars_cells.py`. pins: display-polars-1/C-005
- `polars_cells.py` owns every polars/duckdb cell and dtype spelling used by
  the show doors: `null` / lowercase bools / mixed-mode floats
  (shortest-expansion rules measured probe by probe against polars 1.43.2 —
  fixed, six-decimal, shortest-sci, and four-decimal-sci bands; the probe table
  lives in the unit ledger), nested structs `{a,"b"}` and lists `["a", 1]`
  with double-quoted strings, `str_len` cuts at `str_len` characters plus `…`
  (spark/duckdb keep `...`), `decimal[p,s]`, unit-aware `datetime[ms|μs|ns]`
  (with `, tz` when zoned), `time`, `struct[n]`, `list[inner]`, and the
  `max_cols` column gap. The nested formatter recurses with a depth cap of 8
  (nested Arrow types are genuinely recursive; a flat walk would hide the
  shape; past the cap values fall back to plain text). No plan logic here, so
  the module never imports `plan_collapse` (import direction stays one-way).
  REVIEW-FIX-9 (2026-09-10): lists elide at four items (`[0, 1, … 3]`, threshold
  `> 3`), and `_polars_float_text` is re-derived from polars' `fmt_float` itself —
  integral floats below `999999.0` stay fixed, integral at-or-above go
  shortest-sci while the Rust `{:}`-spelling length stays ≤ 9, and non-integral
  expansions past nine characters go four-decimal-sci when `|v|` leaves
  `[1e-6, 999999.0]`, six-decimal-trimmed otherwise.
  pins: display-polars-1/C-005, review-fix-9/C-002, review-fix-9/C-003
- `udf_bridge.py` owns action-time pandas, classic, and Arrow UDF callbacks without importing
  `DataFrame` at module scope. DFCORE-2 (2026-09-07) keeps callback execution here; only the
  projection rewrites moved out. pins: dfcore-2/C-005
- `writer_readwriter.py` owns `DataFrameWriter`, `DataFrameWriterV2`, statistics, and write
  helpers. **DML-B:** `overwritePartitions()` emits dynamic `INSERT OVERWRITE … PARTITION`
  (ceiling 1117→1113). pins: dml-b-insert-overwrite/C-003, C-004
  DFCORE-3 (2026-09-07): `DataFrameStatFunctions.freqItems` delegates its refusal to
  `statistics._freq_items` (1113 → 1111, mirrored in the CAP-1 test); the class keeps
  the stat accessor shape. pins: dfcore-3/C-005, C-006
- `__init__.py` preserves the package import surface, including private compatibility names.

## Durable contracts

- Transformations remain lazy. Actions and Arrow exports execute the plan.
- `to_arrow_batches` holds one Arrow batch and emits one typed empty batch for an empty result.
  `collect` materializes rows and converts maps to dictionaries; calendar intervals refuse.
- `columns` reads the plan's logical schema, not the analyzed one. A dependent `withColumn`
  chain used to pay one analyzer pass per existing column per call — 5,750 `column_names` calls
  at depth 100 — because `_bind_schema_column` re-resolved each name against `self.columns`.
  `_iter_bound_columns` now reads `columns` once and passes the canonical name through, falling
  back to the resolving path when the frame carries duplicate names so the `[AMBIGUOUS_REFERENCE]`
  contract is unchanged. pins: perf-facade-1/C-004, C-005
- Arrow export maps execution failures to `PySparkException` while preserving the engine message.
  Planning and analysis errors keep their classified facade exceptions.
- DataFrame origins preserve side identity through joins. Semi and anti joins emit left columns
  only; right-origin columns remain unavailable until an emitting join restores them.
- Joins, grouping, windows, generators, and dynamic flattening refuse unsupported combinations
  before unsafe SQL text is built. `dynamicFlatten.max_depth` bounds rewrite passes, not rows.
- Scratch and cache views use home-qualified names. Cache is object-identity based and lazy until
  an action; checkpoint and write publication follow their native lifecycle.
- `declare_sorted` accepts only session-created source views, verifies order, and may tighten
  verified-null-free keys. Writers refuse tightened frames when required non-null fields persist.
- SQL identifiers and literals are quoted before free-SQL construction. Display names may retain
  Spark-legal duplicates; engine names remain unique and private.
- Window structural keys include null placement. `ascending=` follows Spark's truthy/falsy
  re-marking behavior; RePark rejects a short list instead of truncating it.
- Optional pandas, Polars, and DuckDB surfaces fail clearly when their dependencies are absent.
  Streaming UDF bridges preserve plan stability and close Arrow resources on failure.
- `printSchema` stdout is Spark's tree plus the blank line (`treeString`'s newline and
  `print`'s). pins: df-printschema-1-trailing-newline/C-001, C-004

## Navigation

| Need | Home |
|---|---|
| DataFrame methods and plan glue | [`core.py`](core.py) |
| Grouping, pivot, and `applyInPandas` | [`joins_columns.py`](joins_columns.py) |
| Missing-data helpers | [`actions_export.py`](actions_export.py) |
| Export-error mapping | [`export_errors.py`](export_errors.py) |
| `mapInArrow` schema checks | [`udf_schema.py`](udf_schema.py) |
| Grouped-UDF assembly | [`grouped_udf.py`](grouped_udf.py) |
| UDF callbacks | [`udf_bridge.py`](udf_bridge.py) |
| Scalar and classic UDF projection | [`udf_projection.py`](udf_projection.py) |
| Windowed UDF projection | [`udf_window_projection.py`](udf_window_projection.py) |
| Statistics bodies | [`statistics.py`](statistics.py) |
| Sampling bodies | [`sampling.py`](sampling.py) |
| Display bodies | [`display.py`](display.py) |
| Eager materialization bodies | [`eager.py`](eager.py) |
| Plan rewrites and display | [`plan_collapse.py`](plan_collapse.py) |
| Writes and statistics | [`writer_readwriter.py`](writer_readwriter.py) |
| Parent navigation | [`../map.md`](../map.md) |
| Rust engine contracts | [`../../../../../../crates/repark-core/src/map.md`](../../../../../../crates/repark-core/src/map.md) |
| Tests | [`../../../../tests/map.md`](../../../../tests/map.md) |
| Facade design | [`../../../../../../docs/design/python-facade.md`](../../../../../../docs/design/python-facade.md) |

## Debugging

- Import failures: inspect the re-export block in `core.py` and package `__init__.py`.
- Circular imports: region modules may import helpers from `core.py`; `core.py` binds them before
  importing region classes.
- Origin or display regressions: inspect `_origin_map`, engine-name overlays, and `_spawn` paths.
- File-size records: PYC-1 (2026-08-22) moved the UDF callbacks from `core.py` to
  `udf_bridge.py`. Under CAP-1, `core.py` and `plan_collapse.py` carry exact exception rows;
  `udf_bridge.py` stays below the source-size default. TYPES-1 round 4 (2026-09-05): `core.py`
  6305→6303 — one import joined absorbs the round's increase (pins: types-1/C-008).
  DFCORE-1 (2026-09-07): `core.py` 6302→5954, `joins_columns.py` 1239→1238; the three new leaf
  modules stay below the source-size default (pins: dfcore-1/C-007).
  DFCORE-2 (2026-09-07): `core.py` 5954→5263; `udf_projection.py` (349) and
  `udf_window_projection.py` (337) stay below the source-size default
  (pins: dfcore-2/C-006).
  DFCORE-3 (2026-09-07): `core.py` 5263→5060, `writer_readwriter.py` 1113→1111;
  `statistics.py` (261) stays below the source-size default (pins: dfcore-3/C-006).
  DFCORE-4a (2026-09-07): `core.py` 5060→4819; `sampling.py` (288) stays below the
  source-size default (pins: dfcore-4a/C-005).
  DFCORE-4b (2026-09-07): `core.py` 4819→4539; `display.py` (322) stays below the
  source-size default (pins: dfcore-4b/C-005).
  DF-EAGER-1 step 2 (2026-09-09): `core.py` 4525→4487; `eager.py` (92), `display.py`
  (+6 for the eager-shape total), and `polars.py` (+4 for the `eager` mirror) stay below
  the source-size default (pins: df-eager-1/C-001, C-002, C-003, C-004, C-006).
  DFCORE-5 (2026-09-07): `statistics.py` 261→264, no new module, no ceiling row;
  stays below the source-size default (pins: dfcore-5/C-005).
  DFCORE-6 (2026-09-07): `display.py` 322→320, no new module, no ceiling row;
  stays below the source-size default (pins: dfcore-6/C-005).
- Scratch-view failures: inspect `_temp_views.py`. Facade-owned views are home-qualified; engine-
  owned scratch registration has its own lifecycle.
