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
  PERF-UNPIVOT-1 (2026-09-12): `select` dispatches one `StackCall` through
  `functions_stack.select_with_stack_if_present` onto `PyDataFrame.stack` /
  `UnpivotExec`. Baseline 4485 → 4483. pins: perf-unpivot-1/C-003, C-004
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
  DF-COLREGEX-1 (2026-09-11): `colRegex`/`col_regex` delegate to `colregex.py` — a
  backticked pattern returns the `RegexColumn` marker `select` expands, a bare
  `colName` resolves as a literal column name at the call. `select` checks the
  marker before the `*` arm, so expansion keeps the marker's position in the
  projection. Line-neutral at the exact baseline.
  pins: df-colregex-1/C-003
  FACADE-1 (2026-09-12): mapInArrow construction registers an empty Arrow table through
  the capsule helper (IPC only when the native symbol is absent). `to_arrow` /
  `to_arrow_batches` go through `require_pyarrow`. `to_polars` always consumes
  `__arrow_c_stream__` via `pl.DataFrame(self)` and suffixes duplicate display
  names on the polars columns; it does not call `to_arrow`. mapInArrow
  still materializes UDF output batches in Python before the capsule register: a live
  RecordBatchReader over the generator would re-enter `__arrow_c_stream__` while Rust
  holds the GIL and abort. Use `Table.from_batches`, not `RecordBatchReader.from_batches`,
  so tests that patch `pa.RecordBatchReader` keep tracking `from_stream`. Exact baseline
  ratchets 4485 → 4473 → 4470. pins: facade-1/C-001, C-002, C-006
  DF-SURFACE-A-1 step 1 (2026-09-14): the `localCheckpoint` body moves to
  `surface_a.py` (checkpoint's sibling) and the nine surface-a names bind one-line
  each; the `_schema_override` slot lands for `to`/`withMetadata`'s "schema's
  spelling wins" contract. Exact baseline ratchets 4044 → 4041, mirrored in the
  CAP-1 test. pins: df-surface-a-1/C-001, C-002, C-003, C-004, C-005
- `actions_export.py` owns `DataFrameNaFunctions.fill` and `drop`.
- `replace_expr.py` owns the `DataFrame.replace` body (REPLACE-LINEAR-1 step 1, 2026-09-14):
  PySpark 4.1.2-shaped eager validation (argument classes, equal list lengths,
  same-type-group `MIXED_TYPE_REPLACEMENT`, subset resolution through
  `_resolve_getitem_column_name` where a case-variant spelling resolves but never matches —
  the JVM `contains` rule — and a missing name raises `AnalysisException`), then one searched
  `CASE WHEN col = k THEN CAST(v AS coltype) … ELSE col END` per target column built via
  `Column._from_when_pairs` — linear in mapping size where the old nested `when.otherwise`
  chain was exponential. Key literals are built once per call and each
  `lit(value).cast(type_key)` once per distinct column type (S2-21: rebuilding them
  per column × entry was ~20% of wide×deep plan-build); `_replace_case` only
  composes `bound == key_literal` and `_from_when_pairs`. Target columns are chosen by the first key's type family
  classified from the column's physical Arrow type (numeric → numeric columns incl.
  decimal, str → `Utf8`/`LargeUtf8`/`Utf8View` only so a `binary` column never matches,
  bool → boolean); non-matching columns pass through untouched, and non-convertible arm
  values refuse with `IllegalArgumentException` like the JVM `convertToDouble`.
  Duplicate-name equi-join output binds by relation qualifier through
  `_join_qualifiers` (captured by `join` when it auto-aliases overlapping inputs) instead
  of re-resolving display names, so `replace(10, 99)` rewrites both `x` columns while a
  `subset=["x"]` still raises `AMBIGUOUS_REFERENCE`. The module also owns the
  join-side plumbing that feeds it — `_aliased_join_sides` (the generated
  `_repark_jl_*`/`_repark_jr_*` aliases), `_assign_join_qualifiers` (the
  position-aligned qualifier list), and `_inherit_plan_metadata` (the
  display/engine overlay + origin map + qualifier propagation shared by
  `core.py`'s identity spawns) — so `core.py` keeps only the slot and the call
  sites. `DataFrame.replace` is a one-line wrapper.
- `surface_a.py` owns the DF-SURFACE-A-1 method bodies (2026-09-14) behind the
  one-line class bindings: `to` (store-assignment reconciliation over a
  `StructType` — case-insensitive name match with the schema's spelling winning,
  nullable-miss NULL fill, `NULLABLE_COLUMN_OR_FIELD` /
  `INVALID_COLUMN_OR_FIELD_DATA_TYPE` in Spark's exact text, `NOT_STRUCT` for a
  non-schema argument per registry DF-TO-1), `withMetadata` (metadata-replacing
  field stamp over the `alias(..., metadata=)` path, `NOT_DICT` on a non-dict,
  the frame's own unresolved-column raise on a miss), `registerTempTable`
  (FutureWarning + `create_or_replace_temp_view` delegation), `checkpoint` /
  `localCheckpoint` (in-memory materialization, registry DF-CHECKPOINT-1),
  `sparkSession` / `isLocal` / `inputFiles` (deduplicated `file:` URIs parsed
  from the physical plan's `file_groups`; `[]` only for a plan with no file
  scan), `executionInfo` (the `CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF` refusal),
  and `semanticHash` (SHA-256 over the extended EXPLAIN text with scratch view
  names, common-expr ids, and projection alias names normalized; it hashes the
  plan, where `sameSemantics` compares handle identity — registry EX-DF-11).
  `to` and `withMetadata` stamp `_schema_override` so the schema property
  reports the target schema verbatim (spelling, types, nullability, metadata).
  pins: df-surface-a-1/C-001, C-002, C-003, C-004, C-005
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
  moved from `core.py` and `DataFrameStatFunctions.freqItems`). `summary` computes every
  requested statistic for every target column in chunked native aggregate passes over
  the frame's own plan (`aggregate([], exprs)` on quoted engine-field refs — no SQL
  text, no temp view — one plan per ~50 columns, chunk results cross-joined on a
  literal-true condition), then `stack_dataframe`'s labeled mode
  (`apply_labeled_stack`/`StackLabels`, PERF-UNPIVOT-1 step-2 remediation) feeds the
  raw one-row aggregate straight into `UnpivotExec` with the stat names as row-label
  literals and a row-major cell index map — no expression projection at all (the
  2505-expression projection that replaced the bridge cost ~3 s of superlinear
  physical planning at 500 columns, S2-21). The exec emits the label column and
  coerces every cell to Utf8 inside the batch with the engine's own
  `CAST AS STRING` kernel/options, so the grid is byte-identical to the engine
  cast. Order is carried by construction, so
  duplicate stats keep their requested slots; no Python runs at action time — the
  PERF-DESCRIBE-1 `mapInArrow` bridge and its `_summary_unpivot` callback are gone
  because every plan-side unpivot shape they dodged was superlinear while `stack` is
  linear in columns (PERF-UNPIVOT-1 step 1). The column set is Spark's
  numeric+string rule: `mean`/`stddev` run `try_cast(col AS DOUBLE)` on string
  columns (matching Spark's silent cast — `"10","2","a"` answers `6.0`),
  non-numeric non-string columns are skipped by the bare forms and refused with
  `PySparkValueError` when named (DF-DESCRIBE-STR-1). Bare `summary()` still refuses
  because Spark percentile rows are an engine gap. Multi-name frames aggregate on
  unique engine fields — a display name can be ambiguous or absent from the schema.
  Engine aliases stay unique; the facade overlays Spark-legal display names
  afterwards.
  pins: df-describe-str-1/C-001, perf-describe-1/C-002, perf-unpivot-1/C-008
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
  step 2, moved from `core.py`): the `repark.cache.max_bytes` /
  `repark.cache.max_total_bytes` budget resolvers over the shared
  `_resolve_cache_byte_budget(alive_token, key)` parser (one
  `[INVALID_CONF_VALUE.REQUIREMENT]` message family for both keys; `_resolve_cache_budgets`
  returns the `(max_bytes, max_total_bytes)` pair `core.py` forwards to the native
  materialize call), and the frame-first `_eager_materialize` / `_to_lazy` / `_count_rows`.
  `eager()` materializes
  the plan through the existing cache-view call on an `_identity_child` sibling (the source
  frame is untouched), then fills `_eager_shape` once with a count over the built MemTable
  and the column count from the schema — no Arrow copy crosses to Python (D-7 ruling
  2026-09-09: the count runs over the view, not the source plan); the guard refusal is
  wrapped to name `.eager()`. `lazy()` answers `self` on lazy frames and a shape-less view
  scan on eager ones (no re-execution, no drop). `count()` answers a known shape with no
  query. `unpersist()` clears the shape with the view, so a shape never outlives its
  materialization. pins: df-eager-1/C-001, C-002, C-003, C-004
  EAGER-OWN-1 step 1 (2026-09-13): `eager()` on a frame that already scans a live
  cache view returns a wrapper sharing the view, the `CacheViewHandle`, and the
  shape — no collection, no new registration (D-4). A frame whose view was
  explicitly dropped materializes afresh. pins: eager-own-1/C-004
  EAGER-BUDGET-1 step 2 (2026-09-13): `repark.cache.max_total_bytes` is the session-wide
  retained-bytes budget (D-1, Q-E2 REFUSE): the native admission loop checks
  `retained + admitted` after every streamed batch and refuses with
  `[REPARK_CACHE_BUDGET_EXCEEDED]` — budget, retained, admitted, and the
  unpersist/clearCache fix — before the result's peak and before any registration, so a
  refusal leaves no view, no `CacheViewHandle`, no `cache_view_handles` member and no
  `cache_frames` entry (D-6; `bind_registered_view` and `_register_cache_frame` still run
  only after the native call returns). `repark.cache.max_bytes` keeps its per-result
  meaning and byte-identical message (D-4, corrected in the review round: an own
  `get_array_memory_size` running sum, not the distinct-buffer `admitted` total).
  **Review round (2026-09-13, R12b-D-5):** `_cache_conf_lookup` resolves both budget
  keys case-insensitively — tomb, runtime store and builder snapshot scanned newest
  first on the lowercased key, last spelling set wins — sharing
  `_resolve_cache_byte_budget` for both keys. SQL `SET repark.cache.*` still raises
  the DataFusion `config namespace "repark"` error, now pinned.
  pins: eager-budget-1/C-004, C-005, C-007, C-008
- `cache_handle.py` owns the refcounted `CacheViewHandle` for `__repark_cache_*`
  registrations (EAGER-OWN-1 step 1, 2026-09-13). The registering frame is the
  owner; every frame whose plan scans the view carries the handle in its
  immutable `_handles` tuple, propagated O(1) per `_spawn` (shared empty tuple
  when none, unioned only when another parent carries handles). The registration
  drops when the last holder dies — `weakref.finalize(handle, fn, session,
  alive_token, view_name)` with `atexit=False`; the callback skips a stopped
  session and never raises — or when `unpersist` / `clearCache` / checkpoint
  truncation releases it explicitly. `unpersist` on the owner drops the view; on
  an eager-on-eager wrapper it releases only that wrapper's hold. A frame whose
  plan still scans a dropped view keeps answering — the native plan holds the
  resolved provider. `cache()` / `persist()` views follow the same handle (D-3).
  `_warn_storage_level_cosmetic_once` moved here unchanged and is re-imported by
  `core`, keeping the frozen surfaces. pins: eager-own-1/C-002, C-003, C-004,
  C-005, C-006, C-007, C-008, C-010, C-011
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
- `colregex.py` owns the `colRegex` marker (DF-COLREGEX-1, 2026-09-11; §7 EX-DF-1 FIXED).
  `RegexColumn` carries the stripped pattern on a never-resolvable quoted ref, so every
  surface but `select` fails it as an unresolved column while `drop` no-ops through its
  absent name — Spark's `UnresolvedRegex` shape (`withColumn`, `groupBy`, `orderBy`,
  `filter`, and `.alias` on the marker all refuse). `col_regex_column` strips one
  surrounding backtick pair; a bare argument resolves eagerly as a literal column name,
  raising `AnalysisException` when absent (Spark's `UNRESOLVED_COLUMN` class).
  `expand_col_regex` applies Java full-match semantics case-insensitively over
  `frame.columns` and answers the bound columns in frame order, zero matches included —
  on a multi-name frame each duplicate display name contributes its own bound column.
  Remediation (ruling S2-21, review P2-1): the names list is read once and only
  full-matching names bind (`_bind_schema_column(name, name)`); when
  `_display_names`/`_engine_names` is set or names repeat, the positional
  `_iter_bound_columns` path is kept — that branch is the correctness guard that
  keeps duplicate display names positional (per-attribute expansion, no
  `AMBIGUOUS_REFERENCE`) and resolves origins through `_origin_map`. Measured
  500-col/10-match 5.40 → 0.32 ms (ledger C-006).
  pins: df-colregex-1/C-001, C-003, C-005, C-006
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
- `streaming_batch.py` owns the streaming-named DataFrame surface on a batch frame
  (DF-STREAM-BATCH-1 step 1, 2026-09-14), bound on the class from `core.py` at
  exact ceiling: `writeStream` is a property raising `AnalysisException`
  `WRITE_STREAM_NOT_ALLOWED` (SQLSTATE 42601) at attribute access;
  `withWatermark`/`with_watermark` run Spark's own validation (NOT_STR arg checks
  per R-1 — the Connect shape, not classic's `CANNOT_CONVERT_COLUMN_INTO_BOOL` —
  including the empty-string `not s` gate; a small interval-string parser covering
  optional `interval` prefix, sign, decimal amounts, and multi-unit groups —
  `CANNOT_PARSE_INTERVAL` on a miss or a fractional months/days amount — then
  refuse only when the accumulated `CalendarInterval` (months, days, microseconds)
  nets negative under `IntervalUtils.isNegative` with daysPerMonth=31,
  `IllegalArgumentException` echoing the input string) and return
  `self` (R-2: the batch planner eliminates the watermark node);
  `dropDuplicatesWithinWatermark`/`drop_duplicates_within_watermark` validate
  subset shape and column resolution (`_LEGACY_ERROR_TEMP_1201`,
  case-insensitive) before the `_LEGACY_ERROR_TEMP_3102` batch refusal whose
  message is the first line only (R-4: Spark appends a plan dump repark does
  not have — registry DF-STREAM-1). `rdd`, `pandas_api`, and `plot` are declared
  `NOT_IMPLEMENTED` refusals (Spark Connect's `rdd` refusal shape; registry
  DF-DECL-rdd/-pandas_api/-plot). Structured `AnalysisException` errors reuse
  `_integral.py`'s `_spark_error_class` attach helpers; the helpers are bound
  as real class members so `__getattr__` column access can never shadow them.
  pins: df-stream-batch-1/C-001, C-002, C-003, C-004
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

## cache-view ownership (EAGER-OWN-1)

Every `__repark_cache_*` MemTable registration is owned by exactly one refcounted
`CacheViewHandle` ([`cache_handle.py`](cache_handle.py)). Before this unit a bare
`temp_df.eager()` registered a view that nothing owned: the frame registry is a
`WeakSet`, `unpersist` knew only its own frame, and only `catalog.clearCache()`
swept the orphans by prefix, so repeated calls accumulated full result sets.

- **Owner vs sharing holder (R11-D-1).** The frame whose
  `_materialize_cache_if_needed` registered the view keeps the handle in
  `_cache_view_owned_handle`; `bind_registered_view` wires both that owner link
  and the frame's `_handles` entry, and drops the registration if the
  `SELECT *` over the new view fails, so a post-registration failure leaves no
  view and no live handle. `unpersist` on the owner drops the registration and
  marks the handle released; `unpersist` on a D-4 wrapper clears only that
  wrapper's view reference, shape, and hold — a registration other holders use
  is never dropped under it. A frame whose native plan still scans a dropped
  view keeps answering: the plan holds the resolved table provider, and the
  registration was only a name (measured on the base tree).
- **Holder propagation (R11-D-2).** Every frame carries an immutable `_handles`
  tuple — the shared empty tuple when there is nothing to hold, so frames
  without handles pay no per-spawn allocation. `_spawn(inner, *others)` gives
  the child `self._handles` and calls `union_handles` only for an `other` whose
  tuple is non-empty. The audited sites: core join / union / set-op paths and
  `polars.py` already pass `other`; `_identity_child` and
  `_spawn_preserving_identity` carry `self`'s handles by construction;
  `parent_for_stream` copies the parent's tuple so a `mapInArrow` parent keeps
  its view alive; `joins_columns.py`'s mixed-aggregation temp views embed plans
  derived from the same source frame, whose `_handles` already covers them.
- **Finalizer contract (R11-D-3).**
  `weakref.finalize(handle, _drop_cache_view_registration, session,
  alive_token, view_name)` with `atexit=False`; the callback is module-level,
  references neither the handle nor any frame, returns immediately when
  `alive_token["alive"]` is false (a stopped session is never called — pinned
  by the `drop_temp_view` spy test), and never raises: a `drop_temp_view`
  failure inside the GC callback is logged at debug and swallowed there only.
  Explicit `release()` calls `drop_temp_view` directly, so `unpersist` /
  `clearCache` errors still propagate.
- **`clearCache()` order (R11-D-4).** Handles self-register in a per-session
  `WeakSet` under `alive_token["cache_view_handles"]`. `clearCache` releases
  every live handle first, then runs the unchanged registry `unpersist` loop
  and the `_CACHE_VIEW_PREFIX` orphan sweep. Idempotent; `__repark_ckpt_*`
  views are a different family and stay outside the model.
- **D-3.** `cache()` / `persist()` views use the same handle: ownership moved,
  explicit-drop policy unchanged — they now also die with the last holder.
- **D-4.** `eager()` on a frame whose `_cache_view` is backed by a live handle
  returns an `_identity_child` wrapper sharing the view, the handle, and the
  known `_eager_shape` — no collection, no new registration. A frame whose
  view was explicitly dropped is not already-eager and materializes afresh.
- **D-5.** No plan-equivalence caching: two `eager()` calls on the same lazy
  source evaluate twice, so source changes and nondeterminism stay observable;
  each result is an independent snapshot.
- **`eager()` on a cache-backed derived frame.** `derived.eager()` keeps the
  parent's handle alongside its own new one, so the parent snapshot lives until
  the rematerialized child dies — bounded, conservative retention, accepted
  (review L-004).
- **The cosmetic-warning move.** `_warn_storage_level_cosmetic_once` moved from
  `core.py` to `cache_handle.py` verbatim and is re-imported by `core` — with
  `core.py` at its exact ceiling the ownership wiring had to be a net minus,
  and this was the smallest unrelated block that could leave without
  condensing code mid-fix (ledger R11-D-5, orchestrator-accepted).

pins: eager-own-1/C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
C-010, C-011, C-012

## core.py rationale (COMMENT-CORE-1)

In-code comments left `core.py` in this unit. Docstrings and `# noqa` / `# type:` pragmas
stay. The AST equals `origin/main` (pins: comment-core-1/C-001, C-002). Each
`moved-to-map` row in the unit ledger has its sentence here, grouped under the function
that held the comment (pins: comment-core-1/C-003).

- `(module)`: Vertical `show` is Spark-style only; styled displays stay horizontal and
  warn once. WriterV2 `option`/`options` are accepted for signature parity but ignored
  beyond `tableProperty`. Warn once per process; tests reset the flag.
  `_STOPPED_MESSAGE` must match `ReparkSession.stop`. The filter rewriter never binds
  SQL keywords `true` / `false` / `null` as columns — Spark's grammar reads the keyword.
  Semi/anti engine tokens emit the left schema only. Cache MemTable names are
  object-identity and exclude checkpoints, CDF, and mapInArrow. Re-exports keep
  `plan_collapse` first so sibling modules import its helpers.
- `_emit_join_side_columns`: Walk by position so chained-join duplicate displays do not
  hit `AMBIGUOUS_REFERENCE`. Engine ordinals stay unique across chained duplicates.
  Last-write on display duplicates applies to the internal origin map only; bare
  `joined["b"]` stays `AMBIGUOUS`. Nested origin maps propagate.
- `_by_name_casefold_map`: Exact duplicate names must not silently overwrite the prior
  entry.
- `_normalize_subset`: PySpark's error class is per-surface, not derivable from
  `accept_str` (dropDuplicates + fillna → `NOT_LIST_OR_TUPLE`, dropna →
  `NOT_LIST_OR_STR_OR_TUPLE`). Keep `enumerate` for position-aware diagnostics.
- `DataFrame`: Sticky window-merge metadata, display/origin maps, smartCsv diagnostics,
  declared-sort source, and the tighten-nulls flag live on the instance.
  `_semi_anti_right_plan_ids` holds right-side plan ids a semi/anti join did not emit.
  `declareSorted` is the disclosed repark camelCase spelling of `declare_sorted` (no
  PySpark equivalent). `is_empty` and `to_local_iterator` are disclosed snake_case
  aliases, not PySpark names. `repartition` validates arguments; execution is
  single-node. `unionAll` is Spark's historical alias of `union`. `except_` is the
  Python keyword escape and is not shipped. Arrow export uses the facade exception
  taxonomy and positional display names. `toArrowBatches` is a disclosed camelCase
  extension.
- `__init__`: Cache is object-identity and lazy until the first action. Action-ephemeral
  views are replaced on the next action; plan-stable views remain valid for children.
  One plan-stable bridge snapshot serves all plan children. The facade plan token
  resolves join sides. Duplicate displays map to unique engine fields. Declared-sort is
  source-only. Tighten-nulls propagates to derived frames. `_mia_temp_views` holds
  MemTable names for deferred bridge results; they are dropped during finalization.
- `_identity_child`: Identity-preserving operations keep display, engine, and origin
  maps. pins: perf-facade-1/C-004, C-005
- `_materialize_cache_if_needed`: Cache keeps `_map_bridge` so `unpersist` restores
  re-run; checkpoint truncates lineage. Never route VALUES / `createDataFrame` through
  this entry. Commit handle state only after a successful materialize. Converting an
  already-cached pin drops the old `__repark_cache_*` view after the checkpoint view
  registers. Checkpoint keeps the VALUES seam and is not a session cache-registry
  entry. Checkpoint does not advertise as cached (`is_cached` is False).
- `_prepare_for_plan`: An already-pinned child uses the MemTable and does not clear the
  bridge.
- `_action_inner`: An already-pinned action does not re-run the UDF. Fresh bridge
  execution leaves `_map_bridge` in place. After `replace_ephemeral` drops a prior
  action view, rebind `_inner` so later readers cannot use a dangling MemTable. Leave
  `_inner` alone when `_mia_plan_ready` — that view is plan-stable.
- `_execute_map_in_arrow_bridge`: Fall back to the IPC path when the native C-stream
  register is absent (version-skew). Output batches drain in Python first: a live
  `RecordBatchReader` over the generator would re-enter `__arrow_c_stream__` while Rust
  holds the GIL and abort. pins: facade-1/C-001, C-002, C-006
- `_execute_map_in_arrow_bridge_ipc`: An empty iterator writes a schema-only IPC stream
  (zero batches).
- `_register_ipc_bytes_as_inner`: Own the view before `sql()` so finalize drops it even
  if SELECT fails.
- `unpersist`: `blocking` is signature parity; a single-node drop is always synchronous.
- `localCheckpoint`: `storageLevel` is accepted for signature parity and ignored
  (single-node MemTable only). `eager` is live.
- `sameSemantics`: Best-effort identity of the native `PyDataFrame` object, not full
  semantic equality.
- `declare_sorted`: Caching redirects the scan; declare the source before caching. Bind
  with the same case-insensitive overlay as `select`. Re-resolve the table source after
  re-register, or the declaring frame never sees the elision.
- `with_column`: Scalar UDF markers use the `withColumns` → `select` bridge. Aggregates
  are rejected here — a native `with_column` would fail engine-side, and a pure-global
  select would collapse N→1 rows. Generators must go through the select unnest rewrite
  or the array placeholder projects without multiplying rows. The ordinary path routes
  through `with_columns` so alias-chain squash and adjacent same-spec window merge
  apply.
- `with_columns`: Validate keys and values before any `.alias` so a bad map raises
  early. Aggregates are rejected (same N→1 collapse). Adjacent same-spec window merge
  runs only when the immediately-prior sticky layer used the same structural window and
  no new column reads a name that layer defined; `filter` / `drop` / `select` never copy
  sticky meta. Multi-name frames iterate engine/display bindings and preserve origin on
  replacement. The new layer writes sticky meta for a later merge.
- `_try_merge_adjacent_window_layer`: Do not merge past a cache mark (that would orphan
  the intermediate MemTable pin). Replay both maps on the pre-layer frame so DataFusion
  fuses one `WindowAggr`.
- `filter`: A generator predicate would target the array placeholder. Compounds clear
  origin but keep join_sql QCOL tokens — rewrite to local engine fields and use
  `filter_sql`. Pure origin Columns rebind to engine fields before native filter.
- `select`: Multi-name frames cannot re-resolve bare display strings (`AMBIGUOUS_REFERENCE`);
  expand via engine fields plus display identity. DataFusion requires unique engine
  projection names; Spark allows duplicate displays. Origin-qualified duplicates keep
  bare display names; non-origin duplicates use synthetic engine aliases. Keep composed
  `join_sql` so a QCOL select does not fall back to a bare leaf. Classify projections
  from Column metadata, not expression text. A generator and an aggregate cannot share
  one grouping stage. Pure global requires every projection aggregate and/or foldable,
  with no free attributes and no sticky ungroupable (`row_number().over` is neither).
  Bare aggregates use the native aggregate path; composed post-agg ops need SQL because
  DataFusion `aggregate` rejects non-AggregateFunction exprs. Attach the display overlay
  before the early return so `sum,sum` does not leak `__repark_sel_h2_*`. Mixed
  aggregate and free companion without GROUP BY is Spark `[MISSING_GROUP_BY]`. Duplicate
  displays cannot pass the generator SQL rewrite. Compounds that still carry QCOL
  tokens cannot use unrebound native exprs on multi-name frames.
- `_select_global_aggregate_sql`: One plan-stable snapshot for uncached mapInArrow.
  Register the prepared plan — never the empty MIA placeholder and never a second
  action re-run. Do not call `self.group_by()` (that would `_prepare_for_plan` again).
  Case-preserving rebind covers bare AF builders including post-`.alias` pure AFs that
  clear `_agg_name` but keep structural `sql_expr`.
- `_select_with_generator`: The private array field is a uuid so it cannot collide with
  user projection names. Project from `_plan()` not raw `_inner` so an uncached
  mapInArrow parent materializes before unnest — raw `_inner` is the empty schema
  placeholder and would yield zero rows. The second SQL projection refers only to
  quoted identifiers from the intermediate schema. Length is top-level only. Three
  generator kinds: `explode` drops null and empty arrays, and the element type is not
  needed — do not call outer-type resolution (struct arrays are legal);
  `explode_outer` turns null and empty into one null-element row; `explode_keep_null`
  turns a NULL list into one null-element row while an EMPTY list stays empty and
  drops. Take the element type from the intermediate field for the outer CASE (covers
  coalesce/compounds); never fail-open to BIGINT. Keep `make_array(NULL)` untyped so
  the engine infers. Element cast after unnest is a sticky chain (innermost first)
  from chained `.cast()`.
- `_array_element_sql_type`: Bind uses `array_sql` only (no display substring match).
  Prefer exact spelling; otherwise require a unique casefold hit. A bound field with
  an unsupported element type (map / nested-void) still refuses.
- `__getattr__`: Half-built instances (copy/pickle) must not recurse through
  `_ensure_alive` reading `self._inner`. A bare `AttributeError` handles those probes;
  user misses use the classified error. Permanent out-of-scope surfaces use named
  errors. Exact membership is case-sensitive like PySpark; quoted bind keeps
  non-lowercase schema fields re-selectable.
- `_resolve_getitem_column_name`: De-dupe preserving order for case-insensitive
  multi-hit reporting. Same-display join duplicates are handled above the casefold
  multi path.
- `_select_via_qcol_sql`: Token resolution requires a post-join origin map. Prefer
  multi-name engine aliases when the outer select already assigned them. A unique
  display is safe as an engine name; a CAST display needs an alias.
- `_rebind_origin_column`: Only pure leaf refs rebind (no `join_sql`, or a bare QCOL
  token, or a quoted ident). `coalesce` / `CAST` / binary ops keep native + origin.
  Keep join rewrite tokens so further composition is not required. Preserve sort
  markers through origin rebind.
- `_bind_schema_column`: Quote the engine schema field for free-SQL embeds. Join ON
  rewrite uses `origin_plan_id` and `origin_field`, not this fragment.
- `_quote_filter_sql_identifiers`: Do not rewrite function names or SQL boolean and
  null literals. Protect single-quoted SQL string literals, then double-quoted idents
  inside the rest.
- `_rebind_stable_name_column`: Origin pins a specific side/engine field — skip
  bare-name rebind. Sort markers force a new Column and keep sticky bits; prefer the
  bound's schema-quoted `sql_expr` so cube/rollup free-SQL SELECT quotes reserved
  names such as `order`. Keep origin and `join_sql` through sort-marker rebind.
- `__getitem__`: `df["*"]` is the star projection token for `count` and `select`.
  Live PySpark 4.1.2: CI getitem is a NamedExpression with the requested spelling
  (same display identity as `F.col("X")`), not `Alias(canonical AS item)`. Quoted
  schema bind keeps the field re-selectable after a non-lowercase projection.
- `schema`: `"Null"` is the Arrow Debug spelling and reaches every flat void column,
  a plain NULL literal included (W-1). Overlay Spark-legal display names while engine
  fields stay unique.
- `printSchema`: `treeString` ends with a newline and `print` adds Spark's second one.
  pins: df-printschema-1-trailing-newline/C-001, C-004
- `toDF`: Multi-name frames cannot re-bind bare display strings; rename positionally
  via engine/display bindings.
- `selectExpr`: A bare `*` keeps multi-name display identity. Use a plan-stable
  bridge snapshot rather than action registration.
- `alias`: Register one plan-stable bridge snapshot so post-prepare alias agrees with
  `selectExpr` / `select` / `filter`. The NAME stays one-part (the user chose it) but
  the read is home-pinned — a bare one-part reference re-resolves against the live
  default catalog. SQL `SELECT *` surfaces engine field names; re-attach display
  identity so multi-name joins keep duplicate display columns positionally.
- `replace`: Multi-name frames bind by engine/display pairs — or by `_join_qualifiers`
  when the plan carries duplicate display names. Preserve origin for multi-name select
  identity. The CASE lives in `replace_expr.py` (flat searched CASE, not a nested
  `when.otherwise` chain).
- `repartition`: Spark: first position is int count, or a Column/str partition expr
  when the call is `repartition(*cols)`. A list/bool/float always raises
  `NOT_COLUMN_OR_STR`. Reject a sole-argument list instead of treating it as no
  columns.
- `repartitionById`: Type-check simple name refs so non-int partition columns fail
  loud (Spark analysis). Bare attribute only — casts and expressions stay deferred
  to the engine seed.
- `offset`: Fetch a very large tail after skip (practical unbounded offset on one
  node).
- `drop`: Live Spark 4.1.2: `drop(right["k"])` after leftsemi/leftanti is a no-op.
  Name-based drop removes every engine field whose display matches.
- `order_by`: Sort does not change column identity; keep display and engine maps.
- `join`: Normalize Spark aliases to engine tokens. The semi family folds
  `left_semi` / `left_anti` after stripping underscores. A conditionless semi/anti
  join is not a Cartesian product: Spark keeps every left row iff the right side is
  non-empty (semi) / empty (anti), with no m×n fan-out; both `on=None` and `on=[]`
  would otherwise fall through to `crossJoin`, so they refuse here. Cartesian
  product requires `crossJoin` or `spark.sql.crossJoin.enabled`, read the same
  effective value as `RuntimeConfig.get`. Name equi-join SubqueryAlias both sides
  only when names collide or the join is a self-join — unconditional alias leaked
  permanent session views. An empty key list is a cartesian product (same gate as
  `on=None`); a vacuous all-str would otherwise skip the conf check.
- `_join_on_condition_h1`: Register both plans as temp views (plan-stable), analyze
  the SQL join, then drop the views. A semi/anti join emits the left side only, so a
  right-hand name that merely shares a left name is not a duplicate in the output —
  counting it would mangle the left engine field. Always attach identity when any
  display name collides or an origin map is needed.
- `group_by`: Generators lower through select unnest, not as grouping keys.
- `grouping_sets`: The full Spark `groupingSets` API is multi-list; v1 is one set per
  column plus the grand-total empty set `()`.
- `union`: Keep left-side display identity (union-by-position inherits left engine
  field names — Spark keeps left display names). Origin map is left-only; right-parent
  Columns no longer resolve (disclosed).
- `_sql_binary_set_op`: Materialize and register both under `try`/`finally` so a
  right-side mapInArrow failure after left registration cannot leak the left staging
  MemTable. Re-attach left multi-name display maps after the SQL set-op.
- `crossJoin`: Materialize and register both under `try`/`finally`, same as set-ops.
- `drop_duplicates`: Ambiguous display names in a subset expand to every matching
  engine field (Spark keeps one row per distinct key multiset). Empty subset is
  full-row distinct (avoids DataFusion empty ORDER BY). Use `row_number` keep-first
  rather than `groupBy`+`first` so non-key columns survive.
- `with_column_renamed`: Multi-name frames bind by engine/display pairs (bare name
  rebind raises `AMBIGUOUS_REFERENCE` on duplicate displays).
- `with_columns_renamed`: Multi-name frames already carry Spark-legal duplicate
  displays; allow them and rename via engine bindings. Ordinary frames still refuse
  duplicate names. Keep origin so multi-name select identity survives the rename.
- `_column_of`: Stable-name rebind (`F.col` / requested spelling) then origin rebind
  so `orderBy` / `groupBy` / `select` parent Columns hit the correct post-join engine
  field.
- `_cross_join_enabled`: Spark default is true (Cartesian allowed unless conf
  disables).
- `_sort_specs`: Generators lower through select unnest; ordering by the placeholder
  is invalid. `.asc()` / `.desc()` keep the sticky generator marker. PySpark
  `DataFrame._sort_cols`: a falsy `ascending` entry replaces that column's marker
  with `desc()` (descending, nulls last); a truthy entry is a no-op and the column
  keeps whatever it arrived carrying. RePark rejects a short list instead of silently
  truncating it. Tuples are accepted as a sequence.
- `_ascending_remark_flags`: PySpark raises `NOT_BOOL_OR_LIST` here
  (`PySparkTypeError`); a wrong TYPE for the keyword must not arrive as a value
  error.
- `collect`: Convert batches directly so collect does not hold a second full Arrow
  table.
- `take`: Re-run the map bridge but only keep `num` output rows.
- `tail`: Live PySpark routes `tail` through JVM `tailToPython` and accepts a
  negative as empty (unlike `take` / `head` / `limit`, which raise
  `AnalysisException`). Gate stopped sessions even when `num<=0` short-circuits —
  returning `[]` after stop would be a silent wrong lifecycle outcome.
- `isEmpty`: Stop after the first output row.
- `toLocalIterator`: `prefetchPartitions` is signature parity only. Honest streaming
  pulls RecordBatches via the C-stream and converts one batch at a time.
- `_iter_rows_from_record_batch`: RecordBatch shares column/schema APIs with Table —
  skip the `Table.from_batches` wrap.
- `_require_non_negative_limit`: Live PySpark 4.1.2 raises `AnalysisException`
  `[INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE]` with SQLSTATE and a plan dump.
  repark drops SQLSTATE and the plan dump (no repark error carries SQLSTATE; plan
  text is engine-internal).
- `to_arrow_batches`: Capture schema before drain — empty streams yield no batches
  from the reader, but the C-stream still declares a schema (same source `to_arrow`
  uses). Preserve the declared schema when the stream has no rows.

## Navigation

| Need | Home |
|---|---|
| DataFrame methods and plan glue | [`core.py`](core.py) |
| `colRegex` marker and `select` expansion | [`colregex.py`](colregex.py) |
| Grouping, pivot, and `applyInPandas` | [`joins_columns.py`](joins_columns.py) |
| Missing-data helpers | [`actions_export.py`](actions_export.py) |
| `DataFrame.replace` validation + CASE build | [`replace_expr.py`](replace_expr.py) |
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
| Cache-view ownership handle | [`cache_handle.py`](cache_handle.py) |
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
  DF-DESCRIBE-STR-1 (2026-09-11): `statistics.py` 264→325→318, no new module, no
  ceiling row; stays below the source-size default (pins: df-describe-str-1/C-001,
  C-005).
  PERF-DESCRIBE-1 (2026-09-12): `statistics.py` 318→372, no new module, no ceiling
  row; stays below the source-size default (pins: perf-describe-1/C-002, C-005).
  PERF-UNPIVOT-1 step 2 (2026-09-12): `statistics.py` 372→337→334, no new module,
  no ceiling row; stays below the source-size default (pins: perf-unpivot-1/C-008,
  C-011). The chunk loop's ten `RoundRobinBatch(64)` repartitions over the eager
  source measured inside run noise when toggled off and the switch is
  session-global — residue PERF-UNPIVOT-1-R-001 in the unit ledger.
  pins: perf-unpivot-1/C-014, C-016
  FACADE-1 (2026-09-12): `core.py` 4485→4473→4470; `_pyarrow.py` and `_arrow_stream.py`
  stay below the source-size default (pins: facade-1/C-001, C-002, C-006).
  DF-COLREGEX-1 (2026-09-11): `core.py` stays at its exact baseline; the new
  `colregex.py` (52) stays below the source-size default (pins: df-colregex-1/C-003).
  COMMENT-CORE-1 (2026-09-13): `core.py` 4468→4117; comments removed, no code change
  (pins: comment-core-1/C-004, C-005, C-006).
  EAGER-OWN-1 step 1 (2026-09-13): `core.py` 4117→4094 — the ownership wiring is a
  net minus because `_warn_storage_level_cosmetic_once` moved to `cache_handle.py`
  (169 lines, below the source-size default). pins: eager-own-1/C-002
- Scratch-view failures: inspect `_temp_views.py`. Facade-owned views are home-qualified; engine-
  owned scratch registration has its own lifecycle.

EAGER-BUDGET-1 review round (2026-09-13): `eager.py::_cache_conf_lookup` matches cache budget keys
case-insensitively, like the `repark.cache.retained_bytes` intercept: any spelling in the unset tomb disables the key,
and when two spellings coexist the last one set wins (runtime layer over builder). pins: eager-budget-1/C-004
