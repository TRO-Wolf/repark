# map — python/repark/src/repark/spark

**FNP-11A D-10 (2026-09-15, orchestrator):** `functions_temporal.py` and the destubbed forwarder in `functions_expr.py` keep `make_timestamp`'s frozen 1.0 signature — `years`, `months`, `days`, `hours`, `mins`, `secs` required, `timezone` optional — so the API freeze register stays untouched; the new names (`try_make_timestamp`, `make_timestamp_ltz` / `_ntz` and their `try_` forms) keep PySpark 4.1.2's all-optional signatures. The Python `(date, time)` keyword form of `make_timestamp` is registry row EX-FN-28; the SQL door answers `make_timestamp(DATE, TIME)`. pins: fnp-11a/C-001, C-002

## Purpose

This package is the PySpark-compatible facade over the Rust engine. It owns public
Spark names, argument validation, SQL lowering, Arrow/Python boundary handling, and
session-local state. Engine computation stays in Rust; user UDF callbacks execute
in Python over Arrow batches.

The package exposes `ReparkSession`, the `SparkSession` and `ReParkSession`
aliases, `DataFrame`, `Column`, `Catalog`, `Window`, `Row`, `Observation`, Spark data
types, scalar/aggregate/UDF functions, and table/storage helpers. The package's
`sql` and `types` aliases preserve common PySpark import paths.
- `catalog.py` — **CATALOG-1 (2026-09-26):** `currentCatalog()` reads the engine
  (`_native.current_catalog_checked`: `CATALOG_NOT_FOUND` raises), and `tableExists` /
  `databaseExists` / `getDatabase` / `listTables` no longer alias `spark_catalog` to the current
  catalog. `_catalog_is_registered` re-raises a refused catalog's refusal instead of
  answering `False`; `databaseExists` / `_namespace_exists` re-raise it likewise, so
  `listTables` propagates the refusal instead of `SCHEMA_NOT_FOUND`.
  pins: catalog-1/C-002, C-004, C-006, C-007, C-012, C-013

## Modules

- `__init__.py` — public exports, version loading, and process-wide ANSI SQL
  entry point. Version metadata is loaded before facade imports.
- `_csv_smart.py` — deterministic CSV preparation and schema inference. It handles
  BOMs, preambles, delimiter/header detection, ragged rows, and typed inference with
  explicit fallback to string.
  **FACADE-4 step 1 (2026-09-14):** `rung_to_spark_type` / `rung_to_engine_cast` /
  `rung_to_sql_cast` read the shared Rust table (`csv_rung_descriptor`,
  `csv_sql_cast_token`); the rung answers are unchanged (D3–D5 stay pinned).
  **Round 2 (PY-P2-003):** `rung_to_engine_cast` answers `csv_engine_token`
  straight from `csv_rung_type` — no `DataType` construction; all three binds
  go through the cached `_type_table._native_function`.
  pins: facade-4/C-013, C-027
- `_type_table.py` — Python-side descriptor bridge for the shared Rust type table:
  the class→row answer table (descriptor head, `simpleString`, `_engine_type`),
  descriptor encode/decode, tree walks, and the container-token fallbacks for
  foreign `DataType` subtypes. Class references resolve lazily so `types.py` keeps
  a one-directional import.
  **FACADE-4 step-1 remediation (2026-09-14, P1-DTYPES):** atomic token answers are
  Python-side constants/parameter derivations byte-identical to the Rust table
  (pinned in `test_facade_4_census_pins.py`); nested trees compose over them in
  Python because a per-column descriptor FFI (~15 µs for nested3's `mid`) cannot
  meet the +5 % `dtypes` bar.
  P2-DICT follow-through: the descriptor decode caches its kind→class and
  class→head maps and the `types` module handle — the first landing rebuilt a
  19-row map per recursive node and re-imported `types` per call (~30 µs on a
  7-node nested decode, regressing `df.schema` +50 %).
  **FACADE-4 step-1 remediation round 2 (2026-09-14):** the row table grows to
  five per-class answers (descriptor head, `simpleString`, `_engine_type`, SQL
  marker, DDL marker) resolved by an MRO scan so pass-through subclasses keep
  the base answer (`_inherited_type_name` reproduces the dynamic
  `type(self).typeName()` fallback); `_leaf_ddl` dispatches nested DDL leaves
  the same way. `_parse_datatype_string` keeps a Python
  residue (`_parse_datatype_string_python` + `_parse_field_list`) for integer
  parameters beyond i64 and non-printable text whose refusal `repr` bytes
  differ from Rust's; `_native_function` caches lazy native lookups.
  **Round 2 (PY-P2-002):** `_descriptor_to_datatype` decodes the tagged-tuple
  wire shape (`("kind", …)`) the bridge now emits — positional indexing, no
  per-node dict lookups.
  **Round 4 (2026-09-14, L-008/L-009):** base answers each surface with a
  different resolution — MRO for `simpleString`-style answers, an
  integer-first `isinstance` order for Arrow and SQL markers, and a
  string-first order for DDL. `_arrow_order`/`_sql_order`/`_ddl_order` cache
  the three orders; `_primary_class` picks the first matching class in an
  order (exact tabled classes and exact containers short-circuit);
  `_atomic_token`'s `order` parameter switches its subclass scan from MRO to
  the surface order; `_datatype_to_descriptor` propagates `None` upward so
  unknown subtrees reach the Python fallbacks in one walk, and struct members
  build through `_field_descriptor`. `_leaf_ddl`/`_ddl_token_python` keep the
  DDL fallback on `data_type.simpleString().upper()` so `simpleString`
  overrides (intervals and every other leaf) survive nested.
  **Round 5 (2026-09-14, L-011):** the `isinstance(StructField)` arm is deleted
  — a field is encoded only from `StructType.fields` through
  `_field_descriptor`, so a `Left+StructField` class built without `.dataType`
  falls to the Python fallbacks and answers its non-field parent like base.
  **TYPES-GEO-DDL-1 (2026-09-15):** `_descriptor_to_datatype` decodes the two
  spatial tags (`geometry`/`geography` + srid, `-1` for Spark's `ANY`) into the
  `types_bases.py` objects the JSON door already builds; no encoder arm is added,
  so every other surface keeps its existing fallback or refusal bytes.
  Round 2: the non-printable divert narrows by spatial keyword in one line —
  tab/NBSP spatial strings reach the Rust table and parse there; all other
  non-printable text keeps the Python residue path byte-for-byte.
  pins: facade-4/C-010, C-012, C-016, C-018, C-020..C-024, C-026, C-029..C-031, C-033; types-geo-ddl-1/C-002
- `_idents.py` — single home for SQL identifier, path-segment, and string-literal
  escaping. Callers must use these helpers for embedded user names and values.
  **FNP-4B (2026-09-15):** identifier quoting is backticks (both doors read them as
  identifiers; the Spark door reads double quotes as strings).
- `_integral.py` — **Round 3 (2026-09-06):** Spark INTEGRAL-type coercion for facade
  integer knobs (`checked_integral`); numpy `__index__` types run, bool/float/str fail
  with `AnalysisException` / `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` carrying Spark's
  sqlExpr/paramIndex/inputSql/inputType/requiredType (live 4.1.2). **IO-TEXT-1 Round 3
  (2026-09-15, U-10):** `attach_error_condition` pins a Spark error class (and
  SQLSTATE) onto a caught native exception in place, message untouched.
  pins: perf-approxpct-1/C-002; io-text-1/U-10
- `_secrets.py` — secret-property classification and redacted runtime configuration
  listing. Explicit `get` calls do not redact values.
- `_temp_views.py` — temporary-view ownership and cleanup helpers.
  **FNP-4B (2026-09-15):** `local_view_name` also strips backtick quoting (handles are
  backticked since the D-2 quoter move). pins: fnp-4b/C-008
  **CASESENS-1 S3 (2026-09-27):** `home_view_ref` probes through
  `_native.resolve_temp_view_home_ref_for_session` (exact under `true`, folded
  under `false`). pins: casesens-1/C-007
- `_pyarrow.py` — **FACADE-1 (2026-09-12):** `require_pyarrow()` imports pyarrow or raises
  `ImportError` naming `repark[pyarrow]`. Package import does not load pyarrow.
  pins: facade-1/C-002
- `_arrow_stream.py` — **FACADE-1 (2026-09-12):** `register_arrow_exporter_as_temp_view`
  prefers `register_arrow_stream_as_temp_view` (the `__arrow_c_stream__` capsule seam) and
  keeps `pa_ipc.new_stream` + `register_ipc_stream_as_temp_view` as the version-skew fallback
  when the native capsule symbol is absent. pins: facade-1/C-001
- `catalog.py` — Spark catalog facade. It lists namespaces, Iceberg tables, temporary
  views, and schema tables; supports current catalog/database state, function
  registration, cache clearing, and table/view existence operations. Engine-private
  temporary names remain hidden from listing APIs.
  **EAGER-OWN-1 step 1 (2026-09-13):** `clearCache` releases the session's live
  `CacheViewHandle`s (registered in a WeakSet under the alive token) before the
  unchanged registry `unpersist` loop and the `__repark_cache_*` prefix sweep —
  idempotent, checkpoint views untouched.
  pins: eager-own-1/C-006, C-011
  **ICE-CATALOG-SESSION-1 S6 (2026-09-20):** `setCurrentCatalog` / `setCurrentDatabase`
  keep their pre-checks, then run engine `USE` / `USE DATABASE` so the engine and the
  facade box agree (C-028).
  pins: ice-catalog-session-1/C-028
  **CFG-2 step 2 (2026-09-13):** `SourceMetadata` (the `name` / `kind` / `key_path` /
  `auto_register` / `properties` namedtuple) lives beside `CatalogMetadata` — the
  `listCatalogs` idiom — for `ReparkSession.sources()` rows. pins: cfg-2/C-013
- `catalog_surface.py` — **CATALOG-SURFACE-1 (2026-09-14):** the thirteen-name second
  half of the `Catalog` surface — `getTable` / `listColumns` / `listFunctions` /
  `getFunction` metadata (DESCRIBE + SHOW PARTITIONS + the `repark.spark.functions`
  export table), the `cacheTable` / `isCached` / `uncacheTable` trio over EAGER-OWN-1
  cache handles keyed by resolved table identity on the session's alive token,
  `createTable` / `createExternalTable` through the existing CREATE TABLE path
  (non-`iceberg` source and `path=` share the EX-IO-6 refusal), and the maintenance
  no-ops (`dropGlobalTempView` False while EX-DF-2 stands, `recoverPartitions` None —
  §5 CAT-RECOVER-1, `refreshTable` via `refresh_catalog_provider`, `refreshByPath`
  None). `ReparkSession.table` delegates to `catalog_surface.session_table`, which
  resolves the name temp-view-first and scans the held cache view when the identity
  is catalog-cached (the free-SQL DROP rewriter never re-qualifies a temp view —
  the same contract the old inline body kept); `Catalog.clearCache` and
  `Catalog.uncacheTable` release entries from the same registry.
  **CATALOG-SURFACE-1 critic round 1 (2026-09-14, L-001..L-007 / R-6/R-7):** each
  held cache entry stores an identity token — the Iceberg `current-snapshot-id`
  from `DESCRIBE TABLE EXTENDED`'s `Table Properties` row for a table, the
  registered view object for a temp view — and `session_table` / `isCached`
  compare it on every call, dropping a stale entry exactly like `refreshTable`
  (~0.53 ms per cached `spark.table` call, measured 1000×). `persist`'s
  materialize path notes the same baseline on identity-mapped frames via
  `cache_handle.bind_registered_view`; `create_or_replace_temp_view` registers
  through `_register_temp_view` so replacements bump the view token. Every
  raised `AnalysisException` carries its Spark errorClass through the
  `_integral` attach helpers (`getCondition`). `Table Properties` parses as
  comma-joined `k=v` with `=`-less fragments folded into the previous value
  (comments with `,` / `]` round-trip), `# Partitioning` transform rows map to
  their source columns (`bucket`/`truncate`/time transforms included), while
  `# Partition Information` identity rows after the `# col_name` sub-header name
  their columns directly, and
  `createTable` DDL spells arrays `ARRAY<INT>` recursively plus `NOT NULL`
  (**FNP-4B round 7 (2026-09-15):** the one-line R-16b-21 grant; nothing else in
  this file changed). pins: catalog-surface-1/C-001…C-006, C-009
  **C1 SHOW CREATE (2026-09-23):** `_table_comment` reads the `Comment` row of the
  `# Detailed Table Information` block (Spark's measured spelling) — the shared
  Spark-visible `Table Properties` list no longer carries the reserved `comment` key.
  **CATALOG-1 fold (2026-09-26):** `_known_table` re-raises a refused catalog's refusal
  from each probe instead of re-mapping it to not-found, so `getTable` surfaces it
  like Spark. pins: catalog-1/C-013
- `column.py` — lazy expression objects, type gates, aliases, field access, generators,
  aggregates, windows, casts, and Spark-compatible operator behavior. Column identity
  metadata preserves join and duplicate-name semantics.
  **COLUMN-PARITY-1 critic round (2026-09-14):** struct field access carries a second,
  join-ON SQL fragment in bracket form (`(child)['field']`, literal via
  `sql_string_literal`, rendered by `PyColumnParts.field_join_sql`) — this DataFusion
  version refuses dot access on parenthesized expressions, so join conditions over
  `getField` (plain or `update_fields`) now parse. Free-SQL `sql_expr` keeps the dot
  form byte-identical (goldens + hostile-ident pin unchanged). A later `alias`/`name`
  without `metadata=` drops earlier metadata (Spark builds a new alias).
  pins: column-parity-1/C-008
  **FACADE-2 step 2 (2026-09-12):** Group-2 operator/method families make one
  `_native.PyColumnParts` call per operation; display/SQL/join text is rendered in
  `crates/repark-python/src/column/display.rs`. The public `Column` class, `__slots__`,
  and `isinstance(c, repark.Column)` stay. pins: facade-2/C-009
  **FACADE-2 step 2b (2026-09-12):** `alias` keeps `sql_expr` as a Python passthrough
  (`self.sql_expr_part()` — reuse, not assembly) instead of round-tripping the string
  through Rust; the native `alias` returns `(PyColumn, spark_display)`. pins: facade-2/C-013
  **DECIMAL-CACHE-1 remediation (2026-09-15):** `__neg__` documents the native
  unary-minus expression (decimal inputs keep their type, Spark `UnaryMinus` parity);
  the expression itself lives in `display.rs`. pins: decimal-cache-1/C-007
  **CAST-MAP-SPELL-1 (2026-09-19):** `_spark_cast_type_name` forwards any name outside its
  simple table to `_native.PyColumnParts.cast_type_token`, which answers Spark's `MAP<…>`
  token for a map-bearing type (`MapType`, `"map<string,bigint>"`) and raises
  `ParseException` otherwise; the file shrank to 1529 and its ceiling ratcheted down.
  pins: cast-map-spell-1/C-004
  **ATTR-ID-1 S2 (2026-09-30):** `Column` gains the `_attr_id` slot (default `None`,
  set through the new `attr_id` parameter at the frame-field bind sites). Line-neutral
  at 1529: the slot, parameter, and assignment lines are funded by documenting the
  bind triple in two docstring lines and merging the origin pair assignment. `alias`,
  `for_select`, and compound constructors do not propagate it yet (S3).
  pins: attr-id-1/C-008
  **ATTR-ID-1 S3a (2026-09-30):** `alias` now mints: the native `PyColumnParts.alias`
  carries a fresh id in the alias's own metadata, and the facade `Column` it returns
  holds `_attr_id` `None` until bound. `for_select` and compound constructors still do
  not propagate. pins: attr-id-1/C-024
  **ATTR-ID-1 S4 follow-up (2026-10-02):** `Column` gains the `_birth_frame` slot
  (default `None`, set through the new `birth_frame` parameter): the frame the
  column was bound against. Every construction that carries `_attr_id` also carries
  `_birth_frame` — fresh binds set the frame, rewraps (`_with_sort_order`,
  `for_select`) propagate it, a rebind sets the new frame. The select/sort bind
  keeps a birth-frame column's written reference verbatim and rebinds any other
  frame's held attribute by position. The sort-marker family moves to
  `column_sort.py` behind `Column` bindings (1536 → 1485, with the CAP-1
  mirror). Pins: `python/repark/tests/test_attr_id_1_s4.py`. pins: attr-id-1/C-041
  **ATTR-ID-1 S4 alias fix (2026-10-02):** `alias` preserves the base column's
  `_attr_id` and `_birth_frame`, so an aliased side ref binds its own side on
  a condition join instead of reaching the engine with a bare duplicate name.
  The string-predicate family moves to `column_string.py` behind `Column`
  bindings (1485 → 1378, with the CAP-1 mirror). Pins:
  `python/repark/tests/test_attr_id_1_s4.py`. pins: attr-id-1/C-042
  **ATTR-ID-1 SJ-2 (2026-10-02):** the fragment-render family
  (`sql_expr_part`/`join_sql_part`/`sql_expr_without_alias`/`spark_display_part`/
  `spark_wrap_display_part`) moves to `column_render.py` behind `Column`
  bindings (1378 → 1331, with the CAP-1 mirror). Pins:
  `python/repark/tests/test_attr_id_1_sj2.py`.
- `column_fields.py` — **COLUMN-PARITY-1 (2026-09-14):** method bodies bound on
  `Column` (kept out of `column.py`, which is at its exact line baseline):
  `between` / `eqNullSafe` (extracted for headroom), `isin`, `isNaN`, `astype`,
  `name`, `outer`, `withField`, `dropFields`.
  **Critic round (2026-09-14, R-4):** `isin` builds one native `IN`-list expression
  (empty list → `lit(False)`), `isNaN` calls the `repark_isnan` engine UDF, and
  `withField` / `dropFields` build the `update_fields` engine expression immediately
  (one `PyColumnParts` call each, chains nest) — the deferred select-boundary
  resolver is deleted, so filter / `when` / `orderBy` / `groupBy` / join / nested
  positions compose. pins: column-parity-1/C-001, C-002, C-003, C-004, C-005, C-008
  **DF-SUBQUERY-1 (2026-09-15):** `outer()` marks the column through the native
  `column_outer` (`Expr::OuterReferenceColumn`), and `column_or_str_error` is the
  shared `NOT_COLUMN_OR_STR` raise `_column_of` uses for `TableArg` and other
  non-column arguments. pins: df-subquery-1/C-003, C-005
  **U11-EDGE-1 round 7 (2026-09-26, V-002):** `is_bare_star(column)` — whether a Column is an
  unaliased `F.col("*")` (SQL text `` `*` ``, projection name `*`; `lit("*")` and
  `col("*").alias(…)` are not), so `DataFrame.select` expands it like the string `"*"`.
  pins: u11-edge-1/C-029
  **ATTR-ID-1 S2 (2026-09-30):** `_bound_attr_id(frame, engine_field)` reads the stamped
  id at the engine field's native position (stamping on read, failing loud on a missing
  id or a desynced engine name). No docstring: the lane's no-comments ruling covers new
  private helpers; the contract lives here.
  pins: attr-id-1/C-008
  **ATTR-ID-1 S2 fix (2026-09-30):** on a statement frame (`frame_is_relation` false)
  `_bound_attr_id` returns `None` — no identity exists there — while a missing id on a
  relation frame still fails loud.
  pins: attr-id-1/C-008, C-014
  **ATTR-ID-1 S2b (2026-09-30):** `_bound_attr_id` re-stamps (and replaces `_inner`) only
  when the frame's root lacks an id, and finds the engine field among the unanalyzed
  `logical_column_names`, the same schema `attribute_ids` reads. S2 called
  `logical_schema_fields` on a fresh handle per bind, which re-ran the analyzer every time:
  22 of 82 s in a cProfile of the replay's `r3` corpus.
  pins: attr-id-1/C-016
  **ATTR-ID-1 S2 exports (2026-09-30):** `_strip_attribute_id_metadata(table)` drops
  the `repark.attr` key from every top-level Arrow field (Tables and RecordBatches;
  zero-copy when absent), called from `DataFrame._apply_export_display_names`.
  pins: attr-id-1/C-010
  **ATTR-ID-1 S3a (2026-09-30):** the `select` family's one resolve rule lives here.
  `_bind_resolved_name(frame, written)` parses the written name (backtick-aware split;
  unparsable text and `*` fall back to `_bind_schema_column`), stamps on read, and
  resolves under the live `session_case_sensitive`: one hit binds the engine field at
  that position with the written spelling; a folded ambiguous name with one exact
  spelling present births an id-less written ref (V-5; S3a bound that position
  inline); several exact spellings raise the old whole-name
  `AMBIGUOUS_REFERENCE` inline; a quoted spelling delegates to `_bind_schema_column`
  (the old path matches the raw written text, quotes intact); a pure folded ambiguous
  name raises Spark's `AMBIGUOUS_REFERENCE` echoing the written-case candidates; a
  miss raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` with the folded candidates.
  Non-relation frames and display/engine overlays delegate unchanged. Unqualified hits
  come from `_unqualified_candidates` (Python `casefold`, exact-first) grouped by held
  id in `_group_candidates` — the native rule folds ASCII-only, which the first S0
  replay caught on 109 unicode cells. One exact hit with no folded rival binds without
  reading the session rule; the rule, the relation check and the id-loudness check run
  only off that path, so the common bind costs what the old one did.
  Qualified names still call native `resolve_display_name` for the relation narrowing.
  **ATTR-ID-1 V-5 (2026-10-02):** a folded-ambiguous name with one exact spelling
  no longer binds that twin's id; it births an id-less written-ref `Column`
  (exact-engine spelling, birth frame kept), so the engine refuses exactly as
  base and Spark do on the birth frame and every pass-through child, while a
  select-output twin frame (renamed engines) keeps binding. `_born_ambiguous`
  spots those columns in `_bind_sort_key` and returns them verbatim: live Spark
  refuses Column sort keys as `AMBIGUOUS_REFERENCE` but string keys as
  `UNRESOLVED_COLUMN`, so strings keep the old sort path. pins: attr-id-1/C-044
  `_bind_stable_id_column` rebinds a parent Column by `_attr_id` to the first held
  position through `attribute_column`, but only across plans (a same-frame bind stays
  the written ref, so the engine shapes the refusal) and only onto a unique engine
  field (a marked unqualified ref over duplicate engine names dies bare in DataFusion
  analysis instead of the shaped hook refusal — 330 first-replay cells). An id miss
  returns the column unchanged. `_rebind_stable_name_column` tries the id bind first,
  then the old stable-name path over the new resolve rule (a refusal falls through to
  the engine). `_column_of` is the same funnel over Columns and strings.
  No docstrings: the lane's no-comments ruling covers new private helpers; the
  contract lives here. Pins: `python/repark/tests/test_attr_id_1_s3a.py`.
  pins: attr-id-1/C-024
  **ATTR-ID-1 S3b (2026-10-01):** the filter/sort family's one resolve rule lives
  here too. `_rebind_free_names` funnels a filter/sort `Column` through native
  `bind_free_names` (stamping on read, under the live session rule; a frame with
  unicode-folded rivals keeps the legacy path, since the native rule folds
  ASCII-only). `_bind_sort_key` binds strings through `_resolve_sort_name` and
  parent Columns by id first (`_bind_stable_id_column`), then by stable name
  over the same rule, then an aggregate display rebind, else the free-name
  funnel: one id binds the engine field, several bind the oldest on a Project
  child (`sort_child_shape`) and refuse `UNRESOLVED_COLUMN` elsewhere, and a
  miss skips to the join grandchild (`grandchild_key_status`) before refusing.
  `_quote_filter_sql_identifiers` keeps the old parsing (quoted-span
  protection, the function-call lookahead) and binds each bare token through
  `_bind_filter_token`: a literal keyword (`_SQL_LITERAL_KEYWORDS`, re-homed
  from `dataframe/core.py`) never binds, one id quotes the engine field, several
  raise the old `AMBIGUOUS_REFERENCE` text listing the actual display spellings,
  and a miss or a non-unique qualified engine name stays for the engine. No
  exact-preference on either door: an exact spelling among folded rivals
  refuses, as live Spark does. No docstrings on the new private helpers; the
  contract lives here. Pins: `python/repark/tests/test_attr_id_1_s3b.py`.
  pins: attr-id-1/C-025
  **ATTR-ID-1 S3b H-1 (2026-10-01):** the §9e rulings. A one-id multi-hit
  token refuses when `join_dup_below_wrappers` sees a join below (the
  `SubqueryAlias` dedup hides the join but preserves ids) and the echo lists
  every candidate; a sort key with no output hit falls through to the engine;
  dotted tokens route on plan qualifiers only, qualifier-first on a struct tie
  (Spark-measured) with a main's-path fallback on a qualifier miss, anything
  else takes main's bare-ident path. The quoter machinery moves to
  `filter_quote.py` (this module keeps the `_quote_filter_sql_identifiers`
  entry): 1209 → 871 lines. Pins: `python/repark/tests/test_attr_id_1_s3b.py`.
  pins: attr-id-1/C-026
  **ATTR-ID-1 S3c (2026-10-01):** `_live_rule_hits` is the shared hit
  computation the `withColumn(s)`/`withColumn(s)Renamed` family binds through
  (exact hits under the exact rule; insensitive adds `_java_case_equal`, which
  reproduces Java `String.equalsIgnoreCase` — same length plus per-character
  upper/lower/equal, with U+0130 read as `I` because Python's full case mapping
  splits it where Java's char mapping does not; literal names, no qualifier
  split). Python `casefold` is wrong here: it matches `ß`/`SS`, which Spark
  misses, and it misses `İ`/`i`, which Spark matches (live-Spark s3c5 probes).
  `_rebind_stable_name_column` now re-raises an unqualified
  `AMBIGUOUS_REFERENCE` instead of falling through to a bare engine error;
  misses and qualified names still fall through (S3e owns qualified). Pins:
  `python/repark/tests/test_attr_id_1_s3c.py`.
  pins: attr-id-1/C-030
  **ATTR-ID-1 S3d R-S3d-1 (2026-10-01):** `_java_case_equal` is deleted;
  insensitive hits come from the native `java_fold_hits` mode `b` (same
  contract, exact Java tables). The 41 S3c pins are unchanged.
  pins: attr-id-1/C-032
  **ATTR-ID-1 S3e (2026-10-01):** the qualified-name family moves to
  `qualified_names.py` (pure move at the ceiling); `_bind_resolved_name`
  passes the frame qualifiers into `resolve` and prefixes the bound engine
  field with the held (not written) qualifier parts; `_column_of` runs a
  stable-no-op Column through the qualified rewriter, so aliased compounds
  bind; the filter quoter gains the facade qualifiers and substitutes the
  held-qualified engine field under duplicate engines. Pins:
  `python/repark/tests/test_attr_id_1_s3e.py`. pins: attr-id-1/C-039
  **ATTR-ID-1 S4 (2026-10-02):** `_bind_stable_id_column` splits into
  `_exact_rebind_position` (one held position through `attribute_column`, onto a
  unique engine field only) and `_qualified_narrow_position` (a qualifier-carrying
  Column picks its one name-and-qualifier hit before the first-held fallback); an
  id the frame does not hold consults the native `projection_source_ids` lineage
  (alias, single-column `coalesce`, single-column searched `CASE`) before the
  miss, and a miss still returns the column unchanged so the id reaches the
  refusal and the engine fallback. A held id whose SQL already spells its engine
  field stays the written ref (no rebind, so the engine shapes twin refusals).
  `_column_of` routes an `_is_ambiguous_qualified_ref` (qualifiers plus an id
  held twice) through the qualified rewriter first. `_bind_sort_key` takes only
  true rebinds from the id bind; a miss stays verbatim for a plain column but a
  marked (asc/desc) column falls through to the free-name funnel, which binds
  the oldest project hit. Pins: `python/repark/tests/test_attr_id_1_s4.py`.
  pins: attr-id-1/C-040
  **ATTR-ID-1 S4 follow-up (2026-10-02):** the SQL-spells-engine written-ref check
  is deleted and the sameness test moves to `Column._birth_frame`: a column bound
  against the target frame stays verbatim (the engine shapes same-frame twin
  refusals), any other frame's held id rebinds by position through
  `_exact_rebind_position`, which keeps the written ref when the target engine
  name is not exactly unique (a marked duplicate-named ref would fail with a
  bare engine error instead of the shaped ambiguity the engine reports for the
  quoted written ref). A lineage
  hit (`projection_source_ids`) binds only when the output display still shows the
  column's written name under the live session rule, so a rename that drops the
  name refuses while a case-only rename still binds. `outer`, the sort bound
  column, and `_rewrap_with_markers` carry `_birth_frame` with `_attr_id`. Pins:
  `python/repark/tests/test_attr_id_1_s4.py`. pins: attr-id-1/C-041
  **ATTR-ID-1 SJ-2 (2026-10-02):** `_column_frame_id(column)` reads the birth
  frame's node id (`None` without a birth frame); it is the one helper every
  frame-id read uses. Pins: `python/repark/tests/test_attr_id_1_sj2.py`.
  **ATTR-ID-1 SJ-5 F1 (2026-10-03):** `_rebind_stable_name_column` runs the
  native free-name check on compounds that reach it unbound (skipped for
  token-bearing parent refs); `_rebind_free_names` runs the qualified-only
  check on the filter path; `_quote_filter_sql_identifiers` checks the SQL
  text first. The four error builders move to `column_errors.py` (pure move
  at the ceiling). `_window_keys_bound` skips the check for windows whose
  partition and order keys all carry an attribute id (internal
  `drop_duplicates`/`distinct` projections), while user windows with free
  refs still raise. Pins: `python/repark/tests/test_attr_id_1_sj5.py`.
  **CASESENS-2 port phase 2 (2026-10-03):** `_resolve_sort_name`'s project
  branch refuses `UNRESOLVED_COLUMN` when `_native.sort_hits_meet_at_join` says
  the output twins trace to two positions of one join. Spark's child-scope
  fallback cannot resolve those twins. The S0 replay found 105 such cells: they
  answered once the alias join stopped refusing. A computed twin keeps the
  stack's pick (Spark sorts by the child's column). pins: casesens-2/C-012
- `column_sort.py` — **ATTR-ID-1 S4 follow-up (2026-10-02):** the sort-marker
  family, split out of `column.py` at the size ceiling (pure move; `Column`
  binds the six `asc`/`desc` spellings). `_with_sort_order` re-marks the column
  and preserves every other tracked attribute: the marker is the only change,
  so dropping a carried attribute silently breaks another subsystem
  (`sql_expr`, `generator`, the attribute id). No module docstring: the
  lane's no-comments ruling covers the new file; the contract lives here.
  pins: attr-id-1/C-041
- `column_string.py` — **ATTR-ID-1 S4 alias fix (2026-10-02):** the
  string-predicate family (`contains`/`substr`/`startswith`/`endswith`/`like`/
  `ilike`/`rlike` plus `_string_predicate`), split out of `column.py` at the
  size ceiling (pure move; `Column` binds the seven spellings). No module
  docstring: the lane's no-comments ruling covers the new file; the contract
  lives here. pins: attr-id-1/C-042
- `column_render.py` — **ATTR-ID-1 SJ-2 (2026-10-02):** the expression-fragment
  family (`sql_expr_part`/`join_sql_part`/`sql_expr_without_alias`/
  `spark_display_part`/`spark_wrap_display_part`), split out of `column.py` at
  the size ceiling (pure move; `Column` binds the five spellings).
  `join_sql_part` renders the frame field from `_column_frame_id`, `F0` without
  a birth frame. Pins: `python/repark/tests/test_attr_id_1_sj2.py`.
  **ATTR-ID-1 SJ-3 R-SJ3-2 (2026-10-02):** the token carries the bind-time leaf
  display as `__D<UPPERHEX-OF-UTF8>__` after the qualifiers, so a token whose
  birth frame is an unbound dead temp still names its attribute for the native
  preparer's `names` map. Pins: `python/repark/tests/test_attr_id_1_sj3.py`.
- `column_errors.py` — **ATTR-ID-1 SJ-5 F1 (2026-10-03):** the four
  unresolved/ambiguous error builders, moved here from `column_fields.py`
  unchanged (pure move at the size ceiling, re-imported there).
  **CASESENS-2 port (2026-10-03):** `_raise_unresolved_name` suggests every
  display in frame order, as Spark lists every input attribute (live Spark 4.1.2:
  `select("nope")` suggests `` `id`, `s`, `Data` `` under both rules); the
  fold-only `_suggestion_candidates` filter is deleted. pins: casesens-2/C-001, C-010
- `filter_quote.py` — **ATTR-ID-1 S3b H-1 (2026-10-01):** the filter-SQL
  identifier quoter, split out of `column_fields.py` at the size ceiling (pure
  move; the entry stays there). `_FILTER_TOKEN_PATTERN`,
  `_SQL_LITERAL_KEYWORDS`, the candidate pair, the lambda scope tokenizer
  (RC4-1/RC4-6/RC5-2 port: decl sites quoted, bodies resolve params, outer
  columns bind), `_main_path_dotted_token` (main's bare-ident tokenization
  with the byte-identical collision raise), and `_bind_filter_token`.
  pins: attr-id-1/C-026
  **S3b H-1 follow-up (2026-10-01):** a lambda reference that matches its
  parameter only by folding (`X` under param `x`, insensitive rule) raises
  `_FoldedLambdaFallbackError`; the entry catches it and runs the whole
  predicate through `_main_path_filter_sql` (a verbatim port of main's
  fold-everything quoter, string-identical on probes). Spark folds nested
  case-colliding lambda variables to one variable; the engine binds them
  case-sensitively, so only main's path reproduces Spark there. Exact-rule
  references never fold and never trigger. pins: attr-id-1/C-027
  **Gate narrowing (2026-10-01):** the raise fires only when
  `_scopes_have_folded_collision` finds an inner parameter that folds to an
  enclosing parameter with different spelling. Single-level folded
  references and nested scopes without that collision stay on the binder,
  which folds them to the parameter spelling exactly as Spark does.
  pins: attr-id-1/C-028
  **Gate j_cross (2026-10-01):** the qualifier-bound arm carries the same
  one-id multi-hit join-dup refusal as the unqualified arm, echoing
  qualifier-qualified candidates as Spark does. pins: attr-id-1/C-029
  **ATTR-ID-1 S3e (2026-10-01):** `_frame_qualifiers_for_bind` (moved here
  from `column_fields`) and `_known_qualifiers` (plan plus facade names)
  serve every door; `_select_expr_frame` (the `selectExpr` body, moved from
  `core.py` at its ceiling) rewrites qualifier-headed dotted tokens to the
  engine field, aliasing a lone token to its written name; the filter
  binder takes the facade payload. pins: attr-id-1/C-039
  **ATTR-ID-1 SJ-5 F1 (2026-10-03):** `_refuse_ambiguous_free_names` calls the
  native of the same name (one rule for SQL text, expressions, and name
  lists); `_quote_select_expr_dotted` checks each item first.
  `_displays_unique` skips the call when no two displays match under either
  case rule, which the native rule cannot refuse; `str.lower` over-matches
  the native ASCII fold, so a unique verdict always agrees with it.
- `subset_resolve.py` — **ATTR-ID-1 S3d (2026-10-01):** the
  `drop`/`dropDuplicates`/`fillna`/`dropna` name-binding home. `_bindings`
  reads the stamped ids, native engines, and facade displays (or `None` for a
  bridge frame or a desynced overlay, where each caller keeps its legacy path);
  `_grouped` answers bound/ambiguous/missing over the live-rule hits;
  `_guard_passes` is the shared S3a/S3b guard (a multi-hit bind needs unique
  engine fields and no join dup below the wrappers). `_drop_targets` binds each
  drop item: a parent Column by its `_attr_id` (every id position, one side of
  a self-join; an engine-shared id position takes the base-identical native
  path because a bare duplicate-engine frame cannot drop one position by
  name), an origin-mapped Column by its engine, a resolved-but-absent Column
  to a no-op, a free Column by name with ambiguity refusal, a str by fanning
  out to every hit, and a miss to a no-op; dotted or backticked free Columns
  keep the native qualified path (S3e owns qualified). `_fanout_subset` binds
  each `dropDuplicates` key to every hit and misses with Spark's
  `_LEGACY_ERROR_TEMP_1201`; `_bound_subset_positions` binds each
  `fillna`/`dropna` key with ambiguity refusal and guarded fan-out, missing
  with `UNRESOLVED_COLUMN.WITH_SUGGESTION`. No module docstring: the lane's
  no-comments ruling covers the new file; the contract lives here.
  **ATTR-ID-1 S3d follow-up (2026-10-01):** the insensitive folds are
  codepoint Java (A: `toLowerCase`; B: `==`/upper/lower per codepoint) over
  int-version Java tables, so expansion pairs (`ß`/`SS`, U+0130/`i`+U+0307,
  U+FB00/`FF`) and newer-than-Java scripts (Vithkuqi, U+A7Cx) miss; one id
  closes over every same-id position in both rules (a sensitive exact hit
  fans out too); a one-id multi-position bind under a union binds the
  positionally-first hit only for `fillna`/`dropna`/free-Column `drop`,
  while `drop(str)` and `dropDuplicates` keep every-hit fan-out
  (probes s3d8..11).
  pins: attr-id-1/C-034
  **ATTR-ID-1 S3d R-S3d-1 (2026-10-01):** the Python fold tables and helpers
  are deleted; `_hits_folded` takes a mode and calls the native
  `java_fold_hits` (`a` for `fillna`/`dropna`/free-Column `drop`, `b` for
  `drop(str)`/`dropDuplicates`).
  pins: attr-id-1/C-032
  **ATTR-ID-1 S3d follow-up 2 (2026-10-01):** `_trim_union_first` passes the
  closed positions to the native `union_dup_below_wrappers`: the trim fires
  only when two or more positions reach the Union through identity
  projections, so a dup created above the union (`unionbn`) and a
  reorder/select above a dup union fan out (probes s3d12..14).
  pins: attr-id-1/C-036
  **ATTR-ID-1 S3d follow-up 3 (2026-10-01):** mode `a` is length plus
  `String.toLowerCase` (fillna/dropna miss U+0130/`i`, probe s3d16); mode `b`
  is OpenJDK `equalsIgnoreCase` down to the lower-of-uppers step.
  pins: attr-id-1/C-037
  **ATTR-ID-1 S3d follow-up 4 (2026-10-01):** under a union, `fillna`/
  `dropna`/free-Column `drop` refuse a multi-id bind and bind the
  positional-first hit of a single-id bind for either spelling (a
  creation-dup union is multi-id and refuses, a select-dup union is
  single-id and trims — probe s3d18 untangled the two fixtures behind the
  withdrawn multi-exact refusal); `drop(str)` and `dropDuplicates` fan out
  to every hit with no trim and no refusal (probe s3d17).
  pins: attr-id-1/C-038
  **ATTR-ID-1 S3e (2026-10-01):** a dotted free Column whose head names a
  qualifier resolves through `resolve` (unique engines drop by attribute,
  shared engines drop by plan-held qualified reference, a miss is a no-op,
  twins refuse `AMBIGUOUS_REFERENCE`); drop strings stay literal (Spark
  probes s3e1–6: `drop("a.v")` is a no-op). pins: attr-id-1/C-039
  **ATTR-ID-1 S4 alias fix (2026-10-02):** the `_attr_id` drop path takes
  only simple refs (display equals the drop name), so an alias — which now
  carries its base id — falls through to the name path and stays a no-op as
  at base. pins: attr-id-1/C-042
- `qualified_names.py` — **ATTR-ID-1 S3e (2026-10-01):** the qualified-name
  home, split out of `column_fields.py` at the ceiling. `_frame_qualifiers`
  threading (`_alias_frame_qualifiers` names every stamped id,
  `_join_frame_qualifiers` unions each side's names onto the
  output ids, pairing using keys), the compound rewriter
  (`_rebind_qualified_refs`), the qualified star expansion
  (`_expand_qualified_star`, unknown qualifiers fall through to the engine),
  and the qualified sort bind (`_resolve_sort_qualified_name`, ambiguous
  twins and multi-hit binds under a join raise unresolved as Spark does in
  sort). No module docstring: the
  lane's no-comments ruling covers the new file; the contract lives here.
  pins: attr-id-1/C-039
  **ATTR-ID-1 S4 follow-up (2026-10-02):** the qualified star expansion, the
  qualified sort bound column, and the rebound rewrap carry `_birth_frame` with
  `_attr_id`, so expanded columns count as birth-frame columns at the bind.
  pins: attr-id-1/C-041
- `functions.py` — scalar, collection, date/time, aggregate, generator, UDF, and
  window function exports. SQL fragments use centralized escaping helpers and
  unsupported operations fail explicitly.
  **FACADE-2 step 3 (2026-09-13):** `lit` temporal arms and the numpy-array cast path
  use typed `_native.PyColumnParts` constructors (`lit_timestamp` / `lit_date` /
  `lit_time` / `lit_array_cast`); `_scalar` ships child display/SQL/join fragments to
  `PyColumnParts.call_scalar`, which renders `name(args)` in Rust while foldability,
  aggregate and ungroupable flags stay Python-side bookkeeping. `F.expr` stays the
  one `_native.PyColumn.sql` caller — its text is the caller's. `_lit_numpy_ndarray`
  still walks NumPy elements in Python (recorded P3, no change). pins: facade-2/C-014,
  C-016, C-018, C-021
  **ABS-EXPR-1 (2026-09-13):** `abs` is one native `_scalar("abs", …)` call — the old
  `when(c < 0, 0 - c).otherwise(c)` rewrite embedded its child 3× per level, so nested
  `F.abs` chains were exponential in native memory and aborted the process at depth ~14
  (run 9 OBS-R9-6 / INC-R9-1). pins: abs-expr-1/C-002, C-003
  **FNP-11B step 6 (2026-09-15):** `lit` takes `decimal.Decimal` (Spark's
  inferred precision and scale) by casting the decimal text, so no new native
  literal constructor is needed. pins: fnp-11b/C-005
- `functions_agg.py` — aggregate-function re-exports. **FNP-ALIAS-1 (2026-09-15):**
  `approxCountDistinct` is the deprecated alias of `approx_count_distinct` and warns Spark's
  exact `FutureWarning` on every call; it reaches `functions.py` through this module's
  `install_into` (functions.py sits at its baseline, so new names install instead of
  importing). pins: fnp-alias-1/C-001, C-003
- `functions_bitwise.py` — bitwise scalar wrappers. **FNP-ALIAS-1 (2026-09-15):**
  `shiftLeft`/`shiftRight`/`shiftRightUnsigned` are the deprecated camelCase aliases of the
  Spark-equal snake_case kernels and warn Spark's exact `FutureWarning` on every call; they
  reach `functions.py` through this module's `install_into`. pins: fnp-alias-1/C-001, C-003
  **FNP-MISC-1 (2026-09-15):** `bucket` folds a literal Column count to the int path,
  including CAST(int-literal AS INT/SMALLINT/TINYINT/BIGINT); any other Column raises
  `NOT_COLUMN_OR_INT`. Every call warns the Spark 4.1.2 `FutureWarning`; outside
  `partitionedBy` the fragment refuses like the int form.
  pins: fnp-misc-1/C-003, fnp-misc-1/L-003
  **FNP-BITMAP-FACADE-1 (2026-09-15):** `bitmap_construct_agg` / `bitmap_or_agg` /
  `bitmap_and_agg` are one-line aggregate wrappers over
  `column._inner.aggregate(kind, False)` — the same shape `functions.py`'s `sum` builds —
  reaching the FNP-6D Rust UDAFs through the three new `unary_aggregate_udaf` arms, and
  installing through this module's `INSTALL_NAMES`. pins: fnp-bitmap-facade-1/C-001, C-002
- `functions_agg.py` — aggregate-function re-exports.
- `functions_agg_1.py` — FNP-AGG-1 slice-(d) aggregate installed onto `functions.py`
  `__all__` through this module's `install_into`: `grouping_id` over the new
  kernel (zero or more columns; display names the Spark spelling), registered
  for by-name resolution.
  pins: fnp-agg-1/C-001, C-002
- `functions_bitwise.py` — bitwise scalar wrappers.
- `functions_arrow_udf.py` — **FNP-MISC-1 (2026-09-15):** `arrow_udf` over the pandas
  bridge (scalar / iterator / grouped forms chosen by type hints, or forced through
  `functionType`) and `arrow_udtf` over the Python UDTF path. Round 2 runs arrow `eval`
  once per RecordBatch with whole-column Arrays (`_eval_batch`, Tables projected onto
  declared names, per-row `eval` refuses) through the additive `_map_arrow_udtf_batches`
  branch in `udtf.py`, and streams the SCALAR_ITER adapter one batch at a time behind a
  pull feeder. `pyarrow` stays optional: both entry points import it lazily and raise
  Spark's `[PACKAGE_NOT_INSTALLED]` shape when it is absent. Installed onto `functions.py`
  after `__all__` through `install_into`.
  pins: fnp-misc-1/C-004, C-005, C-006, fnp-misc-1/F-1, fnp-misc-1/F-2, fnp-misc-1/L-002
- `functions_byname.py` — **FNP-MISC-1 (2026-09-15):** `call_function` / `call_udf`
  resolved at call time (session-registered UDF wins, else the engine scalar built by name,
  else a `FACADE_ONLY_ROUTINE_NAMES` fallback, else `UNRESOLVED_ROUTINE`; dotted names raise
  `REQUIRES_SINGLE_PART_NAMESPACE`; `BYNAME_NON_ROUTINE_NAMES` never resolve). Round 3 maps
  engine arity failures to `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` (exact counts in Spark's
  words; ranged specs reuse the engine wording). Installed
  onto `functions.py` after `__all__` through `install_into`. **DOOR-CONVERGE-1 round 5
  (2026-09-16):** `char_length` leaves `FACADE_ONLY_ROUTINE_NAMES` — the Rust dispatch
  serves it (`call_function('char_length', lit('abc'))` = 3, A11-callfn-char-length).
  pins: fnp-misc-1/C-002, fnp-misc-1/F-3, fnp-misc-1/F-4, fnp-misc-1/F-5, fnp-misc-1/L-004, fnp-misc-1/L-005, fnp-misc-1/L-010
  **FNP-BITMAP-FACADE-1 (2026-09-15):** `FACADE_ONLY_ROUTINE_NAMES` gains
  `bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg` (owner ruling: PySpark 4.1.2
  resolves them as `call_function` builtins, so they are facade-only routines); the
  by-name route answers the same bytes as the wrappers and the SQL door.
  pins: fnp-bitmap-facade-1/C-002, fnp-bitmap-facade-1/C-004
  **FNP-WIN-1 step 5 (2026-09-15):** `window` and `session_window` join the
  facade-only rows (analyzer-rewrite and GROUP-BY-only marker names the scalar
  dispatch does not carry; step-2 debt for `window`).
  pins: fnp-misc-1/C-002, fnp-misc-1/F-3, fnp-misc-1/F-4, fnp-misc-1/F-5, fnp-misc-1/L-004, fnp-misc-1/L-005, fnp-misc-1/L-010, fnp-win-1/C-008
- `functions_collections.py` — array, map, sequence, and collection wrappers. **FNP-9
  (2026-09-05):** `create_map`, `map_concat` and `array_insert` land here.
  pins: fnp-9-collections-json/C-006
  **ARRAY-NULL-1 (2026-09-14):** `_glue_element` is one `_scalar("array_append"/
  "array_prepend", a, e)` call per level — the null-preserving
  `spark_array_*_udf` shims replaced the `when(isnull(a), NULL).otherwise(flatten(...))`
  rewrite that embedded `array_col` 2× per level (measured ~×3/level: 51 MB at
  depth 12, 269 MB at depth 16 on the base tree).
  pins: array-null-1/C-002, C-003
  **FN-FIX-1:** `arrays_overlap` is the three-valued kernel, not the size-of-intersect shim.
  Live co-collect `test_live_fn_fix_1_arrays`.
  pins: fn-fix-1-registry-rows/C-002
- `functions_datetime.py` — date/time and timestamp wrappers.
- `functions_declared.py` — FNP-15/16 declared-absent refusals (unreachable / deferred-by-cost).
  Installed onto `functions.py` after `__all__` so the sql.functions re-export sees them.
  Sketches (32), CSV/XML/XPath (11), VARIANT (8), and geospatial (5) are deferred-by-cost.
  pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011, C-014, C-016
- `functions_expr.py` — shared expression builders and scalar lowering. **FNP-9/10
  (2026-09-05):** `arrays_zip` and `schema_of_json` stop refusing and route to their kernels.
  pins: fnp-9-collections-json/C-003, C-006
  **FNP-GEN-1 (2026-09-16):** `from_xml` / `schema_of_xml` raise the dated declared
  refusal naming `FNP-16-csv-xml-xpath` (owner ruling D-6, 2026-09-15), verbatim the
  same `{name} is reachable without a JVM …` string the armed parse-altitude refusal
  raises on both SQL doors. pins: fnp-gen-1/C-002, C-003
  **FNP-GEN-1 step 3 (2026-09-16, run 18a):** `from_csv` stops refusing and binds
  its schema string as a literal and its options dict as a `create_map` of literals
  (built inline — `functions_collections` imports this module, so the facade cannot
  import `create_map` back); the output name keeps Spark's first-argument form.
  **Step 4b (run 18a):** `schema_of_csv` stops refusing and binds its CSV literal
  plus an optional options dict the same way; the output name keeps Spark's
  `schema_of_csv(<csv text>)` form. pins: fnp-gen-1/C-003, C-004
  **Gate pass (run 18a):** `from_csv` checks its schema argument with the
  conditioned `NOT_COLUMN_OR_STR` bar. pins: fnp-gen-1/C-006
  **ABS-EXPR-1 (2026-09-13):** `cbrt` and `nullif` are one native `_scalar` call each
  (`expr_fn::cbrt` / `expr_fn::nullif`); both `when(...)` rewrites embedded their child
  more than once per level (cbrt 3×, nullif 2×). `nvl2` stays a `when` — each child is
  embedded exactly once (linear). pins: abs-expr-1/C-002, C-004
  **FNP-MATH-1 step 4 (2026-09-16, run 18a):** `hash` destubs in place to one native
  `_scalar` call (default display already reads Spark's `hash(a, b)`); `hash` leaves
  `functions_byname.py` `FACADE_ONLY_ROUTINE_NAMES`. pins: fnp-math-1/C-001, C-005
  **FNP-MATH-1 step 5 (2026-09-16, run 18a):** `format_number` destubs in place to one
  native `_scalar` call (the `d` scale rides `lit_indices`, so the display reads
  Spark's `format_number(x, 2)`); `format_number` leaves `FACADE_ONLY_ROUTINE_NAMES`.
  pins: fnp-math-1/C-001, C-002, C-003
  **FNP-MATH-1 mask slice (2026-09-16, run 18a):** `mask` lands in
  `functions_math.py` (absent on the base tree, so no destub): Spark defaults
  materialize as `lit` args, and the display already reads
  `mask(masked, X, x, n, NULL)`. pins: fnp-math-1/C-001, C-002, C-003
  **FNP-ALIAS-1 (2026-09-15):** `degrees`/`radians` move to `functions_math.py` (this file sat
  exactly on its ceiling; the baseline ratchets 2247 → 2237 in `check_lib_py.py` and the CAP-1
  mirror). `functions.py` ratchets 1962 → 1960: the two re-export entries move between its
  import blocks, the tail gains a third module-handle line for the new `install_into` modules,
  paid by one narration comment line.
  **FNP-11B step 2 (2026-09-15):** `to_date` / `to_timestamp` / `unix_timestamp` stop
  refusing `format=` and bind it as a literal (`lit_indices={1}`); a Column format raises
  `NOT_ITERABLE`; `unix_timestamp` takes Spark's default `'yyyy-MM-dd HH:mm:ss'`. The file
  holds exactly 2235 lines. pins: fnp-11b/C-001, C-002
- `functions_stack.py` — **PERF-UNPIVOT-1 (2026-09-12):** `F.stack` / `StackCall` /
  `select_with_stack_if_present`. Installed last onto `functions.py`; its
  one-generator gate also reads `_repark_generator` (FNP-GEN-1) so `stack` beside
  an analyzer generator refuses. pins: perf-unpivot-1/C-004, fnp-gen-1/C-003
- `functions_generators.py` — **FNP-GEN-1 step 2 (2026-09-16):** the thin wrappers for
  `posexplode` / `posexplode_outer` / `inline` / `inline_outer`, plus `_GeneratorColumn`
  whose `alias(*names)` packs multi-name output aliases into the `__repark_gen_alias`
  marker call that `repark_functions::generator::GeneratorRewrite` peels. The wrappers
  return plain scalar expressions; the analyzer rule does the unnesting, so
  `dataframe/**` and the `_select_with_generator` allow-list stay untouched.
  `_GeneratorColumn` carries `_repark_generator` (the `functions_stack.py`
  one-generator gate reads it so `stack` + `inline` refuses) and clears
  `_projection_name` so `_collapse_identity_projection_alias` keeps the call bare —
  an identity `Expr::Alias` would otherwise reach the rewrite as a one-name
  `COLUMN_ALIASES_MISMATCH`. `GENERATOR_NAMES` is the export table
  `scripts/check_example_coverage.py` reads, and
  `install_into` runs at the tail of `functions.py`'s installer chain; `posexplode` /
  `posexplode_outer` are additionally imported statically into `functions.py` because
  their `__all__` rows predate the installer. **Step 2 (run 18a):** `json_tuple` joins
  this module — its stub leaves `functions_expr.py` and it is imported statically
  beside `posexplode` / `posexplode_outer`, keeping its presplit `__all__` row, so
  `GENERATOR_NAMES` keeps only the four step-2 names. A lone `.alias('x')` on one
  field is ignored and a wrong count raises `UDTF_ALIAS_NUMBER_MISMATCH`, both decided
  by the rewrite. pins: fnp-gen-1/C-001, C-002, C-003
- `functions_json.py` — **FNP-10 (2026-09-05):** the JSON wrappers (`get_json_object`,
  `json_array_length`, `json_object_keys`, `to_json`, `from_json`). Its `install_into` also
  re-exports the collection constructors from `functions_collections`, so the whole FNP-9/10
  surface reaches `functions.py` through the existing installer chain instead of growing that
  module past its exact size baseline. `FNP9_NAMES` is the export table
  `scripts/check_example_coverage.py` reads, so a name added here is a name that needs an
  example. The unit's unbuilt names (`call_udf`, `call_function` — before FNP-MISC-1)
  were deliberately NOT here (`stack` landed in PERF-UNPIVOT-1; `inline` /
  `inline_outer` landed in FNP-GEN-1 step 2 through `functions_generators.py`):
  exporting a refusal would add rows to an example backlog whose count only ratchets down.
  §7 `FNP9-GENERATORS-1` / `FNP9-BYNAME-1`.
  The `DataType` import is under `TYPE_CHECKING` — a runtime one closes an import cycle through
  `repark.spark.types`. `_refuse_json_options` is the one rule `from_json`, `to_json` and
  `schema_of_json` share: repark implements no JSON option beyond `mode` and
  `columnNameOfCorruptRecord`, and Spark's `ignoreNullFields` / `primitivesAsString` change the
  answer, so a non-empty mapping refuses instead of being ignored.
  `install_into` ran last in `functions.py`'s installer chain until PERF-UNPIVOT-1 appended
  `functions_stack`; `test_functions_split_identity.py` pins the order, which
  `test_functions_split_identity.py` pins by position. The rules each wrapper carries — Spark's
  `map(k1, v1, …)` spelling behind `create_map`, the `-1`-appends and NULL-padding rules of
  `array_insert`, the NULL-fill of `arrays_zip`, PERMISSIVE decoding and the `_corrupt_record`
  column of `from_json`, and `schema_of_json` reading a bare `str` as the document rather than a
  column name — are recorded here and in the unit ledger, not in the function bodies: each keeps
  exactly the one-line docstring the presence gate requires.
  pins: fnp-9-collections-json/C-001, C-007
  FN-REGEXP-EXTRACT-1 (2026-09-04): `regexp_extract` calls the native kernel on both doors; its
  docstring is one line.
  SEM-1: `log(col)` or `log(base, expr)` (PySpark `log(arg1, arg2=None)`).
  LOG1P-1: `log1p` / `expm1` are `_scalar` onto the precise kernels, not
  `log(1+col)` / `exp(col)-1`.
  pins: sem-1-spark-answer-parity/C-006
  pins: log1p-1-precise-kernels/C-002
  **DATE-FN-1 (2026-09-04):** `unix_timestamp` is `_scalar` onto the kernel (format arg still
  unsupported). pins: date-fn-1-spark-date-spelling/C-002
  **FN-FIX-1 (2026-09-03):** `sha2` hex string + bit lengths; `array_sort` vs
  `sort_array`; `percentile_approx` discrete type.
  pins: fn-fix-1-registry-rows/C-002
  **PERF-APPROXPCT-1 (2026-09-05):** `percentile_approx` threads accuracy: the native
  `_inner` call takes it as `Option` (None is the two-arg default), and the `sql_expr`
  carries a `, {accuracy}` tail because the list form always lowers through the
  global-aggregate SQL path (nested parens fail the native classifier), where a missing
  tail would silently run at default accuracy.
  pins: perf-approxpct-1/C-002
  **Round 2 (2026-09-06):** accuracy normalizes through `_integral.checked_integral`
  before either path (Spark's INTEGRAL contract, measured on live 4.1.2): numpy integers
  run as the int on both forms, bool/float/str fail with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` — the NULL-tail fallback is gone. The two
  Column builds merged into one shared return; ceiling 2259 → 2258.
  pins: perf-approxpct-1/C-002
  **Round 3 (2026-09-06):** that refusal is `AnalysisException` with Spark's class,
  message and params (not `PySparkTypeError` / `{arg_name, arg_type}`).
  pins: perf-approxpct-1/C-002
  **FN-FIX-2 (2026-09-04):** `trim`/`ltrim`/`rtrim` optional charset; `initcap` /
  `chr`/`elt`/`rlike` lower onto Spark kernels. pins: fn-fix-2-string-rows/C-002
  **FN-REGEXP-EXTRACT-1 (2026-09-04):** `regexp_extract` is `_scalar` onto the
  kernel (bare pattern forced-lit, optional idx defaulting to 1).
  pins: fn-regexp-extract-1/C-001
  **TYPES-1 (2026-09-05):** `from_unixtime` forwards the optional format argument.
  pins: types-1/C-006
- `functions_lambda.py` — higher-order function and lambda builders. FNP-4c adds
  `transform`, `filter`, `forall`, `aggregate`, `reduce`, `zip_with`, `transform_keys`,
  `transform_values`, `map_filter`, `map_zip_with` (installed onto `functions.py` `__all__`).
  Spark 4.1.2 `NUM_ARGS_MISMATCH` puts the user arity in expects and the declared arity in got.
  FNP-8-REVIEW (2026-09-07) restored the `_lambda_arity` docstring #412 reworded.
  pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012
  **FNP-8 (2026-09-07):** Column builders preserve function-specific
  `NUM_ARGS_MISMATCH` errors after the generic 1–3 parameter check. Invalid-return errors name
  functions, callable objects, and `functools.partial` instances without assuming `__name__`.
  pins: fnp-8/C-003
- `functions_try.py` — FNP-7a/7b `try_*` wrappers installed onto `functions.py` `__all__`.
  pins: fnp-7-try-inversions/C-013, C-016
- `functions_math.py` — mathematical and trigonometric wrappers. **FNP-ALIAS-1 (2026-09-15):**
  `degrees`/`radians` move here from `functions_expr.py` (functions_expr sat exactly on its
  ceiling) and stop composing `(x * 180) / pi()`. The engine's `degrees` scalar has no
  `call_scalar` dispatch arm and Rust is fenced, so the wrapper is one multiply by the
  precomputed double `180.0/pi` (Rust `f64::to_degrees`'s exact form) — measured bit-equal to
  the SQL-door kernel and to live PySpark 4.1.2 on every oracle cell, including INT input
  (the DOUBLE literal coerces before the multiply, so no ANSI overflow). Display is Spark's
  `DEGREES(x)`/`RADIANS(x)` via the `dayname`-style rewrap. `toDegrees`/`toRadians` are the
  deprecated aliases and warn Spark's exact `FutureWarning`; they reach `functions.py`
  through this module's `install_into`. **FNP-MATH-1 step 2 (2026-09-16, run 18a):**
  `bround(col, scale=None)` joins `INSTALL_NAMES` (thin `_scalar` bind, scale default
  materialized as `0` so the display reads Spark's `bround(d, 0)`); the installer path
  keeps `functions.py` untouched under its ceiling. pins: fnp-math-1/C-001, C-002
  **FNP-MATH-1 step 3 (2026-09-16, run 18a):** `conv(col, fromBase, toBase)` joins
  `INSTALL_NAMES` beside it. pins: fnp-math-1/C-001, C-002, C-003, C-004
  **crit-logic-1 L-001 (2026-09-15):** `_rescaled`
  threaded join origin like every house wrapper of that date — `join_sql_expr` from the
  multiply result and the origin threader — so a right-parent column after semi/anti raises
  `MISSING_ATTRIBUTES` instead of silently binding the left, and a two-sided `degrees` ON
  clause binds each side. pins: fnp-alias-1/C-001, C-002, C-003, C-004
  **ATTR-ID-1 S4 (2026-10-02):** the origin threader is deleted with the origin encodings;
  the composed `join_sql_expr` attribute tokens alone carry the semi/anti refusal.
- `functions_temporal.py` — FNP-11A temporal wrappers installed onto `functions.py`
  `__all__`; `make_timestamp` and `months_between` delegate through `functions_expr.py`.
  Interval builders print only the parts the call gave (`try_make_interval()`
  prints one zero); `months_between` prints `roundOff` only when false.
  **FNP-11B step 3 (2026-09-15):** `to_timestamp_ltz` / `to_timestamp_ntz` join
  `FNP11A_EXPORTS` (thin `_scalar` binds, `format` stays a literal position) —
  a new exports tuple would cost the example-coverage gate a binding-table line
  it has no room for (`check_example_coverage.py` sits exactly on its ceiling),
  so the step-3 names ride the existing installer tuple and binding;
  `try_to_timestamp` is destubbed in place in `functions_expr.py`.
  **FNP-11B step 4 (2026-09-15):** `make_time` / `to_time` / `time_diff` /
  `time_trunc` / `current_time` / `typeof` join `FNP11A_EXPORTS` the same way
  (thin `_scalar` binds with PySpark 4.1.2's parameter names, order and defaults).
  **FNP-11B step 5 (2026-09-15):** `to_char` / `to_varchar` / `to_number` /
  `to_binary` join `FNP11A_EXPORTS` the same way (`format` stays a literal
  position; `to_binary` defaults it to `None`).
  **FNP-11B step 6 (2026-09-15):** `make_timestamp` widens to PySpark 4.1.2's
  all-optional signature under owner ruling Q-15a-3 (the freeze register
  regenerates in the same commit); the live def in `functions_expr.py` widens
  identically and forwards to this one.
  **FNP-11B remediation round 1 (2026-09-16):** `functions_expr.py` drops the
  `make_timestamp` forwarder for a direct re-export of this def (identical
  signatures; the freeze builder follows the alias); `ColumnOrName` format
  positions (`to_timestamp_ltz/ntz`, `to_time`, `to_char/varchar/number`,
  `to_binary`, `try_to_timestamp`) stop forcing string formats to literals so a
  bare string is a column reference as Spark measures it, while the
  `str`-typed formats keep `lit_indices`; `current_time` passes `foldable=True`.
  pins: fnp-11a/C-001, C-015; fnp-11b/C-001, C-002, C-005, C-007
- `functions_math.py` — mathematical and trigonometric wrappers.
  clause binds each side. **DEGREES-RUST-1 (2026-09-15, owner Q-15a-1):** `degrees` /
  `radians` move onto the engine's scalar UDFs through new `call_scalar` dispatch arms —
  the single-multiply `f64::to_degrees` / `f64::to_radians` form, swept bit-identical to
  the replaced multiply over 4015 values, so reuse and not a new kernel. The wrappers are
  thin `_scalar` binds over a `double` cast keeping the `DEGREES(x)` / `RADIANS(x)`
  display, the warnings, and every alias pin green; `_rescaled` and the factor constants
  are deleted. **Run 16a round 3 (DEGREES-RUST-1 done properly, owner Q-15a-1):** the
  Python `double` cast is dropped (PYPERF-001) — the wrappers are bare `_scalar` binds
  and the Spark-exact UDFs in `repark_functions::spark_degrees` carry the numeric/STRING
  coercion, the refusals and the ANSI switch for both doors. pins: fnp-alias-1/C-001,
  C-002, C-003, C-004; fnp-bitmap-facade-1/C-011, C-012, C-013, C-014
- `functions_session.py` — session-bound function helpers.
- `functions_udf.py` — Python UDF and pandas UDF markers, validation, and return-type
  contracts. Execution uses the DataFrame Arrow bridge. DFCORE-2 (2026-09-07): the
  `pandas_udf` docstring cross-reference follows the scalar rewrite to its new home,
  `dataframe/udf_projection.py` (line-count neutral; ceiling stays 1300).
  pins: dfcore-2/C-004
- `functions_url.py` — URL parsing and encoding wrappers.
- `functions_window.py` — window function wrappers, plus the thin `window(...)`
  wrapper and its tail-install row (**FNP-WIN-1**, 2026-09-15); step 3 adds the
  thin `window_time(...)` wrapper to the same tail-install row; step 4 adds the
  thin `session_window(timeColumn, gapDuration)` wrapper (string or Column gap).
  Rebased onto FNP-11A: the tail-install row runs after the temporal row
  (`_fz`, then `_fwn`), and its import shares the temporal import line so
  `functions.py` stays at its 1960 baseline.
  pins: fnp-win-1/C-001, C-002, C-003, C-004, C-008
- `merge.py` — `mergeInto` builder and SQL MERGE source registration. DML-A:
  `whenNotMatchedBySource` DELETE/UPDATE execute.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003
  **ATTR-ID-1 S2 (2026-09-30):** the MERGE source view is registered stripped of
  `repark.attr`, like every other write source.
  pins: attr-id-1/C-009
- `merge_aliases.py` — **IPI-56 (2026-09-20):** which `(target, source)` aliases
  the rendered `MERGE INTO` declares. Spark's own condition form qualifies the
  target by its **short table name** and the source by the frame's alias, so a
  hardcoded `AS target` / `AS source` pair answered `No field named <table>.id`
  on five inventory cells. A SQL relation carries exactly one alias, so the pair
  is read off the qualifiers the user's own expressions reference: the short
  table name when they name it, the legacy `target` when they name that instead,
  and whichever single other qualifier they use for the source. That keeps
  RePark's `target.` / `source.` spellings and the bare-key sugar working beside
  Spark's form — owner decision 22 narrows registry row `EX-DF-9` rather than
  retiring it. String literals are removed before the scan so a `'target.x'`
  literal cannot pick an alias.
  pins: ipi-19-56-37-schema-evolution-write/C-008, C-010
- `polars.py` — optional Polars-style facade. Imports Polars lazily and keeps join,
  sort, and null-placement semantics explicit. TYPES-1 round 4: `with_row_index` casts
  `row_number` to BIGINT (pins: types-1/C-005). DF-EAGER-1 step 2 (2026-09-09):
  `PolarsFrame.eager()` wraps the Spark `eager()`; `collect()` is untouched
  (pins: df-eager-1/C-006). **ATTR-ID-1 SJ-3 (2026-10-02):** `PolarsFrame.join`
  re-mints over the lineage shared set and spawns with the `Join` node via
  `join_plan_lineage`, so later self-join checks see its lineage.
- `observation.py` — **DF-SURFACE-B-1 (2026-09-14):** PySpark `Observation`. A
  named (or generated-name) handle filled by the first action on a
  `DataFrame.observe` child; `get` before that action raises
  `NO_OBSERVE_BEFORE_GET` instead of blocking. Exported from `repark.spark` and
  `repark.spark.sql`. pins: df-surface-b-1/C-003, C-004, C-005
- `row.py` — Spark-compatible Row construction, indexing, equality, nested conversion,
  display, and pickling. ROW-TUPLE-1 step 1 (2026-09-14): `count` / `index` delegate to
  the stored values tuple (factory rows: the field-name tuple), answering the recorded
  `row.*` oracle cells; `index_missing` keeps Spark's bare-`ValueError` message. Critic
  round 1 (R-3): both signatures positional-only like CPython's `tuple` — keyword calls
  raise `TypeError`.
  pins: row-tuple-1/C-001, C-002, C-004
- `storage.py` — StorageLevel flags and the facade cache contract. Disk, off-heap,
  and replication flags are recorded; actual persistence is engine-owned.
- `ta.py` — TA-Lib technical-analysis/window helpers and `with_indicators`. ML
  estimators and feature/evaluation surfaces live in [ml/map.md](ml/map.md).
- `types.py` — Spark SQL data types, DDL/JSON conversion, schema inspection, interval
  support, metadata, and Python-value verification.
  **FACADE-4 step 1 (2026-09-14):** the conversion surfaces
  (`fromDDL`/`_parse_datatype_string`, `StructType.toDDL`, `_arrow_type_to_repark`,
  `struct_type_from_arrow`, `repark_type_to_arrow`) thin to descriptor build + one
  `_native` call over `repark_spark::type_table`; the public classes and
  `isinstance` identity are unchanged. `simpleString`/`_engine_type` answer from
  the `_type_table.py` row table (P1-DTYPES: per-column descriptor FFI regressed
  `dtypes` +106 % on wide50). Python residue: descriptor trees containing a
  foreign `DataType` subtype, decimals outside the Arrow FFI scale envelope,
  collation refusal policy, and the `json`/`fromJson` surface.
  **Remediation round 2 (2026-09-14):** `_parse_datatype_string` moved to
  `_type_table.py` (Python parse for beyond-i64 parameters and non-printable
  text); parameterised `jsonValue` formats locally; `repark_type_to_arrow`
  checks the decimal FFI bound first and falls back to
  `_repark_type_to_arrow_python` on any FFI export failure (Arrow nesting-depth
  ceilings); the FFI-covered wide-decimal pre-walks are gone; native calls bind
  through the cached `_type_table._native_function`.
  **Remediation round 3 (2026-09-14, ruling R14b-D-2):** the per-class literal
  `simpleString`/`_engine_type` methods are restored on every atomic class
  exactly as base had them (the five dynamic-answer classes and `DataType`
  keep `type(self).typeName()` bodies) — the row-table dict path was ~0.24 µs
  per leaf against base's ~0.05 µs method call, and `dtypes` breached the
  surface bar. `_atomic_token` still answers for nested composition, foreign
  subclasses and the SQL/DDL marker columns; the MRO mutation still turns the
  marker pins red.
  **Remediation round 4 (2026-09-14, L-007/L-008/L-009):** `DataType.simpleString`
  answers from `_SIMPLE_STRING_FAST` for exact classes then falls back to
  `type(self).typeName()` — the five dynamic-answer classes lose their literal
  methods so multiple-inheritance MRO matches base; `DataType._engine_type`
  delegates to `self.simpleString()`. `ArrayType`/`MapType`/`StructField`/
  `StructType` get base's `simpleString`/`_engine_type` bodies back so child
  and field overrides compose (a `StructField.simpleString` override survives
  inside `StructType`, `ArrayType` and `MapType`); the parameterized
  `jsonValue`s dispatch through `self.simpleString()` again. `StructType.toDDL`
  and `repark_type_to_arrow` pass their surface orders into
  `_type_table`'s descriptor build.
  **Remediation round 5 (2026-09-14, L-010):** `repark_type_to_arrow` builds
  the descriptor before the C-028 wide-decimal envelope guard and keys the
  guard on the descriptor's `decimal` kind — `.precision`/`.scale` are read
  only after the `_arrow_order` pick resolves to `DecimalType`, so a
  `Left+DecimalType` class without decimal attrs keeps base's left-parent
  Arrow answer instead of crashing.
  pins: facade-4/C-011, C-012, C-014, C-015, C-016, C-020..C-024, C-028..C-033
  **TYPES-BASES-1 (2026-09-14):** the concrete classes re-parent onto the Spark
  abstract bases imported from `types_bases.py` (`DataType` moved there so the
  bases can subclass it without an import cycle; `_SIMPLE_STRING_FAST` is a
  shared dict populated here so the fast path survives the move), and
  `types.Row` re-exports `spark.row.Row`. DDL routing through the Rust table is
  unchanged — spatial DDL tokens stay refused pending the Rust spatial step.
  Follow-up: `_merge_type` gains Spark's two mixed-SRID arms
  (Geometry×Geometry / Geography×Geography with different `srid` → the `ANY`
  form) plus `SpatialType` in the `StringType` soft-merge tuple;
  `StructField` / `StructType` gain `needConversion` / `toInternal` /
  `fromInternal` delegating to the shared helpers in `types_bases.py`.
  pins: types-bases-1/C-001, C-003, C-005, C-006
- `types_bases.py` — **TYPES-BASES-1 (2026-09-14):** `DataType` plus the Spark
  abstract bases (`AtomicType`, `NumericType`, `IntegralType`, `FractionalType`,
  `DatetimeType`, `AnyTimeType`, `AnsiIntervalType`, `SpatialType`),
  `GeographyType` / `GeometryType` with the vendored SRID→CRS table and Spark's
  `ST_*` refusals, `UserDefinedType` (TYPES-UDT-1 declared refusal on column
  use; the `serialize` / `deserialize` / `_cachedSqlType` / `toInternal` /
  `fromInternal` / `jsonValue` / `__eq__` template follows Spark on a
  subclass), the spatial JSON token helpers, and the shared
  `_struct_to_internal` / `_struct_from_internal` conversion bodies.
  `types.py` imports and re-exports every public
  name. pins: types-bases-1/C-001, C-002, C-003, C-004, C-006
- `table_arg.py` — **DF-SUBQUERY-1 (2026-09-15):** `TableArg`, the value object
  `DataFrame.asTable()` returns, with exactly Spark's public surface
  (`partitionBy` / `orderBy` / `withSinglePartition`) and Spark's ordering guards
  (`orderBy` before a partitioning call refuses; a second partitioning call
  refuses). `select` rejects it `NOT_COLUMN_OR_STR` like any non-column.
  The file also owns the table-argument UDTF execution path
  (`_as_table_arg` / `_map_table_udtf_batches` / `_execute_table_udtf`, split out of
  `udtf.py` at its file-size ceiling): partition keys group input rows into fresh
  handler instances, `orderBy` sorts in-partition ascending nulls-first, and scalar
  call args broadcast as lit-appended columns through `mapInArrow`.
  pins: df-subquery-1/C-005
- `udtf.py` — user-defined table-function validation, registration, scalar literal
  calls, and Arrow expansion. Round 2 adds the additive `_map_arrow_udtf_batches` branch
  (arrow handlers run batch-wise; the plain path is unchanged). **DF-SUBQUERY-1
  (2026-09-15):** `__call__` detects a `TableArg`/DataFrame argument and hands off to
  `table_arg.py`'s execution path, and `try_sql_registered_udtf` parses `TABLE(name)
  [PARTITION BY …] [ORDER BY …] [WITH SINGLE PARTITION]` call arguments so the SQL
  door reaches the same path.
  pins: fnp-misc-1/F-1; df-subquery-1/C-005
- `window.py` — Window and WindowSpec construction, frame bounds, ordering, and
  partition expressions.

## Durable contracts

- DataFrame transformations are lazy until an action. Metadata inspection does not
  execute UDFs or consume rows.
- SQL identifiers and string literals are escaped centrally. Never rebuild those rules
  in a caller.
- Python UDFs run through Arrow batches; user exceptions retain the PySpark exception
  taxonomy and traceback. Unsupported composition fails loudly.
- Cache and temporary-view names are tracked for cleanup. Intermediate engine names never
  appear in user-facing schemas or catalog listings.
- Spark aliases remain identity aliases where promised. Error classes preserve native
  identity and Python multiple-inheritance behavior.
- Optional dependencies fail at the point of use with a classified error. Importing the
  core facade does not require Polars, pandas, or other optional packages.

## Known limitations

- `struct_type_from_arrow` validates its input with `assert`. Optimized Python removes that
  check; a separate behavior change must replace it with a structured runtime error.

## Pointers

- Parent package: [../map.md](../map.md)
- SQL aliases: [sql/map.md](sql/map.md)
- DataFrame implementation: [dataframe/map.md](dataframe/map.md)
- Session implementation: [session/map.md](session/map.md)
- Tests: [../../../tests/map.md](../../../tests/map.md)
- Design: [../../../../../docs/design/python-facade.md](../../../../../docs/design/python-facade.md)
- **FNP-MISC-1 (2026-09-15, on #597):** `functions_byname.py` classifies #597's camel-case aliases against PySpark 4.1.2 `call_function`: `shiftLeft` / `shiftRight` / `shiftRightUnsigned` resolve through Spark's case-insensitive builtin lookup (facade-only routine rows), while `approxCountDistinct` / `toDegrees` / `toRadians` raise `UNRESOLVED_ROUTINE` (non-routine rows).
- **FNP-MISC-1 (2026-09-15, on ARRAY-NULL-1):** `array_append` / `array_prepend` leave `FACADE_ONLY_ROUTINE_NAMES`: since ARRAY-NULL-1 the engine resolves both names itself, so `call_function` reaches them through `_scalar` like any builtin.
- **FNP-11A (2026-09-15, on 440b2773):** `FNP11A_EXPORTS` lists the eleven names new to `__all__`. `make_timestamp` and `months_between` were already exported through the `functions_expr.py` forwarders, and re-installing them duplicated both names in `__all__` and in `catalog.listFunctions()`. `functions_byname.py` follows the measured PySpark 4.1.2 `call_function` answers: `make_timestamp` / `months_between` resolve in the engine, while `timestamp_add` / `timestamp_diff` raise `UNRESOLVED_ROUTINE`.
- **DOOR-CONVERGE-2 (#622, 2026-09-15, orchestrator):** `functions_byname.py` `FACADE_ONLY_ROUTINE_NAMES` drops `split`: the facade dispatch now resolves `split` on the Spark kernel, so it is no longer a measured engine gap (`test_fnp_misc_1_byname_allowlist_covers_facade`; ruling R-16c-11, run 16a told). `F.split` itself stays run 16a's hand-off. pins: door-converge-2/C-005
- **FNP-MATH-1 step 6 (2026-09-16, run 18a, D-8):** `F.split` binds the kernel
  (`str`/`pattern`/`limit`, `str` patterns arrive as `lit` so the display reads
  Spark's bare `split(csvs, ,, -1)`); the door-converge-2 refusal guard flips to
  answer-compare. pins: fnp-math-1/C-008
- **FNP-MATH-1 step 7 (2026-09-16, run 18a, D-9):** `bin` / `rint` drop the facade
  pre-cast that stringified BOOLEAN past the kernel refusal; the Rust coercion
  raises Spark's `DATATYPE_MISMATCH` on both doors. pins: fnp-math-1/C-009
- **DEGREES-RUST-1 by-name drift (2026-09-15, run 16a):** `degrees` / `radians` leave `functions_byname.py`'s `FACADE_ONLY_ROUTINE_NAMES` — once they bind engine scalar UDFs, `call_function` resolves them in the engine, so the derived allowlist no longer lists them as facade-only. pins: fnp-bitmap-facade-1/C-011
- **FNP-11B step 3 (2026-09-15):** `try_to_timestamp` leaves `FACADE_ONLY_ROUTINE_NAMES` for the same reason — the facade dispatch now resolves it on the tolerant-timestamp kernel. pins: fnp-11b/C-007
- **FNP-GEN-1 orchestrator fix-up (2026-09-16, run 17a):** `functions_byname.py` drops `posexplode`
  and `posexplode_outer` from `FACADE_ONLY_ROUTINE_NAMES`. That tuple lists names reachable **only**
  from the facade; this unit's `GeneratorRewrite` makes both answer on the SQL door too, so they are
  no longer facade-only and `test_fnp_misc_1_byname_allowlist_covers_facade` derives them out of the
  set. The full facade suite caught it — the unit's own pin set does not include that census.
  pins: fnp-gen-1/C-005
- **FNP-GEN-1 step 7 (2026-09-16, run 18a):** `functions_byname.py` drops
  `json_tuple` / `from_csv` / `schema_of_csv` from `FACADE_ONLY_ROUTINE_NAMES`
  for the same reason — the dispatch now resolves all three on the Rust kernels.
  The full facade suite caught it again. pins: fnp-gen-1/C-002, C-006
