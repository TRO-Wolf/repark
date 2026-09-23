# map — repark-spark/src

ICE-MIXED-CASE-1 round 3 (2026-09-17, Q-20b-1): `merge_fragments.rs` passes the clause home scope — NOT MATCHED [BY TARGET] fragments resolve bare references against the source alias, NOT MATCHED BY SOURCE against the target alias, MATCHED/ON against both. pins: ice-mixed-case-1/C-004

ICE-MIXED-CASE-1 round 5 (2026-09-17, Q-20b-2): normalization stays ON (`extension.rs` carries no parser switch); the fold and fragment rewrites emit backticked stored-case spellings. pins: ice-mixed-case-1/C-001…C-006

ICE-MIXED-CASE-1 round 21b: `lib.rs` `spark_door_case_insensitive` negates `SparkCaseSensitiveConfig` (the one `spark.sql.caseSensitive` carrier); `spark_ast.rs` and `merge_fragments.rs` pass it to the repark-core fold. `extension.rs` installs only main's carrier. pins: ice-mixed-case-1/C-006

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Router canonicalize reasons restored byte-exact to `6774ebd` (test-pinned; 102-col line kept). spark_literals rule tokens kept. Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs) and re-exports
`install_integer_overflow` (F-Y10-1; AnsiDialect now installs it at session build).
The router body lives in
`router.rs`. DDL, DML, reference, maintenance, metadata, time-travel, and passthrough handlers
share the session and catalog seams. The MoR valve predicate is owned by
`repark_iceberg::write`; `normalize.rs` keeps the resolution wrapper.
Source documentation may retain model provenance; code-quality grade tags stay outside code.
pins: rp-3-fork-repin/C-010
pins: rp-4-fork-repin/C-005, C-006

## Contents

- `lib.rs` — re-exports G15 collation valves and FNP-15/16 `refuse_declared_function_in_*`
  from `repark-functions`, plus `refuse_sql_fragment` for `F.expr` / `filter_sql`.
  pins: fnp-15-16/C-001
- `router.rs` — **ICE-META-DELETE-1 (2026-09-19):** `execute_delete` asks
  `repark_iceberg::write::meta_delete` whether the statement is one of Spark's metadata-only
  deletes AFTER every existing refusal (read-only table, subquery predicate, MoR multi-spec)
  and BEFORE `execute_passthrough`, so a refusal still wins and the decision cannot reach a
  statement the door rejects. It carries the session's case sensitivity
  (`spark_door_case_insensitive`, Spark's `spark.sql.caseSensitive=false` default), which is the
  flag Java's `SparkTable` reads. A false answer falls through unchanged.
  pins: ice-meta-delete-1/C-001, C-006
- [view_ddl/](view_ddl/map.md) — **ICE-VIEWS-1 (2026-09-20):** view grammar,
  execution, and the wrapper-based read path (`parse` / `execute` / `read`;
  `describe.rs` is a PR2 stub).
  pins: ice-views-1/C-007, C-008, C-016
- `router.rs` — **ICE-VIEWS-1 (2026-09-20):** pre-parse CREATE/DROP/SHOW VIEWS
  arms, the DROP VIEW match arm, INSERT/DELETE/UPDATE view write guards, the
  query-only `execute_view_body_query` (no DDL dispatch, so no `Send` cycle),
  and the CREATE VIEW straight-to-`execute_inner` skip that keeps stored bodies
  verbatim past the time-travel/lineage rewrites. `lib.rs` wires `pub mod
  view_ddl`; `namespace_ddl.rs` refuses DROP TABLE over a view and DROP VIEW
  over a table; `insert_by_name.rs` takes the same write guard;
  `describe_show.rs` exposes `tokenize_with_spans` for the view parsers.
  R2 hardens the guard fail-closed (metadata writes refuse up front, `is_view`
  returns a `Result`, branch selectors still fall through).
  pins: ice-views-1/C-006, C-007, C-008, C-011, C-012, C-015, C-016
  **WO-R3 (2026-09-22):** the query-only `execute_view_body_query` and the
  `refuse_insert_into_view` guard move to `view_dispatch.rs` (router.rs
  1025 → 967, under the file-size ceiling); behavior unchanged, callers
  updated by path only. **V-001 (2026-09-22):** the `bare_name_target` bit is
  gone — the facade's bare-name mark qualifies the name only, and the
  tighten refusal on the CREATE VIEW catalog write is unconditional.
- `router.rs` — `execute` / `execute_with_read_only` / `execute_static_overwrite` / `execute_with_statement_options` / `execute_time_travelled` / `execute_inner`
  + pre-parse intercepts (alter I6/I7, write-order DDL, create-namespace, describe/show, ref DDL) + the
  write-to-branch sniff; full router arm set ([router/map.md](router/map.md) for the tests). The MERGE arm delegates to `execute_merge_statement` (OUTPUT refusal, timestamp_ns cast lowering) so `execute_inner` stays under clippy's 100-line cap (run 22b).
  **ICE-CATALOG-SESSION-1 S4 (2026-09-20):** `Statement::{ShowCatalogs, ShowTables,
  ShowColumns}` arms route to `use_ddl`; the `DESCRIBE TABLE` intercept skips the Iceberg
  path when a bare name resolves as a session table, so temp views keep winning.
  pins: ice-catalog-session-1/C-015, C-016, C-017
  **ICE-CATALOG-SESSION-1 S5 (2026-09-20):** the `REFRESH` pre-parse intercept routes to
  `use_ddl::execute_refresh`, beside the extracted `DESCRIBE TABLE` helper.
  pins: ice-catalog-session-1/C-019
  **CAST-MAP-SPELL-1 (2026-09-19):** `execute_inner` first runs
  `repark_functions::cast_map::rewrite_map_casts`, so a `CAST` / `TRY_CAST` naming `MAP<…>`
  reaches every intercept and the parser as the shared cast UDF call.
  pins: cast-map-spell-1/C-005
  **ICE-SYSTEM-FUNCTIONS-1 round 3 (2026-09-20):** `execute_inner` next runs
  `describe_show::rewrite_system_function_calls` (live-catalog-gated), and
  `try_preparse_intercepts` gains the `SHOW [USER] FUNCTIONS IN <cat>.system`
  arm after SHOW NAMESPACES.
  pins: ice-system-functions-1/C-018, C-020
  `execute_time_travelled` is a **release seam, not a routing step** (H-1b): it exists so
  `execute_with_read_only` can own a `time_travel::PinnedViews` and release it on every `?` /
  `return` path of the rewrite — see the `time_travel.rs` row below. **V3-4:** after time
  travel, `prepare_lineage_sql` pins v3 `_row_id` / `_last_updated_sequence_number` onto a
  temp provider for single-table reads (`LineagePins` released with the time-travel views);
  JOIN/CTE/subquery/time-travel naming lineage refuse `V3-ROWID-2`. RP-6: plain-`WHERE`
  UPDATE/DELETE are Spark-equal. V3-7: MERGE keeps `_row_id`; subquery-WHERE DML still
  refuses `V3-COW-1`. **ICE-METADATA-COLS-1 (2026-09-20):** ahead of the lineage rewrite,
  `prepare_metadata_column_sql` pins `_file` / `_pos` / `_spec_id` / `_partition` reads onto a metadata
  temp provider (`MetadataColumnPins` released with the other pins); `_deleted`
  refuses `[ICE-MC-1]`.
  **WO-R3 (2026-09-22):** the pins carry `_partition` too (a NULLABLE union struct); only
  `_deleted` refuses `[ICE-MC-1]`.
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-015, C-016, C-017,
  C-018, C-019, C-020, C-021, C-022, C-023
  SQP-1: the front door canonicalizes escapes once and
  translates downstream parser locations back to the caller's SQL.
  ICE-WRITE-OPTIONS-1 round 4 (2026-09-17, Q-21c-5): every `execute_inner` arm that
  cannot honour a non-empty statement options map refuses it with
  `refuse_if_non_empty` before it executes — one `refuse_options_on_non_write` gate ahead
  of the arm match covers MERGE, DELETE, UPDATE, TRUNCATE, CALL, DROP, ALTER and the
  passthrough arm (it keeps `execute_inner` under clippy's line limit), and each pre-parse
  intercept is gated once its form is recognised, so nothing runs first. `insert_by_name.rs`
  folds its two parse let-else blocks into one pattern (same fallthrough, under the same
  limit after `1485db96` threaded the options through). INSERT, INSERT
  OVERWRITE, CTAS and the BY NAME overwrite delegations honour the map; the
  non-Iceberg INSERT OVERWRITE fallbacks in `insert_overwrite.rs` refuse it.
  pins: ice-write-options-1/C-010
  **ICE-WRITE-OPTIONS-1 (2026-09-17):** the front door threads the out-of-band
  validated set through `execute_time_travelled` into `execute_inner`, and refuses
  non-empty sets on the non-Iceberg arms so options are never silently dropped.
  Round 3 withdrew the text-clause extractor (L-01/L-02): user-typed `OPTIONS(...)`
  keeps main's parse error / CTAS refusal. `execute_time_travelled`
  runs heap-pinned (`Box::pin`) so the thread-through keeps test-task futures
  under the 16 KiB clippy ceiling.
  pins: ice-write-options-1/C-001, C-004
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** `execute_inner` merges the live
  session write conf into every Iceberg write arm's statement options, so
  `spark.sql.iceberg.snapshot-property.*` and the session codec reach the owned
  commits with writer-option precedence.
  **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18, Q-22b-WO-1):** main's
  `execute_routed` folds into `execute_with_statement_options`; ICE-DYN-OVERWRITE-1's
  typed static flag rides as the typed field
  `StatementWriteOptions::force_static_overwrite` (never an option key, so `is_empty` and
  `refuse_if_non_empty` ignore it). `execute_static_overwrite` sets it on an empty set,
  so the `saveAsTable` static pin and the options travel on one statement and the
  router keeps its signatures (clippy's argument and line limits hold).
  pins: ice-write-options-1/C-014
  **IPI-51 PR5 (2026-09-20):** `try_preparse_intercepts` gains the four v2-command
  intercepts — DESCRIBE AS JSON ahead of the describe-table arm, SET SERDE / MSCK REPAIR /
  ANALYZE TABLE after SHOW PARTITIONS — through `v2_json_preparse` / `v2_tail_preparse`
  helpers in the `show_partitions_preparse` shape, each refusing through the shared
  `v2_command_outcome` helper so the write-options gate still runs first. The DESCRIBE
  NAMESPACE arm moves into `describe_namespace_preparse` so the function holds clippy's
  line cap.
  pins: ice-error-conditions-1/C-011
  **IPI-26/27 round 2 (2026-09-20):** `COMMENT ON TABLE` and the two-name Hive
  `CHANGE COLUMN` are true pre-parse intercepts
  ([`router/comment_on_table.rs`](router/comment_on_table.rs),
  [`router/hive_change_column.rs`](router/hive_change_column.rs)), wired after the
  column-move intercept inside `try_preparse_comment_ddl` so the intercept chain
  keeps clippy's line limit (cells `D-COMMENT-ON`, `D-X-CHANGE-COLUMN-TYPE`).
  **IPI-26/27 round 4 (2026-09-21, cell `D-SET-LOCATION`):** `try_alter_intercepts`
  gains the `ALTER TABLE … SET LOCATION '<literal>'` arm after `column_move`
  ([`router/table_props_ddl.rs`](router/table_props_ddl.rs)) — the statement does not
  survive sqlparser, so it claims exactly that shape (keywords case-insensitive, path
  verbatim), refuses a malformed tail loud naming the clause, leaves
  `SET/UNSET TBLPROPERTIES` and branch/tag forms alone, and executes the metadata
  location move through the fork's `SetLocation` update.
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); MATCHED / NOT MATCHED / NOT MATCHED BY SOURCE (DML-A);
  in-module tests (MG-2: M2 Oracle sub-predicates, M3
  assignment-target qualification, M8 INSERT column list, M10 non-last
  unconditional clause). pins: dml-a-merge-not-matched-by-source/C-005
- `merge_fragments.rs` — **ICE-MIXED-CASE-1 (2026-09-17):** MERGE fragment
  preprocessing for case-insensitive resolution (target/source scope read,
  `ON` / predicate / value fragment rewrite, `maybe_` dispatcher that stamps
  the carrier flag onto the spec). **Round 21b step 7 (R-03):** target field
  names come from the Iceberg metadata (`catalog.load_table` →
  `current_schema()`), and a named source reads its `TableProvider` schema; only
  a subquery source (`USING (SELECT …) AS s`) still plans one `SELECT * … LIMIT
  0`, because no metadata exists for it. `merge.rs` resolves the catalog handle
  before the fragment rewrite. pins: ice-mixed-case-1/C-004, C-018
- `insert_overwrite.rs` — **ICE-SESSION-WRITE-CONF-1 round 4 (2026-09-20):** the
  `OverwritePlan::RowFilter` arm hands the whole `StaticPartitionOverwrite` to
  `commit_overwrite_by_row_filter_with_summary`, so the commit resolves the files the
  filter removes and the collision oracle answers as Spark's does.
  pins: ice-session-write-conf-1/C-054
- `insert_overwrite.rs` — INSERT OVERWRITE: empty probe/validate/provider-wipe (C1-Q-001) +
  non-empty stage-then-swap; **DML-B** `PARTITION (…)` static/dynamic via
  `repark_iceberg::write::partition_overwrite`; 2 in-module tests (`assignment_type_unit_tests`).
  Named-ref targets go through `commit_overwrite_replace_all_to` / partition `_to`.
  Empty overwrite onto a branch wipes via `commit_overwrite_replace_all_to`, not a 4-part
  self-scan. **ICE-DYN-OVERWRITE-1 (2026-09-17):** PARTITION-less overwrite computes
  `dynamic` once in `execute_insert_overwrite` (session conf, bypassed by the
  `force_static_overwrite` flag the `execute_static_overwrite` router entry carries
  (comment-free per the owner's comment ban, 2026-09-17: the entry carries
  `#[allow(clippy::missing_errors_doc)]`);
  round 2, ruling Q-20a-6 removed the in-band marker) and threads it through
  `from_staged_source` into the stage-then-commit — dynamic commits
  `commit_replace_partitions_to` on partitioned tables (the empty arm returns
  before any commit), static and unpartitioned-dynamic keep replace-all.
  **Round 3 (2026-09-17):** the mode decision is one function,
  `overwrite_is_dynamic(ctx, force_static_overwrite)`, called here and by
  `insert_by_name.rs` — no second conf read.
  **ICE-OVERWRITE-MODE-1 (2026-09-19):** the flag is gone; `overwrite_is_dynamic(ctx,
  options)` and `execute_partition_overwrite` both read
  `StatementWriteOptions::overwrite_mode(ctx)` (session mode, the typed
  `overwrite_intent`, the `overwrite-mode` writer option) and ask
  `repark_iceberg::write::plan_overwrite` for the scope. `PARTITION (…)` now answers
  Spark in both modes: static mode with no static value replaces the whole table
  (`commit_overwrite_replace_all_with_summary`; an empty source wipes, zero-record staged
  files dropped), static values filter by row (`overwrite_by_row_filter`), dynamic mode
  replaces the source partitions with the static values injected (mixed lists stage
  through `stage_static_partition_overwrite_files_with`). Pins:
  [tests/overwrite_mode.rs](tests/overwrite_mode.rs).
  pins: ice-overwrite-mode-1/C-002, C-003, C-004, C-005, C-006, C-007
  **ICE-WRITE-OPTIONS-1 (2026-09-17):** `execute_append_with_options` (option-carrying
  plain INSERT stages on the owned path with the merged summary); the overwrite
  family threads `StatementWriteOptions` through staging (option-free arms keep the
  canonical staging byte-identical) into the `*_with_summary` commits. The append
  executor refuses table-function targets, `REPLACE INTO`, explicit column lists, and
  non-3-part names loudly instead of mis-staging them.
  **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18):** the options map and the typed
  `dynamic` answer ride together. A dynamic PARTITION-less overwrite on a partitioned
  table commits `commit_replace_partitions_with_summary` (the `replace_partitions`
  action `commit_replace_partitions_to` uses, plus the merged summary and isolation
  override), so snapshot properties and writer knobs are honoured (Q-22b-WO-2). An
  empty dynamic source returns before any commit, as Spark's `DynamicOverwrite` does;
  with no snapshot there is nothing to stamp, so the options are validated and not
  refused (Q-22b-WO-3). The static PARTITION arm calls
  `stage_static_partition_overwrite_files_with` with the column list and `None` or the
  staging overrides.
  pins: ice-write-options-1/C-014, C-015
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the stage-then-swap overwrite arms
  resolve the merged session write at their commits.
  pins: dml-b-insert-overwrite/C-001, C-002, C-004
  pins: rp-5-fork-repin/C-004
  pins: ice-dyn-overwrite-1/C-014, C-020
  ICE-V3-WRITE-DEFAULT-1 round 5 (2026-09-17): both PARTITION arms fill omitted
  write-defaults through `insert_defaults::overwrite_source_with_defaults` and pass
  the column list into staging (dynamic and static). pins: ice-v3-write-default-1/C-015
  `rewrite_overwrite_default_markers` substitutes `DEFAULT` value markers before any
  overwrite arm runs, so `INSERT OVERWRITE t VALUES (…, DEFAULT)` fills as `INSERT INTO`
  does (ruling Q-21b-4). pins: ice-v3-write-default-1/C-016
  Run 21b round 2 (2026-09-18): the marker pass and its rewritten `(sql, Insert)` pair are
  boxed (`Box::pin` on the call, `Box` on the result) and the passthrough's `DEFAULT`
  marker and fill awaits in `spark_ast.rs` are boxed with their preloaded `Table`, so
  every `execute` future stays under clippy's `large_futures` 16 KiB threshold (the
  round-1 inline awaits grew it to 16,384–16,544 bytes and tripped 135 test call sites).
  pins: ice-v3-write-default-1/C-024
- `append_with_options.rs` — **ICE-SESSION-WRITE-CONF-1 round 8 (2026-09-20):** the
  no-write-options arm is the append that stands in for the fork's DataFusion insert exec, so
  it alone sets `WriterStagingOverrides::fork_insert_dictionary_rule` and writes that exec's
  Parquet layout; the option-carrying arm keeps Java's dictionary default.
  pins: ice-session-write-conf-1/C-064
- `append_with_options.rs` — **ICE-SESSION-WRITE-CONF-1 round 4 (2026-09-20):** the owned
  append plans the WHOLE insert, not a reconstructed `SELECT`. `spark_ast::execute_insert_source`
  runs the passthrough pipeline (marker rewrite, eager analysis, `fill_insert_plan`, the
  timestamp-ns passes) and executes the resulting `Dml` node's INPUT, so the owned route carries
  the target-schema coercion the delegated route has: a VALUES list widens against the target
  column, a compound NULL keeps its type, and `DEFAULT` in an outer select still refuses with
  Spark's `UNRESOLVED_COLUMN … 42703`. The batch that reaches staging is already the table's
  shape, so the column list is empty and `positional_map_overwrite_batch` only checks it. A
  branch target is planned against the base table (`insert_sql_without_write_ref` drops the
  write ref), because DataFusion cannot resolve a 4-part name — the commit still goes to the ref.
  pins: ice-session-write-conf-1/C-055
- `append_with_options.rs` — **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18):**
  `execute_append_with_options` (option-carrying plain INSERT on the owned
  stage-then-commit path), moved verbatim out of `insert_overwrite.rs`, which the merge
  with ICE-DYN-OVERWRITE-1 and ICE-V3-WRITE-DEFAULT-1 would take past the 1000-line
  ceiling. **Q-22b-WO-5:** since ICE-V3-WRITE-DEFAULT-1 the DataFrame writers emit
  `INSERT INTO t (cols) SELECT …`, so an explicit column list is honoured instead of
  refused: it goes through the same `overwrite_source_with_default_fills` step as the
  overwrite arms (omitted columns fill from `write_default`), `stage_overwrite_files_with`
  maps the source by name, and `commit_append_with_summary` commits it with the merged
  summary. A list-free append keeps `append_with_statement_options`. Table-function
  targets, `REPLACE INTO` and non-3-part names still refuse.
  pins: ice-write-options-1/C-014, C-018
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the option-carrying append resolves
  the merged session write at its commit. **Round 1 fix (Q-24c-5):** an
  option-less list-free INSERT stages positionally (`stage_overwrite_files_with`
  with an empty column list) instead of the by-name append, so positional
  `VALUES` resolve over all columns as Spark's do; the by-name arm stays for
  the option-carrying form.
  **IPI-41 WO1 (2026-09-22):** the `write-format` / `delete-format` options are
  copied into staging here, so the option door reaches `resolve_data_format`.
  pins: ice-orc-avro-1/C-005, C-006
- `insert_by_name.rs` — `INSERT … BY NAME` (ICE-RTAS-BYNAME-1, 2026-09-17): the token-level
  strip (sqlparser has no `BY NAME`), the count-first Spark error rule, the positional
  projection build, the staged-append executor (stream → conform → `commit_append_to` →
  reregister) and the overwrite delegation to `insert_overwrite_from_staged_source`. Branch
  targets count as owned write heads (`write_to_branch.rs`), so no temp-view rewrite fires.
  In-module tests (file-backed in [insert_by_name/map.md](insert_by_name/map.md)).
  ICE-WRITE-OPTIONS-1 (2026-09-17): the statement write options travel into the two
  delegating overwrite calls, which honour them; the empty-projection commit and the
  by-name append commit without them, so both refuse a non-empty options set rather than
  dropping it silently.
  pins: ice-rtas-byname-1/C-001, C-002, C-003, C-004; ice-write-options-1/C-006
  **Round 2 (2026-09-17):** `PARTITION` shapes delegate to the positional
  partition arm (static overwrite) or inject clause literals (static append);
  a PARTITION-less `BY NAME` overwrite follows `partitionOverwriteMode` (see round 3);
  missing required targets
  refuse `CANNOT_FIND_DATA`; matching honours `spark.sql.caseSensitive`
  (matching plus projection live in `plan_name_projection`).
  pins: ice-rtas-byname-1/C-007, C-008, C-009, C-010
  **ICE-OVERWRITE-MODE-1 round 2 (2026-09-19):** the mixed-list refusal in
  `static_partition_columns` is gone — `PARTITION (k='v', k2) BY NAME` projects the
  non-static columns by name and delegates to `execute_partition_overwrite`, so it follows
  the same plan as the positional form in both modes. pins: ice-overwrite-mode-1/C-012
  **ICE-DYN-OVERWRITE-1 round 3 (2026-09-17):** `execute_insert_by_name` takes the
  router's `force_static_overwrite` and asks `insert_overwrite::overwrite_is_dynamic`
  once. A non-empty PARTITION-less `BY NAME` overwrite passes that answer into
  `insert_overwrite_from_staged_source` (dynamic replaces only the touched partitions
  on a partitioned table; static and unpartitioned replace the whole table). An empty
  source still runs the assignment-type check, then under dynamic returns with no
  commit and no snapshot (Spark skips the commit, partitioned or not), and under
  static wipes through `wipe_by_name_target` (`commit_overwrite_replace_all_to` plus
  reregister; split out to keep `execute_insert_by_name` under clippy's line limit). Explicit column lists never
  reach this module; they take `execute_insert_overwrite`, which reads the same function.
  pins: ice-dyn-overwrite-1/C-019, C-020, C-021, C-023
  **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):** the overwrite stage-then-swap fills
  omitted columns from `write_default` before staging, so `INSERT OVERWRITE`
  matches Spark's measured answer. The fill helper carries no doc comment per the
  no-code-comments ruling.
  pins: ice-v3-write-default-1/C-007
  **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18):** the static flag is read from
  `write_options.force_static_overwrite` (ICE-OVERWRITE-MODE-1, 2026-09-19: now
  `overwrite_is_dynamic(ctx, write_options)`; `static_partition_columns` validates the clause
  through `validated_static_equalities` and keeps refusing a mixed `BY NAME` list).
  Only the static empty-projection wipe refuses a non-empty map
  (it commits through `wipe_by_name_target` without a summary); the dynamic empty case
  commits nothing, and the non-empty dynamic case honours the map through
  `insert_overwrite_from_staged_source`.
  pins: ice-write-options-1/C-015
  pins: ice-write-options-1/C-001, C-003
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the by-name append commit resolves
  the merged session write.
- `insert_arity.rs` — **IPI-51 PR9 (2026-09-21):** the short-VALUES arity router
  intercept. `refuse_if_short_values` refuses a positional `INSERT INTO t VALUES (…)` whose
  VALUES width is strictly below the Iceberg target's field count with
  `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` / `21S01` through the
  `repark-common` catalogue as `DataFusionError::Plan`, so DataFusion never sees the short
  list. Anything else — overwrite, column list, partition clause, non-VALUES source,
  mixed-length rows, equal-or-wider VALUES, a missing table — falls through untouched.
  Pins: [tests/insert_arity.rs](tests/insert_arity.rs).
  pins: ice-error-conditions-1/C-011
- `write_options.rs` — **ICE-WRITE-OPTIONS-1 (2026-09-17):** last-wins validation of
  the out-of-band option pairs (snapshot-property strip-and-lowercase, parquet
  honour, orc/avro/bogus refusals, option-over-table-property
  numerics/codec/isolation, lenient booleans, Spark-shaped refusal texts),
  in-module units. Round 3 withdrew the text-clause recognizer (L-01 smuggling,
  L-02 UTF-8); the SQL text stays option-free. No inline comments per the owner
  ban (round-2 purge 2026-09-17 removed the `# Errors` sections too); rationale
  lives here and in the ledger.
  pins: ice-write-options-1/C-001, C-002, C-003, C-004
  Run 22b (2026-09-18, Q-22b-WO-1): the struct also carries
  `force_static_overwrite`, the `saveAsTable` static pin, set by the dialect from
  `EngineContext`; it is not an option and never counts toward `is_empty`.
  pins: ice-write-options-1/C-014
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** `StatementWriteOptions` also merges
  the live session write conf (session codec plus snapshot properties), which the
  router folds into every Iceberg write arm.
  **ICE-OVERWRITE-MODE-1 (2026-09-19):** the flag becomes `overwrite_intent`
  (`OverwriteIntent::Session` / `Static` / `Dynamic`), and the `overwrite-mode` key
  (lower-cased like every key) sets `overwrite_mode_dynamic` when its value is `dynamic` in
  any case — Iceberg's `SparkWriteConf` reading; other values are ignored like Spark.
  `overwrite_mode(ctx)` assembles the decision input. pins: ice-overwrite-mode-1/C-006
  **IPI-41 WO3a (2026-09-22):** `validate_write_format` accepts orc/avro
  case-insensitively (parquet-identical normalisation); only unknown names refuse
  with `Invalid file format`. pins: ice-orc-avro-1/C-021
- `truncate.rs` — whole-table `TRUNCATE TABLE` (DML-C): delete-only `commit_truncate_to`;
  PARTITION / IF EXISTS / missing TABLE / multi-target refuse. Pins:
  [tests/truncate.rs](tests/truncate.rs). pins: dml-c-truncate/C-002, C-005, C-006, C-007
  pins: rp-5-fork-repin/C-004
- `update_cast.rs` — **IPI-51 PR10 (2026-09-22):** `execute_update` calls
  `refuse_incompatible_update_cast` after the read-only, subquery-predicate, and MoR
  refusals and before `execute_passthrough`: bare-column SET targets resolve on the
  Iceberg schema, each SET value plans as `SELECT (<expr>) FROM <table>`, and the
  first non-ANSI-store-assignable pair stamps
  `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]`/`KD000` as `Plan`. Missing
  catalog/table, unresolvable names, and failed probes fall through. Pins:
  [tests/update_cast.rs](tests/update_cast.rs). pins: ipi-51/W-UPDATE-TYPE-ERR
- `write_to_branch.rs` — Spark-door write-to-branch routing: tag/missing-branch Spark-shaped
  refuse; two-part names qualify through session defaults; the MOR valve runs on the
  Iceberg ident before the temp rewrite; fork-executed INSERT/UPDATE/DELETE via
  `IcebergTableProvider::with_commit_branch` registered on `datafusion.public`;
  MERGE / INSERT OVERWRITE / TRUNCATE rewrite short names to four-part then `.to_branch`.
  `split_write_ref_parts` sniffs four-part names and two-part `branch_`/`tag_` names;
  a three-part table whose last segment starts with `branch_` is an ordinary table.
  pins: rp-5-fork-repin/C-004
  **ICE-WAP-BRANCH-1 (2026-09-19):** one resolver decides the effective write branch.
  `apply_write_to_branch` now also runs when only `spark.wap.branch` is set: it reads the
  target span (one part is enough, qualified through the session defaults), and an explicit
  `t.branch_b` selector outranks the conf. Without a selector, `wap_branch_target` loads the
  target and takes the conf's branch only when the table carries
  `write.wap.enabled=true` — refusing first when `spark.wap.id` is set too. A wap branch that
  does not exist is NOT pre-checked (`require_existing_branch` is false): the commit creates
  the ref from main, as Java's `SnapshotProducer` does. An explicit selector keeps the
  `Cannot use branch (does not exist)` refusal.
  **Round 2 (2026-09-20, WAP-002):** an append created the missing ref, but `DELETE` and
  `UPDATE` scan through the fork's `resolve_scan_snapshot_id`, which refuses
  `snapshot ref 'audit' not found` before the commit. `create_wap_branch_from_main` now runs on
  the wap path (and only there — `require_existing_branch` is false) whenever the conf named a
  branch the table does not carry: it creates the ref at the table's current snapshot, so the
  scan resolves and the write commits on the branch, which is what Spark answers for every DML
  family (measured: `QW-DELETE-NO-BRANCH`, `QW-UPDATE-NO-BRANCH`, `QW-DELETE-MOR-NO-BRANCH`,
  `QW-MERGE-NO-BRANCH`). An empty table with no current snapshot keeps the old path — there is
  no snapshot to branch from and the append creates the ref itself.
  **IPI-05 (2026-09-21):** `spark.wap.id` alone takes a route of its own. `apply_write_to_branch`
  runs when either WAP key is set; with no selector and no wap branch, `wap_id_route_target` loads
  the target, takes the id only for a `write.wap.enabled=true` table, and hands
  `commit_write_staged` a plain target table registered with `with_stage_only(true)` and
  `with_snapshot_properties({"wap.id": id})` — no `BranchTarget`, because there is no branch to
  name. Neither key set, or the id set on a table without the property: the SQL is returned
  borrowed, byte-identical. R5 (2026-09-21): the session-conf path stages too — with the id
  set and no explicit branch, a plain INSERT carrying only session snapshot properties
  stages a snapshot stamped `wap.id` instead of committing on main.
  pins: ice-wap-branch-1/C-002, C-004, C-005, C-012
- `wap.rs` — **IPI-05 (2026-09-21):** `wap_id_for_table` is the read half for the id, the twin of
  `wap_branch_for_table`: it answers the conf's `spark.wap.id` only for a `write.wap.enabled=true`
  table and refuses both keys together first, and `WAP_ID_SNAPSHOT_PROPERTY` (`wap.id`) is the one
  spelling of the summary key the staged route stamps.
  **ICE-WAP-BRANCH-1 (2026-09-19):** the `spark.wap.*` session carrier and the
  read half of the resolver. `WapSessionConfig` is a DataFusion `ConfigExtension`
  (`repark.wap`) holding `spark.wap.branch` and `spark.wap.id`; the Spark extension installs
  it from the builder conf map and the binding's `set_runtime_config` writes the live one, so
  the decision is Rust's and Python only forwards the string (an empty value clears the key).
  `apply_wap_read_redirect` runs before the time-travel pass: with the conf set, every
  relation in the statement naming a `write.wap.enabled=true` table is pinned to the branch's
  snapshot (falling back to the table's current snapshot when the ref does not exist yet) and
  spliced to an ephemeral provider, so the session's plain reads follow the branch while the
  conf is set. **Round 2 (2026-09-20, WAP-001):** *every* relation means every one —
  `find_read_relations` is a parenthesis-depth state machine, not a scan for FROM/JOIN/USING.
  Each parenthesis carries its own relation list, so a subquery, a CTE body, an IN/EXISTS body
  and each arm of a set operation get their own; FROM/JOIN/USING opens the list at the current
  depth, a comma continues it, and a list-ending keyword (`RELATION_LIST_ENDS` — WHERE, GROUP,
  ORDER, SELECT, SET, WHEN, LATERAL, the set operators …) closes it, so a select-list, GROUP BY
  or alias-list comma is never read as a relation. Before it, only the first name after
  FROM/JOIN/USING was redirected and `FROM t a, t b` silently mixed the audit branch with
  `main`. The skip rules below apply per relation, inside a comma list too. Skipped: a relation
  that carries its own `VERSION/TIMESTAMP AS OF` clause, a
  `branch_`/`tag_` selector, a metadata-table path (including the `table$suffix` word the
  metadata rewrite emits, which re-tokenizes as a `$`-placeholder glued to the base name), and
  the statement's own write target. `both_wap_keys_message` carries Java's exact
  `Cannot set both WAP ID and branch, but got ID [id] and branch [branch]`, raised as an
  `IllegalArgumentException` through `repark_core::illegal_argument_error`.
  `spark.wap.id` on its own takes the staged route recorded under IPI-05 above. `set_value` carries `#[allow(clippy::missing_errors_doc)]` — the
  sanctioned form for the pedantic lint under the comment ban.
  pins: ice-wap-branch-1/C-001, C-003, C-006, C-007, C-010, C-011
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff. Its 14 in-module tests are file-backed in
  [ref_ddl/map.md](ref_ddl/map.md); the module path, and so every pin name, is unchanged.
  `parse_if_not_exists` / `parse_if_exists` take a token index and answer `(matched, next_index)`,
  so `finish_create`, `finish_drop`, `parse_create_with_in` and `parse_drop_with_in` all consume
  the guard at the same position; `refuse_guard_after_replace` is the parse-class refusal for the
  combination Spark's grammar does not admit. The `RefOp::Create.if_not_exists` and
  `RefOp::Drop.if_exists` flags are read in `execute_ref_ddl` against
  `TableMetadata::snapshot_for_ref`, never as a replace — a guarded `DROP` on a missing ref
  reads absent and no-ops rather than raising.
  `IF NOT EXISTS` / `IF EXISTS` are optional **infixes** between `BRANCH|TAG` and the ref name —
  Spark's token position, not a trailing clause — shared by the `ALTER TABLE` and the
  `… IN cat.ns.t` spellings. Both guards are conditional: a missing ref is still created, a
  present ref is still dropped, and an existing ref is left where it is (an `AS OF VERSION` on
  the guarded form is ignored, as Spark ignores it). Spark's grammar attaches `IF NOT EXISTS` to
  the plain `CREATE` alone, so `CREATE OR REPLACE … IF NOT EXISTS` and `REPLACE … IF NOT EXISTS`
  refuse parse-class rather than being accepted and silently given one meaning
  (**IPI-42**, 2026-09-20; registry `REF-2` retired). The create arm's
  create-vs-or-replace dispatch sits in `execute_create_ref` so `execute_ref_ddl`
  stays under clippy's 100-line cap (repark#751 CI).
  pins: ipi-21-25-42-small-parser/C-001, C-002, C-003, C-004
  `WITH SNAPSHOT RETENTION` takes BOTH halves — `n SNAPSHOTS` then an optional
  `k DAYS|HOURS|MINUTES` — because Spark's grammar does; the reversed order is a Spark parse
  error and refuses here too. Write-to-branch routing lives in `write_to_branch.rs` (RP-5):
  the sniff still locates the statement's ONE write target. Registry rows: `REF-1` FIXED,
  `REF-3` BACKLOG, `REF-4` FIXED.
  pins: ref-branch-tag-wap/C-003, C-004, C-006, C-007
  pins: rp-5-fork-repin/C-004
- **ICE-CHANGELOG-1 (2026-09-20):** `time_travel.rs` declares `pub mod changes;` —
  `time_travel/changes.rs` rewrites `FROM <cat>.<ns>.<t>.changes` onto a
  `ChangelogTableProvider` temp view, hooked in `router.rs` right after the metadata-table
  rewrite and before the branch/WAP redirects, releasing through the same `PinnedViews`.
  `changes` is deliberately NOT a metadata table: a metadata table is snapshot-scoped table
  METADATA reached through the fork's `table$suffix` spelling, while `t.changes` is table DATA
  over a snapshot RANGE. `metadata_tables.rs` is untouched (it holds an exact 1059-line
  baseline). pins: ice-changelog-1/C-009
- **ICE-CHANGELOG-1 (2026-09-20):** `call.rs` gains `create_changelog_view` in
  `SUPPORTED_PROCEDURES` and the dispatch; `call/create_changelog_view.rs` parses the six Java
  parameters (`identifier_columns => array(…)` through `call_args::expr_as_string_array`),
  applies Java's `shouldComputeUpdateImages` default (an identifier list with no
  `compute_updates` still pairs), refuses `net_changes` beside update images with Java's exact
  text, falls back to the table's identifier fields, and registers the LAZY
  `ChangelogViewProvider` under the view name — returning the name Java returns, backticked when
  defaulted. pins: ice-changelog-1/C-013, C-014
- **ICE-CHANGELOG-1 (2026-09-20):** `call.rs` declares `mod changelog;` — the changelog row
  transforms live under `call/` because they are `create_changelog_view`'s, and because
  `src/lib.rs` holds a 150-line ceiling. pins: ice-changelog-1/C-011, C-012, C-013
- `call.rs` — **IPI-05 (2026-09-21):** `publish_changes` joins `SUPPORTED_PROCEDURES` (in
  alphabetical place) with a dispatch arm into `branch_ops::execute_publish_changes`, so the WAP
  publish is a procedure rather than one of the names the unknown-procedure refusal lists.
- `call.rs` — twenty-one maintenance procedures: twenty maintenance calls plus `register_table`
  (**ICE-PROCS-ROUTE-1 (2026-09-19):** `ancestors_of`, `compute_table_stats`,
  `compute_partition_stats`, `rewrite_table_path` route through `call/` bodies over the
  fork's maintenance actions; the shared `illegal_argument` helper maps
  procedure-layer validations to `IllegalArgumentException`). Each
  preserves Spark's result schema and count sources. Orphan removal takes Spark's defaults
  (a bare call deletes with `older_than` at now minus 3 days), accepts Spark's optional
  arguments except `file_list_view` — `location` reads named or positionally at index 2
  through `call_args::CallArgs::optional_string_at`, matching Spark's parameter order —
  and refuses shared fallback roots; on a `ServiceManagedLocation`
  catalog (the `s3tables` kind) it refuses before any IO — table buckets answer
  `ListObjectsV2` 405 — naming the service's `unreferencedFileRemoval` maintenance as the
  remedy (**ORPHAN-S3TABLES-1, 2026-09-12**); rewrite-position-delete returns Spark's
  four zeros on a DV-only table and converts admitted parquet deletes to one PUFFIN per data
  file (`B-MOR-3` FIXED 2026-09-03; `B-MOR-3-FLOOR-1` FIXED 2026-09-04 (RP-11));
  rewrite-data-files honors v2 `where` file-selection, refuses
  sort/`sort_order` (`RDF-SORT-1`), answers the `options` map (registry
  `ICE-RDF-OPTIONS-1` round 3: signed sizes, IAE-first RPD order, NULL-key precedence),
  and on v3 drops in-scope DVs (`V3-DANGLE-1`
  FIXED). rewrite-position-delete answers its measured options subset and refuses its
  unwired keys loud. **ICE-PROCEDURES-1 PR1b (2026-09-21):** its `where` wires through
  `call/rewrite_where.rs` into the fork's `RewritePositionDeleteFiles::filter`, and
  `expire_snapshots` binds against its declared list with `snapshot_ids` expiring each id
  in array order while `max_concurrent_deletes`, `stream_results` and
  `clean_expired_metadata` parse and stay ignored, and `add_files` routes through
  `call/add_files.rs` ([call/map.md](call/map.md)). **ICE-PROCEDURES-1 PR2a
  (2026-09-21):** `rewrite_data_files` `branch` threads into the fork's
  `RewriteDataFiles::branch` and commits only that ref; `branch` beside
  `remove-dangling-deletes` refuses loud, and an unknown ref passes the fork text
  through.
  pins: ice-procedures-1/C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-021
  **MAINT-POLICY-1 steps 2–3 (2026-09-10):** `run_maintenance` plans the
  five D-4 steps over the stamped `[<profile>.maintenance]` policy plus inline overrides,
  and `dry_run => false` applies them step by step (`ran` / `failed` / `skipped`).
  **AP-1 step 1 (2026-09-10):** `plan_partitioning` scores the P-2 candidates with exactly
  P-3 over the `files` metadata table and answers the D-1 plan frame (the pure engine lives
  in `call/plan_partitioning_score.rs`). **AP-1 step 2 (2026-09-11):**
  `projected_files_at_target` derives from post-rewrite bytes via the parquet-footer
  `byte_ratio` in `call/plan_partitioning_bytes.rs` (0.55 fallback), reported per row in
  `notes`. **AP-2 step 1 (2026-09-11):** `apply_partitioning` re-derives the plan id at the
  current snapshot and applies the matching candidate (`dry_run` default true; `false`
  executes one commit per step). Body: `call/apply_partitioning.rs`.
  Details and test pointers:
  [call/map.md](call/map.md).
  pins: v3-5-dv-compaction/C-002, C-003, C-006
  pins: maint-rewrite-data-files-options/C-003, C-004, C-008
  pins: b-mor-3-rewrite-position-deletes-v3/C-002, C-003, C-004
  pins: rp-11-repin-f24/C-002
  pins: maint-policy-1/C-007, C-008, C-009, C-010, C-011, C-012
  pins: ap-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
  pins: ap-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
  pins: orphan-s3tables-1/C-001, C-002, C-004, C-005
- [call/branch_ops.rs](call/branch_ops.rs) — **ICE-BRANCH-OPS-1 (2026-09-17):** `fast_forward`,
  `cherrypick_snapshot`, `set_current_snapshot`, `rollback_to_timestamp` (Spark 4.1.2
  parity, oracle-pinned in `python/repark/tests/branch_ops_1_truth.json`).
  Details: [call/map.md](call/map.md).
  pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-007, C-010
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
  **D-5 (2026-09-21):** `refuse_unsupported_create_table_clauses`' options arm
  narrows to `Plain | With` — the `Options` variant leaves the refusal (A-1; the
  rewrite carries iceberg `OPTIONS` clauses as `TBLPROPERTIES` before the parse)
  — and the message text no longer names `OPTIONS`.
  **ICE-WRITE-OPTIONS-1 (2026-09-17):** option-carrying CTAS stages with overrides,
  then publishes once (materialize plus summary on a fork `Transaction`,
  `publish_create_table` / `publish_replace_table`; one snapshot); the tail lives
  in `finish_ctas_staged_commit` so `execute_ctas` keeps the function
  length ceiling. Round 3 withdrew the empty-publish-then-append double commit.
  **ICE-CATALOG-SESSION-1 S6 (2026-09-20):** all three creation sites merge user
  properties through the catalog side map (the service-managed site takes
  `catalogs` for it).
  pins: ice-catalog-session-1/C-027
  pins: ice-write-options-1/C-001, C-003
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the staged commit resolves the
  merged session write, so CTAS stamps session snapshot properties.
  **ICE-MERGE-APPEND-1 (2026-09-19):** the staged-table append commits through
  `merge_append()`, not `fast_append()`. Spark's CTAS writes through `StagedSparkTable`,
  whose table is a `BaseTransaction$TransactionTable`; its `newAppend()` forwards to
  `BaseTransaction.newAppend()`, which the 1.11.0 bytecode shows constructing
  `org.apache.iceberg.MergeAppend` — a CTAS append is a merging append too. The
  `replace_write` arm keeps `overwrite_files` (Java `newOverwrite`).
  pins: ice-merge-append-1/C-001
  **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** the service-managed abort arm skips `drop_table`
  and returns the original error unwrapped when `is_commit_state_unknown` fires — a
  possibly-landed create is never abort-dropped, and the class + `operation_id` reach the
  caller; definite kinds keep the drop-and-explain abort.
  pins: ice-commit-unknown-1/C-001, C-003, C-004
  **IPI-26/27 round 2 (2026-09-20):** the extracted table `COMMENT` lands as the
  `comment` property and the extracted `LOCATION` overrides the namespace-derived
  location in `CreatePlan`, `TableCreation`, and the scheme-selected `FileIO`, so
  CTAS data files land under the given path. Service-managed catalogs and
  `OR REPLACE` refuse a custom location loudly (`check_custom_location`; cells
  `D-CTAS-COMMENT`, `D-CTAS-LOCATION`).
  **CTAS-VIEW-1 (2026-09-03):** unpartitioned `write_ctas_stream` inherits stream conforming
  from `write_data_files_from_stream_with_concurrency` (Utf8View/BinaryView → table schema).
  pins: ctas-view-1-conform-stream/C-001, C-002
  **PERF-ICE-WRITEPATH-1 (2026-09-05):** `write_ctas_stream` is now `write_ctas_query` — it hands
  the SELECT's physical plan to
  [`write/partition_write.rs`](../../repark-iceberg/src/write/map.md), so each DataFusion
  partition writes its own data files instead of one coalesced stream feeding cooperative
  writers. The conform inheritance above is unchanged: the node calls the same stream writer per
  partition. The task context is the frame's own, so the node executes
  under the state that planned it. `write_ctas_query` keeps `write_ctas_stream`'s doc comment verbatim: a renamed
  function carries its pre-existing comment unchanged.
  **V3-2:** `format-version` is consumed at parse and resolved at execute against
  `repark.sql.allowCreateFormatVersion3` (same helper as column-def CREATE).
  **SE-1 PR-D1:** refuses Iceberg CREATE when any `TableScan` source (including
  expression subqueries, R-B) is tighten-derived AND the output has a
  non-nullable field (R-D), or the output schema still carries the tag. The
  write-boundary relax is PR-D2 (via the same source walk).
  **CUTOVER-SCHEMA-1 (2026-09-04):** the derived Arrow schema relaxes to all-nullable
  before Iceberg conversion, so CTAS stores every column optional the way Spark does —
  including provably non-null `SELECT coalesce(x, 0)` outputs. The SE-1 refusal checks
  run first on the un-relaxed schema and still fire; only the derived table schema
  relaxes, never the written batches.
  pins: cutover-schema-1/C-002
  **ICE-RTAS-OPS-2 (2026-09-18):** both staged branches set
  `StagedTableTransaction::with_replace_write(ctas.or_replace)` — the
  `begin_replace` arm and the `begin_create` arm — so an RTAS commits `overwrite`
  (or `delete` when the SELECT is empty) while plain CTAS keeps `append`. The
  service-managed create-first arm never reaches the staged type; **round 2** gave
  `execute_ctas_service_managed` the same answer through
  `repark_iceberg::write::commit_replace_write` when `ctas.or_replace` (new-table
  RTAS → `overwrite`, empty → `delete`); plain service-managed CTAS keeps `commit_append`.
  pins: ice-rtas-ops-2/C-018, C-019
  The two opt-in lines took `execute_ctas` past clippy's `too_many_lines`, so it
  carries the repository's `#[allow(clippy::too_many_lines)]` like 23 other sites.
  pins: ice-rtas-ops-2/C-001, C-002, C-004
  **ICE-WRITE-OPTIONS-1 run 22b (2026-09-18, Q-22b-WO-4):** an option-carrying RTAS keeps
  ICE-RTAS-OPS-2's operation. `finish_ctas_staged_commit` takes `replace_write` and stages
  `overwrite_files().overwrite_by_row_filter(AlwaysTrue).allow_empty_commit()` with the
  merged summary (collision rule against `EngineSummary::for_overwrite`) instead of
  `fast_append`; the service-managed arm calls `commit_replace_write_with_summary`.
  Plain CTAS with options keeps the append summary.
  pins: ice-write-options-1/C-016
- `write_to_branch.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):** when the session
  write conf is set, a plain `INSERT` / `DELETE` / `UPDATE` on `<table>.branch_<name>` counts
  as an owned write head, so the statement keeps its ref-qualified name and reaches RePark's
  own append / identity-DML path instead of the fork temp provider (which carries neither
  snapshot properties nor a codec — `QS-BRANCH-*`, `QZ-BRANCH-*`).
  pins: ice-session-write-conf-1/C-038, C-039, C-040
- `time_travel.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):** the ref-selector scan
  skips the `FROM` that names a `DELETE` target, so `DELETE FROM t.branch_b …` is a write to
  the branch and not a read pinned to it (the pin turned the target into a read-only temp
  view; every other `FROM`, including a subquery's, still pins).
  pins: ice-session-write-conf-1/C-038
- `spark_ast.rs` — **ICE-SESSION-WRITE-CONF-1 round 8 (2026-09-20):**
  `canonicalize_identity_selection` canonicalises the selection and each SET *value* through
  `rewrite_fragment_case` (a SQL fragment in, a SQL fragment out — the repair backticks a
  stored spelling it had to change), and each SET *target* through
  `canonical_assignment_target`, a bare name-to-name lookup. A target is a column name, not a
  fragment: rendered as SQL it reached `validate_update_assignments` as `` `eventName` `` and
  refused where Spark answers (MC-UPD-01/02). No unique case-insensitive match leaves the
  requested spelling alone, so the ambiguity and missing-column refusals still fire there.
  pins: ice-session-write-conf-1/C-063
- `spark_ast.rs` — **ICE-SESSION-WRITE-CONF-1 round 4 (2026-09-20):** `execute_insert_source`
  is `execute_passthrough` stopped one step short — same parse, same rewrites, same analysis,
  but it executes the insert's INPUT instead of the insert. One pipeline answers both routes,
  so an owned INSERT cannot drift from a delegated one.
  pins: ice-session-write-conf-1/C-055
- `spark_ast.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `plain_identity_or_update` adds the plain `UPDATE … SET … WHERE <scalar comparison>` to the
  owned identity-DML route when the session write conf is set. Without it the statement
  commits through the fork's DataFusion DML, which takes no snapshot properties and no codec,
  so Spark's stamp, its MoR delete-file codec and its summary-key refusal were all silently
  lost (`QS-UPDATE-MOR`, `QZ-POSDEL-UPDATE`, `QR-UPDATE-COW-CHANGED-PARTITION`, `SP-UPDATE`).
  The reroute is conf-scoped on purpose: making every plain UPDATE owned is a routing decision
  of its own, tracked outside this unit. pins: ice-session-write-conf-1/C-038, C-040, C-041
- `spark_ast.rs` — **SE-1 D1:** after the SEC-02 plan guard,
  calls the shared belt's `repark_core::PreExecute::guard` (which owns
  `refuse_iceberg_create_of_tightened_ddl`) so `CREATE VIEW cat.ns.v AS …` and
  `SELECT … INTO cat.ns.t` — both of which reach here through the router's `_ =>` catch-all —
  cannot persist a required column from a tighten-derived source — including the one- and
  two-part spellings that resolve into an Iceberg catalog via `SET
  datafusion.catalog.default_catalog` (Z-1). Untightened `CREATE VIEW` behaviour is
  unchanged (that it persists an Iceberg table at all predates this branch). **SQP-1:**
  `rewrite_binary_casts` maps `CAST(x AS BINARY)` → `BYTEA` (plain DataFusion cannot plan
  `BINARY`); the `→ BINARY` verdict lives in `repark-functions` `IntToBinaryCast`, which the
  eager analyze below runs, so the door still fails illegal casts at build. **BL-11
  (2026-09-16):** the old plan-walk refusal is deleted in favour of that single verdict —
  it never saw the native `DataFrame` path, which builds `Expr::Cast` directly.
  **SPARK-SQL-GRAMMAR-1 (2026-09-16):** the same pre-plan slot runs the bare-unit
  (`bare_unit.rs`), nullary-demote (`bare_nullary.rs`) and keyword (`keyword_lower.rs`)
  lowerings, in that order; the range-frame restatement repeats all three.
  **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):** the slot also rewrites INSERT
  `DEFAULT` markers and records the INSERT column list, then fills omitted
  columns from `write_default` on the planned DML (`insert_defaults`). The marker
  pass's loaded table threads into the fill call, so one INSERT loads once.
  pins: ice-v3-write-default-1/C-004, C-007
- `bare_nullary.rs` — **SPARK-SQL-GRAMMAR-1 C-010 (2026-09-16):** bare nullary
  keywords in both Spark directions. `demote_refusing_nullary_calls` lowers a
  no-paren `localtimestamp` call (the Databricks dialect parses it as a function)
  to a plain column reference, so a real column still wins and a missing one
  reaches the planner; `map_bare_nullary_column_error` maps the missing-field
  error for the six names Spark refuses (`localtimestamp`, `current_catalog`,
  `current_database`, `current_schema`, `current_timezone`, `now`) to
  `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` (framed) or `[WITHOUT_SUGGESTION]`
  (frameless), matching PySpark 4.1.2. Runs in `spark_ast::execute_passthrough`
  (rewrite pre-plan, map around the whole passthrough) and the range-frame
  restatement. 8 in-module tests.
  pins: spark-sql-grammar-1/C-010
  **UNRESOLVED-ROUTINE-1 (2026-09-16):** `non_schema_error_passes_through` now
  fixtures a genuinely unrelated `Plan` error — unknown names reshape in
  `repark-core::unknown_routine`, not here.
  pins: unresolved-routine-1/C-006
- `insert_timestamp_ns.rs` — **ICE-TSNS-SQL-1 (2026-09-17):** the SQL door's INSERT conform
  for Iceberg `timestamp_ns` / `timestamptz_ns` target columns, called from `spark_ast`.
  `before_analysis` replaces the planner's `CAST(… AS Timestamp(ns))` over a non-column source
  (and over each `VALUES` row the insert projection reads) with the embedded ns cast, so the
  `spark_ltz_timestamp_cast` rule cannot narrow it to microseconds; a cast over a string
  COLUMN is left for the WI-2 store-assignment gate, which refuses it as it does for
  `TIMESTAMP` columns. `after_analysis` wraps any remaining temporal source whose analyzed type
  differs from the ns target (a `TIMESTAMP` column, a `TIMESTAMP` literal selected in a
  subquery) in the same cast. **Round 2 (2026-09-18, ruling Q-21c-8):**
  `timestamp_typed_values_cells` reads, from the statement before planning, which `VALUES` cells
  are `TIMESTAMP`-typed (`TIMESTAMP '…'`, `CAST(… AS TIMESTAMP)`); the planner types those and a
  bare string identically, and only the bare string is re-read at nine digits — a `TIMESTAMP`
  cell is left for the µs rule and widened like `INSERT … SELECT`. Both hooks return a
  non-insert plan without moving it. Rust tests: `tests/v3_timestamp_ns_door.rs`.
  pins: ice-tsns-sql-1/C-002, C-010
- `keyword_lower.rs` — **SPARK-SQL-GRAMMAR-1 C-003/C-004/C-005 (2026-09-16):**
  Spark-only keyword lowerings onto registered kernels. `x RLIKE p` becomes
  `regexp_like(x, p)` (`NOT RLIKE` becomes `NOT regexp_like`); `CAST(x AS
  TIMESTAMP_LTZ)` becomes `CAST(x AS TIMESTAMP)` (TRY_CAST included); a
  `TIMESTAMP_NTZ` type name still refuses, now as `[UNSUPPORTED_TIMESTAMP_NTZ]`
  naming TZ-6 (no tz-naive CAST path exists — rewriting onto `TIMESTAMP WITHOUT
  TIME ZONE` plans tz-aware, measured). Runs pre-plan in `spark_ast`; the error
  map chains after the nullary map around the whole passthrough. 6 in-module
  tests.
  pins: spark-sql-grammar-1/C-003, C-004, C-005
  **ICE-TSNS-SQL-1 (2026-09-17):** `CAST(x AS timestamp_ns)` / `timestamptz_ns` (any case,
  `::` included, `TRY_CAST` not) lowers to the embedded `__repark_cast_timestamp_ns__` /
  `__repark_cast_timestamptz_ns__` calls; `lower_timestamp_ns_casts` is the same lowering alone,
  applied by `router.rs` to a MERGE's source, `ON` and clauses (MERGE plans its rendered pieces
  outside the passthrough) only when `has_timestamp_ns_cast` finds one, and by `spark_ast.rs` to
  the statement an `EXPLAIN` wraps. pins: ice-tsns-sql-1/C-001, C-011
  **UNRESOLVED-ROUTINE-1 (2026-09-16):** `unrelated_errors_pass_through` now
  fixtures a genuinely unrelated `Plan` error — unknown names reshape in
  `repark-core::unknown_routine`, not here.
  pins: unresolved-routine-1/C-006
- `bare_unit.rs` — **SPARK-SQL-GRAMMAR-1 C-008 (2026-09-16):** the pre-plan rewrite
  for bare datetime-unit keywords. A bare `DAY` in a 3-argument `timestampadd` /
  `timestampdiff` / `dateadd` / `datediff` call becomes the `'DAY'` string literal the
  #606 kernels take (case-insensitive; `dateadd` / `datediff` keep their names — the
  kernels already route 3-argument calls). A quoted unit refuses
  `[INVALID_PARAMETER_VALUE.DATETIME_UNIT]` and an unknown bare unit refuses
  `[UNRESOLVED_ROUTINE]`, both matching PySpark 4.1.2 batch-14. Runs in
  `spark_ast::execute_passthrough` and the range-frame restatement, in lockstep.
  7 in-module tests.
  pins: spark-sql-grammar-1/C-008
- `spark_literals.rs` — **SQP-1:** `canonicalize(sql) -> Cow<str>`, the front-door pass that rewrites
  Spark string-literal escapes once (rule table, dialect, design in the module doc). Sole caller
  `router::execute_with_read_only` (grep-pinned); DataFusion-native `COPY` / `CREATE EXTERNAL TABLE`
  are skipped (their `OPTIONS ('k' 'v')` is a key/value pair, not Spark concatenation). The parser
  maps the passthrough parser's reachable `SQL` and `Diagnostic(SQL)` errors from canonical text to
  original source. Planning, execution, shared, and collection errors remain unchanged; a boundary
  pin holds this contract. Secondary rewrites stop mapping only when their SQL bytes change.
  **FNP-4B (2026-09-15):** the Databricks-lexer canonicalizer — Spark escapes in double-quoted
  STRING literals (escape-free ones keep their quotes, see the keep-double rule below),
  `D/F/S/Y/L/BD` numeric suffixes as CASTs, `* EXCLUDE` → `* EXCEPT`, struct
  call-base field access, the out-of-range `\U` Java artifact, and the `escapedStringLiterals`
  verbatim mode (`canonicalize_verbatim`, `translate_downstream_error_verbatim`,
  `escaped_string_literals_from_config_map` / `with_escaped_string_literals_config` /
  `escaped_verbatim_from_options`); `pub` so the binding's `filter_sql` path reuses it.
  Location maps in `apply_regions` are built only when a downstream parser error needs
  them. **FNP-4B round 8 (2026-09-15):** the numeric-suffix fast path fires on
  digit/`.` + suffix letter or exponent (`1.e2` stays on the rewrite path); bare
  decimals skip the tokenize. The 200-column decimal gap is DataFusion-side.
  pins: fnp-4b/C-001, C-004, C-005, C-006, C-020
  `sql_may_have_insert_partition` keeps quote-free `INSERT … PARTITION` text off the
  fast path so the column-list swap runs.
- `spark_literal_typing.rs` — **SQL-LITERAL-TYPING-1 (2026-09-16):**
  `SparkIntegralLiteral` types unsuffixed integral literals as Spark does —
  Int64 fitting i32 narrows to Int32, UInt64 becomes Decimal128(digits, 0),
  decimal literals past precision 38 refuse with
  `[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION]`, `-2147483648` folds — installed
  from `SparkExtension::configure_analyzer_rules` immediately before the first
  `type_coercion`, so DataFusion's own coercion then produces Spark's promotion
  with no per-operator retag. Skips `Limit` plans, rebuilds `Values` rows. The
  late `SparkIntegerLiteral` stays (idempotent no-op afterwards). The rule
  recurses with subqueries, re-resolves lambda variables after narrowing, and
  re-derives union schemas from their inputs, so the first coercion sees one
  consistent narrow world (else it cements Int64 around lambdas and unions).
  Unit tests beside the change cover the mapping, the refusal shape, the
  insert order and the union refresh.
  pins: sql-literal-typing-1/C-001, C-002, C-003, C-004, C-007
  **Remediation round 1 (2026-09-16):** the seat stays immediately before
  `type_coercion` (a first-among-pre-coercion seat was measured, broke 24
  FNP-8 HOF pins, and reverted — the revert stayed red, exonerating the
  seat; the true cause was the `recompute_schema` skip leaving stale parent
  schemas, fixed by always recomputing); the `Negative` fold is gone so only
  the lexer-level negative token narrows and `-(2147483648)` stays bigint
  (LIT2-SQL-03/04); the late `SparkIntegerLiteral` is uninstalled on both
  Spark doors (`spark_door_post_coercion_rules`, the early rule subsumes it).
  A plan-level apply pre-check skips literal-free plans;
  `resolve_lambda_variables` runs only with a higher-order function on the
  node (`recompute_schema` stays unconditional: gating it left stale parent
  schemas after bottom-up narrowing); `Values` clones rows only on change;
  `Union` rebuilds only on a moved child type.
  pins: sql-literal-typing-1/L-001, L-002, L-003
  **ICE-COUNT-FOLD-1 (2026-09-19):** this is the rule that narrowed `count(*)`'s
  `COUNT_STAR_EXPANSION` on both Spark doors, which kept DataFusion's `AggregateStatistics`
  from folding `count(*)` over an exact Iceberg row count. Expressions now walk through
  `repark_functions::spark_result_types::transform_keeping_count_star`: a non-distinct
  `count` of the literal `1` keeps (or, for the DataFrame door's `Int32(1)`, gets)
  `Int64(1)` while its `FILTER` / `ORDER BY` still narrow; the plan pre-check also fires on
  such a count (`needs_count_star_expansion`), since `groupBy().count()` carries no `Int64`
  literal. Names are preserved (`NamePreserver`), the result stays `Int64`. Unit pins
  `count_star_keeps_the_int64_expansion_and_its_name`,
  `int32_count_of_one_widens_without_an_int64_literal`.
  pins: ice-count-fold-1/C-001, C-002
  **WO-2 xo-muse8 UNIT1 fix-b (2026-09-21):** the same pre-coercion seat now also
  widens a decimal literal compared against a FLOAT/DOUBLE expression to DOUBLE
  (`d = CAST(0.0 AS DOUBLE)`, Spark's analyzed shape — the float side keeps its
  type and DataFusion's coercion then promotes a FLOAT column to double, exactly
  like Spark). DataFusion prefers decimal over float and used to cast the column
  (`CAST(d AS Decimal128(30,15))`), which dies with `Overflowing on NaN`. Covers
  the six comparison operators, `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`, either
  side, and `Negative`-wrapped literals; decimal-vs-decimal and
  decimal-vs-integral comparisons are untouched, as are unresolvable sides
  (conservative no-op). The post-coercion `FoldSparkNumericCasts` folds the new
  cast into a double literal on full sessions. Signed-zero `=`/`<>`/`<`/`>=`
  outcomes still follow the float eq kernel (total order: `-0.0` distinct from
  `0.0`), a separate pre-existing divergence pinned beside the sweep.
- `spark_rewrites/` — the token-rewrite planners of the canonicalize layer;
  `mod.rs` carries the shared span helpers and the families below.
  **D-5 (2026-09-21):** `create_options.rs` (wired last in `spark_literals.rs`
  `canonical_rewrite`, with its `sql_may_have_create_options` fast-path guard)
  rewrites the single well-formed `OPTIONS (k=v, …)` clause of a
  `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name [(cols)] USING iceberg`
  statement into `TBLPROPERTIES` carrying each key raw and `option.`-prefixed,
  so both spellings reach the stored property map. Values copy verbatim from the
  source with inner literal regions spliced in; non-iceberg providers, a missing
  `USING iceberg`, `WITH`/plain variants, malformed pairs, and DataFusion-style
  no-eq pairs stay untouched. Cells `D-CREATE-OPTIONS`, `D-CTAS-OPTIONS`.
  Details: [spark_rewrites/map.md](spark_rewrites/map.md).
- `spark_rewrites/mod.rs` — **FNP-4B (2026-09-15):** numeric suffixes (BD precision/scale from
  digits; D/F as CAST of a decimal operand so the planner keeps them non-null; `1e3L` /
  `0x1D` as identifiers; `128Y`/`40000S` refuse `[INVALID_NUMERIC_LITERAL_RANGE]`),
  `* EXCLUDE` → `* EXCEPT`, DROP TEMPORARY, FROM-less `DELETE t WHERE` → `DELETE FROM t WHERE`,
  and call-base struct field access (chained
  `.s.a` extracts the named_struct value and wraps `__repark_spark_as__` so selectExpr
  display is `named_struct(a, 1).a` and nullability follows the value).
  **Round 5 (2026-09-15):** D/F/decimal rewrites wrap a string operand in
  `__repark_suffix_literal__` so the fold still sees the literal.
  `-9223372036854775808L` folds the unary minus into the BIGINT region.
  **Round 8 (2026-09-15):** Y/S regions absorb a unary minus like the LONG_MIN arm
  (`CAST({signed} AS TINYINT/SMALLINT)`), range-checked on the signed text; the L arm
  refuses out-of-range `BIGINT` at parse with `[INVALID_NUMERIC_LITERAL_RANGE]`.
  pins: fnp-4b/C-001, C-004, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-021, C-023
  ICE-V3-WRITE-DEFAULT-1 round 5 (2026-09-17): `plan_insert_partition_column_list_regions`
  swaps Spark's `INSERT … PARTITION (…) (cols) <query>` into the parser's
  `INSERT … (cols) PARTITION (…) <query>` order; a group that is not a bare
  identifier list, or not followed by a query body, is left alone.
  pins: ice-v3-write-default-1/C-015
- `spark_typed.rs` — **FNP-4B critic (2026-09-15):** `FoldSparkNumericCasts` folds
  `CAST('1e200' AS DOUBLE)` to a non-null Float64 literal; `SparkProjectionDisplay`
  aliases unaliased projections whose DataFusion names carry `Int64(` /
  `datafusion.public` / backticks to Spark display (`(my col + 1)`); `__repark_spark_as__`
  is the identity UDF that carries a Spark display name.
  **Round 5 (2026-09-15):** `__repark_suffix_literal__` is the companion
  provenance-marker UDF with fold-through and display unwrapping, registered in
  `extension.rs` and the binding `sql_context`.
  **JAVA-DOUBLE-FD-1 fix round 1 (2026-09-15, R-17c-2):** `SUFFIX_LITERAL_NAME`
  is a re-export of `repark_functions::java_double::SUFFIX_LITERAL_NAME` — the
  marker's single source lives in the capability crate because
  `SparkFloatStringify` must recognize the token and the DAG forbids a
  functions→spark edge.
  **Round 5 (2026-09-15):** `SparkProjectionDisplay` rewrites only the root
  projection and keeps explicit non-marker aliases.
  **Round 6 (2026-09-15):** `cargo fmt` applied.
  **Round 8 (2026-09-15):** `SuffixLiteral` schema/display names come from the
  value text (Java double/float text, scale-exact decimals), so nested columns never
  carry the marker; the fold covers numeric targets except `Int64` (whose CAST anchor
  must survive re-analysis); the root display fires on single-root scalar-dump
  names only (balanced `Int64(1)`, table-qualified `t.Int64(1)`, negated
  `(- 0.0)`; composites like `a + Int64(1)` or `getbit(Int64(6),Int64(1))`
  keep their plan names) and names unfolded integer `CAST`s from the literal
  inner. Inner renames are out of scope:
  analysis runs twice and outer references would go stale.
  pins: fnp-4b/C-012, C-014, C-015, C-019, C-020, C-021, C-022
- `create_table.rs` — column-def `CREATE TABLE` (I5 schema-only staged create) + the
  Spark-SQL→iceberg type mapping; **V3-2:** `iceberg_create_format_version` (session opt-in;
  `Model: Grok 4.6 xHigh`);
  default `TIMESTAMP` → Iceberg `timestamptz`,
  `TIMESTAMP_NTZ` stays `timestamp` (live Spark 4.1.2 CREATE probe). **Q10:** bare
  `TIMESTAMP` follows `spark.sql.timestampType` (`TIMESTAMP_NTZ` → Iceberg `timestamp`);
  existing `sql_type_to_iceberg` wrapper stays LTZ so default-mode pins are untouched.
  **V3-6 C-003:** Iceberg type names `timestamp_ns` / `timestamptz_ns` map to the V3
  primitives behind the same opt-in (pins: v3-6-v3-types/C-003); v2 CREATE refuses via
  the fork's `check_compatibility`.
  4 in-module tests (`type_mapping_tests`) + `tests/create_table.rs` pin + CTAS type smoke.
  **ICE-CATALOG-SESSION-1 S6 (2026-09-20):** `execute_schema_create` merges user
  `TBLPROPERTIES` through the catalog side map (override > user > default).
  pins: ice-catalog-session-1/C-027
  **FNP-4B round 7 (2026-09-15):** angle-bracket `ARRAY<T>` maps to an Iceberg
  list with nullable `element` fields and table-unique ids from a checked
  allocator (R-16b-21 grant); bare/square-bracket forms still refuse.
  pins: fnp-4b/C-025 (round-7 fmt/clippy follow-ups carry no behavior change)
  **ICE-NESTED-EVO-1 (2026-09-17):** `STRUCT<a: T, …>` maps to an Iceberg struct of nullable
  children and `MAP<K, V>` to an Iceberg map (required key, nullable value), both from the same
  checked id allocator, so `CREATE TABLE` with struct, array-of-struct and map-of-struct columns
  round-trips the `DESCRIBE` types Spark shows. `normalize.rs` parses a column-def `CREATE TABLE`
  whose column list spells `MAP<` (`has_angle_map_column_type`, scanned before the CTAS `AS`)
  with `SparkSqlDialect` (the Databricks dialect has no angle-bracket map type); every other
  statement keeps its dialect.
  pins: ice-nested-evo-1/C-006
  **Round 2 (2026-09-18, run 22b):** `parse_single_normalized` runs
  `repark_iceberg::write::nested_type_sql::rewrite_nested_type_tokens` (round 3: `rewrite_create_column_types`) on every
  `CREATE TABLE`, and `struct_type_to_iceberg` makes a `STRUCT<a: T NOT NULL>` child an Iceberg
  required child, as Spark's metadata records. The field ids this module numbers are
  placeholders: the fork's `TableMetadataBuilder::new` reassigns them level-order (Java's
  `AssignFreshIds`), which is Spark's numbering, pinned by
  `test_ice_nested_evo_1_schema.py::test_nested_ddl_metadata_matches_spark[create_field_ids-*]`.
  pins: ice-nested-evo-1/C-014, C-016
  **Round 3 (2026-09-18, run 22b, V-002):** the call is now `rewrite_create_column_types`, so
  only the column-definition list is rewritten; a CTAS query keeps its `struct < 1 AND x IS
  NOT NULL` as written.
  pins: ice-nested-evo-1/C-022
  **IPI-26/27 round 2 (2026-09-20):** a column `COMMENT` maps to the field doc
  via `NestedField::with_doc`, the extracted table `COMMENT` lands as the `comment`
  property, and the extracted `LOCATION` flows through `SchemaCreate.location`
  into the staged plan (cells `D-CREATE-COL-COMMENT`, `D-CREATE-COMMENT`,
  `D-CREATE-LOCATION`, `D-DESCRIBE`).
  **IPI-51 PR7 (2026-09-21):** the column-option refuse stamps Spark's
  `[UNSUPPORTED_FEATURE.TABLE_OPERATION]` / `0A000` through `repark_common::spark_error`
  as `DataFusionError::Plan` over the backticked three-part table display threaded into
  `schema_from_column_defs` (cells `D-CREATE-DEFAULT`, `D-CREATE-DEFAULT-V2`,
  `D-ALTER-DROP-DEFAULT`, all dead at the CREATE). The facade CREATE DEFAULT pins
  (`test_column_default_ddl_refuses_naming_the_option` CREATE arm,
  `test_iceberg_hygiene` `with_def`) require the stamped condition / `0A000`,
  not `UnsupportedOperationException`.
  pins: ice-error-conditions-1/C-011
  **IPI-51 PR8 (2026-09-21):** the `sql_type_to_iceberg_nested` unsupported-type arm
  stamps Spark's `[UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED]` / `0A000` as
  `DataFusionError::Plan` when `geospatial_sql_type` names `GEOMETRY` / `GEOGRAPHY`
  (bare, `(srid)`, case-folded); every other unsupported type keeps the existing
  `NotImplemented` string (cell `TY-GEOMETRY`).
  pins: ice-error-conditions-1/C-011
- `format_version.rs` — **V3-10:** the Spark-door adapter for `SET TBLPROPERTIES
  ('format-version' = …)`. It lifts the reserved key out of the property map before the
  transaction (so it is never persisted), resolves it against the table's current version and the
  session opt-in against the table it loads ONCE, and hands that loaded table to the transaction.
  It does NOT re-register the namespace: measured, the version-only dirty bit cost one
  `list_tables` and two `namespace_exists` per upgrade and bought nothing, because the DF
  provider reloads table metadata per plan. `tests/v3_upgrade_calls.rs` is the guard — it pins
  the call counts AND reads the v3 lineage columns through the same session afterwards.
  pins: v3-10-upgrade-v2-to-v3/C-003, C-004
- `alter.rs` — ALTER TABLE handlers (SET/UNSET TBLPROPERTIES, RENAME TO, schema evolution I6,
  I7 partition-field DDL, residual refusals) + the ALTER token rewrites the normalizer runs;
  9 in-module tests. **Q10:** ADD/ALTER COLUMN bare `TIMESTAMP` follows the session
  `spark.sql.timestampType` carrier. **ICE-COLUMN-REORDER-1 (2026-09-17):** the I6 move refusal
  is gone; the move lives in `column_move.rs`. **ICE-REPLACE-COLUMNS-1 (2026-09-19):** the
  REPLACE COLUMNS parser, planner and the identity-trap gate left this file for
  `replace_columns.rs`; `alter.rs` only detects the form and routes it.
  **IPI-51 PR4 (2026-09-20):** the residual Hive `ADD PARTITION` refusal now answers
  plan-class through `catalog_ops::partition_management_unsupported` with the backticked
  target (`AnalysisException`, `SQLSTATE: 42601`); the file ratchets 1449 → 1446.
  pins: ice-error-conditions-1/C-011
  **IPI-26/27 round 3 (2026-09-21):** the `REPLACE PARTITION FIELD` parser left this
  file for [`replace_partition_field.rs`](replace_partition_field.rs), which also takes
  the transform-LHS form the file used to refuse.
  **IPI-26/27 round 1 (2026-09-20):** `split_top_level_comma_segments` tracks
  angle depth alongside paren depth, so commas inside `STRUCT<…>` / `MAP<…>` no
  longer split the column list (cells `D-ADD-COL-STRUCT`, `D-X-ADD-COL-MAP-KEY-STRUCT`);
  `ShiftRight` closes two levels for nested `>>`.
- `replace_columns.rs` — **ICE-REPLACE-COLUMNS-1 (2026-09-19):** Spark's Hive-style
  `REPLACE COLUMNS` — one `DropColumn` per current top-level column, then one `AddColumn` per
  listed column, so the fork's `UpdateSchema` assigns every column a **fresh** id from
  `last-column-id + 1` and existing rows read NULL (measured: Spark 4.1.2 + Iceberg 1.11.0, cells
  `RC-*`). The column list goes through the shared sqlparser column-type path
  (`rewrite_nested_type_tokens` → `parse_data_type` → `sql_type_to_iceberg_with_timestamp_type`),
  so STRUCT / ARRAY / MAP land like CREATE TABLE and ADD COLUMN do, and bare `TIMESTAMP` now
  follows the session `spark.sql.timestampType` carrier (it used to be pinned to the LTZ
  wrapper). Refusals carry Spark's text: `NOT NULL` / a column position / a nested name are
  Hive-style parse errors, a duplicate name is `[COLUMN_ALREADY_EXISTS]`, and a live partition
  or sort field whose source id would vanish is Iceberg's `Cannot find source column for …`
  `ValidationException` — raised before the commit, so the table is untouched. The module
  carries no comments (owner ruling): `parse` walks ALTER TABLE → name → REPLACE COLUMNS →
  `(`, then one `parse_column` per entry (name, type, then `NOT NULL` / position / `COMMENT`
  in any order) and requires end-of-statement; `plan` refuses duplicates before it loads the
  table, then the partition and sort checks, then emits the drops before the adds — the
  order the fork's `UpdateSchema` needs to allow a same-name re-add.
  pins: ice-replace-columns-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `replace_partition_field.rs` — **IPI-26/27 round 3 (2026-09-21, cell
  `D-REPLACE-PART-FIELD`):** the `ALTER TABLE … REPLACE PARTITION FIELD <old> WITH
  <transform>(col) [AS name]` parser, moved out of `alter.rs` (exact ceiling). The old
  field is a bare partition name (the by-name `ReplaceField` path) or a
  `transform(col)` call, which becomes `ReplaceFieldByTransform` — the fork-side
  evolution resolves the `(source column, transform)` pair against the CURRENT spec and
  then runs the by-name remove-plus-add, so `days(ts) WITH hours(ts)` lands spec
  `[["ts_hour","hour","ts"]]` with spec-count 2, measured against live Spark. A
  transform LHS matching no current field refuses loud
  (`matches no partition field in the current spec`). A sibling module, not an
  `alter.rs` arm, because that file sat at its exact ceiling (now ratcheted down).
- `column_move.rs` — **ICE-COLUMN-REORDER-1 (2026-09-17, round 2 Q-20b-5):** the `ALTER COLUMN …
  FIRST|AFTER` pre-parse (`try_parse_column_move_ddl` / `execute_column_move_ddl`, wired in
  `router.rs` ahead of the residual refusal): an `ALTER`-prefix fast path before any tokenize,
  dotted `AFTER` references refuse with Spark's `[PARSE_SYNTAX_ERROR]`, nested paths resolve,
  every move commits through one loaded table (`apply_schema_changes_on_table`), unknown names
  refuse with Spark's `UNRESOLVED_COLUMN` framing. A sibling module, not an `alter.rs` arm,
  because that file sits at its exact ceiling. 4 in-module tests + [`tests/column_move.rs`](tests/column_move.rs).
  pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-014
- `table_props_ddl.rs` — **WO-IDENTIFIERS (2026-09-21, cells `D-SET-IDENTIFIER` /
  `D-DROP-IDENTIFIER`):** the `ALTER TABLE … SET|DROP IDENTIFIER FIELDS <col>[, …]` pre-parse
  (`try_parse_set_identifier_fields_ddl` / `try_parse_drop_identifier_fields_ddl` /
  `execute_identifier_fields_ddl`, wired in `router.rs` after the column-move intercept). Every
  named field is resolved against the current schema (top-level and dotted paths,
  case-insensitive) and SET refuses — with Iceberg's message shapes, the recorded nullable one
  being `Cannot add field {name} as an identifier field: not a required field` — optional,
  float/double, non-primitive and list/map-nested candidates before any transaction action is
  built; both statements refuse unknown names; `require_column` is never called. SET commits the
  fork's `UpdateSchemaAction::set_identifier_fields`; DROP replaces the set with
  current-minus-dropped and leaves nullability untouched. Only the fully well-formed statement
  is captured — `SET IDENTIFIER` (no FIELDS), parenthesized, dangling-comma and trailing-token
  variants keep the stock parser fall-through. 4 in-module tests +
  [`tests/identifier_fields.rs`](tests/identifier_fields.rs).
- `nested_column_ddl.rs` — **ICE-NESTED-EVO-1 (2026-09-17):** the nested-path `ALTER TABLE`
  pre-parse (`try_parse_nested_column_ddl` / `execute_nested_column_ddl`, wired in `router.rs`
  ahead of the column-move intercept): `ADD COLUMN[S] s.c T [NOT NULL] [COMMENT '…']
  [FIRST|AFTER x]` (a list, with or without parentheses), `RENAME COLUMN s.a TO a2`,
  `DROP COLUMN[S] [IF EXISTS] s.b, …`. It reads the statement with sqlparser's
  `SparkSqlDialect` (so a `MAP<K, V>` child type parses) and claims it only when a column path
  has a dot; an all-top-level statement falls through to the stock path untouched. `element`
  and `value` path steps (`arrs.element.y`, `m.value.q`) go to the fork verbatim. Every change
  commits through `repark_iceberg::write::nested_column::apply_column_path_changes` — the
  fork's `UpdateSchema` (`add_column_to` / `add_required_column_to` with the parent path,
  `rename_column`, `delete_column`); RePark keeps no schema model of its own. A required child
  without a default refuses with the fork's `Incompatible change: cannot add required column…`.
  Pins: [`tests/nested_column_ddl.rs`](tests/nested_column_ddl.rs).
  pins: ice-nested-evo-1/C-006, C-007, C-008, C-009, C-010, C-011, C-012
  **Round 2 (2026-09-18, run 22b):** a claimed statement that holds a double-quoted word
  (`RENAME COLUMN s.a TO "x.y"`, `ADD COLUMN s."x.y" INT`) refuses Spark's
  `[PARSE_SYNTAX_ERROR] Syntax error at or near '"x.y"'. SQLSTATE: 42601` (Spark reads `"…"`
  as a string literal there; `SparkSqlDialect` had read it as an identifier and renamed the
  child) — the message rides a `Context` over the `SQL` error so it stays a `ParseException`
  and keeps its quotes verbatim. Backticked dotted names (`` TO `x.y` ``) stay accepted, as in
  Spark. Before the commit, `nested_add_refusal` answers Spark's `FIELD_ALREADY_EXISTS` /
  `UNRESOLVED_COLUMN` as an analysis error.
  pins: ice-nested-evo-1/C-015, C-019
  **Round 3 (2026-09-18, run 22b, V-001):** the refusal is positional. A `NameParser` wraps
  the parser and records the first double-quoted identifier it reads in a name position — a
  column-path segment, the new name after `TO`, the `AFTER` reference — and only that refuses.
  A double-quoted token in a string position (`COMMENT "x.y"`) is Spark's string literal and
  becomes the child's `doc`, as Spark measured (round 2 had scanned the whole statement and
  refused it).
  pins: ice-nested-evo-1/C-021
- `alter_write_order.rs` — **WRITE-ORDER-DIST-1 (2026-09-06):** the `ALTER TABLE …
  WRITE …` pre-parse intercept (sqlparser carries none of these forms): `WRITE ORDERED BY`
  (sort order + `write.distribution-mode = range`), `WRITE LOCALLY ORDERED BY` (sort order,
  property untouched), `WRITE DISTRIBUTED BY PARTITION` (`hash`, default order reset to the
  unsorted order 0), `WRITE DISTRIBUTED BY PARTITION [LOCALLY] ORDERED BY` (both + `hash`),
  `WRITE UNORDERED` (order 0 + `none`). A bare `ASC` defaults to `NULLS FIRST`, a bare
  `DESC` to `NULLS LAST`, the way Spark resolves them; a transform sort field, a quoted
  column, and every malformed shape refuse loud before anything commits. Dotted nested names
  (`st.a`) parse to the nested field (round 2, 2026-09-06). It is a sibling
  module, not an `alter.rs` arm, because that file sits at its exact ceiling. Pins:
  [tests/alter_write_order.rs](tests/alter_write_order.rs).
  pins: write-order-dist-1/C-001, C-002, C-003, C-004, C-005, C-006
- `namespace_ddl/` — the table-lifecycle work `namespace_ddl.rs` delegates; see
  [namespace_ddl/map.md](namespace_ddl/map.md). `purge.rs` holds the `DROP TABLE … PURGE`
  reachable-file sweep and the `gc.enabled` gate (**IPI-21**, 2026-09-20); `execute_drop_table`
  runs the sweep before `Catalog::drop_table` because the metadata location must still be
  resolvable when the action reads it. A non-empty `delete_failures` logs once — one
  `tracing::warn` naming the table and the count — and the `DROP` still succeeds (Java's
  log-only suppression, owner ruling 2026-09-20).
  pins: ipi-21-25-42-small-parser/C-008, C-009, C-010
- `sort_order_parse.rs` — **ICE-RDF-SORT-PARSE-1 (2026-09-20):** the single home of the
  identity sort-order grammar. `alter_write_order.rs` owned the only sort-order parser in the
  tree; `CALL rewrite_data_files(sort_order => …)` needs the same grammar, so the tokenizer
  (`Sig`, `tokenize_significant`), the segment splitter and `parse_order_list` /
  `parse_order_segment` were lifted here rather than copied. A second copy would have been free
  to drift on the direction-tied `NULLS` defaults (bare `ASC` → `NULLS FIRST`, bare `DESC` →
  `NULLS LAST`), and a table sorted differently through two doors is invisible until a customer
  reads it back. The parser is now door-neutral: it returns `OrderParseError` variants and each
  door renders its own text — `alter_write_order.rs::alter_order_error` keeps every
  `ALTER TABLE WRITE ORDERED BY …` string byte-for-byte, and the CALL door renders Java's
  `Unable to parse sortOrder: {s}`. `parse_zorder_columns` is the CALL door's alone: it
  classifies every top-level term as a `zorder(…)` term or an identity term, so
  `WRITE ORDERED BY zorder(id)` still hits the ALTER transform refusal (a Z-order is not a
  storable sort order — `P-RDF-ZORDER` measures `md.sort-order` staying `[]`), while the CALL
  door reaches the fork's `RewriteStrategy::ZOrder`. Mixed terms are Java's
  `Cannot mix identity sort columns and a Zorder sort expression`, checked before the strategy
  dispatch because that is where `RewriteDataFilesProcedure.checkAndApplyStrategy` checks it.
  A non-`zorder` transform term (`bucket(4, id)`) is a *registered* refusal on the CALL door,
  not a parse failure: Spark's `ExtendedParser.parseSortOrder` accepts transform sort terms, so
  refusing one is a divergence that has to be declared rather than hidden behind Java's
  `Unable to parse sortOrder`. It gets its own message and its own registry row
  (`RDF-SORT-TRANSFORM-1`); reusing the ALTER door's wording would have mis-stated which door
  refused. The ALTER half of the split — `WRITE ORDERED BY (zorder(id))` refusing with no
  commit — is pinned by
  `tests/alter_write_order.rs::write_order_zorder_term_refuses_and_commits_nothing`.
  pins: ice-rdf-sort-parse-1/C-001, C-002, C-003,
  tests/alter_write_order.rs::write_order_zorder_term_refuses_and_commits_nothing
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`. `IF NOT EXISTS` checks location consistently:
  matching/no-location requests stay idempotent; contradictory `LOCATION` fails loud naming both
  paths (`repark_core::refuse_contradictory_namespace_location`). `DROP` refuses a non-empty
  namespace with Spark's `Namespace <ns> is not empty` text (CASCADE drops nothing, `IF EXISTS`
  does not bypass) and answers `[SCHEMA_NOT_FOUND]` naming `catalog`.`namespace` when it is
  missing — both through the shared `refuse_non_empty_namespace_drop` (**ICE-DROP-NS-1**,
  2026-09-19).
  pins: ice-drop-ns-1/C-002, C-004, C-007
  The missing-namespace refusal is the shared `schema_not_found_on_drop`. pins: ice-drop-ns-1/C-011
  **IPI-51 (2026-09-20):** a `TableNotFound` from `Catalog::drop_table` without `IF EXISTS`
  renders `[TABLE_OR_VIEW_NOT_FOUND]`/`SQLSTATE: 42P01` through `table_or_view_not_found`, and
  the `create_table.rs` / `ctas.rs` already-exists arms render
  `[TABLE_OR_VIEW_ALREADY_EXISTS]`/`SQLSTATE: 42P07` through `table_or_view_already_exists`
  (both `catalog_ops.rs`; `repark-common` is a dev-dependency here, so the catalogue itself
  stays unreachable from this crate's production code).
  pins: ice-error-conditions-1/C-011
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into the positional `execute_with_read_only` call; `#[async_trait(?Send)]` matches the
  core trait; install with `ReparkSessionBuilder::with_sql_dialect` + `SparkExtension`).
  `execute_with_write_options` copies `cx.force_static_overwrite` onto the validated
  options (ICE-WRITE-OPTIONS-1 run 22b, 2026-09-18). pins: ice-write-options-1/C-014
  **ICE-OVERWRITE-MODE-1 (2026-09-19):** it copies `cx.overwrite_intent`; `execute` routes a
  non-`Session` intent through `execute_with_statement_options`. pins: ice-overwrite-mode-1/C-007
  **ICE-CATALOG-SESSION-1 R6 (2026-09-20):** the S9 `on_session_built` flip and the
  temp-view-home placeholder are deleted: planner `default_catalog` / `default_schema`
  stay at the DataFusion builtins in every session, and the auto memory catalog owns
  the real `spark_catalog`.
  Tests: [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension` owns Spark session defaults and installs the ordered
  `InsertStoreAssignment`, function registry, analyzer rules, `StackRewrite` (PERF-UNPIVOT-1,
  after integer-literal narrowing), and composed `TaExtension`. It also
  carries the session timezone, Spark decimal settings, the case-sensitivity
  carrier (`repark_functions::case_sensitive`, default false) and the
  partition-overwrite-mode knob (**ICE-DYN-OVERWRITE-1**, 2026-09-17). Tests:
  [extension/map.md](extension/map.md) and [../tests/session_timezone.rs](../tests/session_timezone.rs).
  **FNP-8 (2026-09-07):** its analyzer-configuration hook inserts the shared HOF preparation rule
  before core's first default type-coercion rule. pins: fnp-8/C-003, C-004
- **FNP-8 (2026-09-07):** the executing parser selects lambda syntax only inside
  recognized higher-order calls. JSON arrows retain the session parser and its AST.
  pins: fnp-8/C-004
- `normalize/` — token rewrites `normalize.rs` has no ceiling headroom for; see
  [normalize/map.md](normalize/map.md). `replace_table.rs` rewrites `REPLACE TABLE` to
  `CREATE OR REPLACE TABLE` **first**, before `is_create_table` gates the other rewrites, and
  carries the missing-table refusal that keeps the two spellings apart (**IPI-25**, 2026-09-20).
  pins: ipi-21-25-42-small-parser/C-005, C-006, C-007
  `clustered_by.rs` rewrites `CLUSTERED BY (col) INTO n BUCKETS` into
  `PARTITIONED BY (bucket(n, col))` between the `USING` strip and the
  `PARTITIONED BY` extraction, so the bucket field lands named `{col}_bucket`
  (**IPI-26/27 round 1**, 2026-09-20, cell `D-X-CLUSTERED-BY`).
- `normalize.rs` — token normalisers (`USING` strip, `PARTITIONED BY` extraction,
  `NAMESPACE`→`SCHEMA`, the ALTER rewrites + GenericDialect switch), statement sniffers,
  multi-statement refuse (BUG-010), the MoR multi-spec DML gate's resolution wrapper (BUG-001
  — predicate hoisted to `repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml`),
  the **G3-E8 subquery-predicate DML valve** (`refuse_dml_subquery_predicate` +
  `DmlSubqueryVerb`: a `WHERE` subquery is lost at DataFusion's DML planning boundary and
  degenerates into match-all — deliberately syntactic and slightly wide; the allow-list
  opens uncorrelated `DELETE … col IN` / `NOT IN (SELECT …)`, `[NOT] EXISTS` ±
  correlation, correlated IN, and identity `UPDATE … IN` onto `execute_predicate_dml`;
  see the module doc and `task/r1-g3e8-pr4-ledger.md`), the MERGE
  star rewrite call, partition-spec builders. `dml_target_ident` (shared with the BUG-001
  valve) completes short names from the session defaults (SEC-001). V3-7 MERGE keeps
  `_row_id`; subquery-WHERE DML still refuses `V3-COW-1`.
  pins: v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002
  **FNP-8 (2026-09-06):** `dialect_for_executing_parse` — `Databricks` when the SQL
  carries a `Token::Arrow` outside strings/comments, else the session dialect — so the
  Spark door parses `x -> y` lambdas without the session-wide FNP-4b flip. Unit pins are
  inline in the module; door pins are [`tests/lambda_door.rs`](tests/lambda_door.rs).
  pins: fnp-8/C-004
  **IPI-51 PR6 slice 3 (2026-09-21):** `build_partition_spec` stamps a missing
  partition-source column as `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` / `42703` through
  `repark_common::spark_error` — the missing column backticked as `{columnName}`, the schema
  fields backticked as `{suggestions}` — on both its callers (`create_table.rs`, `ctas.rs`).
  Cell `W-INSERT-OVERWRITE-HIDDEN-PART` fails at CREATE here; Spark's
  `_LEGACY_ERROR_TEMP_3060` id is not copied. Pins: retargeted U1-P10 plus the CREATE TABLE
  `days(ts)` pin in [`tests/partitioned_ctas.rs`](tests/partitioned_ctas.rs).
  pins: ice-error-conditions-1/C-011
  **IPI-26/27 round 1 (2026-09-20):** the dialect switch sends a `CREATE` or
  `ALTER TABLE` carrying an angle-bracket `MAP<` to `SparkSqlDialect` (cell
  `D-X-ADD-COL-MAP-KEY-STRUCT`); every other `ALTER` stays on `GenericDialect`,
  pinned by the corpus in [`tests/ice_ddl_clauses_1.rs`](tests/ice_ddl_clauses_1.rs).
  **IPI-26/27 round 2 (2026-09-20):** `parse_single_normalized` also runs
  [`normalize/create_clauses.rs`](normalize/create_clauses.rs), which strips the
  table `COMMENT` and `LOCATION` clauses at paren depth zero before the CTAS `AS`
  (column `COMMENT` options and the query pass through), so both clauses parse on
  either side of `TBLPROPERTIES`. `strip_create_table_using` moved into that
  module to hold the 1000-line ceiling.
  **IPI-26/27 round 3 (2026-09-21, cells `D-CREATE-PART-DATE-ALIAS` + bucket/truncate
  near-miss):** `build_transform_field` accepts `date` as an alias of `day` and
  `date_hour` as an alias of `hour` (field names still fall out of the
  `{column}_day` / `{column}_hour` rule), and `bucket` / `truncate` now sniff their
  width argument by type instead of position — exactly one argument parses as an
  integer and names the width, so `bucket(id, 4)` equals `bucket(4, id)` (same
  `id_bucket` field name), while `bucket(1, 2)` (both integers) and
  `bucket('a', 'b')` (neither) refuse loud. [`tests/ice_ddl_clauses_1.rs`](tests/ice_ddl_clauses_1.rs)
  pins all three.
  **IPI-26/27 round 3 remediation (2026-09-21):** `render_transform_arg` keeps string
  literals quoted, so a quoted argument can no longer ride the type sniff as a column:
  `bucket('x', 16)` and `bucket("x", 16)` refuse with the integer error — only a bare
  unquoted integer token can be the width.
- `call_args.rs` — CALL argument bag, scalar coercions, and quoted-name keys for dashed options.
  **ICE-PROCEDURES-1 (2026-09-20):** mixed positional and named arguments are
  legal (Spark accepts the mix), and `bind` binds a `CallArgs` against a
  declared parameter list into a `BoundArgs` — positionals in declared order, SQL
  NULL as unset, a loud duplicate-binding Plan error when one parameter is bound
  both ways, today's unknown/arity/missing strings otherwise. Array-of-literal
  coercions land here for the rounds that wire array parameters. **PR1b
  (2026-09-21):** `BoundArgs` gains the scalar readers the wiring rounds need
  (`optional_i64`, `optional_i32`, `optional_timestamp_ms`, `require_expr`), and
  the expire handler calls the i64-array coercion. **RM-SORTBY-1 (2026-09-21):**
  `CallArgs` gains `optional_string_array`, the named-or-positional string-array
  reader with SQL NULL as unset that the `rewrite_manifests` handler parses
  `sort_by` through. **IPI-30 round 3 (2026-09-22):** `CallArgs` gains
  `optional_string_at`, the named-first-then-positional string reader
  `remove_orphan_files` uses for `location` (Spark position 2).
  pins: ice-procedures-1/C-003, C-004, C-006, C-007, C-008, C-011, C-013, C-015
  pins: ice-procedures-1/C-022
- `collation.rs` — **G15:** parse-altitude collation refuse. Walks
  `Expr::Collate`, column-def `COLLATE`, `CREATE`/`ALTER COLLATION`, `SET NAMES COLLATE`,
  session `SQLConf` keys containing `collation` (including `ParenthesizedAssignments`),
  type-position `STRING COLLATE` (Spark `CAST(x AS STRING COLLATE name)`), and
  `RESET` of a collation key. `refuse_collation_in_statement` is called from
  `spark_ast.rs` (executing parse) and the router's successful parse (intercepted
  CREATE/ALTER). `refuse_collation_in_sql` is `pub` for the Python binding (`F.expr`,
  `filter_sql`). Pins: [`tests/collation.rs`](tests/collation.rs). Ledger:
  [`../../../task/y7-collation-refuse-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-y7-collation-refuse-ledger.md).
- `spark_type_names.rs` — the single canonical Spark DDL type spelling
  (`spark_ddl_type_name`, depth-bounded): `bigint` for 64-bit ints, the way `DESCRIBE`
  prints them (SQL-DESCRIBE-1 D-3, owner ruling R-9). The Python binding calls it for
  nested element tokens; `long` stays in `repark-python` for `printSchema`.
  **FACADE-4 step 1 (2026-09-14):** a delegation shim — every answer now comes from
  `type_table::arrow_name` on the `Describe` surface; spellings unchanged.
  pins: sql-describe-1/C-003
- `type_table.rs` — **FACADE-4 step 1 (2026-09-14):** the shared conversion table —
  one `SparkDataType`/`SparkField` descriptor mirroring the 25 public
  `repark.spark.types` classes, Arrow ↔ descriptor conversion in both directions,
  DDL/simpleString parse and write, and the distinct name surfaces the census found
  disagreeing (`ArrowNameSurface::Describe` vs `::LogicalKey`, engine cast token, DDL
  token, SQL marker, CSV rung row). Every surface keeps its step-0 answer
  byte-for-byte; no D1–D21 row is unified. Depth-bounded at 32 with a `"..."`
  fallback. Pure Rust, no PyO3; `repark-python::type_bridge` is the only consumer
  over the FFI boundary. S2-21 remediation (P3-COLLATION): `SparkString.collation`
  is `Cow<'static, str>` — the `UTF8_BINARY` default borrows the
  `pub const DEFAULT_COLLATION` everywhere (Arrow, DDL fallback, CSV rungs) and
  only a parsed custom collation owns.
  Round-2 remediation: `TypeTableError::IntegerOverflow` marks integer
  parameters beyond i64 so the bridge can route them to Python's unbounded
  parse (`PyOverflowError`).
  **TYPES-GEO-DDL-1 (2026-09-15):** `SparkDataType::Geometry { srid }` /
  `Geography { srid }` (`-1` is Spark's `any`, `SPATIAL_MIXED_SRID`); the
  supported-SRID sets live here once (`GEOMETRY_SRIDS`, `GEOGRAPHY_SRIDS`) —
  Python keeps only the JSON door's CRS spellings. `simple_string` answers the
  oracle `geometry(n)` / `geography(n)` forms; `arrow_type_from_spark` refuses
  both variants naming the type (V3-GEO-1) instead of the `Utf8` catch-all;
  `ddl_token` / `sql_marker_token` fall through to the existing generic arms.
  Round 2: `spatial_srid_supported` validates inbound bridge SRIDs (mixed form
  included) so no Spark-impossible spelling builds.
  pins: facade-4/C-010, C-019, C-020; types-geo-ddl-1/C-001, C-003
  **LOGICAL-WIDTH-1 (2026-09-16):** the `LogicalKey` arms answer Spark's narrow
  widths (Int8→`byte`, Int16→`short`, Float16|Float32→`float`,
  Binary family→`binary`); both surfaces survive because `bigint`/`long`,
  `smallint`/`short` and `tinyint`/`byte` still differ. `logical_type_key` keeps
  its single caller (`repark-python::dataframe::arrow_type_key`).
  pins: logical-width-1/C-001, C-002, C-003, C-005, C-006, C-007
- `type_table/parse.rs` — the text→descriptor half of the table, split out at the
  file-size ceiling: DDL and SQL-token parsing (`parse_ddl`,
  `sql_type_from_token`, field lists, `decimal(p,s)` and interval spellings) with
  Python-faithful trim/int semantics and a `OnceLock`-cached regex set —
  `type_table.rs` re-exports the two public entries so call sites are unchanged.
  Round-2 remediation: `parse_py_int` is checked multiply/add and overflows to
  `TypeTableError::IntegerOverflow` (propagated through `parse_atomic_token`
  and the field-list parse rather than swallowed into a refusal).
  **TYPES-GEO-DDL-1 (2026-09-15):** the `parse_atomic_token` spatial arm
  (`geometry(...)` / `geography(...)`, case-insensitive keyword and `any`,
  inner whitespace): `any` maps to `SPATIAL_MIXED_SRID`, digits map on SRID
  membership, and anything else (bare tokens, unknown SRIDs, `-1`, CRS
  strings) falls through to the existing `cannot parse datatype` refusal.
  Unit battery in `type_table/tests.rs` (file-backed `#[cfg(test)]` module).
  Round 2: the capture is ASCII `[0-9]+` (Spark `INTEGER_VALUE`, not Python
  `int`) — underscores, signs and fullwidth digits refuse; leading zeros parse.
  pins: facade-4/C-010, C-020; types-geo-ddl-1/C-001, C-005
- `spark_ast.rs` — the Spark passthrough: ORDER BY null-placement defaults, eager analysis,
  eager DML/`COPY` commands (F-BR-2), SEC-02 gate call, the **G15 collation valve**
  (`refuse_type_position_collation_in_sql` on the raw executing-parse text, then
  `refuse_collation_in_statement` on the EXECUTING parse, plus `RESET` of a collation
  key — Q-001 pins this attach directly so the router cannot green-wash it), the **G3-E8 valve +
  identity-DELETE/UPDATE attach** (`try_allowed_delete_in` / `try_allowed_update_in` /
  `plain::try_allowed_plain_identity` →
  `execute_predicate_dml` for uncorrelated `DELETE … IN` / `NOT IN`, `[NOT] EXISTS` ±
  correlation, correlated IN, identity `UPDATE … IN`, and a three-part `DELETE … WHERE <comparison>`
  on a catalog in the session registry (branch-commit temp views live in `datafusion.public`
  and stay on the fork), else
  `refuse_dml_subquery_predicate_in_statement` on the EXECUTING
  parse — the only parse every DML route agrees on; the router's own parse is a different dialect), and the **G5b
  temporal-`RANGE` conformance call** (`conform_temporal_range_frames`, between planning and
  analysis — see `window_range.rs`; **W-4:** pre-plan `quote_unquoted_interval_range_bounds`
  for R1, plus `RestateIntervalBoundsAsNumeric` for R5). 6 in-module tests.
  pins: rp-9-repin-f23/C-005
  **ICE-LIST-NULL-2 (2026-09-19):** the plain-identity attach declines through
  `plain::plain_identity_needs_fork` after loading the target — a non-primitive
  selection returns `None` to the fork DELETE path. Door battery:
  [tests/list_null_compound.rs](tests/list_null_compound.rs).
  pins: ice-list-null-2/C-002, C-003
  **TYPES-1 (2026-09-05):** after eager analysis, plain-`INSERT` DML wraps narrowed `Int32`
  sources into `BIGINT` targets (`conform_insert_narrowed_ints`); every other shape passes
  through untouched. pins: types-1/C-001
  **FNP-8 (2026-09-06):** the EXECUTING parse takes `normalize::dialect_for_executing_parse`
  (Databricks when the SQL carries a lambda arrow, else the session dialect), so HOF calls
  with `x -> y` plan to the registered kernels. pins: fnp-8/C-004
- `time_window.rs` — **FNP-WIN-1 (2026-09-15):** the SQL-door `TimeWindowing`
  aliasing. A top-level `GROUP BY window(...)` stages the query over
  `SELECT *, window(...) AS window` so `window.start` / `window.end` resolve to
  a real column; the group and bare projection calls resolve to that column.
  Sliding values stay correct because the analyzer rule expands the staged
  projection. A second window specification refuses loud. Step 3 refuses a
  `window_time(...)` call in the staged projection / having / order-by with
  the oracle `[MISSING_AGGREGATION]` text, because the planner lifts it into a
  projection where the analyzer can no longer tell it apart from the legal
  post-aggregation use. Step 4 stages `GROUP BY session_window(...)` the same
  way (`SELECT *, session_window(...) AS session_window`); the analyzer rule
  strips the marker and rewrites stale parent `session_window` references.
  Remediation 16a stages inside CTE bodies, derived-table subqueries and both
  UNION branches by recursing the query/set tree; `time_window.rs` stays the
  only file this unit touches under `repark-spark`. Round 2 (2026-09-15):
  the second session spec refuses with Spark's `1039` text (shared constant
  from `repark_functions::spark_session_window`).
  pins: fnp-win-1/C-002, C-004, C-005, C-008
- `window_range.rs` — Spark temporal `RANGE` rules. Unit-less bounds over `TIMESTAMP` refuse;
  bounds over `DATE` restate as day intervals because DataFusion reads bare values as months.
  Negative and value-inverted frames retain Spark refusal/empty behavior; numeric-key interval
  bounds restate to numeric magnitude. Pins: [`tests/window_temporal_range.rs`](tests/window_temporal_range.rs);
  ledgers [`../../../task/g5b-temporal-range-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-12-g5b-temporal-range-ledger.md),
  [`../../../task/g5br-range-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md),
  [`../../../task/z4-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-z4-residuals-ledger.md),
  [`../../../task/w4-z-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md).
- `describe_show.rs` — Group Z `DESCRIBE NAMESPACE` + Group AB `SHOW NAMESPACES`
  (pyspark-4.0.0 v2-oracle-pinned rendering, LIKE patterns, secret redaction) +
  SQL-DESCRIBE-1 `DESCRIBE|DESC [TABLE] [EXTENDED|FORMATTED] catalog.namespace.table`
  (live-Spark-4.1.2-pinned rows, `bigint` via `spark_type_names`, secret redaction shared
  with the namespace path). The table parser leaves namespace forms, zero- and four-or-more-
  part names alone; the router arm only shadows registered catalogs, so
  temp views and DataFusion-native tables fall through. `EXTENDED` and `FORMATTED` share one
  flag; rows come from `table.metadata()` (Iceberg schema via `schema_to_arrow_schema`,
  default partition spec, location, properties plus a live `current-snapshot-id`, snapshot
  summary for `Statistics`, the session-built `Owner` below).
  **ICE-CATALOG-SESSION-1 S4 (2026-09-20):** `ShowNamespaces.catalog` is `Option` —
  the bare form lists the session's current catalog (NS-1 FIXED).
  pins: ice-catalog-session-1/C-018
  pins: sql-describe-1/C-001, C-002, C-003, C-004, C-005, C-006
  **REVIEW-FIX-5 (2026-09-10):** the table parser takes one-, two-, and three-part names and
  never filters the table segment of a three-part name, so `cat.ns.files` describes while a
  four-or-more-part metadata path still stays out; the router completes missing parts from
  the engine `datafusion.catalog.default_catalog` / `default_schema` (the same rule `SELECT`
  resolves by) before the registered-catalog check. `Owner` comes from the
  `repark_core::DescribeOwnerConfig` session extension, installed once by
  `ReparkSessionBuilder` (`unknown` when absent) — no environment read on the
  query path. Table Properties redacts
  through `repark_core::prop_key_is_secret` (widened to `pub` for this call; no second
  predicate): **deliberate Spark delta** — Spark prints `s3.access-key-id` in the clear and
  RePark redacts it; printing a credential is the worse divergence (RF-6).
  pins: review-fix-5/C-001, C-002, C-003, C-004, C-006
  **ICE-SYSTEM-FUNCTIONS-1 round 3 (2026-09-20):** the file also hosts the
  `<cat>.system.<fn>(` pre-parse rewrite (`rewrite_system_function_calls`,
  span surgery over `tokenize_with_location` in the `cast_map` shape) and the
  `SHOW [USER] FUNCTIONS IN <catalog>.system` parser + one-column `function`
  executor. The rewrite fires only for unquoted three-part calls whose head is
  a live Iceberg catalog and whose tail names one of the seven functions;
  strings, quoted identifiers, two-part `system.<fn>`, `CALL`, and unknown
  catalogs pass through untouched. The SHOW tail parser claims only an exact
  two-part `<cat>.system` name — one-part, non-`system`, and trailing-token
  forms all fall through to stock handling — and the router gates on a live
  catalog entry, so unknown catalogs keep DataFusion's old error. Unit pins
  live file-backed in [`describe_show/`](describe_show/map.md).
  pins: ice-system-functions-1/C-018, C-019, C-020, C-022, C-023, C-024
  **IPI-51 PR4 (2026-09-20):** the file also parses `SHOW PARTITIONS <table>` (a `SHOW` +
  `PARTITIONS` head check, so the sibling SHOW parsers keep their statements) and the router
  arm always refuses it through `catalog_ops::partition_management_unsupported` with the
  backticked table (`AnalysisException`, `SQLSTATE: 42601`) — Iceberg has no Hive partition
  catalog to list. The arm delegates to a `show_partitions_preparse` helper in `router.rs`
  so `try_preparse_intercepts` stays under clippy's line cap.
  pins: ice-error-conditions-1/C-011
  **IPI-51 PR5 (2026-09-20):** the file also hosts the four v2-command recognizers —
  `try_parse_set_serde` (ALTER TABLE … SET SERDE/SERDEPROPERTIES only),
  `try_parse_describe_as_json` (… AS JSON with EOF, never a NAMESPACE or HISTORY head),
  `try_parse_msck_repair` (MSCK REPAIR TABLE only) and `try_parse_analyze_table` (ANALYZE
  TABLE only, never DATABASE or TABLES) — and the router refuses each through
  `catalog_ops::not_supported_command_for_v2_table` with Spark's recorded command string.
  pins: ice-error-conditions-1/C-011
- `metadata_tables.rs` — I2 metadata-table path rewrite (`.snapshots` → `$snapshots`);
  19 in-module tests. **RP-1:** `METADATA_TABLE_NAMES` includes `position_deletes` (16th
  `MetadataTableType` at pin `5e7b2e4`); **RP-42:** fork #332 ports the scan, so it serves
  rather than refusing. **MW-4b:** Glue/HMS
  `table_exists` returns `DataInvalid` for a two-level namespace (not `NamespaceNotFound`).
  The "real table wins" probe on `cat.ns.tbl.snapshots` treats that as absent so the `$`
  rewrite runs; single-level `DataInvalid` and `Unexpected` stay fatal.
  **xo55-mt R1 (2026-09-22):** AS OF on a served metadata type rewrites (keeping the clause
  for the time-travel pass, quoting the `$` name so it re-tokenizes as one ident); only the
  five `all_*` types refuse, with Spark's `Cannot select snapshot in table` text.
- `time_travel.rs` — I1 SQL-text rewrite to snapshot-pinned providers. `PinnedViews` releases every
  statement-owned registration after planning; reader-option views remain owned by their frame.
  The shared `repark_core::time_travel::next_temp_view_name` counter prevents collisions. Pins:
  `tests/time_travel.rs::time_travel_temp_views_do_not_survive_a_successful_statement`,
  `…::time_travel_temp_views_do_not_survive_a_failed_statement`, and
  `…::time_travel_statement_pins_never_collide_with_a_reader_options_view`.
  It also resolves Spark's dotted READ selectors, `cat.ns.t.branch_b` and `cat.ns.t.tag_v`, onto
  the same pinned providers — a four-or-more-part name whose last segment carries the prefix and
  is not a metadata table, in any READ position — `FROM`, `JOIN`, and `MERGE`'s `USING`. A
  selector overlapping an `AS OF` span is dropped, because Spark does not accept that
  combination. Only the relation a statement WRITES to is out of reach: the router's
  write-to-branch sniff refuses that one first.
  **xo55-mt R1 (2026-09-22):** a span whose last part is `<table>$<meta>` loads the base
  table, resolves the `AS OF` value on it, and registers a `SnapshotMetadataTableProvider`
  (current-table, snapshot-scoped, or empty) through the shared temp-view helper.
  pins: ref-branch-tag-wap/C-002, C-007
  **ICE-TT-RESOLVE-1 (2026-09-19):** `parse_as_of_value` re-slices the original token stream
  (whitespace kept) so the shared evaluator receives parseable SQL; the expression evaluates
  as a constant in the session zone through `repark_core::time_travel::evaluate_sql_timestamp_asof`.
  pins: ice-tt-resolve-1/C-003
  **ICE-TT-RESOLVE-1 round 3 (2026-09-19):** a four-part `AS OF` name refuses
  before resolving — branch selectors with `Can't time travel in branch`, tag
  selectors with the selector/spec text. pins: ice-tt-resolve-1/C-002
  **xo55-bs R1 (2026-09-22):** a `VersionRef` span builds its provider through the fork's
  `try_new_from_table_ref` (current schema for a branch, snapshot schema for a tag) after
  the RePark resolution gatekeeper, so unknown refs keep the pinned refusal; snapshot-id
  and timestamp spans keep `try_new_from_table_snapshot`.
  pins: ipi-07-branch-read-schema-1/C-001, C-003, C-004, C-005, C-006, C-008
- `local_fs_ddl.rs` — SEC-02 local-filesystem DDL gate; 9 in-module tests.
- `catalog_ops.rs` — catalog lookup, P11 refusals, `iceberg_err`, path-escape rejection, and
  `reregister*` provider invalidation. **IPI-21/IPI-25 (2026-09-20):** `table_or_view_not_found`
  is the single home of Spark's `[TABLE_OR_VIEW_NOT_FOUND]` text for a three-part name, condition
  and `SQLSTATE: 42P01` included; `describe_show.rs`, `normalize/replace_table.rs`,
  `namespace_ddl/purge.rs` and `namespace_ddl.rs` DROP all answer through it, so they cannot
  drift apart. **IPI-51 (2026-09-20):** `table_or_view_already_exists` is the sibling home of
  Spark's `[TABLE_OR_VIEW_ALREADY_EXISTS]`/`SQLSTATE: 42P07` text; the `create_table.rs` and
  `ctas.rs` already-exists arms answer through it. **IPI-51 PR4 (2026-09-20):**
  `partition_management_unsupported` is the sibling home of Spark's
  `[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED]`/`SQLSTATE: 42601` text
  over a caller-backticked table display (`quoted_table_display` backticks each name part);
  the `truncate.rs` PARTITION arm answers through it. **IPI-51 PR5 (2026-09-20):**
  `not_supported_command_for_v2_table` is the sibling home of Spark's
  `[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE]`/`SQLSTATE: 0A000` text over a caller-supplied
  command string (newlines flattened); the four v2-command router intercepts answer through it.
  pins: ipi-21-25-42-small-parser/C-007, C-008; ice-error-conditions-1/C-011
- `use_ddl.rs` — **ICE-CATALOG-SESSION-1 (2026-09-20):** the session-defaults seam:
  `session_defaults` / `set_session_defaults` over the registry box
  (`CatalogRegistry::current_defaults` / `set_defaults`, seeded `spark_catalog` /
  `default`; `set` also mirrors the `SessionDefaults` carrier `current_catalog()` reads),
  the one `complete_name` 1/2/3-part resolver every short-name call site shares, and
  `execute_use` (catalog-first one-part rule per live-Spark probe P-1, per-type default
  namespace, `SCHEMA_NOT_FOUND` texts, `USE CATALOG` parse refusal, `USE DEFAULT`
  pre-parse). Planner `default_catalog` / `default_schema` stay at the DataFusion
  builtins; a raw `SET` of either key still lands there and mirrors that side into the
  box (R6). `rename_dest` anchors short `RENAME TO` targets on the source table (T-3);
  the ALTER / CALL / CREATE / CTAS / DROP call sites complete through `complete_name`.
  S4 adds the `SHOW CATALOGS` / `SHOW TABLES` / `SHOW COLUMNS` executors (sorted
  names, ambient scope from the session defaults, `LIKE`-glob suffix, `TERSE` /
  `EXTENDED` / `FULL` parse refusals) and the `SHOW TABLES IN` scope resolver
  (catalog-first, like `USE`). S5 adds native `REFRESH [TABLE] name` /
  `REFRESH 'path'` (temp views and paths answer ok; missing tables refuse
  `TABLE_OR_VIEW_NOT_FOUND`; hits rebuild the catalog provider).
  pins: ice-catalog-session-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-015, C-016, C-017, C-018, C-019, C-022, C-023
- `matrix.rs` — the Q13 surface matrix maps every `repark_common::surfaces` ID to a tested row or
  an explicit absence. `CROSS_DOOR_EQUIVALENCE` uses the `TwoSession` profile and keeps its
  cross-door evidence in `crates/repark-sql/tests/cross_door.rs`.
- `lib.rs` — manifest: module declarations, public re-exports, domain-module imports, and the
  `#[cfg(test)]` root imports required by the unit battery's `use super::*`.
- `tests/` — production-aligned unit modules with shared fixtures in `common.rs`; see
  [tests/map.md](tests/map.md). Pins include
  `tests/ref_ddl.rs::ref_ddl_if_exists_spellings_run_and_unknown_trailing_clauses_still_refuse`,
  `tests/metadata_tables.rs::metadata_tables_spark_dot_form_and_guards`, and
  `tests/metadata_tables.rs::metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door`.

**Where the refusal messages point.** Several modules here name
`docs/spark-sql-iceberg-parity.md` (the **divergence registry**) in their loud-refusal text —
`router.rs`, `normalize.rs`, `metadata_tables.rs`, `ref_ddl.rs`, `insert_overwrite.rs`. The
registry holds the semantics of each of those gaps (repark's behavior, Apache Spark's, the pin,
the rationale); this map links, it does not restate. A refusal message that cites a section is
part of that section's pin — changing either one changes both.


## I want to...

| ...do this | go to |
|---|---|
| Trace statement routing / targeted refusals | `router.rs` |
| MERGE lowering / star sentinel / MG-2 door refusals | `merge.rs` |
| INSERT OVERWRITE probe / stage-then-swap | `insert_overwrite.rs` |
| Branch/tag DDL, write-to-branch sniff | `ref_ddl.rs` |
| Maintenance CALL procedures | `call.rs` |
| `rewrite_manifests` counts, guards and Spark's no-op rule | [`call/`](call/map.md) |
| CTAS lowering / location policy | `ctas.rs` |
| Column-def CREATE / type mapping | `create_table.rs` |
| ALTER TABLE / token rewrites | `alter.rs` |
| Namespace / DROP TABLE DDL | `namespace_ddl.rs` |
| Pin a router behavior end to end | [`tests/`](tests/map.md) (lib-root battery, by production module) |
| Find why a statement form refuses, and whether it is declared | `../../../docs/spark-sql-iceberg-parity.md` §2 |
| Pin a door-native gate (P11/BUG-010) | `router/tests.rs` |
| `TRUNCATE TABLE` | `truncate.rs` (pins: `tests/truncate.rs`) |
| ORDER BY / eager-command passthrough semantics | `spark_ast.rs` |
| Temporal / unit-less `RANGE` window-frame semantics | `window_range.rs` |
| Namespace introspection rendering | `describe_show.rs` |
| Time-travel span scanning | `time_travel.rs` (pin half: `repark-core/src/time_travel.rs`) |
| See what this door ships vs deliberately does NOT | `matrix.rs` (the Q13 surface matrix) |

**FNP-4B (2026-09-15), in flight:** `apply_spark_parser_dialect` is wired in
`SparkExtension::configure` (step 1 of the card); generated SQL still uses ANSI double-quoted
identifiers, which the Databricks lexer reads as string literals — D-2 moves that write-path
contract to backticks. Ledger: `task/ledgers/staging/fnp-4b-ledger.md`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Statement unexpectedly passes to DataFusion | `router.rs` arm order; `normalize::parse_single_normalized` returned `None` |
| Time-travel clause not rewritten | `time_travel::sql_has_time_travel` span scan (comments/strings never match) |
| A `__repark_tt_*` name appeared in `SHOW TABLES` / `information_schema.tables` | Identify the producer BEFORE calling it anything: from either SQL door it is a LEAK, from the reader-options path it is the DOCUMENTED RESIDUAL and must be left alone. Three producers, one shared prefix — the bullet below tells them apart |
| P11 refusal missing | read-only set threading: `execute_with_read_only` → registry snapshot |
| A `DELETE`/`UPDATE` with a subquery `WHERE` was refused | By design (G3-E8): `normalize::refuse_dml_subquery_predicate`. It over-refuses the uncorrelated-scalar spelling on purpose — the correlated twin is the same parse tree and destroys the table |
| A `DELETE`/`UPDATE` with a subquery `WHERE` was NOT refused | Ask FIRST which parse saw it. The valve's load-bearing call is in `spark_ast::execute_passthrough`, on the statement the executing dialect parsed (`normalize::dialect_for_executing_parse` — the session dialect unless the SQL carries a lambda arrow); the router arms' call is an early duplicate for valve ORDER only. If `execute_passthrough` planned a `Statement::Delete`/`::Update` and the valve did not fire, the predicate genuinely carries no `Query` node — e.g. the subquery sits in an `UPDATE … SET` assignment, which is deliberately ungated (correct, or a loud plan error — never silently wrong). If it never reached `execute_passthrough` as a `Delete`/`Update` statement at all, see the row below |
| **A DML statement executed WITHOUT any router arm running (the fail-open attachment class)** | This router parses with `DatabricksDialect`; the executor re-parses with the SESSION dialect. Every form the two disagree about — Spark's FROM-less `DELETE <table> WHERE …` is the live one — fails `parse_single_normalized`, falls through `execute_unparsable_fallthrough`, and is planned from the SECOND parse. **A DML guard attached to a router arm is fail-open by construction**; attach it inside `spark_ast::execute_passthrough` (which is what G3-E8 does — panel finding L1 M-1). The same trap applies to `Statement::Query`-shaped DML: `WITH … DELETE` never reaches either `Delete` arm (loud `NotImplemented` today, pinned by `tests::dml::g3e8_cte_prefixed_dml_is_loud_today_and_writes_nothing`) |
| A `RANGE` window answered a wider/narrower window than Spark | `window_range.rs`: is the bound unit-less? over a datetime key a bare number is Arrow's MONTHS, which is why it is refused (TIMESTAMP) or restated as days (DATE). A mixed numeric/DATE statement is deliberately left alone. A **negative or value-inverted** interval over TIMESTAMP must be Spark's empty frame (R3 — kind *or* same-kind magnitude after sign-normalize); `DAY TO SECOND` must restate, not Arrow-parse-fail (R2). Mixed inverted-TIMESTAMP + numeric-bare refuses (`UNSUPPORTED.NEGATIVE_RANGE_OFFSET`). W-4: unquoted `INTERVAL 1 DAY` is quoted pre-plan (R1); interval-over-int restates to numeric `n` (R5); R4 FOLLOWING-to-FOLLOWING stays recorded (`task/w4-z-residuals-ledger.md`) |
| `DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE` where Spark answers | the order key resolved to `Timestamp`, not `Date` — Spark refuses that spelling too; use `INTERVAL '<n>' DAY` |
| `matrix::matrix_maps_every_surface` RED | a surface ID was added to `repark_common::surfaces::ALL` with no row here — add `Tested`/`DeliberatelyAbsent` |
| Doc comment names a crate that does not exist | report the stale pointer and update it to the owning crate |

First checks: `cargo test -p repark-spark <module>::`. Escalate to: [../map.md#debug](../map.md).

- **The `__repark_tt_` prefix has THREE producers, and ONE minter** (H-1b). Tell them apart before
  calling a leftover a leak in *this* door:
  1. **The Spark rewrite** — `time_travel.rs`, this crate. Releases via `PinnedViews` in
     `router::execute_with_read_only`, on every `?` / `return` path (not unwind / future-drop,
     which no code path produces today). A leftover here means a new early return was added
     between the `PinnedViews::default()` and the `pinned.release(ctx)`.
  2. **The reader-options path** — `repark_core::session`'s
     `spark.read.option("snapshot-id" | "as-of-timestamp" | "branch" | "tag", …)`, which calls
     `repark_core::time_travel::read_table_at` and **keeps** the registration: that view backs the
     `DataFrame` handed to the user and has no statement boundary to release at. This is a
     DOCUMENTED RESIDUAL, not a bug, and it is what makes the facade pin
     `python/repark/tests/test_time_travel.py::test_time_travel_temp_views_hidden_from_list_tables`
     non-vacuous (the `listTables` prefix filter has something real to hide).
  3. **The ANSI door** — `repark-sql`'s `FOR … AS OF` composes its `__repark_ansi_tt_<n>` view
     over the same `read_table_at`, so it minted a `__repark_tt_<n>` underneath. It leaked until
     H-1b; `repark_sql::time_travel::register_pinned_view` now records BOTH names in the ANSI
     ledger, and `crates/repark-sql/tests/introspection.rs` asserts both prefixes.

  **The three share ONE process-global counter**, `repark_core::time_travel::next_temp_view_name`.
  Do not add a second minter. The pin
  `tests/time_travel.rs::time_travel_statement_pins_never_collide_with_a_reader_options_view`
  asserts reader-view survival and sequence separation.

  To tell a leftover from a fixture, run the statement/read in isolation and compare
  `leftover_time_travel_views` (test helper in `tests/time_travel.rs`) before and after.
- **`__repark` is an engine-reserved name prefix** — user tables and views must not use it. The
  mint step deregisters an occupied name before registering the pinned provider. This is required
  because the schema provider rejects duplicate registration; do not remove that cleanup.
- **FNP-4B remediation (2026-09-15):** added code comments in `spark_literals.rs`, `spark_rewrites.rs`, `normalize.rs` removed (ruling 2026-08-26); the pre-existing one-line `spark_literals.rs` module doc that `python/repark-parity/tests/test_sqp_1_record.py` pins is kept verbatim.

## IPI-19 + IPI-37 (2026-09-20) — the merge-schema decision

- `write_options.rs` — `mergeSchema` and Iceberg's `merge-schema` are
  recognised write-option keys (boolean, last-wins, case-insensitive like the
  rest; any value but `true` is `false`, as Java's `Boolean.parseBoolean`
  reads it). `StatementWriteOptions::merge_schema(ctx)` is the precedence:
  **per-write option > session conf `spark.sql.iceberg.merge-schema` > false**,
  matching Java `SparkWriteConf.mergeSchema()`'s `confParser` order.
  `refuse_if_non_empty` now skips those two keys, because the by-name append
  honours them — every other unrecognised key is still ignored, which is
  Spark's measured behaviour and registry row `ICE-WRITE-OPTIONS-1`; this unit
  does not reopen it.
- `insert_by_name.rs` + [`insert_by_name/evolution.rs`](insert_by_name/evolution.rs)
  — extra source columns are a schema-evolution question, decided in
  `evolution::columns_to_add` **before** the arity and name arms run, so the
  three measured outcomes come out of three different places and keep their own
  error classes: without `write.spark.accept-any-schema` nothing changes and the
  existing arms answer (`INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS` /
  `21S01` when the source is wider, `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS`
  / `KD000` when it is not); with the property but no flag, Java's
  `IllegalArgumentException: Field <name> not found in source schema`; with
  both, the extras join the projection and the append evolves. The conf is inert
  without the property — that asymmetry is Spark's, and it is the single thing
  most easily got wrong here.
  pins: ipi-19-56-37-schema-evolution-write/C-002, C-004, C-011
- `merge.rs` + [`merge/`](merge/map.md) — the `MERGE WITH SCHEMA EVOLUTION`
  pre-parse strip and the `schema_evolution` flag it carries into the lowered
  plan. The star-sentinel rewrite moved to `merge/stars.rs` untouched to keep
  `merge.rs` off the size ceiling. A plain `MERGE INTO` never matches the strip.
  pins: ipi-19-56-37-schema-evolution-write/C-005, C-007
- `time_travel.rs` — **ICE-METADATA-COLS-1 WO-R1 (2026-09-21):** the Spark-door `snapshot_id_<id>`
  and `at_timestamp_<ms>` ref selectors resolve to snapshot pins beside `branch_`/`tag_`; an
  unparsable numeric suffix refuses `IllegalArgumentException` naming the selector, never
  table-not-found, and `branch_`/`tag_` behaviour is unchanged.
  pins: ice-metadata-cols-1/C-011, C-012, C-013, C-014
