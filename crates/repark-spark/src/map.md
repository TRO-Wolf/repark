# map — repark-spark/src

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
- `router.rs` — `execute` / `execute_with_read_only` / `execute_static_overwrite` / `execute_routed` / `execute_time_travelled` / `execute_inner`
  + pre-parse intercepts (alter I6/I7, write-order DDL, create-namespace, describe/show, ref DDL) + the
  write-to-branch sniff; full router arm set ([router/map.md](router/map.md) for the tests).
  `execute_time_travelled` is a **release seam, not a routing step** (H-1b): it exists so
  `execute_with_read_only` can own a `time_travel::PinnedViews` and release it on every `?` /
  `return` path of the rewrite — see the `time_travel.rs` row below. **V3-4:** after time
  travel, `prepare_lineage_sql` pins v3 `_row_id` / `_last_updated_sequence_number` onto a
  temp provider for single-table reads (`LineagePins` released with the time-travel views);
  JOIN/CTE/subquery/time-travel naming lineage refuse `V3-ROWID-2`. RP-6: plain-`WHERE`
  UPDATE/DELETE are Spark-equal. V3-7: MERGE keeps `_row_id`; subquery-WHERE DML still
  refuses `V3-COW-1`.
  SQP-1: the front door canonicalizes escapes once and
  translates downstream parser locations back to the caller's SQL.
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); MATCHED / NOT MATCHED / NOT MATCHED BY SOURCE (DML-A);
  in-module tests (MG-2: M2 Oracle sub-predicates, M3
  assignment-target qualification, M8 INSERT column list, M10 non-last
  unconditional clause). pins: dml-a-merge-not-matched-by-source/C-005
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
- `insert_by_name.rs` — `INSERT … BY NAME` (ICE-RTAS-BYNAME-1, 2026-09-17): the token-level
  strip (sqlparser has no `BY NAME`), the count-first Spark error rule, the positional
  projection build, the staged-append executor (stream → conform → `commit_append_to` →
  reregister) and the overwrite delegation to `insert_overwrite_from_staged_source`. Branch
  targets count as owned write heads (`write_to_branch.rs`), so no temp-view rewrite fires.
  In-module tests (file-backed in [insert_by_name/map.md](insert_by_name/map.md)).
  pins: ice-rtas-byname-1/C-001, C-002, C-003, C-004
  **Round 2 (2026-09-17):** `PARTITION` shapes delegate to the positional
  partition arm (static overwrite) or inject clause literals (static append);
  a PARTITION-less `BY NAME` overwrite follows `partitionOverwriteMode` (see round 3);
  missing required targets
  refuse `CANNOT_FIND_DATA`; matching honours `spark.sql.caseSensitive`
  (matching plus projection live in `plan_name_projection`).
  pins: ice-rtas-byname-1/C-007, C-008, C-009, C-010
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
- `truncate.rs` — whole-table `TRUNCATE TABLE` (DML-C): delete-only `commit_truncate_to`;
  PARTITION / IF EXISTS / missing TABLE / multi-target refuse. Pins:
  [tests/truncate.rs](tests/truncate.rs). pins: dml-c-truncate/C-002, C-005, C-006, C-007
  pins: rp-5-fork-repin/C-004
- `write_to_branch.rs` — Spark-door write-to-branch routing: tag/missing-branch Spark-shaped
  refuse; two-part names qualify through session defaults; the MOR valve runs on the
  Iceberg ident before the temp rewrite; fork-executed INSERT/UPDATE/DELETE via
  `IcebergTableProvider::with_commit_branch` registered on `datafusion.public`;
  MERGE / INSERT OVERWRITE / TRUNCATE rewrite short names to four-part then `.to_branch`.
  `split_write_ref_parts` sniffs four-part names and two-part `branch_`/`tag_` names;
  a three-part table whose last segment starts with `branch_` is an ordinary table.
  pins: rp-5-fork-repin/C-004
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff. Its 14 in-module tests are file-backed in
  [ref_ddl/map.md](ref_ddl/map.md); the module path, and so every pin name, is unchanged.
  `WITH SNAPSHOT RETENTION` takes BOTH halves — `n SNAPSHOTS` then an optional
  `k DAYS|HOURS|MINUTES` — because Spark's grammar does; the reversed order is a Spark parse
  error and refuses here too. Write-to-branch routing lives in `write_to_branch.rs` (RP-5):
  the sniff still locates the statement's ONE write target. Registry rows: `REF-1` FIXED,
  `REF-3` BACKLOG, `REF-4` FIXED.
  pins: ref-branch-tag-wap/C-003, C-004, C-006, C-007
  pins: rp-5-fork-repin/C-004
- `call.rs` — fourteen maintenance procedures: thirteen maintenance calls plus `register_table`. Each
  preserves Spark's result schema and count sources. Orphan removal requires `older_than`, defaults
  `dry_run` to true, and refuses shared fallback roots; on a `ServiceManagedLocation`
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
  unwired keys loud.
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
- `call/branch_ops.rs` — **ICE-BRANCH-OPS-1 (2026-09-17):** `fast_forward`,
  `cherrypick_snapshot`, `set_current_snapshot`, `rollback_to_timestamp` (Spark 4.1.2
  parity, oracle-pinned in `python/repark/tests/branch_ops_1_truth.json`).
  Details: [call/map.md](call/map.md).
  pins: ice-branch-ops-1/C-001, C-002, C-003, C-004, C-007, C-010
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
  **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** the service-managed abort arm skips `drop_table`
  and returns the original error unwrapped when `is_commit_state_unknown` fires — a
  possibly-landed create is never abort-dropped, and the class + `operation_id` reach the
  caller; definite kinds keep the drop-and-explain abort.
  pins: ice-commit-unknown-1/C-001, C-003, C-004
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
- `spark_rewrites.rs` — **FNP-4B (2026-09-15):** numeric suffixes (BD precision/scale from
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
  **FNP-4B round 7 (2026-09-15):** angle-bracket `ARRAY<T>` maps to an Iceberg
  list with nullable `element` fields and table-unique ids from a checked
  allocator (R-16b-21 grant); bare/square-bracket forms still refuse.
  pins: fnp-4b/C-025 (round-7 fmt/clippy follow-ups carry no behavior change)
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
  `spark.sql.timestampType` carrier. REPLACE COLUMNS stays on the LTZ wrapper
  (parse-time, no session). **ICE-COLUMN-REORDER-1 (2026-09-17):** the I6 move refusal
  is gone; the move lives in `column_move.rs`.
- `column_move.rs` — **ICE-COLUMN-REORDER-1 (2026-09-17, round 2 Q-20b-5):** the `ALTER COLUMN …
  FIRST|AFTER` pre-parse (`try_parse_column_move_ddl` / `execute_column_move_ddl`, wired in
  `router.rs` ahead of the residual refusal): an `ALTER`-prefix fast path before any tokenize,
  dotted `AFTER` references refuse with Spark's `[PARSE_SYNTAX_ERROR]`, nested paths resolve,
  every move commits through one loaded table (`apply_schema_changes_on_table`), unknown names
  refuse with Spark's `UNRESOLVED_COLUMN` framing. A sibling module, not an `alter.rs` arm,
  because that file sits at its exact ceiling. 4 in-module tests + [`tests/column_move.rs`](tests/map.md).
  pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-014
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
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`. `IF NOT EXISTS` checks location consistently:
  matching/no-location requests stay idempotent; contradictory `LOCATION` fails loud naming both
  paths (`repark_core::refuse_contradictory_namespace_location`).
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into the positional `execute_with_read_only` call; `#[async_trait(?Send)]` matches the
  core trait; install with `ReparkSessionBuilder::with_sql_dialect` + `SparkExtension`).
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
  inline in the module; door pins are [`tests/lambda_door.rs`](tests/map.md).
  pins: fnp-8/C-004
- `call_args.rs` — CALL argument bag, scalar coercions, and quoted-name keys for dashed options.
- `collation.rs` — **G15:** parse-altitude collation refuse. Walks
  `Expr::Collate`, column-def `COLLATE`, `CREATE`/`ALTER COLLATION`, `SET NAMES COLLATE`,
  session `SQLConf` keys containing `collation` (including `ParenthesizedAssignments`),
  type-position `STRING COLLATE` (Spark `CAST(x AS STRING COLLATE name)`), and
  `RESET` of a collation key. `refuse_collation_in_statement` is called from
  `spark_ast.rs` (executing parse) and the router's successful parse (intercepted
  CREATE/ALTER). `refuse_collation_in_sql` is `pub` for the Python binding (`F.expr`,
  `filter_sql`). Pins: [`tests/collation.rs`](tests/map.md). Ledger:
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
  bounds restate to numeric magnitude. Pins: [`tests/window_temporal_range.rs`](tests/map.md);
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
- `metadata_tables.rs` — I2 metadata-table path rewrite (`.snapshots` → `$snapshots`);
  19 in-module tests. **RP-1:** `METADATA_TABLE_NAMES` includes `position_deletes` (16th
  `MetadataTableType` at pin `5e7b2e4`; scan is fork schema-only). **MW-4b:** Glue/HMS
  `table_exists` returns `DataInvalid` for a two-level namespace (not `NamespaceNotFound`).
  The "real table wins" probe on `cat.ns.tbl.snapshots` treats that as absent so the `$`
  rewrite runs; single-level `DataInvalid` and `Unexpected` stay fatal.
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
  pins: ref-branch-tag-wap/C-002, C-007
- `local_fs_ddl.rs` — SEC-02 local-filesystem DDL gate; 9 in-module tests.
- `catalog_ops.rs` — catalog lookup, P11 refusals, `iceberg_err`, path-escape rejection, and
  `reregister*` provider invalidation.
- `matrix.rs` — the Q13 surface matrix maps every `repark_common::surfaces` ID to a tested row or
  an explicit absence. `CROSS_DOOR_EQUIVALENCE` uses the `TwoSession` profile and keeps its
  cross-door evidence in `crates/repark-sql/tests/cross_door.rs`.
- `lib.rs` — manifest: module declarations, public re-exports, domain-module imports, and the
  `#[cfg(test)]` root imports required by the unit battery's `use super::*`.
- `tests/` — production-aligned unit modules with shared fixtures in `common.rs`; see
  [tests/map.md](tests/map.md). Pins include
  `tests/ref_ddl.rs::ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud`,
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
