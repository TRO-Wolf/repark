# map — repark-spark/src

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Router canonicalize reasons and spark_literals rule tokens restored as complete one-line comments. Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. DDL, DML, reference, maintenance, metadata, time-travel, and passthrough handlers
share the session and catalog seams. The MoR valve predicate is owned by
`repark_iceberg::write`; `normalize.rs` keeps the resolution wrapper.
Source documentation may retain model provenance; code-quality grade tags stay outside code.
pins: rp-3-fork-repin/C-010

## Contents

- `router.rs` — `execute` / `execute_with_read_only` / `execute_time_travelled` / `execute_inner`
  + pre-parse intercepts (alter I6/I7, create-namespace, describe/show, ref DDL) + the
  write-to-branch sniff; full router arm set ([router/map.md](router/map.md) for the tests).
  `execute_time_travelled` is a **release seam, not a routing step** (H-1b): it exists so
  `execute_with_read_only` can own a `time_travel::PinnedViews` and release it on every `?` /
  `return` path of the rewrite — see the `time_travel.rs` row below. V3R-1: DELETE / UPDATE call
  `refuse_v3_cow_dml` after the BUG-001 valve; a v3 COW DELETE after an overwrite snapshot refuses
  before fork lineage divergence. SQP-1: the front door canonicalizes escapes once and
  translates downstream parser locations back to the caller's SQL.
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); 24 in-module tests (MG-2: M2 Oracle sub-predicates, M3
  assignment-target qualification, M8 INSERT column list, M10 non-last
  unconditional clause).
- `insert_overwrite.rs` — INSERT OVERWRITE: empty probe/validate/provider-wipe (C1-Q-001) +
  non-empty stage-then-swap; 2 in-module tests (`assignment_type_unit_tests`).
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff; 14 in-module tests.
- `call.rs` — seven maintenance procedures: six maintenance calls plus `register_table`. Each
  preserves Spark's result schema and count sources. Orphan removal requires `older_than`, defaults
  `dry_run` to true, and refuses shared fallback roots; rewrite-position-delete still refuses
  live Puffin DVs (`B-MOR-3`; RP-3 C-007 measured R136 as a parquet-to-DV conversion no-op);
  rewrite-data-files refuses v3 lineage loss. Details and test pointers:
  [call/map.md](call/map.md).
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
  **V3-2:** `format-version` is consumed at parse and resolved at execute against
  `repark.sql.allowCreateFormatVersion3` (same helper as column-def CREATE).
  **SE-1 PR-D1:** refuses Iceberg CREATE when any `TableScan` source (including
  expression subqueries, R-B) is tighten-derived AND the output has a
  non-nullable field (R-D), or the output schema still carries the tag. The
  write-boundary relax is PR-D2 (via the same source walk).
- `spark_ast.rs` — **SE-1 D1:** after the SEC-02 plan guard,
  calls the shared belt's `repark_core::PreExecute::guard` (which owns
  `refuse_iceberg_create_of_tightened_ddl`) so `CREATE VIEW cat.ns.v AS …` and
  `SELECT … INTO cat.ns.t` — both of which reach here through the router's `_ =>` catch-all —
  cannot persist a required column from a tighten-derived source — including the one- and
  two-part spellings that resolve into an Iceberg catalog via `SET
  datafusion.catalog.default_catalog` (Z-1). Untightened `CREATE VIEW` behaviour is
  unchanged (that it persists an Iceberg table at all predates this branch). **SQP-1:**
  `rewrite_binary_casts` maps `CAST(x AS BINARY)` → `BYTEA`; `refuse_illegal_binary_cast` refuses a
  numeric/bool/date/decimal source on the planned tree (Spark `DATATYPE_MISMATCH`, else silent
  int→bytes), threading the cast kind (module doc for the message contract).
- `spark_literals.rs` — **SQP-1:** `canonicalize(sql) -> Cow<str>`, the front-door pass that rewrites
  Spark string-literal escapes once (rule table, dialect, design in the module doc). Sole caller
  `router::execute_with_read_only` (grep-pinned); DataFusion-native `COPY` / `CREATE EXTERNAL TABLE`
  are skipped (their `OPTIONS ('k' 'v')` is a key/value pair, not Spark concatenation). The parser
  maps the passthrough parser's reachable `SQL` and `Diagnostic(SQL)` errors from canonical text to
  original source. Planning, execution, shared, and collection errors remain unchanged; a boundary
  pin holds this contract. Secondary rewrites stop mapping only when their SQL bytes change.
- `create_table.rs` — column-def `CREATE TABLE` (I5 schema-only staged create) + the
  Spark-SQL→iceberg type mapping; **V3-2:** `iceberg_create_format_version` (session opt-in;
  `Model: Grok 4.6 xHigh`);
  default `TIMESTAMP` → Iceberg `timestamptz`,
  `TIMESTAMP_NTZ` stays `timestamp` (live Spark 4.1.2 CREATE probe). **Q10:** bare
  `TIMESTAMP` follows `spark.sql.timestampType` (`TIMESTAMP_NTZ` → Iceberg `timestamp`);
  existing `sql_type_to_iceberg` wrapper stays LTZ so default-mode pins are untouched.
  4 in-module tests (`type_mapping_tests`) + `tests/create_table.rs` pin + CTAS type smoke.
- `alter.rs` — ALTER TABLE handlers (SET/UNSET TBLPROPERTIES, RENAME TO, schema evolution I6,
  I7 partition-field DDL, residual refusals) + the ALTER token rewrites the normalizer runs;
  9 in-module tests. **Q10:** ADD/ALTER COLUMN bare `TIMESTAMP` follows the session
  `spark.sql.timestampType` carrier. REPLACE COLUMNS stays on the LTZ wrapper
  (parse-time, no session).
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`. `IF NOT EXISTS` checks location consistently:
  matching/no-location requests stay idempotent; contradictory `LOCATION` fails loud naming both
  paths (`repark_core::refuse_contradictory_namespace_location`).
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into the positional `execute_with_read_only` call; `#[async_trait(?Send)]` matches the
  core trait; install with `ReparkSessionBuilder::with_sql_dialect` + `SparkExtension`).
  Tests: [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension` owns Spark session defaults and installs the ordered
  `InsertStoreAssignment`, function registry, analyzer rules, and composed `TaExtension`. It also
  carries the session timezone and Spark decimal settings. Tests:
  [extension/map.md](extension/map.md) and [../tests/session_timezone.rs](../tests/session_timezone.rs).
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
  star rewrite call, partition-spec builders. V3R-1: `refuse_v3_cow_dml`, the `V3-COW-1` passthrough seat (lifted for the
  plain-`WHERE` DELETE on DV-free v3 tables); `dml_target_ident` (shared
  with the BUG-001 valve) completes short names from the session defaults (SEC-001). A v3 COW
  DELETE after an overwrite snapshot stays a pre-write V3-COW-1 refusal (RP-3 C-004 C7).
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
- `spark_ast.rs` — the Spark passthrough: ORDER BY null-placement defaults, eager analysis,
  eager DML/`COPY` commands (F-BR-2), SEC-02 gate call, the **G15 collation valve**
  (`refuse_type_position_collation_in_sql` on the raw executing-parse text, then
  `refuse_collation_in_statement` on the EXECUTING parse, plus `RESET` of a collation
  key — Q-001 pins this attach directly so the router cannot green-wash it), the **G3-E8 valve +
  identity-DELETE/UPDATE attach** (`try_allowed_delete_in` / `try_allowed_update_in` →
  `execute_predicate_dml` for uncorrelated `DELETE … IN` / `NOT IN`, `[NOT] EXISTS` ±
  correlation, correlated IN, and identity `UPDATE … IN`, else
  `refuse_dml_subquery_predicate_in_statement` on the EXECUTING
  parse — the only parse every DML route agrees on; the router's own parse is a different dialect), and the **G5b
  temporal-`RANGE` conformance call** (`conform_temporal_range_frames`, between planning and
  analysis — see `window_range.rs`; **W-4:** pre-plan `quote_unquoted_interval_range_bounds`
  for R1, plus `RestateIntervalBoundsAsNumeric` for R5). 6 in-module tests.
- `window_range.rs` — Spark temporal `RANGE` rules. Unit-less bounds over `TIMESTAMP` refuse;
  bounds over `DATE` restate as day intervals because DataFusion reads bare values as months.
  Negative and value-inverted frames retain Spark refusal/empty behavior; numeric-key interval
  bounds restate to numeric magnitude. Pins: [`tests/window_temporal_range.rs`](tests/map.md);
  ledgers [`../../../task/g5b-temporal-range-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-12-g5b-temporal-range-ledger.md),
  [`../../../task/g5br-range-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md),
  [`../../../task/z4-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-z4-residuals-ledger.md),
  [`../../../task/w4-z-residuals-ledger.md`](../../../task/ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md).
- `describe_show.rs` — Group Z `DESCRIBE NAMESPACE` + Group AB `SHOW NAMESPACES`
  (pyspark-4.0.0 v2-oracle-pinned rendering, LIKE patterns, secret redaction).
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
| Pin a door-native gate (TRUNCATE/P11/BUG-010) | `router/tests.rs` |
| ORDER BY / eager-command passthrough semantics | `spark_ast.rs` |
| Temporal / unit-less `RANGE` window-frame semantics | `window_range.rs` |
| Namespace introspection rendering | `describe_show.rs` |
| Time-travel span scanning | `time_travel.rs` (pin half: `repark-core/src/time_travel.rs`) |
| See what this door ships vs deliberately does NOT | `matrix.rs` (the Q13 surface matrix) |

**FNP-4a — `apply_spark_parser_dialect` is present but not wired.** Generated SQL still uses ANSI
double-quoted identifiers, which Spark parsing treats as string literals. Fix that write-path
contract before enabling the helper.

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
| A `DELETE`/`UPDATE` with a subquery `WHERE` was NOT refused | Ask FIRST which parse saw it. The valve's load-bearing call is in `spark_ast::execute_passthrough`, on the statement the session dialect parsed; the router arms' call is an early duplicate for valve ORDER only. If `execute_passthrough` planned a `Statement::Delete`/`::Update` and the valve did not fire, the predicate genuinely carries no `Query` node — e.g. the subquery sits in an `UPDATE … SET` assignment, which is deliberately ungated (correct, or a loud plan error — never silently wrong). If it never reached `execute_passthrough` as a `Delete`/`Update` statement at all, see the row below |
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
