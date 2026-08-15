# map — repark-spark/src

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. Module bodies are ported from v1 `repark-sql` at the port-source pin (declared edit
classes only: prefix renames, seam adaptation). PR-3a restored the DDL handler modules
(`ctas`, `create_table`, `alter`, `namespace_ddl`) and completed `catalog_ops`/`normalize`;
PR-3b restored the DML/ref modules (`merge`, `insert_overwrite`, `ref_ddl`, `call`) — the
router now matches v1's execute family end-to-end. The MoR BUG-001 valve predicate is hoisted
to `repark_iceberg::write` (PR-3b declared rename); `normalize.rs` keeps the resolution
wrapper.

## Contents

- `router.rs` — `execute` / `execute_with_read_only` / `execute_time_travelled` / `execute_inner`
  + pre-parse intercepts (alter I6/I7, create-namespace, describe/show, ref DDL) + the r25 T2
  write-to-branch sniff; full v1 arm set ([router/map.md](router/map.md) for the tests).
  `execute_time_travelled` is a **release seam, not a routing step** (H-1b): it exists so
  `execute_with_read_only` can own a `time_travel::PinnedViews` and release it on every `?` /
  `return` path of the rewrite — see the `time_travel.rs` row below.
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); 24 in-module tests (MG-2: M2 Oracle sub-predicates, M3
  assignment-target qualification, M8 INSERT column list, M10 non-last
  unconditional clause).
- `insert_overwrite.rs` — INSERT OVERWRITE: empty probe/validate/provider-wipe (C1-Q-001) +
  non-empty r23 OV1 stage-then-swap; 2 in-module tests (`assignment_type_unit_tests`).
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff; 14 in-module tests.
- `call.rs` — I3 maintenance `CALL` procedures (expire_snapshots / rewrite_data_files /
  rollback_to_snapshot; LOCAL catalogs only); 3 in-module tests.
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
- `create_table.rs` — column-def `CREATE TABLE` (I5 schema-only staged create) + the
  Spark-SQL→iceberg type mapping; **TZ-4 PR-1:** default `TIMESTAMP` → Iceberg `timestamptz`,
  `TIMESTAMP_NTZ` stays `timestamp` (live Spark 4.1.2 CREATE probe). 3 in-module tests
  (`type_mapping_tests`) + `tests/create_table.rs` pin + CTAS type smoke.
- `alter.rs` — ALTER TABLE handlers (SET/UNSET TBLPROPERTIES, RENAME TO, schema evolution I6,
  I7 partition-field DDL, residual refusals) + the ALTER token rewrites the normalizer runs;
  9 in-module tests.
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`. **R-6 / G-6 Q1 (2026-08-14):**
  `IF NOT EXISTS` no longer early-returns without the location check — matching
  / no-location stay idempotent; a contradictory `LOCATION` fails loud naming
  both paths (`repark_core::refuse_contradictory_namespace_location`).
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into v1's positional `execute_with_read_only` call; `#[async_trait(?Send)]` matches the
  core trait; install with `ReparkSessionBuilder::with_sql_dialect` + `SparkExtension`).
  Tests: [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension: repark_core::SessionExtension` (`configure` = cardinality
  `repark.sql.*` config **+ `spark.sql.ansi.enabled` default TRUE (U5 / Q10=A)** **+ Spark-door
  `parse_float_as_decimal=true` (DEC-1 / U2)** **+ the session-timezone carrier**; `register` =
  `repark_functions::register_all` + analyzer rules + the composed `repark_ta::TaExtension`, in v1
  `build()`'s order; the DF-54.1 subquery guard stays a core session default, G8). The TA half is
  **composed, not re-implemented** — the TA set is door-neutral (design Q11), so this door installs
  the owning crate's extension. **H-1a split B (2026-08-10):** `configure` takes a
  `repark_core::SessionBuildConf` and installs the zone `build()` already resolved onto the
  `SessionConfig` (`repark_functions::session_time_zone::with_session_time_zone`), which is how
  timestamp extraction honors `spark.sql.session.timeZone`. This door is the ONE crossing point:
  `repark-core` owns the key and may not import `repark-functions` (a forbidden upward edge), and
  `repark-functions` is a leaf with no engine edge — only this crate depends on both.
  Tests: [extension/map.md](extension/map.md); end-to-end
  [../tests/session_timezone.rs](../tests/session_timezone.rs).
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
  star rewrite call, partition-spec builders.
- `collation.rs` — **G15 (2026-08-12):** parse-altitude collation refuse. Walks
  `Expr::Collate`, column-def `COLLATE`, `CREATE`/`ALTER COLLATION`, `SET NAMES COLLATE`,
  session `SQLConf` keys containing `collation` (including `ParenthesizedAssignments`),
  type-position `STRING COLLATE` (Spark `CAST(x AS STRING COLLATE name)`), and
  `RESET` of a collation key. `refuse_collation_in_statement` is called from
  `spark_ast.rs` (executing parse) and the router's successful parse (intercepted
  CREATE/ALTER). `refuse_collation_in_sql` is `pub` for the Python binding (`F.expr`,
  `filter_sql`). Pins: [`tests/collation.rs`](tests/map.md). Ledger:
  [`../../../../task/y7-collation-refuse-ledger.md`](../../../../task/y7-collation-refuse-ledger.md).
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
- `window_range.rs` — **G5b (2026-08-11) + G5b-R (Y-1, 2026-08-12) + Half-B:** Spark's rules
  for a **unit-less** `RANGE` frame offset over a datetime order key, plus residuals that
  share the same seam. DataFusion coerces a unit-less bound to `Interval(MonthDayNano)`,
  where Arrow reads a bare `"1"` as one **month**; Spark refuses it on a `TIMESTAMP` key
  (`DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE`) and reads it as **days** on a `DATE` key.
  Two mechanisms because a window expression's schema name embeds its frame: the TIMESTAMP
  arm refuses on the planned tree (`classify_planned_range_frames`), the DATE arm restates
  the **AST** (`rewrite_bare_range_bounds_to_days`) and re-plans. Y-1 extends that restatement
  for **R3** (negative / value-inverted interval over TIMESTAMP) and
  **R2** (`DAY TO SECOND` → Arrow-accepted interval text). Half-B detects invert as **kind
  or same-kind magnitude** after sign-normalize (`-2 PRECEDING AND -1 PRECEDING` →
  `2 FOLLOWING AND 1 FOLLOWING`). Kind invert vs CURRENT ROW empties with
  `FILTER (WHERE false)` over a current-row frame; same-kind magnitude invert refuses
  with Spark's `SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON`. The far-future
  `10000 YEAR FOLLOWING` pair is gone. A cheap AST probe
  (`statement_has_bare_range_bound`) still keeps ordinary statements on the single-plan path
  (now also fires for any interval bound, so R5 classify can see a numeric key). **W-4
  (2026-08-13):** R1 quotes unquoted `INTERVAL 1 DAY` before first plan; R5 restates
  `INTERVAL 'n' UNIT` over a numeric key to unit-less `n` (Spark 4.1.2, unit ignored).
  R4 stays recorded (DF 54.1.0 range-search; sqlparser 0.62 `EXCLUDE` TBD). ANSI-door
  wrapping is a named residual (this seam is Spark-door only).
  Pins: [`tests/window_temporal_range.rs`](tests/map.md); ledgers
  [`../../../task/g5b-temporal-range-ledger.md`](../../../task/g5b-temporal-range-ledger.md),
  [`../../../task/g5br-range-residuals-ledger.md`](../../../task/g5br-range-residuals-ledger.md),
  [`../../../task/z4-residuals-ledger.md`](../../../task/z4-residuals-ledger.md),
  [`../../../task/w4-z-residuals-ledger.md`](../../../task/w4-z-residuals-ledger.md).
- `describe_show.rs` — Group Z `DESCRIBE NAMESPACE` + Group AB `SHOW NAMESPACES`
  (pyspark-4.0.0 v2-oracle-pinned rendering, LIKE patterns, secret redaction).
- `metadata_tables.rs` — I2 metadata-table path rewrite (`.snapshots` → `$snapshots`);
  15 in-module tests.
- `time_travel.rs` — I1 SQL-TEXT half: token span scan + FROM/JOIN splice to snapshot-pinned
  static providers; the pin half (spec/parsers/resolution/`read_table_at`) is
  `repark_core::time_travel`. 8 in-module tests (2 rode the phase-1 hoist).
  **Ephemeral-view leak fix (H-1b, 2026-08-11 — closes the p2g rider,
  `docs/history/port-v2/p2g-ansi-m2-ledger.md` "Riders carried forward" 4):** the rewrite's
  `__repark_tt_N` temp views used to survive the statement — unbounded per-query accumulation on
  a long-lived session, and rows in the introspection surface (`SHOW TABLES` /
  `information_schema.tables`) after a statement that SUCCEEDED *and* after one that FAILED.
  `PinnedViews` (this module) records every name the rewrite registers — **before** the
  `register_table` call, so a registration that fails after taking the name is still drained —
  and `router::execute_with_read_only` releases them on every `?` / `return` path via the
  `execute_time_travelled` split (planning is done by then; the plan owns its provider, so the
  returned `DataFrame` still collects). NOT on unwind or future-drop: `PinnedViews` carries no
  `Drop` impl by design (it would need to own a `SessionContext` clone), and today there is no
  cancellation source (panics are banned in prod, and the PyO3 facade drives this via `block_on`).
  Pins: `tests/time_travel.rs::time_travel_temp_views_do_not_survive_a_successful_statement`
  (3 sequential pins + a two-pin JOIN) and
  `…::time_travel_temp_views_do_not_survive_a_failed_statement` (mid-rewrite failure — the
  right-hand pin registers before the left one fails to resolve — and a post-rewrite planning
  failure). NOT covered (same prefix, different path): the reader-options `read_table_at`
  registration in `repark_core::session`, whose view is the returned frame's backing and has no
  statement boundary — see `## Debug`.
  **Counter unification (H-1b fix pass, 2026-08-11):** this module no longer keeps a
  `TEMP_VIEW_SEQ` of its own — names come from `repark_core::time_travel::next_temp_view_name`,
  the single minter of the shared `__repark_tt_` namespace. Two counters both starting at 1 meant
  the door's names COLLIDED with the reader-options path's, so a `VERSION AS OF` statement
  deregistered (and then released) a live reader's view. Pin:
  `…::time_travel_statement_pins_never_collide_with_a_reader_options_view`.
- `local_fs_ddl.rs` — SEC-02 local-filesystem DDL gate (r24 SB1); 9 in-module tests.
- `catalog_ops.rs` — catalog lookup, P11 refusals, `iceberg_err`, path-escape reject, the
  r24 P7 `reregister*` provider-invalidation family (complete — PR-2 PARTIAL rider closed).
- `matrix.rs` — the Q13 surface matrix (`#[cfg(test)]`, design `docs/design/sql-doors.md` §2
  Q13 / graft G2): every `repark_common::surfaces` ID mapped to `Row::Tested { test, profile }`
  or `Row::DeliberatelyAbsent { reason, adr }`, plus the compile-run audit that fails on an
  unmapped surface. **47 tested / 3 deliberately absent** as of R-3 (the three PR-6
  structural absences — sort order + unknown-key refuse are ANSI-only; the wrong-door
  sniff points AT this door). `SEMANTICS_JOIN_NULL_KEYS` flipped from pin-absence to
  Tested at R-3. `CROSS_DOOR_EQUIVALENCE` is
  `Tested` under the `TwoSession` profile, and its evidence deliberately lives in the OTHER
  crate's test binary — `crates/repark-sql/tests/cross_door.rs`, the only place a dev-dependency
  may put both doors in one process; `cargo test -p repark-spark` alone will not run it. 3 tests.
- `lib.rs` — manifest: module decls, `execute`/`SparkDialect`/`SparkExtension` re-exports, the
  v1 domain-module `pub(crate) use` groups, and the `#[cfg(test)]` root imports that
  reconstruct v1's crate-root scope for the battery's `use super::*`.
- `tests/` — the ported v1 lib-root battery, **split by production-module alignment** (G-4,
  2026-08-10 declared-rename unit). Was a ~14.5-KLOC `tests.rs` monolith; now
  `tests/mod.rs` + leaf modules (`ctas`, `merge`, `dml`, `insert_overwrite`, `ref_ddl`, …)
  plus path-preserving sibling lifts of the former nested mods (`partitioned_ctas`,
  `partitioned_merge`, `transform_overwrite`, `service_managed_ctas`). Shared fixtures live in
  `tests/common.rs`. Navigation: [tests/map.md](tests/map.md). Identity gate: 352 lib tests,
  202 path renames, leaf multiset unchanged — see `docs/history/hardening-h1/g4-tests-split-ledger.md`.
  Registry / matrix pin strings moved with the renames (path-string updates only).
  Notable pins carried through the split: the **declared-divergence pin**
  `tests/ref_ddl.rs::ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud` (H-1d,
  2026-08-10 — the refusal cites the registry row it defends, and its leftover-token assertion
  binds the **dynamic** `(got word "…")` span, never the bare word);
  `tests/metadata_tables.rs::metadata_tables_spark_dot_form_and_guards`, which exercises **all
  ten** write forms registry row MT-2 names, because a row may not assert more than its pin
  proves; and `tests/metadata_tables.rs::metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door`
  (H-1c, 2026-08-10) — this door's half of
  [ADR-0006](../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md): the
  fork's synthesized `$`-metadata names do not enumerate in `information_schema.tables` or
  `SHOW TABLES`, while both spellings still resolve; the decision is made once at the catalog
  layer, never here — this row proves it reaches this door.

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
| INSERT OVERWRITE probe / OV1 swap | `insert_overwrite.rs` |
| Branch/tag DDL, write-to-branch sniff | `ref_ddl.rs` |
| Maintenance CALL procedures | `call.rs` |
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
| Doc comment names a crate that doesn't exist | v1-port doc text re-homes to `repark_core` (verify-panel fix); report any straggler |

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

  **The three share ONE process-global counter**, `repark_core::time_travel::next_temp_view_name`
  — the reason that function is `pub`. Producer 1 minted from a SECOND counter of its own until
  the H-1b fix pass (2026-08-11), and both sequences started at 1: on a session that had used
  producer 2 first, the door's mint step deregistered the reader's LIVE view before registering
  its own under the same name, and its post-planning release then deleted it outright. So
  producer 2's "keeps the registration" was only true until an unrelated `VERSION AS OF` statement
  ran. Do not add a second minter; the pin is
  `tests/time_travel.rs::time_travel_statement_pins_never_collide_with_a_reader_options_view`,
  which asserts both the survival and the shared sequence (the second half reds whatever the
  numbers happen to be).

  To tell a leftover from a fixture, run the statement/read in isolation and compare
  `leftover_time_travel_views` (test helper in `tests/time_travel.rs`) before and after.
- **`__repark` is an ENGINE-RESERVED name prefix** — user tables/views must not use it. A user
  table registered as `__repark_tt_<n>` is DESTROYED by the next time-travel statement (the mint
  step deregisters the name before registering the pinned provider, `time_travel.rs`), where
  before the leak fix it was silently REPLACED and stayed listed. Same reserved-prefix rule,
  different symptom; not a regression. That `deregister_table` is NOT dead code, even though
  engine-minted collisions are now impossible: DataFusion's schema provider refuses a duplicate
  `register_table`, so without it a squatted name would fail the statement instead of being
  clobbered. The sequence is a single process-global counter from 1, so the names are guessable —
  if a hard guarantee is ever wanted, mint with a per-process nonce or refuse rather than clobber
  an occupied name.

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `tests/` (five doc comments + the MERGE source-view fixture in
  `merge_star_forms_upsert`, now `staging_view`) and `merge.rs` (two doc comments).
