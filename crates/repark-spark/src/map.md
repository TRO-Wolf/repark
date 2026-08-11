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

- `router.rs` — `execute` / `execute_with_read_only` / `execute_inner` + pre-parse intercepts
  (alter I6/I7, create-namespace, describe/show, ref DDL) + the r25 T2 write-to-branch sniff;
  full v1 arm set ([router/map.md](router/map.md) for the tests).
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); 10 in-module tests.
- `insert_overwrite.rs` — INSERT OVERWRITE: empty probe/validate/provider-wipe (C1-Q-001) +
  non-empty r23 OV1 stage-then-swap; 2 in-module tests (`assignment_type_unit_tests`).
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff; 14 in-module tests.
- `call.rs` — I3 maintenance `CALL` procedures (expire_snapshots / rewrite_data_files /
  rollback_to_snapshot; LOCAL catalogs only); 3 in-module tests.
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
- `create_table.rs` — column-def `CREATE TABLE` (I5 schema-only staged create) + the
  Spark-SQL→iceberg type mapping; 3 in-module tests (`type_mapping_tests`).
- `alter.rs` — ALTER TABLE handlers (SET/UNSET TBLPROPERTIES, RENAME TO, schema evolution I6,
  I7 partition-field DDL, residual refusals) + the ALTER token rewrites the normalizer runs;
  9 in-module tests.
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`.
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into v1's positional `execute_with_read_only` call; install with
  `ReparkSessionBuilder::with_sql_dialect` + `SparkExtension`). Tests:
  [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension: repark_core::SessionExtension` (`configure` = cardinality
  `repark.sql.*` config **+ the session-timezone carrier**; `register` =
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
  the MERGE star rewrite call, partition-spec builders.
- `spark_ast.rs` — the Spark passthrough: ORDER BY null-placement defaults, eager analysis,
  eager DML/`COPY` commands (F-BR-2), SEC-02 gate call. 6 in-module tests.
- `describe_show.rs` — Group Z `DESCRIBE NAMESPACE` + Group AB `SHOW NAMESPACES`
  (pyspark-4.0.0 v2-oracle-pinned rendering, LIKE patterns, secret redaction).
- `metadata_tables.rs` — I2 metadata-table path rewrite (`.snapshots` → `$snapshots`);
  15 in-module tests.
- `time_travel.rs` — I1 SQL-TEXT half: token span scan + FROM/JOIN splice to snapshot-pinned
  static providers; the pin half (spec/parsers/resolution/`read_table_at`) is
  `repark_core::time_travel`. 8 in-module tests (2 rode the phase-1 hoist).
- `local_fs_ddl.rs` — SEC-02 local-filesystem DDL gate (r24 SB1); 9 in-module tests.
- `catalog_ops.rs` — catalog lookup, P11 refusals, `iceberg_err`, path-escape reject, the
  r24 P7 `reregister*` provider-invalidation family (complete — PR-2 PARTIAL rider closed).
- `matrix.rs` — the Q13 surface matrix (`#[cfg(test)]`, design `docs/design/sql-doors.md` §2
  Q13 / graft G2): every `repark_common::surfaces` ID mapped to `Row::Tested { test, profile }`
  or `Row::DeliberatelyAbsent { reason, adr }`, plus the compile-run audit that fails on an
  unmapped surface. **40 tested / 3 deliberately absent** as of PR-6 (sort order + unknown-key
  refuse are ANSI-only; the wrong-door sniff points AT this door). `CROSS_DOOR_EQUIVALENCE` is
  `Tested` under the `TwoSession` profile, and its evidence deliberately lives in the OTHER
  crate's test binary — `crates/repark-sql/tests/cross_door.rs`, the only place a dev-dependency
  may put both doors in one process; `cargo test -p repark-spark` alone will not run it. 3 tests.
- `lib.rs` — manifest: module decls, `execute`/`SparkDialect`/`SparkExtension` re-exports, the
  v1 domain-module `pub(crate) use` groups, and the `#[cfg(test)]` root imports that
  reconstruct v1's crate-root scope for the battery's `use super::*`.
- `tests.rs` — the ported v1 lib-root battery (move-only identity unit, 334 census names:
  342 at the pin − 6 `postgres_p11_tests` (post-milestone-one) − 2 time-travel parser pins
  hoisted to repark-core in phase 1). Includes the `bug001_*` MoR-valve set and the
  `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` / `service_managed_ctas`
  groups. Plus the **declared-divergence pin**
  `ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud` (H-1d, 2026-08-10): the
  `IF EXISTS` / `IF NOT EXISTS` spellings and every other trailing clause refuse loud on
  snapshot-ref DDL, and the refusal still cites the registry row it defends. Its leftover-token
  assertion binds the **dynamic** `(got word "…")` span, never the bare word — the message's
  constant tail already contains `NOT` and `EXISTS`, so a bare substring check would be a
  tautology for two of the three cases.
  `metadata_tables_spark_dot_form_and_guards` likewise exercises **all ten** write forms registry
  row MT-2 names (INSERT / UPDATE / DELETE / MERGE / CTAS / CREATE OR REPLACE / TRUNCATE /
  CREATE VIEW / DROP / ALTER), because a row may not assert more than its pin proves.
  `metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door` (H-1c,
  2026-08-10) is this door's half of
  [ADR-0006](../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md): the fork's
  synthesized `$`-metadata names do not enumerate in `information_schema.tables` or its twin
  `SHOW TABLES`, and **both** spellings still resolve — the door's own `t.snapshots` and the
  `t$snapshots` it rewrites onto. The decision is made once at the catalog layer, never here; this
  row proves it reaches this door.

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
| MERGE lowering / star sentinel | `merge.rs` |
| INSERT OVERWRITE probe / OV1 swap | `insert_overwrite.rs` |
| Branch/tag DDL, write-to-branch sniff | `ref_ddl.rs` |
| Maintenance CALL procedures | `call.rs` |
| CTAS lowering / location policy | `ctas.rs` |
| Column-def CREATE / type mapping | `create_table.rs` |
| ALTER TABLE / token rewrites | `alter.rs` |
| Namespace / DROP TABLE DDL | `namespace_ddl.rs` |
| Pin a router behavior end to end | `tests.rs` (lib-root battery) |
| Find why a statement form refuses, and whether it is declared | `../../../docs/spark-sql-iceberg-parity.md` §2 |
| Pin a door-native gate (TRUNCATE/P11/BUG-010) | `router/tests.rs` |
| ORDER BY / eager-command passthrough semantics | `spark_ast.rs` |
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
| P11 refusal missing | read-only set threading: `execute_with_read_only` → registry snapshot |
| `matrix::matrix_maps_every_surface` RED | a surface ID was added to `repark_common::surfaces::ALL` with no row here — add `Tested`/`DeliberatelyAbsent` |
| Doc comment names a crate that doesn't exist | v1-port doc text re-homes to `repark_core` (verify-panel fix); report any straggler |

First checks: `cargo test -p repark-spark <module>::`. Escalate to: [../map.md#debug](../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `tests.rs` (five doc comments + the MERGE source-view fixture in
  `merge_star_forms_upsert`, now `staging_view`) and `merge.rs` (two doc comments).
