//! The ANSI door's **surface matrix** — every `repark_common::surfaces` ID, disposed.
//!
//! Design SSOT: `docs/design/sql-doors.md` §2 Q13 (graft G2). The registry in
//! `repark_common::surfaces` is the dialect-neutral universe; this file says what THIS door does
//! with each ID, and [`matrix_maps_every_surface`] fails the build if any ID has no row.
//!
//! This door is NEW code, so the matrix does double duty: it is the audit, and it is the
//! milestone boundary. PR-5 (M1) ships the delegation core, the guard set, the wrong-door sniff,
//! the CTAS/`WITH (…)` vocabulary and the schema DDL; PR-6 (M2) ships ALTER, MERGE, `FOR … AS
//! OF`, branch/tag DDL, the full refuse set and the cross-door rows. Every M2 surface is a
//! `DeliberatelyAbsent` row here naming PR-6 — **sequencing recorded as a decision**, which is
//! exactly what design §6 R5 asks for (no deferral without its equivalent and its trigger).
//!
//! The whole module is `#[cfg(test)]` — audit evidence, not product code.
//!
//! **Test names are `cargo test -p repark-sql -- --list` names**, verbatim.

use repark_common::surfaces::{self, Row, SessionProfile, SurfaceId};

use SessionProfile::{Native, Unit};

/// Shorthand for a shipped surface.
const fn t(test: &'static str, profile: SessionProfile) -> Row {
    Row::Tested { test, profile }
}

/// Shorthand for a deliberate absence.
const fn absent(reason: &'static str, adr: &'static str) -> Row {
    Row::DeliberatelyAbsent { reason, adr }
}

/// The design section every M2 row cites, alongside its own ruling.
const M2: &str = "briefs/phase-2-sql-doors.md §1 PR-6";

/// ===========================================================================================
/// The ANSI door's disposition of every surface ID, as of PR-5 (M1).
///
/// Note the profile column: ANSI rows run `Native` (a session with NO extension) or `Unit`.
/// That is not a formality — extensions are session-scoped, so evidence gathered on a
/// Spark-extended session would say nothing about this door's semantics (design §2 Q13, graft
/// G5). No row here may claim `TwoSession`; that profile belongs to PR-6's cross-door protocol.
/// ===========================================================================================
const ROWS: &[(SurfaceId, Row)] = &[
    // --- Statement forms: what M1 ships ---
    (
        surfaces::SELECT_PASSTHROUGH,
        t("router::tests::select_delegates_to_datafusion", Native),
    ),
    (
        surfaces::CTAS,
        t(
            "tests::ctas_into_registered_iceberg_catalog_round_trips",
            Native,
        ),
    ),
    (
        surfaces::CTAS_TARGET_ROUTING,
        t(
            "tests::ctas_unregistered_target_refuses_requiring_qualification",
            Native,
        ),
    ),
    (
        surfaces::CREATE_OR_REPLACE_TABLE,
        t(
            "tests::create_or_replace_table_as_select_replaces_rows",
            Native,
        ),
    ),
    (
        surfaces::CREATE_TABLE_COLUMN_DEF,
        t(
            "tests::create_table_column_def_creates_iceberg_table",
            Native,
        ),
    ),
    (
        surfaces::DROP_TABLE,
        t("tests::drop_table_if_exists_is_idempotent", Native),
    ),
    (
        surfaces::CREATE_SCHEMA,
        t("tests::create_schema_creates_the_namespace", Native),
    ),
    (
        surfaces::DROP_SCHEMA,
        t("tests::drop_schema_drops_the_namespace", Native),
    ),
    (
        surfaces::METADATA_TABLES,
        t("router::tests::metadata_dollar_form_passes_through", Unit),
    ),
    (
        surfaces::INTROSPECTION,
        absent(
            "Q8 says DELEGATE — but the R2 day-1 spike found there is nothing to delegate TO: \
             `ReparkSession` cannot turn `information_schema` on (the builder's config map is \
             repark/spark-shaped and never reaches `SessionConfig`), so `SHOW TABLES` / \
             `DESCRIBE` / `information_schema.*` are dead in BOTH doors. The enumeration \
             machinery itself is proved working on a raw DataFusion context, so this is a \
             repark-core gap, NOT a door parser to write around (design §2 Q8 says exactly \
             that). Filed against core; evidence + repro in this PR's ledger and in \
             `tests::information_schema_enumerates_registered_iceberg_catalogs`.",
            "docs/design/sql-doors.md §2 Q8 (R2 spike)",
        ),
    ),
    // --- Statement forms: M2 (PR-6). Each names its callable/native equivalent, per §6 R5. ---
    (
        surfaces::ALTER_TABLE_RENAME,
        absent(
            "The ALTER family lands in M2 with the rest of the schema-evolution handlers; until \
             then the equivalent is the Spark door or the callable rename op on the catalog.",
            M2,
        ),
    ),
    (
        surfaces::ALTER_TABLE_SCHEMA_EVOLUTION,
        absent(
            "M2 ships the ALTER schema-evolution handlers over the fork's `UpdateSchema`. The \
             R1 spike (does the DF-re-exported sqlparser parse `ALTER … SET PROPERTIES`?) is \
             recorded in this PR's ledger precisely so M2 knows whether it needs the ~50-LOC \
             pre-parse recognizer fallback (design §6 R1).",
            M2,
        ),
    ),
    (
        surfaces::ALTER_TABLE_PROPERTIES,
        absent(
            "Same M2 handler family as the schema evolution above; `WITH (extra_properties = \
             MAP(…))` already reaches raw Iceberg keys at CREATE time, so M1 users are not \
             blocked from setting them — only from changing them in place.",
            M2,
        ),
    ),
    (
        surfaces::ALTER_TABLE_PARTITION_FIELDS,
        absent(
            "DEFERRED FROM SQL ENTIRELY, not just from M1 (design §2 Q3): partition-spec \
             evolution is a callable op (the fork's `UpdatePartitionSpec` via repark-iceberg). \
             The designated future spelling is `ALTER TABLE t SET PROPERTIES partitioning = \
             ARRAY[…]` (replace-spec, Trino semantics); the trigger is dbt-repark or the first \
             user need.",
            "docs/design/sql-doors.md §2 Q3",
        ),
    ),
    (
        surfaces::INSERT_INTO,
        absent(
            "DML delegation (INSERT/DELETE/UPDATE through the fork's `TableProvider`, ADR-0003) \
             lands with the M2 DML set, behind the same guard set M1 installs. M1's scope is the \
             delegation core plus DDL (brief §1 PR-5).",
            M2,
        ),
    ),
    (
        surfaces::INSERT_OVERWRITE,
        absent(
            "OMITTED, Trino-faithful — not deferred (design §2 Q9). The steer is MERGE, \
             DELETE+INSERT, or `CREATE OR REPLACE TABLE … AS SELECT`. Evidence for the choice: \
             dbt-trino ships no insert_overwrite strategy (graft G10). The OV1 machinery stays \
             reachable through the Spark door and the callable op.",
            "docs/design/sql-doors.md §2 Q9",
        ),
    ),
    (
        surfaces::DELETE,
        absent("Same M2 DML set as INSERT_INTO above.", M2),
    ),
    (
        surfaces::UPDATE,
        absent("Same M2 DML set as INSERT_INTO above.", M2),
    ),
    (
        surfaces::MERGE,
        absent(
            "M2 ships the ~150-LOC `Statement::Merge` → `MergeSpec` lowering; execution is \
             already shared at tier 1 (`repark_iceberg::write::merge::execute_merge`), so this \
             is lowering only. Output clauses will refuse loud (design §2 Q4).",
            M2,
        ),
    ),
    (
        surfaces::TRUNCATE,
        absent(
            "Part of M2's full refuse set (the Spark door's permanent targeted refuse, \
             C4-L-001, gets its ANSI twin there).",
            M2,
        ),
    ),
    (
        surfaces::TIME_TRAVEL,
        absent(
            "M2 ships the quote-parameterized `FOR … AS OF` scanner (graft G7) plus the \
             double-quote pin set. `FOR` is mandatory ANSI-side (design §2 Q5); the resolution \
             half is already hoisted to repark-core. M1 deliberately does NOT scan for it — \
             half a scanner is worse than none, and the router's ordering (multi-statement \
             refuse FIRST) is fixed here so M2 only inserts a stage.",
            M2,
        ),
    ),
    (
        surfaces::BRANCH_TAG_DDL,
        absent(
            "M2, as precedent-copying (graft G6): v1's ALTER-scoped grammar over the same \
             tier-1 `ManageSnapshots` executor. Design §2 Q6 names this the first deferral \
             candidate if M2 overruns — and pins that the rationale would then be SCOPE, never \
             'no precedent'. Equivalent today: the Spark door.",
            "docs/design/sql-doors.md §2 Q6",
        ),
    ),
    (
        surfaces::MAINTENANCE_CALL,
        absent(
            "CALLABLE OPS ONLY — the ADR pin stands (design §2 Q7), so this is a standing \
             decision, not a milestone deferral. M2 adds the loud refuses for `Statement::Call` \
             and `ALTER TABLE … EXECUTE` that steer to the ops. Pre-designated future spelling: \
             `EXECUTE proc(arg => v)`; trigger: dbt-repark post-hooks showing a \
             statement-shaped need, superseding ADR note first.",
            "docs/design/sql-doors.md §2 Q7 / ADR-0002",
        ),
    ),
    // --- Table-creation options: the curated `WITH (…)` vocabulary (design §2 Q1/Q2) ---
    (
        surfaces::TABLE_OPTION_FORMAT,
        t(
            "tests::with_format_parquet_accepted_orc_and_avro_refuse_loud",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_FORMAT_VERSION,
        t(
            "tests::with_format_version_sets_the_table_format_version",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_PARTITIONING,
        t(
            "tests::with_partitioning_array_builds_the_partition_spec",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_LOCATION,
        t(
            "tests::with_location_lands_the_table_under_the_path",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_RAW_PROPERTIES,
        t(
            "tests::with_extra_properties_map_passes_raw_iceberg_keys",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_SORT_ORDER,
        t(
            "tests::with_sorted_by_refuses_loud_naming_the_trigger",
            Native,
        ),
    ),
    (
        surfaces::TABLE_OPTION_UNKNOWN_KEY_REFUSE,
        t(
            "tests::unknown_bare_property_refuses_listing_the_curated_set",
            Native,
        ),
    ),
    (
        surfaces::PARTITION_TRANSFORM_VALIDATION,
        t(
            "partitioning::tests::transform_arg_counts_and_bounds_validated",
            Unit,
        ),
    ),
    (
        surfaces::MOR_TABLE_CREATION,
        t(
            "tests::extra_properties_write_merge_mode_creates_a_mor_table",
            Native,
        ),
    ),
    (
        surfaces::SCHEMA_OPTION_LOCATION,
        t(
            "tests::create_schema_with_location_stores_both_keys",
            Native,
        ),
    ),
    // --- Guard rails: the M1 set (design §2 Q12) ---
    (
        surfaces::GUARD_MULTI_STATEMENT,
        t(
            "guards::tests::multi_statement_refuses_first_and_quote_aware",
            Unit,
        ),
    ),
    (
        surfaces::GUARD_READ_ONLY_CATALOG,
        t(
            "guards::tests::read_only_catalog_dml_refuses_generically",
            Unit,
        ),
    ),
    (
        surfaces::GUARD_LOCAL_FILESYSTEM,
        t("guards::tests::local_filesystem_plan_refuses", Unit),
    ),
    (
        surfaces::GUARD_WRITE_TO_BRANCH,
        t("guards::tests::write_to_branch_refuses", Unit),
    ),
    (
        surfaces::GUARD_MOR_MULTI_SPEC_DML,
        absent(
            "The hoisted BUG-001 valve gates DML, and this door's DML rows are M2 — the guard \
             is wired at the same time as the statements it can fire on. Wiring it against \
             statements that all refuse would produce a test that proves the refuse, not the \
             valve (design §2 Q12; the valve itself is tier-1 and already tested in \
             repark-iceberg).",
            M2,
        ),
    ),
    // --- Ergonomics + seams ---
    (
        surfaces::WRONG_DOOR_SNIFF,
        t("sniff::tests::spark_isms_upgrade_the_parse_error", Unit),
    ),
    (
        surfaces::IDENTIFIER_CASE_FOLDING,
        absent(
            "Stock DataFusion ANSI folding applies from day one, but the divergence-from-Spark \
             DOC ROW (one per door, design §2 Q10 case rules) is written against both doors at \
             once and lands with M2's cross-door work. An untested claim about folding is worse \
             than an absence row that says so.",
            M2,
        ),
    ),
    (
        surfaces::TA_FUNCTIONS,
        absent(
            "`repark-ta` is owned by neither door: native sessions opt in by installing \
             `TaExtension` (PR-4). The ANSI toll — one smoke row (f64::to_bits vs golden) plus \
             the non-literal-period refuse row — rides M2, once PR-4 has landed the extension \
             this door would compose (design §2 Q11).",
            "docs/design/sql-doors.md §2 Q11",
        ),
    ),
    (
        surfaces::SQL_DIALECT_SEAM,
        t(
            "dialect::tests::ansi_dialect_execute_runs_the_router",
            Native,
        ),
    ),
    (
        surfaces::CROSS_DOOR_EQUIVALENCE,
        absent(
            "M2, under the TWO-session protocol (graft G5): a native/no-extension session and a \
             Spark-extended session, each driven through its OWN door, compared on the Arrow \
             path (value AND type). A single-session `sql_with` row is legal only for surfaces \
             the analyzer/UDF layer cannot touch, and is explicitly NOT what this ID means — \
             faking it here is forbidden.",
            M2,
        ),
    ),
];

/// ===========================================================================================
/// The compile-run audit (design §2 Q13): this door maps EXACTLY the registry, once each.
///
/// For a NEW door this is the test that keeps M1 honest about M2 — a surface cannot sit in the
/// registry unmentioned while the door quietly does not implement it. `audit` reports unmapped
/// IDs, stale IDs, duplicates and untraceable rows together.
/// MUTATION: delete the `CTAS` row → this REDs naming `CTAS`.
/// ===========================================================================================
#[test]
fn matrix_maps_every_surface() {
    if let Err(problems) = surfaces::audit("repark-sql", ROWS) {
        panic!(
            "repark-sql surface matrix is out of sync with repark_common::surfaces:\n{problems}"
        );
    }
}

/// No `Tested` row claims `SparkExtended` or `TwoSession`. This is graft G5 as a test: the ANSI
/// door's evidence must come from a session with NO extension installed, because a
/// Spark-extended session has Spark expression semantics through EVERY door — including this
/// one. A row that gathered its evidence on an extended session would be describing the Spark
/// analyzer, not the ANSI door.
/// MUTATION: set any row's profile to `SparkExtended` → this REDs.
#[test]
fn ansi_rows_are_native_or_unit_only() {
    for (id, row) in ROWS {
        if let Row::Tested { profile, .. } = row {
            assert!(
                matches!(profile, SessionProfile::Native | SessionProfile::Unit),
                "{id}: ANSI-door evidence must be Native or Unit, got {profile:?}"
            );
        }
    }
}

/// M1's shipped set is exactly the brief's PR-5 scope. The risk this pins is scope creep in
/// either direction: a surface quietly shipped without its design ruling being applied, or an
/// M1 surface quietly downgraded to an absence row to make a gate green.
/// MUTATION: flip any `Tested` row to `absent(...)` → this REDs.
#[test]
fn m1_ships_the_briefed_scope() {
    let tested: Vec<SurfaceId> = ROWS
        .iter()
        .filter(|(_, row)| row.is_tested())
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        tested,
        vec![
            surfaces::SELECT_PASSTHROUGH,
            surfaces::CTAS,
            surfaces::CTAS_TARGET_ROUTING,
            surfaces::CREATE_OR_REPLACE_TABLE,
            surfaces::CREATE_TABLE_COLUMN_DEF,
            surfaces::DROP_TABLE,
            surfaces::CREATE_SCHEMA,
            surfaces::DROP_SCHEMA,
            surfaces::METADATA_TABLES,
            surfaces::TABLE_OPTION_FORMAT,
            surfaces::TABLE_OPTION_FORMAT_VERSION,
            surfaces::TABLE_OPTION_PARTITIONING,
            surfaces::TABLE_OPTION_LOCATION,
            surfaces::TABLE_OPTION_RAW_PROPERTIES,
            surfaces::TABLE_OPTION_SORT_ORDER,
            surfaces::TABLE_OPTION_UNKNOWN_KEY_REFUSE,
            surfaces::PARTITION_TRANSFORM_VALIDATION,
            surfaces::MOR_TABLE_CREATION,
            surfaces::SCHEMA_OPTION_LOCATION,
            surfaces::GUARD_MULTI_STATEMENT,
            surfaces::GUARD_READ_ONLY_CATALOG,
            surfaces::GUARD_LOCAL_FILESYSTEM,
            surfaces::GUARD_WRITE_TO_BRANCH,
            surfaces::WRONG_DOOR_SNIFF,
            surfaces::SQL_DIALECT_SEAM,
        ],
        "the ANSI door's M1 surface set changed — update this pin AND task/p2f-ansi-m1-ledger.md"
    );
    assert_eq!(ROWS.len() - tested.len(), 18, "deliberate-absence count");
}
