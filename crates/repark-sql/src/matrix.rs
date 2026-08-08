//! The ANSI door's **surface matrix** — every `repark_common::surfaces` ID, disposed.
//!
//! Design SSOT: `docs/design/sql-doors.md` §2 Q13 (graft G2). The registry in
//! `repark_common::surfaces` is the dialect-neutral universe; this file says what THIS door does
//! with each ID, and [`matrix_maps_every_surface`] fails the build if any ID has no row.
//!
//! This door is NEW code, so the matrix does double duty: it is the audit, and it is the
//! milestone boundary. PR-5 (M1) shipped the delegation core (reads, metadata tables, and the
//! `INSERT`/`DELETE`/`UPDATE` the fork's `TableProvider` services — with the BUG-001 merge-on-read
//! valve wired over them), the guard set, the wrong-door sniff, the CTAS/`WITH (…)` vocabulary and
//! the schema DDL. **PR-6 (M2) closes the door**: ALTER (schema evolution, `SET PROPERTIES`,
//! `RENAME TO`), MERGE, `FOR … AS OF` time travel, branch/tag DDL, the full refuse set, the Q11 TA
//! toll, Q8 introspection (unblocked by the repark-core R2 config fix) and the two-session
//! cross-door rows.
//!
//! What remains `DeliberatelyAbsent` after M2 is absent **by ruling, not by sequencing** — four
//! rows, each a standing design decision with its callable-op equivalent and its trigger named
//! (design §6 R5). There are no `M2`-deferral rows left.
//!
//! The whole module is `#[cfg(test)]` — audit evidence, not product code.
//!
//! **Test names are `cargo test -p repark-sql -- --list` names**, verbatim.

use repark_common::surfaces::{self, Row, SessionProfile, SurfaceId};

use SessionProfile::{Native, TwoSession, Unit};

/// Shorthand for a shipped surface.
const fn t(test: &'static str, profile: SessionProfile) -> Row {
    Row::Tested { test, profile }
}

/// Shorthand for a deliberate absence.
const fn absent(reason: &'static str, adr: &'static str) -> Row {
    Row::DeliberatelyAbsent { reason, adr }
}

/// ===========================================================================================
/// The ANSI door's disposition of every surface ID, as of PR-6 (M2 — the door is closed).
///
/// Note the profile column. It is not a formality: extensions are session-scoped, so evidence
/// gathered on a Spark-extended session would say nothing about this door's semantics (design §2
/// Q13, graft G5). `Native` = a session with NO extension; `Unit` = no session at all;
/// `TwoSession` = the cross-door protocol, whose ANSI half runs on a native session and whose
/// Spark half is the control. `SparkExtended` may never appear here, and a test forbids it.
/// ===========================================================================================
const ROWS: &[(SurfaceId, Row)] = &[
    // --- Statement forms ---
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
        t(
            "introspection::information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door",
            Native,
        ),
    ),
    // --- The ALTER family (M2). Stock-parsed, executed through the SAME tier-1 fork
    // `UpdateSchema` / `rename_table` calls the Spark door uses — no door→door edge. ---
    (
        surfaces::ALTER_TABLE_RENAME,
        t(
            "cross_door::cross_door_alter_lands_the_same_evolved_schema",
            TwoSession,
        ),
    ),
    (
        surfaces::ALTER_TABLE_SCHEMA_EVOLUTION,
        t(
            "cross_door::cross_door_alter_lands_the_same_evolved_schema",
            TwoSession,
        ),
    ),
    (
        surfaces::ALTER_TABLE_PROPERTIES,
        t("alter::tests::extra_properties_sets_raw_iceberg_keys", Unit),
    ),
    (
        surfaces::ALTER_TABLE_PARTITION_FIELDS,
        absent(
            "DEFERRED FROM SQL ENTIRELY, not just from M1 (design §2 Q3): partition-spec \
             evolution is a callable op (the fork's `UpdatePartitionSpec` via repark-iceberg). \
             The designated future spelling is `ALTER TABLE t SET PROPERTIES partitioning = \
             ARRAY[…]` (replace-spec, Trino semantics); the trigger is dbt-repark or the first \
             user need. The M2 `SET PROPERTIES` handler therefore refuses the key BY NAME, \
             saying exactly that and naming the callable op that does the job today — pinned by \
             `alter::tests::partitioning_refuses_citing_q3_and_names_the_callable_op`.",
            "docs/design/sql-doors.md §2 Q3",
        ),
    ),
    // --- Delegated DML: shipped by M1 because delegation ships it (ADR-0003). These are WRITE
    // surfaces, so each carries a round-trip row and the MoR valve below is wired over them.
    (
        surfaces::INSERT_INTO,
        t("tests::insert_into_iceberg_table_round_trips", Native),
    ),
    (
        surfaces::INSERT_OVERWRITE,
        absent(
            "OMITTED, Trino-faithful — not deferred (design §2 Q9). The steer is MERGE, \
             DELETE+INSERT, or `CREATE OR REPLACE TABLE … AS SELECT`. Evidence for the choice: \
             dbt-trino ships no insert_overwrite strategy (graft G10). The OV1 machinery stays \
             reachable through the Spark door and the callable op. The omission is DELIVERED as \
             a loud refuse steering all three ways and citing the evidence — pinned by \
             `refusals::tests::insert_overwrite_refusal_steers_three_ways_and_cites_the_evidence`.",
            "docs/design/sql-doors.md §2 Q9",
        ),
    ),
    (
        surfaces::DELETE,
        t(
            "tests::delete_from_iceberg_table_removes_matching_rows",
            Native,
        ),
    ),
    (
        surfaces::UPDATE,
        t("tests::update_iceberg_table_rewrites_matching_rows", Native),
    ),
    (
        surfaces::MERGE,
        t(
            "cross_door::cross_door_merge_produces_the_same_result_table",
            TwoSession,
        ),
    ),
    (
        surfaces::TRUNCATE,
        absent(
            "PERMANENT targeted refuse, not a deferral — the ANSI twin of the Spark door's \
             C4-L-001 refusal. `TRUNCATE TABLE` means different things in different engines \
             (delete-all-rows vs drop-and-recreate), so the door names BOTH meanings and steers \
             to the unambiguous spelling for each (`DELETE FROM t` / `CREATE OR REPLACE TABLE … \
             AS SELECT`). Pinned by `refusals::tests::truncate_refusal_names_both_meanings`.",
            "docs/design/sql-doors.md §2 Q9 (refuse-set completion)",
        ),
    ),
    (
        surfaces::TIME_TRAVEL,
        t(
            "cross_door::cross_door_time_travel_pins_the_same_snapshot_content",
            TwoSession,
        ),
    ),
    (
        surfaces::BRANCH_TAG_DDL,
        t("ref_ddl::tests::parses_create_branch_and_tag", Unit),
    ),
    (
        surfaces::MAINTENANCE_CALL,
        absent(
            "CALLABLE OPS ONLY — the ADR pin stands (design §2 Q7), so this is a standing \
             decision, not a milestone deferral. M2 adds the loud refuses for `Statement::Call` \
             and `ALTER TABLE … EXECUTE` that steer to the ops. Pre-designated future spelling: \
             `EXECUTE proc(arg => v)`; trigger: dbt-repark post-hooks showing a \
             statement-shaped need, superseding ADR note first. Both refuses are LIVE and \
             pinned: `refusals::tests::call_refusal_steers_to_callable_ops_and_names_the_trigger` \
             and `refusals::tests::alter_execute_refusal_declares_itself_the_future_spelling`.",
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
        t("tests::mor_unpartitioned_multi_spec_dml_refuses", Native),
    ),
    // --- Ergonomics + seams ---
    (
        surfaces::WRONG_DOOR_SNIFF,
        t("sniff::tests::spark_isms_upgrade_the_parse_error", Unit),
    ),
    (
        surfaces::IDENTIFIER_CASE_FOLDING,
        t(
            "cross_door::cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted",
            TwoSession,
        ),
    ),
    (
        surfaces::TA_FUNCTIONS,
        t(
            "ta_toll::ta_ema_through_the_ansi_door_is_bit_exact_against_the_golden",
            Native,
        ),
    ),
    (
        surfaces::SQL_DIALECT_SEAM,
        t(
            "session_wiring::ansi_dialect_on_a_repark_session_runs_the_door",
            Native,
        ),
    ),
    (
        surfaces::CROSS_DOOR_EQUIVALENCE,
        t(
            "cross_door::cross_door_ctas_produces_the_same_table_content_and_schema",
            TwoSession,
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

/// No `Tested` row claims `SparkExtended`. This is graft G5 as a test: the ANSI door's evidence
/// must come from a session with NO extension installed, because a Spark-extended session has
/// Spark expression semantics through EVERY door — including this one. A row that gathered its
/// evidence on an extended session would be describing the Spark analyzer, not the ANSI door.
///
/// `TwoSession` IS allowed from PR-6 on, and only because of what the profile means: a two-session
/// row runs the ANSI side on a NATIVE session and the Spark side on an extended one, comparing
/// results. The native half is the ANSI evidence; the extended half is the control. That is the
/// opposite of laundering — it is the protocol design §2 Q13 mandates. What stays banned is the
/// thing that would launder: a single Spark-extended session claimed as this door's evidence.
/// MUTATION: set any row's profile to `SparkExtended` → this REDs.
#[test]
fn ansi_rows_never_claim_a_spark_extended_session() {
    for (id, row) in ROWS {
        if let Row::Tested { profile, .. } = row {
            assert_ne!(
                *profile,
                SessionProfile::SparkExtended,
                "{id}: ANSI-door evidence may never come from a Spark-extended session"
            );
        }
    }
}

/// Every `TwoSession` row must be one the two-session protocol can actually produce — i.e. a
/// surface BOTH doors have. A `TwoSession` claim on a surface the Spark door marks
/// `DeliberatelyAbsent` would be describing a comparison that cannot exist.
/// MUTATION: mark any of these rows `TwoSession` on an ANSI-only surface → this REDs.
#[test]
fn two_session_rows_name_surfaces_both_doors_have() {
    const ANSI_ONLY: &[SurfaceId] = &[
        surfaces::TABLE_OPTION_SORT_ORDER,
        surfaces::TABLE_OPTION_UNKNOWN_KEY_REFUSE,
        surfaces::WRONG_DOOR_SNIFF,
    ];
    for (id, row) in ROWS {
        if let Row::Tested {
            profile: SessionProfile::TwoSession,
            ..
        } = row
        {
            assert!(
                !ANSI_ONLY.contains(id),
                "{id}: this surface has no Spark-door counterpart, so no two-session comparison \
                 exists for it"
            );
        }
    }
}

/// M2 closes the door: the shipped set is PR-5's plus everything PR-6 landed, and the four rows
/// left absent are absent BY RULING (Q3 partition-spec evolution, Q9 `INSERT OVERWRITE`, the
/// permanent `TRUNCATE` refuse, Q7 maintenance-as-callable-ops) — none of them a sequencing
/// deferral. The risk this pins is scope creep in either direction: a surface quietly shipped
/// without its design ruling being applied, or a shipped surface quietly downgraded to an absence
/// row to make a gate green.
/// MUTATION: flip any `Tested` row to `absent(...)` → this REDs.
#[test]
fn m2_closes_the_ansi_door() {
    let absent_ids: Vec<SurfaceId> = ROWS
        .iter()
        .filter(|(_, row)| !row.is_tested())
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        absent_ids,
        vec![
            surfaces::ALTER_TABLE_PARTITION_FIELDS,
            surfaces::INSERT_OVERWRITE,
            surfaces::TRUNCATE,
            surfaces::MAINTENANCE_CALL,
        ],
        "the ANSI door's deliberate absences changed — update this pin AND \
         task/p2g-ansi-m2-ledger.md"
    );
    assert_eq!(
        ROWS.len() - absent_ids.len(),
        39,
        "shipped-surface count (PR-5 shipped 29; PR-6 added 10)"
    );
}
