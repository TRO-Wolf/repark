//! The Spark door's **surface matrix** — every `repark_common::surfaces` ID, disposed.
//!
//! Design SSOT: `docs/design/sql-doors.md` §2 Q13 (graft G2). The registry in
//! `repark_common::surfaces` is the dialect-neutral universe; this file says what THIS door does
//! with each ID, and [`matrix_maps_every_surface`] fails the build if any ID has no row. Absence
//! is typed: a surface this door does not ship carries a reason and a design citation, never
//! silence.
//!
//! The whole module is `#[cfg(test)]` — it is audit evidence, not product code, so it adds
//! nothing to the shipped crate.
//!
//! **Test names are `cargo test -p repark-spark -- --list` names**, verbatim. They are strings,
//! so nothing but review keeps them honest — when a test is renamed, its row moves with it.
//! (The ported battery's names are pinned by the census, which makes drift here loud.)

use repark_common::surfaces::{self, Row, SessionProfile, SurfaceId};

use SessionProfile::{SparkExtended, TwoSession, Unit};

/// Shorthand for a shipped surface.
const fn t(test: &'static str, profile: SessionProfile) -> Row {
    Row::Tested { test, profile }
}

/// Shorthand for a deliberate absence.
const fn absent(reason: &'static str, adr: &'static str) -> Row {
    Row::DeliberatelyAbsent { reason, adr }
}

/// ===========================================================================================
/// The Spark door's disposition of every surface ID.
///
/// The door is a VERBATIM port of v1 `repark-sql` (design §0: delegate-first, no half-file
/// surgery), so almost every row is `Tested` and names a ported battery test. The three
/// remaining absences are structural, not gaps: two are ANSI-only ergonomics (`sorted_by` /
/// unknown-key refuse have no Spark spelling to guard) and one is the wrong-door sniff (this IS
/// the Spark door). `TA_FUNCTIONS` flipped at PR-4 (`TaExtension` composition) and
/// `CROSS_DOOR_EQUIVALENCE` at PR-6, when the two-session protocol was actually run.
/// ===========================================================================================
const ROWS: &[(SurfaceId, Row)] = &[
    // --- Statement forms ---
    (
        surfaces::SELECT_PASSTHROUGH,
        t(
            "router::tests::select_passthrough_still_executes",
            SparkExtended,
        ),
    ),
    (
        surfaces::CTAS,
        t("ctas_end_to_end_through_spark_sql", SparkExtended),
    ),
    (
        surfaces::CTAS_TARGET_ROUTING,
        t(
            "tests::ctas_location_less_namespace_fails_loud_for_non_memory_catalog",
            SparkExtended,
        ),
    ),
    (
        surfaces::CREATE_OR_REPLACE_TABLE,
        t(
            "tests::ctas_or_replace_success_replaces_rows",
            SparkExtended,
        ),
    ),
    (
        surfaces::CREATE_TABLE_COLUMN_DEF,
        t(
            "tests::column_def_create_schema_equals_ctas_twin",
            SparkExtended,
        ),
    ),
    (surfaces::DROP_TABLE, t("tests::drop_table", SparkExtended)),
    (
        surfaces::CREATE_SCHEMA,
        t(
            "tests::sql_create_namespace_with_properties_round_trips",
            SparkExtended,
        ),
    ),
    (
        surfaces::DROP_SCHEMA,
        t("tests::create_and_drop_namespace", SparkExtended),
    ),
    (
        surfaces::ALTER_TABLE_RENAME,
        t("tests::alter_rename_table", SparkExtended),
    ),
    (
        surfaces::ALTER_TABLE_SCHEMA_EVOLUTION,
        t(
            "tests::alter_add_rename_drop_column_schema_and_read_after",
            SparkExtended,
        ),
    ),
    (
        surfaces::ALTER_TABLE_PROPERTIES,
        t("tests::alter_set_tblproperties", SparkExtended),
    ),
    (
        surfaces::ALTER_TABLE_PARTITION_FIELDS,
        t(
            "tests::alter_add_drop_partition_field_and_write_after_evolution",
            SparkExtended,
        ),
    ),
    (
        surfaces::INSERT_INTO,
        t("tests::bare_insert_applies_without_collect", SparkExtended),
    ),
    (
        surfaces::INSERT_OVERWRITE,
        t("tests::insert_overwrite_replaces_all", SparkExtended),
    ),
    (
        surfaces::DELETE,
        t("tests::delete_where_copy_on_write", SparkExtended),
    ),
    (
        surfaces::UPDATE,
        t("tests::update_where_copy_on_write", SparkExtended),
    ),
    (
        surfaces::MERGE,
        t("tests::merge_upsert_updates_and_inserts", SparkExtended),
    ),
    (
        surfaces::TRUNCATE,
        t(
            "tests::truncate_table_refuses_loud_naming_gap",
            SparkExtended,
        ),
    ),
    (
        surfaces::TIME_TRAVEL,
        t(
            "tests::time_travel_version_timestamp_branch_tag_and_errors",
            SparkExtended,
        ),
    ),
    (
        surfaces::BRANCH_TAG_DDL,
        t(
            "tests::branch_tag_ddl_create_drop_round_trip",
            SparkExtended,
        ),
    ),
    (
        surfaces::MAINTENANCE_CALL,
        t(
            "tests::call_rewrite_data_files_preserves_rows_and_reduces_files",
            SparkExtended,
        ),
    ),
    (
        surfaces::METADATA_TABLES,
        t(
            "tests::metadata_tables_spark_dot_form_and_guards",
            SparkExtended,
        ),
    ),
    // The Spark door ships its OWN `SHOW NAMESPACES` / `DESCRIBE NAMESPACE` intercepts, so this
    // row is Tested independently of the R2 core gap (`ReparkSession` cannot enable
    // `information_schema`, so DF-delegated `SHOW TABLES` is dead in BOTH doors — filed against
    // core in task/p2f-ansi-m1-ledger.md).
    (
        surfaces::INTROSPECTION,
        t(
            "tests::show_namespaces_returns_spark_column_shape_and_real_namespaces",
            SparkExtended,
        ),
    ),
    // --- Table-creation options. The Spark spellings are `USING` + `TBLPROPERTIES` +
    // `PARTITIONED BY` + `LOCATION`; `ctas_parses_using_and_threads_tblproperties` is the one
    // test that pins the format half AND the raw-properties half of that clause set, so it
    // legitimately backs both rows (the surfaces are distinct; the evidence is shared).
    (
        surfaces::TABLE_OPTION_FORMAT,
        t(
            "tests::ctas_parses_using_and_threads_tblproperties",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_FORMAT_VERSION,
        t(
            "tests::ctas_format_version_two_consumed_others_rejected",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_PARTITIONING,
        t(
            "tests::partitioned_ctas::ctas_mixed_identity_and_transform_spec",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_LOCATION,
        t(
            "tests::ctas_location_check_precedes_source_execution",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_RAW_PROPERTIES,
        t(
            "tests::ctas_parses_using_and_threads_tblproperties",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_SORT_ORDER,
        absent(
            "Spark's Iceberg extensions spell sort orders as `WRITE ORDERED BY`, which v1 never \
             shipped and this door does not add — the port is verbatim. The ANSI door holds the \
             spelling as a loud refuse naming the trigger (graft G9); the callable equivalent is \
             the fork's sort-order update op.",
            "docs/design/sql-doors.md §0 G9 / §2 Q1",
        ),
    ),
    (
        surfaces::TABLE_OPTION_UNKNOWN_KEY_REFUSE,
        absent(
            "`TBLPROPERTIES` is a raw key/value map in Spark — there is no curated vocabulary to \
             typo-guard, so an unknown key is data, not an error. The refuse-lists-the-set \
             behaviour exists only where a curated vocabulary does: the ANSI door's `WITH (…)`.",
            "docs/design/sql-doors.md §2 Q1",
        ),
    ),
    (
        surfaces::PARTITION_TRANSFORM_VALIDATION,
        t(
            "tests::partitioned_ctas::ctas_partition_transform_zero_width_and_unknown_rejected",
            SparkExtended,
        ),
    ),
    (
        surfaces::MOR_TABLE_CREATION,
        t(
            "tests::merge_merge_on_read_mode_runs_end_to_end",
            SparkExtended,
        ),
    ),
    (
        surfaces::SCHEMA_OPTION_LOCATION,
        t(
            "tests::sql_create_schema_synonym_with_location_round_trips",
            SparkExtended,
        ),
    ),
    // --- Guard rails (design §2 Q12: the Spark door keeps all v1 guards verbatim) ---
    (
        surfaces::GUARD_MULTI_STATEMENT,
        t(
            "router::tests::multi_statement_still_refuses_before_refuse_arms",
            Unit,
        ),
    ),
    (
        surfaces::GUARD_READ_ONLY_CATALOG,
        t("router::tests::read_only_set_reaches_p11_refusal", Unit),
    ),
    (
        surfaces::GUARD_LOCAL_FILESYSTEM,
        t(
            "local_fs_ddl::tests::refuses_copy_to_outside_warehouse_by_default",
            Unit,
        ),
    ),
    (
        surfaces::GUARD_WRITE_TO_BRANCH,
        t(
            "tests::write_to_branch_refuses_loud_naming_fork_gap",
            SparkExtended,
        ),
    ),
    (
        surfaces::GUARD_MOR_MULTI_SPEC_DML,
        t(
            "tests::bug001_mor_delete_refuses_unpartitioned_after_partition_evolution",
            SparkExtended,
        ),
    ),
    // --- Ergonomics + seams ---
    (
        surfaces::WRONG_DOOR_SNIFF,
        absent(
            "The sniff steers a user who typed Spark SQL at the ANSI door. This IS the Spark \
             door — there is nothing to steer to, and adding a reverse sniff would fire on the \
             door's own grammar. Error-path-only, ANSI-side, by design.",
            "docs/design/sql-doors.md §2 Q10 / §0 G3",
        ),
    ),
    (
        surfaces::IDENTIFIER_CASE_FOLDING,
        t(
            "tests::alter_column_case_insensitive_rename_and_drop",
            SparkExtended,
        ),
    ),
    (
        surfaces::TA_FUNCTIONS,
        t(
            "ta_window::sql_route_single_series_kernels_match_the_kernel",
            SparkExtended,
        ),
    ),
    (
        surfaces::SQL_DIALECT_SEAM,
        t(
            "dialect::tests::dialect_execute_runs_the_spark_router",
            SparkExtended,
        ),
    ),
    (
        surfaces::CROSS_DOOR_EQUIVALENCE,
        // The evidence lives in the OTHER crate's test binary
        // (`crates/repark-sql/tests/cross_door.rs`), and that is the honest place for it: the
        // protocol needs both doors in one process, and only a dev-dependency may cross the
        // door boundary. This row cites it with its crate so the reference is followable —
        // running `cargo test -p repark-spark` alone will not execute it.
        t(
            "repark-sql tests/cross_door.rs::cross_door_ctas_produces_the_same_table_content_and_schema",
            TwoSession,
        ),
    ),
];

/// ===========================================================================================
/// The compile-run audit (design §2 Q13): this door maps EXACTLY the registry, once each.
///
/// The failure this prevents is the quiet one — a surface added to
/// `repark_common::surfaces::ALL` that this door neither ships nor refuses, drifting into an
/// undocumented gap. `audit` reports unmapped IDs, stale IDs, duplicates and untraceable rows
/// together, so one run names every problem.
/// MUTATION: delete the `MERGE` row → this REDs naming `MERGE`.
/// ===========================================================================================
#[test]
fn matrix_maps_every_surface() {
    if let Err(problems) = surfaces::audit("repark-spark", ROWS) {
        panic!(
            "repark-spark surface matrix is out of sync with repark_common::surfaces:\n{problems}"
        );
    }
}

/// The Spark door is a verbatim port of a shipped v1 engine, so the shipped/absent split is
/// itself a fact worth pinning: three deliberate absences after PR-6, every one of them named
/// above. A fourth absence appearing without a reviewer noticing is exactly how "typed absence"
/// rots into "typed excuse".
/// MUTATION: flip any `Tested` row to `absent(...)` → this REDs.
#[test]
fn spark_door_absences_are_the_three_declared_ones() {
    let absent_ids: Vec<SurfaceId> = ROWS
        .iter()
        .filter(|(_, row)| !row.is_tested())
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        absent_ids,
        vec![
            surfaces::TABLE_OPTION_SORT_ORDER,
            surfaces::TABLE_OPTION_UNKNOWN_KEY_REFUSE,
            surfaces::WRONG_DOOR_SNIFF,
        ],
        "the Spark door's deliberate absences changed — update this pin AND the ledger"
    );
    assert_eq!(ROWS.len() - absent_ids.len(), 40, "shipped-surface count");
}

/// `TwoSession` may be claimed by exactly ONE row — `CROSS_DOOR_EQUIVALENCE` — and only because
/// PR-6 actually ran the protocol. The failure this prevents is the original one restated: a
/// single-door battery cannot produce cross-door evidence, so letting an ordinary ported test
/// claim the profile would launder exactly what the protocol exists to establish.
/// MUTATION: mark any other row `TwoSession` → this REDs.
#[test]
fn only_the_cross_door_row_claims_the_two_session_profile() {
    for (id, row) in ROWS {
        if let Row::Tested {
            profile: SessionProfile::TwoSession,
            ..
        } = row
        {
            assert_eq!(
                *id,
                surfaces::CROSS_DOOR_EQUIVALENCE,
                "{id}: cross-door evidence cannot come from a single-door test"
            );
        }
    }
}
