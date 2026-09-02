//! The Spark door's test-only surface matrix.

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

/// The Spark door's disposition of every surface ID.
const ROWS: &[(SurfaceId, Row)] = &[
    // --- Statement forms ---.
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
            "tests::ctas::ctas_location_less_namespace_fails_loud_for_non_memory_catalog",
            SparkExtended,
        ),
    ),
    (
        surfaces::CREATE_OR_REPLACE_TABLE,
        t(
            "tests::ctas::ctas_or_replace_success_replaces_rows",
            SparkExtended,
        ),
    ),
    (
        surfaces::CREATE_TABLE_COLUMN_DEF,
        t(
            "tests::create_table::column_def_create_schema_equals_ctas_twin",
            SparkExtended,
        ),
    ),
    (
        surfaces::DROP_TABLE,
        t("tests::catalog_ops::drop_table", SparkExtended),
    ),
    (
        surfaces::CREATE_SCHEMA,
        t(
            "tests::namespace_ddl::sql_create_namespace_with_properties_round_trips",
            SparkExtended,
        ),
    ),
    (
        surfaces::DROP_SCHEMA,
        t(
            "tests::namespace_ddl::create_and_drop_namespace",
            SparkExtended,
        ),
    ),
    (
        surfaces::ALTER_TABLE_RENAME,
        t("tests::alter::alter_rename_table", SparkExtended),
    ),
    (
        surfaces::ALTER_TABLE_SCHEMA_EVOLUTION,
        t(
            "tests::alter::alter_add_rename_drop_column_schema_and_read_after",
            SparkExtended,
        ),
    ),
    (
        surfaces::ALTER_TABLE_PROPERTIES,
        t("tests::alter::alter_set_tblproperties", SparkExtended),
    ),
    (
        surfaces::ALTER_TABLE_PARTITION_FIELDS,
        t(
            "tests::alter::alter_add_drop_partition_field_and_write_after_evolution",
            SparkExtended,
        ),
    ),
    (
        surfaces::INSERT_INTO,
        t(
            "tests::router::bare_insert_applies_without_collect",
            SparkExtended,
        ),
    ),
    (
        surfaces::INSERT_OVERWRITE,
        t(
            "tests::insert_overwrite::insert_overwrite_replaces_all",
            SparkExtended,
        ),
    ),
    (
        surfaces::DELETE,
        t("tests::dml::delete_where_copy_on_write", SparkExtended),
    ),
    (
        surfaces::UPDATE,
        t("tests::dml::update_where_copy_on_write", SparkExtended),
    ),
    (
        surfaces::MERGE,
        t(
            "tests::merge::merge_upsert_updates_and_inserts",
            SparkExtended,
        ),
    ),
    (
        surfaces::TRUNCATE,
        t(
            "tests::truncate::truncate_table_wipes_rows_stamps_delete_and_preserves_history",
            SparkExtended,
        ), // pins: dml-c-truncate/C-008
    ),
    (
        surfaces::TIME_TRAVEL,
        t(
            "tests::time_travel::time_travel_version_timestamp_branch_tag_and_errors",
            SparkExtended,
        ),
    ),
    (
        surfaces::BRANCH_TAG_DDL,
        t(
            "tests::ref_ddl::branch_tag_ddl_create_drop_round_trip",
            SparkExtended,
        ),
    ),
    (
        surfaces::MAINTENANCE_CALL,
        t(
            "tests::call::call_rewrite_data_files_preserves_rows_and_reduces_files",
            SparkExtended,
        ),
    ),
    (
        surfaces::METADATA_TABLES,
        t(
            "tests::metadata_tables::metadata_tables_spark_dot_form_and_guards",
            SparkExtended,
        ),
    ),
    // The Spark door ships its OWN `SHOW NAMESPACES` / `DESCRIBE NAMESPACE` intercepts.
    (
        surfaces::INTROSPECTION,
        t(
            "tests::describe_show::show_namespaces_returns_spark_column_shape_and_real_namespaces",
            SparkExtended,
        ),
    ),
    // --- Table-creation options.
    (
        surfaces::TABLE_OPTION_FORMAT,
        t(
            "tests::ctas::ctas_parses_using_and_threads_tblproperties",
            SparkExtended,
        ),
    ),
    // pins: v3-2-create-v3-opt-in/C-012
    (
        surfaces::TABLE_OPTION_FORMAT_VERSION,
        t(
            "tests::ctas::ctas_format_version_two_consumed_others_rejected",
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
            "tests::ctas::ctas_location_check_precedes_source_execution",
            SparkExtended,
        ),
    ),
    (
        surfaces::TABLE_OPTION_RAW_PROPERTIES,
        t(
            "tests::ctas::ctas_parses_using_and_threads_tblproperties",
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
            "tests::merge::merge_merge_on_read_mode_runs_end_to_end",
            SparkExtended,
        ),
    ),
    (
        surfaces::SCHEMA_OPTION_LOCATION,
        t(
            "tests::namespace_ddl::sql_create_schema_synonym_with_location_round_trips",
            SparkExtended,
        ),
    ),
    // --- Guard rails (design §2 Q12: the Spark door keeps all v1 guards verbatim) ---.
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
            "tests::write_to_branch::write_to_tag_refuses_spark_shaped",
            SparkExtended,
        ),
    ),
    (
        surfaces::GUARD_MOR_MULTI_SPEC_DML,
        t(
            "tests::dml::bug001_mor_delete_refuses_unpartitioned_after_partition_evolution",
            SparkExtended,
        ),
    ),
    // --- Ergonomics + seams ---.
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
            "tests::alter::alter_column_case_insensitive_rename_and_drop",
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
        // The evidence lives in the OTHER crate's test binary, and that is the honest place for it.
        t(
            "repark-sql tests/cross_door.rs::cross_door_ctas_produces_the_same_table_content_and_schema",
            TwoSession,
        ),
    ),
    // --- Value semantics (H-2 G8).
    (
        surfaces::SEMANTICS_NULL_ORDERING,
        t(
            "spark_ast::tests::order_by_defaults_are_spark",
            SparkExtended,
        ),
    ),
    (
        surfaces::SEMANTICS_DECIMAL_ARITHMETIC,
        t(
            "tests::decimal::pin_add_same_precision_scale_i128",
            SparkExtended,
        ),
    ),
    (
        surfaces::SEMANTICS_CAST_MATRIX,
        t(
            "spark_door_timestamp_cast_to_bigint_is_epoch_seconds",
            SparkExtended,
        ),
    ),
    (
        surfaces::SEMANTICS_SESSION_TIMEZONE,
        t("year_extractor_resolves_in_the_session_zone", SparkExtended),
    ),
    (
        surfaces::SEMANTICS_WINDOW_FRAMES,
        t(
            "tests::window_temporal_range::temporal_range_interval_bounds_still_match_spark",
            SparkExtended,
        ),
    ),
    (
        surfaces::SEMANTICS_JOIN_NULL_KEYS,
        t(
            "tests::join_null_keys::spark_door_null_keys_never_match_inner_left_semi_anti",
            SparkExtended,
        ),
    ),
    (
        surfaces::SEMANTICS_FLOAT_DETERMINISM,
        t(
            "tests::float_agg::pin_sum_f64_bits_at_target_partitions_1",
            SparkExtended,
        ),
    ),
];

/// MUTATION: delete the `MERGE` row → this REDs naming `MERGE`.
/// The compile-run audit (design §2 Q13): this door maps EXACTLY the registry, once each.
#[test]
fn matrix_maps_every_surface() {
    if let Err(problems) = surfaces::audit("repark-spark", ROWS) {
        panic!(
            "repark-spark surface matrix is out of sync with repark_common::surfaces:\n{problems}"
        );
    }
}

/// MUTATION: flip any `Tested` row to `absent(...)` → this REDs.
/// Pin the three declared structural absences so an unreviewed fourth absence cannot pass.
#[test]
fn spark_door_absences_are_the_declared_ones() {
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
        "the Spark door's deliberate absences changed — update this pin AND \
         task/r3-g8-absences-ledger.md"
    );
    assert_eq!(ROWS.len() - absent_ids.len(), 47, "shipped-surface count");
}

/// MUTATION: mark any other row `TwoSession` → this REDs.
/// `TwoSession` may be claimed only by `CROSS_DOOR_EQUIVALENCE`, whose protocol uses both doors.
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
