use crate::call_args::ParamDecl;

pub(crate) const ADD_FILES_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "source_table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "partition_filter",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "check_duplicate_files",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "parallelism",
        data_type: "IntegerType",
        required: false,
    },
];

pub(crate) const ANCESTORS_OF_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: false,
    },
];

pub(crate) const CHERRYPICK_SNAPSHOT_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: true,
    },
];

pub(crate) const COMPUTE_PARTITION_STATS_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: false,
    },
];

pub(crate) const COMPUTE_TABLE_STATS_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: false,
    },
    ParamDecl {
        name: "columns",
        data_type: "None",
        required: false,
    },
];

pub(crate) const CREATE_CHANGELOG_VIEW_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "changelog_view",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "options",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "compute_updates",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "identifier_columns",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "net_changes",
        data_type: "BooleanType",
        required: false,
    },
];

pub(crate) const EXPIRE_SNAPSHOTS_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "older_than",
        data_type: "TimestampType",
        required: false,
    },
    ParamDecl {
        name: "retain_last",
        data_type: "IntegerType",
        required: false,
    },
    ParamDecl {
        name: "max_concurrent_deletes",
        data_type: "IntegerType",
        required: false,
    },
    ParamDecl {
        name: "stream_results",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "snapshot_ids",
        data_type: "createArrayType",
        required: false,
    },
    ParamDecl {
        name: "clean_expired_metadata",
        data_type: "BooleanType",
        required: false,
    },
];

pub(crate) const FAST_FORWARD_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "branch",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "to",
        data_type: "StringType",
        required: true,
    },
];

pub(crate) const MIGRATE_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "properties",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "drop_backup",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "backup_table_name",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "parallelism",
        data_type: "IntegerType",
        required: false,
    },
];

pub(crate) const PUBLISH_CHANGES_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "wap_id",
        data_type: "StringType",
        required: true,
    },
];

pub(crate) const REGISTER_TABLE_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "metadata_file",
        data_type: "StringType",
        required: true,
    },
];

pub(crate) const REMOVE_ORPHAN_FILES_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "older_than",
        data_type: "TimestampType",
        required: false,
    },
    ParamDecl {
        name: "location",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "dry_run",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "max_concurrent_deletes",
        data_type: "IntegerType",
        required: false,
    },
    ParamDecl {
        name: "file_list_view",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "equal_schemes",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "equal_authorities",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "prefix_mismatch_mode",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "prefix_listing",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "stream_results",
        data_type: "BooleanType",
        required: false,
    },
];

pub(crate) const REWRITE_DATA_FILES_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "strategy",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "sort_order",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "options",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "where",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "branch",
        data_type: "StringType",
        required: false,
    },
];

pub(crate) const REWRITE_MANIFESTS_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "use_caching",
        data_type: "BooleanType",
        required: false,
    },
    ParamDecl {
        name: "spec_id",
        data_type: "IntegerType",
        required: false,
    },
    ParamDecl {
        name: "sort_by",
        data_type: "createArrayType",
        required: false,
    },
];

pub(crate) const REWRITE_POSITION_DELETE_FILES_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "options",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "where",
        data_type: "StringType",
        required: false,
    },
];

pub(crate) const REWRITE_TABLE_PATH_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "source_prefix",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "target_prefix",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "start_version",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "end_version",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "staging_location",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "create_file_list",
        data_type: "BooleanType",
        required: false,
    },
];

pub(crate) const ROLLBACK_TO_SNAPSHOT_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: true,
    },
];

pub(crate) const ROLLBACK_TO_TIMESTAMP_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "timestamp",
        data_type: "TimestampType",
        required: true,
    },
];

pub(crate) const SET_CURRENT_SNAPSHOT_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "snapshot_id",
        data_type: "LongType",
        required: false,
    },
    ParamDecl {
        name: "ref",
        data_type: "StringType",
        required: false,
    },
];

pub(crate) const SNAPSHOT_PARAMS: &[ParamDecl] = &[
    ParamDecl {
        name: "source_table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "table",
        data_type: "StringType",
        required: true,
    },
    ParamDecl {
        name: "location",
        data_type: "StringType",
        required: false,
    },
    ParamDecl {
        name: "properties",
        data_type: "None",
        required: false,
    },
    ParamDecl {
        name: "parallelism",
        data_type: "IntegerType",
        required: false,
    },
];

pub(crate) fn params_for(procedure: &str) -> &'static [ParamDecl] {
    match procedure {
        "add_files" => ADD_FILES_PARAMS,
        "ancestors_of" => ANCESTORS_OF_PARAMS,
        "cherrypick_snapshot" => CHERRYPICK_SNAPSHOT_PARAMS,
        "compute_partition_stats" => COMPUTE_PARTITION_STATS_PARAMS,
        "compute_table_stats" => COMPUTE_TABLE_STATS_PARAMS,
        "create_changelog_view" => CREATE_CHANGELOG_VIEW_PARAMS,
        "expire_snapshots" => EXPIRE_SNAPSHOTS_PARAMS,
        "fast_forward" => FAST_FORWARD_PARAMS,
        "migrate" => MIGRATE_PARAMS,
        "publish_changes" => PUBLISH_CHANGES_PARAMS,
        "register_table" => REGISTER_TABLE_PARAMS,
        "remove_orphan_files" => REMOVE_ORPHAN_FILES_PARAMS,
        "rewrite_data_files" => REWRITE_DATA_FILES_PARAMS,
        "rewrite_manifests" => REWRITE_MANIFESTS_PARAMS,
        "rewrite_position_delete_files" => REWRITE_POSITION_DELETE_FILES_PARAMS,
        "rewrite_table_path" => REWRITE_TABLE_PATH_PARAMS,
        "rollback_to_snapshot" => ROLLBACK_TO_SNAPSHOT_PARAMS,
        "rollback_to_timestamp" => ROLLBACK_TO_TIMESTAMP_PARAMS,
        "set_current_snapshot" => SET_CURRENT_SNAPSHOT_PARAMS,
        "snapshot" => SNAPSHOT_PARAMS,
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names_of(params: &[ParamDecl]) -> Vec<&str> {
        params.iter().map(|param| param.name).collect()
    }

    #[test]
    fn rewrite_data_files_params_follow_the_jar_order() {
        assert_eq!(
            names_of(REWRITE_DATA_FILES_PARAMS),
            vec![
                "table",
                "strategy",
                "sort_order",
                "options",
                "where",
                "branch"
            ]
        );
        assert!(REWRITE_DATA_FILES_PARAMS[0].required);
        assert!(
            REWRITE_DATA_FILES_PARAMS[1..]
                .iter()
                .all(|param| !param.required)
        );
        assert_eq!(REWRITE_DATA_FILES_PARAMS[0].data_type, "StringType");
        assert_eq!(REWRITE_DATA_FILES_PARAMS[3].data_type, "None");
        assert_eq!(REWRITE_DATA_FILES_PARAMS[5].data_type, "StringType");
    }

    #[test]
    fn rewrite_position_delete_files_params_follow_the_jar_order() {
        assert_eq!(
            names_of(REWRITE_POSITION_DELETE_FILES_PARAMS),
            vec!["table", "options", "where"]
        );
        assert!(REWRITE_POSITION_DELETE_FILES_PARAMS[0].required);
        assert!(
            REWRITE_POSITION_DELETE_FILES_PARAMS[1..]
                .iter()
                .all(|param| !param.required)
        );
    }

    #[test]
    fn params_for_resolves_every_transcribed_procedure() {
        for procedure in [
            "add_files",
            "ancestors_of",
            "cherrypick_snapshot",
            "compute_partition_stats",
            "compute_table_stats",
            "create_changelog_view",
            "expire_snapshots",
            "fast_forward",
            "migrate",
            "publish_changes",
            "register_table",
            "remove_orphan_files",
            "rewrite_data_files",
            "rewrite_manifests",
            "rewrite_position_delete_files",
            "rewrite_table_path",
            "rollback_to_snapshot",
            "rollback_to_timestamp",
            "set_current_snapshot",
            "snapshot",
        ] {
            assert!(
                !params_for(procedure).is_empty(),
                "{procedure} must resolve to a declared list"
            );
            let first = if procedure == "snapshot" {
                "source_table"
            } else {
                "table"
            };
            assert_eq!(params_for(procedure)[0].name, first);
        }
        assert!(params_for("not_a_procedure").is_empty());
    }
}
