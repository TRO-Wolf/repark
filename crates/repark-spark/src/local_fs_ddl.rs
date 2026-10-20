pub(crate) use repark_core::local_fs_policy::refuse_local_filesystem_plan;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use repark_core::CatalogRegistry;
    use repark_core::local_fs_policy::local_filesystem_path;
    use repark_functions::cardinality::{
        ALLOW_LOCAL_FILESYSTEM_DDL_KEY, ReparkSqlSettings, with_repark_sql_config,
    };
    use tempfile::TempDir;

    #[test]
    fn classifies_local_and_remote() {
        assert!(local_filesystem_path("/etc/passwd").is_some());
        assert!(local_filesystem_path("file:///etc/passwd").is_some());
        // Case variants of file:// must classify local (not unknown remote).
        assert_eq!(
            local_filesystem_path("FILE:///etc/passwd").expect("FILE:// is local"),
            PathBuf::from("/etc/passwd")
        );
        assert_eq!(
            local_filesystem_path("File:///etc/passwd").expect("File:// is local"),
            PathBuf::from("/etc/passwd")
        );
        assert!(local_filesystem_path("relative/out.parquet").is_some());
        assert!(local_filesystem_path("s3://bucket/key").is_none());
        assert!(local_filesystem_path("S3://bucket/key").is_none());
        assert!(local_filesystem_path("s3a://bucket/key").is_none());
        assert!(local_filesystem_path("https://example/x").is_none());
    }

    #[test]
    fn file_url_strips_scheme() {
        let path = local_filesystem_path("file:///tmp/wh/out").expect("file:// local");
        assert_eq!(path, PathBuf::from("/tmp/wh/out"));
        let localhost =
            local_filesystem_path("file://localhost/tmp/wh/out").expect("file://localhost local");
        assert_eq!(localhost, PathBuf::from("/tmp/wh/out"));
        let localhost_upper =
            local_filesystem_path("FILE://LOCALHOST/tmp/wh/out").expect("FILE://LOCALHOST local");
        assert_eq!(localhost_upper, PathBuf::from("/tmp/wh/out"));
    }

    async fn plan_sql(ctx: &SessionContext, sql: &str) -> datafusion::logical_expr::LogicalPlan {
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state
            .sql_to_statement(sql, &dialect)
            .expect("sql_to_statement");
        state
            .statement_to_plan(statement)
            .await
            .expect("statement_to_plan")
    }

    fn default_ctx() -> SessionContext {
        let settings = ReparkSqlSettings::default();
        assert!(!settings.allow_local_filesystem_ddl);
        let config = with_repark_sql_config(SessionConfig::new(), settings);
        SessionContext::new_with_config(config)
    }

    #[tokio::test]
    async fn refuses_copy_to_outside_warehouse_by_default() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let outside = TempDir::new().expect("outside tempdir");
        let dest = outside.path().join("leak.parquet");
        let dest_str = dest.to_str().expect("utf8 dest");

        let ctx = default_ctx();
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(warehouse.path().to_string_lossy());

        let sql = format!("COPY (SELECT 1 AS a) TO '{dest_str}' STORED AS PARQUET");
        let plan = plan_sql(&ctx, &sql).await;
        let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect_err("outside warehouse must refuse")
            .to_string();
        assert!(
            err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
            "must name conf: {err}"
        );
        assert!(err.contains("COPY TO"), "must name surface: {err}");
    }

    /// Classic sensitive path + case-variant FILE:// must refuse.
    #[tokio::test]
    async fn refuses_copy_to_etc_passwd_and_file_scheme_case() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let ctx = default_ctx();
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(warehouse.path().to_string_lossy());

        for location in [
            "/etc/passwd",
            "file:///etc/passwd",
            "FILE:///etc/passwd",
            "File:///etc/passwd",
        ] {
            let sql = format!("COPY (SELECT 1 AS a) TO '{location}' STORED AS PARQUET");
            let plan = plan_sql(&ctx, &sql).await;
            let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
                .expect_err("etc/passwd-class must refuse")
                .to_string();
            assert!(
                err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
                "must name conf for {location}: {err}"
            );
            assert!(
                err.contains("COPY TO"),
                "must name surface for {location}: {err}"
            );
        }
    }

    /// CREATE EXTERNAL TABLE LOCATION is gated (not only COPY TO).
    #[tokio::test]
    async fn refuses_create_external_local_outside_warehouse() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let outside = TempDir::new().expect("outside tempdir");
        let loc = outside.path().join("ext_table");
        let loc_str = loc.to_str().expect("utf8 loc");

        let ctx = default_ctx();
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(warehouse.path().to_string_lossy());

        let sql = format!(
            "CREATE EXTERNAL TABLE blocked_ext (a INT) STORED AS PARQUET LOCATION '{loc_str}'"
        );
        let plan = plan_sql(&ctx, &sql).await;
        assert!(
            matches!(
                plan,
                datafusion::logical_expr::LogicalPlan::Ddl(
                    datafusion::logical_expr::DdlStatement::CreateExternalTable(_)
                )
            ),
            "fixture must plan as CreateExternalTable, got {plan:?}"
        );
        let err = refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect_err("CREATE EXTERNAL local outside warehouse must refuse")
            .to_string();
        assert!(
            err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
            "must name conf: {err}"
        );
        assert!(
            err.contains("CREATE EXTERNAL TABLE"),
            "must name surface: {err}"
        );
    }

    /// CREATE EXTERNAL under warehouse root is grandfathered.
    #[tokio::test]
    async fn grandfathers_create_external_under_warehouse_root() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let loc = warehouse.path().join("ext_ok");
        let loc_str = loc.to_str().expect("utf8 loc");

        let ctx = default_ctx();
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(warehouse.path().to_string_lossy());

        let sql =
            format!("CREATE EXTERNAL TABLE ok_ext (a INT) STORED AS PARQUET LOCATION '{loc_str}'");
        let plan = plan_sql(&ctx, &sql).await;
        refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect("CREATE EXTERNAL under warehouse must grandfather");
    }

    #[tokio::test]
    async fn grandfathers_copy_under_warehouse_root() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let dest = warehouse.path().join("exported");
        let dest_str = dest.to_str().expect("utf8 dest");

        let ctx = default_ctx();
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(warehouse.path().to_string_lossy());

        let sql = format!("COPY (SELECT 1 AS a) TO '{dest_str}' STORED AS PARQUET");
        let plan = plan_sql(&ctx, &sql).await;
        refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect("COPY under warehouse must grandfather");
    }

    #[tokio::test]
    async fn allow_conf_true_permits_outside() {
        let outside = TempDir::new().expect("outside tempdir");
        let dest = outside.path().join("ok.parquet");
        let dest_str = dest.to_str().expect("utf8 dest");

        let settings = ReparkSqlSettings {
            allow_local_filesystem_ddl: true,
            ..ReparkSqlSettings::default()
        };
        let config = with_repark_sql_config(SessionConfig::new(), settings);
        let ctx = SessionContext::new_with_config(config);
        let catalogs = CatalogRegistry::new();

        let sql = format!("COPY (SELECT 1 AS a) TO '{dest_str}' STORED AS PARQUET");
        let plan = plan_sql(&ctx, &sql).await;
        refuse_local_filesystem_plan(&ctx, &catalogs, &plan)
            .expect("conf true must permit local outside warehouse");
    }

    #[test]
    fn config_map_default_is_false() {
        let empty = HashMap::<String, String>::new();
        let settings = repark_functions::cardinality::repark_sql_settings_from_config_map(&empty)
            .expect("empty conf map parses");
        assert!(!settings.allow_local_filesystem_ddl);
        let _ = Arc::new(settings); // silence unused in some toolchains
    }
}
