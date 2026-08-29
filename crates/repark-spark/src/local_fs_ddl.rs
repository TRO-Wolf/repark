//! Local-filesystem DDL gate.
//!
//! The default `repark.sql.allowLocalFilesystemDDL = false` refuses local and `file://` targets
//! outside a registered session warehouse root. Remote schemes are outside this gate.

use std::path::{Component, Path, PathBuf};

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use repark_functions::cardinality::{
    ALLOW_LOCAL_FILESYSTEM_DDL_KEY, repark_sql_settings_from_options,
};

use repark_core::CatalogRegistry;

/// ===========================================================================================
/// Refuse local-filesystem `CREATE EXTERNAL TABLE` / `COPY TO` when conf is false and the path
/// is not under a registered warehouse root. No-op for non-DDL / remote locations.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] naming [`ALLOW_LOCAL_FILESYSTEM_DDL_KEY`] when blocked.
pub fn refuse_local_filesystem_plan(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    let settings = repark_sql_settings_from_options(ctx.copied_config().options());
    if settings.allow_local_filesystem_ddl {
        return Ok(());
    }
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)) => {
            refuse_local_path(catalogs, "CREATE EXTERNAL TABLE", &create.location)
        }
        LogicalPlan::Copy(copy_to) => refuse_local_path(catalogs, "COPY TO", &copy_to.output_url),
        _ => Ok(()),
    }
}

fn refuse_local_path(catalogs: &CatalogRegistry, surface: &str, raw_location: &str) -> Result<()> {
    let Some(local_path) = local_filesystem_path(raw_location) else {
        return Ok(());
    };
    if path_under_any_warehouse(catalogs, &local_path) {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "{surface} to local filesystem path `{raw_location}` is disabled by default. Set conf \
         `{ALLOW_LOCAL_FILESYSTEM_DDL_KEY}` = true to allow local CREATE EXTERNAL TABLE / COPY TO \
         outside the session warehouse root, or use a path under a registered warehouse \
         (grandfather). Remote locations (s3://, s3a://) are unaffected."
    )))
}

/// Case-insensitive strip of an ASCII scheme prefix (`file://`, `file:`). Returns the remainder.
fn strip_ascii_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix_len = prefix.len();
    if value.len() >= prefix_len && value[..prefix_len].eq_ignore_ascii_case(prefix) {
        Some(&value[prefix_len..])
    } else {
        None
    }
}

/// Map a location string to a local [`PathBuf`] when it is bare or `file://`; `None` for remote.
///
/// Scheme matching is **case-insensitive** for both the remote allowlist and `file:` / `file://`
/// (`FILE:///etc/passwd` must not classify as remote and skip the gate).
#[must_use]
pub fn local_filesystem_path(location: &str) -> Option<PathBuf> {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("s3://")
        || lower.starts_with("s3a://")
        || lower.starts_with("s3n://")
        || lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("hdfs://")
        || lower.starts_with("viewfs://")
        || lower.starts_with("gs://")
        || lower.starts_with("abfs://")
        || lower.starts_with("abfss://")
        || lower.starts_with("wasb://")
        || lower.starts_with("wasbs://")
    {
        return None;
    }
    // file:// first (before bare `file:`), case-insensitive — FILE:// must stay local.
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file://") {
        // file:///abs → /abs; file://localhost/abs → /abs; LocalHost variant too.
        let path = strip_ascii_prefix_ci(rest, "localhost").unwrap_or(rest);
        return Some(PathBuf::from(path));
    }
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file:") {
        return Some(PathBuf::from(rest));
    }
    // scheme-looking tokens that are not file: leave to remote / DF
    if let Some(idx) = trimmed.find("://") {
        let scheme = &trimmed[..idx];
        if scheme
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '.')
            && !scheme.is_empty()
        {
            return None;
        }
    }
    Some(PathBuf::from(trimmed))
}

fn path_under_any_warehouse(catalogs: &CatalogRegistry, path: &Path) -> bool {
    let roots = catalogs.local_warehouse_roots();
    if roots.is_empty() {
        return false;
    }
    let canonical_target = canonicalize_best_effort(path);
    for root in roots {
        let canonical_root = canonicalize_best_effort(Path::new(root));
        if path_is_under(&canonical_target, &canonical_root) {
            return true;
        }
    }
    false
}

fn canonicalize_best_effort(path: &Path) -> PathBuf {
    if let Ok(canon) = path.canonicalize() {
        return canon;
    }
    // Parent may exist when the leaf does not yet (COPY TO destination).
    if let Some(parent) = path.parent()
        && let Ok(canon_parent) = parent.canonicalize()
    {
        if let Some(name) = path.file_name() {
            return canon_parent.join(name);
        }
        return canon_parent;
    }
    normalize_lexically(path)
}

/// Collapse `.` / `..` without requiring the path to exist.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn path_is_under(path: &Path, root: &Path) -> bool {
    if path == root {
        return true;
    }
    path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use repark_functions::cardinality::{ReparkSqlSettings, with_repark_sql_config};
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
