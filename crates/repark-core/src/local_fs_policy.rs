use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;
use crate::dialect::{EngineContext, SqlDialect};
use crate::pre_execute::PreExecute;

pub const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

const REPARK_SQL_EXTENSION_PREFIX: &str = "repark.sql";

const ALLOW_LOCAL_FILESYSTEM_DDL_FIELD: &str = "allow_local_filesystem_ddl";

const REMOTE_SCHEMES: &[&str] = &[
    "s3://",
    "s3a://",
    "s3n://",
    "http://",
    "https://",
    "hdfs://",
    "viewfs://",
    "gs://",
    "abfs://",
    "abfss://",
    "wasb://",
    "wasbs://",
];

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalFsGuardedDialect;

#[async_trait(?Send)]
impl SqlDialect for LocalFsGuardedDialect {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> Result<DataFrame> {
        let belt = PreExecute::from_engine_context(&cx);
        let plan = belt.plan(query).await?;
        refuse_local_filesystem_plan(cx.ctx, cx.catalogs, &plan)?;
        belt.guard(&plan)?;
        belt.execute(plan).await
    }
}

#[must_use]
pub fn allow_local_filesystem_ddl(ctx: &SessionContext) -> bool {
    let config = ctx.copied_config();
    config
        .options()
        .extensions
        .iter()
        .filter(|(prefix, _)| *prefix == REPARK_SQL_EXTENSION_PREFIX)
        .flat_map(|(_, extension)| extension.entries())
        .find(|entry| {
            let key = entry.key.as_str();
            let field = key
                .strip_prefix(REPARK_SQL_EXTENSION_PREFIX)
                .and_then(|rest| rest.strip_prefix('.'))
                .unwrap_or(key);
            field == ALLOW_LOCAL_FILESYSTEM_DDL_FIELD
        })
        .and_then(|entry| entry.value)
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_local_filesystem_plan(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    if allow_local_filesystem_ddl(ctx) {
        return Ok(());
    }
    refuse_local_filesystem_writes(catalogs, plan)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_local_filesystem_writes(
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    plan.apply(|node| {
        match node {
            LogicalPlan::Explain(_) => return Ok(TreeNodeRecursion::Jump),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)) => {
                refuse_local_path(catalogs, "CREATE EXTERNAL TABLE", &create.location)?;
            }
            LogicalPlan::Copy(copy_to) => {
                refuse_local_path(catalogs, "COPY TO", &copy_to.output_url)?;
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
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

#[must_use]
pub fn strip_ascii_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        value.get(prefix.len()..)
    } else {
        None
    }
}

#[must_use]
pub fn local_filesystem_path(location: &str) -> Option<PathBuf> {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return None;
    }
    if REMOTE_SCHEMES
        .iter()
        .any(|scheme| strip_ascii_prefix_ci(trimmed, scheme).is_some())
    {
        return None;
    }
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file://") {
        let path = strip_ascii_prefix_ci(rest, "localhost").unwrap_or(rest);
        return Some(PathBuf::from(path));
    }
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file:") {
        return Some(PathBuf::from(rest));
    }
    if let Some((scheme, _)) = trimmed.split_once("://")
        && !scheme.is_empty()
        && scheme
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
    {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

#[must_use]
pub fn path_under_any_warehouse(catalogs: &CatalogRegistry, path: &Path) -> bool {
    let Some(target) = resolve_physical_destination(path) else {
        return false;
    };
    catalogs
        .local_warehouse_roots()
        .iter()
        .filter_map(|root| resolve_physical_destination(Path::new(root)))
        .any(|root| target.starts_with(&root))
}

#[must_use]
pub fn resolve_physical_destination(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    let components: Vec<Component<'_>> = absolute.components().collect();
    for existing in (1..=components.len()).rev() {
        let (head, tail) = components.split_at(existing);
        let ancestor: PathBuf = head.iter().collect();
        match ancestor.symlink_metadata() {
            Ok(_) => return join_missing_tail(ancestor.canonicalize().ok()?, tail),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    None
}

fn join_missing_tail(mut resolved: PathBuf, tail: &[Component<'_>]) -> Option<PathBuf> {
    for component in tail {
        match component {
            Component::Normal(name) => resolved.push(name),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn registry(root: &Path) -> CatalogRegistry {
        let mut catalogs = CatalogRegistry::new();
        catalogs.note_local_warehouse_root(root.to_string_lossy());
        catalogs
    }

    #[test]
    fn a_multibyte_character_across_every_prefix_boundary_classifies_without_panic() {
        assert_eq!(local_filesystem_path("aaaaé"), Some(PathBuf::from("aaaaé")));
        assert_eq!(
            local_filesystem_path("aaaaaaé"),
            Some(PathBuf::from("aaaaaaé"))
        );
        assert_eq!(
            local_filesystem_path("file://aaaaaaaaé"),
            Some(PathBuf::from("aaaaaaaaé"))
        );
        assert_eq!(local_filesystem_path("aé"), Some(PathBuf::from("aé")));
        assert_eq!(
            local_filesystem_path("日本語"),
            Some(PathBuf::from("日本語"))
        );
        assert_eq!(
            local_filesystem_path("file://localhost/srv/é"),
            Some(PathBuf::from("/srv/é"))
        );
        assert_eq!(
            local_filesystem_path("FILE://LocalHost/srv/日本"),
            Some(PathBuf::from("/srv/日本"))
        );
        assert_eq!(
            local_filesystem_path("file:/srv/é"),
            Some(PathBuf::from("/srv/é"))
        );
        assert_eq!(local_filesystem_path("s3://bucket/é"), None);
        assert_eq!(
            local_filesystem_path("gé://bucket/key"),
            Some(PathBuf::from("gé://bucket/key"))
        );
        assert_eq!(local_filesystem_path("custom+x://bucket/key"), None);
        assert_eq!(local_filesystem_path("   "), None);
    }

    #[test]
    fn the_checked_prefix_strip_refuses_a_split_character() {
        assert_eq!(strip_ascii_prefix_ci("aaaaé", "file:"), None);
        assert_eq!(strip_ascii_prefix_ci("aaaaaaé", "file://"), None);
        assert_eq!(strip_ascii_prefix_ci("aaaaaaaaé", "localhost"), None);
        assert_eq!(strip_ascii_prefix_ci("FiLe:é", "file:"), Some("é"));
        assert_eq!(strip_ascii_prefix_ci("file", "file:"), None);
    }

    #[test]
    fn the_nearest_existing_ancestor_resolves_through_a_symlink() {
        let warehouse = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        symlink(outside.path(), warehouse.path().join("link")).unwrap();
        let real_outside = outside.path().canonicalize().unwrap();
        assert_eq!(
            resolve_physical_destination(&warehouse.path().join("link/new/export")),
            Some(real_outside.join("new/export"))
        );
        assert_eq!(
            resolve_physical_destination(&warehouse.path().join("link/m1/m2/m3/export")),
            Some(real_outside.join("m1/m2/m3/export"))
        );
        let catalogs = registry(warehouse.path());
        assert!(!path_under_any_warehouse(
            &catalogs,
            &warehouse.path().join("link/new/export")
        ));
        assert!(path_under_any_warehouse(
            &catalogs,
            &warehouse.path().join("m1/m2/export")
        ));
    }

    #[test]
    fn a_dot_dot_after_the_nearest_existing_ancestor_is_indeterminate() {
        let warehouse = TempDir::new().unwrap();
        let path = warehouse.path().join("missing/../../escaped");
        assert_eq!(resolve_physical_destination(&path), None);
        assert!(!path_under_any_warehouse(
            &registry(warehouse.path()),
            &path
        ));
    }

    #[test]
    fn a_dangling_symlink_is_indeterminate() {
        let warehouse = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        symlink(
            outside.path().join("absent"),
            warehouse.path().join("dangling"),
        )
        .unwrap();
        assert_eq!(
            resolve_physical_destination(&warehouse.path().join("dangling")),
            None
        );
        assert_eq!(
            resolve_physical_destination(&warehouse.path().join("dangling/child")),
            None
        );
    }

    #[test]
    fn a_dot_dot_inside_the_existing_part_resolves_physically() {
        let warehouse = TempDir::new().unwrap();
        std::fs::create_dir(warehouse.path().join("a")).unwrap();
        let path = warehouse.path().join("a/../b/c");
        assert_eq!(
            resolve_physical_destination(&path),
            Some(warehouse.path().canonicalize().unwrap().join("b/c"))
        );
    }

    #[test]
    fn a_missing_warehouse_root_still_contains_its_own_children() {
        let base = TempDir::new().unwrap();
        let root = base.path().join("not_created_yet");
        let catalogs = registry(&root);
        assert!(path_under_any_warehouse(&catalogs, &root.join("t/data")));
        assert!(!path_under_any_warehouse(
            &catalogs,
            &base.path().join("sibling")
        ));
    }

    #[derive(Debug, Clone, Default)]
    struct StandInAllow {
        allow: bool,
    }

    impl datafusion::common::config::ConfigExtension for StandInAllow {
        const PREFIX: &'static str = "repark.sql";
    }

    impl datafusion::common::config::ExtensionOptions for StandInAllow {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
        fn cloned(&self) -> Box<dyn datafusion::common::config::ExtensionOptions> {
            Box::new(self.clone())
        }
        fn set(&mut self, key: &str, value: &str) -> Result<()> {
            if key == "allow_local_filesystem_ddl" {
                self.allow = value.eq_ignore_ascii_case("true");
            }
            Ok(())
        }
        fn entries(&self) -> Vec<datafusion::common::config::ConfigEntry> {
            vec![datafusion::common::config::ConfigEntry {
                key: format!("{REPARK_SQL_EXTENSION_PREFIX}.{ALLOW_LOCAL_FILESYSTEM_DDL_FIELD}"),
                value: Some(self.allow.to_string()),
                description: "stand-in",
            }]
        }
    }

    datafusion::common::extensions_options! {
        struct Unprefixed {
            allow_local_filesystem_ddl: bool, default = false
        }
    }

    impl datafusion::common::config::ConfigExtension for Unprefixed {
        const PREFIX: &'static str = "repark.sql";
    }

    fn unprefixed_ctx(allow: bool) -> SessionContext {
        let mut config = datafusion::prelude::SessionConfig::new();
        config.options_mut().extensions.insert(Unprefixed {
            allow_local_filesystem_ddl: allow,
        });
        SessionContext::new_with_config(config)
    }

    fn native_ctx(allow: bool) -> SessionContext {
        let mut config = datafusion::prelude::SessionConfig::new();
        config
            .options_mut()
            .extensions
            .insert(StandInAllow { allow });
        SessionContext::new_with_config(config)
    }

    async fn native_run(
        ctx: &SessionContext,
        catalogs: &CatalogRegistry,
        sql: &str,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let read_only = std::collections::HashSet::new();
        LocalFsGuardedDialect
            .execute(EngineContext::new(ctx, catalogs, &read_only), sql)
            .await?
            .collect()
            .await
    }

    fn file_count(root: &Path) -> usize {
        std::fs::read_dir(root).map_or(0, |entries| {
            entries
                .flatten()
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() { file_count(&path) } else { 1 }
                })
                .sum()
        })
    }

    #[tokio::test]
    async fn the_native_door_refuses_every_executing_write_outside() {
        let warehouse = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        symlink(outside.path(), warehouse.path().join("link")).unwrap();
        let ctx = native_ctx(false);
        let catalogs = registry(warehouse.path());
        let out = outside.path().to_str().unwrap();
        let wh = warehouse.path().to_str().unwrap();
        for sql in [
            format!("COPY (SELECT 1 AS a) TO '{out}/direct' STORED AS PARQUET"),
            format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{out}/wrapped' STORED AS PARQUET"),
            format!("COPY (SELECT 1 AS a) TO '{wh}/link/m1/m2/m3/export' STORED AS PARQUET"),
            "COPY (SELECT 1 AS a) TO 'aaaaaaé' STORED AS PARQUET".to_string(),
            format!("CREATE EXTERNAL TABLE ext_n (a INT) STORED AS PARQUET LOCATION '{out}/ext/'"),
        ] {
            let err = native_run(&ctx, &catalogs, &sql)
                .await
                .expect_err("the native door must refuse")
                .to_string();
            assert!(err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY), "{sql}: {err}");
        }
        assert_eq!(file_count(outside.path()), 0, "nothing may land outside");
        assert!(!Path::new("aaaaaaé").exists());
    }

    #[tokio::test]
    async fn the_native_door_keeps_explain_and_both_controls() {
        let warehouse = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let ctx = native_ctx(false);
        let catalogs = registry(warehouse.path());
        let out = outside.path().to_str().unwrap();
        let wh = warehouse.path().to_str().unwrap();
        native_run(
            &ctx,
            &catalogs,
            &format!("EXPLAIN COPY (SELECT 1 AS a) TO '{out}/explained' STORED AS PARQUET"),
        )
        .await
        .expect("plain EXPLAIN must answer");
        assert_eq!(
            file_count(outside.path()),
            0,
            "plain EXPLAIN must not write"
        );
        native_run(
            &ctx,
            &catalogs,
            &format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{wh}/m1/inside' STORED AS PARQUET"),
        )
        .await
        .expect("the warehouse control must answer");
        assert!(file_count(&warehouse.path().join("m1/inside")) > 0);
        let open = native_ctx(true);
        native_run(
            &open,
            &CatalogRegistry::new(),
            &format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{out}/opened' STORED AS PARQUET"),
        )
        .await
        .expect("the opt-in control must answer");
        assert!(file_count(&outside.path().join("opened")) > 0);
    }

    async fn copy_plan(ctx: &SessionContext, location: &Path) -> LogicalPlan {
        ctx.state()
            .create_logical_plan(&format!(
                "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
                location.display()
            ))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_walk_checks_a_write_below_every_carrier_but_plain_explain() {
        let warehouse = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let catalogs = registry(warehouse.path());
        let ctx = SessionContext::new();
        let copy = copy_plan(&ctx, &outside.path().join("x")).await;
        let prepared = LogicalPlan::Statement(datafusion::logical_expr::Statement::Prepare(
            datafusion::logical_expr::Prepare {
                name: "p".to_string(),
                fields: vec![],
                input: std::sync::Arc::new(copy.clone()),
            },
        ));
        let analyzed = datafusion::logical_expr::LogicalPlanBuilder::from(copy.clone())
            .explain(false, true)
            .unwrap()
            .build()
            .unwrap();
        let explained = datafusion::logical_expr::LogicalPlanBuilder::from(copy.clone())
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();
        let explained_analyze =
            datafusion::logical_expr::LogicalPlanBuilder::from(analyzed.clone())
                .explain(false, false)
                .unwrap()
                .build()
                .unwrap();
        assert!(matches!(analyzed, LogicalPlan::Analyze(_)));
        assert!(matches!(explained, LogicalPlan::Explain(_)));
        for refused in [&copy, &prepared, &analyzed] {
            let err = refuse_local_filesystem_writes(&catalogs, refused)
                .unwrap_err()
                .to_string();
            assert!(err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY), "{err}");
        }
        refuse_local_filesystem_writes(&catalogs, &explained).unwrap();
        refuse_local_filesystem_writes(&catalogs, &explained_analyze).unwrap();
        let inside = copy_plan(&ctx, &warehouse.path().join("x")).await;
        let inside_analyzed = datafusion::logical_expr::LogicalPlanBuilder::from(inside)
            .explain(false, true)
            .unwrap()
            .build()
            .unwrap();
        refuse_local_filesystem_writes(&catalogs, &inside_analyzed).unwrap();
    }

    #[test]
    fn the_conf_read_is_scoped_to_the_repark_sql_extension_and_closed_when_absent() {
        assert!(!allow_local_filesystem_ddl(&SessionContext::new()));
        assert!(!allow_local_filesystem_ddl(&native_ctx(false)));
        assert!(allow_local_filesystem_ddl(&native_ctx(true)));
        assert!(allow_local_filesystem_ddl(&unprefixed_ctx(true)));
        assert!(!allow_local_filesystem_ddl(&unprefixed_ctx(false)));
    }

    #[test]
    fn an_empty_location_is_not_contained() {
        let warehouse = TempDir::new().unwrap();
        assert_eq!(resolve_physical_destination(Path::new("")), None);
        assert!(!path_under_any_warehouse(
            &registry(warehouse.path()),
            Path::new("")
        ));
    }
}
