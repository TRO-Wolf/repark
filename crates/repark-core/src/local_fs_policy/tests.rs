use super::*;
use std::os::unix::fs::symlink;
use tempfile::TempDir;

fn registry(root: &Path) -> CatalogRegistry {
    let mut catalogs = CatalogRegistry::new();
    catalogs.note_local_warehouse_root(root.to_string_lossy());
    catalogs
}

fn under(catalogs: &CatalogRegistry, path: &Path) -> bool {
    location_under_any_warehouse(catalogs, path.to_str().unwrap())
}

fn cwd_join(relative: &str) -> PathBuf {
    std::env::current_dir().unwrap().join(relative)
}

#[test]
fn a_multibyte_character_across_every_prefix_boundary_classifies_without_panic() {
    assert_eq!(local_filesystem_path("aaaaé"), Some(cwd_join("aaaaé")));
    assert_eq!(local_filesystem_path("aaaaaaé"), Some(cwd_join("aaaaaaé")));
    assert_eq!(local_filesystem_path("file://aaaaaaaaé"), None);
    assert_eq!(local_filesystem_path("aé"), Some(cwd_join("aé")));
    assert_eq!(local_filesystem_path("日本語"), Some(cwd_join("日本語")));
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
        Some(cwd_join("gé:/bucket/key"))
    );
    assert_eq!(local_filesystem_path("custom+x://bucket/key"), None);
    assert_eq!(local_filesystem_path("   "), Some(cwd_join("   ")));
    assert_eq!(local_filesystem_path(""), None);
}

#[test]
fn the_location_is_read_the_way_the_write_reads_it() {
    for (location, written) in [
        ("file:///srv/wh/%2e%2e/out/x", "/srv/out/x"),
        ("file:///srv/wh/%2E%2E/out/x", "/srv/out/x"),
        ("file:///srv/wh/.%2e/out/x", "/srv/out/x"),
        ("file:/srv/wh/%2e%2e/out/x", "/srv/out/x"),
        ("file://localhost/srv/wh/%2e%2e/out/x", "/srv/out/x"),
        ("file:///srv/wh/deep/../../out/x", "/srv/out/x"),
        ("/srv/wh/deep/../../out/x", "/srv/out/x"),
        ("file:srv/out/x", "/srv/out/x"),
        ("file:./srv/out/x", "/srv/out/x"),
        ("file:///srv/wh/a%20b/x", "/srv/wh/a b/x"),
        ("/srv/wh/%2e%2e/x", "/srv/wh/%2e%2e/x"),
        ("file:///srv/wh/x?query", "/srv/wh/x"),
    ] {
        assert_eq!(
            local_filesystem_path(location),
            Some(PathBuf::from(written)),
            "{location}"
        );
    }
    assert_eq!(
        local_filesystem_path(" /srv/wh/x"),
        Some(cwd_join(" /srv/wh/x"))
    );
    assert_eq!(local_filesystem_path("file://127.0.0.1/srv/wh/x"), None);
    assert_eq!(local_filesystem_path("S3://bucket/key"), None);
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
    assert!(!under(&catalogs, &warehouse.path().join("link/new/export")));
    assert!(under(&catalogs, &warehouse.path().join("m1/m2/export")));
}

#[test]
fn a_dot_dot_after_the_nearest_existing_ancestor_is_indeterminate() {
    let warehouse = TempDir::new().unwrap();
    let path = warehouse.path().join("missing/../../escaped");
    assert_eq!(resolve_physical_destination(&path), None);
    assert!(!under(&registry(warehouse.path()), &path));
    let inside = warehouse.path().join("missing/../kept");
    assert!(!under(&registry(warehouse.path()), &inside));
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
    assert!(under(&registry(warehouse.path()), &path));
}

#[test]
fn a_missing_warehouse_root_still_contains_its_own_children() {
    let base = TempDir::new().unwrap();
    let root = base.path().join("not_created_yet");
    let catalogs = registry(&root);
    assert!(under(&catalogs, &root.join("t/data")));
    assert!(!under(&catalogs, &base.path().join("sibling")));
}

fn nested_warehouse() -> (TempDir, PathBuf) {
    let base = TempDir::new().unwrap();
    let warehouse = base.path().join("wh");
    std::fs::create_dir_all(warehouse.join("a/b")).unwrap();
    symlink(warehouse.join("a/b"), warehouse.join("deep")).unwrap();
    (base, warehouse)
}

#[test]
fn v01_a_symlink_then_dot_dot_writes_outside_on_todays_head_and_now_refuses() {
    let (base, warehouse) = nested_warehouse();
    let catalogs = registry(&warehouse);
    let wh = warehouse.to_str().unwrap();
    for location in [
        format!("{wh}/deep/../../out/x"),
        format!("file://{wh}/deep/../../out/x"),
    ] {
        assert_eq!(
            local_filesystem_path(&location),
            Some(base.path().join("out/x"))
        );
        assert!(
            !location_under_any_warehouse(&catalogs, &location),
            "{location}"
        );
    }
}

#[test]
fn a_dot_dot_that_cancels_a_symlink_under_the_warehouse_refuses_even_when_it_lands_inside() {
    let (_base, warehouse) = nested_warehouse();
    std::fs::create_dir(warehouse.join("sib")).unwrap();
    symlink(warehouse.join("sib"), warehouse.join("s2")).unwrap();
    std::fs::create_dir_all(warehouse.join("real/x")).unwrap();
    symlink(warehouse.join("real"), warehouse.join("inner")).unwrap();
    let catalogs = registry(&warehouse);
    assert!(!under(&catalogs, &warehouse.join("deep/../dd_in")));
    assert!(!under(&catalogs, &warehouse.join("s2/../sb_in")));
    assert!(under(&catalogs, &warehouse.join("inner/x/../y")));
}

#[test]
fn v02_a_percent_encoded_dot_segment_writes_outside_on_todays_head_and_now_refuses() {
    let base = TempDir::new().unwrap();
    let warehouse = base.path().join("wh");
    std::fs::create_dir(&warehouse).unwrap();
    let catalogs = registry(&warehouse);
    let wh = warehouse.to_str().unwrap();
    for location in [
        format!("file://{wh}/%2e%2e/out/x"),
        format!("file://{wh}/%2E%2E/out/x"),
        format!("file://{wh}/.%2e/out/x"),
        format!("file:{wh}/%2e%2e/out/x"),
        format!("file://localhost{wh}/%2e%2e/out/x"),
    ] {
        assert!(
            !location_under_any_warehouse(&catalogs, &location),
            "{location}"
        );
    }
    assert!(location_under_any_warehouse(
        &catalogs,
        &format!("file://{wh}/a%20b/x")
    ));
    assert!(location_under_any_warehouse(
        &catalogs,
        &format!("{wh}/%2e%2e/x")
    ));
}

#[test]
fn v03_a_relative_file_url_is_read_from_the_root_not_the_cwd() {
    let base = TempDir::new().unwrap();
    let relative = base.path().to_str().unwrap().trim_start_matches('/');
    assert_eq!(
        local_filesystem_path(&format!("file:{relative}/out/fc")),
        Some(base.path().join("out/fc"))
    );
    assert_eq!(
        local_filesystem_path(&format!("file:./{relative}/out/fc4")),
        Some(base.path().join("out/fc4"))
    );
}

#[test]
fn a_warehouse_whose_root_is_a_symlink_still_accepts_writes_inside_it() {
    let base = TempDir::new().unwrap();
    let real = base.path().join("real");
    std::fs::create_dir_all(real.join("r2")).unwrap();
    let linked_root = base.path().join("whl");
    symlink(&real, &linked_root).unwrap();
    symlink(real.join("r2"), real.join("inner")).unwrap();
    let catalogs = registry(&linked_root);
    for path in [
        linked_root.join("direct"),
        linked_root.join("inner/x"),
        linked_root.join("m1/m2/m3/x"),
        real.join("by_real_path"),
    ] {
        assert!(under(&catalogs, &path), "{}", path.display());
    }
    assert!(location_under_any_warehouse(
        &catalogs,
        &format!("file://{}/direct", linked_root.display())
    ));
    assert!(!under(&catalogs, &base.path().join("sibling")));
}

#[test]
fn an_ancestor_whose_metadata_read_fails_other_than_not_found_refuses() {
    let warehouse = TempDir::new().unwrap();
    std::fs::write(warehouse.path().join("plain"), b"x").unwrap();
    let path = warehouse.path().join("plain/child/x");
    assert_eq!(resolve_physical_destination(&path), None);
    assert!(!under(&registry(warehouse.path()), &path));
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
        assert!(err.contains(NATIVE_ENTRY_REFUSAL), "{sql}: {err}");
        assert!(
            !err.contains(ALLOW_LOCAL_FILESYSTEM_DDL_KEY),
            "{sql}: {err}"
        );
    }
    assert_eq!(file_count(outside.path()), 0, "nothing may land outside");
    assert!(!Path::new("aaaaaaé").exists());
}

#[tokio::test]
async fn the_native_door_keeps_explain_and_the_warehouse_control_and_has_no_opt_in() {
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
    let err = native_run(
        &open,
        &CatalogRegistry::new(),
        &format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{out}/opened' STORED AS PARQUET"),
    )
    .await
    .expect_err("the native door has no opt-in")
    .to_string();
    assert!(err.contains(NATIVE_ENTRY_REFUSAL), "{err}");
    assert_eq!(file_count(outside.path()), 0, "nothing may land outside");
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
    let explained_analyze = datafusion::logical_expr::LogicalPlanBuilder::from(analyzed.clone())
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
    assert!(!location_under_any_warehouse(
        &registry(warehouse.path()),
        ""
    ));
}
