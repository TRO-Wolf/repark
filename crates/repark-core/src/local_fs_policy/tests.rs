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
    assert_eq!(
        local_filesystem_path(inside.to_str().unwrap()),
        Some(warehouse.path().join("kept"))
    );
    assert!(under(&registry(warehouse.path()), &inside));
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
fn the_cells_of_ruling_114_land_inside_where_the_written_path_lands() {
    let (_base, warehouse) = nested_warehouse();
    std::fs::create_dir(warehouse.join("sib")).unwrap();
    symlink(warehouse.join("sib"), warehouse.join("s2")).unwrap();
    std::fs::create_dir_all(warehouse.join("real/x")).unwrap();
    symlink(warehouse.join("real"), warehouse.join("inner")).unwrap();
    let catalogs = registry(&warehouse);
    let wh = warehouse.to_str().unwrap();
    let canonical = warehouse.canonicalize().unwrap();
    for (location, landing) in [
        (format!("{wh}/deep/../dd_in"), canonical.join("dd_in")),
        (
            format!("{wh}/deep/../m1/dd_in2"),
            canonical.join("m1/dd_in2"),
        ),
        (format!("{wh}/s2/../sb_in"), canonical.join("sb_in")),
        (format!("file://{wh}/x/%2e%2e/pin2"), canonical.join("pin2")),
        (format!("{wh}/x/../pin_bare"), canonical.join("pin_bare")),
        (
            format!("file://{wh}/x/../pin_url"),
            canonical.join("pin_url"),
        ),
        (format!("{wh}/inner/x/../y"), canonical.join("real/y")),
    ] {
        let written = local_filesystem_path(&location).unwrap();
        assert_eq!(
            resolve_physical_destination(&written),
            Some(landing),
            "{location}"
        );
        assert!(
            location_under_any_warehouse(&catalogs, &location),
            "{location}"
        );
    }
}

#[test]
fn the_written_reading_never_keeps_a_dot_dot() {
    for location in [
        "/srv/wh/a/../b",
        "/srv/wh/a/../../../../b",
        "file:///srv/wh/a/../b",
        "file:///srv/wh/a/%2e%2e/b",
        "file:///srv/wh/a/.%2E/b",
        "file:/srv/wh/a/..\\b",
        "file:///srv/wh/.\t./b",
        "file:srv/../../b",
        "file://localhost/srv/wh/a/../../b",
        "relative/../../b",
    ] {
        let written = local_filesystem_path(location).unwrap();
        assert!(
            !written
                .components()
                .any(|component| component == Component::ParentDir),
            "{location}: {}",
            written.display()
        );
    }
}

#[test]
fn rv01_a_glob_character_in_the_warehouse_path_writes_outside_on_todays_head_and_now_refuses() {
    for name in ["wh[1]", "wh?", "wh*"] {
        let base = TempDir::new().unwrap();
        let root = base.path().join(name);
        std::fs::create_dir(&root).unwrap();
        let catalogs = registry(&root);
        let b = base.path().to_str().unwrap();
        for outside in [
            format!("{b}/out/gr"),
            format!("{b}/sib/gr2"),
            format!("file://{b}/out/gr3"),
            format!("{b}/{name}x/gr4"),
        ] {
            assert!(
                !location_under_any_warehouse(&catalogs, &outside),
                "{name}: {outside}"
            );
        }
        assert!(
            !under(&catalogs, &root.join("gin")),
            "{name}: the bare path is cut at the glob"
        );
        assert_eq!(
            local_filesystem_path(root.join("gin").to_str().unwrap()),
            Some(base.path().to_path_buf())
        );
        let encoded = format!("file://{b}/{}/gin2", name.replace('?', "%3F"));
        assert_eq!(local_filesystem_path(&encoded), Some(root.join("gin2")));
        assert!(
            location_under_any_warehouse(&catalogs, &encoded),
            "{name}: {encoded}"
        );
    }
}

#[test]
fn rv02_a_dot_dot_over_a_symlink_above_the_warehouse_lands_inside() {
    let base = TempDir::new().unwrap();
    std::fs::create_dir_all(base.path().join("o1/o2")).unwrap();
    symlink(base.path().join("o1/o2"), base.path().join("lnk")).unwrap();
    let warehouse = base.path().join("wh");
    std::fs::create_dir(&warehouse).unwrap();
    let catalogs = registry(&warehouse);
    let b = base.path().to_str().unwrap();
    assert!(location_under_any_warehouse(
        &catalogs,
        &format!("{b}/lnk/../wh/anc_in")
    ));
    assert!(location_under_any_warehouse(
        &catalogs,
        &format!("file://{b}/lnk/../wh/anc_in2")
    ));
    assert!(!location_under_any_warehouse(
        &catalogs,
        &format!("{b}/lnk/../out/anc_out")
    ));
}

#[test]
fn rv03_private_use_characters_are_ordinary_characters() {
    let warehouse = TempDir::new().unwrap();
    let catalogs = registry(warehouse.path());
    let wh = warehouse.path().to_str().unwrap();
    for (location, landing) in [
        (
            format!("file://{wh}/%EE%80%80%EE%80%80/pua"),
            "\u{E000}\u{E000}/pua",
        ),
        (
            format!("file://{wh}/\u{E000}\u{E001}\u{E002}/pua3"),
            "\u{E000}\u{E001}\u{E002}/pua3",
        ),
        (
            format!("{wh}/\u{E000}\u{E000}/pua4"),
            "\u{E000}\u{E000}/pua4",
        ),
    ] {
        assert_eq!(
            local_filesystem_path(&location),
            Some(warehouse.path().join(landing))
        );
        assert!(
            location_under_any_warehouse(&catalogs, &location),
            "{location}"
        );
    }
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
    LocalFsGuardedDialect::wrapping(Arc::new(DataFusionDialect))
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

#[derive(Debug)]
struct MicrosecondTimestamps;

impl TypePlanner for MicrosecondTimestamps {
    fn plan_type(
        &self,
        sql_type: &datafusion::sql::sqlparser::ast::DataType,
    ) -> Result<Option<datafusion::arrow::datatypes::DataType>> {
        Ok(matches!(
            sql_type,
            datafusion::sql::sqlparser::ast::DataType::Timestamp(
                None,
                datafusion::sql::sqlparser::ast::TimezoneInfo::None
            )
        )
        .then_some(datafusion::arrow::datatypes::DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        )))
    }
}

#[derive(Debug)]
struct MarkerRule;

impl AnalyzerRule for MarkerRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "local_fs_guard_inner_marker"
    }
}

#[derive(Default)]
struct Recording {
    calls: std::sync::Mutex<Vec<String>>,
}

impl Recording {
    fn note(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait(?Send)]
impl SqlDialect for Recording {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> Result<DataFrame> {
        let frame = DataFusionDialect.execute(cx, query).await?;
        self.note(format!(
            "execute {query} -> {}",
            frame.schema().field(0).data_type()
        ));
        Ok(frame)
    }

    async fn execute_with_write_options(
        &self,
        cx: EngineContext<'_>,
        query: &str,
        options: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        let frame = DataFusionDialect.execute(cx, query).await?;
        self.note(format!(
            "execute_with_write_options {query} ({}) -> {}",
            options.len(),
            frame.schema().field(0).data_type()
        ));
        Ok(frame)
    }

    fn on_session_built(&self, _ctx: &SessionContext) {
        self.note("on_session_built".to_string());
    }

    fn type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        self.note("type_planner".to_string());
        Some(Arc::new(MicrosecondTimestamps))
    }

    fn configure_analyzer_rules(
        &self,
        mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        self.note("configure_analyzer_rules".to_string());
        rules.push(Arc::new(MarkerRule));
        Ok(rules)
    }
}

fn session_over(recording: &Arc<Recording>, guarded: bool) -> crate::ReparkSession {
    let builder = crate::ReparkSession::builder()
        .with_sql_dialect(Arc::clone(recording) as Arc<dyn SqlDialect>);
    let builder = if guarded {
        builder.with_local_fs_guard()
    } else {
        builder
    };
    builder.build().unwrap()
}

const WALL_CAST: &str = "SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) AS v";

fn outside_copies(out: &str) -> [String; 2] {
    [
        format!("COPY (SELECT 1 AS a) TO '{out}/direct' STORED AS PARQUET"),
        format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{out}/wrapped' STORED AS PARQUET"),
    ]
}

#[tokio::test]
async fn the_guard_forwards_every_method_to_the_dialect_it_wraps() {
    let recording = Arc::new(Recording::default());
    let guard = LocalFsGuardedDialect::wrapping(Arc::clone(&recording) as Arc<dyn SqlDialect>);
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let read_only = std::collections::HashSet::new();
    let options = HashMap::from([("k".to_string(), "v".to_string())]);
    guard.on_session_built(&ctx);
    assert!(guard.type_planner().is_some());
    assert_eq!(guard.configure_analyzer_rules(Vec::new()).unwrap().len(), 1);
    guard
        .execute(
            EngineContext::new(&ctx, &catalogs, &read_only),
            "SELECT 1 AS a",
        )
        .await
        .unwrap();
    guard
        .execute_with_write_options(
            EngineContext::new(&ctx, &catalogs, &read_only),
            "SELECT 2 AS a",
            &options,
        )
        .await
        .unwrap();
    let forwarded = [
        "on_session_built",
        "type_planner",
        "configure_analyzer_rules",
        "execute SELECT 1 AS a -> Int64",
        "execute_with_write_options SELECT 2 AS a (1) -> Int64",
    ];
    assert_eq!(recording.calls(), forwarded);
    let outside = TempDir::new().unwrap();
    for sql in outside_copies(outside.path().to_str().unwrap()) {
        let err = guard
            .execute(EngineContext::new(&ctx, &catalogs, &read_only), &sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(NATIVE_ENTRY_REFUSAL), "{sql}: {err}");
        let err = guard
            .execute_with_write_options(
                EngineContext::new(&ctx, &catalogs, &read_only),
                &sql,
                &options,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(NATIVE_ENTRY_REFUSAL), "{sql}: {err}");
    }
    assert_eq!(
        recording.calls(),
        forwarded,
        "a refused write never reaches the inner dialect"
    );
    assert_eq!(file_count(outside.path()), 0, "nothing may land outside");
}

#[tokio::test]
async fn the_builder_wraps_the_dialect_it_would_otherwise_use() {
    let recording = Arc::new(Recording::default());
    let session = session_over(&recording, true);
    assert_eq!(
        recording.calls(),
        [
            "configure_analyzer_rules",
            "type_planner",
            "on_session_built"
        ],
        "the build reaches the inner dialect's hooks through the guard"
    );
    session
        .sql("SELECT 1 AS a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        recording.calls().last().unwrap(),
        "execute_with_write_options SELECT 1 AS a (0) -> Int64"
    );
    let outside = TempDir::new().unwrap();
    for sql in outside_copies(outside.path().to_str().unwrap()) {
        let err = session.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains(NATIVE_ENTRY_REFUSAL), "{sql}: {err}");
    }
    assert_eq!(
        recording.calls().len(),
        4,
        "a refused write never reaches the inner dialect"
    );
    assert_eq!(file_count(outside.path()), 0, "nothing may land outside");
    let unguarded = session_over(&Arc::new(Recording::default()), false);
    let [direct, _] = outside_copies(outside.path().to_str().unwrap());
    unguarded
        .sql(&direct)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(
        file_count(outside.path()) > 0,
        "without the guard the same dialect writes"
    );
}

#[tokio::test]
async fn the_judgment_plan_and_the_inner_plan_see_the_same_session_state() {
    let recording = Arc::new(Recording::default());
    let session = session_over(&recording, true);
    let catalogs = CatalogRegistry::new();
    let read_only = std::collections::HashSet::new();
    let judged = judged_plan(
        &EngineContext::new(session.context(), &catalogs, &read_only),
        WALL_CAST,
    )
    .await
    .unwrap();
    let microseconds = datafusion::arrow::datatypes::DataType::Timestamp(
        datafusion::arrow::datatypes::TimeUnit::Microsecond,
        None,
    );
    assert_eq!(judged.schema().field(0).data_type(), &microseconds);
    session
        .sql(WALL_CAST)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        recording.calls().last().unwrap(),
        &format!("execute_with_write_options {WALL_CAST} (0) -> {microseconds}")
    );
    assert!(
        session
            .context()
            .state()
            .analyzer()
            .rules
            .iter()
            .any(|rule| rule.name() == "local_fs_guard_inner_marker"),
        "the inner dialect's analyzer rule is in the state both plans use"
    );
}
