use super::super::*;
use super::common::*;
use repark_iceberg::catalog::{CatalogCaches, IcebergCacheSettings};

async fn shared_catalog(wh: &TempDir, caches: &CatalogCaches) -> (Arc<dyn Catalog>, String) {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog = repark_iceberg::catalog::memory_catalog_cached(&warehouse, caches)
        .await
        .unwrap();
    let ns_props = HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]);
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), ns_props)
        .await
        .unwrap();
    (catalog, warehouse)
}

async fn door_on(catalog: &Arc<dyn Catalog>, warehouse: &str) -> (SessionContext, CatalogRegistry) {
    let config = repark_functions::cardinality::with_repark_sql_config(
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new()),
        repark_functions::cardinality::ReparkSqlSettings::default(),
    );
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a"), (2, "b"), (3, "c")]);
    let mut catalogs = CatalogRegistry::from([("ice".to_string(), catalog.clone())]);
    catalogs.note_local_warehouse_root(warehouse.to_string());
    (ctx, catalogs)
}

async fn two_doors(
    wh: &TempDir,
    caches: &CatalogCaches,
) -> (
    (SessionContext, CatalogRegistry),
    (SessionContext, CatalogRegistry),
) {
    let (catalog, warehouse) = shared_catalog(wh, caches).await;
    let first = door_on(&catalog, &warehouse).await;
    let second = door_on(&catalog, &warehouse).await;
    (first, second)
}

async fn refresh(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    let catalog = catalogs.get("ice").expect("ice").clone();
    repark_iceberg::catalog::invalidate_catalog_namespaces(ctx, catalog, "ice", &["sales"])
        .await
        .unwrap();
}

async fn ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 id");
        for index in 0..column.len() {
            out.push(column.value(index));
        }
    }
    out.sort_unstable();
    out
}

/// pins: perf-ice-catalog-io-1/C-003
#[tokio::test]
async fn a_commit_in_one_session_is_visible_to_the_next_statement_of_another() {
    let wh = TempDir::new().unwrap();
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &CatalogCaches::default()).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );

    run(
        &ctx_a,
        &cat_a,
        "INSERT INTO ice.sales.t VALUES (4, 'd'), (5, 'e')",
    )
    .await;

    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4, 5]
    );
}

/// pins: perf-ice-catalog-io-1/C-003
#[tokio::test]
async fn a_schema_change_in_one_session_is_seen_by_the_next_statement_of_another() {
    let wh = TempDir::new().unwrap();
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &CatalogCaches::default()).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    let before = execute(&ctx_b, &cat_b, "SELECT * FROM ice.sales.t")
        .await
        .unwrap()
        .schema()
        .fields()
        .len();
    assert_eq!(before, 2);

    run(
        &ctx_a,
        &cat_a,
        "ALTER TABLE ice.sales.t ADD COLUMNS (grade INT)",
    )
    .await;

    let after = execute(&ctx_b, &cat_b, "SELECT * FROM ice.sales.t")
        .await
        .unwrap()
        .schema()
        .fields()
        .len();
    assert_eq!(
        after, 3,
        "BUG-005: each statement plans against the current schema"
    );
}

/// pins: perf-ice-catalog-io-1/C-003
#[tokio::test]
async fn a_merge_after_another_sessions_commit_reads_that_sessions_rows() {
    let wh = TempDir::new().unwrap();
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &CatalogCaches::default()).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    run(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await;

    run(
        &ctx_a,
        &cat_a,
        "INSERT INTO ice.sales.t VALUES (4, 'd'), (5, 'e')",
    )
    .await;
    register_source(&ctx_b, "upd", &[(4, "D"), (9, "I")]);
    run(
        &ctx_b,
        &cat_b,
        "MERGE INTO ice.sales.t t USING upd u ON t.id = u.id \
         WHEN MATCHED THEN UPDATE SET t.name = u.name \
         WHEN NOT MATCHED THEN INSERT *",
    )
    .await;

    assert_eq!(
        ids(&ctx_a, &cat_a, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4, 5, 9]
    );
    let updated = ids(
        &ctx_a,
        &cat_a,
        "SELECT id FROM ice.sales.t WHERE name = 'D'",
    )
    .await;
    assert_eq!(
        updated,
        vec![4],
        "the MERGE matched the row A had committed"
    );
}

/// pins: perf-ice-catalog-io-1/C-003
#[tokio::test]
async fn a_read_after_another_sessions_maintenance_still_answers() {
    let wh = TempDir::new().unwrap();
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &CatalogCaches::default()).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    run(
        &ctx_a,
        &cat_a,
        "INSERT INTO ice.sales.t VALUES (4, 'd'), (5, 'e')",
    )
    .await;
    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4, 5]
    );

    run(
        &ctx_a,
        &cat_a,
        "CALL ice.system.rewrite_manifests(table => 'sales.t')",
    )
    .await;
    let older_than = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
        + 60_000;
    run(
        &ctx_a,
        &cat_a,
        &format!(
            "CALL ice.system.expire_snapshots(table => 'sales.t', \
             older_than => {older_than}, retain_last => 1)"
        ),
    )
    .await;

    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4, 5]
    );
}

/// pins: perf-ice-catalog-io-1/C-003
#[tokio::test]
async fn a_dropped_and_recreated_table_is_never_served_from_the_old_location() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &caches).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );

    run(&ctx_a, &cat_a, "DROP TABLE ice.sales.t").await;
    register_source(&ctx_a, "again", &[(7, "g")]);
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM again",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;

    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![7]
    );
}

/// pins: perf-ice-catalog-io-1/C-002
#[tokio::test]
async fn a_table_is_never_served_a_sibling_tables_cached_metadata() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    register_source(&ctx, "other", &[(40, "x"), (50, "y"), (60, "z"), (70, "w")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u AS SELECT * FROM other",
    )
    .await;

    run(&ctx, &catalogs, "SELECT id FROM ice.sales.u").await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.u VALUES (80, 'v')").await;

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.u").await,
        vec![40, 50, 60, 70, 80]
    );
    assert!(
        caches.metadata_len() >= 2,
        "both tables must be cached for this pin to mean anything"
    );
}

/// pins: perf-ice-catalog-io-1/C-002
#[tokio::test]
async fn an_unchanged_pointer_costs_no_metadata_body_fetch() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await;

    let before = caches.metadata_stats().expect("cache on").body_fetches;
    let retained = caches.metadata_len();
    caches.trim();
    run(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await;
    caches.trim();
    run(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await;
    let after = caches.metadata_stats().expect("cache on").body_fetches;

    assert_eq!(
        after, before,
        "two repeated reads on an unmoved pointer must read no metadata document"
    );
    assert_eq!(
        caches.metadata_len(),
        retained,
        "a trim under the bound must keep every entry: it is a high-water clear, not a per-statement flush"
    );
}

/// pins: perf-ice-catalog-io-1/C-002
#[tokio::test]
async fn a_commit_keys_a_new_location_and_seeds_it_rather_than_re_reading() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await;

    let fetches_before = caches.metadata_stats().expect("cache on").body_fetches;
    let keys_before = caches.metadata_len();
    run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (4, 'd')").await;

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3, 4],
        "the read after the commit must serve the committed snapshot"
    );
    assert_eq!(
        caches.metadata_len(),
        keys_before,
        "a commit evicts the pointer it replaced and keys the new one: retention is flat"
    );
    assert_eq!(
        caches.metadata_stats().expect("cache on").body_fetches,
        fetches_before,
        "the commit seeds the document it just wrote, so the reader pays no GET"
    );
}

/// pins: perf-ice-catalog-io-1/C-002
#[tokio::test]
async fn the_disabled_knob_reads_the_metadata_document_on_every_load() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::disabled();
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    assert!(caches.metadata_stats().is_none());
    assert_eq!(caches.metadata_len(), 0);
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
}

/// pins: perf-ice-catalog-io-1/C-004
#[tokio::test]
async fn the_retained_location_bound_holds_across_many_commits() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache: true,
        metadata_cache_entries: 4,
    });
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    for index in 0..24 {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.t{index} AS SELECT * FROM src"),
        )
        .await;
        caches.trim();
        assert!(
            caches.metadata_len() <= 5,
            "retained locations {} exceeded the bound",
            caches.metadata_len()
        );
    }

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3],
        "a trimmed cache still answers: the catalog pointer, not the cache, is authoritative"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t23").await,
        vec![1, 2, 3]
    );
}
