use super::super::*;
use super::common::*;
use repark_iceberg::catalog::{
    CatalogCaches, DEFAULT_MANIFEST_CACHE_BYTES, IcebergCacheSettings, MANIFEST_CACHE_BYTES_KEY,
};

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

#[tokio::test]
async fn one_statement_over_many_tables_retains_one_entry_each_until_the_next_door() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache: true,
        metadata_cache_entries: 1,
        manifest_cache_bytes: DEFAULT_MANIFEST_CACHE_BYTES,
    });
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    for index in 0..8 {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.t{index} AS SELECT * FROM src"),
        )
        .await;
        caches.trim();
    }

    let union = (0..8)
        .map(|index| format!("SELECT id FROM ice.sales.t{index}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    assert_eq!(
        rows(&ctx, &catalogs, &format!("SELECT id FROM ({union})")).await,
        24
    );

    assert_eq!(
        caches.metadata_len(),
        8,
        "the bound is a statement-door clear: one statement over N tables retains N"
    );
    caches.trim();
    assert!(
        caches.metadata_len() <= 1,
        "the next door brings it back under the bound"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t3").await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn a_hadoop_pointer_adopted_by_register_table_stays_correct_across_commits() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::default();
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.seed (id INT, name STRING)",
    )
    .await;

    let catalog = catalogs.get("ice").expect("ice").clone();
    let seed_ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "seed".to_string());
    let seed = catalog.load_table(&seed_ident).await.expect("load seed");
    let seed_location = seed.metadata_location().expect("seed location").to_string();
    let seed_dir = seed.metadata().location().to_string();

    let adopted_dir = format!("{}/hadoop_adopted", wh.path().to_str().unwrap());
    std::fs::create_dir_all(format!("{adopted_dir}/metadata")).expect("adopted metadata dir");
    let document = std::fs::read_to_string(seed_location.trim_start_matches("file://"))
        .expect("read seed metadata")
        .replace(&seed_dir, &adopted_dir);
    let hadoop_pointer = format!("{adopted_dir}/metadata/v1.metadata.json");
    std::fs::write(&hadoop_pointer, document).expect("write hadoop pointer");

    run(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.adopted', \
             metadata_file => '{hadoop_pointer}')"
        ),
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.adopted VALUES (1, 'a')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.adopted VALUES (2, 'b')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.adopted VALUES (7, 'g')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.adopted VALUES (8, 'h')",
    )
    .await;

    let adopted_ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "adopted".to_string(),
    );
    let live = catalog
        .load_table(&adopted_ident)
        .await
        .expect("load adopted");
    let live_location = live.metadata_location().expect("adopted location");
    assert!(
        live_location.ends_with("/v5.metadata.json"),
        "a Hadoop pointer stays Hadoop and is deterministic, not uuid-drawn: {live_location}"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.adopted").await,
        vec![7, 8]
    );
}

async fn delete_manifest_files(catalogs: &CatalogRegistry, table: &str) -> usize {
    let catalog = catalogs.get("ice").expect("ice").clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let loaded = catalog.load_table(&ident).await.expect("load table");
    let location = loaded
        .metadata_location()
        .expect("metadata location")
        .trim_start_matches("file://");
    let dir = std::path::Path::new(location)
        .parent()
        .expect("metadata dir");
    let mut removed = 0;
    for entry in std::fs::read_dir(dir).expect("read metadata dir") {
        let path = entry.expect("dir entry").path();
        if path
            .extension()
            .is_some_and(|extension| extension == "avro")
        {
            std::fs::remove_file(&path).expect("remove manifest");
            removed += 1;
        }
    }
    removed
}

#[tokio::test]
async fn a_second_door_reads_manifests_from_the_cache_the_first_door_filled() {
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
        ids(&ctx_a, &cat_a, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
    let removed = delete_manifest_files(&cat_a, "t").await;
    assert!(
        removed > 0,
        "the pin means nothing without a manifest to delete"
    );
    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn with_zero_bytes_a_repeated_read_opens_manifests_again() {
    let wh = TempDir::new().unwrap();
    let config = HashMap::from([(MANIFEST_CACHE_BYTES_KEY.to_string(), "0".to_string())]);
    let caches =
        CatalogCaches::new(IcebergCacheSettings::from_config_map(&config).expect("parse zero"));
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
    let removed = delete_manifest_files(&catalogs, "t").await;
    assert!(
        removed > 0,
        "the pin means nothing without a manifest to delete"
    );
    let outcome = execute(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await;
    let failed = match outcome {
        Err(_) => true,
        Ok(frame) => frame.collect().await.is_err(),
    };
    assert!(
        failed,
        "with zero bytes the repeated read must open manifests again"
    );
}

#[tokio::test]
async fn a_configured_byte_value_reaches_the_shared_cache() {
    let wh = TempDir::new().unwrap();
    let config = HashMap::from([(MANIFEST_CACHE_BYTES_KEY.to_string(), "1048576".to_string())]);
    let settings = IcebergCacheSettings::from_config_map(&config).unwrap();
    assert_eq!(settings.manifest_cache_bytes, 1_048_576);
    let caches = CatalogCaches::new(settings);
    let ((ctx_a, cat_a), (ctx_b, cat_b)) = two_doors(&wh, &caches).await;
    run(
        &ctx_a,
        &cat_a,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    refresh(&ctx_b, &cat_b).await;
    assert_eq!(
        ids(&ctx_a, &cat_a, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
    let removed = delete_manifest_files(&cat_a, "t").await;
    assert!(
        removed > 0,
        "the pin means nothing without a manifest to delete"
    );
    assert_eq!(
        ids(&ctx_b, &cat_b, "SELECT id FROM ice.sales.t").await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn a_tiny_byte_budget_still_answers_across_many_tables() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        manifest_cache_bytes: 512,
        ..IcebergCacheSettings::default()
    });
    let ((ctx, catalogs), _) = two_doors(&wh, &caches).await;
    for index in 0..8 {
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.t{index} AS SELECT * FROM src"),
        )
        .await;
    }
    for index in 0..8 {
        assert_eq!(
            ids(
                &ctx,
                &catalogs,
                &format!("SELECT id FROM ice.sales.t{index}")
            )
            .await,
            vec![1, 2, 3]
        );
    }
}

#[tokio::test]
async fn the_retained_location_bound_holds_across_many_commits() {
    let wh = TempDir::new().unwrap();
    let caches = CatalogCaches::new(IcebergCacheSettings {
        metadata_cache: true,
        metadata_cache_entries: 4,
        manifest_cache_bytes: DEFAULT_MANIFEST_CACHE_BYTES,
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
