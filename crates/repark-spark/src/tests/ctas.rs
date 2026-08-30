/// The #1 op: `CREATE TABLE cat.ns.t AS SELECT * FROM <temp view>` into Iceberg, read back.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn ctas_from_temp_view_into_iceberg_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );
    // A projection + filter through the Iceberg scan still works.
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT name FROM ice.sales.orders WHERE id > 1"
        )
        .await,
        2
    );
}

/// A location-less namespace on a non-memory catalog fails loudly.
#[tokio::test]
async fn ctas_location_less_namespace_fails_loud_for_non_memory_catalog() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    // A namespace created WITHOUT a `location` property — the BUG-002 trigger.
    catalog
        .create_namespace(
            &NamespaceIdent::new("nolocation".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();

    let ctx = SessionContext::new();
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "glue_like", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a")]);

    // Register under the strict policy a real Glue / S3 Tables catalog would carry.
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_like".to_string(),
        catalog,
        LocationPolicy::RequireExplicitLocation,
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.nolocation.t AS SELECT * FROM src",
    )
    .await
    .expect_err("a location-less namespace on a non-memory catalog must fail loud");
    let message = error.to_string();
    assert!(
        message.contains("nolocation"),
        "error must name the namespace, got: {message}"
    );
    assert!(
        message.contains("location"),
        "error must tell the user to set the `location` property, got: {message}"
    );
    // The error names both supported fixes.
    assert!(
        message.contains("LOCATION"),
        "error must name the SQL `CREATE NAMESPACE … LOCATION` path (WG-5), got: {message}"
    );
    assert!(
        message.contains("WITH DBPROPERTIES"),
        "error must name the SQL `WITH DBPROPERTIES ('location' = …)` path (WG-5), got: {message}"
    );
    assert!(
        message.contains("create_namespace"),
        "error must also name the programmatic create_namespace path, got: {message}"
    );
    assert!(
        !message.contains("cannot set"),
        "error must NOT claim SQL CREATE NAMESPACE cannot set properties (WG-5), got: {message}"
    );
    // A fail-loud location error must not leave an orphan table behind.
    let ident = TableIdent::new(
        NamespaceIdent::new("nolocation".to_string()),
        "t".to_string(),
    );
    assert!(
        !catalogs["glue_like"].table_exists(&ident).await.unwrap(),
        "a fail-loud location error must not create a table"
    );
}

/// A strict-catalog CTAS with malformed `s3:/bucket/wh` location fails loudly and leaves no table.
#[tokio::test]
async fn ctas_malformed_single_slash_namespace_location_fails_loud() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    // A namespace whose `location` is the malformed single-slash `s3:/…` typo.
    catalog
        .create_namespace(
            &NamespaceIdent::new("mistyped".to_string()),
            HashMap::from([("location".to_string(), "s3:/bucket/wh".to_string())]),
        )
        .await
        .unwrap();

    let ctx = SessionContext::new();
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "glue_like", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a")]);

    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_like".to_string(),
        catalog,
        LocationPolicy::RequireExplicitLocation,
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.mistyped.t AS SELECT * FROM src",
    )
    .await
    .expect_err("a single-slash `s3:/` namespace location must fail loud, not publish silently");
    let message = error.to_string();
    assert!(
        message.contains("s3:/bucket/wh"),
        "the CTAS error must name the malformed location, got: {message}"
    );
    // A fail-loud location error must not leave an orphan table behind.
    let ident = TableIdent::new(NamespaceIdent::new("mistyped".to_string()), "t".to_string());
    assert!(
        !catalogs["glue_like"].table_exists(&ident).await.unwrap(),
        "a fail-loud location error must not create a table"
    );
}

/// CTAS validates the target location before source materialization, so the location error wins.
#[tokio::test]
async fn ctas_location_check_precedes_source_execution() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    // A namespace with NO `location` property — the fail-loud trigger.
    catalog
        .create_namespace(
            &NamespaceIdent::new("nolocation".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();

    let ctx = SessionContext::new();
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "glue_like", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a"), (2, "b")]);
    // A source that ERRORS at runtime on row value 2 — if the SELECT executed, this would win.
    register_failing_scalar(&ctx);

    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_like".to_string(),
        catalog,
        LocationPolicy::RequireExplicitLocation,
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.nolocation.t AS SELECT repark_fail_on_two(id) AS id, name FROM src",
    )
    .await
    .expect_err("a location-less RequireExplicitLocation namespace must fail loud");
    let message = error.to_string();
    // The LOCATION error wins — resolved before the SELECT, which therefore never ran.
    assert!(
        message.contains("location"),
        "the location error must win (resolved before the SELECT), got: {message}"
    );
    assert!(
        !message.contains("injected CTAS source failure"),
        "the source query must NOT execute (the location check precedes it), got: {message}"
    );
}

/// WU-3: OR REPLACE whose SELECT fails mid-stream must leave the original table intact.
#[tokio::test]
async fn ctas_or_replace_source_failure_leaves_original_intact() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );
    register_failing_scalar(&ctx);
    let error = execute(
            &ctx,
            &catalogs,
            "CREATE OR REPLACE TABLE ice.sales.orders AS SELECT repark_fail_on_two(id) AS id, name FROM src",
        )
        .await
        .expect_err("OR REPLACE with failing source must error");
    let message = error.to_string();
    assert!(
        message.contains("injected CTAS source failure"),
        "must surface the source error, got: {message}"
    );
    // Original still readable with unchanged row count and contents.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.orders WHERE name = 'a'"
        )
        .await,
        1
    );
}

/// WU-3: plain CTAS whose SELECT fails must not leave an empty orphan registered.
#[tokio::test]
async fn ctas_plain_source_failure_leaves_no_orphan() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_failing_scalar(&ctx);
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orphan AS SELECT repark_fail_on_two(id) AS id FROM src",
    )
    .await
    .expect_err("plain CTAS with failing source must error");
    assert!(
        error.to_string().contains("injected CTAS source failure"),
        "must surface the source error, got: {error}"
    );
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orphan".to_string(),
    );
    assert!(
        !catalogs["ice"].table_exists(&ident).await.unwrap(),
        "failing plain CTAS must not leave an orphan table"
    );
}

/// MUTATION: delete the `build_ctas` column-list guard → this pin REDs (a table named `cl` is created and no error is raised).
/// An explicit CTAS column list is rejected before a table is created.
#[tokio::test]
async fn ctas_explicit_column_list_rejected_spark_parity() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cl (a INT, b STRING) USING iceberg AS \
             SELECT id AS x, name AS y FROM src",
    )
    .await
    .expect_err("an explicit column list in CTAS must be rejected (Spark parity)");
    let message = error.to_string();
    assert!(
        message.contains("Schema may not be specified in a Create Table As Select (CTAS)"),
        "the Spark CTAS message class, got: {message}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "cl".to_string(),
            ))
            .await
            .unwrap(),
        "no table may be created"
    );
}

/// An explicit RTAS column list returns Spark's schema error before replacement.
#[tokio::test]
async fn rtas_explicit_column_list_rejected_and_preserves_existing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.orders (a INT, b STRING) USING iceberg AS \
             SELECT id AS a, name AS b FROM src WHERE id = 1",
    )
    .await
    .expect_err("an explicit column list in RTAS must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("Schema may not be specified in a Replace Table As Select (RTAS)"),
        "the Spark RTAS message class, got: {message}"
    );
    // The existing table is untouched — the reject fires before the staged replace.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3,
        "a rejected RTAS must leave the original table's rows intact"
    );
}

/// An explicit CTAS column list reports the schema error before the typed partition error.
#[tokio::test]
async fn ctas_column_list_with_partitioned_by_reports_schema_error_first() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cp (a INT) USING iceberg PARTITIONED BY (name STRING) AS \
             SELECT id AS a, name FROM src",
    )
    .await
    .expect_err("column list + typed partition must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("Schema may not be specified in a Create Table As Select (CTAS)"),
        "the SCHEMA error must win over the partition-type error (Spark order), got: {message}"
    );
    assert!(
        !message.contains("Partition column types may not be specified"),
        "the partition-type message must NOT be the one surfaced, got: {message}"
    );
}

/// OR REPLACE success still replaces rows (positive control for the staged transaction path).
#[tokio::test]
async fn ctas_or_replace_success_replaces_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.orders AS SELECT id, name FROM src WHERE id = 1",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        1
    );
}

/// Failure *between* write and publish (stale replace CAS) leaves the original table current.
#[tokio::test]
async fn ctas_or_replace_failed_publish_leaves_original_current() {
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
        Struct, Type,
    };
    use iceberg::transaction::StagedTableTransaction;
    use iceberg::{TableCreation, TableIdent};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );

    let catalog = catalogs["ice"].clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "orders".into());
    let original = catalog.load_table(&ident).await.unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();
    // Stage a replace (writes pending) then let a concurrent CTAS OR REPLACE win first.
    let staged = StagedTableTransaction::begin_replace(
        &original,
        TableCreation::builder()
            .name("orders".into())
            .schema(schema)
            .build(),
    )
    .await
    .unwrap();
    let staged = staged.add_data_files(vec![
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("{}/stale.parquet", wh.path().to_string_lossy()))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(10)
            .record_count(1)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap(),
    ]);

    execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.orders AS SELECT id, name FROM src WHERE id = 1",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        1
    );

    let err = staged
        .commit(catalog.as_ref())
        .await
        .expect_err("stale CAS");
    assert!(
        err.to_string().contains("concurrent")
            || err.kind() == iceberg::ErrorKind::CatalogCommitConflicts,
        "expected concurrent-modification conflict, got: {err}"
    );
    // Winner remains current.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        1
    );
}

/// `USING iceberg` parses and `TBLPROPERTIES (...)` reaches table creation.
#[tokio::test]
async fn ctas_parses_using_and_threads_tblproperties() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.props USING iceberg \
             TBLPROPERTIES('write.format.default'='parquet', 'team'='example-team') \
             AS SELECT * FROM src",
    )
    .await
    .unwrap();
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "props".to_string(),
    );
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let props = table.metadata().properties();
    assert_eq!(props.get("team").map(String::as_str), Some("example-team"));
    assert_eq!(
        props.get("write.format.default").map(String::as_str),
        Some("parquet")
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.props").await,
        3
    );
}

/// pins: v3-2-create-v3-opt-in/C-003, C-004, C-007
/// The reserved `format-version` TBLPROPERTY: '2' is consumed.
#[tokio::test]
async fn ctas_format_version_two_consumed_others_rejected() {
    use iceberg::spec::FormatVersion;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2 USING iceberg \
             TBLPROPERTIES('format-version' = 2, 'team'='example-team') AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v2".to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    assert!(!table.metadata().properties().contains_key("format-version"));
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("team")
            .map(String::as_str),
        Some("example-team")
    );

    let err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v1 TBLPROPERTIES('format-version' = 1) \
             AS SELECT * FROM src",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("format-version"),
        "expected the format-version reject, got: {err}"
    );

    // pins: v3-2-create-v3-opt-in/C-004, C-007
    let err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3 TBLPROPERTIES('format-version' = 3) \
             AS SELECT * FROM src",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("repark.sql.allowCreateFormatVersion3")
            && err.to_string().contains("format-version"),
        "opt-in refuse must name conf and property: {err}"
    );
}

/// pins: v3-2-create-v3-opt-in/C-002, C-005
#[tokio::test]
async fn ctas_format_version_three_needs_opt_in() {
    use iceberg::spec::FormatVersion;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3 USING iceberg \
             TBLPROPERTIES('format-version' = 3) AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v3".to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    assert!(!table.metadata().properties().contains_key("format-version"));
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.v3").await, 3);

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.still_v2 USING iceberg AS SELECT * FROM src",
    )
    .await;
    let still = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "still_v2".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(still.metadata().format_version(), FormatVersion::V2);

    run(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.still_v2 USING iceberg \
             TBLPROPERTIES('format-version' = 3) AS SELECT * FROM src",
    )
    .await;
    let replaced = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "still_v2".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(replaced.metadata().format_version(), FormatVersion::V3);
}

/// `IF NOT EXISTS` is idempotent; a plain re-create errors.
#[tokio::test]
async fn ctas_if_not_exists_is_idempotent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let create = "CREATE TABLE ice.sales.t AS SELECT * FROM src";
    execute(&ctx, &catalogs, create).await.unwrap();
    // Plain re-create is rejected (table exists).
    assert!(execute(&ctx, &catalogs, create).await.is_err());
    // IF NOT EXISTS is a no-op.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE IF NOT EXISTS ice.sales.t AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
}

/// A non-CTAS statement passes straight through to DataFusion.
#[tokio::test]
async fn non_ctas_passes_through() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM src").await, 3);
}

/// CTAS derives its schema through the Spark passthrough.
#[tokio::test]
async fn ctas_integer_division_lands_as_double() {
    use datafusion::arrow::array::Float64Array;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.halves AS SELECT id, id/2 AS half FROM src",
    )
    .await
    .unwrap();

    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT half FROM ice.sales.halves WHERE id = 1",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let halves = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| {
            panic!(
                "CTAS division column must be Float64, got {:?}",
                batches[0].schema().field(0).data_type()
            )
        });
    assert!((halves.value(0) - 0.5).abs() < f64::EPSILON);
}

/// Read the written Iceberg column back as Option<f64>, sorted, NULLs first.
async fn ctas_f64_column_sorted(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    column: &str,
) -> Vec<Option<f64>> {
    use datafusion::arrow::array::{Array, Float64Array};
    let batches = execute(ctx, catalogs, &format!("SELECT {column} FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values: Vec<Option<f64>> = Vec::new();
    for batch in &batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "CTAS column `{column}` must be Float64 in storage, got {:?}",
                    batch.schema().field(0).data_type()
                )
            });
        for row in 0..array.len() {
            values.push(array.is_valid(row).then(|| array.value(row)));
        }
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    values
}

/// The Arrow `DataType` of `table.column` as stored in the written Iceberg table (round-trip).
async fn ctas_stored_type(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    column: &str,
) -> DataType {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT {column} FROM {table} LIMIT 1"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    batches[0].schema().field(0).data_type().clone()
}

/// A CTAS union of integer divisions must derive its write schema to a fixpoint.
#[tokio::test]
async fn ctas_union_of_integer_division_lands_as_double() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.union_div AS SELECT 5/2 AS q UNION ALL SELECT 7/2",
    )
    .await
    .expect("union-of-division CTAS must not fail at the parquet writer");
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.union_div", "q").await,
        DataType::Float64,
        "the UNION division column must land as double in storage"
    );
    assert_eq!(
        ctas_f64_column_sorted(&ctx, &catalogs, "ice.sales.union_div", "q").await,
        vec![Some(2.5), Some(3.5)]
    );
}

/// A non-union CTAS of bare integer division lands as Spark `double 3.5`.
#[tokio::test]
async fn ctas_bare_integer_division_lands_as_double() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bare_div AS SELECT 7/2 AS q",
    )
    .await
    .unwrap();
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.bare_div", "q").await,
        DataType::Float64
    );
    assert_eq!(
        ctas_f64_column_sorted(&ctx, &catalogs, "ice.sales.bare_div", "q").await,
        vec![Some(3.5)]
    );
}

/// A zero-divisor CTAS yields NULL with the PROMOTED result type, written and read back.
#[tokio::test]
async fn ctas_union_zero_divisor_is_null_double() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_ansi(&wh, false).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.zero_div AS SELECT 5/0 AS q UNION ALL SELECT 7/2",
    )
    .await
    .unwrap();
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.zero_div", "q").await,
        DataType::Float64,
        "a zero divisor keeps the promoted double result type"
    );
    assert_eq!(
        ctas_f64_column_sorted(&ctx, &catalogs, "ice.sales.zero_div", "q").await,
        vec![None, Some(3.5)]
    );
}

/// The fix must not OVER-promote.
#[tokio::test]
async fn ctas_union_double_stays_double_and_modulo_stays_integer() {
    use datafusion::arrow::array::{Array, Int64Array};
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    // Already-double union: unchanged, stored double.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dbl_div AS \
             SELECT CAST(5 AS DOUBLE)/CAST(2 AS DOUBLE) AS q UNION ALL SELECT CAST(9 AS DOUBLE)/2",
    )
    .await
    .unwrap();
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.dbl_div", "q").await,
        DataType::Float64
    );
    assert_eq!(
        ctas_f64_column_sorted(&ctx, &catalogs, "ice.sales.dbl_div", "q").await,
        vec![Some(2.5), Some(4.5)]
    );

    // Modulo union: Spark keeps the integer operand type — the fix must NOT promote it.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mod_u AS SELECT 5 % 2 AS q UNION ALL SELECT 7 % 3",
    )
    .await
    .unwrap();
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.mod_u", "q").await,
        DataType::Int64,
        "Spark `%` keeps the integer operand type — the fix must not widen it to double"
    );
    let batches = execute(&ctx, &catalogs, "SELECT q FROM ice.sales.mod_u")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values: Vec<i64> = Vec::new();
    for batch in &batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..array.len() {
            values.push(array.value(row));
        }
    }
    values.sort_unstable();
    assert_eq!(values, vec![1, 1]);
}

/// A CTAS of a union of DECIMAL divisions stays decimal.
#[tokio::test]
async fn ctas_union_decimal_division_stays_decimal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dec_div AS \
             SELECT CAST(1 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2)) AS q \
             UNION ALL SELECT CAST(7 AS DECIMAL(10,2)) / CAST(2 AS DECIMAL(10,2))",
    )
    .await
    .expect("union-of-decimal-division CTAS must round-trip without over-promotion");
    assert!(
        matches!(
            ctas_stored_type(&ctx, &catalogs, "ice.sales.dec_div", "q").await,
            DataType::Decimal128(..)
        ),
        "decimal `/` must stay decimal in storage, not widen to double"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT q FROM ice.sales.dec_div").await,
        2
    );
}

/// A THREE-branch union mixing an integer division, an already-double division, and a zero divisor.
#[tokio::test]
async fn ctas_three_branch_mixed_union_reconciles_to_double() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_ansi(&wh, false).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tri AS SELECT 5/2 AS q \
             UNION ALL SELECT CAST(9 AS DOUBLE)/CAST(2 AS DOUBLE) \
             UNION ALL SELECT 1/0",
    )
    .await
    .expect("three-branch mixed-division CTAS must round-trip without a writer type mismatch");
    assert_eq!(
        ctas_stored_type(&ctx, &catalogs, "ice.sales.tri", "q").await,
        DataType::Float64
    );
    assert_eq!(
        ctas_f64_column_sorted(&ctx, &catalogs, "ice.sales.tri", "q").await,
        vec![None, Some(2.5), Some(4.5)]
    );
}

/// The SELECT path still returns union-of-division as double with the right values.
#[tokio::test]
async fn select_union_of_integer_division_still_double() {
    use datafusion::arrow::array::{Array, Float64Array};
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let batches = execute(&ctx, &catalogs, "SELECT 5/2 AS q UNION ALL SELECT 7/2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values: Vec<Option<f64>> = Vec::new();
    for batch in &batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "SELECT union division must be Float64, got {:?}",
                    batch.schema().field(0).data_type()
                )
            });
        for row in 0..array.len() {
            values.push(array.is_valid(row).then(|| array.value(row)));
        }
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    assert_eq!(values, vec![Some(2.5), Some(3.5)]);
}

/// A strict catalog with only `location_uri` must resolve CTAS using the Glue namespace shape.
#[tokio::test]
async fn ctas_resolves_location_uri_only_namespace_the_glue_db_shape() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let glue_db_location = format!("{warehouse}/pre_existing_glue_db");

    catalogs["glue_like"]
        .create_namespace(
            &NamespaceIdent::new("legacy".to_string()),
            HashMap::from([("location_uri".to_string(), glue_db_location.clone())]),
        )
        .await
        .unwrap();

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.legacy.orders AS SELECT * FROM src",
    )
    .await
    .expect("CTAS into a location_uri-only namespace (a pre-existing Glue DB) must resolve");

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.legacy.orders").await,
        3
    );
    assert!(
        count_parquet_files(std::path::Path::new(&glue_db_location)) > 0,
        "CTAS data must land under the namespace `location_uri` `{glue_db_location}`"
    );
}

/// U2-P3: BOTH location keys set.
#[tokio::test]
async fn ctas_prefers_location_over_location_uri_when_both_set() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;

    // Both keys EQUAL — the shape every post-U2 RePark-created namespace has.
    let equal_location = format!("{warehouse}/both_equal");
    catalogs["glue_like"]
        .create_namespace(
            &NamespaceIdent::new("both_equal".to_string()),
            HashMap::from([
                ("location".to_string(), equal_location.clone()),
                ("location_uri".to_string(), equal_location.clone()),
            ]),
        )
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.both_equal.orders AS SELECT * FROM src",
    )
    .await
    .expect("a dual-keyed (equal) namespace must resolve");
    assert!(
        count_parquet_files(std::path::Path::new(&equal_location)) > 0,
        "CTAS data must land under the (equal) dual-keyed location"
    );

    // Both keys DIFFERENT — `location` must win, deterministically.
    let primary_location = format!("{warehouse}/primary_by_location");
    let other_location = format!("{warehouse}/other_by_location_uri");
    catalogs["glue_like"]
        .create_namespace(
            &NamespaceIdent::new("both_differ".to_string()),
            HashMap::from([
                ("location".to_string(), primary_location.clone()),
                ("location_uri".to_string(), other_location.clone()),
            ]),
        )
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.both_differ.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM glue_like.both_differ.orders"
        )
        .await,
        3
    );
    assert!(
        count_parquet_files(std::path::Path::new(&primary_location)) > 0,
        "with both keys set, CTAS data must land under `location` (the documented precedence)"
    );
    assert_eq!(
        count_parquet_files(std::path::Path::new(&other_location)),
        0,
        "no data may land under the losing `location_uri` path — precedence is deterministic"
    );
}

/// CTAS with an explicit column list remains rejected and must not create an orphan table.
#[tokio::test]
async fn ctas_explicit_column_list_rejected_still_pinned() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cl (a INT, b STRING) USING iceberg AS \
             SELECT id AS x, name AS y FROM src",
    )
    .await
    .expect_err("CTAS+cols must stay rejected");
    assert!(
        error
            .to_string()
            .contains("Schema may not be specified in a Create Table As Select (CTAS)"),
        "got: {error}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "cl".to_string(),
            ))
            .await
            .unwrap(),
        "rejected CTAS+cols must not leave an orphan table"
    );
}

/// O2-C1-L-002: CTAS path-escape rejection is wired on the shipping path (not unit-only).
#[tokio::test]
async fn ctas_path_escape_table_ident_refuses_on_shipping_path() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        r#"CREATE TABLE ice.sales.".." AS SELECT * FROM src"#,
    )
    .await
    .expect_err("path-escape table segment must fail on CTAS");
    let message = error.to_string();
    assert!(
        message.contains("path traversal") || message.contains(".."),
        "error must name path traversal, got: {message}"
    );
}

/// O3-C2-SEC-001: CTAS path-escape on namespace segment (shipping path, not unit-only).
#[tokio::test]
async fn ctas_path_escape_namespace_ident_refuses_on_shipping_path() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        r#"CREATE TABLE ice."..".escaped AS SELECT * FROM src"#,
    )
    .await
    .expect_err("path-escape namespace segment must fail on CTAS");
    let message = error.to_string();
    assert!(
        message.contains("path traversal") || message.contains(".."),
        "error must name path traversal, got: {message}"
    );
}

/// WG-2 P1.
#[tokio::test]
async fn ctas_multi_batch_source_streams_all_values_and_types() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_multi_batch_source(
        &ctx,
        "multi",
        &[&[(1, "a"), (2, "b")], &[(3, "c")], &[(4, "d"), (5, "e")]],
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.streamed AS SELECT id, name FROM multi",
    )
    .await
    .unwrap();
    assert_eq!(
        read_back_typed(&ctx, &catalogs, "ice.sales.streamed").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string()),
            (5, "e".to_string()),
        ],
        "every row from every source batch must land, value AND type"
    );
}

/// WG-2 P1.
#[tokio::test]
async fn ctas_partitioned_multi_batch_source_streams_all_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_multi_batch_source(
        &ctx,
        "multi",
        &[&[(1, "x"), (2, "y")], &[(3, "x")], &[(4, "y"), (5, "x")]],
    );
    execute(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.parts USING iceberg PARTITIONED BY (name) AS SELECT id, name FROM multi",
        )
        .await
        .unwrap();
    assert_eq!(
        read_back_typed(&ctx, &catalogs, "ice.sales.parts").await,
        vec![
            (1, "x".to_string()),
            (2, "y".to_string()),
            (3, "x".to_string()),
            (4, "y".to_string()),
            (5, "x".to_string()),
        ],
        "every row from every source batch must land in its partition, value AND type"
    );
}

/// WG-2 P4.
#[tokio::test]
async fn ctas_empty_select_creates_empty_unpartitioned_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empties AS SELECT id, name FROM src WHERE id < 0",
    )
    .await
    .unwrap();
    // The table exists with the derived schema (id int, name string) and zero rows.
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "empties".to_string(),
        ))
        .await
        .unwrap();
    let fields = table.metadata().current_schema().as_struct().fields();
    assert_eq!(fields.len(), 2, "the empty CTAS still derives both columns");
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[1].name, "name");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.empties").await,
        0,
        "an empty-result CTAS reads back zero rows"
    );
}

/// WG-2 P3.
#[tokio::test]
async fn ctas_multi_batch_midstream_failure_leaves_no_orphan() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Batch 1 is clean (id 1); a later batch carries the value `2` the injected UDF errors on.
    register_multi_batch_source(&ctx, "multi", &[&[(1, "a")], &[(2, "b")], &[(3, "c")]]);
    register_failing_scalar(&ctx);
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.torn AS SELECT repark_fail_on_two(id) AS id, name FROM multi",
    )
    .await
    .expect_err("a mid-stream source failure must error");
    assert!(
        error.to_string().contains("injected CTAS source failure"),
        "must surface the source error, got: {error}"
    );
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "torn".to_string());
    assert!(
        !catalogs["ice"].table_exists(&ident).await.unwrap(),
        "a mid-stream failure must not publish an orphan table"
    );
}

/// `build_ctas` on a non-CTAS `CREATE TABLE` returns a planning error instead of panicking.
#[test]
fn build_ctas_rejects_missing_query_without_panicking() {
    let sql = "CREATE TABLE ice.sales.t (id INT)";
    let statements =
        Parser::parse_sql(&DatabricksDialect {}, sql).expect("CREATE TABLE should parse");
    let Statement::CreateTable(create) = &statements[0] else {
        panic!("expected a CreateTable statement");
    };
    assert!(create.query.is_none(), "fixture must be a non-CTAS create");

    let Err(error) = build_ctas(create, &[]) else {
        panic!("build_ctas must reject a query-less CREATE TABLE");
    };
    assert!(
        error.to_string().contains("CTAS"),
        "error should name the missing-CTAS cause, got: {error}"
    );
}

/// A13: `register_memory_catalog` plus location-less Spark CTAS lands under the warehouse.
#[tokio::test]
async fn register_memory_catalog_location_less_ctas_lands_under_warehouse() {
    use std::sync::Arc;

    use repark_core::ReparkSession;

    use crate::{SparkDialect, SparkExtension};

    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog("a13mem", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace("a13mem", "a13ns", HashMap::new())
        .await
        .unwrap();
    session
        .sql("CREATE TABLE a13mem.a13ns.events USING iceberg AS SELECT 1 AS id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let under_warehouse = warehouse_dir
        .path()
        .join("repark_ctas")
        .join("a13mem")
        .join("a13ns")
        .join("events");
    assert!(
        under_warehouse.exists(),
        "location-less Spark CTAS must write under the warehouse, missing {under_warehouse:?}"
    );
    let global = std::env::temp_dir()
        .join("repark_ctas")
        .join("a13mem")
        .join("a13ns")
        .join("events");
    assert!(
        !global.exists(),
        "must not share the process-temp root, found {global:?}"
    );
}
