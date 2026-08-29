//! End-to-end Spark-door DDL tests for CTAS, namespace locations, and catalog metadata.
//! Sessions use both [`SparkExtension`] and [`SparkDialect`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::NamespaceIdent;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

/// A two-row batch: an id (Int32), a label (Utf8), and a date (Date32, days since epoch).
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("d", DataType::Date32, false),
    ]));
    // 2024-03-15 is 19797 days since the epoch; 2021-01-01 is 18628.
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Date32Array::from(vec![19797, 18628])),
        ],
    )
    .unwrap()
}

/// Build a Spark-doored session with the extension and dialect installed as defaults.
fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

/// End-to-end through the Python-facing `ReparkSession`: register an in-memory Iceberg catalog
/// (via the `register_memory_catalog` convenience), create a namespace + temp view, then run
/// CTAS and read back via `spark.sql` — exercising session → repark-spark → catalog → iceberg,
/// and the date-function shim on Iceberg-read data.
#[tokio::test]
async fn ctas_end_to_end_through_spark_sql() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();

    let spark = spark_session();
    spark
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    spark
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    spark
        .create_or_replace_temp_view("src", vec![sample_batch()])
        .unwrap();

    // The #1 op, end to end through `spark.sql`.
    spark
        .sql("CREATE TABLE ice.sales.orders AS SELECT * FROM src")
        .await
        .unwrap();

    // Read back; `year(d)` proves the function shim composes with the Iceberg scan.
    let batches = spark
        .sql("SELECT year(d) AS y FROM ice.sales.orders ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let years = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((years.value(0), years.value(1)), (2024, 2021));
}

/// ADV-1: a namespace created programmatically WITH a `location` property lets a CTAS succeed on
/// a `RequireExplicitLocation` catalog (Glue / S3 Tables), where SQL `CREATE NAMESPACE` (which
/// drops properties) would leave it location-less and the N5 fail-loud would reject the CTAS.
/// This is the path the AWS acceptance harness switched to; the location property is load-bearing
/// (dropping it makes the CTAS fail loud — mutation-proved).
#[tokio::test]
async fn create_namespace_with_location_lets_ctas_succeed_on_strict_catalog() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();

    let spark = spark_session();
    // `register_iceberg_catalog` tags the handle `RequireExplicitLocation` (a real warehouse) —
    // the strict policy Glue / S3 Tables carry, distinct from `register_memory_catalog`'s temp
    // fallback. So a location-less namespace here would make CTAS fail loud (N5).
    let catalog = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .unwrap();
    spark
        .register_iceberg_catalog("glue_like", catalog)
        .await
        .unwrap();

    // The programmatic path the harness uses: create the namespace WITH its warehouse location
    // (settable via SQL `CREATE NAMESPACE … LOCATION` since WG-5, or programmatically).
    spark
        .create_namespace(
            "glue_like",
            "silver",
            HashMap::from([("location".to_string(), format!("{warehouse}/silver"))]),
        )
        .await
        .unwrap();

    // CTAS into the strict catalog now succeeds — the namespace has a location to write to.
    spark
        .sql("CREATE TABLE glue_like.silver.t AS SELECT 1 AS id")
        .await
        .unwrap();
    let batches = spark
        .sql("SELECT id FROM glue_like.silver.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total, 1,
        "the CTAS into the strict catalog must have written its row"
    );
}

/// Both namespace-create routes store equal `location` and `location_uri` keys. The CTAS then
/// proves the dual-keyed namespace resolves end to end.
#[tokio::test]
async fn create_namespace_with_location_stores_both_location_keys() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();

    let spark = spark_session();
    let catalog = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .unwrap();
    spark
        .register_iceberg_catalog("glue_like", catalog.clone())
        .await
        .unwrap();

    let location = format!("{warehouse}/silver");
    spark
        .create_namespace(
            "glue_like",
            "silver",
            HashMap::from([("location".to_string(), location.clone())]),
        )
        .await
        .unwrap();

    // The stored property map, read back through the catalog handle: BOTH keys, equal, and
    // nothing invented beyond them.
    let namespace = catalog
        .get_namespace(&NamespaceIdent::new("silver".to_string()))
        .await
        .unwrap();
    assert_eq!(
        namespace.properties(),
        &HashMap::from([
            ("location".to_string(), location.clone()),
            ("location_uri".to_string(), location.clone()),
        ]),
        "create_namespace(location=…) must store BOTH `location` and `location_uri` \
             (equal), and no other keys"
    );

    // The dual-keyed namespace resolves end to end (both-equal read arm).
    spark
        .sql("CREATE TABLE glue_like.silver.t AS SELECT 1 AS id")
        .await
        .unwrap();
    let batches = spark
        .sql("SELECT id FROM glue_like.silver.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1);
}

/// The `spark.catalog` metadata surface the source publish job uses:
/// `tableExists` on a three-part name flips false → true across CTAS (and is false for an
/// absent namespace), a plan registers as a lazy temp view (`createOrReplaceTempView`),
/// one-part `tableExists` sees it, and `dropTempView` reports existence.
#[tokio::test]
async fn catalog_surface_table_exists_and_temp_views() {
    let wh = TempDir::new().unwrap();
    let spark = spark_session();
    spark
        .register_memory_catalog("glue_catalog", wh.path().to_str().unwrap())
        .await
        .unwrap();
    spark
        .create_namespace("glue_catalog", "silver", HashMap::new())
        .await
        .unwrap();

    assert!(!spark.table_exists("glue_catalog.silver.t").await.unwrap());
    // An absent NAMESPACE is `false` (PySpark), not an error.
    assert!(!spark.table_exists("glue_catalog.nope.t").await.unwrap());
    // An unregistered catalog IS an error — silently false would mask a wiring bug.
    assert!(spark.table_exists("nope.silver.t").await.is_err());
    assert!(spark.table_exists("a.b").await.is_err());

    // A plan (not batches) as a temp view: the `DataFrame.createOrReplaceTempView` path.
    let frame = spark.sql("SELECT 1 AS id, 'a' AS name").await.unwrap();
    spark
        .create_or_replace_temp_view_from("staging_view", &frame)
        .unwrap();
    assert!(spark.table_exists("staging_view").await.unwrap());

    spark
        .sql("CREATE TABLE glue_catalog.silver.t AS SELECT * FROM staging_view")
        .await
        .unwrap();
    assert!(spark.table_exists("glue_catalog.silver.t").await.unwrap());

    assert!(spark.drop_temp_view("staging_view").unwrap());
    assert!(!spark.drop_temp_view("staging_view").unwrap());
    assert!(!spark.table_exists("staging_view").await.unwrap());

    // T6: temp name directory is a sync SchemaProvider walk (no information_schema load).
    spark
        .create_or_replace_temp_view_from("tv_list_pin", &frame)
        .unwrap();
    let temps = spark.list_temp_view_names().unwrap();
    assert!(
        temps.iter().any(|name| name == "tv_list_pin"),
        "list_temp_view_names must see registered temp: {temps:?}"
    );
    assert!(spark.drop_temp_view("tv_list_pin").unwrap());

    // Re-registering an in-memory catalog under a taken name is a targeted error, never a
    // silent handle replacement that orphans the old catalog's tables.
    let err = spark
        .register_memory_catalog("glue_catalog", wh.path().to_str().unwrap())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already registered"));
}

/// `.config(...)` collects into the builder and drives catalog registration: a
/// source-publish-job-shaped block with `type = memory` (the AWS-free `RePark` form) builds the
/// session, and `register_configured_catalogs` wires the catalog so a CTAS round-trips —
/// proving the config path (not just an explicit `register_memory_catalog` call) drives a real
/// catalog. AWS-free by construction (`memory` kind only).
#[tokio::test]
async fn config_driven_memory_catalog_registers_and_runs() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();

    // The measured block shape, but with `type = memory` swapped in for AWS-free use, and the
    // Spark `io-impl` still present (it must be dropped, not choke the parse).
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let spark = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(
            "spark.sql.catalog.glue_alt",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.glue_alt.type", "memory")
        .config("spark.sql.catalog.glue_alt.warehouse", &warehouse)
        .config(
            "spark.sql.catalog.glue_alt.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .build()
        .unwrap();
    spark.register_configured_catalogs().await.unwrap();

    spark
        .create_namespace("glue_alt", "silver", HashMap::new())
        .await
        .unwrap();
    spark
        .create_or_replace_temp_view("src", vec![sample_batch()])
        .unwrap();
    spark
        .sql("CREATE TABLE glue_alt.silver.orders AS SELECT * FROM src")
        .await
        .unwrap();

    assert!(spark.table_exists("glue_alt.silver.orders").await.unwrap());
    let batches = spark
        .sql("SELECT count(*) AS n FROM glue_alt.silver.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 2);
}
