use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int32Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_iceberg::write::conform_stage::{
    clear_stage_record, install_stage_record, last_stage_sites,
};
use repark_iceberg::write::ntz_store::NTZ_WALL_CAST_UDF_NAME;
use repark_iceberg::write::{
    StageSite, concurrency_from_ctx, conform_table_frame, resolve_empty_session_write,
    stage_overwrite_files_with,
};
use repark_spark::write_door::{WriteDoor, last_write_doors};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

async fn ansi_door() -> (ReparkSession, TempDir) {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native ansi session");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE SCHEMA IF NOT EXISTS ice.ns",
        "CREATE TABLE ice.ns.t (id INT, k INT) WITH (partitioning = ARRAY['k'])",
        "INSERT INTO ice.ns.t VALUES (1, 0)",
    ] {
        run(&session, setup).await;
    }
    (session, warehouse)
}

async fn run(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("the ANSI door plans {sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("the ANSI door runs {sql}: {error}"))
}

async fn rows(session: &ReparkSession, table: &str) -> Vec<(i32, i32)> {
    let batches = run(session, &format!("SELECT id, k FROM {table} ORDER BY id")).await;
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let keys = batch.column(1).as_primitive::<Int32Type>();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row) && !keys.is_null(row));
            out.push((ids.value(row), keys.value(row)));
        }
    }
    out
}

#[tokio::test]
async fn d15_native_insert_with_a_session_write_conf_stores_the_row() {
    let (session, _warehouse) = ansi_door().await;
    assert!(session.set_iceberg_session_write_conf("spark.sql.iceberg.compression-codec", "zstd"));
    run(&session, "INSERT INTO ice.ns.t VALUES (2, 0)").await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 0), (2, 0)]);
}

#[tokio::test]
async fn d16_native_insert_overwrite_partition_replaces_the_partition() {
    let (session, _warehouse) = ansi_door().await;
    run(&session, "INSERT INTO ice.ns.t VALUES (5, 1)").await;
    run(
        &session,
        "INSERT OVERWRITE ice.ns.t PARTITION (k = 0) SELECT 2 AS id",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(2, 0), (5, 1)]);
}

#[tokio::test]
async fn d17_native_create_table_as_stores_the_query_rows() {
    let (session, _warehouse) = ansi_door().await;
    run(
        &session,
        "CREATE TABLE ice.ns.c AS SELECT CAST(3 AS INT) AS id, CAST(4 AS INT) AS k",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.c").await, [(3, 4)]);
}

#[tokio::test]
async fn d18_the_rust_append_api_stores_the_batch() {
    let (session, _warehouse) = ansi_door().await;
    let catalogs = session.catalogs_snapshot();
    let catalog = catalogs.get("ice").expect("the ice catalog");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("k", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(6)])),
            Arc::new(Int32Array::from(vec![Some(0)])),
        ],
    )
    .expect("batch");
    let ident = TableIdent::new(NamespaceIdent::new("ns".to_string()), "t".to_string());
    repark_iceberg::append(catalog, &ident, vec![batch])
        .await
        .expect("append");
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 0), (6, 0)]);
}

#[tokio::test]
async fn d19_native_merge_inserts_and_updates() {
    let (session, _warehouse) = ansi_door().await;
    run(
        &session,
        "MERGE INTO ice.ns.t t USING (SELECT 1 AS id, 9 AS k UNION ALL SELECT 7 AS id, 0 AS k) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET k = s.k \
         WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, s.k)",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 9), (7, 0)]);
}

#[tokio::test]
async fn d20_native_update_and_delete_change_the_rows() {
    let (session, _warehouse) = ansi_door().await;
    run(&session, "INSERT INTO ice.ns.t VALUES (8, 0)").await;
    run(&session, "UPDATE ice.ns.t SET k = 3 WHERE id = 1").await;
    run(&session, "DELETE FROM ice.ns.t WHERE id = 8").await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 3)]);
}

const INSTANT_LITERAL: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const MISSING_WALL_KERNEL: &str = "the store kernel `__repark_cast_timestamp_ntz__` is not \
                                   registered in this session";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Stage(&'static [StageSite]),
    PlanConformed,
    TypesCopied,
    ForkProvider,
    NativeStager,
    Control,
}

fn route_of(door: WriteDoor) -> Route {
    match door {
        WriteDoor::D1Insert | WriteDoor::D2InsertOptions | WriteDoor::D5ReplaceWhere => {
            Route::PlanConformed
        }
        WriteDoor::D6Ctas | WriteDoor::D17NativeCtas => Route::TypesCopied,
        WriteDoor::D14NativeInsert => Route::ForkProvider,
        WriteDoor::D15NativeInsertConf | WriteDoor::D16NativeOverwritePartition => {
            Route::NativeStager
        }
        WriteDoor::D18NativeAppendApi => Route::Control,
        WriteDoor::D3InsertByName => Route::Stage(&[StageSite::InsertByName]),
        WriteDoor::D4InsertOverwrite => {
            Route::Stage(&[StageSite::InsertOverwrite, StageSite::PartitionOverwrite])
        }
        WriteDoor::D7MergeInsert => Route::Stage(&[StageSite::MergeInsert]),
        WriteDoor::D8MergeUpdateStruct | WriteDoor::D9MergeUpdateField => {
            Route::Stage(&[StageSite::MergeUpdate])
        }
        WriteDoor::D10Update | WriteDoor::D20NativePredicateDml => {
            Route::Stage(&[StageSite::PredicateUpdate])
        }
        WriteDoor::D19NativeMerge => {
            Route::Stage(&[StageSite::MergeInsert, StageSite::MergeUpdate])
        }
        WriteDoor::D21MicroBatchSink => Route::Stage(&[StageSite::MicroBatchAppend]),
    }
}

fn spark_door(zone: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config("spark.sql.session.timeZone", zone)
        .build()
        .expect("spark session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<(), String> {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn spark_tables(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE NAMESPACE ice.ns".to_string(),
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP) USING iceberg".to_string(),
        format!("INSERT INTO ice.ns.src VALUES (1, {INSTANT_LITERAL})"),
        "CREATE TABLE ice.ns.t (id INT, ts TIMESTAMP_NTZ) USING iceberg".to_string(),
        "INSERT INTO ice.ns.t VALUES (1, TIMESTAMP_NTZ '2020-01-01 00:00:00')".to_string(),
        "CREATE TABLE ice.ns.st (id INT, st STRUCT<a: TIMESTAMP>) USING iceberg".to_string(),
        "INSERT INTO ice.ns.st VALUES (1, named_struct('a', TIMESTAMP '2020-01-01 00:00:00'))"
            .to_string(),
        "CREATE TABLE ice.ns.p (id INT, ts TIMESTAMP_NTZ, k INT) USING iceberg PARTITIONED BY (k)"
            .to_string(),
    ] {
        assert_eq!(attempt(session, &setup).await, Ok(()), "{setup}");
    }
    warehouse
}

fn spark_spellings(door: WriteDoor) -> &'static [&'static str] {
    match door {
        WriteDoor::D3InsertByName => {
            &["INSERT INTO ice.ns.t BY NAME SELECT id, x AS ts FROM ice.ns.src"]
        }
        WriteDoor::D4InsertOverwrite => &[
            "INSERT OVERWRITE ice.ns.t SELECT id, x FROM ice.ns.src",
            "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) SELECT id, x FROM ice.ns.src",
        ],
        WriteDoor::D7MergeInsert => &[
            "MERGE INTO ice.ns.t AS t USING ice.ns.src AS s ON t.id = s.id + 100 \
             WHEN NOT MATCHED THEN INSERT (id, ts) VALUES (s.id, s.x)",
        ],
        WriteDoor::D8MergeUpdateStruct => &[
            "MERGE INTO ice.ns.st AS t USING ice.ns.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET st = named_struct('a', s.x)",
        ],
        WriteDoor::D9MergeUpdateField => &[
            "MERGE INTO ice.ns.t AS t USING ice.ns.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET ts = s.x",
        ],
        WriteDoor::D10Update => {
            &["UPDATE ice.ns.t SET ts = TIMESTAMP '2026-01-02 03:04:05.123456+00:00' WHERE id >= 0"]
        }
        _ => &[],
    }
}

async fn spark_stage_sites(session: &ReparkSession, sql: &str) -> (Vec<WriteDoor>, Vec<StageSite>) {
    let ctx = session.context();
    clear_stage_record(ctx);
    assert_eq!(attempt(session, sql).await, Ok(()), "{sql}");
    (last_write_doors(ctx), last_stage_sites(ctx))
}

async fn native_stage_sites(sql: &str) -> Vec<StageSite> {
    let (session, _warehouse) = ansi_door().await;
    install_stage_record(session.context());
    clear_stage_record(session.context());
    run(&session, sql).await;
    last_stage_sites(session.context())
}

async fn micro_batch_stage_sites() -> Vec<StageSite> {
    let (session, _warehouse) = ansi_door().await;
    let ctx = session.context();
    install_stage_record(ctx);
    let sink = load(&session, "t").await;
    let frame = session.sql("SELECT 2 AS id, 0 AS k").await.expect("frame");
    conform_table_frame(ctx, &sink, frame, StageSite::MicroBatchAppend)
        .await
        .expect("the micro-batch frame conforms");
    last_stage_sites(ctx)
}

async fn micro_sink(session: &ReparkSession, warehouse: &TempDir) -> iceberg::table::Table {
    let catalog = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("catalog");
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .expect("namespace");
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
        ])
        .build()
        .expect("schema");
    let location = format!("{}/ns/t", warehouse.path().display());
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(location)
        .schema(schema)
        .properties(std::collections::HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("table")
}

async fn load(session: &ReparkSession, table: &str) -> iceberg::table::Table {
    let catalog = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("catalog");
    catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("ns".to_string()),
            table.to_string(),
        ))
        .await
        .expect("table")
}

#[tokio::test]
async fn every_value_writing_door_passes_the_stage() {
    let spark = spark_door("America/New_York");
    let _warehouse = spark_tables(&spark).await;
    install_stage_record(spark.context());
    for door in WriteDoor::ALL {
        let Route::Stage(expected) = route_of(door) else {
            continue;
        };
        let recorded = match door {
            WriteDoor::D19NativeMerge => {
                native_stage_sites(
                    "MERGE INTO ice.ns.t t USING (SELECT 1 AS id, 9 AS k UNION ALL SELECT 7 AS \
                     id, 0 AS k) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET k = s.k \
                     WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, s.k)",
                )
                .await
            }
            WriteDoor::D20NativePredicateDml => {
                native_stage_sites("UPDATE ice.ns.t SET k = 3 WHERE id = 1").await
            }
            WriteDoor::D21MicroBatchSink => micro_batch_stage_sites().await,
            spark_door_id => {
                let mut sites = Vec::new();
                for sql in spark_spellings(spark_door_id) {
                    let (doors, staged) = spark_stage_sites(&spark, sql).await;
                    assert!(doors.contains(&spark_door_id), "{sql}: {doors:?}");
                    sites.extend(staged);
                }
                sites
            }
        };
        let mut recorded = recorded;
        recorded.sort_by_key(|site| format!("{site:?}"));
        recorded.dedup();
        let mut wanted = expected.to_vec();
        wanted.sort_by_key(|site| format!("{site:?}"));
        assert_eq!(recorded, wanted, "{door:?} skipped the stage");
    }
    let unrouted: Vec<WriteDoor> = WriteDoor::ALL
        .into_iter()
        .filter(|door| !matches!(route_of(*door), Route::Stage(_)))
        .collect();
    assert_eq!(
        unrouted,
        [
            WriteDoor::D1Insert,
            WriteDoor::D2InsertOptions,
            WriteDoor::D5ReplaceWhere,
            WriteDoor::D6Ctas,
            WriteDoor::D14NativeInsert,
            WriteDoor::D15NativeInsertConf,
            WriteDoor::D16NativeOverwritePartition,
            WriteDoor::D17NativeCtas,
            WriteDoor::D18NativeAppendApi,
        ]
    );
}

async fn stored_ts(session: &ReparkSession, table: &str) -> Vec<i64> {
    let batches = session
        .sql(&format!("SELECT ts FROM {table} ORDER BY id"))
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut values = Vec::new();
    for batch in &batches {
        let ticks = cast(batch.column(0), &DataType::Int64).expect("ticks");
        let ticks = ticks.as_primitive::<Int64Type>();
        values.extend(
            (0..ticks.len())
                .filter(|row| !ticks.is_null(*row))
                .map(|row| ticks.value(row)),
        );
    }
    values
}

#[tokio::test]
async fn a_missing_kernel_refuses_on_every_routed_door() {
    let session = spark_door("America/New_York");
    let _warehouse = spark_tables(&session).await;
    let before = stored_ts(&session, "ice.ns.t").await;
    session.context().deregister_udf(NTZ_WALL_CAST_UDF_NAME);
    for door in [
        WriteDoor::D3InsertByName,
        WriteDoor::D4InsertOverwrite,
        WriteDoor::D7MergeInsert,
        WriteDoor::D9MergeUpdateField,
        WriteDoor::D10Update,
    ] {
        let sql = spark_spellings(door)[0];
        let refusal = attempt(&session, sql)
            .await
            .expect_err("a write that needs a missing kernel refuses");
        assert!(
            refusal.contains(MISSING_WALL_KERNEL) || refusal.contains(NTZ_WALL_CAST_UDF_NAME),
            "{door:?}: {refusal}"
        );
        assert_eq!(
            stored_ts(&session, "ice.ns.t").await,
            before,
            "{door:?} stored a value"
        );
    }
    let bare = ReparkSession::builder().build().expect("bare session");
    let warehouse = TempDir::new().expect("warehouse");
    bare.register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    let sink = micro_sink(&bare, &warehouse).await;
    let frame = bare
        .sql("SELECT 1 AS id, arrow_cast(0, 'Timestamp(µs, \"UTC\")') AS ts")
        .await
        .expect("frame");
    let refusal = conform_table_frame(bare.context(), &sink, frame, StageSite::MicroBatchAppend)
        .await
        .expect_err("the micro-batch sink refuses without the wall kernel");
    assert!(
        refusal.to_string().contains(MISSING_WALL_KERNEL),
        "{refusal}"
    );
    assert!(
        refusal
            .to_string()
            .contains("the Spark dialect or the ANSI dialect"),
        "{refusal}"
    );
}

const FLOORED_MICROS: i64 = -1;

fn ns_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .build()
        .expect("spark session")
}

async fn ns_tables(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE NAMESPACE ice.ns",
        "CREATE TABLE ice.ns.src (id INT, ns timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
        "INSERT INTO ice.ns.src SELECT 1, arrow_cast(-1, 'Timestamp(ns)')",
        "CREATE TABLE ice.ns.t (id INT, ts TIMESTAMP_NTZ) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
        "CREATE TABLE ice.ns.u (id INT, ns timestamp_ns, ts TIMESTAMP_NTZ) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
        "INSERT INTO ice.ns.u SELECT 1, arrow_cast(-1, 'Timestamp(ns)'), CAST(NULL AS TIMESTAMP_NTZ)",
    ] {
        assert_eq!(attempt(session, setup).await, Ok(()), "{setup}");
    }
    warehouse
}

#[tokio::test]
async fn the_sink_guard_never_fires_on_a_routed_door() {
    let spark = ns_door();
    let _warehouse = ns_tables(&spark).await;
    for (door, sql) in [
        (
            "D3",
            "INSERT INTO ice.ns.t BY NAME SELECT id, ns AS ts FROM ice.ns.src",
        ),
        (
            "D4",
            "INSERT OVERWRITE ice.ns.t SELECT id, ns FROM ice.ns.src",
        ),
        (
            "D7",
            "MERGE INTO ice.ns.t AS t USING ice.ns.src AS s ON t.id = s.id + 100 \
             WHEN NOT MATCHED THEN INSERT (id, ts) VALUES (s.id + 1, s.ns)",
        ),
        (
            "D9",
            "MERGE INTO ice.ns.t AS t USING ice.ns.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET ts = s.ns",
        ),
    ] {
        assert_eq!(attempt(&spark, sql).await, Ok(()), "{door}: {sql}");
        let stored = stored_ts(&spark, "ice.ns.t").await;
        assert!(
            stored.iter().all(|value| *value == FLOORED_MICROS),
            "{door}: {stored:?}"
        );
    }
    assert_eq!(
        attempt(&spark, "UPDATE ice.ns.u SET ts = ns WHERE id >= 0").await,
        Ok(()),
        "D10"
    );
    assert_eq!(stored_ts(&spark, "ice.ns.u").await, [FLOORED_MICROS], "D10");

    let (ansi, warehouse) = ansi_door().await;
    let bare_sink = instant_sink(&ansi, &warehouse).await;
    let frame = || async {
        ansi.sql(
            "SELECT 1 AS id, arrow_cast(-1, 'Timestamp(ns)') AS ts, \
             arrow_cast(-1, 'Timestamp(ns, \"UTC\")') AS tz",
        )
        .await
        .expect("frame")
    };
    let conformed = conform_table_frame(
        ansi.context(),
        &bare_sink,
        frame().await,
        StageSite::MicroBatchAppend,
    )
    .await
    .expect("the micro-batch frame floors");
    let batches = conformed.collect().await.expect("collect");
    for column in [1, 2] {
        let ticks = cast(batches[0].column(column), &DataType::Int64).expect("ticks");
        assert_eq!(
            ticks.as_primitive::<Int64Type>().value(0),
            FLOORED_MICROS,
            "D21 column {column}"
        );
    }
    let (_, staging) = resolve_empty_session_write(ansi.context()).expect("staging");
    let stream = conform_table_frame(
        ansi.context(),
        &bare_sink,
        frame().await,
        StageSite::MicroBatchAppend,
    )
    .await
    .expect("conform")
    .execute_stream()
    .await
    .expect("stream");
    stage_overwrite_files_with(
        &bare_sink,
        stream,
        Vec::new(),
        concurrency_from_ctx(ansi.context()),
        &staging,
    )
    .await
    .expect("the D21 stager stores a nanosecond value without the coarser guard");
}

async fn instant_sink(session: &ReparkSession, warehouse: &TempDir) -> iceberg::table::Table {
    let catalog = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("catalog");
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::optional(3, "tz", Type::Primitive(PrimitiveType::Timestamptz)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("micro".to_string())
        .location(format!("{}/ns/micro", warehouse.path().display()))
        .schema(schema)
        .properties(std::collections::HashMap::new())
        .build();
    catalog
        .create_table(&NamespaceIdent::new("ns".to_string()), creation)
        .await
        .expect("table")
}

const NEW_YORK_SESSION_MIDNIGHT: i64 = 1_769_922_000_000_000;

async fn instant_micros(session: &ReparkSession, sql: &str) -> Vec<i64> {
    let batches = session
        .sql(sql)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut values = Vec::new();
    for batch in &batches {
        let ticks = batch.column(0).as_primitive::<Int64Type>();
        values.extend(
            (0..ticks.len())
                .filter(|row| !ticks.is_null(*row))
                .map(|row| ticks.value(row)),
        );
    }
    values
}

#[tokio::test]
async fn a_plain_update_from_add_months_stores_session_midnight() {
    let session = spark_door("America/New_York");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    assert_eq!(attempt(&session, "CREATE NAMESPACE ice.ns").await, Ok(()));
    for (name, update, read) in [
        (
            "leaf",
            "UPDATE ice.ns.am SET ts = add_months(ntz, 1) WHERE id >= 0",
            "SELECT unix_micros(ts) FROM ice.ns.am",
        ),
        (
            "field",
            "UPDATE ice.ns.am SET st.a = add_months(ntz, 1) WHERE id >= 0",
            "SELECT unix_micros(st.a) FROM ice.ns.am",
        ),
        (
            "whole",
            "UPDATE ice.ns.am SET st = named_struct('a', add_months(ntz, 1)) WHERE id >= 0",
            "SELECT unix_micros(st.a) FROM ice.ns.am",
        ),
    ] {
        for setup in [
            "DROP TABLE IF EXISTS ice.ns.am",
            "CREATE TABLE ice.ns.am (id INT, ntz TIMESTAMP_NTZ, ts TIMESTAMP, st STRUCT<a: TIMESTAMP>) \
             USING iceberg",
            "INSERT INTO ice.ns.am VALUES (1, TIMESTAMP_NTZ '2026-01-01 00:00:00', NULL, \
             named_struct('a', CAST(NULL AS TIMESTAMP)))",
        ] {
            assert_eq!(attempt(&session, setup).await, Ok(()), "{setup}");
        }
        assert_eq!(attempt(&session, update).await, Ok(()), "{name}: {update}");
        assert_eq!(
            instant_micros(&session, read).await,
            [NEW_YORK_SESSION_MIDNIGHT],
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_partition_overwrite_guards_an_integer_overflow_at_plan_time() {
    let session = spark_door("UTC");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE NAMESPACE ice.ns",
        "CREATE TABLE ice.ns.p (id INT, v INT, k INT) USING iceberg PARTITIONED BY (k)",
        "CREATE TABLE ice.ns.wide (id INT, d DOUBLE) USING iceberg",
        "INSERT INTO ice.ns.wide VALUES (1, 3000000000.0)",
    ] {
        assert_eq!(attempt(&session, setup).await, Ok(()), "{setup}");
    }
    for sql in [
        "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) SELECT id, d AS v FROM ice.ns.wide",
        "INSERT OVERWRITE ice.ns.p SELECT id, d AS v, 0 AS k FROM ice.ns.wide",
    ] {
        let refusal = attempt(&session, sql)
            .await
            .expect_err("an overflowing value refuses");
        assert!(
            refusal.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
            "{sql}: {refusal}"
        );
    }
}
