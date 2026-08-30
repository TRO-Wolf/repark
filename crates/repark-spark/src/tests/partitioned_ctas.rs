/// U1: CTAS PARTITIONED BY agrees on manifests, plan pruning, and Arrow value and type.
use std::collections::HashSet;

use futures::TryStreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{DataFile, Datum, Literal, ManifestContentType, Transform};
use iceberg::table::Table;

use super::super::*;
use super::common::*;

/// Load `catalog.namespace.table` through the iceberg handle (manifest/scan oracle).
async fn loaded_table(
    catalogs: &CatalogRegistry,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Table {
    catalogs[catalog]
        .load_table(&TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table.to_string(),
        ))
        .await
        .expect("load table")
}

/// The live DATA-file entries in the current snapshot's manifests.
async fn live_data_files(table: &Table) -> Vec<DataFile> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                files.push(entry.data_file().clone());
            }
        }
    }
    files
}

/// The data-file paths a filtered scan PLANS.
async fn planned_paths(table: &Table, predicate: Predicate) -> HashSet<String> {
    let scan = table
        .scan()
        .with_filter(predicate)
        .build()
        .expect("build filtered scan");
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .expect("plan files")
        .try_collect()
        .await
        .expect("collect planned tasks");
    tasks
        .iter()
        .map(|task| task.data_file_path().to_string())
        .collect()
}

/// PIN U1-P1.
#[tokio::test]
async fn ctas_partitioned_single_column_manifest_pruning_and_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "part_src", &[(1, "a"), (2, "b"), (1, "c")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_src",
    )
    .await
    .expect("partitioned CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "pt").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields().len(), 1, "one identity partition field");
    assert_eq!(spec.fields()[0].name, "id", "field name = column name (D3)");
    assert_eq!(spec.fields()[0].transform, Transform::Identity);

    let files = live_data_files(&table).await;
    assert!(!files.is_empty(), "the CTAS must produce data files");
    let mut rows = 0;
    let mut id1_paths = HashSet::new();
    for file in &files {
        let slot = file.partition().fields().first().cloned().flatten();
        let key = match slot {
            Some(Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(key))) => key,
            other => panic!("partition slot must be an int literal, got {other:?}"),
        };
        assert!(
            key == 1 || key == 2,
            "every DataFile must carry a real key value, got {key}"
        );
        if key == 1 {
            id1_paths.insert(file.file_path().to_string());
        }
        rows += file.record_count();
    }
    assert_eq!(rows, 3, "manifest record counts must cover all rows");
    assert!(
        !id1_paths.is_empty() && id1_paths.len() < files.len(),
        "both partitions must have their own file set"
    );

    let planned = planned_paths(&table, Reference::new("id").equal_to(Datum::int(1))).await;
    assert_eq!(
        planned, id1_paths,
        "a partition predicate must plan ONLY the matching partition's files"
    );

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (1, "c".to_string()),
            (2, "b".to_string())
        ],
    );
}

/// PIN U1-P2.
#[tokio::test]
async fn ctas_partitioned_two_columns_spec_and_manifest_values() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "part_src", &[(1, "a"), (2, "b"), (1, "a")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt2 USING iceberg PARTITIONED BY (id, name) AS \
             SELECT * FROM part_src",
    )
    .await
    .expect("two-column partitioned CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "pt2").await;
    let spec = table.metadata().default_partition_spec();
    let field_names: Vec<_> = spec.fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(
        field_names,
        vec!["id".to_string(), "name".to_string()],
        "both fields, clause order"
    );
    assert!(
        spec.fields()
            .iter()
            .all(|f| f.transform == Transform::Identity),
        "identity transforms"
    );

    let files = live_data_files(&table).await;
    assert!(!files.is_empty());
    let mut seen = HashSet::new();
    for file in &files {
        let fields = file.partition().fields().to_vec();
        assert_eq!(
            fields.len(),
            2,
            "both keys in every DataFile, got {fields:?}"
        );
        seen.insert(format!("{fields:?}"));
    }
    assert_eq!(
        seen.len(),
        2,
        "two distinct (id, name) partitions: {seen:?}"
    );

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt2").await,
        vec![
            (1, "a".to_string()),
            (1, "a".to_string()),
            (2, "b".to_string())
        ],
    );
}

/// PIN U1-P3.
#[tokio::test]
async fn ctas_partitioned_unsorted_source_fanout_one_file_set_per_partition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(
        &ctx,
        "part_src",
        &[(1, "a"), (2, "b"), (3, "c"), (1, "d"), (2, "e"), (1, "f")],
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt3 USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_src",
    )
    .await
    .expect("unsorted-source partitioned CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "pt3").await;
    let files = live_data_files(&table).await;
    let mut rows_by_partition: HashMap<String, u64> = HashMap::new();
    for file in &files {
        let slot = file.partition().fields().first().cloned().flatten();
        *rows_by_partition.entry(format!("{slot:?}")).or_insert(0) += file.record_count();
    }
    assert_eq!(
        rows_by_partition,
        HashMap::from([
            (format!("{:?}", Some(Literal::int(1))), 3),
            (format!("{:?}", Some(Literal::int(2))), 2),
            (format!("{:?}", Some(Literal::int(3))), 1),
        ]),
        "one file set per partition value, manifest counts matching the routed rows"
    );
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.pt3").await.len(), 6);
}

/// Register an id-name-timestamp source for the temporal-transform CTAS pins.
fn register_ts_source(ctx: &SessionContext, name: &str, rows: &[(i32, &str, i64)]) {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::TimeUnit;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// The distinct partition slots (first field) across the committed data files.
async fn partition_slots(table: &Table) -> HashSet<String> {
    live_data_files(table)
        .await
        .iter()
        .map(|file| format!("{:?}", file.partition().fields().first().cloned().flatten()))
        .collect()
}

/// PIN P1.
#[tokio::test]
async fn ctas_bucket_partition_spec_and_fork_hash_routing() {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int32Type;
    use iceberg::transform::create_transform_function;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ids: Vec<i32> = vec![1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233];
    let rows: Vec<(i32, &str)> = ids.iter().map(|&i| (i, "x")).collect();
    register_source(&ctx, "bsrc", &rows);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM bsrc",
    )
    .await
    .expect("bucket CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "bkt").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields().len(), 1);
    assert_eq!(
        spec.fields()[0].name,
        "id_bucket",
        "Java field name = col + _bucket"
    );
    assert_eq!(spec.fields()[0].transform, Transform::Bucket(4));

    // Self-oracle: the fork's own Bucket(4) over the ids is ground truth.
    let bucket_fn = create_transform_function(&Transform::Bucket(4)).unwrap();
    let buckets = bucket_fn
        .transform(Arc::new(Int32Array::from(ids.clone())))
        .unwrap();
    let buckets = buckets.as_primitive::<Int32Type>();
    let mut expected: HashMap<String, u64> = HashMap::new();
    for row in 0..buckets.len() {
        *expected
            .entry(format!("{:?}", Some(Literal::int(buckets.value(row)))))
            .or_insert(0) += 1;
    }
    assert!(
        expected.len() >= 2,
        "the ids must span >= 2 buckets: {expected:?}"
    );

    let mut actual: HashMap<String, u64> = HashMap::new();
    for file in &live_data_files(&table).await {
        let slot = file.partition().fields().first().cloned().flatten();
        if let Some(Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(b))) = slot {
            assert!(
                (0..4).contains(&b),
                "slot must be a bucket ordinal 0..4, got {b}"
            );
        } else {
            panic!("bucket slot must be an int literal, got {slot:?}");
        }
        *actual.entry(format!("{slot:?}")).or_insert(0) += file.record_count();
    }
    assert_eq!(
        actual, expected,
        "manifest routing must match the fork's Bucket(4) hash"
    );
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.bkt").await.len(), 12);
}

/// PIN P2.
#[tokio::test]
async fn ctas_truncate_str_partition_spec_and_routing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(
        &ctx,
        "tsrc",
        &[(1, "apple"), (2, "apricot"), (3, "cherry"), (4, "cocoa")],
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tr USING iceberg PARTITIONED BY (truncate(2, name)) AS \
             SELECT * FROM tsrc",
    )
    .await
    .expect("truncate CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "tr").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields()[0].name, "name_trunc");
    assert_eq!(spec.fields()[0].transform, Transform::Truncate(2));

    let slots = partition_slots(&table).await;
    let expected: HashSet<String> = ["ap", "ch", "co"]
        .iter()
        .map(|p| format!("{:?}", Some(Literal::string(*p))))
        .collect();
    assert_eq!(slots, expected, "truncate(2) prefixes: got {slots:?}");
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.tr").await.len(), 4);
}

/// PIN P3.
#[tokio::test]
async fn ctas_truncate_int_partition_spec_and_routing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ids = [5, 15, 23, 105];
    let rows: Vec<(i32, &str)> = ids.iter().map(|&i| (i, "x")).collect();
    register_source(&ctx, "isrc", &rows);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tri USING iceberg PARTITIONED BY (truncate(10, id)) AS \
             SELECT * FROM isrc",
    )
    .await
    .expect("truncate-int CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "tri").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields()[0].name, "id_trunc");
    assert_eq!(spec.fields()[0].transform, Transform::Truncate(10));

    let slots = partition_slots(&table).await;
    let expected: HashSet<String> = ids
        .iter()
        .map(|i| format!("{:?}", Some(Literal::int(i - i % 10))))
        .collect();
    assert_eq!(slots, expected, "truncate(10) buckets: got {slots:?}");
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.tri").await.len(), 4);
}

/// PIN P4.
#[tokio::test]
async fn ctas_temporal_partition_spec_and_routing() {
    // Three timestamps: 1970-01-01T00, 1970-01-02T00, 1970-01-02T05 (micros).
    const DAY: i64 = 86_400_000_000;
    const HOUR: i64 = 3_600_000_000;
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ts_rows: &[(i32, &str, i64)] = &[(1, "a", 0), (2, "b", DAY), (3, "c", DAY + 5 * HOUR)];
    register_ts_source(&ctx, "tssrc", ts_rows);

    for (func, suffix, transform, distinct) in [
        ("years", "ts_year", Transform::Year, 1usize),
        ("months", "ts_month", Transform::Month, 1),
        ("days", "ts_day", Transform::Day, 2),
        ("hours", "ts_hour", Transform::Hour, 3),
    ] {
        let tbl = format!("t_{func}");
        execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{tbl} USING iceberg PARTITIONED BY ({func}(ts)) \
                     AS SELECT * FROM tssrc"
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("temporal CTAS {func} must succeed: {e}"));

        let table = loaded_table(&catalogs, "ice", "sales", &tbl).await;
        let spec = table.metadata().default_partition_spec();
        assert_eq!(spec.fields()[0].name, suffix, "{func} field name");
        assert_eq!(spec.fields()[0].transform, transform, "{func} transform");
        assert_eq!(
            partition_slots(&table).await.len(),
            distinct,
            "{func} must route the 3 rows into {distinct} distinct partition(s)"
        );
        assert_eq!(
            table_rows(&ctx, &catalogs, &format!("ice.sales.{tbl}"))
                .await
                .len(),
            3
        );
    }
}

/// PIN P5.
#[tokio::test]
async fn ctas_mixed_identity_and_transform_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "msrc", &[(1, "a"), (2, "a"), (3, "b"), (4, "b")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mx USING iceberg \
             PARTITIONED BY (name, bucket(4, id)) AS SELECT * FROM msrc",
    )
    .await
    .expect("mixed CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "mx").await;
    let spec = table.metadata().default_partition_spec();
    let names: Vec<_> = spec.fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(names, vec!["name".to_string(), "id_bucket".to_string()]);
    assert_eq!(spec.fields()[0].transform, Transform::Identity);
    assert_eq!(spec.fields()[1].transform, Transform::Bucket(4));
    for file in &live_data_files(&table).await {
        assert_eq!(
            file.partition().fields().len(),
            2,
            "both partition slots in every DataFile"
        );
    }
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.mx").await.len(), 4);
}

/// PIN P6.
#[tokio::test]
async fn ctas_partition_transform_zero_width_and_unknown_rejected() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for (clause, needle) in [
        ("bucket(0, id)", "> 0"),
        ("truncate(0, name)", "> 0"),
        ("bucket(-1, id)", "> 0"),
        ("truncate(-5, name)", "> 0"),
        ("nonsense(id)", "not a supported partition transform"),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.bad USING iceberg PARTITIONED BY ({clause}) AS \
                     SELECT * FROM src"
            ),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{clause} must be rejected, not accepted"));
        let message = error.to_string();
        assert!(
            message.contains(needle),
            "the `{clause}` error must contain `{needle}`, got: {message}"
        );
    }
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "bad".to_string(),
            ))
            .await
            .unwrap(),
        "no table may be created on a rejected transform"
    );
}

/// PIN U1-P5 — an unpartitioned CTAS stays unpartitioned.
#[tokio::test]
async fn ctas_without_partitioned_by_stays_unpartitioned() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.plain AS SELECT * FROM src",
    )
    .await
    .expect("plain CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "plain").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned(),
        "no clause ⇒ unpartitioned spec"
    );
}

/// PIN U1-P6.
#[tokio::test]
async fn ctas_or_replace_carries_new_partition_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "part_src", &[(1, "a"), (2, "b"), (1, "c")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rp AS SELECT * FROM src",
    )
    .await
    .expect("initial unpartitioned CTAS must succeed");
    assert!(
        loaded_table(&catalogs, "ice", "sales", "rp")
            .await
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.rp USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_src",
    )
    .await
    .expect("OR REPLACE with PARTITIONED BY must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "rp").await;
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields().len(), 1, "the replace carries the NEW spec");
    assert_eq!(spec.fields()[0].name, "id");
    assert_eq!(spec.fields()[0].transform, Transform::Identity);

    let files = live_data_files(&table).await;
    assert!(files.iter().all(|f| f.partition().fields().len() == 1));
    let planned = planned_paths(&table, Reference::new("id").equal_to(Datum::int(2))).await;
    let id2_paths: HashSet<String> = files
        .iter()
        .filter(|f| f.partition().fields() == [Some(Literal::int(2))])
        .map(|f| f.file_path().to_string())
        .collect();
    assert_eq!(planned, id2_paths, "pruning works on the replaced table");
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.rp").await,
        vec![
            (1, "a".to_string()),
            (1, "c".to_string()),
            (2, "b".to_string())
        ],
    );
}

/// PIN U1-P13.
#[tokio::test]
async fn ctas_or_replace_without_clause_resets_to_unpartitioned() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "part_src", &[(1, "a"), (2, "b")]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rr USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_src",
    )
    .await
    .expect("partitioned CTAS must succeed");
    assert!(
        !loaded_table(&catalogs, "ice", "sales", "rr")
            .await
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.rr AS SELECT * FROM src",
    )
    .await
    .expect("clause-less OR REPLACE must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "rr").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned(),
        "no clause on the replace ⇒ the table resets to unpartitioned"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.rr").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
    );
}

/// PIN U1-P7a.
#[tokio::test]
async fn ctas_partitioned_on_strict_catalog_lands_under_location() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/strict_ns");
    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .expect("create namespace with LOCATION");

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.pt USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM src",
    )
    .await
    .expect("partitioned CTAS on a strict catalog must succeed");

    let table = loaded_table(&catalogs, "glue_like", "silver", "pt").await;
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    assert!(
        count_parquet_files(std::path::Path::new(&location)) >= 3,
        "one file per partition must land under the namespace LOCATION"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.pt").await,
        3
    );
}

/// PIN U1-P7b.
#[tokio::test]
async fn ctas_partitioned_location_check_precedes_source_execution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, _warehouse) = setup_strict_catalog(&wh).await;
    catalogs["glue_like"]
        .create_namespace(
            &NamespaceIdent::new("nolocation".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();
    register_failing_scalar(&ctx);

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.nolocation.pt USING iceberg PARTITIONED BY (id) AS \
             SELECT repark_fail_on_two(id) AS id, name FROM src",
    )
    .await
    .expect_err("a location-less strict namespace must fail loud");
    let message = error.to_string();
    assert!(
        message.contains("location"),
        "the LOCATION error must win, got: {message}"
    );
    assert!(
        !message.contains("injected CTAS source failure"),
        "the source must NOT execute, got: {message}"
    );
}

/// Typed partition columns refuse with Spark's partition-type error class and create no table.
#[tokio::test]
async fn ctas_partitioned_by_typed_column_rejected_spark_parity() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.typed USING iceberg PARTITIONED BY (name STRING) AS \
             SELECT * FROM src",
    )
    .await
    .expect_err("a typed partition column in CTAS must be rejected (Spark parity)");
    let message = error.to_string();
    assert!(
        message.contains("Partition column types may not be specified"),
        "the Spark message class, got: {message}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "typed".to_string(),
            ))
            .await
            .unwrap(),
        "no table may be created"
    );
}

/// PIN U1-P10.
#[tokio::test]
async fn ctas_partitioned_by_unknown_column_rejected_before_source_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_failing_scalar(&ctx);

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.uk USING iceberg PARTITIONED BY (nope) AS \
             SELECT repark_fail_on_two(id) AS id, name FROM src",
    )
    .await
    .expect_err("an unknown partition column must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("`nope`") && message.contains("id, name"),
        "must name the column and the available outputs, got: {message}"
    );
    assert!(
        !message.contains("injected CTAS source failure"),
        "the source must NOT execute (resolved before collect), got: {message}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "uk".to_string(),
            ))
            .await
            .unwrap(),
    );
}

/// PIN U1-P11.
#[tokio::test]
async fn ctas_partitioned_by_duplicate_column_rejected_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dup USING iceberg PARTITIONED BY (id, id) AS \
             SELECT * FROM src",
    )
    .await
    .expect_err("a duplicate partition column must be rejected");
    assert!(
        error.to_string().contains("more than once"),
        "the fork's duplicate-name reject, got: {error}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "dup".to_string(),
            ))
            .await
            .unwrap(),
    );
}

/// PIN U1-P12.
#[tokio::test]
async fn ctas_partitioned_by_utf8view_key_conforms_and_roundtrips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vw USING iceberg PARTITIONED BY (name) AS \
             SELECT id, arrow_cast(name, 'Utf8View') AS name FROM src",
    )
    .await
    .expect("a view-typed partition column must conform and write");

    let table = loaded_table(&catalogs, "ice", "sales", "vw").await;
    let files = live_data_files(&table).await;
    let mut seen: Vec<String> = files
        .iter()
        .map(|f| format!("{:?}", f.partition().fields()))
        .collect();
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        3,
        "one distinct partition value per name, got {seen:?}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.vw").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
    );
}

/// PIN U1-P14.
#[tokio::test]
async fn ctas_partitioned_empty_select_creates_empty_partitioned_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empty USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM src WHERE false",
    )
    .await
    .expect("an empty partitioned CTAS must succeed");

    let table = loaded_table(&catalogs, "ice", "sales", "empty").await;
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    assert!(live_data_files(&table).await.is_empty(), "zero data files");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.empty").await,
        0
    );
}

/// PIN U1-P15.
#[tokio::test]
async fn ctas_partitioned_by_nested_reference_rejected() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nested USING iceberg PARTITIONED BY (s.f) AS \
             SELECT * FROM src",
    )
    .await
    .expect_err("a nested partition reference must be gated");
    let message = error.to_string();
    assert!(
        message.contains("s.f") && message.contains("not supported"),
        "must name the nested reference, got: {message}"
    );
}

/// PIN U1-P16.
#[tokio::test]
async fn ctas_partitioned_by_malformed_clause_shapes_rejected() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let duplicate = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m1 USING iceberg PARTITIONED BY (id) \
             PARTITIONED BY (name) AS SELECT * FROM src",
    )
    .await
    .expect_err("a duplicate clause must be rejected");
    assert!(
        duplicate.to_string().contains("duplicate PARTITIONED BY"),
        "got: {duplicate}"
    );

    let empty = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.m2 USING iceberg PARTITIONED BY () AS \
             SELECT * FROM src",
    )
    .await
    .expect_err("an empty field list must be rejected");
    assert!(empty.to_string().contains("PARTITIONED BY"), "got: {empty}");
}
