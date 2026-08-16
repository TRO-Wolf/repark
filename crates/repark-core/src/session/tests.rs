use super::*;
use arrow::array::{Date32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use iceberg::ErrorKind;

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

/// 2026-08 perf baseline: an unset builder lands the repark default (65536), and a
/// `datafusion.execution.batch_size` conf key still beats it (precedence: typed setter > conf >
/// default — the typed-setter arm is `builder_applies_config_without_error` below).
#[tokio::test]
async fn builder_default_batch_size_is_65536_and_conf_key_wins() {
    let default_session = ReparkSession::builder().build().unwrap();
    assert_eq!(
        default_session.context().copied_config().batch_size(),
        crate::session::DEFAULT_BATCH_SIZE
    );
    assert_eq!(crate::session::DEFAULT_BATCH_SIZE, 65536);

    let conf_session = ReparkSession::builder()
        .config("datafusion.execution.batch_size", "1234")
        .build()
        .unwrap();
    assert_eq!(conf_session.context().copied_config().batch_size(), 1234);
}

#[tokio::test]
async fn builder_applies_config_without_error() {
    let session = ReparkSession::builder()
        .memory_limit_gb(2)
        .batch_size(4096)
        .target_partitions(4)
        .build()
        .unwrap();
    let cfg = session.context().copied_config();
    assert_eq!(cfg.batch_size(), 4096);
    assert_eq!(cfg.target_partitions(), 4);
}

/// C1-Q-002 / C1-L-005: a default `build()` installs a finite `FairSpillPool` so peak RAM is
/// bounded without an explicit `.memory_limit_gb`. RAM-relative: Finite, in
/// `[MIN_MEMORY_LIMIT_BYTES, 8 GiB]`, equal to `default_memory_limit_bytes`.
#[tokio::test]
async fn builder_default_installs_eight_gib_fair_spill_pool() {
    use datafusion::execution::memory_pool::MemoryLimit;

    let session = ReparkSession::builder().build().unwrap();
    let limit = session.context().runtime_env().memory_pool.memory_limit();
    match limit {
        MemoryLimit::Finite(bytes) => {
            assert!(
                bytes >= MIN_MEMORY_LIMIT_BYTES,
                "default pool {bytes} is below the 1 MiB floor"
            );
            assert!(
                bytes <= DEFAULT_MEMORY_LIMIT_BYTES,
                "default pool {bytes} exceeds the 8 GiB cap"
            );
            assert_eq!(
                bytes,
                default_memory_limit_bytes(),
                "default pool must equal the detection helper"
            );
        }
        MemoryLimit::Infinite => panic!("expected Finite FairSpillPool, got Infinite"),
        MemoryLimit::Unknown => panic!("expected Finite FairSpillPool, got Unknown"),
    }
}

/// Explicit `.memory_limit_gb(n)` overrides the RAM-relative default.
#[tokio::test]
async fn builder_explicit_memory_limit_overrides_default() {
    use datafusion::execution::memory_pool::MemoryLimit;

    let session = ReparkSession::builder().memory_limit_gb(2).build().unwrap();
    let limit = session.context().runtime_env().memory_pool.memory_limit();
    match limit {
        MemoryLimit::Finite(bytes) => {
            assert_eq!(
                bytes,
                2 * BYTES_PER_GB,
                "explicit 2 GiB must win over default"
            );
        }
        MemoryLimit::Infinite => panic!("expected Finite(2 GiB), got Infinite"),
        MemoryLimit::Unknown => panic!("expected Finite(2 GiB), got Unknown"),
    }
}

/// Audit SAF-006: `batch_size(0)` / `target_partitions(0)` fail loud at build.
#[test]
fn builder_rejects_zero_batch_size_and_target_partitions() {
    let err = ReparkSession::builder()
        .batch_size(0)
        .build()
        .expect_err("batch_size(0) must be Config error");
    assert!(
        matches!(err, Error::Config(_)),
        "expected Config, got {err:?}"
    );
    assert!(err.to_string().contains("batch_size"));

    let err = ReparkSession::builder()
        .target_partitions(0)
        .build()
        .expect_err("target_partitions(0) must be Config error");
    assert!(
        matches!(err, Error::Config(_)),
        "expected Config, got {err:?}"
    );
    assert!(err.to_string().contains("target_partitions"));
}

/// Audit SAF-007: non-zero memory budgets below 1 MiB fail loud; `0` still opts out.
#[test]
fn builder_rejects_tiny_nonzero_memory_limit() {
    let err = ReparkSession::builder()
        .memory_limit_bytes(1)
        .build()
        .expect_err("1-byte budget must be Config error");
    assert!(
        matches!(err, Error::Config(_)),
        "expected Config, got {err:?}"
    );
    assert!(err.to_string().contains("memory_limit_bytes"));
    assert!(
        err.to_string()
            .contains(&MIN_MEMORY_LIMIT_BYTES.to_string())
    );

    ReparkSession::builder()
        .memory_limit_bytes(MIN_MEMORY_LIMIT_BYTES)
        .build()
        .expect("1 MiB floor must be accepted");
}

/// Audit SAF-007 reachability: the refused `(0, 1 MiB)` gap is unreachable through
/// `memory_limit_gb` — the only route the PyO3 constructor (and therefore the Python facade)
/// can take for a non-zero budget. The smallest non-zero whole GB is 1 GiB, and the conversion
/// saturates rather than wrapping, so no `gb` lands in the gap. MUTATION: change the
/// conversion to MB (`* 1024 * 1024`) or to a wrapping multiply → RED.
#[test]
fn memory_limit_gb_never_lands_below_the_floor() {
    const {
        assert!(
            MIN_MEMORY_LIMIT_BYTES <= BYTES_PER_GB,
            "the reachability claim assumes one whole GB clears the floor"
        );
    }
    for gb in [1usize, 2, 4096, usize::MAX] {
        let bytes = ReparkSession::builder()
            .memory_limit_gb(gb)
            .memory_limit_bytes
            .expect("memory_limit_gb always records a budget");
        assert!(
            bytes >= MIN_MEMORY_LIMIT_BYTES,
            "memory_limit_gb({gb}) produced {bytes}, inside the refused (0, 1 MiB) gap"
        );
        // The stronger claim the gap argument rests on: a whole GB really is >= 1 GiB, so the
        // margin above the floor is 1024x — not an accident of the floor's current value.
        assert!(
            bytes >= BYTES_PER_GB,
            "memory_limit_gb({gb}) produced {bytes}, below one whole GiB"
        );
        ReparkSession::builder()
            .memory_limit_gb(gb)
            .build()
            .expect("a whole-GB budget must never trip the SAF-007 floor");
    }
}

/// `memory_limit_bytes(0)` opts out of a bounded pool (DataFusion unbounded = Infinite).
/// C3-L-003: pin `Infinite` specifically — `!Finite` would also accept `Unknown`.
#[tokio::test]
async fn builder_zero_memory_limit_opts_out_of_pool() {
    use datafusion::execution::memory_pool::MemoryLimit;

    let session = ReparkSession::builder()
        .memory_limit_bytes(0)
        .build()
        .unwrap();
    let limit = session.context().runtime_env().memory_pool.memory_limit();
    match limit {
        MemoryLimit::Infinite => {}
        MemoryLimit::Finite(bytes) => {
            panic!("opt-out must not install a finite FairSpillPool, got Finite({bytes})")
        }
        MemoryLimit::Unknown => {
            panic!("opt-out must yield Infinite (unbounded), got Unknown")
        }
    }
}

/// O3-C3-Q-001: `memory_limit_gb(0)` must opt out the same way as `memory_limit_bytes(0)`
/// (`saturating_mul` → 0 → Some(0) → no `FairSpillPool`).
#[tokio::test]
async fn builder_zero_memory_limit_gb_opts_out_of_pool() {
    use datafusion::execution::memory_pool::MemoryLimit;

    let session = ReparkSession::builder().memory_limit_gb(0).build().unwrap();
    let limit = session.context().runtime_env().memory_pool.memory_limit();
    match limit {
        MemoryLimit::Infinite => {}
        MemoryLimit::Finite(bytes) => {
            panic!("memory_limit_gb(0) must opt out, got Finite({bytes})")
        }
        MemoryLimit::Unknown => {
            panic!("memory_limit_gb(0) must yield Infinite, got Unknown")
        }
    }
}

/// C2-L-006: quote-aware multipart identifiers (matches Python `_sql_table_ref` segments).
#[test]
fn parse_table_identifier_segments_quote_aware() {
    assert_eq!(
        parse_table_identifier_segments("catalog.db.t").expect("plain"),
        vec!["catalog", "db", "t"]
    );
    assert_eq!(
        parse_table_identifier_segments(r#"catalog."db.with.dot".t"#).expect("quoted"),
        vec!["catalog", "db.with.dot", "t"]
    );
    assert_eq!(
        parse_table_identifier_segments("cat.db.`my-table`").expect("backtick"),
        vec!["cat", "db", "my-table"]
    );
    // O3-C4-SEC-001: path-escape segments rejected at identity parse (not only CTAS compose).
    let traversal = parse_table_identifier_segments(r#"cat."..".t"#).unwrap_err();
    assert!(
        traversal.contains("path traversal") || traversal.contains(".."),
        "must reject '..' segment, got: {traversal}"
    );
    let separator = parse_table_identifier_segments(r#"cat."a/b".t"#).unwrap_err();
    assert!(
        separator.contains("path separators") || separator.contains('/'),
        "must reject '/' segment, got: {separator}"
    );
    assert!(
        parse_table_identifier_segments("t; DROP").is_err(),
        "SQL fragments must be rejected"
    );
    assert!(parse_table_identifier_segments("catalog.db.").is_err());

    // === r23 QI1: idents === shared probe table (lockstep with repark_iceberg::write::idents::probes)
    for &(segment, kind_tag) in repark_iceberg::write::idents::probes::PATH_ESCAPE_PROBES {
        let err = reject_path_escape_segment(segment).unwrap_err();
        match kind_tag {
            "traversal" => assert!(
                err.contains("path traversal") || err.contains(".."),
                "segment {segment:?}: {err}"
            ),
            "separator" => assert!(
                err.contains("path separators") || err.contains('/') || err.contains('\\'),
                "segment {segment:?}: {err}"
            ),
            other => panic!("unknown kind tag {other}"),
        }
    }
    for safe in repark_iceberg::write::idents::probes::PATH_ESCAPE_SAFE {
        assert!(
            reject_path_escape_segment(safe).is_ok(),
            "safe segment {safe:?}"
        );
    }
}

/// The S3 region override accepts both spellings (2026-07-12 naming decision): either key
/// alone works; both with identical values collapse; different values fail loud naming both
/// keys (never a silent prefer-one pick).
#[tokio::test]
async fn s3_region_override_accepts_both_spellings() {
    let spark_only = ReparkSession::builder()
        .config(object_store_s3::S3A_REGION_CONFIG_KEY, "us-east-1")
        .build()
        .unwrap();
    assert_eq!(spark_only.s3_region_override.as_deref(), Some("us-east-1"));

    let repark_only = ReparkSession::builder()
        .config(object_store_s3::REPARK_S3A_REGION_CONFIG_KEY, "us-west-2")
        .build()
        .unwrap();
    assert_eq!(repark_only.s3_region_override.as_deref(), Some("us-west-2"));

    let both_same = ReparkSession::builder()
        .config(object_store_s3::S3A_REGION_CONFIG_KEY, "us-east-1")
        .config(object_store_s3::REPARK_S3A_REGION_CONFIG_KEY, "us-east-1")
        .build()
        .unwrap();
    assert_eq!(
        both_same.s3_region_override.as_deref(),
        Some("us-east-1"),
        "identical dual-spelling values collapse"
    );

    let conflict = ReparkSession::builder()
        .config(object_store_s3::S3A_REGION_CONFIG_KEY, "us-east-1")
        .config(object_store_s3::REPARK_S3A_REGION_CONFIG_KEY, "us-west-2")
        .build();
    let message = conflict.unwrap_err().to_string();
    assert!(
        message.contains(object_store_s3::S3A_REGION_CONFIG_KEY)
            && message.contains(object_store_s3::REPARK_S3A_REGION_CONFIG_KEY),
        "conflict must name both keys, got: {message}"
    );
    assert!(
        !message.contains("us-east-1") && !message.contains("us-west-2"),
        "conflict must not echo raw region values, got: {message}"
    );
}

/// Re-registering via `register_iceberg_catalog` itself fails loud (not only the memory
/// convenience's early return).
#[tokio::test]
async fn register_iceberg_catalog_rejects_duplicate_name() {
    let session = ReparkSession::new().unwrap();
    let warehouse = std::env::temp_dir()
        .join("repark-dup-wh")
        .to_string_lossy()
        .into_owned();
    let first = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    session
        .register_iceberg_catalog("dup", first)
        .await
        .unwrap();
    let second = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let err = session
        .register_iceberg_catalog("dup", second)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already registered"), "got: {err}");
}

/// R-PERF-VALUES: materialize a VALUES plan into a `MemTable` so a second scan does not
/// re-execute the body (the pin that goes red if materialize only registers a lazy view).
#[tokio::test]
async fn materialize_dataframe_as_temp_view_is_scan_not_replan() {
    let session = ReparkSession::new().unwrap();
    let values = session
        .sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)")
        .await
        .unwrap();
    session
        .materialize_dataframe_as_temp_view("mat_v", values)
        .await
        .unwrap();
    // Two independent scans — both must see the same three rows (MemTable, not re-plan).
    for _ in 0..2 {
        let count = session
            .sql("SELECT count(*) AS c FROM mat_v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let c = count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 3);
    }
}

// === r23 CACHE1: cache-honesty ===
/// Cache entry point: size guard fails loud when collected bytes exceed `max_bytes`.
#[tokio::test]
async fn materialize_dataframe_as_cache_view_respects_max_bytes() {
    let session = ReparkSession::new().expect("session");
    let values = session
        .sql("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)")
        .await
        .expect("values plan");
    // One-byte budget must refuse (collected Arrow arrays are far larger).
    let err = session
        .materialize_dataframe_as_cache_view("cache_too_big", values, Some(1))
        .await
        .expect_err("max_bytes=1 must refuse a multi-row materialize");
    let message = err.to_string();
    assert!(
        message.contains("repark.cache.max_bytes"),
        "error must name the conf key; got {message}"
    );
    // View must not be registered after a refused materialize.
    assert!(
        !session
            .list_temp_view_names()
            .expect("list temps")
            .iter()
            .any(|name| name == "cache_too_big"),
        "refused cache materialize must not leave a temp view"
    );
}

/// Cache entry point without a budget still pins a [`MemTable`] (same as VALUES path).
#[tokio::test]
async fn materialize_dataframe_as_cache_view_without_budget_pins_memtable() {
    let session = ReparkSession::new().expect("session");
    let values = session
        .sql("SELECT * FROM (VALUES (10), (20)) AS t(id)")
        .await
        .expect("values plan");
    session
        .materialize_dataframe_as_cache_view("cache_ok", values, None)
        .await
        .expect("cache materialize without budget");
    let count = session
        .sql("SELECT count(*) AS c FROM cache_ok")
        .await
        .expect("count plan")
        .collect()
        .await
        .expect("count collect");
    let c = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("int64 count column")
        .value(0);
    assert_eq!(c, 2);
}

#[tokio::test]
async fn create_or_replace_temp_view_replaces() {
    let session = ReparkSession::new().unwrap();
    session
        .create_or_replace_temp_view("v", vec![sample_batch()])
        .unwrap();
    // Re-register a one-row batch under the same name; the view must reflect the replacement.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let one = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();
    session.create_or_replace_temp_view("v", vec![one]).unwrap();
    let batches = session
        .sql("SELECT COUNT(*) AS n FROM v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 1);
}

#[tokio::test]
async fn empty_temp_view_is_an_error() {
    let session = ReparkSession::new().unwrap();
    let err = session
        .create_or_replace_temp_view("empty", vec![])
        .unwrap_err();
    assert!(matches!(err, Error::DataFusion(_)));
}

#[tokio::test]
async fn read_parquet_round_trips() {
    use parquet::arrow::ArrowWriter;

    let batch = sample_batch();
    let path = std::env::temp_dir().join(format!("repark_session_{}.parquet", std::process::id()));
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let session = ReparkSession::new().unwrap();
    let df = session.read_parquet(path.to_str().unwrap()).await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 2);

    let _ = std::fs::remove_file(&path);
}

/// R1: CSV path read with Spark-ish header option (engine asset under the facade).
#[tokio::test]
async fn read_csv_with_header_round_trips() {
    let dir = std::env::temp_dir().join(format!("repark_session_csv_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.csv");
    std::fs::write(&path, "id,name\n1,a\n2,b\n").unwrap();

    let session = ReparkSession::new().unwrap();
    let mut options = HashMap::new();
    options.insert("header".to_string(), "true".to_string());
    let frame = session
        .read_csv(path.to_str().unwrap(), &options)
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 2);
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string(), "name".to_string()]);

    let _ = std::fs::remove_dir_all(&dir);
}

/// R1: NDJSON path read (Spark multiLine=false default).
#[tokio::test]
async fn read_json_ndjson_round_trips() {
    let dir = std::env::temp_dir().join(format!("repark_session_json_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.json");
    std::fs::write(
        &path,
        "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n",
    )
    .unwrap();

    let session = ReparkSession::new().unwrap();
    let options = HashMap::new();
    let frame = session
        .read_json(path.to_str().unwrap(), &options)
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 2);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Serialize `sample_batch()` to an in-memory Parquet byte buffer (the payload written into the
/// object store for the scheme-routing test).
fn sample_batch_as_parquet_bytes() -> Vec<u8> {
    use parquet::arrow::ArrowWriter;
    let batch = sample_batch();
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buffer
}

/// AWS-FREE proof that `read_parquet` routes an `s3://` AND an `s3a://` path to the SAME
/// registered object store: register an `object_store::memory::InMemory` under both scheme
/// forms of one bucket, write a Parquet object into it via the object-store API, then read it
/// back through BOTH schemes and assert identical rows. This exercises the URL→store resolution
/// (`parse_s3_bucket` + `register_bucket_store` + DataFusion's registry) end-to-end without any
/// AWS call — the bucket is pre-seeded in the registered set, so `read_parquet` never builds a
/// real S3 store. The risk pinned: an s3a path failing to resolve to the s3 store (scheme alias
/// regression), and a store registered under only one scheme.
#[tokio::test]
async fn read_parquet_routes_both_s3_schemes_to_the_registered_store() {
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectStorePath;
    use object_store::{ObjectStore, ObjectStoreExt};

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("x.parquet"),
            sample_batch_as_parquet_bytes().into(),
        )
        .await
        .expect("write the parquet object into the in-memory store");

    let session = ReparkSession::new().unwrap();
    session
        .register_s3_bucket_store_for_test("bucket", &store)
        .unwrap();

    for path in ["s3://bucket/x.parquet", "s3a://bucket/x.parquet"] {
        let batches = session
            .read_parquet(path)
            .await
            .unwrap_or_else(|error| panic!("read_parquet({path}) failed: {error:?}"))
            .collect()
            .await
            .unwrap();
        let total: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(
            total, 2,
            "both schemes must round-trip the two-row fixture ({path})"
        );
    }
}

/// `.config(...)` collects into the builder and drives catalog registration: a
/// source-publish-job-shaped block with `type = memory` (the AWS-free `RePark` form) builds the
/// session, and `register_configured_catalogs` wires the catalog so a CTAS round-trips —
/// proving the config path (not just an explicit `register_memory_catalog` call) drives a real
/// catalog. AWS-free by construction (`memory` kind only).
#[tokio::test]
async fn late_catalog_registration_adds_new_names_and_skips_existing() {
    use tempfile::TempDir;

    let wh_a = TempDir::new().unwrap();
    let wh_b = TempDir::new().unwrap();
    let spark = ReparkSession::builder()
        .config("spark.sql.catalog.cat_a.type", "memory")
        .config(
            "spark.sql.catalog.cat_a.warehouse",
            wh_a.path().to_str().unwrap(),
        )
        .build()
        .unwrap();
    spark.register_configured_catalogs().await.unwrap();

    // Late map: cat_a AGAIN (different warehouse — must be SKIPPED, original kept) + a NEW
    // cat_b (must register and become usable).
    let late = HashMap::from([
        (
            "spark.sql.catalog.cat_a.type".to_string(),
            "memory".to_string(),
        ),
        (
            "spark.sql.catalog.cat_a.warehouse".to_string(),
            wh_b.path().to_str().unwrap().to_string(),
        ),
        (
            "spark.sql.catalog.cat_b.type".to_string(),
            "memory".to_string(),
        ),
        (
            "spark.sql.catalog.cat_b.warehouse".to_string(),
            wh_b.path().to_str().unwrap().to_string(),
        ),
    ]);
    let (added, skipped) = spark
        .register_late_configured_catalogs(&late)
        .await
        .unwrap();
    assert_eq!(added, vec!["cat_b".to_string()], "new name registered");
    assert_eq!(skipped, vec!["cat_a".to_string()], "existing name skipped");
    spark
        .create_namespace("cat_b", "ns", HashMap::new())
        .await
        .expect("late-registered catalog is live");

    // Idempotent second pass: everything now exists -> nothing added.
    let (added2, skipped2) = spark
        .register_late_configured_catalogs(&late)
        .await
        .unwrap();
    assert!(
        added2.is_empty() && skipped2.len() == 2,
        "second pass all-skip"
    );

    // A late map with no catalog blocks is a clean no-op.
    let (a3, s3) = spark
        .register_late_configured_catalogs(&HashMap::from([(
            "spark.sql.shuffle.partitions".to_string(),
            "4".to_string(),
        )]))
        .await
        .unwrap();
    assert!(a3.is_empty() && s3.is_empty());

    // A malformed late block fails loud (memory kind REQUIRES a warehouse).
    let err = spark
        .register_late_configured_catalogs(&HashMap::from([(
            "spark.sql.catalog.cat_c.type".to_string(),
            "memory".to_string(),
        )]))
        .await
        .expect_err("malformed late block must error");
    assert!(err.to_string().contains("warehouse"), "got: {err}");
}

/// A malformed catalog block fails loud at `build()` (synchronously), before any async step —
/// the `getOrCreate` boundary surfaces the config error.
#[tokio::test]
async fn config_bad_catalog_block_fails_at_build() {
    let err = ReparkSession::builder()
        .config("spark.sql.catalog.x.type", "hive")
        .build()
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
    assert!(err.to_string().contains("spark.sql.catalog.x.type"));
}

/// A build with no catalog config registers nothing (the sync-`build()` contract holds for
/// existing callers) and `register_configured_catalogs` is a no-op.
#[tokio::test]
async fn config_free_build_registers_no_catalogs() {
    let spark = ReparkSession::builder().batch_size(2048).build().unwrap();
    spark.register_configured_catalogs().await.unwrap();
    // No catalog was configured, so a three-part probe hits the unknown-catalog path.
    assert!(spark.table_exists("glue_alt.silver.t").await.is_err());
}

// ---- WG-3 error taxonomy: DataFusionError classification -------------------------------------

/// A bare `DataFusionError::SQL` (a syntax error) for classification tests.
fn sql_parse_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(
            datafusion::sql::sqlparser::parser::ParserError::ParserError(
                "Expected an expression".to_string(),
            ),
        ),
        None,
    )
}

/// A `SchemaError::FieldNotFound` (an unresolved column) — the analysis sub-case a bare `Plan`
/// error does not exercise.
fn schema_field_not_found() -> DataFusionError {
    DataFusionError::SchemaError(
        Box::new(datafusion::common::SchemaError::FieldNotFound {
            field: Box::new(datafusion::common::Column::new_unqualified("a")),
            valid_fields: vec![],
        }),
        Box::new(None),
    )
}

/// Risk: a syntax error surfacing as anything but a parse error (it must become a
/// `ParseException` at the PyO3 layer, not `AnalysisException` or the base type).
#[test]
fn classify_sql_syntax_error_is_parse() {
    assert!(matches!(
        classify_datafusion_error(&sql_parse_error()),
        EngineErrorKind::Parse
    ));
    assert!(matches!(engine_err(sql_parse_error()), Error::Parse(_)));
}

/// Risk: a planning error (unknown table, type error) misrouted away from `AnalysisException`.
#[test]
fn classify_plan_error_is_analysis() {
    let error = DataFusionError::Plan("table 'x' not found".to_string());
    assert!(matches!(
        classify_datafusion_error(&error),
        EngineErrorKind::Analysis
    ));
    assert!(matches!(engine_err(error), Error::Analysis(_)));
}

/// The `SchemaError` (unresolved-column) branch, distinct from `Plan` — pins that a schema
/// error is analysis, so dropping `SchemaError` from the analysis arm goes red.
#[test]
fn classify_schema_error_is_analysis() {
    assert!(matches!(
        classify_datafusion_error(&schema_field_not_found()),
        EngineErrorKind::Analysis
    ));
    assert!(matches!(
        engine_err(schema_field_not_found()),
        Error::Analysis(_)
    ));
}

/// Risk: an execution error (a runtime cast failure, resource exhaustion, IO) misclassified as
/// analysis/parse — it must fall to the `RuntimeError`-compatible base bucket.
#[test]
fn classify_execution_error_is_base() {
    let error =
        DataFusionError::Execution("Cast error: cannot cast Utf8 'abc' to Int32".to_string());
    assert!(matches!(
        classify_datafusion_error(&error),
        EngineErrorKind::Other
    ));
    assert!(matches!(engine_err(error), Error::DataFusion(_)));
}

/// DataFusion wraps errors (`Context` / `Collection` / `Shared`); the classifier must peel to
/// the inner kind — stopping at the wrapper would misclassify every wrapped plan error as base.
#[test]
fn classify_peels_wrapper_to_inner_kind() {
    let wrapped = DataFusionError::Context(
        "while planning".to_string(),
        Box::new(DataFusionError::Plan("no such table".to_string())),
    );
    assert!(matches!(
        classify_datafusion_error(&wrapped),
        EngineErrorKind::Analysis
    ));

    let collected = DataFusionError::Collection(vec![sql_parse_error()]);
    assert!(matches!(
        classify_datafusion_error(&collected),
        EngineErrorKind::Parse
    ));

    let shared = DataFusionError::Shared(std::sync::Arc::new(schema_field_not_found()));
    assert!(matches!(
        classify_datafusion_error(&shared),
        EngineErrorKind::Analysis
    ));
}

/// The taxonomy must not lose the original engine diagnostic (the cause-chain requirement).
#[test]
fn engine_err_preserves_the_inner_message() {
    let converted = engine_err(DataFusionError::Plan("No field named zzz".to_string()));
    assert!(converted.to_string().contains("No field named zzz"));
}

// ---- U4 error taxonomy: NotImplemented + iceberg-kind classification ------------------------

/// A fabricated live iceberg error of the given kind, for classification pins.
fn iceberg_error_of(kind: ErrorKind, message: &str) -> iceberg::Error {
    iceberg::Error::new(kind, message.to_string())
}

/// The `External`-route fold the sql/write crates and the fork provider all use:
/// `DataFusionError::External(Box::new(iceberg_error))`.
fn external_iceberg(kind: ErrorKind, message: &str) -> DataFusionError {
    DataFusionError::External(Box::new(iceberg_error_of(kind, message)))
}

/// U4 pin (CQ-002): the risk is a deterministic scope gate (`DataFusionError::NotImplemented`
/// — partitioned MERGE, `MoR` mode, CTAS transforms, …) collapsing into the base bucket, so the
/// facade raises a bare `PySparkException` where PySpark raises
/// `UnsupportedOperationException`. The gate's message must survive verbatim (DataFusion's
/// "This feature is not implemented: " rendering included — message preserved, type changed).
#[test]
fn classify_not_implemented_is_unsupported_with_message_preserved() {
    let gate = DataFusionError::NotImplemented(
        "MERGE INTO a partitioned table is not supported yet".to_string(),
    );
    assert!(matches!(
        classify_datafusion_error(&gate),
        EngineErrorKind::Unsupported
    ));
    let converted = engine_err(gate);
    assert!(
        matches!(converted, Error::NotImplemented(_)),
        "expected NotImplemented, got {converted:?}"
    );
    assert_eq!(
        converted.to_string(),
        "This feature is not implemented: MERGE INTO a partitioned table is not supported yet"
    );
}

/// U4 pin (CQ-015, the External route): an iceberg-origin COMMIT error must be classified
/// from its live `ErrorKind` — `Error::Iceberg`, kind name leading the message — not
/// pre-stringified into the base `Error::DataFusion` with a misattributing
/// "External error:" wrapper. The risk: the facade shows "datafusion engine error: …" for a
/// catalog commit conflict and downstream code cannot tell an OCC conflict from a cast error.
#[test]
fn engine_err_iceberg_commit_conflict_classifies_kind_not_stringified() {
    let converted = engine_err(external_iceberg(
        ErrorKind::CatalogCommitConflicts,
        "metadata changed concurrently",
    ));
    assert!(
        matches!(converted, Error::Iceberg(_)),
        "expected Error::Iceberg, got {converted:?}"
    );
    let message = converted.to_string();
    assert!(
        message.starts_with("CatalogCommitConflicts"),
        "the kind must lead the message: {message}"
    );
    assert!(message.contains("metadata changed concurrently"));
    assert!(
        !message.contains("External error:"),
        "the DataFusion wrapper prefix must not misattribute the origin: {message}"
    );
}

/// U4 pin (CQ-004/CQ-015, the direct session fold): the same commit error through
/// `iceberg_err` (`create_namespace` / `table_exists` paths) classifies identically — one
/// kind→class mapping for both routes, kind visible, no "datafusion engine error:" prefix.
#[test]
fn iceberg_err_commit_conflict_keeps_kind_visible() {
    let converted = iceberg_err(iceberg_error_of(
        ErrorKind::CatalogCommitConflicts,
        "metadata changed concurrently",
    ));
    assert!(
        matches!(converted, Error::Iceberg(_)),
        "expected Error::Iceberg, got {converted:?}"
    );
    let message = converted.to_string();
    assert!(
        message.starts_with("CatalogCommitConflicts"),
        "the kind must lead the message: {message}"
    );
    assert!(
        !message.contains("datafusion engine error"),
        "the old stringify-into-DataFusion fold must stay dead: {message}"
    );
}

/// U4 pin: `FeatureUnsupported` (the fork's "iceberg feature is not supported" kind — e.g.
/// the A2-4 "Conversion from Timestamptz is not supported" class) routes to the Unsupported
/// class through BOTH routes, exactly like a DataFusion scope gate. The risk: half the
/// unsupported surface raising a different exception type than the other half.
#[test]
fn iceberg_feature_unsupported_classifies_unsupported() {
    let via_external = engine_err(external_iceberg(
        ErrorKind::FeatureUnsupported,
        "Conversion from Timestamptz is not supported",
    ));
    assert!(
        matches!(via_external, Error::NotImplemented(_)),
        "External route: expected NotImplemented, got {via_external:?}"
    );
    let via_direct = iceberg_err(iceberg_error_of(
        ErrorKind::FeatureUnsupported,
        "Conversion from Timestamptz is not supported",
    ));
    assert!(
        matches!(via_direct, Error::NotImplemented(_)),
        "direct route: expected NotImplemented, got {via_direct:?}"
    );
    assert!(via_direct.to_string().contains("FeatureUnsupported"));
}

/// U4 pin: the full 12-kind partition routes per the D-U4-2 oracle mapping (1 Unsupported /
/// 6 Analysis / 5 iceberg-base). The risk: a not-found kind silently landing in the base
/// bucket (PySpark raises `AnalysisException` — `NoSuchTableException` et al. extend it), or
/// a commit kind landing in Analysis. Each kind's name must also survive in the message.
#[test]
fn iceberg_kind_partition_routes_per_oracle() {
    let analysis_kinds = [
        ErrorKind::TableNotFound,
        ErrorKind::NamespaceNotFound,
        ErrorKind::ViewNotFound,
        ErrorKind::TableAlreadyExists,
        ErrorKind::NamespaceAlreadyExists,
        ErrorKind::ViewAlreadyExists,
    ];
    for kind in analysis_kinds {
        let converted = iceberg_err(iceberg_error_of(kind, "probe"));
        assert!(
            matches!(converted, Error::Analysis(_)),
            "{kind:?} must classify Analysis, got {converted:?}"
        );
        assert!(
            converted.to_string().contains(kind.into_static()),
            "{kind:?} must survive in the message"
        );
    }
    let base_kinds = [
        ErrorKind::PreconditionFailed,
        ErrorKind::Unexpected,
        ErrorKind::DataInvalid,
        ErrorKind::CatalogCommitConflicts,
        ErrorKind::CommitStateUnknown,
    ];
    for kind in base_kinds {
        let converted = iceberg_err(iceberg_error_of(kind, "probe"));
        assert!(
            matches!(converted, Error::Iceberg(_)),
            "{kind:?} must classify to the iceberg base bucket, got {converted:?}"
        );
    }
    assert!(matches!(
        iceberg_err(iceberg_error_of(ErrorKind::FeatureUnsupported, "probe")),
        Error::NotImplemented(_)
    ));
}

/// U4 pin: the `External` downcast NARROWS to iceberg errors — a non-iceberg external error
/// (an IO fault from an object store, say) keeps today's base classification AND today's full
/// DataFusion rendering. The risk: the new arm hijacking every external error into the
/// iceberg bucket.
#[test]
fn external_non_iceberg_error_stays_base() {
    let external = DataFusionError::External(Box::new(std::io::Error::other("disk on fire")));
    assert!(matches!(
        classify_datafusion_error(&external),
        EngineErrorKind::Other
    ));
    let converted = engine_err(external);
    assert!(
        matches!(converted, Error::DataFusion(_)),
        "expected the base DataFusion bucket, got {converted:?}"
    );
    assert!(
        converted
            .to_string()
            .contains("External error: disk on fire"),
        "non-iceberg external errors keep the full DataFusion rendering"
    );
}

/// U4 pin: wrapper peeling and the iceberg downcast COMPOSE — a `Context`-wrapped
/// `External(TableNotFound)` still classifies Analysis. The risk: DataFusion adding a context
/// wrapper during plan/execute and the kind classification silently degrading to base.
#[test]
fn classify_peels_wrapper_to_iceberg_kind() {
    let wrapped = DataFusionError::Context(
        "while resolving the MERGE target".to_string(),
        Box::new(external_iceberg(
            ErrorKind::TableNotFound,
            "Table sales.orders not found",
        )),
    );
    let converted = engine_err(wrapped);
    assert!(
        matches!(converted, Error::Analysis(_)),
        "expected Analysis, got {converted:?}"
    );
    assert!(converted.to_string().contains("TableNotFound"));
}

/// Real path: DataFusion actually emits `DataFusionError::SQL` for incomplete SQL, and the
/// session `sql` boundary classifies it as `Parse` (→ `ParseException` at the PyO3 layer).
#[tokio::test]
async fn session_sql_syntax_error_is_parse() {
    let session = ReparkSession::new().unwrap();
    let err = session.sql("SELECT * FROM").await.unwrap_err();
    assert!(
        matches!(err, Error::Parse(_)),
        "expected Parse, got {err:?}"
    );
}

/// Real path: an unresolved table is an analysis error (→ `AnalysisException`); the table name
/// survives in the message (cause chain preserved).
#[tokio::test]
async fn session_sql_unknown_table_is_analysis() {
    let session = ReparkSession::new().unwrap();
    let err = session
        .sql("SELECT * FROM __no_such_table__")
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Analysis(_)),
        "expected Analysis, got {err:?}"
    );
    assert!(
        err.to_string().contains("__no_such_table__"),
        "message must name the table: {err}"
    );
}

/// G8 (phase-2 design): the DF-54.1 uncorrelated-scalar-subquery regression guard lives in
/// core session defaults, NOT in a door extension — a bare `build()` with no extension must
/// carry it, so extension-less native sessions keep the pre-54 `ScalarSubqueryToJoin` rewrite
/// (fuzzer repros fuzz-42-1/2: the 54.1 physical path drops the query's top-level Sort).
/// Mutation-proof: hoisting the flag into an extension flips this test red.
#[tokio::test]
async fn bare_session_without_extension_carries_df_54_1_subquery_guard() {
    let session = ReparkSession::new().unwrap();
    let options = session.context().copied_config().options().clone();
    assert!(
        !options
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery,
        "a no-extension session must force the pre-54 scalar-subquery rewrite (DF-54.1 guard)"
    );
}

// === P2G R2: builder `datafusion.*` config reaches SessionConfig (design §2 Q8) ==============

/// The R2 core gap, pinned at its narrowest: a builder-set `datafusion.*` key LANDS in the
/// session's `SessionConfig`. Before the fix the builder map was repark/spark-shaped only and
/// this key was silently inert (P2F ledger, R2 spike). Bare session — no dialect, no extension —
/// so this is core plumbing, not door behavior.
#[tokio::test]
async fn builder_datafusion_config_key_reaches_session_config() {
    let session = ReparkSession::builder()
        .config("datafusion.catalog.information_schema", "true")
        .config("datafusion.execution.collect_statistics", "false")
        .build()
        .unwrap();
    let options = session.context().copied_config().options().clone();
    assert!(
        options.catalog.information_schema,
        "a builder-set `datafusion.catalog.information_schema` must reach SessionConfig"
    );
    assert!(
        !options.execution.collect_statistics,
        "a second `datafusion.*` key must land too (the whole prefixed subset is applied)"
    );
}

/// A key WITHOUT the `datafusion.` prefix is still ignored (PySpark tolerance) — the fix widens
/// the map's reach, it does not turn every unknown `.config` key into a build error.
#[tokio::test]
async fn builder_non_datafusion_config_keys_stay_ignored() {
    let session = ReparkSession::builder()
        .config("spark.sql.some.unknown.knob", "whatever")
        .config("repark.not.a.real.key", "1")
        .build()
        .unwrap();
    assert!(
        !session
            .context()
            .copied_config()
            .options()
            .catalog
            .information_schema,
        "unprefixed keys must not touch DataFusion options (information_schema stays off)"
    );
}

/// A MISSPELLED `datafusion.*` key fails the build loud, naming the key — a silently-inert conf
/// key is the exact defect the R2 fix removes, so the fix must not reintroduce it one typo over.
#[tokio::test]
async fn builder_unknown_datafusion_config_key_fails_loud() {
    let error = ReparkSession::builder()
        .config("datafusion.catalog.information_schemaa", "true")
        .build()
        .unwrap_err();
    assert!(
        matches!(error, Error::Config(_)),
        "expected Error::Config, got {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("datafusion.catalog.information_schemaa"),
        "the message must name the offending key: {message}"
    );
}

/// An explicit `datafusion.*` conf wins over the core default it addresses — the keys are applied
/// AFTER the defaults. Pinned on the G8 DF-54.1 guard because that is the one core default a user
/// might knowingly re-enable; `bare_session_without_extension_carries_df_54_1_subquery_guard`
/// still pins the unset case, so the two together fix the ordering.
#[tokio::test]
async fn explicit_datafusion_config_overrides_a_core_default() {
    let session = ReparkSession::builder()
        .config(
            "datafusion.optimizer.enable_physical_uncorrelated_scalar_subquery",
            "true",
        )
        .build()
        .unwrap();
    assert!(
        session
            .context()
            .copied_config()
            .options()
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery,
        "an explicit datafusion.* conf must be applied after (and therefore over) the default"
    );
}

/// Q8 delivery, core half: with `information_schema` enabled through the BUILDER, a registered
/// Iceberg catalog enumerates through stock DataFusion — `SHOW TABLES` and `DESCRIBE` plan and
/// execute, and `information_schema.tables` lists the created table. This is the enumeration
/// verification design §2 Q8 asks for, run on the PRODUCT path (`ReparkSession`), not a raw
/// `SessionContext`. AWS-free (memory catalog over a temp warehouse).
#[tokio::test]
async fn information_schema_enumerates_a_registered_iceberg_catalog_through_the_session() {
    use tempfile::TempDir;

    let warehouse = TempDir::new().unwrap();
    let session = ReparkSession::builder()
        .config("datafusion.catalog.information_schema", "true")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session
        .create_namespace("ice", "sales", HashMap::new())
        .await
        .unwrap();
    // Create through the Catalog API, not SQL: repark-core has no CTAS door (that is a door
    // crate's job), so the table is created the way core itself can, then the provider is
    // rebuilt so the name directory is current.
    session
        .testing_oob_create_table("ice", "sales", "orders", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();

    // Namespace enumeration.
    let schemata = session
        .sql(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE catalog_name = 'ice' AND schema_name = 'sales'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        schemata.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "the registered catalog's namespace must enumerate in information_schema.schemata"
    );

    // Table enumeration, on the Arrow path (value AND type).
    let tables = session
        .sql(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = 'ice' AND table_schema = 'sales' AND table_name = 'orders'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        tables[0].column(0).data_type(),
        &DataType::Utf8,
        "information_schema.tables.table_name is Utf8"
    );
    let names = tables[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "orders");

    // Stock `SHOW TABLES` + `DESCRIBE` now PLAN (they refused outright before the fix).
    let shown = session.sql("SHOW TABLES").await.unwrap().collect().await;
    assert!(shown.is_ok(), "SHOW TABLES must work: {:?}", shown.err());
    let described = session
        .sql("DESCRIBE ice.sales.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        described.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "DESCRIBE must report the created table's single column"
    );
}

/// **Pin flipped on purpose (2026-08-10, ADR-0006 / campaign decision D2, unit H-1c).** This row
/// used to assert the opposite — that the fork's `$`-suffixed metadata tables enumerate alongside
/// the real table — and it was named `information_schema_still_exposes_the_dollar_metadata_tables`.
/// The P2F R2 spike's "product question" is closed: `repark_iceberg::catalog`'s
/// `MetadataProjectionSchemaProvider::table_names` drops the names the fork SYNTHESIZES, so the
/// listing is the catalog's tables. Rationale + rejected alternative:
/// `docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md`.
///
/// This is the **bare core session** half of the claim — no door, no facade — which is what makes
/// the decision attributable to the catalog layer rather than to a SQL front end. The companion
/// half (hidden, not removed) is asserted below.
///
/// Mutation: drop the `.filter(…)` in `MetadataProjectionSchemaProvider::table_names` → this reds.
#[tokio::test]
async fn information_schema_hides_the_dollar_metadata_tables_on_the_bare_session() {
    use tempfile::TempDir;

    let warehouse = TempDir::new().unwrap();
    let session = ReparkSession::builder()
        .config("datafusion.catalog.information_schema", "true")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session
        .create_namespace("ice", "sales", HashMap::new())
        .await
        .unwrap();
    session
        .testing_oob_create_table("ice", "sales", "orders", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();

    let rows = session
        .sql(
            "SELECT count(*) AS n FROM information_schema.tables \
             WHERE table_catalog = 'ice' AND table_schema = 'sales' \
             AND table_name LIKE 'orders$%'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let counts = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(
        counts.value(0),
        0,
        "metadata tables must not enumerate (ADR-0006); got {}",
        counts.value(0)
    );

    // The real table is still there — the filter must hide the synthesized names, not the listing.
    let real = session
        .sql(
            "SELECT count(*) AS n FROM information_schema.tables \
             WHERE table_catalog = 'ice' AND table_schema = 'sales' AND table_name = 'orders'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        real[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1,
        "the base table must still enumerate"
    );
}

/// The other half of ADR-0006, on the same bare core session: hidden from the LISTING is not
/// removed from the ENGINE. `t$snapshots` still resolves by name and still executes.
///
/// Risk pinned: a filter written into `SchemaProvider::table` / `table_exist` instead of
/// `table_names` would hide the name AND break every query that references it — including the
/// Spark door's `t.snapshots` rewrite, which lands on exactly this name.
///
/// Mutation: move the filter from `table_names` into `table` → this reds while the row above
/// stays green, which is precisely why both halves are pinned.
#[tokio::test]
async fn a_hidden_metadata_table_is_still_queryable_on_the_bare_session() {
    use tempfile::TempDir;

    let warehouse = TempDir::new().unwrap();
    let session = ReparkSession::builder()
        .config("datafusion.catalog.information_schema", "true")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session
        .create_namespace("ice", "sales", HashMap::new())
        .await
        .unwrap();
    session
        .testing_oob_create_table("ice", "sales", "orders", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();

    // A freshly created table has committed no snapshot, so the exact expected count is 0 — the
    // claim is that the name RESOLVES and EXECUTES, and an exact count says so without a
    // tautology.
    let rows = session
        .sql("SELECT count(*) AS n FROM ice.sales.\"orders$snapshots\"")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        0,
        "the hidden metadata table must still resolve and execute (an OOB-created table has \
         committed no snapshot)"
    );
}

/// The negative half of the row above: WITHOUT the conf, `SHOW TABLES` still refuses with
/// DataFusion's own message. This is what makes the Q8 delivery attributable to the builder key
/// rather than to a defaulted-on `information_schema`.
#[tokio::test]
async fn show_tables_still_refuses_without_the_information_schema_conf() {
    let session = ReparkSession::new().unwrap();
    let error = session.sql("SHOW TABLES").await.unwrap_err();
    assert!(
        error.to_string().contains("information_schema"),
        "the refusal must name the conf that enables it: {error}"
    );
}

/// B-1 (p3e ledger): the repark-owned pseudo-key `datafusion.runtime.memory_limit` shares the
/// `datafusion.` prefix but is NOT a DataFusion `ConfigOptions` key — at the port pin it is the
/// facade's LIVE resize knob, applied after build. The build-time sweep must skip it (a builder
/// carrying it must construct), and it must stay in the kept config map for downstream readers.
#[tokio::test]
async fn builder_pseudo_key_datafusion_runtime_memory_limit_builds() {
    ReparkSession::builder()
        .config("datafusion.runtime.memory_limit", "256M")
        .build()
        .expect("the repark-owned pseudo-key must not be swept into ConfigOptions at build");
}

/// The exclusion is EXACT-KEY: a typo of the pseudo-key is an unknown DataFusion key and must
/// still fail loud — prefix-scoped exclusion would silently re-create the inert-conf defect
/// this sweep exists to fix. Two fixtures discriminate the two wrong implementations: the
/// truncated form catches a namespace-prefix exclusion, and the EXTENDED form (the pseudo-key
/// plus a suffix) catches a `starts_with(pseudo_key)` exclusion — either wrong shape lets one
/// of these build silently.
#[tokio::test]
async fn builder_pseudo_key_typo_still_fails_loud() {
    for typo in [
        "datafusion.runtime.memory_limt",
        "datafusion.runtime.memory_limit2",
    ] {
        let error = ReparkSession::builder()
            .config(typo, "256M")
            .build()
            .expect_err("a typo of the pseudo-key is an unknown DataFusion key: loud, never inert");
        let message = error.to_string();
        assert!(
            message.contains(typo),
            "the refusal must name the offending key: {message}"
        );
    }
}
