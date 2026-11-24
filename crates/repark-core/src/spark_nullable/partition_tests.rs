use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{col, lit};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::ReparkSession;
use crate::plan_introspect::plan_reads_s3_prefix;
use crate::spark_nullable::PARTITION_COLUMN_TYPE_INFERENCE_KEY;

fn write_parquet_file(path: &Path, batch: &RecordBatch) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn id_s_batch(id: i64, text: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![text])),
        ],
    )
    .unwrap()
}

fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    buffer
}

#[tokio::test]
async fn one_level_and_two_level_discover_types_and_prune_files() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet_file(
        &dir.path().join("one/p=1/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("one/p=2/part-0.parquet"),
        &id_s_batch(2, "b"),
    );
    let session = ReparkSession::new().unwrap();
    let root = dir.path().join("one");
    let frame = session.read_parquet(root.to_str().unwrap()).await.unwrap();
    let field_pairs: Vec<(String, DataType)> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();
    assert_eq!(
        field_pairs,
        vec![
            ("id".to_string(), DataType::Int64),
            ("s".to_string(), DataType::Utf8View),
            ("p".to_string(), DataType::Int32),
        ]
    );
    let filtered = frame.filter(col("p").eq(lit(2i32))).unwrap();
    let plan = filtered.clone().create_physical_plan().await.unwrap();
    let rendered = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(rendered.contains("p=2/part-0.parquet"), "{rendered}");
    assert!(!rendered.contains("p=1/part-0.parquet"), "{rendered}");
    let batches = filtered.collect().await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);

    let empty = session
        .read_parquet(root.to_str().unwrap())
        .await
        .unwrap()
        .filter(col("p").eq(lit(99i32)))
        .unwrap();
    let empty_batches = empty.collect().await.unwrap();
    let empty_rows: usize = empty_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(empty_rows, 0);

    let only_p = session
        .read_parquet(root.to_str().unwrap())
        .await
        .unwrap()
        .select_columns(&["p"])
        .unwrap();
    let mut vals: Vec<i32> = Vec::new();
    for batch in only_p.collect().await.unwrap() {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..array.len() {
            vals.push(array.value(row));
        }
    }
    vals.sort_unstable();
    assert_eq!(vals, vec![1, 2]);

    let leaf = root.join("p=1");
    let leaf_frame = session.read_parquet(leaf.to_str().unwrap()).await.unwrap();
    let leaf_names: Vec<&str> = leaf_frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(leaf_names, vec!["id", "s"]);

    write_parquet_file(
        &dir.path().join("two/year=2024/month=1/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("two/year=2025/month=2/part-0.parquet"),
        &id_s_batch(2, "b"),
    );
    let two_frame = session
        .read_parquet(dir.path().join("two").to_str().unwrap())
        .await
        .unwrap();
    let two_names: Vec<&str> = two_frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(two_names, vec!["id", "s", "year", "month"]);
}

#[tokio::test]
async fn inference_ladder_timestamps_decimals_and_infer_off() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet_file(
        &dir.path()
            .join("dec_only/p=123456789012345678901234567890/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path()
            .join("dec_mix/p=123456789012345678901234567890/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("dec_mix/p=1.23/part-1.parquet"),
        &id_s_batch(2, "b"),
    );
    write_parquet_file(
        &dir.path().join("double_dec/p=1.50/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("lead_zero/p=01/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path()
            .join("ts_frac/p=2024-01-02 03:04:05.5/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path()
            .join("ts_frac2/p=2024-01-02 03:04:05.12/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path()
            .join("hive_only/p=__HIVE_DEFAULT_PARTITION__/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("empty_val/p=/part-0.parquet"),
        &id_s_batch(1, "a"),
    );

    let session = ReparkSession::builder()
        .config("spark.sql.session.timeZone", "America/New_York")
        .build()
        .unwrap();

    let dec_only = session
        .read_parquet(dir.path().join("dec_only").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        dec_only.schema().field(2).data_type(),
        &DataType::Decimal128(30, 0)
    );
    let dec_batches = dec_only.collect().await.unwrap();
    let dec_col = dec_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        dec_col.value(0),
        123_456_789_012_345_678_901_234_567_890_i128
    );

    let dec_mix = session
        .read_parquet(dir.path().join("dec_mix").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(dec_mix.schema().field(2).data_type(), &DataType::Utf8);

    let dbl = session
        .read_parquet(dir.path().join("double_dec").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(dbl.schema().field(2).data_type(), &DataType::Float64);
    let dbl_batches = dbl.collect().await.unwrap();
    let dbl_col = dbl_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((dbl_col.value(0) - 1.5).abs() < 1e-12);

    let lz = session
        .read_parquet(dir.path().join("lead_zero").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(lz.schema().field(2).data_type(), &DataType::Int32);

    let ts_frac = session
        .read_parquet(dir.path().join("ts_frac").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        ts_frac.schema().field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
    );
    let ts_batches = ts_frac.collect().await.unwrap();
    let ts_col = ts_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts_col.value(0), 1_704_182_645_500_000);

    let ts_frac2 = session
        .read_parquet(dir.path().join("ts_frac2").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(ts_frac2.schema().field(2).data_type(), &DataType::Utf8);

    let hive_only = session
        .read_parquet(dir.path().join("hive_only").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(hive_only.schema().field(2).data_type(), &DataType::Null);

    let empty_err = session
        .read_parquet(dir.path().join("empty_val").to_str().unwrap())
        .await
        .unwrap_err();
    assert!(empty_err.to_string().contains("Empty partition value"));

    session
        .set_partition_column_type_inference("false", true)
        .unwrap();
    let lz_off = session
        .read_parquet(dir.path().join("lead_zero").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(lz_off.schema().field(2).data_type(), &DataType::Utf8);
    let lz_batches = lz_off.collect().await.unwrap();
    let lz_col = lz_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(lz_col.value(0), "01");

    let hive_off = session
        .read_parquet(dir.path().join("hive_only").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(hive_off.schema().field(2).data_type(), &DataType::Null);

    let bad = session
        .set_partition_column_type_inference("maybe", true)
        .unwrap_err();
    assert!(
        bad.to_string()
            .contains(PARTITION_COLUMN_TYPE_INFERENCE_KEY)
    );
}

#[tokio::test]
async fn overlap_case_sensitivity_hidden_files_and_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let with_p = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["orig"])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    write_parquet_file(&dir.path().join("overlap/P=10/part-0.parquet"), &with_p);

    let only_p_schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Int32, true)]));
    let only_p = RecordBatch::try_new(
        only_p_schema,
        vec![Arc::new(Int32Array::from(vec![99i32, 99i32]))],
    )
    .unwrap();
    write_parquet_file(&dir.path().join("shadow_all/p=7/part-0.parquet"), &only_p);

    let session = ReparkSession::new().unwrap();

    let insens = session
        .read_parquet_with_case_sensitivity(dir.path().join("overlap").to_str().unwrap(), false)
        .await
        .unwrap();
    let insens_fields: Vec<(&str, &DataType)> = insens
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type()))
        .collect();
    assert_eq!(
        insens_fields,
        vec![
            ("id", &DataType::Int64),
            ("P", &DataType::Int32),
            ("s", &DataType::Utf8View),
        ]
    );
    let batches = insens.collect().await.unwrap();
    let p_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(p_col.value(0), 10);

    let sens = session
        .read_parquet_with_case_sensitivity(dir.path().join("overlap").to_str().unwrap(), true)
        .await
        .unwrap();
    let sens_names: Vec<&str> = sens
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(sens_names, vec!["id", "p", "s", "P"]);

    let shadowed = session
        .read_parquet(dir.path().join("shadow_all").to_str().unwrap())
        .await
        .unwrap();
    let shadow_batches = shadowed.collect().await.unwrap();
    let s_col = shadow_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(s_col.len(), 2);
    assert_eq!(s_col.value(0), 7);
    assert_eq!(s_col.value(1), 7);

    write_parquet_file(
        &dir.path().join("noise/p=1/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("noise/_temporary/0/part-tmp.parquet"),
        &id_s_batch(9, "tmp"),
    );
    write_parquet_file(
        &dir.path().join("noise/.hidden.parquet"),
        &id_s_batch(8, "hid"),
    );
    write_parquet_file(
        &dir.path().join("noise/p=1/.dot.parquet"),
        &id_s_batch(7, "dot"),
    );
    write_parquet_file(
        &dir.path().join("noise/p=1/_under.parquet"),
        &id_s_batch(6, "under"),
    );
    std::fs::write(dir.path().join("noise/_SUCCESS"), b"").unwrap();
    let clean = session
        .read_parquet(dir.path().join("noise").to_str().unwrap())
        .await
        .unwrap();
    let clean_rows: usize = clean
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(clean_rows, 1);

    write_parquet_file(
        &dir.path().join("mixed_case/p=1/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("mixed_case/P=2/part-1.parquet"),
        &id_s_batch(2, "b"),
    );
    let ok_mixed = session
        .read_parquet_with_case_sensitivity(dir.path().join("mixed_case").to_str().unwrap(), false)
        .await
        .unwrap();
    assert_eq!(ok_mixed.schema().field(2).name(), "p");
    let err_mixed = session
        .read_parquet_with_case_sensitivity(dir.path().join("mixed_case").to_str().unwrap(), true)
        .await
        .unwrap_err();
    assert!(
        err_mixed
            .to_string()
            .starts_with("[CONFLICTING_PARTITION_COLUMN_NAMES]")
    );
}

#[tokio::test]
async fn s3_in_memory_partitioned_parquet_shares_discovery_and_introspection() {
    let session = ReparkSession::new().unwrap();
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test("part-bucket", &store)
        .unwrap();
    let b1 = parquet_bytes(&id_s_batch(1, "a"));
    let b2 = parquet_bytes(&id_s_batch(2, "b"));
    memory
        .put(
            &ObjectPath::from("tbl/p=1/part-0.parquet"),
            PutPayload::from(b1),
        )
        .await
        .unwrap();
    memory
        .put(
            &ObjectPath::from("tbl/p=2/part-0.parquet"),
            PutPayload::from(b2),
        )
        .await
        .unwrap();
    let frame = session.read_parquet("s3://part-bucket/tbl").await.unwrap();
    assert!(plan_reads_s3_prefix(
        frame.logical_plan(),
        "part-bucket",
        "tbl"
    ));
    let names: Vec<&str> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, vec!["id", "s", "p"]);
    let rows: usize = frame
        .filter(col("p").eq(lit(2i32)))
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn spark_numeric_and_uri_edge_rules() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet_file(
        &dir.path().join("nan_inf/p=NaN/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("nan_inf/p=-Infinity/part-0.parquet"),
        &id_s_batch(2, "b"),
    );
    write_parquet_file(
        &dir.path().join("nan_lower/p=nan/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("sci_dec/p=1e5/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("sci_mix/p=1e5/part-0.parquet"),
        &id_s_batch(1, "a"),
    );
    write_parquet_file(
        &dir.path().join("sci_mix/p=2.5e-2/part-0.parquet"),
        &id_s_batch(2, "b"),
    );
    write_parquet_file(
        &dir.path().join("empty_key/=1/part-0.parquet"),
        &id_s_batch(1, "a"),
    );

    let session = ReparkSession::new().unwrap();
    let nan_df = session
        .read_parquet(dir.path().join("nan_inf").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(nan_df.schema().field(2).data_type(), &DataType::Float64);

    let nan_lower = session
        .read_parquet(dir.path().join("nan_lower").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(nan_lower.schema().field(2).data_type(), &DataType::Utf8);

    let sci_dec = session
        .read_parquet(dir.path().join("sci_dec").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        sci_dec.schema().field(2).data_type(),
        &DataType::Decimal128(6, 0)
    );

    let sci_mix = session
        .read_parquet(dir.path().join("sci_mix").to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(sci_mix.schema().field(2).data_type(), &DataType::Utf8);

    assert!(
        session
            .read_parquet(dir.path().join("empty_key").to_str().unwrap())
            .await
            .is_err()
    );

    let uri_no_slash = format!("file://{}", dir.path().join("sci_dec").to_str().unwrap());
    let uri_df = session.read_parquet(&uri_no_slash).await.unwrap();
    assert_eq!(
        uri_df.schema().field(2).data_type(),
        &DataType::Decimal128(6, 0)
    );
}
