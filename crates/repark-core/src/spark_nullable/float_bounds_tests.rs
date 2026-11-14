use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use super::FloatBoundsInexactFormat;
use crate::session::ReparkSession;

#[derive(Clone, Copy)]
struct Floats {
    wide: [f64; 4],
    narrow: [f32; 4],
}

const CONSTANT: Floats = Floats {
    wide: [f64::NAN, 1.0, f64::NAN, 1.0],
    narrow: [f32::NAN, 1.0, f32::NAN, 1.0],
};
const SPREAD: Floats = Floats {
    wide: [f64::NAN, 0.5, f64::NAN, 1.0],
    narrow: [f32::NAN, 0.5, f32::NAN, 1.0],
};

fn float_batch(values: Floats) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![20, 21, 22, 23])),
            Arc::new(Float64Array::from(values.wide.to_vec())),
            Arc::new(Float32Array::from(values.narrow.to_vec())),
            Arc::new(StringArray::from(vec!["a", "a", "a", "a"])),
        ],
    )
    .expect("the float batch is well formed")
}

fn write_float_file(dir: &Path, values: Floats, row_group_rows: usize) -> std::path::PathBuf {
    let batch = float_batch(values);
    let path = dir.join("part-0.parquet");
    let file = std::fs::File::create(&path).expect("create the parquet file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .expect("open the parquet writer");
    writer.write(&batch).expect("write the batch");
    writer.close().expect("close the parquet writer");
    path
}

async fn file_meta(store: &Arc<dyn ObjectStore>, path: &Path) -> ObjectMeta {
    let location = ObjectPath::from_filesystem_path(path).expect("a local object path");
    store.head(&location).await.expect("head the parquet file")
}

async fn stats_pair(values: Floats, row_group_rows: usize) -> (Statistics, Statistics, Statistics) {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = write_float_file(dir.path(), values, row_group_rows);
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let meta = file_meta(&store, &path).await;
    let context = SessionContext::new();
    let state = context.state();
    let inner: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let schema = inner
        .infer_schema(&state, &store, std::slice::from_ref(&meta))
        .await
        .expect("infer the schema");
    let wrapped = FloatBoundsInexactFormat::new(Arc::clone(&inner));
    let plain = inner
        .infer_stats(&state, &store, Arc::clone(&schema), &meta)
        .await
        .expect("plain statistics");
    let demoted = wrapped
        .infer_stats(&state, &store, Arc::clone(&schema), &meta)
        .await
        .expect("wrapped statistics");
    let combined = wrapped
        .infer_stats_and_ordering(&state, &store, schema, &meta)
        .await
        .expect("wrapped statistics and ordering")
        .statistics;
    (plain, demoted, combined)
}

fn assert_only_float_bounds_inexact(plain: &Statistics, wrapped: &Statistics) {
    assert_eq!(wrapped.num_rows, plain.num_rows);
    assert_eq!(wrapped.num_rows, Precision::Exact(4));
    assert_eq!(wrapped.total_byte_size, plain.total_byte_size);
    assert_eq!(wrapped.column_statistics.len(), 4);
    for (index, (before, after)) in plain
        .column_statistics
        .iter()
        .zip(&wrapped.column_statistics)
        .enumerate()
    {
        assert_eq!(after.null_count, before.null_count, "column {index}");
        assert_eq!(
            after.distinct_count, before.distinct_count,
            "column {index}"
        );
        assert_eq!(after.sum_value, before.sum_value, "column {index}");
        if index == 1 || index == 2 {
            assert!(
                matches!(before.min_value, Precision::Exact(_)),
                "column {index}"
            );
            assert_eq!(after.min_value, before.min_value.clone().to_inexact());
            assert_eq!(after.max_value, before.max_value.clone().to_inexact());
            assert!(
                matches!(after.max_value, Precision::Inexact(_)),
                "column {index}"
            );
        } else {
            assert_eq!(after.min_value, before.min_value, "column {index}");
            assert_eq!(after.max_value, before.max_value, "column {index}");
        }
    }
    assert!(matches!(
        wrapped.column_statistics[0].max_value,
        Precision::Exact(_)
    ));
}

#[tokio::test]
async fn constant_float_bounds_turn_inexact_and_every_other_statistic_is_unchanged() {
    for row_group_rows in [4, 2] {
        let (plain, demoted, combined) = stats_pair(CONSTANT, row_group_rows).await;
        assert_only_float_bounds_inexact(&plain, &demoted);
        assert_only_float_bounds_inexact(&plain, &combined);
    }
}

#[tokio::test]
async fn spread_float_bounds_stay_exact_for_file_pruning() {
    for row_group_rows in [4, 2] {
        let (plain, demoted, combined) = stats_pair(SPREAD, row_group_rows).await;
        assert_eq!(demoted, plain);
        assert_eq!(combined, plain);
        assert!(matches!(
            plain.column_statistics[1].max_value,
            Precision::Exact(_)
        ));
    }
}

async fn first_column_floats(session: &ReparkSession, query: &str) -> Vec<f64> {
    let batches = session
        .sql(query)
        .await
        .expect("plan the query")
        .collect()
        .await
        .expect("run the query");
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
            out.extend(values.iter().map(|v| v.unwrap_or(f64::INFINITY)));
        } else if let Some(values) = column.as_any().downcast_ref::<Float32Array>() {
            out.extend(values.iter().map(|v| f64::from(v.unwrap_or(f32::INFINITY))));
        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            out.extend(values.iter().map(|v| {
                v.and_then(|n| i32::try_from(n).ok())
                    .map_or(f64::INFINITY, f64::from)
            }));
        }
    }
    out
}

fn same_floats(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| (a.is_nan() && b.is_nan()) || (a - b).abs() < f64::EPSILON)
}

async fn assert_view_keeps_nan(session: &ReparkSession, view: &str, values: Floats) {
    let lowest = values.wide[1].min(values.wide[3]);
    for column in ["d", "f"] {
        let rows =
            first_column_floats(session, &format!("SELECT {column} FROM {view} ORDER BY id")).await;
        assert!(
            same_floats(&rows, &values.wide),
            "{view}.{column} rows: {rows:?}"
        );
        let top = first_column_floats(session, &format!("SELECT max({column}) FROM {view}")).await;
        assert!(
            top.len() == 1 && top[0].is_nan(),
            "{view} max({column}): {top:?}"
        );
        let bottom =
            first_column_floats(session, &format!("SELECT min({column}) FROM {view}")).await;
        assert!(
            same_floats(&bottom, &[lowest]),
            "{view} min({column}): {bottom:?}"
        );
        let nans = first_column_floats(
            session,
            &format!("SELECT count(*) FROM {view} WHERE isnan({column})"),
        )
        .await;
        assert_eq!(nans, vec![2.0], "{view} isnan({column})");
    }
}

#[tokio::test]
async fn read_parquet_keeps_every_nan_and_reads_max_from_the_column() {
    for (values, row_group_rows) in [(CONSTANT, 4), (CONSTANT, 2), (SPREAD, 4), (SPREAD, 2)] {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_float_file(dir.path(), values, row_group_rows);
        let session = ReparkSession::new().expect("a session");
        let frame = session
            .read_parquet(dir.path().to_str().expect("a utf-8 path"))
            .await
            .expect("read the float file");
        session
            .context()
            .register_table("float_read", frame.into_view())
            .expect("register the view");
        assert_view_keeps_nan(&session, "float_read", values).await;
    }
}

#[tokio::test]
async fn create_external_table_keeps_every_nan_and_reads_max_from_the_column() {
    for values in [CONSTANT, SPREAD] {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_float_file(dir.path(), values, 4);
        let session = ReparkSession::new().expect("a session");
        let location = format!("{}/", dir.path().to_str().expect("a utf-8 path"));
        session
            .sql(&format!(
                "CREATE EXTERNAL TABLE float_ext STORED AS PARQUET LOCATION '{location}'"
            ))
            .await
            .expect("plan the external table")
            .collect()
            .await
            .expect("create the external table");
        assert_view_keeps_nan(&session, "float_ext", values).await;
    }
}
