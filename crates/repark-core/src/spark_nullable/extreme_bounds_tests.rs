use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;

use super::extreme_bounds::{has_type_extreme_bound, without_type_extreme_bounds};
use crate::session::ReparkSession;

type Layout<'a> = &'a [(&'a str, &'a [Option<i64>], usize)];

fn column(min: ScalarValue, max: ScalarValue) -> ColumnStatistics {
    ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max))
        .with_null_count(Precision::Exact(1))
}

#[test]
fn only_a_bound_at_the_type_extreme_is_dropped() {
    let statistics = Statistics {
        num_rows: Precision::Exact(5),
        total_byte_size: Precision::Absent,
        column_statistics: vec![
            column(
                ScalarValue::Int64(Some(i64::MIN)),
                ScalarValue::Int64(Some(i64::MAX)),
            ),
            column(
                ScalarValue::Int64(Some(i64::MIN + 1)),
                ScalarValue::Int64(Some(i64::MAX - 1)),
            ),
            column(
                ScalarValue::Int32(Some(i32::MIN)),
                ScalarValue::Int32(Some(i32::MAX)),
            ),
            column(
                ScalarValue::TimestampMicrosecond(Some(i64::MIN), None),
                ScalarValue::TimestampMicrosecond(Some(7), None),
            ),
            column(
                ScalarValue::UInt64(Some(3)),
                ScalarValue::UInt64(Some(u64::MAX)),
            ),
        ],
    };
    assert!(has_type_extreme_bound(&statistics));
    let kept = without_type_extreme_bounds(statistics.clone());
    assert!(!has_type_extreme_bound(&kept));
    assert_eq!(kept.num_rows, statistics.num_rows);
    let bounds = kept
        .column_statistics
        .iter()
        .map(|column| (column.min_value.clone(), column.max_value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(bounds[0], (Precision::Absent, Precision::Absent));
    assert_eq!(
        bounds[1],
        (
            statistics.column_statistics[1].min_value.clone(),
            statistics.column_statistics[1].max_value.clone()
        )
    );
    assert_eq!(
        bounds[2],
        (
            statistics.column_statistics[2].min_value.clone(),
            statistics.column_statistics[2].max_value.clone()
        )
    );
    assert_eq!(
        bounds[3],
        (
            Precision::Absent,
            Precision::Exact(ScalarValue::TimestampMicrosecond(Some(7), None))
        )
    );
    assert_eq!(
        bounds[4],
        (
            Precision::Exact(ScalarValue::UInt64(Some(3))),
            Precision::Absent
        )
    );
    assert!(
        kept.column_statistics
            .iter()
            .all(|column| column.null_count == Precision::Exact(1))
    );
}

fn write_file(dir: &Path, name: &str, values: &[Option<i64>], row_group_rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int32, true),
        Field::new("x", DataType::Int64, true),
    ]));
    let count = i32::try_from(values.len()).expect("a small fixture");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..count)),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("the batch");
    let file = std::fs::File::create(dir.join(name)).expect("create the file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).expect("open the writer");
    writer.write(&batch).expect("write the batch");
    writer.close().expect("close the writer");
}

async fn x_values(session: &ReparkSession, sql: &str) -> Vec<i64> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an int64 column");
        values.extend(
            (0..column.len())
                .filter(|&row| column.is_valid(row))
                .map(|row| column.value(row)),
        );
    }
    values.sort_unstable();
    values
}

async fn external_table(session: &ReparkSession, dir: &Path) {
    let location = format!("{}/", dir.to_str().expect("a utf-8 path"));
    session
        .sql(&format!(
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '{location}'"
        ))
        .await
        .expect("plan the table")
        .collect()
        .await
        .expect("create the table");
}

#[tokio::test]
async fn full_range_bigint_files_answer_range_and_equality_filters() {
    let full = [Some(0), Some(i64::MAX), Some(i64::MIN), Some(5), None];
    let layouts: [Layout; 3] = [
        &[("part-0.parquet", &full, 8)],
        &[("part-0.parquet", &full, 2)],
        &[
            ("part-0.parquet", &[Some(i64::MIN), Some(-1)], 8),
            ("part-1.parquet", &[Some(0), Some(5), Some(i64::MAX)], 8),
        ],
    ];
    for layout in layouts {
        let dir = tempfile::tempdir().expect("a temp dir");
        for (name, values, row_group_rows) in layout {
            write_file(dir.path(), name, values, *row_group_rows);
        }
        let session = ReparkSession::new().expect("a session");
        external_table(&session, dir.path()).await;
        assert_eq!(
            x_values(&session, "SELECT x FROM t WHERE x > 0").await,
            [5, i64::MAX]
        );
        assert_eq!(x_values(&session, "SELECT x FROM t WHERE x = 5").await, [5]);
        assert_eq!(
            x_values(&session, "SELECT CAST(rid AS BIGINT) FROM t WHERE rid >= 0")
                .await
                .len(),
            layout
                .iter()
                .map(|(_, values, _)| values.len())
                .sum::<usize>()
        );
        assert_eq!(x_values(&session, "SELECT max(x) FROM t").await, [i64::MAX]);
        assert_eq!(x_values(&session, "SELECT min(x) FROM t").await, [i64::MIN]);
    }
}
