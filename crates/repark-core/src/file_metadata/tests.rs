use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, TableScan, col};
use datafusion::prelude::{DataFrame, SessionContext};

use super::ensure::{hidden_field_or_reject, rewrite_metadata_refs};
use super::scan::FileMetadataScan;
use super::udf::metadata_outer_field;
use super::*;

fn memory_scan(names: &[(&str, DataType, bool)]) -> TableScan {
    let fields = names
        .iter()
        .map(|(name, data_type, nullable)| Field::new(*name, data_type.clone(), *nullable))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let columns = names
        .iter()
        .map(|(_, data_type, _)| match data_type {
            DataType::Int32 => Arc::new(Int32Array::from(vec![1])) as _,
            _ => Arc::new(StringArray::from(vec!["a"])) as _,
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("test batch builds");
    let provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable builds"));
    TableScan::try_new("test", provider_as_source(provider), None, vec![], None)
        .expect("test scan builds")
}

fn marked_scan(names: &[(&str, DataType, bool)]) -> TableScan {
    let scan = memory_scan(names);
    let provider = source_as_provider(&scan.source).expect("default source");
    let marker = provider_as_source(Arc::new(FileMetadataScan::new(provider, FileKind::Parquet)));
    TableScan::try_new(
        scan.table_name.clone(),
        marker,
        scan.projection.clone(),
        scan.filters.clone(),
        scan.fetch,
    )
    .expect("marked scan builds")
}

fn plan_of(scan: TableScan) -> LogicalPlan {
    LogicalPlan::TableScan(scan)
}

#[test]
fn udf_outer_field_is_non_nullable_with_metadata_keys() {
    for with_row_index in [true, false] {
        let field = metadata_outer_field(with_row_index);
        assert_eq!(field.name(), METADATA_COLUMN_NAME);
        assert!(!field.is_nullable());
        assert_eq!(
            field
                .metadata()
                .get(FILE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            field.metadata().get(METADATA_COL_KEY).map(String::as_str),
            Some(METADATA_COLUMN_NAME)
        );
        let DataType::Struct(children) = field.data_type() else {
            panic!("metadata outer field is a struct");
        };
        assert!(children.iter().all(|child| !child.is_nullable()));
        assert_eq!(
            children.iter().any(|child| child.name() == "row_index"),
            with_row_index
        );
    }
}

#[test]
fn status_reports_available_shadowed_and_absent() {
    let plain = plan_of(memory_scan(&[
        ("i", DataType::Int32, true),
        ("s", DataType::Utf8, true),
    ]));
    assert_eq!(file_metadata_status(&plain), FileMetadataStatus::Absent);
    let marked = plan_of(marked_scan(&[
        ("i", DataType::Int32, true),
        ("s", DataType::Utf8, true),
    ]));
    assert_eq!(file_metadata_status(&marked), FileMetadataStatus::Available);
    let shadowed = plan_of(marked_scan(&[
        ("i", DataType::Int32, true),
        ("_metadata", DataType::Utf8, true),
    ]));
    assert_eq!(
        file_metadata_status(&shadowed),
        FileMetadataStatus::Shadowed
    );
}

#[test]
fn status_survives_transparent_projection() {
    let marked = plan_of(marked_scan(&[("i", DataType::Int32, true)]));
    let projected = LogicalPlanBuilder::from(marked)
        .project(vec![col("i")])
        .expect("projection builds")
        .build()
        .expect("plan builds");
    assert_eq!(
        file_metadata_status(&projected),
        FileMetadataStatus::Available
    );
}

#[tokio::test]
async fn metadata_ref_across_join_is_missing_not_unresolved() {
    let context = SessionContext::new();
    let left_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("batch builds");
    let right_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .expect("batch builds");
    let left = context.read_batch(left_batch).expect("left frame builds");
    let right = context.read_batch(right_batch).expect("right frame builds");
    let marked = mark_file_scan(left, FileKind::Parquet).expect("mark wraps");
    let joined = marked
        .join(right, JoinType::Inner, &["i"], &["k"], None)
        .expect("join builds");
    let (state, plan) = joined.into_parts();
    assert_eq!(file_metadata_status(&plan), FileMetadataStatus::Absent);
    let failure = ensure_file_metadata(&state, plan, vec![col("_metadata")]).await;
    let error = failure.expect_err("join drops the metadata attribute");
    assert_eq!(
        error.error_class,
        Some("MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT")
    );
}

#[test]
fn unresolved_error_carries_spark_condition() {
    let error = FileMetadataError::unresolved("_metadata");
    assert_eq!(
        error.error_class,
        Some("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")
    );
    assert_eq!(error.sql_state, Some("42703"));
    assert!(error.message.contains("`_metadata` cannot be resolved."));
}

#[test]
fn missing_error_names_available_columns() {
    let error = FileMetadataError::missing(
        "_metadata",
        &["i".to_string(), "s".to_string()],
        "_metadata",
    );
    assert_eq!(
        error.error_class,
        Some("MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT")
    );
    assert_eq!(error.sql_state, Some("XX000"));
    assert!(error.message.contains("missing from \"i\", \"s\""));
}

#[test]
fn hidden_field_rejects_unknown_and_csv_row_index() {
    assert!(hidden_field_or_reject("file_name", METADATA_COLUMN_NAME, true).is_none());
    assert_eq!(
        hidden_field_or_reject("bogus", METADATA_COLUMN_NAME, true),
        Some("_metadata.bogus".to_string())
    );
    assert_eq!(
        hidden_field_or_reject("row_index", METADATA_COLUMN_NAME, false),
        Some("_metadata.row_index".to_string())
    );
}

#[test]
fn rewrite_names_top_level_qualified_field() {
    let renamed =
        rewrite_metadata_refs(vec![col("_metadata.file_name")], METADATA_COLUMN_NAME, true)
            .expect("known field rewrites");
    assert_eq!(renamed.len(), 1);
    assert!(matches!(renamed[0], Expr::Alias(_)));
}

#[tokio::test]
async fn mark_then_status_round_trips_listing_table() {
    let context = SessionContext::new();
    let directory = tempfile::tempdir().expect("tempdir builds");
    let path = directory.path().join("part.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .expect("batch builds");
    let file = std::fs::File::create(&path).expect("file creates");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer builds");
    writer.write(&batch).expect("batch writes");
    writer.close().expect("writer closes");
    let frame = context
        .read_parquet(
            directory.path().to_string_lossy().as_ref(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .expect("parquet reads");
    let marked = mark_file_scan(frame, FileKind::Parquet).expect("mark wraps");
    let (_, plan) = marked.into_parts();
    assert_eq!(file_metadata_status(&plan), FileMetadataStatus::Available);
}

#[tokio::test]
async fn narrowed_select_still_resolves_hidden_on_reselect() {
    let directory = tempfile::tempdir().expect("tempdir builds");
    let path = directory.path().join("part.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as _,
            Arc::new(StringArray::from(vec!["a", "b"])) as _,
        ],
    )
    .expect("batch builds");
    let file = std::fs::File::create(&path).expect("file creates");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer builds");
    writer.write(&batch).expect("batch writes");
    writer.close().expect("writer closes");
    let context = SessionContext::new();
    let frame = context
        .read_parquet(
            directory.path().to_string_lossy().as_ref(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .expect("parquet reads");
    let (state, raw_plan) = frame.into_parts();
    let frame = DataFrame::new(state, raw_plan);
    let marked = mark_file_scan(frame, FileKind::Parquet).expect("mark wraps");
    let narrowed = marked
        .select(vec![col("\"i\"").alias("i")])
        .expect("facade-style narrow select builds");
    let (state, plan) = narrowed.into_parts();
    assert_eq!(file_metadata_status(&plan), FileMetadataStatus::Available);
    let (augmented, rewritten) = ensure_file_metadata(&state, plan, vec![col("_metadata")])
        .await
        .expect("narrowed plan widens for the hidden struct");
    let reselected = DataFrame::new(state, augmented)
        .select(rewritten)
        .expect("reselect builds over the widened plan");
    let (state, reselected_plan) = reselected.into_parts();
    assert_eq!(
        file_metadata_status(&reselected_plan),
        FileMetadataStatus::Available
    );
    let (regathered, regathered_exprs) =
        ensure_file_metadata(&state, reselected_plan, vec![col("_metadata")])
            .await
            .expect("realized augmentation answers a second hop");
    DataFrame::new(state, regathered)
        .select(regathered_exprs)
        .expect("second reselect builds");
}
