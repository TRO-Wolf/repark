use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::concurrency::WriteConcurrency;
use crate::write::illegal_argument::{IllegalArgumentMarker, NumberFormatMarker};
use crate::write::output_spec::{parse_output_spec_id, staged_spec_is_partitioned, staging_table};
use crate::write::partition_spec::{PartitionSpecChange, apply_partition_spec_changes};
use crate::write::write_options::{
    WriterStagingOverrides, append_with_statement_options, stage_partitioned_stream_with_overrides,
};

fn illegal_argument_text(error: &DataFusionError) -> String {
    let DataFusionError::External(inner) = error else {
        panic!("expected an External marker, got {error:?}");
    };
    inner.downcast_ref::<IllegalArgumentMarker>().map_or_else(
        || panic!("expected an IllegalArgumentMarker, got {inner:?}"),
        |marker| marker.0.clone(),
    )
}

async fn evolved_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, Table) {
    let path = warehouse.path().to_str().expect("utf-8 path").to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("ns".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ident = TableIdent::new(namespace, "t".into());
    apply_partition_spec_changes(
        catalog.as_ref(),
        &ident,
        &[PartitionSpecChange::AddField {
            source_name: "cat".to_string(),
            transform: Transform::Identity,
            name: None,
        }],
    )
    .await
    .expect("add partition field cat");
    let table = catalog.load_table(&ident).await.expect("reload");
    (catalog, table)
}

fn frame() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("cat", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec!["g", "h"])),
            Arc::new(StringArray::from(vec!["x", "w"])),
        ],
    )
    .expect("batch")
}

async fn committed_spec_ids(table: &Table) -> Vec<i32> {
    let snapshot = table.metadata().current_snapshot().expect("a snapshot");
    let manifests = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("manifest list");
    let mut ids = Vec::new();
    for manifest in manifests.entries() {
        let loaded = manifest
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for entry in loaded.entries() {
            ids.push(entry.data_file().partition_spec_id());
        }
    }
    ids.sort_unstable();
    ids
}

async fn append_with(catalog: &Arc<dyn Catalog>, table: &Table, spec_id: Option<i32>) -> Table {
    let staging = WriterStagingOverrides {
        output_spec_id: spec_id,
        ..WriterStagingOverrides::none()
    };
    let stream = futures::stream::iter(vec![Ok::<_, DataFusionError>(frame())]);
    append_with_statement_options(
        catalog,
        table,
        stream,
        &[],
        &staging,
        WriteConcurrency::default(),
        None,
    )
    .await
    .expect("append")
}

#[test]
fn output_spec_id_parses_like_java_integer_parse_int() {
    assert_eq!(parse_output_spec_id("0").expect("zero"), 0);
    assert_eq!(parse_output_spec_id("+1").expect("plus"), 1);
    assert_eq!(parse_output_spec_id("-1").expect("minus"), -1);
    for raw in ["x", "", " 1", "1.0", "99999999999"] {
        let error = parse_output_spec_id(raw).expect_err("non-int refuses");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        let marker = inner
            .downcast_ref::<NumberFormatMarker>()
            .unwrap_or_else(|| panic!("expected a NumberFormatMarker, got {inner:?}"));
        assert_eq!(marker.0, format!("For input string: \"{raw}\""));
    }
}

#[tokio::test]
async fn staging_table_swaps_the_default_spec_for_the_requested_one() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = evolved_table(&warehouse).await;
    assert_eq!(table.metadata().default_partition_spec_id(), 1);
    let none = staging_table(&table, &WriterStagingOverrides::none()).expect("no option");
    assert_eq!(none.metadata().default_partition_spec_id(), 1);
    let current = WriterStagingOverrides {
        output_spec_id: Some(1),
        ..WriterStagingOverrides::none()
    };
    let same = staging_table(&table, &current).expect("current spec");
    assert!(matches!(same, std::borrow::Cow::Borrowed(_)));
    let old = WriterStagingOverrides {
        output_spec_id: Some(0),
        ..WriterStagingOverrides::none()
    };
    let viewed = staging_table(&table, &old).expect("old spec");
    assert_eq!(viewed.metadata().default_partition_spec_id(), 0);
    assert!(
        viewed
            .metadata()
            .default_partition_spec()
            .is_unpartitioned()
    );
    assert_eq!(table.metadata().default_partition_spec_id(), 1);
}

#[tokio::test]
async fn unknown_output_spec_id_refuses_with_spark_text() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = evolved_table(&warehouse).await;
    for spec_id in [7, -1] {
        let staging = WriterStagingOverrides {
            output_spec_id: Some(spec_id),
            ..WriterStagingOverrides::none()
        };
        let error = staging_table(&table, &staging).expect_err("unknown spec refuses");
        assert_eq!(
            illegal_argument_text(&error),
            format!("Output spec id {spec_id} is not a valid spec id for table")
        );
    }
}

#[tokio::test]
async fn append_with_an_old_output_spec_commits_files_under_that_spec() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = evolved_table(&warehouse).await;
    let after = append_with(&catalog, &table, Some(0)).await;
    assert_eq!(committed_spec_ids(&after).await, vec![0]);
    assert_eq!(after.metadata().default_partition_spec_id(), 1);
    let rows: usize = after
        .scan()
        .select_all()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("arrow")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn append_without_the_option_lands_under_the_current_spec() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = evolved_table(&warehouse).await;
    let after = append_with(&catalog, &table, None).await;
    assert_eq!(committed_spec_ids(&after).await, vec![1, 1]);
}

#[tokio::test]
async fn the_staged_spec_decides_whether_a_dynamic_overwrite_replaces_partitions() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = evolved_table(&warehouse).await;
    let with = |output_spec_id| WriterStagingOverrides {
        output_spec_id,
        ..WriterStagingOverrides::none()
    };
    assert!(staged_spec_is_partitioned(&table, &with(None)).expect("current"));
    assert!(staged_spec_is_partitioned(&table, &with(Some(1))).expect("spec 1"));
    assert!(!staged_spec_is_partitioned(&table, &with(Some(0))).expect("spec 0"));
    let error = staged_spec_is_partitioned(&table, &with(Some(7))).expect_err("unknown");
    assert_eq!(
        illegal_argument_text(&error),
        "Output spec id 7 is not a valid spec id for table"
    );
}

#[tokio::test]
async fn a_partitioned_writer_follows_an_unpartitioned_output_spec() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = evolved_table(&warehouse).await;
    let staging = WriterStagingOverrides {
        output_spec_id: Some(0),
        ..WriterStagingOverrides::none()
    };
    let stream = futures::stream::iter(vec![Ok::<_, DataFusionError>(frame())]);
    let files = stage_partitioned_stream_with_overrides(
        &table,
        stream,
        &staging,
        WriteConcurrency::default(),
    )
    .await
    .expect("staged under spec 0");
    let specs: Vec<i32> = files
        .iter()
        .map(iceberg::spec::DataFile::partition_spec_id)
        .collect();
    assert_eq!(specs, vec![0]);
}
