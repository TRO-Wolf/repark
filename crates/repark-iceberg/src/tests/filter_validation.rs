use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use iceberg::expr::Reference;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFile, Datum, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::commit_target::{FilterValidation, commit_append_to};
use crate::write::concurrency::WriteConcurrency;
use crate::write::illegal_argument::NumberFormatMarker;
use crate::write::overwrite_filter::commit_overwrite_by_filter_with_summary;

struct Seeded {
    _warehouse: TempDir,
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    first: i64,
    latest: i64,
}

async fn catalog_table() -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().expect("temp warehouse");
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
    let namespace = NamespaceIdent::new("u7".into());
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
    (
        warehouse,
        catalog,
        TableIdent::new(namespace, "t".to_string()),
    )
}

async fn stage(table: &Table, id: i64, data: &str, cat: &str) -> Vec<DataFile> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("cat", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![data])),
            Arc::new(StringArray::from(vec![cat])),
        ],
    )
    .expect("batch builds");
    crate::write::write_overwrite_staged_files_from_stream(
        table,
        futures::stream::iter(vec![Ok(batch)]),
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
    )
    .await
    .expect("stage files")
}

fn id_is(value: i64) -> iceberg::expr::Predicate {
    Reference::new("id").equal_to(Datum::long(value))
}

async fn overwrite_id(
    seeded: &Seeded,
    id: i64,
    added: Option<(i64, &str, &str)>,
    validation: FilterValidation<'_>,
) -> Result<Table> {
    let table = seeded
        .catalog
        .load_table(&seeded.ident)
        .await
        .expect("load table");
    let staged = match added {
        Some((added_id, data, cat)) => stage(&table, added_id, data, cat).await,
        None => Vec::new(),
    };
    commit_overwrite_by_filter_with_summary(
        &seeded.catalog,
        &table,
        staged,
        id_is(id),
        None,
        &[],
        validation,
    )
    .await
}

async fn seeded_then(change: &str) -> Seeded {
    let (warehouse, catalog, ident) = catalog_table().await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let mut files = stage(&table, 1, "a", "x").await;
    files.extend(stage(&table, 2, "b", "y").await);
    files.extend(stage(&table, 3, "c", "x").await);
    let seeded = commit_append_to(&catalog, &table, files, None)
        .await
        .expect("seed three files");
    let first = seeded
        .metadata()
        .current_snapshot_id()
        .expect("seed snapshot");
    let mut result = Seeded {
        _warehouse: warehouse,
        catalog,
        ident,
        first,
        latest: first,
    };
    let changed = match change {
        "delete id 1" => overwrite_id(&result, 1, None, FilterValidation::default()).await,
        "delete id 2" => overwrite_id(&result, 2, None, FilterValidation::default()).await,
        "append id 1" => {
            let table = result
                .catalog
                .load_table(&result.ident)
                .await
                .expect("load table");
            let files = stage(&table, 1, "z", "q").await;
            commit_append_to(&result.catalog, &table, files, None).await
        }
        other => panic!("unknown change {other}"),
    };
    let changed = changed.expect("the concurrent change commits");
    result.latest = changed
        .metadata()
        .current_snapshot_id()
        .expect("change snapshot");
    result
}

fn message(error: &DataFusionError) -> String {
    error.to_string()
}

#[tokio::test]
async fn an_explicit_isolation_validates_from_the_requested_snapshot() {
    let seeded = seeded_then("delete id 1").await;
    let first = seeded.first.to_string();
    for level in ["serializable", "snapshot"] {
        let refused = overwrite_id(
            &seeded,
            1,
            Some((7, "g", "x")),
            FilterValidation {
                isolation: Some(level),
                validate_from_snapshot_id: Some(first.as_str()),
            },
        )
        .await
        .expect_err("a delete after the requested snapshot conflicts");
        assert!(
            message(&refused)
                .contains("Found conflicting deleted files that can contain records matching"),
            "{level}: {refused}"
        );
    }
    let latest = seeded.latest.to_string();
    overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: Some(latest.as_str()),
        },
    )
    .await
    .expect("nothing conflicts after the latest snapshot");
}

#[tokio::test]
async fn without_an_isolation_option_the_requested_snapshot_is_not_validated() {
    let seeded = seeded_then("delete id 1").await;
    let first = seeded.first.to_string();
    overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: None,
            validate_from_snapshot_id: Some(first.as_str()),
        },
    )
    .await
    .expect("the table's isolation validates from the loaded snapshot");
}

#[tokio::test]
async fn an_explicit_isolation_without_a_start_validates_from_the_loaded_snapshot() {
    let seeded = seeded_then("delete id 1").await;
    overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: None,
        },
    )
    .await
    .expect("residue R-6: Spark validates the whole history here and refuses");
}

#[tokio::test]
async fn serializable_refuses_a_matching_append_and_snapshot_does_not() {
    let seeded = seeded_then("append id 1").await;
    let first = seeded.first.to_string();
    let refused = overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: Some(first.as_str()),
        },
    )
    .await
    .expect_err("a matching append after the start conflicts");
    assert!(
        message(&refused).contains("Found conflicting files that can contain records matching"),
        "{refused}"
    );
    overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("snapshot"),
            validate_from_snapshot_id: Some(first.as_str()),
        },
    )
    .await
    .expect("snapshot isolation checks deletes only");
}

#[tokio::test]
async fn a_change_to_other_rows_does_not_conflict() {
    let seeded = seeded_then("delete id 2").await;
    let first = seeded.first.to_string();
    overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: Some(first.as_str()),
        },
    )
    .await
    .expect("the delete of id 2 cannot match id 1");
}

#[tokio::test]
async fn the_requested_snapshot_parses_like_java_long_and_must_be_an_ancestor() {
    let seeded = seeded_then("delete id 2").await;
    let refused = overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: Some("abc"),
        },
    )
    .await
    .expect_err("abc is not a long");
    let DataFusionError::External(inner) = &refused else {
        panic!("expected a NumberFormatMarker, got {refused:?}");
    };
    let marker = inner
        .downcast_ref::<NumberFormatMarker>()
        .unwrap_or_else(|| panic!("expected a NumberFormatMarker, got {inner:?}"));
    assert_eq!(marker.0, "For input string: \"abc\"");
    let unknown = overwrite_id(
        &seeded,
        1,
        Some((7, "g", "x")),
        FilterValidation {
            isolation: Some("serializable"),
            validate_from_snapshot_id: Some("12345"),
        },
    )
    .await
    .expect_err("12345 is not an ancestor");
    assert!(
        message(&unknown).contains(&format!(
            "Cannot determine history between starting snapshot 12345 and the last known \
             ancestor {}",
            seeded.first
        )),
        "{unknown}"
    );
}
