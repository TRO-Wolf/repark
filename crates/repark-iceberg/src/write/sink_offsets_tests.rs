use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use iceberg::spec::{NestedField, Operation, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

const STAMP_KEY: &str = "repark.cdc.epoch";
const STAMP_PROPERTY: &str = "repark.cdc.offsets.dm-5";

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

async fn fixture(name: &str) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("silver".to_string()), name.to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(name.to_string())
                .schema(id_schema())
                .build(),
        )
        .await
        .expect("create table");
    (warehouse, catalog, ident)
}

async fn empty_merge_append(catalog: &Arc<dyn Catalog>, table: &Table) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .merge_append()
        .set_snapshot_properties(HashMap::from([(STAMP_KEY.to_string(), "0".to_string())]))
        .apply(tx)
        .expect("apply empty merge_append");
    let tx = tx
        .update_table_properties()
        .set(STAMP_PROPERTY.to_string(), "{}".to_string())
        .apply(tx)
        .expect("apply property");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit stamp-only")
}

#[tokio::test]
async fn dm5_empty_merge_append_commits_one_stamp_only_append_snapshot() {
    let (_warehouse, catalog, ident) = fixture("dm5").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let base_log = table.metadata().metadata_log().len();
    let committed = empty_merge_append(&catalog, &table).await;
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    for view in [&committed, &reloaded] {
        let metadata = view.metadata();
        assert_eq!(metadata.metadata_log().len(), base_log + 1);
        assert_eq!(metadata.snapshots().count(), 1);
        let head = metadata.current_snapshot().expect("stamp-only snapshot");
        assert_eq!(head.summary().operation, Operation::Append);
        assert_eq!(
            head.summary()
                .additional_properties
                .get(STAMP_KEY)
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            head.summary()
                .additional_properties
                .get("added-data-files")
                .map(String::as_str),
            None
        );
        assert_eq!(
            metadata
                .properties()
                .get(STAMP_PROPERTY)
                .map(String::as_str),
            Some("{}")
        );
    }
    let second = empty_merge_append(&catalog, &reloaded).await;
    let head = second.metadata().current_snapshot().expect("second stamp");
    assert_eq!(head.summary().operation, Operation::Append);
    assert_eq!(
        head.parent_snapshot_id(),
        reloaded.metadata().current_snapshot_id()
    );
    let files = second
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan");
    let tasks: Vec<_> = files.try_collect().await.expect("tasks");
    assert!(tasks.is_empty());
}
