//! Identity-DELETE pins: empty/full match, duplicate rows, NULL columns, `MoR` vs COW.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, ManifestContentType, NestedField, PrimitiveType, Schema, Type,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

#[test]
fn allow_list_accepts_only_uncorrelated_col_in_select() {
    for sql in [
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "delete from ice.sales.tgt where id in ( select id from ice.sales.keys )",
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE t.id IN (SELECT k.id FROM ice.sales.keys k)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM src WHERE id = 2)",
    ] {
        assert!(
            try_allowed_delete_in(&parse_statement(sql))
                .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
                .is_some(),
            "must allow {sql:?}"
        );
    }
}

#[test]
fn allow_list_refuses_every_other_subquery_spelling() {
    for sql in [
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE NOT (id IN (SELECT id FROM ice.sales.keys))",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id = (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM (SELECT id FROM ice.sales.keys) x)",
        "DELETE FROM ice.sales.tgt WHERE id = 1 OR id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id > 1 AND id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT k.id FROM ice.sales.keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt USING ice.sales.keys WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id = 2",
    ] {
        assert!(
            try_allowed_delete_in(&parse_statement(sql))
                .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
                .is_none(),
            "must NOT allow {sql:?}"
        );
    }
}

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("build memory catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

async fn create_target(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    properties: HashMap<String, String>,
) -> TableIdent {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build target schema");
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .properties(properties)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create target table");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

fn consumer_batch(ids: &[Option<i32>], values: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .expect("consumer batch builds")
}

async fn append_file(catalog: &Arc<dyn Catalog>, ident: &TableIdent, batch: RecordBatch) {
    crate::write::append::append(catalog, ident, vec![batch])
        .await
        .expect("append a data file");
}

fn register_keys(ctx: &SessionContext, ids: &[Option<i32>]) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(ids.to_vec()))],
    )
    .expect("keys batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("keys memtable");
    ctx.register_table("keys", Arc::new(table))
        .expect("register keys");
}

async fn read_back(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> Vec<(Option<i32>, Option<String>)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let scan = table
        .scan()
        .select(["id", "v"])
        .build()
        .expect("build scan");
    let batches: Vec<RecordBatch> = scan
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v Utf8");
        for row in 0..batch.num_rows() {
            let id = ids.is_valid(row).then(|| ids.value(row));
            let value = values.is_valid(row).then(|| values.value(row).to_string());
            rows.push((id, value));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    rows
}

async fn snapshot_id(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Option<i64> {
    catalog
        .load_table(ident)
        .await
        .expect("load")
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
}

async fn live_delete_file_count(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return 0;
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut count = 0;
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load delete manifest");
        count += manifest
            .entries()
            .iter()
            .filter(|entry| {
                entry.is_alive()
                    && entry.data_file().content_type() == DataContentType::PositionDeletes
            })
            .count();
    }
    count
}

async fn live_data_file_paths(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<String> {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut paths = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load data manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                paths.push(entry.data_file().file_path().to_string());
            }
        }
    }
    paths.sort();
    paths
}

fn identity_spec(table: &str) -> PredicateDmlSpec {
    PredicateDmlSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string()),
        target_alias: table.to_string(),
        selection_sql: "id IN (SELECT id FROM keys)".to_string(),
    }
}

#[tokio::test]
async fn identity_delete_empty_match_commits_nothing() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "empty", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let before = snapshot_id(&catalog, &ident).await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(99)]);
    execute_predicate_dml(&ctx, &catalog, &identity_spec("empty"))
        .await
        .expect("empty-match DELETE");
    assert_eq!(snapshot_id(&catalog, &ident).await, before);
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("a".into())), (Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_full_match_empties_the_table() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "full", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1), Some(2)]);
    execute_predicate_dml(&ctx, &catalog, &identity_spec("full"))
        .await
        .expect("full-match DELETE");
    assert!(read_back(&catalog, &ident).await.is_empty());
}

#[tokio::test]
async fn identity_delete_duplicate_rows_deletes_every_copy() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "dups", HashMap::new()).await;
    // Two identical (id, v) rows in TWO files — all-column MERGE identity would cardinality-fail.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1)], &[Some("same")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1)], &[Some("same")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(2)], &[Some("keep")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1)]);
    execute_predicate_dml(&ctx, &catalog, &identity_spec("dups"))
        .await
        .expect("duplicate-row DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(2), Some("keep".into()))]
    );
}

#[tokio::test]
async fn identity_delete_null_column_row_is_still_deleted() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nullcol", HashMap::new()).await;
    // (1, NULL) must be deleted by id IN (1). All-column 3VL identity would miss it.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[None, Some("keep")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1)]);
    execute_predicate_dml(&ctx, &catalog, &identity_spec("nullcol"))
        .await
        .expect("NULL-column DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(2), Some("keep".into()))]
    );
}

#[tokio::test]
async fn identity_delete_null_key_is_unknown_and_survives() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nullkey", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[None, Some(2)], &[Some("n"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(&ctx, &catalog, &identity_spec("nullkey"))
        .await
        .expect("NULL-key DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(None, Some("n".into()))]
    );
}

#[tokio::test]
async fn identity_delete_honors_write_delete_mode_not_merge_mode() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target(
        &catalog,
        "mode_mor",
        HashMap::from([
            (WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string()),
            ("write.merge.mode".to_string(), "copy-on-write".to_string()),
        ]),
    )
    .await;
    let cow = create_target(
        &catalog,
        "mode_cow",
        HashMap::from([
            (WRITE_DELETE_MODE.to_string(), "copy-on-write".to_string()),
            ("write.merge.mode".to_string(), "merge-on-read".to_string()),
        ]),
    )
    .await;
    for ident in [&mor, &cow] {
        append_file(
            &catalog,
            ident,
            consumer_batch(&[Some(1), Some(3)], &[Some("drop"), Some("keep")]),
        )
        .await;
        append_file(&catalog, ident, consumer_batch(&[Some(2)], &[Some("stay")])).await;
    }
    let mor_data_before = live_data_file_paths(&catalog, &mor).await;
    let cow_data_before = live_data_file_paths(&catalog, &cow).await;

    for name in ["mode_mor", "mode_cow"] {
        let ctx = SessionContext::new();
        register_keys(&ctx, &[Some(1)]);
        execute_predicate_dml(&ctx, &catalog, &identity_spec(name))
            .await
            .unwrap_or_else(|error| panic!("{name} identity DELETE: {error}"));
    }

    let expected = vec![
        (Some(2), Some("stay".into())),
        (Some(3), Some("keep".into())),
    ];
    assert_eq!(read_back(&catalog, &mor).await, expected);
    assert_eq!(read_back(&catalog, &cow).await, expected);

    assert!(
        live_delete_file_count(&catalog, &mor).await >= 1,
        "write.delete.mode=merge-on-read must commit position deletes (not follow write.merge.mode)"
    );
    assert_eq!(
        live_data_file_paths(&catalog, &mor).await,
        mor_data_before,
        "MoR DELETE must leave original data files in place"
    );
    assert_eq!(
        live_delete_file_count(&catalog, &cow).await,
        0,
        "write.delete.mode=copy-on-write must NOT write position deletes even if merge.mode is MoR"
    );
    let cow_after = live_data_file_paths(&catalog, &cow).await;
    assert_ne!(
        cow_after, cow_data_before,
        "COW DELETE must rewrite the affected data file away"
    );
}
