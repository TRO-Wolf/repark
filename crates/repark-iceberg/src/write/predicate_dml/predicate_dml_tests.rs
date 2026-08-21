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

use super::{IsolationLevel, WRITE_DELETE_ISOLATION_LEVEL, resolve_delete_isolation, *};

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

#[test]
fn allow_list_accepts_in_not_in_and_exists_family() {
    for sql in [
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "delete from ice.sales.tgt where id in ( select id from ice.sales.keys )",
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE t.id IN (SELECT k.id FROM ice.sales.keys k)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM src WHERE id = 2)",
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "delete from ice.sales.tgt where id not in ( select id from ice.sales.keys )",
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE id NOT IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE t.id NOT IN (SELECT k.id FROM ice.sales.keys k)",
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM src WHERE id = 2)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "delete from ice.sales.tgt where exists ( select 1 from ice.sales.keys )",
        "DELETE FROM \"ice\".\"sales\".\"tgt\" WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE EXISTS (SELECT 1 FROM ice.sales.keys k WHERE k.id = t.id)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt t WHERE NOT EXISTS \
         (SELECT 1 FROM ice.sales.keys k WHERE k.id = t.id)",
        "DELETE FROM ice.sales.tgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys WHERE id = 2)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT k.id FROM ice.sales.keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "DELETE FROM ice.sales.tgt t WHERE t.id IN (SELECT k.id FROM ice.sales.keys k \
         WHERE k.id = t.id)",
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
        "DELETE FROM ice.sales.tgt WHERE NOT (id IN (SELECT id FROM ice.sales.keys))",
        "DELETE FROM ice.sales.tgt WHERE NOT (EXISTS (SELECT 1 FROM ice.sales.keys))",
        "DELETE FROM ice.sales.tgt WHERE id = (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT max(id) FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM (SELECT id FROM ice.sales.keys) x)",
        "DELETE FROM ice.sales.tgt WHERE id = 1 OR id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id > 1 AND id IN (SELECT id FROM ice.sales.keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt USING ice.sales.keys WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt USING ice.sales.keys WHERE id NOT IN \
         (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (SELECT id FROM ice.sales.keys) RETURNING *",
        "DELETE FROM ice.sales.tgt USING ice.sales.keys WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys) RETURNING *",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM (SELECT id FROM ice.sales.keys) x)",
        "DELETE FROM ice.sales.tgt WHERE id > 1 AND EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys) OR id = 1",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE EXISTS (SELECT 1 FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt WHERE EXISTS (SELECT 1 FROM ice.sales.keys k \
         WHERE k.id = other.id)",
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
    identity_spec_for(table, "id IN (SELECT id FROM keys)")
}

fn identity_spec_for(table: &str, selection_sql: &str) -> PredicateDmlSpec {
    PredicateDmlSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string()),
        target_alias: table.to_string(),
        selection_sql: selection_sql.to_string(),
        assignments: None,
    }
}

const NOT_IN_SELECTION: &str = "id NOT IN (SELECT id FROM keys)";

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

/// A4: the identity path is this SELECT. DataFusion must reproduce Spark 4.1.2 3VL here —
/// empty subquery matches every row; ANY NULL in the subquery matches none; NULL LHS is
/// UNKNOWN. A hand-rolled "match none" shortcut is not this pin.
#[tokio::test]
async fn identity_select_not_in_matches_spark_three_valued_logic() {
    let ctx = SessionContext::new();
    let target_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        true,
    )]));
    let target_batch = RecordBatch::try_new(
        Arc::clone(&target_schema),
        vec![Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
        ]))],
    )
    .expect("target batch");
    ctx.register_table(
        "tgt",
        Arc::new(MemTable::try_new(target_schema, vec![vec![target_batch]]).expect("tgt")),
    )
    .expect("register tgt");

    let select = |keys: &[Option<i32>]| {
        let ctx = ctx.clone();
        let keys = keys.to_vec();
        async move {
            let _ = ctx.deregister_table("keys");
            register_keys(&ctx, &keys);
            let batches = ctx
                .sql("SELECT id FROM tgt WHERE id NOT IN (SELECT id FROM keys)")
                .await
                .expect("identity SELECT plans")
                .collect()
                .await
                .expect("identity SELECT collects");
            let mut ids = Vec::new();
            for batch in &batches {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id Int32");
                for row in 0..batch.num_rows() {
                    ids.push(column.is_valid(row).then(|| column.value(row)));
                }
            }
            ids.sort();
            ids
        }
    };

    assert_eq!(
        select(&[Some(2)]).await,
        vec![Some(1), Some(3)],
        "no-NULL NOT IN is set-difference: drop the key, keep the rest (NULL LHS is UNKNOWN)"
    );
    assert_eq!(
        select(&[Some(2), None]).await,
        Vec::<Option<i32>>::new(),
        "ANY NULL in the subquery ⇒ NOT IN is never TRUE (Spark 3VL trap)"
    );
    assert_eq!(
        select(&[]).await,
        vec![None, Some(1), Some(2), Some(3)],
        "empty subquery ⇒ NOT IN is vacuously TRUE, including a NULL target column"
    );
}

#[tokio::test]
async fn identity_delete_not_in_keeps_only_the_key_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "notin", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("notin", NOT_IN_SELECTION),
    )
    .await
    .expect("NOT IN DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_in_null_in_subquery_matches_nothing() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nullsub", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        ),
    )
    .await;
    let before = snapshot_id(&catalog, &ident).await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2), None]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("nullsub", NOT_IN_SELECTION),
    )
    .await
    .expect("NOT IN + NULL subquery");
    assert_eq!(snapshot_id(&catalog, &ident).await, before);
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (Some(1), Some("a".into())),
            (Some(2), Some("b".into())),
            (Some(3), Some("c".into())),
        ]
    );
}

#[tokio::test]
async fn identity_delete_not_in_empty_subquery_deletes_every_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "emptykeys", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("emptykeys", NOT_IN_SELECTION),
    )
    .await
    .expect("NOT IN empty subquery");
    assert!(read_back(&catalog, &ident).await.is_empty());
}

#[tokio::test]
async fn identity_delete_not_in_null_target_column_survives() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nulltgt", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(
            &[None, Some(1), Some(2)],
            &[Some("n"), Some("a"), Some("b")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("nulltgt", NOT_IN_SELECTION),
    )
    .await
    .expect("NOT IN NULL-target");
    // NULL NOT IN {1} is UNKNOWN — the NULL-id row survives. id=2 is deleted.
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(None, Some("n".into())), (Some(1), Some("a".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_in_duplicates_both_sides() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "notindups", HashMap::new()).await;
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
    register_keys(&ctx, &[Some(2), Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("notindups", NOT_IN_SELECTION),
    )
    .await
    .expect("NOT IN duplicate-key DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(2), Some("keep".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_in_honors_write_delete_mode_not_merge_mode() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target(
        &catalog,
        "notin_mor",
        HashMap::from([
            (WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string()),
            ("write.merge.mode".to_string(), "copy-on-write".to_string()),
        ]),
    )
    .await;
    let cow = create_target(
        &catalog,
        "notin_cow",
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
            consumer_batch(&[Some(1), Some(3)], &[Some("keep"), Some("drop")]),
        )
        .await;
        append_file(&catalog, ident, consumer_batch(&[Some(2)], &[Some("drop")])).await;
    }
    let mor_data_before = live_data_file_paths(&catalog, &mor).await;
    let cow_data_before = live_data_file_paths(&catalog, &cow).await;

    for name in ["notin_mor", "notin_cow"] {
        let ctx = SessionContext::new();
        register_keys(&ctx, &[Some(1)]);
        execute_predicate_dml(&ctx, &catalog, &identity_spec_for(name, NOT_IN_SELECTION))
            .await
            .unwrap_or_else(|error| panic!("{name} identity NOT IN DELETE: {error}"));
    }

    let expected = vec![(Some(1), Some("keep".into()))];
    assert_eq!(read_back(&catalog, &mor).await, expected);
    assert_eq!(read_back(&catalog, &cow).await, expected);

    assert!(
        live_delete_file_count(&catalog, &mor).await >= 1,
        "write.delete.mode=merge-on-read must commit position deletes (not follow write.merge.mode)"
    );
    assert_eq!(
        live_data_file_paths(&catalog, &mor).await,
        mor_data_before,
        "MoR NOT IN DELETE must leave original data files in place"
    );
    assert_eq!(
        live_delete_file_count(&catalog, &cow).await,
        0,
        "write.delete.mode=copy-on-write must NOT write position deletes even if merge.mode is MoR"
    );
    let cow_after = live_data_file_paths(&catalog, &cow).await;
    assert_ne!(
        cow_after, cow_data_before,
        "COW NOT IN DELETE must rewrite the affected data file away"
    );
}

const EXISTS_UNCORRELATED: &str = "EXISTS (SELECT 1 FROM keys)";
const NOT_EXISTS_UNCORRELATED: &str = "NOT EXISTS (SELECT 1 FROM keys)";
const EXISTS_CORRELATED: &str = "EXISTS (SELECT 1 FROM keys k WHERE k.id = tgt.id)";
const NOT_EXISTS_CORRELATED: &str = "NOT EXISTS (SELECT 1 FROM keys k WHERE k.id = tgt.id)";

#[derive(Clone)]
struct ExistsSparkCase {
    name: &'static str,
    target_ids: Vec<Option<i32>>,
    target_names: Vec<Option<String>>,
    key_ids: Vec<Option<i32>>,
    selection: &'static str,
    spark_remaining: Vec<(Option<i32>, Option<String>)>,
}

fn exists_case(
    name: &'static str,
    target_ids: &[Option<i32>],
    target_names: &[&str],
    key_ids: &[Option<i32>],
    selection: &'static str,
    remaining: &[(Option<i32>, &str)],
) -> ExistsSparkCase {
    ExistsSparkCase {
        name,
        target_ids: target_ids.to_vec(),
        target_names: target_names
            .iter()
            .map(|name| Some((*name).to_string()))
            .collect(),
        key_ids: key_ids.to_vec(),
        selection,
        spark_remaining: remaining
            .iter()
            .map(|(id, name)| (*id, Some((*name).to_string())))
            .collect(),
    }
}

#[allow(clippy::too_many_lines)] // recorded Spark 4.1.2 fixture table — one case per A4 row
fn exists_spark_cases() -> Vec<ExistsSparkCase> {
    let abc = [Some(1), Some(2), Some(3)];
    let names = ["a", "b", "c"];
    let keep_all = [(Some(1), "a"), (Some(2), "b"), (Some(3), "c")];
    vec![
        exists_case(
            "exists_uncorrelated_nonempty",
            &abc,
            &names,
            &[Some(2)],
            EXISTS_UNCORRELATED,
            &[],
        ),
        exists_case(
            "exists_uncorrelated_empty",
            &abc,
            &names,
            &[],
            EXISTS_UNCORRELATED,
            &keep_all,
        ),
        exists_case(
            "not_exists_uncorrelated_nonempty",
            &abc,
            &names,
            &[Some(2)],
            NOT_EXISTS_UNCORRELATED,
            &keep_all,
        ),
        exists_case(
            "not_exists_uncorrelated_empty",
            &abc,
            &names,
            &[],
            NOT_EXISTS_UNCORRELATED,
            &[],
        ),
        exists_case(
            "exists_correlated_some",
            &abc,
            &names,
            &[Some(2)],
            EXISTS_CORRELATED,
            &[(Some(1), "a"), (Some(3), "c")],
        ),
        exists_case(
            "not_exists_correlated_some",
            &abc,
            &names,
            &[Some(2)],
            NOT_EXISTS_CORRELATED,
            &[(Some(2), "b")],
        ),
        exists_case(
            "exists_correlated_none",
            &abc,
            &names,
            &[Some(99)],
            EXISTS_CORRELATED,
            &keep_all,
        ),
        exists_case(
            "exists_correlated_all",
            &abc,
            &names,
            &[Some(1), Some(2), Some(3)],
            EXISTS_CORRELATED,
            &[],
        ),
        exists_case(
            "exists_correlated_empty",
            &abc,
            &names,
            &[],
            EXISTS_CORRELATED,
            &keep_all,
        ),
        exists_case(
            "not_exists_correlated_none",
            &abc,
            &names,
            &[Some(99)],
            NOT_EXISTS_CORRELATED,
            &[],
        ),
        exists_case(
            "not_exists_correlated_all",
            &abc,
            &names,
            &[Some(1), Some(2), Some(3)],
            NOT_EXISTS_CORRELATED,
            &keep_all,
        ),
        exists_case(
            "not_exists_correlated_empty",
            &abc,
            &names,
            &[],
            NOT_EXISTS_CORRELATED,
            &[],
        ),
        exists_case(
            "exists_correlated_null_keys",
            &[Some(1), Some(2), None],
            &["a", "b", "n"],
            &[Some(2), None],
            EXISTS_CORRELATED,
            &[(None, "n"), (Some(1), "a")],
        ),
        exists_case(
            "not_exists_correlated_null_keys",
            &[Some(1), Some(2), None],
            &["a", "b", "n"],
            &[Some(2), None],
            NOT_EXISTS_CORRELATED,
            &[(Some(2), "b")],
        ),
        exists_case(
            "exists_correlated_duplicates",
            &[Some(1), Some(1), Some(2)],
            &["a", "a", "b"],
            &[Some(1), Some(1)],
            EXISTS_CORRELATED,
            &[(Some(2), "b")],
        ),
        exists_case(
            "not_exists_correlated_duplicates",
            &[Some(1), Some(1), Some(2)],
            &["a", "a", "b"],
            &[Some(1), Some(1)],
            NOT_EXISTS_CORRELATED,
            &[(Some(1), "a"), (Some(1), "a")],
        ),
    ]
}

/// A4: the identity path is this SELECT. DataFusion must reproduce live Spark 4.1.2
/// `[NOT] EXISTS` row-sets (recorded 2026-08-13 under `/tmp/grok-jvm-record.lock`).
/// No hand-rolled empty/all shortcut — the executed SELECT is the pin.
#[tokio::test]
async fn identity_select_exists_matches_spark_412_row_sets() {
    for case in exists_spark_cases() {
        let ctx = SessionContext::new();
        let target_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let target_batch = RecordBatch::try_new(
            Arc::clone(&target_schema),
            vec![
                Arc::new(Int32Array::from(case.target_ids.clone())),
                Arc::new(StringArray::from(case.target_names.clone())),
            ],
        )
        .expect("target batch");
        ctx.register_table(
            "tgt",
            Arc::new(MemTable::try_new(target_schema, vec![vec![target_batch]]).expect("tgt")),
        )
        .expect("register tgt");
        register_keys(&ctx, &case.key_ids);
        // Exact identity-path SELECT: rows the DELETE would remove. Remaining = seed − that set.
        let delete_sql = format!("SELECT id, name FROM tgt WHERE {}", case.selection);
        let batches = ctx
            .sql(&delete_sql)
            .await
            .unwrap_or_else(|error| panic!("{} identity SELECT plans: {error}", case.name))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("{} identity SELECT collects: {error}", case.name));
        let mut deleted = std::collections::HashSet::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name");
            for row in 0..batch.num_rows() {
                deleted.insert((
                    ids.is_valid(row).then(|| ids.value(row)),
                    names.is_valid(row).then(|| names.value(row).to_string()),
                ));
            }
        }
        let mut remaining: Vec<(Option<i32>, Option<String>)> = case
            .target_ids
            .iter()
            .zip(case.target_names.iter())
            .map(|(id, name)| (*id, name.clone()))
            .filter(|row| !deleted.contains(row))
            .collect();
        remaining.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut expected: Vec<(Option<i32>, Option<String>)> = case.spark_remaining.clone();
        expected.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        assert_eq!(
            remaining, expected,
            "{}: identity SELECT remaining must match live Spark 4.1.2 (deleted={deleted:?})",
            case.name
        );
    }
}

#[test]
fn allow_list_rewrites_target_fqn_to_scratch_alias() {
    let statement = parse_statement(
        "DELETE FROM ice.sales.tgt WHERE EXISTS \
         (SELECT 1 FROM ice.sales.keys k WHERE k.id = ice.sales.tgt.id)",
    );
    let allowed = try_allowed_delete_in(&statement)
        .expect("parse")
        .expect("allowed");
    assert_eq!(allowed.spec.target_alias, "tgt");
    assert!(
        allowed.spec.selection_sql.contains("tgt.id")
            || allowed.spec.selection_sql.contains("tgt`.`id"),
        "FQN target ref must become the scratch alias, got {}",
        allowed.spec.selection_sql
    );
    assert!(
        !allowed.spec.selection_sql.contains("ice.sales.tgt.id"),
        "must not leave the user-table FQN in the identity SELECT: {}",
        allowed.spec.selection_sql
    );
}

#[tokio::test]
async fn identity_delete_exists_uncorrelated_nonempty_deletes_every_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "ex_all", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("ex_all", EXISTS_UNCORRELATED),
    )
    .await
    .expect("uncorrelated nonempty EXISTS");
    assert!(read_back(&catalog, &ident).await.is_empty());
}

#[tokio::test]
async fn identity_delete_exists_uncorrelated_empty_deletes_nothing() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "ex_empty", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let before = snapshot_id(&catalog, &ident).await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("ex_empty", EXISTS_UNCORRELATED),
    )
    .await
    .expect("uncorrelated empty EXISTS");
    assert_eq!(snapshot_id(&catalog, &ident).await, before);
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("a".into())), (Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_exists_uncorrelated_nonempty_deletes_nothing() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nex_keep", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("nex_keep", NOT_EXISTS_UNCORRELATED),
    )
    .await
    .expect("uncorrelated nonempty NOT EXISTS");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("a".into())), (Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_exists_uncorrelated_empty_deletes_every_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nex_all", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for("nex_all", NOT_EXISTS_UNCORRELATED),
    )
    .await
    .expect("uncorrelated empty NOT EXISTS");
    assert!(read_back(&catalog, &ident).await.is_empty());
}

#[tokio::test]
async fn identity_delete_exists_correlated_some_nulls_and_duplicates() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;

    let some = create_target(&catalog, "ex_some", HashMap::new()).await;
    append_file(
        &catalog,
        &some,
        consumer_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for(
            "ex_some",
            "EXISTS (SELECT 1 FROM keys k WHERE k.id = ex_some.id)",
        ),
    )
    .await
    .expect("correlated some EXISTS");
    assert_eq!(
        read_back(&catalog, &some).await,
        vec![(Some(1), Some("a".into())), (Some(3), Some("c".into()))]
    );

    let nulls = create_target(&catalog, "ex_nulls", HashMap::new()).await;
    append_file(
        &catalog,
        &nulls,
        consumer_batch(
            &[Some(1), Some(2), None],
            &[Some("a"), Some("b"), Some("n")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2), None]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for(
            "ex_nulls",
            "EXISTS (SELECT 1 FROM keys k WHERE k.id = ex_nulls.id)",
        ),
    )
    .await
    .expect("correlated NULL-key EXISTS");
    assert_eq!(
        read_back(&catalog, &nulls).await,
        vec![(None, Some("n".into())), (Some(1), Some("a".into()))]
    );

    let dups = create_target(&catalog, "ex_dups", HashMap::new()).await;
    append_file(&catalog, &dups, consumer_batch(&[Some(1)], &[Some("a")])).await;
    append_file(&catalog, &dups, consumer_batch(&[Some(1)], &[Some("a")])).await;
    append_file(&catalog, &dups, consumer_batch(&[Some(2)], &[Some("b")])).await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1), Some(1)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for(
            "ex_dups",
            "EXISTS (SELECT 1 FROM keys k WHERE k.id = ex_dups.id)",
        ),
    )
    .await
    .expect("correlated duplicate EXISTS");
    assert_eq!(
        read_back(&catalog, &dups).await,
        vec![(Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_not_exists_correlated_some_and_nulls() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let some = create_target(&catalog, "nex_some", HashMap::new()).await;
    append_file(
        &catalog,
        &some,
        consumer_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for(
            "nex_some",
            "NOT EXISTS (SELECT 1 FROM keys k WHERE k.id = nex_some.id)",
        ),
    )
    .await
    .expect("correlated some NOT EXISTS");
    assert_eq!(
        read_back(&catalog, &some).await,
        vec![(Some(2), Some("b".into()))]
    );

    let nulls = create_target(&catalog, "nex_nulls", HashMap::new()).await;
    append_file(
        &catalog,
        &nulls,
        consumer_batch(
            &[Some(1), Some(2), None],
            &[Some("a"), Some("b"), Some("n")],
        ),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2), None]);
    execute_predicate_dml(
        &ctx,
        &catalog,
        &identity_spec_for(
            "nex_nulls",
            "NOT EXISTS (SELECT 1 FROM keys k WHERE k.id = nex_nulls.id)",
        ),
    )
    .await
    .expect("correlated NULL-key NOT EXISTS");
    assert_eq!(
        read_back(&catalog, &nulls).await,
        vec![(Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_delete_exists_honors_write_delete_mode_not_merge_mode() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target(
        &catalog,
        "ex_mor",
        HashMap::from([
            (WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string()),
            ("write.merge.mode".to_string(), "copy-on-write".to_string()),
        ]),
    )
    .await;
    let cow = create_target(
        &catalog,
        "ex_cow",
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

    for name in ["ex_mor", "ex_cow"] {
        let ctx = SessionContext::new();
        register_keys(&ctx, &[Some(1)]);
        let selection = format!("EXISTS (SELECT 1 FROM keys k WHERE k.id = {name}.id)");
        execute_predicate_dml(&ctx, &catalog, &identity_spec_for(name, &selection))
            .await
            .unwrap_or_else(|error| panic!("{name} identity EXISTS DELETE: {error}"));
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
        "MoR EXISTS DELETE must leave original data files in place"
    );
    assert_eq!(
        live_delete_file_count(&catalog, &cow).await,
        0,
        "write.delete.mode=copy-on-write must NOT write position deletes even if merge.mode is MoR"
    );
    let cow_after = live_data_file_paths(&catalog, &cow).await;
    assert_ne!(
        cow_after, cow_data_before,
        "COW EXISTS DELETE must rewrite the affected data file away"
    );
}

async fn table_with_delete_isolation(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    value: Option<&str>,
) -> iceberg::table::Table {
    let mut properties = HashMap::new();
    if let Some(level) = value {
        properties.insert(WRITE_DELETE_ISOLATION_LEVEL.to_string(), level.to_string());
    }
    let ident = create_target(catalog, name, properties).await;
    catalog.load_table(&ident).await.expect("load")
}

/// Isolation-property cases (M19) for `write.delete.isolation-level`. Live resolver
/// semantics (conductor-13 A10): no trim, `to_ascii_lowercase`, default serializable,
/// garbage ⇒ `DataFusionError::Plan` `Invalid isolation level: {name}`.
#[tokio::test]
async fn delete_isolation_property_a10_no_trim_lowercase_default_garbage() {
    use datafusion::error::DataFusionError;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;

    assert_eq!(
        resolve_delete_isolation(&table_with_delete_isolation(&catalog, "def", None).await)
            .expect("default"),
        IsolationLevel::Serializable
    );
    assert_eq!(
        resolve_delete_isolation(
            &table_with_delete_isolation(&catalog, "up", Some("SNAPSHOT")).await,
        )
        .expect("upper"),
        IsolationLevel::Snapshot
    );
    assert_eq!(
        resolve_delete_isolation(
            &table_with_delete_isolation(&catalog, "mix", Some("Serializable")).await,
        )
        .expect("mixed"),
        IsolationLevel::Serializable
    );

    let padded = resolve_delete_isolation(
        &table_with_delete_isolation(&catalog, "pad", Some("  snapshot  ")).await,
    )
    .expect_err("padded is garbage — resolver does not trim");
    match padded {
        DataFusionError::Plan(message) => {
            assert_eq!(message, "Invalid isolation level:   snapshot  ");
        }
        other => panic!("expected Plan, got {other}"),
    }

    let garbage = resolve_delete_isolation(
        &table_with_delete_isolation(&catalog, "garb", Some("read-committed")).await,
    )
    .expect_err("unknown name is loud");
    match garbage {
        DataFusionError::Plan(message) => {
            assert_eq!(message, "Invalid isolation level: read-committed");
        }
        other => panic!("expected Plan, got {other}"),
    }
}
