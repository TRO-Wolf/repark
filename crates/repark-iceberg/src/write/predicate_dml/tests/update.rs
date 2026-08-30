//! Identity-UPDATE pins: multi-column SET, scalar expressions, NULL keys, dups, empty, `MoR` vs COW.

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

use super::super::{
    IsolationLevel, WRITE_UPDATE_ISOLATION_LEVEL, commit_overwrite, resolve_update_isolation, *,
};

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

#[test]
fn allow_list_accepts_uncorrelated_update_in_with_scalar_set() {
    for sql in [
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM ice.sales.keys)",
        "UPDATE ice.sales.tgt SET name = 'z', id = id + 10 WHERE id IN (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt SET name = concat(name, '_x') WHERE id IN (SELECT id FROM keys)",
        "update ice.sales.tgt set name = 'z' where id in ( select id from keys )",
        "UPDATE \"ice\".\"sales\".\"tgt\" SET name = 'z' WHERE id IN (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt t SET t.name = 'z' WHERE t.id IN (SELECT k.id FROM keys k)",
    ] {
        assert!(
            try_allowed_update_in(&parse_statement(sql))
                .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
                .is_some(),
            "must allow {sql:?}"
        );
    }
}

#[test]
fn allow_list_refuses_update_spellings_outside_the_hole() {
    for sql in [
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id NOT IN (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE EXISTS (SELECT 1 FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT k.id FROM keys k \
         WHERE k.id = ice.sales.tgt.id)",
        "UPDATE ice.sales.tgt SET name = (SELECT max(name) FROM keys) \
         WHERE id IN (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id = (SELECT max(id) FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id > ANY (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT max(id) FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id IN (SELECT id FROM (SELECT id FROM keys) x)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id > 1 AND id IN (SELECT id FROM keys)",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM keys)",
        "UPDATE ice.sales.tgt SET name = 'z' WHERE id = 2",
    ] {
        assert!(
            try_allowed_update_in(&parse_statement(sql))
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

fn update_spec(table: &str, assignments: Vec<(&str, &str)>) -> PredicateDmlSpec {
    PredicateDmlSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string()),
        target_alias: table.to_string(),
        selection_sql: "id IN (SELECT id FROM keys)".to_string(),
        assignments: Some(
            assignments
                .into_iter()
                .map(|(column, expr)| (column.to_string(), expr.to_string()))
                .collect(),
        ),
    }
}

#[tokio::test]
async fn identity_update_rewrites_only_the_matching_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "basic", HashMap::new()).await;
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
    execute_predicate_dml(&ctx, &catalog, &update_spec("basic", vec![("v", "'z'")]))
        .await
        .expect("UPDATE IN");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (Some(1), Some("a".into())),
            (Some(2), Some("z".into())),
            (Some(3), Some("c".into())),
        ]
    );
}

#[tokio::test]
async fn identity_update_multi_column_set_and_expression() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "multi", HashMap::new()).await;
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
        &update_spec("multi", vec![("v", "'z'"), ("id", "id + 10")]),
    )
    .await
    .expect("multi-column UPDATE IN");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (Some(1), Some("a".into())),
            (Some(3), Some("c".into())),
            (Some(12), Some("z".into())),
        ]
    );
}

#[tokio::test]
async fn identity_update_empty_subquery_rewrites_nothing() {
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
    register_keys(&ctx, &[]);
    execute_predicate_dml(&ctx, &catalog, &update_spec("empty", vec![("v", "'z'")]))
        .await
        .expect("empty-subquery UPDATE IN");
    assert_eq!(snapshot_id(&catalog, &ident).await, before);
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("a".into())), (Some(2), Some("b".into()))]
    );
}

#[tokio::test]
async fn identity_update_null_target_key_is_unknown() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nulltgt", HashMap::new()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[None, Some(2)], &[Some("n"), Some("b")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(2)]);
    execute_predicate_dml(&ctx, &catalog, &update_spec("nulltgt", vec![("v", "'z'")]))
        .await
        .expect("NULL-target UPDATE IN");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(None, Some("n".into())), (Some(2), Some("z".into()))]
    );
}

#[tokio::test]
async fn identity_update_null_in_subquery_still_matches_the_found_key() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "nullkeys", HashMap::new()).await;
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
    register_keys(&ctx, &[Some(2), None]);
    execute_predicate_dml(&ctx, &catalog, &update_spec("nullkeys", vec![("v", "'z'")]))
        .await
        .expect("NULL-in-keys UPDATE IN");
    // 2 IN (2, NULL) is TRUE; 1/3 IN (2, NULL) is UNKNOWN.
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (Some(1), Some("a".into())),
            (Some(2), Some("z".into())),
            (Some(3), Some("c".into())),
        ]
    );
}

#[tokio::test]
async fn identity_update_duplicate_rows_update_every_copy() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "dups", HashMap::new()).await;
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
    register_keys(&ctx, &[Some(1), Some(1)]);
    execute_predicate_dml(&ctx, &catalog, &update_spec("dups", vec![("v", "'z'")]))
        .await
        .expect("duplicate-row UPDATE IN");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (Some(1), Some("z".into())),
            (Some(1), Some("z".into())),
            (Some(2), Some("keep".into())),
        ]
    );
}

#[tokio::test]
async fn identity_update_honors_write_update_mode_not_merge_or_delete_mode() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target(
        &catalog,
        "upd_mor",
        HashMap::from([
            (WRITE_UPDATE_MODE.to_string(), "merge-on-read".to_string()),
            (WRITE_DELETE_MODE.to_string(), "copy-on-write".to_string()),
            ("write.merge.mode".to_string(), "copy-on-write".to_string()),
        ]),
    )
    .await;
    let cow = create_target(
        &catalog,
        "upd_cow",
        HashMap::from([
            (WRITE_UPDATE_MODE.to_string(), "copy-on-write".to_string()),
            (WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string()),
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

    for name in ["upd_mor", "upd_cow"] {
        let ctx = SessionContext::new();
        register_keys(&ctx, &[Some(1)]);
        execute_predicate_dml(&ctx, &catalog, &update_spec(name, vec![("v", "'z'")]))
            .await
            .unwrap_or_else(|error| panic!("{name} identity UPDATE: {error}"));
    }

    let expected = vec![
        (Some(1), Some("z".into())),
        (Some(2), Some("stay".into())),
        (Some(3), Some("keep".into())),
    ];
    assert_eq!(read_back(&catalog, &mor).await, expected);
    assert_eq!(read_back(&catalog, &cow).await, expected);

    assert!(
        live_delete_file_count(&catalog, &mor).await >= 1,
        "write.update.mode=merge-on-read must commit position deletes (not follow delete/merge mode)"
    );
    let mor_after = live_data_file_paths(&catalog, &mor).await;
    assert!(
        mor_after.len() > mor_data_before.len(),
        "MoR UPDATE must append a new data file (delete-old + insert-new); before={mor_data_before:?} after={mor_after:?}"
    );
    assert_eq!(
        live_delete_file_count(&catalog, &cow).await,
        0,
        "write.update.mode=copy-on-write must NOT write position deletes"
    );
    let cow_after = live_data_file_paths(&catalog, &cow).await;
    assert_ne!(
        cow_after, cow_data_before,
        "COW UPDATE must rewrite the affected data file"
    );
}

/// pins: mw-9-delete-granularity/C-004
#[tokio::test]
async fn unknown_granularity_refuses_identity_update_before_any_parquet_write() {
    use crate::write::position_delete::DELETE_GRANULARITY_PROP;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(
        &catalog,
        "upd_banana",
        HashMap::from([
            (WRITE_UPDATE_MODE.to_string(), "merge-on-read".to_string()),
            (DELETE_GRANULARITY_PROP.to_string(), "banana".to_string()),
        ]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[Some(1)], &[Some("keep")]),
    )
    .await;
    let parquet_before = count_parquet_files(warehouse.path());
    let ctx = SessionContext::new();
    register_keys(&ctx, &[Some(1)]);
    let err = execute_predicate_dml(
        &ctx,
        &catalog,
        &update_spec("upd_banana", vec![("v", "'z'")]),
    )
    .await
    .expect_err("unknown granularity must refuse identity UPDATE")
    .to_string();
    assert!(
        err.contains(DELETE_GRANULARITY_PROP)
            && err.contains("'file'")
            && err.contains("'partition'")
            && err.contains("banana"),
        "refuse must name the property and both legal values: {err}"
    );
    assert_eq!(
        count_parquet_files(warehouse.path()),
        parquet_before,
        "a refused identity UPDATE must not write new parquet"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("keep".into()))]
    );
    assert_eq!(live_delete_file_count(&catalog, &ident).await, 0);
}

fn count_parquet_files(root: &std::path::Path) -> usize {
    let mut stack = vec![root.to_path_buf()];
    let mut count = 0;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                count += 1;
            }
        }
    }
    count
}

struct CorrelatedInCase {
    name: &'static str,
    target_ids: Vec<Option<i32>>,
    key_ids: Vec<Option<i32>>,
    spark_remaining: Vec<Option<i32>>,
}

async fn collect_sorted_ids(ctx: &SessionContext, sql: &str) -> Vec<Option<i32>> {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} plans: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} collects: {error}"));
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id");
        for row in 0..batch.num_rows() {
            ids.push(column.is_valid(row).then(|| column.value(row)));
        }
    }
    ids.sort();
    ids
}

/// A4: identity SELECT remaining set for correlated IN equals correlated EXISTS, matching live
#[tokio::test]
async fn identity_select_correlated_in_matches_exists_and_spark_412() {
    let cases = [
        CorrelatedInCase {
            name: "some",
            target_ids: vec![Some(1), Some(2), Some(3)],
            key_ids: vec![Some(2)],
            spark_remaining: vec![Some(1), Some(3)],
        },
        CorrelatedInCase {
            name: "empty",
            target_ids: vec![Some(1), Some(2), Some(3)],
            key_ids: vec![],
            spark_remaining: vec![Some(1), Some(2), Some(3)],
        },
        CorrelatedInCase {
            name: "null_both",
            target_ids: vec![Some(1), Some(2), None],
            key_ids: vec![Some(2), None],
            spark_remaining: vec![None, Some(1)],
        },
        CorrelatedInCase {
            name: "dups",
            target_ids: vec![Some(1), Some(1), Some(2)],
            key_ids: vec![Some(1), Some(1)],
            spark_remaining: vec![Some(2)],
        },
    ];
    for case in cases {
        let name = case.name;
        let target_ids = case.target_ids;
        let key_ids = case.key_ids;
        let spark_remaining = case.spark_remaining;
        let ctx = SessionContext::new();
        let target_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let target_batch = RecordBatch::try_new(
            Arc::clone(&target_schema),
            vec![Arc::new(Int32Array::from(target_ids.clone()))],
        )
        .expect("target");
        ctx.register_table(
            "tgt",
            Arc::new(MemTable::try_new(target_schema, vec![vec![target_batch]]).expect("tgt")),
        )
        .expect("register tgt");
        register_keys(&ctx, &key_ids);
        let deleted_in = collect_sorted_ids(
            &ctx,
            "SELECT id FROM tgt WHERE id IN (SELECT k.id FROM keys k WHERE k.id = tgt.id)",
        )
        .await;
        let deleted_exists = collect_sorted_ids(
            &ctx,
            "SELECT id FROM tgt WHERE EXISTS (SELECT 1 FROM keys k WHERE k.id = tgt.id)",
        )
        .await;
        assert_eq!(
            deleted_in, deleted_exists,
            "{name}: correlated IN delete-set must equal correlated EXISTS"
        );
        let mut remaining: Vec<Option<i32>> = target_ids
            .into_iter()
            .filter(|id| !deleted_in.contains(id))
            .collect();
        remaining.sort();
        let mut expected = spark_remaining;
        expected.sort();
        assert_eq!(
            remaining, expected,
            "{name}: remaining after correlated IN must match live Spark 4.1.2"
        );
    }
}

#[tokio::test]
async fn identity_delete_correlated_in_deletes_the_key_row() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "corrin", HashMap::new()).await;
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
        &PredicateDmlSpec {
            target: TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "corrin".to_string(),
            ),
            target_alias: "corrin".to_string(),
            selection_sql: "id IN (SELECT k.id FROM keys k WHERE k.id = corrin.id)".to_string(),
            assignments: None,
        },
    )
    .await
    .expect("correlated IN DELETE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(Some(1), Some("a".into())), (Some(3), Some("c".into())),]
    );
}

async fn table_with_update_isolation(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    value: Option<&str>,
) -> iceberg::table::Table {
    let mut properties = HashMap::new();
    if let Some(level) = value {
        properties.insert(WRITE_UPDATE_ISOLATION_LEVEL.to_string(), level.to_string());
    }
    let ident = create_target(catalog, name, properties).await;
    catalog.load_table(&ident).await.expect("load")
}

/// Isolation-property cases (M19) for `write.update.isolation-level`.
#[tokio::test]
async fn update_isolation_property_a10_no_trim_lowercase_default_garbage() {
    use datafusion::error::DataFusionError;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;

    assert_eq!(
        resolve_update_isolation(&table_with_update_isolation(&catalog, "udef", None).await)
            .expect("default"),
        IsolationLevel::Serializable
    );
    assert_eq!(
        resolve_update_isolation(
            &table_with_update_isolation(&catalog, "uup", Some("SNAPSHOT")).await,
        )
        .expect("upper"),
        IsolationLevel::Snapshot
    );
    assert_eq!(
        resolve_update_isolation(
            &table_with_update_isolation(&catalog, "umix", Some("Serializable")).await,
        )
        .expect("mixed"),
        IsolationLevel::Serializable
    );

    let padded = resolve_update_isolation(
        &table_with_update_isolation(&catalog, "upad", Some("  snapshot  ")).await,
    )
    .expect_err("padded is garbage — resolver does not trim");
    match padded {
        DataFusionError::Plan(message) => {
            assert_eq!(message, "Invalid isolation level:   snapshot  ");
        }
        other => panic!("expected Plan, got {other}"),
    }

    let garbage = resolve_update_isolation(
        &table_with_update_isolation(&catalog, "ugarb", Some("read-committed")).await,
    )
    .expect_err("unknown name is loud");
    match garbage {
        DataFusionError::Plan(message) => {
            assert_eq!(message, "Invalid isolation level: read-committed");
        }
        other => panic!("expected Plan, got {other}"),
    }
}

fn synthetic_data_file(path: &str) -> iceberg::spec::DataFile {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build data file")
}

async fn fast_append_files(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    files: Vec<iceberg::spec::DataFile>,
) -> (iceberg::table::Table, i64) {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files);
    let tx = action.apply(tx).expect("apply fast_append");
    let table = tx
        .commit(catalog.as_ref())
        .await
        .expect("commit fast_append");
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    (table, snapshot_id)
}

/// Isolation policy thread (M19): `write.update.isolation-level = serializable` (default) rejects
#[tokio::test]
async fn update_isolation_serializable_rejects_concurrent_append() {
    use datafusion::error::DataFusionError;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(
        &catalog,
        "updser",
        HashMap::from([(
            WRITE_UPDATE_ISOLATION_LEVEL.to_string(),
            "serializable".to_string(),
        )]),
    )
    .await;
    let a = synthetic_data_file("test/a.parquet");
    let (table_at_pin, pin) = fast_append_files(&catalog, &ident, vec![a.clone()]).await;
    let isolation = resolve_update_isolation(&table_at_pin).expect("resolve");
    assert_eq!(isolation, IsolationLevel::Serializable);

    fast_append_files(
        &catalog,
        &ident,
        vec![synthetic_data_file("test/concurrent.parquet")],
    )
    .await;

    let error = commit_overwrite(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![synthetic_data_file("test/a-prime.parquet")],
        isolation,
    )
    .await
    .expect_err("serializable UPDATE must reject a concurrent append");
    let DataFusionError::External(boxed) = error else {
        panic!("expected External(iceberg), got {error}");
    };
    let ice = boxed
        .downcast_ref::<iceberg::Error>()
        .expect("iceberg error");
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(
        ice.message().contains("Found conflicting files"),
        "got: {}",
        ice.message()
    );
}

/// Isolation policy thread (M19): `write.update.isolation-level = SNAPSHOT` (case-folded) commits
#[tokio::test]
async fn update_isolation_snapshot_commits_through_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(
        &catalog,
        "updsnap",
        HashMap::from([(
            WRITE_UPDATE_ISOLATION_LEVEL.to_string(),
            "SNAPSHOT".to_string(),
        )]),
    )
    .await;
    let a = synthetic_data_file("test/a.parquet");
    let (table_at_pin, pin) = fast_append_files(&catalog, &ident, vec![a.clone()]).await;
    let isolation = resolve_update_isolation(&table_at_pin).expect("resolve");
    assert_eq!(isolation, IsolationLevel::Snapshot);

    fast_append_files(
        &catalog,
        &ident,
        vec![synthetic_data_file("test/concurrent.parquet")],
    )
    .await;

    commit_overwrite(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![synthetic_data_file("test/a-prime.parquet")],
        isolation,
    )
    .await
    .expect("snapshot UPDATE must commit through a concurrent append");
}
