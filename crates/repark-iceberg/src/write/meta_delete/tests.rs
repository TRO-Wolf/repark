use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{ListType, NestedField, Operation, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;
use crate::write::predicate_dml::{PredicateDmlSpec, execute_predicate_dml};

const MERGE_ON_READ: &str = "merge-on-read";
const COPY_ON_WRITE: &str = "copy-on-write";

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

fn target_of(sql: &str) -> MetaDeleteTarget {
    try_meta_delete_target(&parse_statement(sql))
        .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
        .unwrap_or_else(|| panic!("{sql:?} must claim a metadata-delete target"))
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

fn target_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build target schema")
}

fn schema_with_a_list_column() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(
                3,
                "tags",
                Type::List(ListType {
                    element_field: NestedField::list_element(
                        4,
                        Type::Primitive(PrimitiveType::String),
                        true,
                    )
                    .into(),
                }),
            )
            .into(),
        ])
        .build()
        .expect("build list schema")
}

async fn create_target(catalog: &Arc<dyn Catalog>, name: &str, delete_mode: &str) -> TableIdent {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(target_schema())
        .properties(HashMap::from([(
            "write.delete.mode".to_string(),
            delete_mode.to_string(),
        )]))
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create target table");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

fn batch(ids: &[i32], values: &[&str]) -> RecordBatch {
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
    .expect("data batch builds")
}

async fn append_file(catalog: &Arc<dyn Catalog>, ident: &TableIdent, ids: &[i32], values: &[&str]) {
    crate::write::append::append(catalog, ident, vec![batch(ids, values)])
        .await
        .expect("append a data file");
}

async fn seed_two_files(catalog: &Arc<dyn Catalog>, name: &str, delete_mode: &str) -> TableIdent {
    let ident = create_target(catalog, name, delete_mode).await;
    append_file(catalog, &ident, &[1, 2, 3], &["a", "b", "c"]).await;
    append_file(catalog, &ident, &[7], &["x"]).await;
    ident
}

async fn read_ids(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<i32> {
    let table = catalog.load_table(ident).await.expect("load table");
    let scan = table.scan().select(["id"]).build().expect("scan builds");
    let batches: Vec<RecordBatch> = scan
        .to_arrow()
        .await
        .expect("scan stream")
        .try_collect()
        .await
        .expect("collect scan");
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column is int32");
        for index in 0..batch.num_rows() {
            ids.push(column.value(index));
        }
    }
    ids.sort_unstable();
    ids
}

async fn snapshot_log(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> Vec<(Operation, HashMap<String, String>)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let mut ordered: Vec<_> = table.metadata().snapshots().collect();
    ordered.sort_by_key(|snapshot| snapshot.sequence_number());
    ordered
        .into_iter()
        .map(|snapshot| {
            (
                snapshot.summary().operation.clone(),
                snapshot.summary().additional_properties.clone(),
            )
        })
        .collect()
}

fn summary_value(summary: &HashMap<String, String>, key: &str) -> Option<String> {
    summary.get(key).cloned()
}

/// pins: ice-meta-delete-1/C-001
#[tokio::test]
async fn a_whole_file_delete_removes_the_file_in_both_delete_modes() {
    for mode in [MERGE_ON_READ, COPY_ON_WRITE] {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = seed_two_files(&catalog, "whole", mode).await;
        let target = target_of("DELETE FROM ice.sales.whole WHERE id = 7");

        assert!(
            try_metadata_delete(&catalog, &target, true)
                .await
                .expect("metadata delete runs"),
            "mode={mode}: a whole-file predicate is answered from metadata"
        );

        assert_eq!(read_ids(&catalog, &ident).await, vec![1, 2, 3], "{mode}");
        let log = snapshot_log(&catalog, &ident).await;
        assert_eq!(
            log.len(),
            3,
            "{mode}: one delete snapshot after two appends"
        );
        let (operation, summary) = log.last().expect("a last snapshot");
        assert_eq!(*operation, Operation::Delete, "{mode}");
        assert_eq!(
            summary_value(summary, "deleted-data-files").as_deref(),
            Some("1"),
            "{mode}"
        );
        assert_eq!(
            summary_value(summary, "deleted-records").as_deref(),
            Some("1"),
            "{mode}"
        );
        assert_eq!(
            summary_value(summary, "total-delete-files").as_deref(),
            Some("0"),
            "{mode}"
        );
        assert_eq!(
            summary_value(summary, "total-position-deletes").as_deref(),
            Some("0"),
            "{mode}"
        );
        assert_eq!(summary_value(summary, "added-delete-files"), None, "{mode}");
        assert_eq!(summary_value(summary, "added-data-files"), None, "{mode}");
    }
}

/// pins: ice-meta-delete-1/C-002
#[tokio::test]
async fn a_no_match_delete_commits_an_empty_delete_snapshot() {
    for mode in [MERGE_ON_READ, COPY_ON_WRITE] {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = seed_two_files(&catalog, "nomatch", mode).await;
        let target = target_of("DELETE FROM ice.sales.nomatch WHERE id = 99");

        assert!(
            try_metadata_delete(&catalog, &target, true)
                .await
                .expect("metadata delete runs"),
            "{mode}: a predicate that plans no file is vacuously metadata-only"
        );

        assert_eq!(read_ids(&catalog, &ident).await, vec![1, 2, 3, 7], "{mode}");
        let log = snapshot_log(&catalog, &ident).await;
        assert_eq!(log.len(), 3, "{mode}: the empty delete is still a snapshot");
        let (operation, summary) = log.last().expect("a last snapshot");
        assert_eq!(*operation, Operation::Delete, "{mode}");
        assert_eq!(summary_value(summary, "deleted-data-files"), None, "{mode}");
        assert_eq!(summary_value(summary, "added-data-files"), None, "{mode}");
        assert_eq!(summary_value(summary, "added-delete-files"), None, "{mode}");
        assert_eq!(
            summary_value(summary, "total-data-files").as_deref(),
            Some("2"),
            "{mode}"
        );
        assert_eq!(
            summary_value(summary, "total-records").as_deref(),
            Some("4"),
            "{mode}"
        );
    }
}

/// pins: ice-meta-delete-1/C-003
#[tokio::test]
async fn a_partial_match_declines_before_any_commit() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = seed_two_files(&catalog, "partial", MERGE_ON_READ).await;

    for sql in [
        "DELETE FROM ice.sales.partial WHERE id = 2",
        "DELETE FROM ice.sales.partial WHERE id >= 3",
    ] {
        let target = target_of(sql);
        assert!(
            plan_metadata_delete(&catalog, &target, true)
                .await
                .expect("the decision runs")
                .is_none(),
            "{sql:?} matches part of a file, so the decision refuses BEFORE the commit"
        );
        assert!(
            !try_metadata_delete(&catalog, &target, true)
                .await
                .expect("the decision runs"),
            "{sql:?}"
        );
    }

    assert_eq!(read_ids(&catalog, &ident).await, vec![1, 2, 3, 7]);
    assert_eq!(
        snapshot_log(&catalog, &ident).await.len(),
        2,
        "a declined decision commits nothing at all"
    );
}

/// pins: ice-meta-delete-1/C-004
#[tokio::test]
async fn no_predicate_and_literal_true_delete_every_file() {
    for (name, sql) in [
        ("allnopred", "DELETE FROM ice.sales.allnopred"),
        ("alltrue", "DELETE FROM ice.sales.alltrue WHERE true"),
    ] {
        let warehouse = TempDir::new().expect("temp warehouse");
        let catalog = memory_catalog(&warehouse).await;
        let ident = seed_two_files(&catalog, name, MERGE_ON_READ).await;
        let target = target_of(sql);

        assert!(
            try_metadata_delete(&catalog, &target, true)
                .await
                .expect("metadata delete runs"),
            "{sql:?}"
        );

        assert!(read_ids(&catalog, &ident).await.is_empty(), "{sql:?}");
        let log = snapshot_log(&catalog, &ident).await;
        let (operation, summary) = log.last().expect("a last snapshot");
        assert_eq!(*operation, Operation::Delete, "{sql:?}");
        assert_eq!(
            summary_value(summary, "deleted-data-files").as_deref(),
            Some("2"),
            "{sql:?}"
        );
        assert_eq!(
            summary_value(summary, "deleted-records").as_deref(),
            Some("4"),
            "{sql:?}"
        );
        assert_eq!(
            summary_value(summary, "total-data-files").as_deref(),
            Some("0"),
            "{sql:?}"
        );
    }
}

/// pins: ice-meta-delete-1/C-005
#[tokio::test]
async fn a_prior_position_delete_does_not_change_the_route() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = seed_two_files(&catalog, "prior", MERGE_ON_READ).await;
    let context = SessionContext::new();
    let spec = PredicateDmlSpec {
        target: ident.clone(),
        target_alias: "prior".to_string(),
        selection_sql: "id = 1".to_string(),
        assignments: None,
        case_insensitive: false,
    };
    execute_predicate_dml(&context, &catalog, &spec)
        .await
        .expect("the row-level delete commits");
    assert_eq!(read_ids(&catalog, &ident).await, vec![2, 3, 7]);

    let target = target_of("DELETE FROM ice.sales.prior WHERE id <= 3");
    assert!(
        try_metadata_delete(&catalog, &target, true)
            .await
            .expect("metadata delete runs"),
        "the live delete file does not push the whole-file delete off the metadata route"
    );

    assert_eq!(read_ids(&catalog, &ident).await, vec![7]);
    let log = snapshot_log(&catalog, &ident).await;
    let (operation, summary) = log.last().expect("a last snapshot");
    assert_eq!(*operation, Operation::Delete);
    assert_eq!(
        summary_value(summary, "deleted-data-files").as_deref(),
        Some("1")
    );
    assert_eq!(
        summary_value(summary, "deleted-records").as_deref(),
        Some("3")
    );
}

/// pins: ice-meta-delete-1/C-006
#[tokio::test]
async fn the_decision_binds_columns_with_the_doors_case_sensitivity() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = seed_two_files(&catalog, "cased", MERGE_ON_READ).await;
    let schema = target_schema();
    let unquoted = target_of("DELETE FROM ice.sales.cased WHERE ID = 7");
    let quoted = target_of("DELETE FROM ice.sales.cased WHERE \"ID\" = 7");

    assert!(
        delete_predicate(&unquoted, &schema, true).is_some(),
        "a case-insensitive door folds `ID` onto `id`, as the Spark door does"
    );
    assert!(
        delete_predicate(&quoted, &schema, true).is_some(),
        "the Spark door folds a quoted reference too"
    );
    assert!(
        delete_predicate(&unquoted, &schema, false).is_some(),
        "an exact door lower-cases an UNQUOTED reference, as its planner does"
    );
    assert!(
        delete_predicate(&quoted, &schema, false).is_none(),
        "an exact door binds a QUOTED reference verbatim, so `\"ID\"` finds no column"
    );
    assert!(
        !try_metadata_delete(&catalog, &quoted, false)
            .await
            .expect("the decision runs"),
        "the exact door declines the quoted reference rather than deleting a file"
    );
    assert_eq!(read_ids(&catalog, &ident).await, vec![1, 2, 3, 7]);
    assert!(
        try_metadata_delete(&catalog, &unquoted, false)
            .await
            .expect("the decision runs"),
        "the unquoted reference takes the same route the lower-cased spelling does"
    );
    assert_eq!(read_ids(&catalog, &ident).await, vec![1, 2, 3]);
}

/// pins: ice-meta-delete-1/C-007
#[test]
fn a_branch_selector_and_every_non_identity_clause_decline() {
    for sql in [
        "DELETE FROM ice.sales.tgt.branch_b WHERE id = 7",
        "DELETE FROM sales.tgt WHERE id = 7",
        "DELETE FROM ice.sales.tgt USING ice.sales.keys WHERE id = 7",
        "DELETE FROM ice.sales.tgt WHERE id = 7 RETURNING *",
        "DELETE FROM ice.sales.tgt WHERE id = 7 LIMIT 1",
        "UPDATE ice.sales.tgt SET v = 'z' WHERE id = 7",
    ] {
        assert!(
            try_meta_delete_target(&parse_statement(sql))
                .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
                .is_none(),
            "{sql:?} must not claim a metadata delete"
        );
    }
    assert!(
        try_meta_delete_target(&parse_statement("DELETE FROM ice.sales.tgt WHERE id = 7"))
            .expect("the claim runs")
            .is_some()
    );
}

/// pins: ice-meta-delete-1/C-003
#[test]
fn the_translation_keeps_spark_answerable_shapes_and_refuses_the_rest() {
    let schema = target_schema();
    for sql in [
        "DELETE FROM ice.sales.tgt WHERE id = 7",
        "DELETE FROM ice.sales.tgt WHERE id >= 7",
        "DELETE FROM ice.sales.tgt WHERE 7 < id",
        "DELETE FROM ice.sales.tgt WHERE v = 'x'",
        "DELETE FROM ice.sales.tgt WHERE v IS NULL",
        "DELETE FROM ice.sales.tgt WHERE v IS NOT NULL",
        "DELETE FROM ice.sales.tgt WHERE id IN (1, 2, 3)",
        "DELETE FROM ice.sales.tgt WHERE v LIKE 'x%'",
        "DELETE FROM ice.sales.tgt WHERE id = 7 OR id = 99",
        "DELETE FROM ice.sales.tgt WHERE v = 'x' AND id <= 2",
        "DELETE FROM ice.sales.tgt t WHERE t.id = 7",
        "DELETE FROM ice.sales.tgt WHERE true",
        "DELETE FROM ice.sales.tgt",
    ] {
        assert!(
            delete_predicate(&target_of(sql), &schema, true).is_some(),
            "{sql:?} is one of Spark's convertible predicates"
        );
    }
    assert!(
        delete_predicate(
            &target_of("DELETE FROM ice.sales.tgt WHERE tags IS NULL"),
            &schema_with_a_list_column(),
            true
        )
        .is_none(),
        "a list column never reaches the fork's binder"
    );
    for sql in [
        "DELETE FROM ice.sales.tgt WHERE id NOT IN (1, 2, 3)",
        "DELETE FROM ice.sales.tgt WHERE id <> 7",
        "DELETE FROM ice.sales.tgt WHERE id != 7",
        "DELETE FROM ice.sales.tgt WHERE NOT (id = 7)",
        "DELETE FROM ice.sales.tgt WHERE v NOT LIKE 'x%'",
        "DELETE FROM ice.sales.tgt WHERE v LIKE '%x'",
        "DELETE FROM ice.sales.tgt WHERE v LIKE 'x_%'",
        "DELETE FROM ice.sales.tgt WHERE false",
        "DELETE FROM ice.sales.tgt WHERE id BETWEEN 1 AND 3",
        "DELETE FROM ice.sales.tgt WHERE nosuch = 7",
        "DELETE FROM ice.sales.tgt WHERE id + 1 = 8",
        "DELETE FROM ice.sales.tgt WHERE id = id",
        "DELETE FROM ice.sales.tgt WHERE upper(v) = 'X'",
        "DELETE FROM ice.sales.tgt WHERE id IN (SELECT id FROM ice.sales.keys)",
        "DELETE FROM ice.sales.tgt s WHERE t.id = 7",
    ] {
        assert!(
            delete_predicate(&target_of(sql), &schema, true).is_none(),
            "{sql:?} must keep the row-level route"
        );
    }
}
