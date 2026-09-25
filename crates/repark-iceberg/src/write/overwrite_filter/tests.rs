use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, Operation, PrimitiveType, Schema, Type, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;
use crate::write::concurrency::WriteConcurrency;

fn table_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "d", Type::Primitive(PrimitiveType::Date)).into(),
        ])
        .build()
        .expect("build table schema")
}

fn predicate_expr(sql: &str) -> Expr {
    Parser::new(&DatabricksDialect {})
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_expr())
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
}

fn converted(sql: &str) -> String {
    spark_overwrite_filter(&predicate_expr(sql), &table_schema())
        .unwrap_or_else(|error| panic!("{sql:?} must convert: {error}"))
        .to_string()
}

fn refused(sql: &str) -> String {
    match spark_overwrite_filter(&predicate_expr(sql), &table_schema()) {
        Ok(predicate) => panic!("{sql:?} must refuse, converted to {predicate}"),
        Err(DataFusionError::External(inner)) => {
            assert!(
                inner.is::<crate::write::IllegalArgumentMarker>(),
                "{sql:?} must refuse as an IllegalArgumentException"
            );
            inner.to_string()
        }
        Err(other) => other.to_string(),
    }
}

#[test]
fn spark_translatable_predicates_convert_to_iceberg_filters() {
    let cases = [
        ("cat = 'x'", "cat = \"x\""),
        ("'x' = cat", "cat = \"x\""),
        ("(cat = 'x')", "cat = \"x\""),
        ("id = '2'", "id = 2"),
        ("id = 2.0", "id = 2"),
        ("id >= 1", "id >= 1"),
        ("1 < id", "id > 1"),
        ("cat <> 'x'", "NOT (cat = \"x\")"),
        ("cat != 'y'", "NOT (cat = \"y\")"),
        ("NOT cat = 'y'", "NOT (cat = \"y\")"),
        ("cat IS NULL", "cat IS NULL"),
        ("cat IS NOT NULL", "cat IS NOT NULL"),
        ("cat LIKE 'x%'", "cat STARTS WITH \"x\""),
        ("cat LIKE 'x'", "cat = \"x\""),
        ("id BETWEEN 1 AND 3", "(id >= 1) AND (id <= 3)"),
        ("cat <=> 'x'", "cat = \"x\""),
        ("cat <=> NULL", "cat IS NULL"),
        ("cat = 'x' AND id > 1", "(cat = \"x\") AND (id > 1)"),
        ("cat = 'x' OR id = 2", "(cat = \"x\") OR (id = 2)"),
        ("d = '2024-01-02'", "d = 2024-01-02"),
        ("d = DATE'2024-01-02'", "d = 2024-01-02"),
        ("true", "TRUE"),
        ("false", "FALSE"),
        ("cat = 'x' AND false", "FALSE"),
    ];
    for (sql, expected) in cases {
        assert_eq!(converted(sql), expected, "{sql}");
    }
}

#[test]
fn in_lists_follow_spark_null_semantics() {
    let within = spark_overwrite_filter(&predicate_expr("cat IN ('x', 'y')"), &table_schema())
        .expect("IN converts");
    assert!(matches!(within, Predicate::Set(_)), "{within}");
    let outside = spark_overwrite_filter(&predicate_expr("cat NOT IN ('y')"), &table_schema())
        .expect("NOT IN converts");
    let Predicate::And(parts) = &outside else {
        panic!("NOT IN must pair notNull with notIn, got {outside}");
    };
    let [not_null, not_in] = parts.inputs();
    assert_eq!(not_null.to_string(), "cat IS NOT NULL");
    assert!(
        matches!(not_in, Predicate::Set(set) if set.op() == iceberg::expr::PredicateOperator::NotIn),
        "{not_in}"
    );
    let parenthesised =
        spark_overwrite_filter(&predicate_expr("NOT (cat IN ('y'))"), &table_schema())
            .expect("NOT over a nested IN converts");
    assert_eq!(parenthesised.to_string(), outside.to_string());
    assert!(converted("id IN ('1', '3', NULL)").contains("id IN"));
}

#[test]
fn untranslatable_predicates_refuse_with_the_spark_message() {
    let cases = [
        (
            "upper(cat) = 'X'",
            "Cannot convert Spark predicate to Iceberg expression: upper(cat) = 'X'",
        ),
        (
            "CAST(id AS STRING) = '2'",
            "Cannot convert Spark predicate to Iceberg expression: CAST(id AS STRING) = '2'",
        ),
        (
            "id + 1 = 3",
            "Cannot convert Spark predicate to Iceberg expression: id + 1 = 3",
        ),
        (
            "cat = NULL",
            "Cannot convert Spark predicate to Iceberg expression: null",
        ),
        (
            "cat LIKE '%x'",
            "Cannot convert Spark predicate to Iceberg expression: cat LIKE '%x'",
        ),
        (
            "cat = 'x' AND upper(cat) = 'X'",
            "Cannot convert Spark predicate to Iceberg expression: cat = 'x' AND upper(cat) = 'X'",
        ),
        (
            "NOT (cat = 'x' AND cat IN ('y'))",
            "Cannot convert Spark predicate to Iceberg expression: NOT (cat = 'x' AND cat IN ('y'))",
        ),
        (
            "id = 2.5",
            "Cannot convert Spark predicate to Iceberg expression: id = 2.5",
        ),
        (
            "cat NOT IN ('y', NULL)",
            "Cannot convert Spark predicate to Iceberg expression: cat NOT IN ('y', NULL)",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(refused(sql), expected, "{sql}");
    }
}

#[test]
fn a_reference_in_another_case_fails_the_iceberg_field_lookup() {
    assert_eq!(
        refused("CAT = 'x'"),
        "Error during planning: Cannot find field 'CAT' in struct: struct<1: id: optional long, \
         2: data: optional string, 3: cat: optional string, 4: d: optional date>"
    );
}

async fn seeded_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
    let (catalog, ident) = empty_table(warehouse).await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let staged = stage(&table, &[1, 2, 3], &["a", "b", "c"], &["x", "y", "x"]).await;
    crate::write::commit_append_with_summary(&catalog, &table, staged, &[], None)
        .await
        .expect("seed append");
    (catalog, ident)
}

async fn empty_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
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
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build seeded schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(3, "cat", iceberg::spec::Transform::Identity)
        .expect("identity field")
        .build();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .partition_spec(spec)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    (catalog, TableIdent::new(namespace, "t".to_string()))
}

async fn stage(table: &Table, ids: &[i64], data: &[&str], cats: &[&str]) -> Vec<DataFile> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("cat", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(data.to_vec())),
            Arc::new(StringArray::from(cats.to_vec())),
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

async fn rows(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<(i64, String, String)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("scan builds")
        .to_arrow()
        .await
        .expect("scan streams")
        .try_collect()
        .await
        .expect("scan collects");
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let data = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("data column");
        let cats = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cat column");
        for row in 0..batch.num_rows() {
            out.push((
                ids.value(row),
                data.value(row).to_string(),
                cats.value(row).to_string(),
            ));
        }
    }
    out.sort();
    out
}

async fn replace_where(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    predicate: &str,
    added: &[(i64, &str, &str)],
) -> Result<Table> {
    let table = catalog.load_table(ident).await.expect("load table");
    let filter = spark_overwrite_filter(&predicate_expr(predicate), &table_schema())?;
    let staged = if added.is_empty() {
        Vec::new()
    } else {
        let ids: Vec<i64> = added.iter().map(|row| row.0).collect();
        let data: Vec<&str> = added.iter().map(|row| row.1).collect();
        let cats: Vec<&str> = added.iter().map(|row| row.2).collect();
        stage(&table, &ids, &data, &cats).await
    };
    commit_overwrite_by_filter_with_summary(catalog, &table, staged, filter, None, &[], None).await
}

fn summary_of(table: &Table) -> (Operation, HashMap<String, String>) {
    let snapshot = table.metadata().current_snapshot().expect("a snapshot");
    (
        snapshot.summary().operation.clone(),
        snapshot.summary().additional_properties.clone(),
    )
}

#[tokio::test]
async fn a_whole_partition_filter_replaces_its_rows_as_an_overwrite() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, ident) = seeded_table(&warehouse).await;
    let table = replace_where(&catalog, &ident, "cat = 'x'", &[(9, "z", "x")])
        .await
        .expect("replace where commits");
    let (operation, summary) = summary_of(&table);
    assert_eq!(operation, Operation::Overwrite);
    assert_eq!(summary.get("added-records").map(String::as_str), Some("1"));
    assert_eq!(
        summary.get("deleted-records").map(String::as_str),
        Some("2")
    );
    assert_eq!(
        rows(&catalog, &ident).await,
        vec![(2, "b".into(), "y".into()), (9, "z".into(), "x".into())]
    );
}

#[tokio::test]
async fn added_rows_outside_the_filter_are_not_validated() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, ident) = seeded_table(&warehouse).await;
    replace_where(&catalog, &ident, "cat = 'x'", &[(9, "z", "y")])
        .await
        .expect("Spark does not validate added files against the filter");
    assert_eq!(
        rows(&catalog, &ident).await,
        vec![(2, "b".into(), "y".into()), (9, "z".into(), "y".into())]
    );
}

#[tokio::test]
async fn an_empty_source_commits_a_delete_even_when_nothing_matches() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, ident) = seeded_table(&warehouse).await;
    let table = replace_where(&catalog, &ident, "cat = 'nope'", &[])
        .await
        .expect("an empty replace commits");
    let (operation, summary) = summary_of(&table);
    assert_eq!(operation, Operation::Delete);
    assert_eq!(summary.get("total-records").map(String::as_str), Some("3"));
    assert_eq!(table.metadata().snapshots().count(), 2);
}

#[tokio::test]
async fn a_filter_that_splits_a_file_refuses_before_any_commit() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, ident) = seeded_table(&warehouse).await;
    let error = replace_where(&catalog, &ident, "id = 1", &[(9, "z", "x")])
        .await
        .expect_err("a partial file match refuses");
    let text = error.to_string();
    assert!(
        text.contains("Cannot delete file where some, but not all, rows match filter"),
        "{text}"
    );
    let table = catalog.load_table(&ident).await.expect("load table");
    assert_eq!(table.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn an_empty_source_on_a_table_without_snapshots_commits_a_delete() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, ident) = empty_table(&warehouse).await;
    let table = replace_where(&catalog, &ident, "cat = 'x'", &[])
        .await
        .expect("an empty replace on an empty table commits");
    let (operation, summary) = summary_of(&table);
    assert_eq!(operation, Operation::Delete);
    assert_eq!(summary.get("total-records").map(String::as_str), Some("0"));
    assert_eq!(table.metadata().snapshots().count(), 1);
}
