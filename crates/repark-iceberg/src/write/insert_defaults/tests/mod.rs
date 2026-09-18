use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::io::FileIO;
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, SortOrder, TableMetadataBuilder, Type,
    UnboundPartitionSpec,
};
use iceberg::{Namespace, TableCommit, TableCreation};

use super::*;

fn fill(data_type: ArrowDataType, primitive: PrimitiveLiteral) -> ColumnDefault {
    ColumnDefault {
        data_type,
        primitive,
    }
}

fn parse_one(sql: &str) -> Statement {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse test statement");
    assert_eq!(statements.len(), 1);
    statements.into_iter().next().expect("one statement")
}

#[test]
fn string_default_renders_cast_varchar() {
    let fill = fill(
        ArrowDataType::Utf8,
        PrimitiveLiteral::String("anon".to_string()),
    );
    assert_eq!(fill.sql_text().expect("text"), "CAST('anon' AS VARCHAR)");
    assert_eq!(
        fill.scalar().expect("scalar"),
        ScalarValue::Utf8(Some("anon".to_string()))
    );
}

#[test]
fn int_default_fills() {
    let fill = fill(ArrowDataType::Int32, PrimitiveLiteral::Int(5));
    assert_eq!(fill.sql_text().expect("text"), "CAST(5 AS INT)");
    assert_eq!(fill.scalar().expect("scalar"), ScalarValue::Int32(Some(5)));
}

#[test]
fn exotic_primitive_fails_use_not_load() {
    let fill = fill(ArrowDataType::Utf8, PrimitiveLiteral::AboveMax);
    assert!(fill.scalar().is_err());
    assert!(fill.sql_text().is_err());
}

#[test]
fn column_list_and_target_come_from_insert() {
    let statement = parse_one("INSERT INTO catalog.ns.tbl (id, name) VALUES (1, 'a')");
    assert_eq!(
        insert_column_list(&statement),
        Some(vec!["id".to_string(), "name".to_string()])
    );
    let (catalog, ident) = insert_target(&statement).expect("target");
    assert_eq!(catalog, "catalog");
    assert_eq!(ident.name(), "tbl");
}

#[test]
fn bare_insert_has_no_list_or_target() {
    let statement = parse_one("INSERT INTO tbl VALUES (1, 'a')");
    assert_eq!(insert_column_list(&statement), None);
    assert_eq!(insert_target(&statement), None);
}

#[derive(Debug)]
struct LoadCountingCatalog {
    table: Table,
    loads: AtomicUsize,
}

impl LoadCountingCatalog {
    fn counting(table: Table) -> Arc<Self> {
        Arc::new(Self {
            table,
            loads: AtomicUsize::new(0),
        })
    }

    fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }
}

fn unsupported_catalog() -> iceberg::Error {
    iceberg::Error::new(ErrorKind::FeatureUnsupported, "load-counting test catalog")
}

#[async_trait]
impl Catalog for LoadCountingCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        Err(unsupported_catalog())
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        Err(unsupported_catalog())
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        Err(unsupported_catalog())
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        Err(unsupported_catalog())
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Err(unsupported_catalog())
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        Err(unsupported_catalog())
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        Err(unsupported_catalog())
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        Err(unsupported_catalog())
    }

    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.table.clone())
    }

    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported_catalog())
    }

    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        Err(unsupported_catalog())
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        Err(unsupported_catalog())
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        Err(unsupported_catalog())
    }

    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        Err(unsupported_catalog())
    }
}

fn memory_table(fields: Vec<Arc<NestedField>>) -> Table {
    let schema = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("test schema");
    let built = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "memory://insert-defaults-load".to_string(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("metadata");
    Table::builder()
        .file_io(FileIO::new_with_memory())
        .metadata(built.metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .build()
        .expect("table")
}

fn plain_table() -> Table {
    memory_table(vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
    ])
}

fn defaulted_table() -> Table {
    memory_table(vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))
            .with_write_default(Literal::int(5))
            .into(),
    ])
}

fn table_ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

fn insert_plan(name: &str) -> LogicalPlan {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        ],
    )
    .expect("empty batch");
    let provider = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    let input = LogicalPlanBuilder::empty(false)
        .build()
        .expect("empty input plan");
    LogicalPlanBuilder::insert_into(
        input,
        TableReference::full("memory", "sales", name),
        source,
        InsertOp::Append,
    )
    .expect("insert plan")
    .build()
    .expect("built insert plan")
}

#[tokio::test]
async fn insert_without_default_marker_skips_catalog_load() {
    let counting = LoadCountingCatalog::counting(plain_table());
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ident = table_ident("plain");
    let mut statement = parse_one("INSERT INTO memory.sales.plain (id, name) VALUES (1, 'a')");
    let outcome = rewrite_insert_markers(&catalog, &ident, &mut statement)
        .await
        .expect("rewrite without marker");
    assert!(outcome.rewritten.is_none());
    assert!(outcome.preloaded.is_none());
    assert_eq!(counting.loads(), 0);
    let listed = vec!["id".to_string(), "name".to_string()];
    fill_insert_plan(&catalog, &ident, Some(&listed), insert_plan("plain"), None)
        .await
        .expect("fill loads its own table");
    assert_eq!(counting.loads(), 1);
}

#[tokio::test]
async fn fill_reuses_marker_pass_table_without_second_load() {
    let counting = LoadCountingCatalog::counting(defaulted_table());
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ident = table_ident("defaults");
    let mut statement = parse_one("INSERT INTO memory.sales.defaults VALUES (1, 'a', DEFAULT)");
    let outcome = rewrite_insert_markers(&catalog, &ident, &mut statement)
        .await
        .expect("rewrite with marker");
    assert_eq!(counting.loads(), 1);
    let rewritten = outcome.rewritten.expect("marker substitutes");
    assert!(
        rewritten.contains("CAST(5 AS INT)"),
        "default fill renders: {rewritten}"
    );
    let listed = vec!["id".to_string(), "name".to_string(), "c".to_string()];
    fill_insert_plan(
        &catalog,
        &ident,
        Some(&listed),
        insert_plan("defaults"),
        outcome.preloaded,
    )
    .await
    .expect("fill reuses the loaded table");
    assert_eq!(counting.loads(), 1);
    fill_insert_plan(
        &catalog,
        &ident,
        Some(&listed),
        insert_plan("defaults"),
        None,
    )
    .await
    .expect("fill without preload loads");
    assert_eq!(counting.loads(), 2);
}

#[test]
fn decimal_and_date_literals_render() {
    let decimal = fill(
        ArrowDataType::Decimal128(10, 2),
        PrimitiveLiteral::Int128(314),
    );
    assert_eq!(
        decimal.sql_text().expect("d text"),
        "CAST('3.14' AS DECIMAL(10,2))"
    );
    assert_eq!(
        decimal.scalar().expect("d scalar"),
        ScalarValue::Decimal128(Some(314), 10, 2)
    );
    let date = fill(ArrowDataType::Date32, PrimitiveLiteral::Int(20_000));
    assert_eq!(
        date.sql_text().expect("dt text"),
        "CAST('2024-10-04' AS DATE)"
    );
    assert_eq!(
        date.scalar().expect("dt scalar"),
        ScalarValue::Date32(Some(20_000))
    );
}
