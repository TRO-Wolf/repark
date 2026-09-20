use super::*;
use crate::catalog::memory_catalog;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Type};
use iceberg::table::Table;
use iceberg::{CatalogBuilder, MemoryCatalog, Namespace, TableCommit, TableCreation};
use tempfile::TempDir;

const SQL: &str = "SELECT id, data FROM t WHERE id > 0";

fn int_string_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap_or_else(|error| panic!("fixture schema must build: {error}"))
}

fn arrow_output() -> ArrowSchema {
    ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("data", DataType::Utf8, true),
    ])
}

fn definition(warehouse: &TempDir, sql: &str) -> ViewDefinition {
    ViewDefinition {
        sql: sql.to_string(),
        schema: int_string_schema(),
        properties: HashMap::new(),
        warehouse: warehouse
            .path()
            .to_str()
            .unwrap_or_else(|| panic!("warehouse path must be utf8"))
            .to_string(),
        location_override: None,
        default_catalog: "sc".to_string(),
    }
}

async fn catalog_with_namespace(
    warehouse: &TempDir,
    namespace: &str,
) -> std::sync::Arc<dyn Catalog> {
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    catalog
        .create_namespace(&NamespaceIdent::new(namespace.to_string()), HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("namespace must create: {error}"));
    catalog
}

#[tokio::test]
async fn create_view_stores_sql_schema_defaults_and_location() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "vw",
    };
    let view = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .unwrap_or_else(|error| panic!("view must create: {error}"))
        .unwrap_or_else(|| panic!("plain create must return the view"));
    let metadata = view.metadata();
    assert_eq!(
        metadata.current_version().representations().len(),
        1,
        "pins: ice-views-1/C-001"
    );
    assert_eq!(
        view_read_spec(&view)
            .unwrap_or_else(|error| panic!("read spec must build: {error}"))
            .sql,
        SQL,
        "pins: ice-views-1/C-001"
    );
    assert_eq!(
        metadata.current_schema().as_struct().fields()[1].name,
        "data",
        "pins: ice-views-1/C-001"
    );
    assert_eq!(
        metadata.current_version().default_catalog(),
        Some(&"sc".to_string()),
        "pins: ice-views-1/C-001"
    );
    assert_eq!(
        metadata.current_version().default_namespace(),
        &NamespaceIdent::new("ns".to_string()),
        "pins: ice-views-1/C-001"
    );
    let expected_location = format!(
        "{}/ns/vw",
        warehouse.path().to_str().unwrap_or_else(|| {
            panic!("warehouse path must be utf8");
        })
    );
    assert_eq!(
        metadata.location(),
        expected_location,
        "pins: ice-views-1/C-001"
    );
}

#[tokio::test]
async fn duplicate_create_view_reports_view_already_exists() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .unwrap_or_else(|error| panic!("view must create: {error}"));
    let error = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .expect_err("a duplicate create must fail");
    let message = error.to_string();
    assert!(
        message.contains("[VIEW_ALREADY_EXISTS]"),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("Cannot create view ns.v1 because it already exists."),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("SQLSTATE: 42P07"),
        "pins: ice-views-1/C-010, got: {message}"
    );
}

#[tokio::test]
async fn create_view_over_a_table_reports_view_already_exists() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let creation = iceberg::TableCreation::builder()
        .name("t".to_string())
        .location(format!(
            "{}/t",
            warehouse.path().to_str().unwrap_or_else(|| {
                panic!("warehouse path must be utf8");
            })
        ))
        .schema(int_string_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .unwrap_or_else(|error| panic!("table must create: {error}"));
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "t",
    };
    let error = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .expect_err("a create over a table must fail");
    assert!(
        error.to_string().contains("[VIEW_ALREADY_EXISTS]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn create_view_if_not_exists_is_a_noop_that_keeps_the_body() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .unwrap_or_else(|error| panic!("view must create: {error}"));
    let outcome = create_or_replace_view(
        &target,
        false,
        true,
        definition(&warehouse, "SELECT 99 AS id"),
    )
    .await
    .unwrap_or_else(|error| panic!("if-not-exists must not fail: {error}"));
    assert!(outcome.is_none(), "pins: ice-views-1/C-004");
    let view = catalog
        .load_view(&TableIdent::new(namespace, "v1".to_string()))
        .await
        .unwrap_or_else(|error| panic!("view must load: {error}"));
    assert_eq!(
        view_read_spec(&view)
            .unwrap_or_else(|error| panic!("read spec must build: {error}"))
            .sql,
        SQL,
        "pins: ice-views-1/C-004"
    );
}

#[tokio::test]
async fn create_or_replace_adds_a_version_and_moves_current() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    let created = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .unwrap_or_else(|error| panic!("view must create: {error}"))
        .unwrap_or_else(|| panic!("create must return the view"));
    let created_current = created.metadata().current_version_id();
    let replaced = create_or_replace_view(
        &target,
        true,
        false,
        definition(&warehouse, "SELECT id FROM t"),
    )
    .await
    .unwrap_or_else(|error| panic!("replace must succeed: {error}"))
    .unwrap_or_else(|| panic!("replace must return the view"));
    let metadata = replaced.metadata();
    assert_eq!(metadata.versions().len(), 2, "pins: ice-views-1/C-002");
    assert_ne!(
        metadata.current_version_id(),
        created_current,
        "pins: ice-views-1/C-002"
    );
    assert_eq!(
        view_read_spec(&replaced)
            .unwrap_or_else(|error| panic!("read spec must build: {error}"))
            .sql,
        "SELECT id FROM t",
        "pins: ice-views-1/C-002"
    );
}

#[tokio::test]
async fn create_or_replace_over_a_table_reports_view_already_exists() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let creation = iceberg::TableCreation::builder()
        .name("t".to_string())
        .location(format!(
            "{}/t",
            warehouse.path().to_str().unwrap_or_else(|| {
                panic!("warehouse path must be utf8");
            })
        ))
        .schema(int_string_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .unwrap_or_else(|error| panic!("table must create: {error}"));
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "t",
    };
    let error = create_or_replace_view(&target, true, false, definition(&warehouse, SQL))
        .await
        .expect_err("replace over a table must fail");
    assert!(
        error.to_string().contains("[VIEW_ALREADY_EXISTS]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn drop_view_reports_view_not_found_for_a_table_name() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let creation = iceberg::TableCreation::builder()
        .name("t".to_string())
        .location(format!(
            "{}/t",
            warehouse.path().to_str().unwrap_or_else(|| {
                panic!("warehouse path must be utf8");
            })
        ))
        .schema(int_string_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .unwrap_or_else(|error| panic!("table must create: {error}"));
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "t",
    };
    let error = drop_catalog_view(&target, false)
        .await
        .expect_err("dropping a table as a view must fail");
    let message = error.to_string();
    assert!(
        message.contains("[VIEW_NOT_FOUND]"),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("The view ns.t cannot be found."),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("SQLSTATE: 42P01"),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        catalog
            .table_exists(&TableIdent::new(namespace, "t".to_string()))
            .await
            .unwrap_or_else(|error| panic!("table check must run: {error}")),
        "pins: ice-views-1/C-010"
    );
}

#[tokio::test]
async fn drop_view_if_exists_still_refuses_a_table_name() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let creation = iceberg::TableCreation::builder()
        .name("t".to_string())
        .location(format!(
            "{}/t",
            warehouse.path().to_str().unwrap_or_else(|| {
                panic!("warehouse path must be utf8");
            })
        ))
        .schema(int_string_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .unwrap_or_else(|error| panic!("table must create: {error}"));
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "t",
    };
    let error = drop_catalog_view(&target, true)
        .await
        .expect_err("if-exists tolerates absence, not a present table");
    assert!(
        error.to_string().contains("[VIEW_NOT_FOUND]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn drop_view_if_exists_tolerates_a_missing_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("missing".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "missing",
        view_name: "v",
    };
    drop_catalog_view(&target, true)
        .await
        .unwrap_or_else(|error| panic!("if-exists drop must pass: {error}"));
    let error = drop_catalog_view(&target, false)
        .await
        .expect_err("a bare missing drop must fail");
    assert!(
        error.to_string().contains("[VIEW_NOT_FOUND]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn drop_view_if_exists_tolerates_a_missing_view() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "missing",
    };
    drop_catalog_view(&target, true)
        .await
        .unwrap_or_else(|error| panic!("if-exists drop must pass: {error}"));
    let error = drop_catalog_view(&target, false)
        .await
        .expect_err("a bare missing drop must fail");
    assert!(
        error.to_string().contains("[VIEW_NOT_FOUND]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn list_views_reports_schema_not_found_for_a_missing_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let error = list_catalog_views(
        "sc",
        catalog.as_ref(),
        &NamespaceIdent::new("missing".to_string()),
        "missing",
    )
    .await
    .expect_err("listing a missing namespace must fail");
    assert!(
        error.to_string().contains("[SCHEMA_NOT_FOUND]"),
        "pins: ice-views-1/C-003, got: {error}"
    );
}

#[tokio::test]
async fn create_view_in_a_missing_namespace_reports_schema_not_found() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("missing".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "missing",
        view_name: "v1",
    };
    let error = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .expect_err("a create in a missing namespace must fail");
    assert!(
        error.to_string().contains("[SCHEMA_NOT_FOUND]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[test]
fn split_view_properties_validates_provider_and_extracts_location() {
    let mut tblproperties = HashMap::new();
    tblproperties.insert("k".to_string(), "v".to_string());
    tblproperties.insert("provider".to_string(), "iceberg".to_string());
    tblproperties.insert("format-version".to_string(), "9".to_string());
    tblproperties.insert("location".to_string(), "/tmp/custom".to_string());
    let (stored, location) =
        split_view_properties(Some("hello"), &tblproperties).unwrap_or_else(|error| {
            panic!("valid properties must split: {error}");
        });
    assert_eq!(stored.get("k"), Some(&"v".to_string()));
    assert_eq!(stored.get("comment"), Some(&"hello".to_string()));
    assert!(!stored.contains_key("provider"));
    assert!(!stored.contains_key("format-version"));
    assert!(!stored.contains_key("location"));
    assert_eq!(location, Some("/tmp/custom".to_string()));
}

#[test]
fn split_view_properties_refuses_a_non_iceberg_provider() {
    let mut tblproperties = HashMap::new();
    tblproperties.insert("provider".to_string(), "x".to_string());
    let error =
        split_view_properties(None, &tblproperties).expect_err("a foreign provider must refuse");
    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(
        error.to_string().contains("Unsupported format in USING: x"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[test]
fn split_view_properties_lets_tblproperties_win_over_comment() {
    let mut tblproperties = HashMap::new();
    tblproperties.insert("comment".to_string(), "from-map".to_string());
    let (stored, _) = split_view_properties(Some("from-clause"), &tblproperties)
        .unwrap_or_else(|error| panic!("properties must split: {error}"));
    assert_eq!(stored.get("comment"), Some(&"from-map".to_string()));
}

#[test]
fn view_schema_for_output_applies_aliases_and_docs() {
    let aliases = vec![
        ("i".to_string(), Some("the id".to_string())),
        ("d".to_string(), None),
    ];
    let schema = view_schema_for_output(&arrow_output(), &aliases, "sc.ns.v1")
        .unwrap_or_else(|error| panic!("schema must build: {error}"));
    let fields = schema.as_struct().fields();
    assert_eq!(fields[0].name, "i");
    assert_eq!(fields[1].name, "d");
    assert_eq!(fields[0].doc, Some("the id".to_string()));
    assert_eq!(fields[1].doc, None);
}

#[test]
fn view_schema_for_output_reports_arity_mismatches() {
    let too_many_aliases = vec![
        ("a".to_string(), None),
        ("b".to_string(), None),
        ("c".to_string(), None),
    ];
    let error = view_schema_for_output(&arrow_output(), &too_many_aliases, "sc.ns.va")
        .expect_err("extra aliases must fail");
    let message = error.to_string();
    assert!(
        message.contains("[CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]"),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("View columns: a, b, c."),
        "pins: ice-views-1/C-010, got: {message}"
    );
    assert!(
        message.contains("SQLSTATE: 21S01"),
        "pins: ice-views-1/C-010, got: {message}"
    );
    let too_few_aliases = vec![("a".to_string(), None)];
    let error = view_schema_for_output(&arrow_output(), &too_few_aliases, "sc.ns.va")
        .expect_err("missing aliases must fail");
    assert!(
        error
            .to_string()
            .contains("[CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]"),
        "pins: ice-views-1/C-010, got: {error}"
    );
}

#[tokio::test]
async fn view_read_spec_reports_sql_defaults_and_columns() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "vw",
    };
    let view = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .unwrap_or_else(|error| panic!("view must create: {error}"))
        .unwrap_or_else(|| panic!("plain create must return the view"));
    let spec = view_read_spec(&view).unwrap_or_else(|error| panic!("spec: {error}"));
    assert_eq!(spec.sql, SQL);
    assert_eq!(spec.default_catalog, Some("sc".to_string()));
    assert_eq!(
        spec.default_namespace,
        NamespaceIdent::new("ns".to_string())
    );
    assert_eq!(
        spec.column_names,
        vec!["id".to_string(), "data".to_string()]
    );
}

#[derive(Debug)]
struct ViewlessCatalog {
    inner: MemoryCatalog,
}

impl ViewlessCatalog {
    async fn with_namespace(warehouse: &TempDir, namespace: &str) -> Self {
        let path = warehouse
            .path()
            .to_str()
            .unwrap_or_else(|| panic!("warehouse path must be utf8"))
            .to_string();
        let inner = MemoryCatalogBuilder::default()
            .with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory))
            .load(
                "viewless",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .unwrap_or_else(|error| panic!("viewless catalog must build: {error}"));
        inner
            .create_namespace(&NamespaceIdent::new(namespace.to_string()), HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("namespace must create: {error}"));
        Self { inner }
    }
}

#[async_trait]
impl Catalog for ViewlessCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

#[tokio::test]
async fn create_view_on_a_viewless_catalog_reports_creating_unsupported() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = ViewlessCatalog::with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "glue",
        catalog: &catalog,
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    let error = create_or_replace_view(&target, false, false, definition(&warehouse, SQL))
        .await
        .expect_err("a create on a viewless catalog must fail");
    let message = error.to_string();
    assert!(
        message.contains("Creating a view is not supported by catalog: glue"),
        "pins: ice-views-1/C-005, got: {message}"
    );
}

#[tokio::test]
async fn replace_view_on_a_viewless_catalog_reports_replacing_unsupported() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = ViewlessCatalog::with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "glue",
        catalog: &catalog,
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    let error = create_or_replace_view(&target, true, false, definition(&warehouse, SQL))
        .await
        .expect_err("a replace on a viewless catalog must fail");
    let message = error.to_string();
    assert!(
        message.contains("Replacing a view is not supported by catalog: glue"),
        "pins: ice-views-1/C-005, got: {message}"
    );
}

#[tokio::test]
async fn list_views_on_a_viewless_catalog_returns_empty() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = ViewlessCatalog::with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let views = list_catalog_views("glue", &catalog, &namespace, "ns")
        .await
        .unwrap_or_else(|error| panic!("list on a viewless catalog must succeed: {error}"));
    assert!(views.is_empty(), "pins: ice-views-1/C-005");
}

#[tokio::test]
async fn drop_view_on_a_viewless_catalog_reports_view_not_found() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = ViewlessCatalog::with_namespace(&warehouse, "ns").await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let target = ViewTarget {
        catalog_name: "glue",
        catalog: &catalog,
        namespace: &namespace,
        namespace_name: "ns",
        view_name: "v1",
    };
    let error = drop_catalog_view(&target, false)
        .await
        .expect_err("a drop on a viewless catalog must fail");
    let message = error.to_string();
    assert!(
        message.contains("[VIEW_NOT_FOUND]"),
        "pins: ice-views-1/C-005, got: {message}"
    );
}
