use std::collections::{BTreeMap, HashMap};

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::Session;
use crate::time_travel::microbatch_source::SourceOptions;

pub(crate) const SOURCE: &str = "ice.sales.orders";
pub(crate) const SINK: &str = "ice.sales.silver";

pub(crate) struct Fixture {
    pub(crate) _warehouse: TempDir,
    pub(crate) session: Session,
}

impl Fixture {
    pub(crate) async fn new() -> Fixture {
        let warehouse = TempDir::new().expect("a scratch warehouse");
        let root = warehouse
            .path()
            .to_str()
            .expect("the warehouse path is text")
            .to_string();
        let session = Session::builder().build().expect("a session");
        session
            .register_memory_catalog("ice", &root)
            .await
            .expect("the memory catalog registers");
        let catalog = session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog is visible");
        let sales = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&sales, HashMap::new())
            .await
            .expect("the namespace creates");
        for name in ["orders", "silver", "other"] {
            catalog
                .create_table(&sales, creation(name, &format!("{root}/sales/{name}")))
                .await
                .expect("the table creates");
        }
        session
            .refresh_catalog_provider("ice")
            .await
            .expect("the provider refreshes");
        Fixture {
            _warehouse: warehouse,
            session,
        }
    }

    pub(crate) async fn table(&self, name: &str) -> Table {
        let catalog = self
            .session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the catalog is visible");
        catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                name.to_string(),
            ))
            .await
            .expect("the table loads")
    }
}

pub(crate) fn options(pairs: &[(&str, &str)]) -> SourceOptions {
    let map: BTreeMap<String, String> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    SourceOptions::from_options(&map).expect("the options parse")
}

fn creation(name: &str, location: &str) -> TableCreation {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("the schema builds");
    TableCreation::builder()
        .name(name.to_string())
        .location(location.to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build()
}
