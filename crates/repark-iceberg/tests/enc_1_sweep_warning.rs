use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

type Fields = Vec<(String, String)>;

#[derive(Default)]
struct WarnCapture {
    events: Mutex<Vec<Fields>>,
}

struct FieldVisitor(Fields);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        self.0.push((
            field.name().to_string(),
            rendered.trim_matches('"').to_string(),
        ));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

impl tracing::Subscriber for WarnCapture {
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if *metadata.level() <= tracing::Level::WARN {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = FieldVisitor(Vec::new());
        event.record(&mut visitor);
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(visitor.0);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

fn ident() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string())
}

async fn keyed_stale_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, iceberg::table::Table) {
    let catalog = repark_iceberg::memory_catalog(warehouse.path().to_str().expect("utf8 path"))
        .await
        .expect("the guarded memory catalog builds");
    catalog
        .create_namespace(ident().namespace(), HashMap::new())
        .await
        .expect("the namespace creates");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("the schema builds");
    catalog
        .create_table(
            ident().namespace(),
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .expect("the table creates");
    let stale = catalog.load_table(&ident()).await.expect("the table loads");
    let tx = Transaction::new(&stale);
    let tx = tx
        .update_table_properties()
        .set(
            repark_iceberg::write::ENCRYPTION_KEY_ID_PROPERTY.to_string(),
            "sweep-warning-key".to_string(),
        )
        .apply(tx)
        .expect("the key property applies");
    tx.commit(catalog.as_ref()).await.expect("the key lands");
    (catalog, stale)
}

#[tokio::test(flavor = "current_thread")]
async fn a_refused_keyed_commit_warns_the_full_path_of_a_staged_file_it_could_not_delete() {
    let capture = Arc::new(WarnCapture::default());
    tracing::subscriber::set_global_default(Arc::clone(&capture))
        .expect("this binary installs the only subscriber");
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, stale) = keyed_stale_table(&warehouse).await;
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2])) as _])
        .expect("the batch builds");
    let staged = repark_iceberg::write::write_data_files(&stale, vec![batch])
        .await
        .expect("the staged file writes");
    let named = staged[0].file_path().to_string();
    let path = named.strip_prefix("file://").unwrap_or(&named).to_string();
    assert!(path.starts_with('/'), "the staged path is absolute: {path}");
    let directory = std::path::Path::new(&path)
        .parent()
        .expect("a staged file has a directory")
        .to_path_buf();
    let original = std::fs::metadata(&directory)
        .expect("the data directory reads")
        .permissions();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o555))
        .expect("the data directory locks");
    let result = repark_iceberg::write::commit_append(&catalog, &stale, staged).await;
    std::fs::set_permissions(&directory, original).expect("the data directory unlocks");
    let error = result.expect_err("the keyed append refuses");
    assert!(
        repark_iceberg::write::EncryptedTableRefusal::find(&error).is_some(),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains(&format!(" failed to delete staged file '{named}'")),
        "{error}"
    );
    let events = capture
        .events
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let warned = events.iter().any(|fields| {
        fields.iter().any(|(name, value)| {
            name == "message" && value == "failed to delete a staged file after a refused commit"
        }) && fields
            .iter()
            .any(|(name, value)| name == "path" && value == &named)
    });
    assert!(
        warned,
        "the warning must carry the staged path {named}, got {events:?}"
    );
}
