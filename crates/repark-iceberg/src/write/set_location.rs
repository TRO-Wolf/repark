use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result, TableIdent};

pub async fn set_table_location(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    location: &str,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let action = tx.update_location().set_location(location.to_string());
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};
    use tempfile::TempDir;

    async fn setup(wh: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
        let warehouse = wh.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );
        let ns = NamespaceIdent::new("sales".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();
        (catalog, TableIdent::new(ns, "t".to_string()))
    }

    fn metadata_files(location: &std::path::Path) -> Vec<std::path::PathBuf> {
        let dir = location.join("metadata");
        let mut files: Vec<std::path::PathBuf> = if dir.is_dir() {
            std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.to_string_lossy().ends_with(".metadata.json"))
                .collect()
        } else {
            Vec::new()
        };
        files.sort();
        files
    }

    #[tokio::test]
    async fn set_location_moves_the_metadata_location_and_writes_the_new_file_under_it() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        let old_location = wh.path().join("sales").join("t");
        let new_location = wh.path().join("moved").join("t");

        set_table_location(catalog.as_ref(), &ident, new_location.to_str().unwrap())
            .await
            .unwrap();

        let table = catalog.load_table(&ident).await.unwrap();
        assert_eq!(
            table.metadata().location(),
            new_location.to_str().unwrap(),
            "the metadata `location` field must record the new path"
        );
        assert_eq!(
            metadata_files(&old_location).len(),
            1,
            "the original metadata file is never moved or deleted"
        );
        let moved_files = metadata_files(&new_location);
        assert_eq!(
            moved_files.len(),
            1,
            "the location-move commit itself must write exactly one new metadata file under the \
             new location"
        );
        assert_eq!(
            table.metadata_location().unwrap(),
            moved_files[0].to_str().unwrap(),
            "the catalog pointer must advance to the file written under the new location"
        );
    }

    #[tokio::test]
    async fn next_commit_lands_under_the_new_location() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        let old_location = wh.path().join("sales").join("t");
        let new_location = wh.path().join("moved").join("t");
        set_table_location(catalog.as_ref(), &ident, new_location.to_str().unwrap())
            .await
            .unwrap();

        crate::write::alter::set_table_properties(
            catalog.as_ref(),
            &ident,
            &HashMap::from([("team".to_string(), "example-team".to_string())]),
        )
        .await
        .unwrap();

        let table = catalog.load_table(&ident).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .properties()
                .get("team")
                .map(String::as_str),
            Some("example-team")
        );
        assert_eq!(
            metadata_files(&old_location).len(),
            1,
            "no new metadata may appear under the old location"
        );
        assert_eq!(
            metadata_files(&new_location).len(),
            2,
            "the next metadata commit must land under the new location"
        );
    }
}
