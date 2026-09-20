use datafusion::error::{DataFusionError, Result};
use iceberg::io::FileIO;
use iceberg::maintenance::{DeleteReachableFiles, DeleteReachableFilesResult};
use iceberg::spec::TableProperties;
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, TableIdent};

use crate::catalog_ops::{iceberg_err, table_or_view_not_found};

pub(crate) const GC_DISABLED_REFUSAL: &str =
    "Cannot purge table: GC is disabled (deleting files may corrupt other tables)";

pub(crate) async fn plan_purge(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    qualified: [&str; 3],
) -> Result<(String, FileIO)> {
    let [catalog_name, namespace, table] = qualified;
    let loaded = catalog.load_table(ident).await.map_err(|error| {
        if error.kind() == ErrorKind::TableNotFound {
            table_or_view_not_found(catalog_name, namespace, table)
        } else {
            iceberg_err(error)
        }
    })?;
    let _ = gc_enabled(&loaded);
    let location = loaded
        .metadata_location_result()
        .map_err(iceberg_err)?
        .to_string();
    Ok((location, loaded.file_io().clone()))
}

pub(crate) async fn purge_table_files(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    qualified: [&str; 3],
) -> Result<DeleteReachableFilesResult> {
    let (location, file_io) = plan_purge(catalog, ident, qualified).await?;
    DeleteReachableFiles::new(location)
        .io(file_io)
        .execute()
        .await
        .map_err(iceberg_err)
}

fn gc_enabled(table: &Table) -> bool {
    table
        .metadata()
        .properties()
        .get(TableProperties::PROPERTY_GC_ENABLED)
        .map_or(TableProperties::PROPERTY_GC_ENABLED_DEFAULT, |value| {
            value.eq_ignore_ascii_case("true")
        })
}
