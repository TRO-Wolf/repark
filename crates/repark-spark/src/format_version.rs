use std::collections::HashMap;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::format_version::resolve_alter_format_version;
use repark_iceberg::write::format_version::{
    FORMAT_VERSION_PROPERTY, format_version_from_number, format_version_number,
    set_properties_and_format_version,
};

use crate::iceberg_err;

pub(crate) async fn alter_set_tblproperties(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    ident: &TableIdent,
    mut sets: HashMap<String, String>,
    unsets: &[String],
) -> Result<()> {
    let requested = sets.remove(FORMAT_VERSION_PROPERTY);
    let (loaded, target) = match requested {
        Some(raw) => {
            let allow_v3 = repark_sql_settings_from_options(ctx.copied_config().options())
                .allow_create_format_version_3;
            let table = catalog.load_table(ident).await.map_err(iceberg_err)?;
            let target = resolve_upgrade_target(
                &table,
                &raw,
                allow_v3,
                FORMAT_VERSION_PROPERTY,
                "TBLPROPERTIES",
            )?;
            (Some(table), target)
        }
        None => (None, None),
    };
    set_properties_and_format_version(catalog, ident, loaded, sets, unsets, target)
        .await
        .map_err(iceberg_err)
}

fn resolve_upgrade_target(
    table: &Table,
    requested: &str,
    allow_v3: bool,
    property_name: &str,
    form: &str,
) -> Result<Option<FormatVersion>> {
    let current = format_version_number(table);
    let number = resolve_alter_format_version(requested, current, allow_v3, property_name, form)?;
    number
        .map(|value| format_version_from_number(value).map_err(iceberg_err))
        .transpose()
}
