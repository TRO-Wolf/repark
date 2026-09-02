use std::collections::HashMap;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::spec::FormatVersion;
use iceberg::{Catalog, TableIdent};
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::format_version::resolve_alter_format_version;
use repark_iceberg::write::format_version::{
    FORMAT_VERSION_PROPERTY, current_format_version, format_version_from_number,
    set_properties_and_format_version,
};

use crate::iceberg_err;

pub(crate) async fn alter_set_tblproperties(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    ident: &TableIdent,
    mut sets: HashMap<String, String>,
    unsets: &[String],
) -> Result<bool> {
    let requested = sets.remove(FORMAT_VERSION_PROPERTY);
    let target = match requested {
        Some(raw) => {
            let allow_v3 = repark_sql_settings_from_options(ctx.copied_config().options())
                .allow_create_format_version_3;
            resolve_upgrade_target(
                catalog,
                ident,
                &raw,
                allow_v3,
                FORMAT_VERSION_PROPERTY,
                "TBLPROPERTIES",
            )
            .await?
        }
        None => None,
    };
    set_properties_and_format_version(catalog, ident, &sets, unsets, target)
        .await
        .map_err(iceberg_err)?;
    Ok(target.is_some())
}

async fn resolve_upgrade_target(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    requested: &str,
    allow_v3: bool,
    property_name: &str,
    form: &str,
) -> Result<Option<FormatVersion>> {
    let current = current_format_version(catalog, ident)
        .await
        .map_err(iceberg_err)?;
    let number = resolve_alter_format_version(requested, current, allow_v3, property_name, form)?;
    number
        .map(|value| format_version_from_number(value).map_err(iceberg_err))
        .transpose()
}
