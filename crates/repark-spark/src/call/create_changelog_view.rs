use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::Catalog;
use repark_core::time_travel::incremental::IncrementalWindow;
use repark_iceberg::catalog::{ChangelogTableProvider, ChangelogViewProvider};

use super::changelog::ChangelogTransform;
use super::params::params_for;
use super::{CallArgs, illegal_argument, resolve_table_ident};
use crate::call_args::{BoundArgs, bind, expr_as_string_array};
use crate::iceberg_err;

const NET_CHANGES_WITH_UPDATES: &str = "Not support net changes with update images";
const NO_IDENTIFIER_COLUMNS: &str =
    "Cannot compute the update images because identifier columns are not set";

#[allow(clippy::missing_errors_doc)]
pub(super) async fn execute_create_changelog_view(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    args: &CallArgs,
) -> Result<DataFrame> {
    let bound = bind(args, params_for("create_changelog_view"), &[])?;
    let table_arg = bound.require_string("table")?;
    let view_arg = bound.optional_string("changelog_view")?;
    let identifier_argument = bound
        .get("identifier_columns")
        .map(|expr| expr_as_string_array(expr, "identifier_columns"))
        .transpose()?;
    let net_changes = bound.optional_bool("net_changes")?.unwrap_or(false);
    let compute_update_images = bound
        .optional_bool("compute_updates")?
        .unwrap_or_else(|| identifier_argument.is_some());
    let options = window_options(&bound)?;

    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    let identifier_columns = match identifier_argument {
        Some(columns) => columns,
        None => table_identifier_columns(&table),
    };
    let window = IncrementalWindow::from_options(&options)?.unwrap_or_default();
    let bounds = window.changelog_bounds(table.metadata())?;
    let transform = if compute_update_images {
        if net_changes {
            return Err(illegal_argument(NET_CHANGES_WITH_UPDATES.to_string()));
        }
        if identifier_columns.is_empty() {
            return Err(illegal_argument(NO_IDENTIFIER_COLUMNS.to_string()));
        }
        ChangelogTransform::UpdateImages { identifier_columns }
    } else {
        ChangelogTransform::Carryovers { net_changes }
    };
    let source = Arc::new(ChangelogTableProvider::try_new(table, bounds)?);
    let view = ChangelogViewProvider::new(
        source,
        Arc::new(move |batch: &RecordBatch| transform.apply(batch)),
    );

    let view_name = view_arg.unwrap_or_else(|| default_view_name(&table_arg));
    let registered = view_name.trim_matches('`').replace("``", "`");
    let _ = ctx.deregister_table(registered.as_str());
    ctx.register_table(registered.as_str(), Arc::new(view))?;
    output_frame(ctx, &view_name)
}

fn window_options(bound: &BoundArgs) -> Result<BTreeMap<String, String>> {
    let pairs = super::rewrite_options::extract_option_pairs(bound, "create_changelog_view")?;
    let mut options = BTreeMap::new();
    for (key, value) in pairs {
        if let Some(value) = value {
            options.insert(key, value);
        }
    }
    Ok(options)
}

fn table_identifier_columns(table: &iceberg::table::Table) -> Vec<String> {
    let schema = table.metadata().current_schema();
    schema
        .identifier_field_ids()
        .filter_map(|field_id| schema.name_by_field_id(field_id).map(ToString::to_string))
        .collect()
}

fn default_view_name(table_arg: &str) -> String {
    let leaf = table_arg.rsplit('.').next().unwrap_or(table_arg);
    let escaped = leaf.trim_matches('`').replace('`', "``");
    format!("`{escaped}_changes`")
}

fn output_frame(ctx: &SessionContext, view_name: &str) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "changelog_view",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![view_name.to_string()]))],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    ctx.read_batch(batch)
}
