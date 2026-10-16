use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::utils::datafusion_strsim::levenshtein;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Expr, SelectItem, SetExpr, TableFactor};
use repark_common::spark_error;

use super::render;

fn written_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            parts.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

fn written_source_names(source: &TableFactor) -> Option<Vec<String>> {
    let TableFactor::Derived { subquery, .. } = source else {
        return None;
    };
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return None;
    };
    select.projection.iter().map(written_name).collect()
}

pub(super) fn refuse_unwritten_star_columns(
    schema: &ArrowSchema,
    source: &TableFactor,
) -> Result<()> {
    let Some(mut names) = written_source_names(source) else {
        return Ok(());
    };
    let Some(missing) = schema
        .fields()
        .iter()
        .find(|field| !names.iter().any(|name| name == field.name()))
    else {
        return Ok(());
    };
    let target = missing.name().to_lowercase();
    names.sort();
    names.sort_by_key(|name| levenshtein(&name.to_lowercase(), &target));
    let suggestions = names
        .iter()
        .map(|name| render::backtick(name))
        .collect::<Vec<_>>()
        .join(", ");
    Err(DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", render::backtick(missing.name()).as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    )))
}
