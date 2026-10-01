use std::collections::HashSet;

use datafusion::error::Result;
use datafusion::execution::SessionState;

use super::fold;

#[allow(clippy::missing_errors_doc)]
pub async fn fold_query_text(state: &SessionState, query_sql: &str) -> Result<String> {
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(query_sql, &dialect)?;
    let datafusion::sql::parser::Statement::Statement(mut inner) = statement else {
        return Ok(query_sql.to_string());
    };
    let first = state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
        .await;
    let Err(mut error) = first else {
        return Ok(query_sql.to_string());
    };
    let catalog_options = &state.config().options().catalog;
    let defaults = [
        catalog_options.default_catalog.clone(),
        catalog_options.default_schema.clone(),
    ];
    let written = super::written_references(&inner, defaults);
    let mut known: Option<fold::Known> = None;
    let mut seen: HashSet<(Option<String>, String)> = HashSet::new();
    loop {
        let Some((field, valid)) = super::missing_field(&error) else {
            return Ok(repark_iceberg::write::sql_text::render_for_reparse(
                &mut inner,
            ));
        };
        let miss = (
            field.relation.as_ref().map(ToString::to_string),
            field.name.clone(),
        );
        if !seen.insert(miss) {
            return Ok(repark_iceberg::write::sql_text::render_for_reparse(
                &mut inner,
            ));
        }
        let catalog = match known.take() {
            Some(catalog) => catalog,
            None => fold::Known::with_tables(super::catalog_fields(state, &inner).await),
        };
        let catalog = known.insert(catalog);
        catalog.absorb(valid);
        if !fold::fold_statement(&mut inner, catalog, &written)? {
            return Ok(repark_iceberg::write::sql_text::render_for_reparse(
                &mut inner,
            ));
        }
        match state
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
            .await
        {
            Ok(_) => {
                return Ok(repark_iceberg::write::sql_text::render_for_reparse(
                    &mut inner,
                ));
            }
            Err(next) => error = next,
        }
    }
}
