use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{Insert, ObjectName, TableObject, TableWithJoins};
use repark_common::SourceKind;
use repark_core::CatalogRegistry;
use repark_core::write_postgres::{
    PostgresWrite, PostgresWritePath, PostgresWriteTarget, execute_postgres_write,
    parse_write_path_option, postgres_write_modes_refusal, postgres_write_upsert_refusal,
    record_postgres_write_report,
};

use crate::write_options::StatementWriteOptions;

#[must_use]
pub(crate) fn is_postgres_source(catalogs: &CatalogRegistry, name: &str) -> bool {
    catalogs.database_source_kind(name) == Some(SourceKind::Postgres)
}

#[must_use]
pub(crate) fn postgres_target_parts(
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Option<(String, String, String)> {
    let mut parts = Vec::new();
    for part in &name.0 {
        parts.push(part.as_ident()?.value.as_str());
    }
    let [source, schema, table] = parts.as_slice() else {
        return None;
    };
    if !is_postgres_source(catalogs, source) {
        return None;
    }
    Some((source.to_string(), schema.to_string(), table.to_string()))
}

pub(crate) fn postgres_write_path(
    write_options: &StatementWriteOptions,
) -> Result<PostgresWritePath> {
    let raw = write_options.raw.iter().rev().find_map(|(key, value)| {
        key.eq_ignore_ascii_case("write.path")
            .then_some(value.as_str())
    });
    parse_write_path_option(raw)
}

#[must_use]
pub(crate) fn refuse_postgres_update(
    catalogs: &CatalogRegistry,
    table: &TableWithJoins,
) -> Option<DataFusionError> {
    let name = crate::object_name_from_table_with_joins(table)?;
    postgres_target_parts(catalogs, name)?;
    Some(DataFusionError::NotImplemented(
        postgres_write_upsert_refusal("UPDATE"),
    ))
}

async fn execute_postgres_insert(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    source: &str,
    schema: &str,
    table: &str,
    write_options: &StatementWriteOptions,
) -> Result<DataFrame> {
    if insert.overwrite {
        return Err(DataFusionError::NotImplemented(
            postgres_write_modes_refusal("INSERT OVERWRITE"),
        ));
    }
    if insert.replace_into {
        return Err(DataFusionError::NotImplemented(
            postgres_write_upsert_refusal("REPLACE INTO"),
        ));
    }
    let deduplicated = super::insert_positional::deduplicate_source_names(insert);
    let current = deduplicated.as_ref().unwrap_or(insert);
    let Some(origin) = current.source.as_ref() else {
        return Err(DataFusionError::Plan(
            "Inserts without a source not supported".to_string(),
        ));
    };
    let frame = crate::spark_ast::execute_passthrough(ctx, catalogs, &origin.to_string()).await?;
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(column_name).collect())
    };
    let state = ctx.state();
    let options = state.config().options();
    let zone = repark_functions::session_time_zone::session_time_zone_from_options(options);
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: source.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        },
        columns,
        case_insensitive: crate::spark_door_case_insensitive(options),
        path: postgres_write_path(write_options)?,
    };
    let report = execute_postgres_write(catalogs, frame, write, zone).await?;
    record_postgres_write_report(ctx, report);
    ctx.read_empty()
}

fn column_name(name: &ObjectName) -> String {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map_or_else(|| name.to_string(), |ident| ident.value.clone()),
        _ => name.to_string(),
    }
}

pub(crate) async fn route_postgres_insert(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    write_options: &StatementWriteOptions,
) -> Result<Option<DataFrame>> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(None);
    };
    let Some((source, schema, table)) = postgres_target_parts(catalogs, name) else {
        return Ok(None);
    };
    Box::pin(execute_postgres_insert(
        ctx,
        catalogs,
        insert,
        &source,
        &schema,
        &table,
        write_options,
    ))
    .await
    .map(Some)
}
