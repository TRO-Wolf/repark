use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};

pub(crate) async fn conform_types(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    column_names: &[String],
    source_df: DataFrame,
) -> Result<DataFrame> {
    let arrow = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(crate::iceberg_err)?;
    let mut targets = Vec::new();
    if column_names.is_empty() {
        for field in arrow.fields() {
            targets.push((field.name().clone(), field.data_type().clone()));
        }
    } else {
        for name in column_names {
            let mut found = None;
            for field in arrow.fields() {
                if field.name().eq_ignore_ascii_case(name) {
                    found = Some(field);
                    break;
                }
            }
            let Some(field) = found else {
                return Ok(source_df);
            };
            targets.push((field.name().clone(), field.data_type().clone()));
        }
    }
    let plan = repark_iceberg::write::store_overflow::wrap_store_outputs(
        source_df.logical_plan().clone(),
        &targets,
        false,
        true,
        Some("INSERT OVERWRITE"),
    )?;
    ctx.execute_logical_plan(plan).await
}
