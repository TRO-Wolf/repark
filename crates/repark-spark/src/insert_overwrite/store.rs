use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_iceberg::catalog::uuid_presentation::presented_arrow_schema;
use repark_iceberg::write::struct_pairing::StructPairing;
use repark_iceberg::write::{StageRequest, StageSite, conform_frame, table_targets};

pub(crate) struct OverwriteColumns<'a> {
    pub(crate) listed: &'a [String],
    pub(crate) reserved: &'a [String],
}

pub(crate) async fn conform_types(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    columns: OverwriteColumns<'_>,
    source_df: DataFrame,
    door: (StructPairing, &str, StageSite),
) -> Result<DataFrame> {
    let arrow =
        presented_arrow_schema(table.metadata().current_schema()).map_err(crate::iceberg_err)?;
    let targets = table_targets(&arrow, columns.listed, columns.reserved)?;
    let request = StageRequest {
        site: door.2,
        targets: &targets,
        by_name: false,
        fold: true,
        gate_op: (door.2 == StageSite::InsertOverwrite).then_some("INSERT OVERWRITE"),
        pairing: door.0,
        table: door.1,
    };
    conform_frame(ctx, source_df, &request).await
}
