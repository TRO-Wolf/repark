use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Expr;
use repark_iceberg::write::update_cast::{NtzDecision, nested_micro_ntz_decision};

use super::AssignmentScope;

pub(super) struct Probed {
    pub(super) written: DataType,
    pub(super) stored: Option<DataType>,
}

pub(super) async fn probe_type(
    ctx: &SessionContext,
    scope: &AssignmentScope,
    value: &Expr,
) -> Option<Probed> {
    let mut rendered = value.clone();
    let pretty = repark_iceberg::write::sql_text::render_for_reparse(&mut rendered);
    let sql = format!("SELECT ({pretty}) FROM {} LIMIT 0", scope.probe_from);
    let frame = ctx.sql(&sql).await.ok()?;
    let written = frame.schema().fields().first()?.data_type().clone();
    let stored = repark_iceberg::write::ntz_store::analyzed_types(ctx, frame.logical_plan())
        .and_then(|types| types.into_iter().next());
    Some(Probed { written, stored })
}

pub(super) fn stores_through_the_kernel(stored: Option<&DataType>, target: &DataType) -> bool {
    matches!(
        nested_micro_ntz_decision(stored, target),
        NtzDecision::Rebuild(_)
    )
}
