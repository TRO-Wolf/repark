use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use crate::instant_ts::is_ltz_timestamp;

#[derive(Debug, Default)]
pub struct SparkNtzLtzWiden;

#[must_use]
pub fn ntz_ltz_widen_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(SparkNtzLtzWiden)
}

impl AnalyzerRule for SparkNtzLtzWiden {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(localize_node).data()
    }

    fn name(&self) -> &'static str {
        "spark_ntz_ltz_widen"
    }
}

fn localized_instant(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    let source = cast.expr.get_type(schema).ok()?;
    if !matches!(source, DataType::Timestamp(TimeUnit::Microsecond, None))
        || !is_ltz_timestamp(cast.field.data_type())
    {
        return None;
    }
    if let Expr::Cast(wall) = cast.expr.as_ref()
        && wall
            .expr
            .get_type(schema)
            .is_ok_and(|instant| &instant == cast.field.data_type())
    {
        return Some(*wall.expr.clone());
    }
    crate::zone_seam::seam_call(*cast.expr.clone(), cast.field.data_type(), false)
}

fn localize_date_trunc(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::ScalarFunction(function) = &expr else {
        return Transformed::no(expr);
    };
    if function.func.name() != "date_trunc" || function.args.len() != 2 {
        return Transformed::no(expr);
    }
    let Some(instant) = localized_instant(&function.args[1], schema) else {
        return Transformed::no(expr);
    };
    let mut args = function.args.clone();
    args[1] = instant;
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&function.func),
        args,
    )))
}

fn localize_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(localize_date_trunc(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    if transformed.transformed {
        transformed.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(transformed)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{Field, Schema, TimestampMicrosecondType};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;
    use crate::instant_ts::{ltz_timestamp_type, ntz_timestamp_type};
    use crate::timestamp_type::{SparkTimestampType, with_spark_timestamp_type};

    const GAP_WALL: i64 = 1_710_037_800_000_000;
    const GAP_HOUR_IN_NEW_YORK: i64 = 1_710_054_000_000_000;
    const INSTANT: i64 = 1_704_092_400_000_000;

    fn context(zone: &str, session_type: SparkTimestampType) -> SessionContext {
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let config = with_spark_timestamp_type(config, session_type);
        let ctx = crate::spark_type_planner::planned_context(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", ntz_timestamp_type(), false),
            Field::new("l", ltz_timestamp_type(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![GAP_WALL])),
                Arc::new(TimestampMicrosecondArray::from(vec![INSTANT]).with_timezone("UTC")),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    async fn answers(ctx: &SessionContext, sql: &str) -> (DataType, Vec<i64>) {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let kind = batches[0].schema().field(0).data_type().clone();
        let mut values = Vec::new();
        for batch in &batches {
            values.extend(
                batch
                    .column(0)
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .flatten(),
            );
        }
        values.sort_unstable();
        (kind, values)
    }

    #[tokio::test]
    async fn date_trunc_of_a_wall_is_an_instant_on_the_session_calendar() {
        for session_type in [SparkTimestampType::Ltz, SparkTimestampType::Ntz] {
            let ctx = context("America/New_York", session_type);
            assert_eq!(
                answers(&ctx, "SELECT date_trunc('HOUR', n) FROM t").await,
                (ltz_timestamp_type(), vec![GAP_HOUR_IN_NEW_YORK]),
                "{session_type:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_wall_only_or_instant_only_union_keeps_its_type() {
        let ctx = context("America/New_York", SparkTimestampType::Ntz);
        assert_eq!(
            answers(&ctx, "SELECT n AS v FROM t UNION ALL SELECT n FROM t").await,
            (ntz_timestamp_type(), vec![GAP_WALL, GAP_WALL])
        );
        assert_eq!(
            answers(&ctx, "SELECT l AS v FROM t UNION ALL SELECT l FROM t").await,
            (ltz_timestamp_type(), vec![INSTANT, INSTANT])
        );
    }

}
