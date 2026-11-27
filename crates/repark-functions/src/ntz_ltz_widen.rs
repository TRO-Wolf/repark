use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result};
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

fn widens_a_wall(source: &DataType, target: &DataType) -> Option<bool> {
    match (source, target) {
        (DataType::Timestamp(TimeUnit::Microsecond, None), target) if is_ltz_timestamp(target) => {
            Some(true)
        }
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target))
        | (DataType::Map(source, _), DataType::Map(target, _)) => {
            widens_a_wall(source.data_type(), target.data_type())
        }
        (DataType::FixedSizeList(source, width), DataType::FixedSizeList(target, other))
            if width == other =>
        {
            widens_a_wall(source.data_type(), target.data_type())
        }
        (DataType::Struct(sources), DataType::Struct(targets))
            if sources.len() == targets.len() =>
        {
            let mut widened = false;
            for (source, target) in sources.iter().zip(targets.iter()) {
                widened |= widens_a_wall(source.data_type(), target.data_type())?;
            }
            Some(widened)
        }
        (source, target) if source == target => Some(false),
        _ => None,
    }
}

fn localize_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    let Ok(source) = cast.expr.get_type(schema) else {
        return Transformed::no(expr);
    };
    if widens_a_wall(&source, cast.field.data_type()) != Some(true) {
        return Transformed::no(expr);
    }
    match crate::zone_seam::seam_call(*cast.expr.clone(), cast.field.data_type(), false) {
        Some(localized) => Transformed::yes(localized),
        None => Transformed::no(expr),
    }
}

fn localize_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if matches!(plan, LogicalPlan::Values(_)) {
        return Ok(Transformed::no(plan));
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(localize_cast(node, &schema)))?;
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
    use datafusion::arrow::datatypes::{Field, Fields, Schema, TimestampMicrosecondType};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;
    use crate::instant_ts::{ltz_timestamp_type, ntz_timestamp_type};
    use crate::timestamp_type::{SparkTimestampType, with_spark_timestamp_type};

    const GAP_WALL: i64 = 1_710_037_800_000_000;
    const GAP_IN_NEW_YORK: i64 = 1_710_055_800_000_000;
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
    async fn a_wall_beside_an_instant_localizes_in_the_session_zone() {
        let ctx = context("America/New_York", SparkTimestampType::Ntz);
        for sql in [
            "SELECT n AS v FROM t UNION ALL SELECT l FROM t",
            "SELECT l AS v FROM t UNION ALL SELECT n FROM t",
        ] {
            assert_eq!(
                answers(&ctx, sql).await,
                (ltz_timestamp_type(), vec![INSTANT, GAP_IN_NEW_YORK]),
                "{sql}"
            );
        }
        for sql in [
            "SELECT coalesce(n, l) FROM t",
            "SELECT CASE WHEN l IS NULL THEN l ELSE n END FROM t",
            "SELECT greatest(n, l) FROM t",
        ] {
            assert_eq!(
                answers(&ctx, sql).await,
                (ltz_timestamp_type(), vec![GAP_IN_NEW_YORK]),
                "{sql}"
            );
        }
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

    #[test]
    fn only_a_wall_to_instant_shape_widens() {
        let wall = ntz_timestamp_type();
        let instant = ltz_timestamp_type();
        let list = |inner: &DataType| DataType::List(Arc::new(Field::new("item", inner.clone(), true)));
        let pair = |first: &DataType| {
            DataType::Struct(Fields::from(vec![
                Field::new("a", first.clone(), true),
                Field::new("b", DataType::Int32, true),
            ]))
        };
        assert_eq!(widens_a_wall(&wall, &instant), Some(true));
        assert_eq!(widens_a_wall(&list(&wall), &list(&instant)), Some(true));
        assert_eq!(widens_a_wall(&pair(&wall), &pair(&instant)), Some(true));
        assert_eq!(widens_a_wall(&wall, &wall), Some(false));
        assert_eq!(widens_a_wall(&instant, &wall), None);
        assert_eq!(widens_a_wall(&DataType::Utf8, &instant), None);
    }
}
