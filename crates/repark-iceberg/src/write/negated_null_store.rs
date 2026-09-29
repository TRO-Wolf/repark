use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Distinct, Expr, JoinType, LogicalPlan};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::store_assign::{ansi_store_assignable, normalize_for_assignment};
use super::update_cast::incompatible_store_message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullOrigin {
    Negated,
    Bare,
    Other,
}

pub type ViewPlanResolver = fn(&dyn TableProvider) -> Option<LogicalPlan>;

#[derive(Debug, Clone, Copy)]
pub struct ViewDefinitionPlans {
    resolve: ViewPlanResolver,
}

impl ViewDefinitionPlans {
    #[must_use]
    pub fn definition_plan(&self, provider: &dyn TableProvider) -> Option<LogicalPlan> {
        (self.resolve)(provider)
    }
}

#[must_use]
pub fn with_view_definition_plans(
    config: SessionConfig,
    resolve: ViewPlanResolver,
) -> SessionConfig {
    config.with_extension(Arc::new(ViewDefinitionPlans { resolve }))
}

#[must_use]
pub fn refuses_double(target: &DataType) -> bool {
    !ansi_store_assignable(&DataType::Float64, normalize_for_assignment(target))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_negated_null_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    let fields = plan.schema().fields();
    if fields.len() != targets.len() {
        return Ok(());
    }
    let mut views: Option<Option<Arc<ViewDefinitionPlans>>> = None;
    for (index, (field, (column, target))) in fields.iter().zip(targets).enumerate() {
        if !matches!(field.data_type(), DataType::Null) || !refuses_double(target) {
            continue;
        }
        let views = views
            .get_or_insert_with(|| ctx.state().config().get_extension::<ViewDefinitionPlans>());
        if column_origin(plan, index, views.as_deref()) != NullOrigin::Negated {
            continue;
        }
        if let Some(text) =
            incompatible_store_message(table, &format!("`{column}`"), &DataType::Float64, target)
        {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}

fn column_origin(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
) -> NullOrigin {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)
            .map_or(NullOrigin::Other, |expr| {
                expr_origin(expr, Some(&projection.input), views)
            }),
        LogicalPlan::SubqueryAlias(alias) => column_origin(&alias.input, index, views),
        LogicalPlan::Filter(filter) => column_origin(&filter.input, index, views),
        LogicalPlan::Sort(sort) => column_origin(&sort.input, index, views),
        LogicalPlan::Limit(limit) => column_origin(&limit.input, index, views),
        LogicalPlan::Repartition(repartition) => column_origin(&repartition.input, index, views),
        LogicalPlan::Distinct(Distinct::All(input)) => column_origin(input, index, views),
        LogicalPlan::Values(values) => combine(values.values.iter().map(|row| {
            row.get(index)
                .map_or(NullOrigin::Other, |expr| expr_origin(expr, None, views))
        })),
        LogicalPlan::Union(union) => combine(
            union
                .inputs
                .iter()
                .map(|input| column_origin(input, index, views)),
        ),
        LogicalPlan::Join(join) => {
            let left_width = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if index < left_width {
                        column_origin(&join.left, index, views)
                    } else {
                        column_origin(&join.right, index - left_width, views)
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => column_origin(&join.left, index, views),
                JoinType::RightSemi | JoinType::RightAnti => {
                    column_origin(&join.right, index, views)
                }
                JoinType::LeftMark | JoinType::RightMark => NullOrigin::Other,
            }
        }
        LogicalPlan::TableScan(scan) => {
            let source_index = match &scan.projection {
                Some(projection) => projection.get(index).copied(),
                None => Some(index),
            };
            let Some(source_index) = source_index else {
                return NullOrigin::Other;
            };
            if let Some(source) = scan.source.get_logical_plan() {
                return column_origin(&source, source_index, views);
            }
            views
                .zip(source_as_provider(&scan.source).ok())
                .and_then(|(views, provider)| (views.resolve)(provider.as_ref()))
                .map_or(NullOrigin::Other, |source| {
                    column_origin(&source, source_index, views)
                })
        }
        _ => NullOrigin::Other,
    }
}

fn expr_origin(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    views: Option<&ViewDefinitionPlans>,
) -> NullOrigin {
    match expr {
        Expr::Alias(alias) => expr_origin(&alias.expr, input, views),
        Expr::Literal(ScalarValue::Null, _) => NullOrigin::Bare,
        Expr::Negative(operand) => match expr_origin(operand, input, views) {
            NullOrigin::Negated | NullOrigin::Bare => NullOrigin::Negated,
            NullOrigin::Other => NullOrigin::Other,
        },
        Expr::Column(column) => input.map_or(NullOrigin::Other, |input| {
            input
                .schema()
                .index_of_column(column)
                .map_or(NullOrigin::Other, |position| {
                    column_origin(input, position, views)
                })
        }),
        _ => NullOrigin::Other,
    }
}

fn combine(origins: impl Iterator<Item = NullOrigin>) -> NullOrigin {
    let mut negated = false;
    for origin in origins {
        match origin {
            NullOrigin::Negated => negated = true,
            NullOrigin::Bare => {}
            NullOrigin::Other => return NullOrigin::Other,
        }
    }
    if negated {
        NullOrigin::Negated
    } else {
        NullOrigin::Bare
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::{NullOrigin, column_origin, refuse_negated_null_writes, refuses_double};

    async fn plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
        ctx.register_table("s", Arc::new(table)).unwrap();
        ctx.sql("CREATE VIEW v AS SELECT -NULL AS c")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql(sql).await.unwrap().logical_plan().clone()
    }

    #[tokio::test]
    async fn a_negated_null_is_found_through_aliases_derived_tables_views_and_joins() {
        for sql in [
            "SELECT -NULL AS c",
            "SELECT - -NULL AS c",
            "SELECT -(NULL) AS c",
            "SELECT c FROM (SELECT -NULL AS c) d",
            "SELECT c FROM v",
            "SELECT d.c FROM (SELECT -NULL AS c) d JOIN s ON true",
            "VALUES (-NULL)",
            "VALUES (NULL), (-NULL)",
            "SELECT -NULL AS c UNION ALL SELECT NULL AS c",
        ] {
            let planned = plan(sql).await;
            assert_eq!(
                column_origin(&planned, 0, None),
                NullOrigin::Negated,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn a_bare_or_typed_null_is_not_a_negated_null() {
        for sql in [
            "SELECT NULL AS c",
            "SELECT c FROM (SELECT NULL AS c) d",
            "VALUES (NULL)",
            "SELECT -CAST(NULL AS INT) AS c",
            "SELECT -NULL AS c UNION ALL SELECT 1 AS c",
        ] {
            let planned = plan(sql).await;
            assert_ne!(
                column_origin(&planned, 0, None),
                NullOrigin::Negated,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn a_negated_null_refuses_only_where_double_cannot_store() {
        let planned = plan("SELECT -NULL AS c").await;
        let ctx = SessionContext::new();
        let text =
            refuse_negated_null_writes(&ctx, "`sc`.`ns`.`t`", &planned, [("c", &DataType::Date32)])
                .expect_err("DOUBLE into DATE must refuse")
                .to_string();
        assert!(
            text.ends_with(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
                 for the table `sc`.`ns`.`t`: Cannot safely cast `c` \"DOUBLE\" to \"DATE\". \
                 SQLSTATE: KD000"
            ),
            "{text}"
        );
        for target in [DataType::Int32, DataType::Decimal128(10, 2), DataType::Utf8] {
            refuse_negated_null_writes(&ctx, "``", &planned, [("c", &target)])
                .unwrap_or_else(|error| panic!("{target:?} must store: {error}"));
        }
        let bare = plan("SELECT NULL AS c").await;
        refuse_negated_null_writes(&ctx, "``", &bare, [("c", &DataType::Date32)])
            .expect("a bare NULL stores into DATE");
    }

    #[test]
    fn double_stores_into_numbers_and_strings_only() {
        assert!(!refuses_double(&DataType::Int8));
        assert!(!refuses_double(&DataType::Utf8));
        assert!(refuses_double(&DataType::Boolean));
        assert!(refuses_double(&DataType::Date32));
        assert!(refuses_double(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        )));
        assert!(refuses_double(&DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        )));
    }
}
