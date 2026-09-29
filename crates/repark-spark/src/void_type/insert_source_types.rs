use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::ScalarValue;
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Distinct, Expr, JoinType, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Insert, TableObject};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::negated_null_store::{
    ViewDefinitionPlans, refuse_negated_null_writes, refuses_double,
};
use repark_iceberg::write::update_cast::incompatible_store_message;

use super::column_name;
use super::spark_widen::{
    BranchShape, FLOAT_TO_STRING_WRAPPER, branch_shape, is_string_type, widen_operand_types,
    widened_stores,
};
use crate::catalog_ops::{name_parts, quoted_table_display};
use crate::write_to_branch::qualify_table_parts;

pub(crate) async fn refuse_insert_source_types(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    insert: &Insert,
    by_name: bool,
) -> Result<()> {
    let TableObject::TableName(name) = &insert.table else {
        return Ok(());
    };
    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let parts = qualify_table_parts(ctx, name_parts(name));
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(catalog) = catalogs.get(&parts[0]) else {
        return Ok(());
    };
    let Ok(namespace) = NamespaceIdent::from_vec(parts[1..parts.len() - 1].to_vec()) else {
        return Ok(());
    };
    let ident = TableIdent::new(namespace, parts[parts.len() - 1].clone());
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(());
    };
    let Ok(presented) = repark_iceberg::catalog::uuid_presentation::presented_arrow_schema(
        table.metadata().current_schema(),
    ) else {
        return Ok(());
    };
    if !presented
        .fields()
        .iter()
        .any(|field| refuses_double(field.data_type()) || is_float(field.data_type()))
    {
        return Ok(());
    }
    let Ok(frame) = ctx.sql(&source.to_string()).await else {
        return Ok(());
    };
    let plan = frame.logical_plan();
    let planned = plan.schema().fields();
    if !planned.iter().any(|field| {
        matches!(field.data_type(), DataType::Null) || is_string_type(field.data_type())
    }) {
        return Ok(());
    }
    let case_insensitive = crate::spark_door_case_insensitive(ctx.state().config().options());
    let targets: Option<Vec<&Field>> = if by_name {
        planned
            .iter()
            .map(|field| find_target(&presented, field.name(), case_insensitive))
            .collect()
    } else if insert.columns.is_empty() {
        (planned.len() == presented.fields().len())
            .then(|| presented.fields().iter().map(AsRef::as_ref).collect())
    } else {
        insert
            .columns
            .iter()
            .map(|column| find_target(&presented, &column_name(column), case_insensitive))
            .collect()
    };
    let Some(targets) = targets else {
        return Ok(());
    };
    let display = quoted_table_display(&parts);
    refuse_negated_null_writes(
        ctx,
        &display,
        plan,
        targets
            .iter()
            .map(|field| (field.name().as_str(), field.data_type())),
    )?;
    let mut views: Option<Option<Arc<ViewDefinitionPlans>>> = None;
    for (index, (source_field, target)) in planned.iter().zip(&targets).enumerate() {
        if !is_string_type(source_field.data_type()) || !is_float(target.data_type()) {
            continue;
        }
        let views = views
            .get_or_insert_with(|| ctx.state().config().get_extension::<ViewDefinitionPlans>());
        if spark_widened_store(plan, index, views.as_deref(), target.data_type()) {
            continue;
        }
        if let Some(text) = incompatible_store_message(
            &display,
            &format!("`{}`", target.name()),
            source_field.data_type(),
            target.data_type(),
        ) {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}

fn is_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn spark_widened_store(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
    target: &DataType,
) -> bool {
    match spark_source_type(plan, index, views) {
        Some(widened) => widened_stores(&widened, target),
        None => false,
    }
}

fn spark_source_type(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
) -> Option<DataType> {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)
            .and_then(|expr| spark_expr_type(expr, Some(&projection.input), views)),
        LogicalPlan::SubqueryAlias(alias) => spark_source_type(&alias.input, index, views),
        LogicalPlan::Filter(filter) => spark_source_type(&filter.input, index, views),
        LogicalPlan::Sort(sort) => spark_source_type(&sort.input, index, views),
        LogicalPlan::Limit(limit) => spark_source_type(&limit.input, index, views),
        LogicalPlan::Repartition(repartition) => {
            spark_source_type(&repartition.input, index, views)
        }
        LogicalPlan::Distinct(Distinct::All(input)) => spark_source_type(input, index, views),
        LogicalPlan::Values(values) => combine_branch_types(values.values.iter().map(|row| {
            row.get(index)
                .and_then(|expr| spark_expr_type(expr, None, views))
        })),
        LogicalPlan::Union(union) => combine_branch_types(
            union
                .inputs
                .iter()
                .map(|input| spark_source_type(input, index, views)),
        ),
        LogicalPlan::Join(join) => {
            let left_width = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if index < left_width {
                        spark_source_type(&join.left, index, views)
                    } else {
                        spark_source_type(&join.right, index - left_width, views)
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => {
                    spark_source_type(&join.left, index, views)
                }
                JoinType::RightSemi | JoinType::RightAnti => {
                    spark_source_type(&join.right, index, views)
                }
                JoinType::LeftMark | JoinType::RightMark => None,
            }
        }
        LogicalPlan::TableScan(scan) => {
            let source_index = match &scan.projection {
                Some(projection) => projection.get(index).copied(),
                None => Some(index),
            };
            let source_index = source_index?;
            if let Some(source) = scan.source.get_logical_plan() {
                return spark_source_type(&source, source_index, views);
            }
            if let Some(resolved) = views
                .zip(source_as_provider(&scan.source).ok())
                .and_then(|(views, provider)| views.definition_plan(provider.as_ref()))
            {
                return spark_source_type(&resolved, source_index, views);
            }
            scan.source
                .schema()
                .fields()
                .get(source_index)
                .map(|field| field.data_type().clone())
        }
        _ => None,
    }
}

fn spark_expr_type(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    views: Option<&ViewDefinitionPlans>,
) -> Option<DataType> {
    match expr {
        Expr::Alias(alias) => spark_expr_type(&alias.expr, input, views),
        Expr::Column(column) => input.and_then(|input| {
            input
                .schema()
                .index_of_column(column)
                .ok()
                .and_then(|position| spark_source_type(input, position, views))
        }),
        Expr::Literal(value, _) => Some(value.data_type()),
        Expr::Cast(cast) => Some(cast.field.data_type().clone()),
        Expr::TryCast(cast) => Some(cast.field.data_type().clone()),
        Expr::Negative(operand) => match operand.as_ref() {
            Expr::Literal(ScalarValue::Null, _) => Some(DataType::Float64),
            _ => spark_expr_type(operand, input, views),
        },
        Expr::Case(case) => {
            let mut branches = Vec::with_capacity(case.when_then_expr.len() + 1);
            for (_, value) in &case.when_then_expr {
                branches.push(spark_expr_type(value, input, views)?);
            }
            if let Some(else_expr) = &case.else_expr {
                branches.push(spark_expr_type(else_expr, input, views)?);
            }
            widen_operand_types(&branches)
        }
        Expr::ScalarFunction(function) => {
            if function.func.name() == FLOAT_TO_STRING_WRAPPER
                && let [argument] = function.args.as_slice()
            {
                return spark_expr_type(argument, input, views);
            }
            let picked = match branch_shape(function.func.name(), function.args.len())? {
                BranchShape::All => &function.args[..],
                BranchShape::First => function.args.get(..1)?,
                BranchShape::AfterFirst => function.args.get(1..)?,
            };
            let mut branches = Vec::with_capacity(picked.len());
            for argument in picked {
                branches.push(spark_expr_type(argument, input, views)?);
            }
            widen_operand_types(&branches)
        }
        _ => None,
    }
}

fn combine_branch_types(branches: impl Iterator<Item = Option<DataType>>) -> Option<DataType> {
    let mut distinct: Option<DataType> = None;
    for branch in branches {
        let branch = branch?;
        if matches!(branch, DataType::Null) {
            continue;
        }
        let branch = if is_string_type(&branch) {
            DataType::Utf8
        } else {
            branch
        };
        match &distinct {
            None => distinct = Some(branch),
            Some(current) if *current == branch => {}
            Some(_) => return None,
        }
    }
    Some(distinct.unwrap_or(DataType::Null))
}

fn find_target<'a>(
    presented: &'a ArrowSchema,
    wanted: &str,
    case_insensitive: bool,
) -> Option<&'a Field> {
    presented
        .fields()
        .iter()
        .find(|field| field.name() == wanted)
        .or_else(|| {
            presented
                .fields()
                .iter()
                .find(|field| case_insensitive && field.name().eq_ignore_ascii_case(wanted))
        })
        .map(AsRef::as_ref)
}
