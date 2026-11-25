use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, ScalarValue, internal_err};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Cast, DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Union, Values,
};
use datafusion::optimizer::AnalyzerRule;

use crate::instant_ts::{
    is_ltz_timestamp, is_wall_clock_cast_source, ltz_cast, ltz_timestamp_type, ntz_timestamp_type,
};
use crate::spark_string_timestamp::is_string_type;
use crate::spark_type_planner::planned_instant_cast;
use crate::timestamp_ntz_cast::timestamp_ntz_cast_expr;
use crate::timestamp_type::{is_spark_timestamp_type_deferred, spark_timestamp_type_from_options};

pub const NTZ_TIMESTAMP_CAST_RULE_NAME: &str = "spark_ntz_timestamp_cast";
const TYPE_COERCION_RULE_NAME: &str = "type_coercion";

#[derive(Debug, Default)]
struct SparkNtzTimestampCast;

#[allow(clippy::missing_errors_doc)]
pub fn insert_ntz_timestamp_cast_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules
        .iter()
        .position(|rule| rule.name() == TYPE_COERCION_RULE_NAME)
    else {
        return internal_err!(
            "cannot install {NTZ_TIMESTAMP_CAST_RULE_NAME}: analyzer rules lack \
             {TYPE_COERCION_RULE_NAME}"
        );
    };
    rules.insert(position, Arc::new(SparkNtzTimestampCast));
    Ok(rules)
}

impl AnalyzerRule for SparkNtzTimestampCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if is_spark_timestamp_type_deferred(config)
            || !spark_timestamp_type_from_options(config).is_ntz()
        {
            return Ok(plan);
        }
        let dml_stores = collect_dml_ltz_targets(&plan)?;
        plan.transform_up_with_subqueries(|node| rewrite_ntz_plan(node, &dml_stores))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        NTZ_TIMESTAMP_CAST_RULE_NAME
    }
}

struct DmlLtzStore {
    exprs: Vec<Expr>,
    ltz_targets: Vec<bool>,
}

fn collect_dml_ltz_targets(plan: &LogicalPlan) -> Result<Vec<DmlLtzStore>> {
    let mut stores = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Dml(statement) = node
            && let LogicalPlan::Projection(projection) = statement.input.as_ref()
            && projection.expr.len() == statement.target.schema().fields().len()
        {
            let ltz_targets = statement
                .target
                .schema()
                .fields()
                .iter()
                .map(|field| is_ltz_timestamp(field.data_type()))
                .collect();
            stores.push(DmlLtzStore {
                exprs: projection.expr.clone(),
                ltz_targets,
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(stores)
}

fn rewrite_ntz_plan(
    plan: LogicalPlan,
    dml_stores: &[DmlLtzStore],
) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Values(values) => rewrite_ntz_values(values),
        LogicalPlan::Union(union) => rebuild_stale_union(union),
        LogicalPlan::Projection(projection) => rewrite_ntz_projection(projection, dml_stores),
        LogicalPlan::Dml(statement) => ensure_dml_ltz_casts(statement),
        other => rewrite_ntz_expressions(other),
    }
}

fn is_synthesized_ltz_store_cast(expr: &Expr) -> bool {
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    matches!(
        inner,
        Expr::Cast(cast)
            if is_ltz_timestamp(cast.field.data_type())
                && matches!(cast.expr.as_ref(), Expr::Column(_))
    )
}

fn wrap_inner_as_ltz(expr: Expr, schema: &DFSchema) -> Expr {
    match expr {
        Expr::Alias(alias) => Expr::Alias(Alias {
            expr: Box::new(wrap_inner_as_ltz(*alias.expr, schema)),
            ..alias
        }),
        other => {
            let nullable = other.nullable(schema).unwrap_or(true);
            let field = Arc::new(Field::new("ts", ltz_timestamp_type(), nullable));
            Expr::Cast(Cast::new_from_field(Box::new(other), field))
        }
    }
}

fn rewrite_ntz_projection(
    projection: Projection,
    dml_stores: &[DmlLtzStore],
) -> Result<Transformed<LogicalPlan>> {
    let Some(ltz_targets) = dml_stores
        .iter()
        .find(|store| store.exprs == projection.expr)
        .map(|store| store.ltz_targets.as_slice())
    else {
        return rewrite_ntz_expressions(LogicalPlan::Projection(projection));
    };
    let schema = projection.input.schema();
    let name_preserver = NamePreserver::new_for_projection();
    let mut changed = false;
    let mut exprs = Vec::with_capacity(projection.expr.len());
    for (expr, &targets_ltz) in projection.expr.into_iter().zip(ltz_targets) {
        if targets_ltz && is_synthesized_ltz_store_cast(&expr) {
            exprs.push(expr);
            continue;
        }
        let saved_name = name_preserver.save(&expr);
        let rewritten = rewrite_ntz_expr_tree(expr, schema.as_ref())?;
        changed |= rewritten.transformed;
        let restored = saved_name.restore(rewritten.data);
        if targets_ltz
            && restored
                .get_type(schema.as_ref())
                .is_ok_and(|found| is_wall_clock_cast_source(&found))
        {
            changed = true;
            exprs.push(wrap_inner_as_ltz(restored, schema.as_ref()));
        } else {
            exprs.push(restored);
        }
    }
    let rebuilt = Projection::try_new(exprs, projection.input)?;
    Ok(Transformed::new_transformed(
        LogicalPlan::Projection(rebuilt),
        changed,
    ))
}

fn ensure_dml_ltz_casts(statement: DmlStatement) -> Result<Transformed<LogicalPlan>> {
    let targets = statement.target.schema();
    let input_schema = statement.input.schema();
    if input_schema.fields().len() != targets.fields().len() {
        return Ok(Transformed::no(LogicalPlan::Dml(statement)));
    }
    let needs_cast = input_schema
        .fields()
        .iter()
        .zip(targets.fields())
        .any(|(source, target)| {
            is_ltz_timestamp(target.data_type()) && is_wall_clock_cast_source(source.data_type())
        });
    if !needs_cast {
        return Ok(Transformed::no(LogicalPlan::Dml(statement)));
    }
    let exprs = input_schema
        .iter()
        .zip(targets.fields())
        .map(|((qualifier, source), target)| {
            let col = Expr::Column(Column::new(qualifier.cloned(), source.name()));
            if is_ltz_timestamp(target.data_type()) && is_wall_clock_cast_source(source.data_type())
            {
                let field = Arc::new(Field::new(
                    target.name(),
                    ltz_timestamp_type(),
                    source.is_nullable(),
                ));
                Expr::Cast(Cast::new_from_field(Box::new(col), field)).alias(target.name())
            } else {
                col.alias(target.name())
            }
        })
        .collect();
    let projection = Projection::try_new(exprs, statement.input)?;
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(LogicalPlan::Projection(projection)),
        ..statement
    })))
}

fn rewrite_ntz_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let empty = DFSchema::empty();
    let mut changed = false;
    let mut rows = Vec::with_capacity(values.values.len());
    for row in values.values {
        let mut rewritten_row = Vec::with_capacity(row.len());
        for cell in row {
            let rewritten = rewrite_ntz_expr_tree(cell, &empty)?;
            changed |= rewritten.transformed;
            rewritten_row.push(rewritten.data);
        }
        rows.push(rewritten_row);
    }
    let mut fields: Vec<(Option<datafusion::common::TableReference>, FieldRef)> = values
        .schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect();
    let mut schema_changed = false;
    for (col_index, (_, field)) in fields.iter_mut().enumerate() {
        if !is_ltz_timestamp(field.data_type()) {
            continue;
        }
        let Ok(types) = rows
            .iter()
            .map(|row| row[col_index].get_type(&empty))
            .collect::<Result<Vec<DataType>>>()
        else {
            continue;
        };
        let any_naive = types
            .iter()
            .any(|found| matches!(found, DataType::Timestamp(TimeUnit::Microsecond, None)));
        let any_instant = types.iter().any(is_ltz_timestamp);
        if any_naive && !any_instant {
            *field = Arc::new(field.as_ref().clone().with_data_type(ntz_timestamp_type()));
            schema_changed = true;
        }
    }
    if !changed && !schema_changed {
        return Ok(Transformed::no(LogicalPlan::Values(Values {
            schema: values.schema,
            values: rows,
        })));
    }
    let schema = if schema_changed {
        Arc::new(DFSchema::new_with_metadata(
            fields,
            values.schema.metadata().clone(),
        )?)
    } else {
        values.schema
    };
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema,
        values: rows,
    })))
}

fn rebuild_stale_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let stale = union.inputs.iter().any(|input| {
        let merged = union.schema.fields();
        let mine = input.schema().fields();
        mine.len() != merged.len()
            || mine
                .iter()
                .zip(merged.iter())
                .any(|(mine, merged)| mine.data_type() != merged.data_type())
    });
    if !stale {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let rebuilt = Union::try_new_with_loose_types(union.inputs)?;
    Ok(Transformed::yes(LogicalPlan::Union(rebuilt)))
}

fn rewrite_ntz_expressions(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = rewrite_ntz_expr_tree(expr, &schema)?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_ntz_expr_tree(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let protected = protect_ltz_arrow_casts(expr, schema)?;
    let changed = protected.transformed;
    let rewritten = protected
        .data
        .transform_up(|node| Ok(rewrite_cast_as_ntz(node, schema)))?;
    Ok(Transformed::new_transformed(
        rewritten.data,
        changed || rewritten.transformed,
    ))
}

fn is_ltz_arrow_cast_target(expr: &Expr) -> bool {
    let Expr::Literal(
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)),
        _,
    ) = expr
    else {
        return false;
    };
    text.parse::<DataType>()
        .is_ok_and(|parsed| is_ltz_timestamp(&parsed))
}

fn protect_ltz_arrow_casts(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.transform_down(|mut node| {
        if let Expr::ScalarFunction(ref mut function) = node
            && function.func.name() == "arrow_cast"
            && function.args.len() == 2
            && is_ltz_arrow_cast_target(&function.args[1])
            && let Expr::Cast(cast) = &function.args[0]
            && is_ltz_timestamp(cast.field.data_type())
        {
            let inner = *cast.expr.clone();
            let rewritten_inner = rewrite_ntz_expr_tree(inner, schema)?.data;
            function.args[0] = ltz_cast::timestamp_ltz_cast_expr(rewritten_inner, false);
            return Ok(Transformed::new(node, true, TreeNodeRecursion::Jump));
        }
        Ok(Transformed::no(node))
    })
}

fn rewrite_cast_as_ntz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if let Expr::TryCast(try_cast) = &expr {
        let targeting_ltz = is_ltz_timestamp(try_cast.field.data_type());
        if targeting_ltz
            && let Ok(source) = try_cast.expr.get_type(schema)
            && (is_ltz_timestamp(&source)
                || is_wall_clock_cast_source(&source)
                || matches!(source, DataType::Null))
        {
            return Transformed::yes(timestamp_ntz_cast_expr(*try_cast.expr.clone(), true));
        }
        return Transformed::no(expr);
    }
    if let Expr::Cast(cast) = &expr {
        let targeting_ltz = is_ltz_timestamp(cast.field.data_type());
        let targeting_seconds = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Second, _)
        );
        if let Ok(source) = cast.expr.get_type(schema) {
            if let Some(planned) = planned_instant_cast(cast, &source, schema, true) {
                return Transformed::yes(planned);
            }
            if targeting_ltz {
                if is_ltz_timestamp(&source) {
                    return Transformed::yes(timestamp_ntz_cast_expr(*cast.expr.clone(), false));
                }
                if matches!(source, DataType::Timestamp(TimeUnit::Microsecond, None)) {
                    return Transformed::yes(*cast.expr.clone());
                }
                if is_string_type(&source) || matches!(source, DataType::Date32 | DataType::Date64)
                {
                    return Transformed::yes(timestamp_ntz_cast_expr(*cast.expr.clone(), false));
                }
            }
            let source_is_int_or_null = source.is_integer() || matches!(source, DataType::Null);
            if targeting_seconds && source_is_int_or_null {
                return wrap_as_ntz(expr, schema);
            }
        }
    }
    Transformed::no(expr)
}

fn wrap_as_ntz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let nullable = expr.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ntz_timestamp_type(), nullable));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(expr), field)))
}
