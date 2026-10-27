use std::sync::{Arc, Mutex, PoisonError};

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF,
};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::table::Table;
use repark_common::udf_names::{NARROW_TIMESTAMP_NS_NAME, STORE_PAIR_KERNEL_NAME, ZONE_SEAM_NAME};

use super::ntz_store::{
    is_ltz_instant_target, needs_ltz_instant_cast, store_kernel_udf_name, wall_kernel_input,
    wall_kernel_reads,
};
use super::store_assign::{refuse_unless_write_store_assignable, without_field_metadata};
use super::store_overflow::{
    ConformStoreFlags, PairSite, conform_one_store_expr, rewrap_aliases, split_aliases,
    store_output_name,
};
use super::struct_pairing::{StructPairing, holds_struct_type, pair_store_value};

const ARROW_CAST_NAME: &str = "arrow_cast";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageSite {
    InsertByName,
    InsertOverwrite,
    PartitionOverwrite,
    MergeInsert,
    MergeUpdate,
    PredicateUpdate,
    MicroBatchAppend,
}

#[derive(Clone, Copy, Debug)]
pub struct StageRequest<'a> {
    pub site: StageSite,
    pub targets: &'a [(String, DataType)],
    pub by_name: bool,
    pub fold: bool,
    pub gate_op: Option<&'a str>,
    pub pairing: StructPairing,
    pub table: &'a str,
}

#[derive(Debug, Default)]
pub struct StageRecord {
    sites: Mutex<Vec<StageSite>>,
}

pub fn install_stage_record(ctx: &SessionContext) {
    let state = ctx.state_ref();
    state
        .write()
        .config_mut()
        .set_extension(Arc::new(StageRecord::default()));
}

fn stage_record(ctx: &SessionContext) -> Option<Arc<StageRecord>> {
    ctx.state_ref()
        .read()
        .config()
        .get_extension::<StageRecord>()
}

pub fn clear_stage_record(ctx: &SessionContext) {
    if let Some(record) = stage_record(ctx) {
        record
            .sites
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

#[must_use]
pub fn last_stage_sites(ctx: &SessionContext) -> Vec<StageSite> {
    stage_record(ctx).map_or_else(Vec::new, |record| {
        record
            .sites
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    })
}

fn record_site(ctx: &SessionContext, site: StageSite) {
    if let Some(record) = stage_record(ctx) {
        let mut sites = record.sites.lock().unwrap_or_else(PoisonError::into_inner);
        if !sites.contains(&site) {
            sites.push(site);
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn table_targets(
    arrow: &ArrowSchema,
    listed: &[String],
    reserved: &[String],
) -> Result<Vec<(String, DataType)>> {
    let names: Vec<String> = if listed.is_empty() {
        arrow
            .fields()
            .iter()
            .filter(|field| {
                !reserved
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(field.name()))
            })
            .map(|field| field.name().clone())
            .collect()
    } else {
        listed.to_vec()
    };
    names
        .iter()
        .map(|name| {
            let field = arrow
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .or_else(|| {
                    arrow
                        .fields()
                        .iter()
                        .find(|field| field.name().eq_ignore_ascii_case(name))
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "the write names column `{name}`, which the target table does not have"
                    ))
                })?;
            Ok((field.name().clone(), field.data_type().clone()))
        })
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub async fn conform_frame(
    ctx: &SessionContext,
    frame: DataFrame,
    request: &StageRequest<'_>,
) -> Result<DataFrame> {
    let plan = conform_stage(ctx, frame.logical_plan().clone(), request)?;
    ctx.execute_logical_plan(plan).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn conform_table_frame(
    ctx: &SessionContext,
    table: &Table,
    frame: DataFrame,
    site: StageSite,
) -> Result<DataFrame> {
    let arrow = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let targets: Vec<(String, DataType)> = arrow
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();
    let request = StageRequest {
        site,
        targets: &targets,
        by_name: true,
        fold: true,
        gate_op: Some("append"),
        pairing: StructPairing::by_name(false),
        table: "``",
    };
    conform_frame(ctx, frame, &request).await
}

#[allow(clippy::missing_errors_doc)]
pub fn conform_stage(
    ctx: &SessionContext,
    plan: LogicalPlan,
    request: &StageRequest<'_>,
) -> Result<LogicalPlan> {
    if !request.by_name && request.targets.len() != plan.schema().fields().len() {
        record_site(ctx, request.site);
        return Ok(plan);
    }
    let planned = if analyzed_by_the_door(request.site) {
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    } else {
        planned_types(ctx, &plan)?
    };
    let plan = store_leaves(ctx, plan, &planned, request)?;
    let plan = conform_outputs(ctx, plan, &planned, request)?;
    record_site(ctx, request.site);
    Ok(plan)
}

fn analyzed_by_the_door(site: StageSite) -> bool {
    matches!(
        site,
        StageSite::MergeInsert | StageSite::MergeUpdate | StageSite::PredicateUpdate
    )
}

fn planned_types(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Vec<DataType>> {
    let state = ctx.state();
    let analyzed =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})?;
    let planned: Vec<DataType> = analyzed
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    if planned.len() != plan.schema().fields().len() {
        return Err(DataFusionError::Internal(format!(
            "the session analyzer planned {} columns for a {}-column write",
            planned.len(),
            plan.schema().fields().len()
        )));
    }
    Ok(planned)
}

fn resolve_target<'a>(
    request: &StageRequest<'a>,
    width: usize,
    position: usize,
    name: &str,
) -> Option<&'a (String, DataType)> {
    if !request.by_name && request.targets.len() == width {
        return request.targets.get(position);
    }
    request
        .targets
        .iter()
        .find(|(candidate, _)| candidate == name)
        .or_else(|| {
            request
                .targets
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        })
}

fn store_leaves(
    ctx: &SessionContext,
    plan: LogicalPlan,
    planned: &[DataType],
    request: &StageRequest<'_>,
) -> Result<LogicalPlan> {
    let schema = Arc::clone(plan.schema());
    let width = schema.fields().len();
    let mut converted = false;
    let mut exprs = Vec::with_capacity(width);
    for (position, ((qualifier, field), source)) in schema.iter().zip(planned).enumerate() {
        let column = Expr::Column(Column::from((qualifier, field.as_ref())));
        let stored = match resolve_target(request, width, position, field.name()) {
            Some((_, target)) => leaf_store(ctx, column.clone(), field, source, target)?,
            None => None,
        };
        match stored {
            Some(store) => {
                converted = true;
                exprs.push(store.alias(field.name()));
            }
            None => exprs.push(column),
        }
    }
    if !converted {
        return Ok(plan);
    }
    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}

fn leaf_store(
    ctx: &SessionContext,
    column: Expr,
    field: &Field,
    source: &DataType,
    target: &DataType,
) -> Result<Option<Expr>> {
    if wall_kernel_reads(source, target)
        && let Some(name) = store_kernel_udf_name(target)
    {
        let udf = registered_kernel(ctx, name, source, target)?;
        let value = match wall_kernel_input(source) {
            Some(plain) => {
                let plain = Field::new(field.name(), plain.clone(), field.is_nullable());
                Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(plain)))
            }
            None => column,
        };
        return Ok(Some(call(udf, vec![value])));
    }
    if !(is_ltz_instant_target(target) && needs_ltz_instant_cast(source)) {
        return Ok(None);
    }
    if matches!(source, DataType::Timestamp(TimeUnit::Nanosecond, _)) {
        let udf = registered_kernel(ctx, NARROW_TIMESTAMP_NS_NAME, source, target)?;
        return Ok(Some(call(udf, vec![column])));
    }
    let instant = Field::new(field.name(), target.clone(), field.is_nullable());
    Ok(Some(Expr::Cast(Cast::new_from_field(
        Box::new(column),
        Arc::new(instant),
    ))))
}

fn registered_kernel(
    ctx: &SessionContext,
    name: &str,
    source: &DataType,
    target: &DataType,
) -> Result<Arc<ScalarUDF>> {
    ctx.udf(name).map_err(|_| {
        DataFusionError::Plan(format!(
            "the store kernel `{name}` is not registered in this session, so a {source} value \
             cannot be stored into a {target} column in the session time zone; RePark refuses \
             the write instead of storing the value as UTC. A session built with the Spark \
             dialect or the ANSI dialect registers it"
        ))
    })
}

fn call(udf: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
}

fn into_projection(plan: LogicalPlan) -> Result<Projection> {
    match plan {
        LogicalPlan::Projection(projection) => Ok(projection),
        other => {
            let columns = other
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            Projection::try_new(columns, Arc::new(other))
        }
    }
}

fn conform_outputs(
    ctx: &SessionContext,
    plan: LogicalPlan,
    planned: &[DataType],
    request: &StageRequest<'_>,
) -> Result<LogicalPlan> {
    let original = plan.clone();
    let projection = into_projection(plan)?;
    let width = projection.expr.len();
    if let Some(op) = request.gate_op {
        judge_store_assignable(&projection, request, op)?;
    }
    let mut input = Arc::clone(&projection.input);
    let schema = input.schema().clone();
    let mut changed = false;
    let mut out = Vec::with_capacity(width);
    for (position, expr) in projection.expr.iter().enumerate() {
        let (inner, frames) = split_aliases(expr);
        let name = store_output_name(inner, &frames);
        let Some((column, target_type)) = resolve_target(request, width, position, &name) else {
            out.push(expr.clone());
            continue;
        };
        let flags = ConformStoreFlags {
            fold: request.fold,
            in_values: false,
            unwrap_cast: false,
            site: PairSite {
                pairing: request.pairing,
                table: request.table,
            },
        };
        let pairs =
            !(request.site == StageSite::PartitionOverwrite && holds_struct_type(target_type));
        let conformed = if pairs {
            conform_one_store_expr(
                inner,
                schema.as_ref(),
                &mut input,
                column,
                target_type,
                flags,
            )?
        } else {
            None
        };
        let mut touched = conformed.is_some();
        let mut current = conformed.unwrap_or_else(|| inner.clone());
        if let Some(partial) = planned.get(position).and_then(|source| {
            nested_partial(
                request,
                column,
                &current,
                source,
                target_type,
                pairs,
                schema.as_ref(),
            )
        }) {
            current = seam_call(ctx, current, &partial, target_type)?;
            touched = true;
        }
        let (seamed, rewritten) = seam_nested_arrow_casts(ctx, current, schema.as_ref())?;
        current = seamed;
        touched |= rewritten;
        if !touched {
            out.push(expr.clone());
            continue;
        }
        if frames.is_empty() {
            out.push(current.alias(column));
        } else {
            out.push(rewrap_aliases(current, &frames));
        }
        changed = true;
    }
    if !changed {
        return Ok(original);
    }
    Ok(LogicalPlan::Projection(Projection::try_new(out, input)?))
}

fn judge_store_assignable(
    projection: &Projection,
    request: &StageRequest<'_>,
    op: &str,
) -> Result<()> {
    let schema = projection.input.schema();
    let width = projection.expr.len();
    for (position, expr) in projection.expr.iter().enumerate() {
        let (inner, frames) = split_aliases(expr);
        let name = store_output_name(inner, &frames);
        let Some((column, target_type)) = resolve_target(request, width, position, &name) else {
            continue;
        };
        let Ok(source) = inner.get_type(schema.as_ref()) else {
            continue;
        };
        refuse_unless_write_store_assignable(op, column, &source, target_type)?;
    }
    Ok(())
}

fn nested_partial(
    request: &StageRequest<'_>,
    column: &str,
    current: &Expr,
    source: &DataType,
    target: &DataType,
    pairs: bool,
    schema: &DFSchema,
) -> Option<DataType> {
    if !is_container(target) || matches!(current, Expr::Cast(_) | Expr::TryCast(_)) {
        return None;
    }
    let value = if pairs && holds_struct_type(target) {
        pair_store_value(request.pairing, request.table, column, source, target)
            .ok()?
            .paired_type()
    } else {
        source.clone()
    };
    zone_partial_for(Some(current), &value, target, schema)
}

fn seam_nested_arrow_casts(
    ctx: &SessionContext,
    expr: Expr,
    schema: &DFSchema,
) -> Result<(Expr, bool)> {
    let mut rewritten = false;
    let expr = expr
        .transform_up(|node| {
            if let Expr::ScalarFunction(function) = &node {
                if function.func.name() == ARROW_CAST_NAME && function.args.len() == 2 {
                    return seam_one_arrow_cast(ctx, node, schema, &mut rewritten);
                }
                return Ok(Transformed::no(node));
            }
            if matches!(node, Expr::Case(_)) {
                return wrap_scalar_seam_case(node, schema, &mut rewritten);
            }
            Ok(Transformed::no(node))
        })
        .data()?;
    Ok((expr, rewritten))
}

fn seam_one_arrow_cast(
    ctx: &SessionContext,
    node: Expr,
    schema: &DFSchema,
    rewritten: &mut bool,
) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(function) = &node else {
        return Ok(Transformed::no(node));
    };
    let (Ok(target), Ok(source)) = (node.get_type(schema), function.args[0].get_type(schema))
    else {
        return Ok(Transformed::no(node));
    };
    let Some(partial) = is_container(&target)
        .then(|| zone_partial_for(Some(&function.args[0]), &source, &target, schema))
        .flatten()
    else {
        return Ok(Transformed::no(node));
    };
    let Expr::ScalarFunction(mut function) = node else {
        return Ok(Transformed::no(node));
    };
    let value = std::mem::replace(
        &mut function.args[0],
        Expr::Literal(ScalarValue::Null, None),
    );
    function.args[0] = seam_call(ctx, value, &partial, &target)?;
    *rewritten = true;
    Ok(Transformed::yes(Expr::ScalarFunction(function)))
}

fn wrap_scalar_seam_case(
    node: Expr,
    schema: &DFSchema,
    rewritten: &mut bool,
) -> Result<Transformed<Expr>> {
    if !scalar_seam_inside(&node)? {
        return Ok(Transformed::no(node));
    }
    let Ok(case_type) = node.get_type(schema) else {
        return Ok(Transformed::no(node));
    };
    if !is_container(&case_type) || !has_field_metadata(&case_type) {
        return Ok(Transformed::no(node));
    }
    *rewritten = true;
    Ok(Transformed::yes(Expr::Cast(Cast::new(
        Box::new(node),
        without_field_metadata(&case_type),
    ))))
}

fn scalar_seam_inside(expr: &Expr) -> Result<bool> {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::ScalarFunction(function)
                if function.func.name() == ZONE_SEAM_NAME
                    && function
                        .args
                        .first()
                        .is_some_and(|arg| arg.column_refs().is_empty())
        ))
    })
}

fn has_field_metadata(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| !field.metadata().is_empty() || has_field_metadata(field.data_type())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            !field.metadata().is_empty() || has_field_metadata(field.data_type())
        }
        _ => false,
    }
}

fn seam_call(
    ctx: &SessionContext,
    value: Expr,
    partial: &DataType,
    target: &DataType,
) -> Result<Expr> {
    let udf = ctx.udf(ZONE_SEAM_NAME).map_err(|_| {
        DataFusionError::Plan(format!(
            "the store kernel `{ZONE_SEAM_NAME}` is not registered in this session, so a nested \
             value cannot be stored into a {target} column in the session time zone; RePark \
             refuses the write instead of storing the value as UTC. A session built with the \
             Spark dialect or the ANSI dialect registers it"
        ))
    })?;
    let carrier = ScalarValue::try_new_null(partial)?;
    Ok(call(udf, vec![value, Expr::Literal(carrier, None)]))
}

fn is_container(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

fn is_zone_leaf(source: &DataType, target: &DataType) -> bool {
    let wall = DataType::Timestamp(TimeUnit::Microsecond, None);
    let instant = matches!(target, DataType::Timestamp(TimeUnit::Microsecond, Some(_)));
    let from_instant = matches!(source, DataType::Timestamp(TimeUnit::Microsecond, Some(_)));
    (instant && (*source == wall || matches!(source, DataType::Date32)))
        || (from_instant && *target == wall)
}

fn partial_field(source: &Field, target: &Field) -> (Field, bool) {
    match zone_partial(source.data_type(), target.data_type()) {
        Some(partial) => (Field::new(source.name(), partial, true), true),
        None => (source.clone(), false),
    }
}

fn is_instant(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, Some(_)))
}

fn is_micro_wall(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

fn try_cast_marks_excluded(expr: &Expr, schema: &DFSchema) -> bool {
    let mut current = expr;
    while let Expr::Alias(alias) = current {
        current = alias.expr.as_ref();
    }
    let Expr::TryCast(cast) = current else {
        return false;
    };
    is_micro_wall(cast.field.data_type())
        && cast
            .expr
            .get_type(schema)
            .is_ok_and(|source| is_instant(&source))
}

fn unwrap_seam_position(expr: &Expr) -> &Expr {
    let mut current = expr;
    loop {
        let inner = match current {
            Expr::Alias(alias) => Some(alias.expr.as_ref()),
            Expr::Cast(cast) => Some(cast.expr.as_ref()),
            Expr::ScalarFunction(function)
                if matches!(
                    function.func.name(),
                    ARROW_CAST_NAME | STORE_PAIR_KERNEL_NAME | ZONE_SEAM_NAME
                ) && !function.args.is_empty() =>
            {
                Some(&function.args[0])
            }
            _ => None,
        };
        let Some(inner) = inner else {
            return current;
        };
        current = inner;
    }
}

fn struct_value_args(expr: &Expr) -> Option<Vec<&Expr>> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.name() == "named_struct" {
        if function.args.len() % 2 != 0 {
            return None;
        }
        Some(function.args.iter().skip(1).step_by(2).collect())
    } else if function.func.name() == "struct" {
        Some(function.args.iter().collect())
    } else {
        None
    }
}

fn array_element_args(expr: &Expr) -> Option<Vec<&Expr>> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !matches!(function.func.name(), "array" | "make_array") || function.args.is_empty() {
        return None;
    }
    Some(function.args.iter().collect())
}

fn map_key_value_args(expr: &Expr) -> Option<(Vec<&Expr>, Vec<&Expr>)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.name() != "map" || function.args.is_empty() || function.args.len() % 2 != 0 {
        return None;
    }
    Some((
        function.args.iter().step_by(2).collect(),
        function.args.iter().skip(1).step_by(2).collect(),
    ))
}

fn shaped_field(
    args: Option<Vec<&Expr>>,
    source: &Field,
    target: &Field,
    schema: &DFSchema,
) -> (Field, bool) {
    let Some(args) = args else {
        return partial_field(source, target);
    };
    let mut partials = args
        .iter()
        .map(|arg| zone_partial_for(Some(arg), source.data_type(), target.data_type(), schema));
    let Some(first) = partials.next() else {
        return partial_field(source, target);
    };
    if !partials.all(|partial| partial == first) {
        return partial_field(source, target);
    }
    match first {
        Some(partial) => (Field::new(source.name(), partial, true), true),
        None => (source.clone(), false),
    }
}

fn zone_partial_for(
    expr: Option<&Expr>,
    source: &DataType,
    target: &DataType,
    schema: &DFSchema,
) -> Option<DataType> {
    let shaped = expr.map(unwrap_seam_position);
    match (source, target) {
        (DataType::Struct(sources), DataType::Struct(targets))
            if sources.len() == targets.len() =>
        {
            let values = shaped
                .and_then(struct_value_args)
                .filter(|values| values.len() == sources.len());
            let mut found = false;
            let fields: Vec<Field> = sources
                .iter()
                .zip(targets.iter())
                .enumerate()
                .map(|(index, (source, target))| {
                    let child = values.as_ref().map(|values| values[index]);
                    match zone_partial_for(child, source.data_type(), target.data_type(), schema) {
                        Some(partial) => {
                            found = true;
                            Field::new(source.name(), partial, true)
                        }
                        None => source.as_ref().clone(),
                    }
                })
                .collect();
            found.then(|| DataType::Struct(Fields::from(fields)))
        }
        (DataType::List(source), DataType::List(target)) => {
            let (field, converted) =
                shaped_field(shaped.and_then(array_element_args), source, target, schema);
            converted.then(|| DataType::List(Arc::new(field)))
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => {
            let (field, converted) =
                shaped_field(shaped.and_then(array_element_args), source, target, schema);
            converted.then(|| DataType::LargeList(Arc::new(field)))
        }
        (DataType::FixedSizeList(source, size), DataType::FixedSizeList(target, other))
            if size == other =>
        {
            let (field, converted) =
                shaped_field(shaped.and_then(array_element_args), source, target, schema);
            converted.then(|| DataType::FixedSizeList(Arc::new(field), *size))
        }
        (DataType::Map(source, sorted), DataType::Map(target, _)) => {
            let (DataType::Struct(sources), DataType::Struct(targets)) =
                (source.data_type(), target.data_type())
            else {
                return None;
            };
            if sources.len() != 2 || targets.len() != 2 {
                return None;
            }
            let (keys, values) = shaped
                .and_then(map_key_value_args)
                .map_or((None, None), |(keys, values)| (Some(keys), Some(values)));
            let (key, keyed) = shaped_field(keys, &sources[0], &targets[0], schema);
            let (value, converted) = shaped_field(values, &sources[1], &targets[1], schema);
            let entries = DataType::Struct(Fields::from(vec![key, value]));
            (keyed || converted).then(|| {
                DataType::Map(
                    Arc::new(Field::new(source.name(), entries, source.is_nullable())),
                    *sorted,
                )
            })
        }
        (source, target) if !is_container(source) && !is_container(target) => {
            if !is_zone_leaf(source, target) {
                return None;
            }
            let excluded = shaped.is_some_and(|shaped| try_cast_marks_excluded(shaped, schema));
            (!excluded).then(|| target.clone())
        }
        _ => None,
    }
}

#[must_use]
pub(crate) fn zone_partial(source: &DataType, target: &DataType) -> Option<DataType> {
    zone_partial_for(None, source, target, &DFSchema::empty())
}

#[cfg(test)]
mod seam_pins;
#[cfg(test)]
mod tests;
