use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Cast, Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{PrimitiveType, Type};
use iceberg::table::Table;

use super::store_assign::without_field_metadata;
use super::update_cast::incompatible_update_message;

pub const NTZ_WALL_CAST_UDF_NAME: &str = "__repark_cast_timestamp_ntz__";
pub const NS_WALL_CAST_UDF_NAME: &str = "__repark_cast_timestamp_ns__";

#[must_use]
pub fn is_ntz_wall_target(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

#[must_use]
pub fn is_ltz_instant_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

#[must_use]
pub fn wall_cast_udf_name(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(NTZ_WALL_CAST_UDF_NAME),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(NS_WALL_CAST_UDF_NAME),
        _ => None,
    }
}

#[must_use]
pub fn wall_cast_sql(expr_sql: &str, target: &DataType) -> Option<String> {
    wall_cast_udf_name(target).map(|name| format!("{name}(({expr_sql}))"))
}

#[must_use]
pub fn holds_nested_ns_wall(data_type: &DataType) -> bool {
    let holds = |field: &Field| {
        matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        ) || holds_nested_ns_wall(field.data_type())
    };
    match data_type {
        DataType::Struct(fields) => fields.iter().any(|field| holds(field)),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => holds(field),
        _ => false,
    }
}

#[must_use]
pub fn nested_wall_conform_sql(expr_sql: &str, target: &DataType) -> Option<String> {
    holds_nested_ns_wall(target).then(|| {
        let shape = without_field_metadata(target)
            .to_string()
            .replace('\'', "''");
        format!("{NS_WALL_CAST_UDF_NAME}(({expr_sql}), arrow_cast(NULL, '{shape}'))")
    })
}

#[must_use]
pub fn wall_kernel_reads(source: &DataType, target: &DataType) -> bool {
    match target {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            source != target
                && matches!(
                    source,
                    DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
                )
        }
        _ => matches!(source, DataType::Timestamp(_, Some(_))),
    }
}

#[must_use]
pub fn ltz_instant_cast_sql(expr_sql: &str) -> String {
    format!("CAST(({expr_sql}) AS TIMESTAMP)")
}

#[must_use]
pub fn needs_ltz_instant_cast(source: &DataType) -> bool {
    matches!(
        source,
        DataType::Timestamp(TimeUnit::Microsecond, None) | DataType::Date32 | DataType::Date64
    )
}

#[must_use]
pub fn analyzed_types(ctx: &SessionContext, plan: &LogicalPlan) -> Option<Vec<DataType>> {
    let state = ctx.state();
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        .ok()?;
    Some(
        analyzed
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect(),
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn zone_stores(
    ctx: &SessionContext,
    frame: DataFrame,
    table: &Table,
    listed: &[String],
    reserved: &[String],
) -> Result<DataFrame> {
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    let names: Vec<String> = if listed.is_empty() {
        fields
            .iter()
            .filter(|field| {
                !reserved
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&field.name))
            })
            .map(|field| field.name.clone())
            .collect()
    } else {
        listed.to_vec()
    };
    let targets: Vec<Option<DataType>> = names
        .iter()
        .map(|name| {
            let field = fields
                .iter()
                .find(|field| field.name == *name)
                .or_else(|| {
                    fields
                        .iter()
                        .find(|field| field.name.eq_ignore_ascii_case(name))
                })?;
            match field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Timestamp) => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                Type::Primitive(PrimitiveType::TimestampNs) => {
                    Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
                }
                Type::Primitive(PrimitiveType::Timestamptz) => Some(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(Arc::from("+00:00")),
                )),
                Type::Primitive(_) => None,
                nested => iceberg::arrow::type_to_arrow_type(nested)
                    .ok()
                    .map(|arrow| without_field_metadata(&arrow))
                    .filter(holds_nested_ns_wall),
            }
        })
        .collect();
    zone_store_frame(ctx, frame, &targets)
}

#[allow(clippy::missing_errors_doc)]
pub fn zone_stores_by_name(
    ctx: &SessionContext,
    frame: DataFrame,
    table: &Table,
) -> Result<DataFrame> {
    let names: Vec<String> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    zone_stores(ctx, frame, table, &names, &[])
}

fn zone_store_frame(
    ctx: &SessionContext,
    frame: DataFrame,
    targets: &[Option<DataType>],
) -> Result<DataFrame> {
    if targets.len() != frame.schema().fields().len() || targets.iter().all(Option::is_none) {
        return Ok(frame);
    }
    let Some(planned) = analyzed_types(ctx, frame.logical_plan()) else {
        return Ok(frame);
    };
    if planned.len() != targets.len() {
        return Ok(frame);
    }
    let mut converted = false;
    let exprs: Vec<Expr> = frame
        .schema()
        .iter()
        .zip(planned.iter().zip(targets))
        .map(|((qualifier, field), (source, target))| {
            let column = Expr::Column(Column::from((qualifier, field.as_ref())));
            let wall = target
                .as_ref()
                .filter(|target| wall_kernel_reads(source, target))
                .and_then(wall_cast_udf_name)
                .and_then(|name| ctx.udf(name).ok());
            let nested = target
                .as_ref()
                .filter(|target| holds_nested_ns_wall(target) && source != *target)
                .and_then(|target| ScalarValue::try_from(target).ok())
                .zip(ctx.udf(NS_WALL_CAST_UDF_NAME).ok());
            let store = match (target, wall) {
                (_, Some(udf)) => Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![column])),
                (_, None) if nested.is_some() => {
                    let Some((shape, udf)) = nested else {
                        return column;
                    };
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        udf,
                        vec![column, Expr::Literal(shape, None)],
                    ))
                }
                (Some(target), None)
                    if is_ltz_instant_target(target) && needs_ltz_instant_cast(source) =>
                {
                    let instant = Field::new(field.name(), target.clone(), field.is_nullable());
                    Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(instant)))
                }
                _ => return column,
            };
            converted = true;
            store.alias(field.name())
        })
        .collect();
    if !converted {
        return Ok(frame);
    }
    frame.select(exprs)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_ntz_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if !targets
        .iter()
        .any(|(_, data_type)| is_ntz_wall_target(data_type))
    {
        return Ok(());
    }
    let state = ctx.state();
    let analyzed =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})?;
    let planned = analyzed.schema().fields();
    if planned.len() != targets.len() {
        return Ok(());
    }
    for (field, (column, target)) in planned.iter().zip(targets) {
        if !is_ntz_wall_target(target) {
            continue;
        }
        if let Some(text) =
            incompatible_update_message(table, &format!("`{column}`"), field.data_type(), target)
        {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

    use super::{holds_nested_ns_wall, wall_kernel_reads};

    #[test]
    fn the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast() {
        let instant = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        for (source, into_micros, into_nanos) in [
            (&instant, true, true),
            (&micros, false, true),
            (&DataType::Date32, false, true),
            (&nanos, false, false),
            (&DataType::Utf8, false, false),
            (&DataType::Int64, false, false),
        ] {
            assert_eq!(wall_kernel_reads(source, &micros), into_micros, "{source}");
            assert_eq!(wall_kernel_reads(source, &nanos), into_nanos, "{source}");
        }
    }

    #[test]
    fn only_a_naive_nanosecond_leaf_makes_a_nested_wall_target() {
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let wrap = |leaf: DataType| DataType::Struct(vec![Field::new("v", leaf, true)].into());
        let list = |item: DataType| DataType::List(Arc::new(Field::new("element", item, true)));
        assert!(holds_nested_ns_wall(&wrap(nanos.clone())));
        assert!(holds_nested_ns_wall(&list(wrap(nanos.clone()))));
        assert!(holds_nested_ns_wall(&wrap(list(nanos.clone()))));
        assert!(!holds_nested_ns_wall(&nanos));
        assert!(!holds_nested_ns_wall(&wrap(DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        ))));
        assert!(!holds_nested_ns_wall(&wrap(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(Arc::from("UTC"))
        ))));
    }
}
