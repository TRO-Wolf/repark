use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use super::update_cast::incompatible_update_message;

pub use repark_common::udf_names::{
    TIMESTAMP_NS_CAST_NAME as NS_WALL_CAST_UDF_NAME,
    TIMESTAMP_NTZ_CAST_NAME as NTZ_WALL_CAST_UDF_NAME,
    TIMESTAMPTZ_NS_CAST_NAME as NS_INSTANT_CAST_UDF_NAME,
};

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
pub fn store_kernel_udf_name(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(NS_INSTANT_CAST_UDF_NAME),
        wall => wall_cast_udf_name(wall),
    }
}

#[must_use]
pub fn store_kernel_sql(expr_sql: &str, target: &DataType) -> Option<String> {
    store_kernel_udf_name(target).map(|name| format!("{name}(({expr_sql}))"))
}

#[must_use]
pub fn wall_kernel_reads(source: &DataType, target: &DataType) -> bool {
    match (target, source) {
        (DataType::Timestamp(TimeUnit::Nanosecond, _), DataType::Dictionary(_, values)) => {
            wall_kernel_reads(values, target)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), DataType::RunEndEncoded(_, values)) => {
            wall_kernel_reads(values.data_type(), target)
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), _) => {
            source != target
                && matches!(
                    source,
                    DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
                )
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ) => true,
        _ => matches!(source, DataType::Timestamp(_, Some(_))),
    }
}

#[must_use]
pub fn wall_kernel_input(source: &DataType) -> Option<&DataType> {
    match source {
        DataType::Dictionary(_, values) => Some(values.as_ref()),
        DataType::RunEndEncoded(_, values) => Some(values.data_type()),
        _ => None,
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
        DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Date32
            | DataType::Date64
    )
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

    use super::{
        NS_INSTANT_CAST_UDF_NAME, NS_WALL_CAST_UDF_NAME, NTZ_WALL_CAST_UDF_NAME,
        needs_ltz_instant_cast, store_kernel_sql, store_kernel_udf_name, wall_cast_udf_name,
        wall_kernel_input, wall_kernel_reads,
    };

    #[test]
    fn each_nanosecond_or_wall_target_names_one_store_kernel() {
        let zoned_nanos = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let zoned_micros = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        assert_eq!(
            store_kernel_udf_name(&zoned_nanos),
            Some(NS_INSTANT_CAST_UDF_NAME)
        );
        assert_eq!(store_kernel_udf_name(&nanos), Some(NS_WALL_CAST_UDF_NAME));
        assert_eq!(store_kernel_udf_name(&micros), Some(NTZ_WALL_CAST_UDF_NAME));
        assert_eq!(store_kernel_udf_name(&zoned_micros), None);
        assert_eq!(store_kernel_udf_name(&DataType::Int64), None);
        assert_eq!(wall_cast_udf_name(&zoned_nanos), None);
        assert_eq!(
            store_kernel_sql("s.v", &zoned_nanos).as_deref(),
            Some("__repark_cast_timestamptz_ns__((s.v))")
        );
    }

    #[test]
    fn a_zoned_nanosecond_target_hands_every_other_temporal_source_to_its_kernel() {
        let target = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
        let runs = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Date32, true)),
        );
        for (source, read) in [
            (DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            (DataType::Timestamp(TimeUnit::Microsecond, None), true),
            (DataType::Date32, true),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
            (runs, true),
            (target.clone(), false),
            (DataType::Utf8, false),
            (DataType::Int64, false),
        ] {
            assert_eq!(wall_kernel_reads(&source, &target), read, "{source}");
        }
    }

    #[test]
    fn the_kernel_reads_what_its_target_cannot_take_by_a_plain_cast() {
        let instant = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let runs = |values: &DataType| {
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", values.clone(), true)),
            )
        };
        let keyed = |values: &DataType| {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(values.clone()))
        };
        for (source, into_micros, into_nanos) in [
            (instant.clone(), true, true),
            (micros.clone(), false, true),
            (DataType::Date32, false, true),
            (nanos.clone(), true, false),
            (DataType::Utf8, false, false),
            (DataType::Int64, false, false),
            (runs(&instant), false, true),
            (keyed(&instant), false, true),
            (runs(&micros), false, true),
            (runs(&nanos), false, false),
            (runs(&DataType::Int64), false, false),
        ] {
            assert_eq!(wall_kernel_reads(&source, &micros), into_micros, "{source}");
            assert_eq!(wall_kernel_reads(&source, &nanos), into_nanos, "{source}");
        }
        for (source, cast) in [
            (instant.clone(), false),
            (micros.clone(), true),
            (nanos.clone(), true),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                true,
            ),
            (DataType::Date32, true),
            (DataType::Utf8, false),
        ] {
            assert_eq!(needs_ltz_instant_cast(&source), cast, "{source}");
        }
        assert_eq!(wall_kernel_input(&runs(&instant)), Some(&instant));
        assert_eq!(wall_kernel_input(&keyed(&micros)), Some(&micros));
        assert_eq!(wall_kernel_input(&instant), None);
    }
}
