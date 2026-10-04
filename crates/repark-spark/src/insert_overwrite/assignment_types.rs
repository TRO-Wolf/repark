use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

pub(crate) fn cast_may_fail_at_runtime(from: &DataType, to: &DataType) -> bool {
    if assignment_types_compatible(from, to) {
        return false;
    }
    if matches!(from, DataType::Null) {
        return false;
    }
    !(utf8_family(to) && renders_as_text_infallibly(from))
}

pub(crate) fn renders_as_text_infallibly(data_type: &DataType) -> bool {
    use DataType::{
        Boolean, Date32, Date64, Decimal128, Decimal256, Float16, Float32, Float64, Int8, Int16,
        Int32, Int64, Time32, Time64, Timestamp, UInt8, UInt16, UInt32, UInt64,
    };
    matches!(
        data_type,
        Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(_, _)
    )
}

pub(crate) fn field_type_case_insensitive(
    schema: &datafusion::common::DFSchema,
    name: &str,
) -> Result<DataType> {
    let mut found: Option<DataType> = None;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "INSERT OVERWRITE column `{name}` is ambiguous under case-insensitive matching"
                )));
            }
            found = Some(field.data_type().clone());
        }
    }
    found.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "INSERT OVERWRITE empty source column `{name}` does not exist in the target table"
        ))
    })
}

pub(crate) fn assignment_types_compatible(source: &DataType, target: &DataType) -> bool {
    use DataType::{Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64};
    if source == target {
        return true;
    }
    if utf8_family(source) && utf8_family(target) {
        return true;
    }
    matches!(
        (source, target),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64)
            | (UInt16, UInt32 | UInt64 | Int32 | Int64)
            | (UInt32, UInt64 | Int64)
            | (Float32, Float64)
    )
}

pub(crate) fn utf8_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[cfg(test)]
mod assignment_type_unit_tests {
    use super::{assignment_types_compatible, utf8_family};
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn assignment_types_compatible_matrix() {
        assert!(assignment_types_compatible(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(assignment_types_compatible(
            &DataType::Int32,
            &DataType::Int64
        ));
        assert!(assignment_types_compatible(
            &DataType::Utf8,
            &DataType::LargeUtf8
        ));
        assert!(utf8_family(&DataType::Utf8View));
        assert!(!assignment_types_compatible(
            &DataType::Utf8,
            &DataType::Int32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Utf8,
            &DataType::Date32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Int64,
            &DataType::Int32
        ));
        assert!(!assignment_types_compatible(
            &DataType::Float64,
            &DataType::Float32
        ));
    }

    #[test]
    fn cast_may_fail_at_runtime_matrix() {
        use super::cast_may_fail_at_runtime;
        use std::sync::Arc;

        assert!(!cast_may_fail_at_runtime(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(!cast_may_fail_at_runtime(
            &DataType::Utf8,
            &DataType::Utf8View
        ));
        assert!(!cast_may_fail_at_runtime(
            &DataType::Int32,
            &DataType::Int64
        ));
        for from in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Decimal256(40, 2),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(datafusion::arrow::datatypes::TimeUnit::Second),
            DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
        ] {
            assert!(
                !cast_may_fail_at_runtime(&from, &DataType::Utf8),
                "{from} → Utf8 is total and must not block a wipe"
            );
        }
        for to in [
            DataType::Utf8,
            DataType::Int32,
            DataType::Date32,
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Null,
        ] {
            assert!(
                !cast_may_fail_at_runtime(&DataType::Null, &to),
                "Null → {to} is total and must not block a wipe"
            );
        }
        assert!(cast_may_fail_at_runtime(&DataType::Utf8, &DataType::Int32));
        assert!(cast_may_fail_at_runtime(&DataType::Utf8, &DataType::Date32));
        assert!(cast_may_fail_at_runtime(&DataType::Int64, &DataType::Int32));
        assert!(cast_may_fail_at_runtime(
            &DataType::Float64,
            &DataType::Float32
        ));
        assert!(cast_may_fail_at_runtime(
            &DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            &DataType::Utf8
        ));
    }
}
