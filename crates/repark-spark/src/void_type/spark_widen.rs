use datafusion::arrow::datatypes::{DataType, TimeUnit};
use repark_iceberg::write::update_cast::incompatible_store_message;

pub(crate) const FLOAT_TO_STRING_WRAPPER: &str = "__repark_float_to_string__";

pub(crate) enum BranchShape {
    All,
    First,
    AfterFirst,
}

#[must_use]
pub(crate) fn branch_shape(name: &str, operands: usize) -> Option<BranchShape> {
    if name.eq_ignore_ascii_case("coalesce")
        || name.eq_ignore_ascii_case("greatest")
        || name.eq_ignore_ascii_case("least")
    {
        (operands >= 1).then_some(BranchShape::All)
    } else if name.eq_ignore_ascii_case("nvl") || name.eq_ignore_ascii_case("ifnull") {
        (operands == 2).then_some(BranchShape::All)
    } else if name.eq_ignore_ascii_case("nullif") {
        (operands == 2).then_some(BranchShape::First)
    } else if name.eq_ignore_ascii_case("if") || name.eq_ignore_ascii_case("nvl2") {
        (operands == 3).then_some(BranchShape::AfterFirst)
    } else {
        None
    }
}

#[must_use]
pub(crate) fn widened_stores(widened: &DataType, target: &DataType) -> bool {
    if !matches!(
        widened,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    ) {
        return false;
    }
    if matches!(target, DataType::Timestamp(TimeUnit::Microsecond, Some(_))) && widened.is_numeric()
    {
        return false;
    }
    incompatible_store_message("", "", widened, target).is_none()
}

#[must_use]
pub(crate) fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[must_use]
pub(crate) fn widen_operand_types(types: &[DataType]) -> Option<DataType> {
    let mut saw_string = false;
    let mut widest_numeric: Option<&DataType> = None;
    let mut widest_rank = -1;
    let mut single_other: Option<&DataType> = None;
    for data_type in types {
        if matches!(data_type, DataType::Null) {
            continue;
        }
        if is_string_type(data_type) {
            saw_string = true;
            continue;
        }
        if let Some(rank) = numeric_rank(data_type) {
            if rank > widest_rank {
                widest_numeric = Some(data_type);
                widest_rank = rank;
            }
            continue;
        }
        match single_other {
            None => single_other = Some(data_type),
            Some(current) if current == data_type => {}
            Some(_) => return None,
        }
    }
    match (widest_numeric, single_other) {
        (Some(widest), None) => Some(widest.clone()),
        (None, Some(single)) => {
            if saw_string && !string_castable(single) {
                None
            } else {
                Some(single.clone())
            }
        }
        (None, None) => Some(if saw_string {
            DataType::Utf8
        } else {
            DataType::Null
        }),
        (Some(_), Some(_)) => None,
    }
}

fn numeric_rank(data_type: &DataType) -> Option<i8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Some(4),
        DataType::Float32 => Some(5),
        DataType::Float64 => Some(6),
        _ => None,
    }
}

fn string_castable(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_widen_to_string_and_nulls_to_null() {
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Null]),
            Some(DataType::Utf8)
        );
        assert_eq!(
            widen_operand_types(&[DataType::LargeUtf8, DataType::Utf8View]),
            Some(DataType::Utf8)
        );
        assert_eq!(widen_operand_types(&[DataType::Null]), Some(DataType::Null));
        assert_eq!(widen_operand_types(&[]), Some(DataType::Null));
    }

    #[test]
    fn mixed_string_and_numeric_widens_to_the_widest_numeric() {
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Int32]),
            Some(DataType::Int32)
        );
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Int32, DataType::Float64]),
            Some(DataType::Float64)
        );
        assert_eq!(
            widen_operand_types(&[DataType::Int32, DataType::Int64]),
            Some(DataType::Int64)
        );
        assert_eq!(
            widen_operand_types(&[DataType::Decimal128(10, 2), DataType::Int64]),
            Some(DataType::Decimal128(10, 2))
        );
    }

    #[test]
    fn mixed_string_and_date_widens_to_date_but_not_to_boolean() {
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Date32]),
            Some(DataType::Date32)
        );
        assert_eq!(
            widen_operand_types(&[
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Microsecond, None)
            ]),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Boolean]),
            None
        );
        assert_eq!(
            widen_operand_types(&[DataType::Utf8, DataType::Binary]),
            None
        );
    }

    #[test]
    fn agreeing_non_string_operands_keep_their_type() {
        assert_eq!(
            widen_operand_types(&[DataType::Date32, DataType::Date32, DataType::Null]),
            Some(DataType::Date32)
        );
        assert_eq!(
            widen_operand_types(&[DataType::Boolean, DataType::Null]),
            Some(DataType::Boolean)
        );
    }

    #[test]
    fn disagreeing_non_string_operands_have_no_widened_type() {
        assert_eq!(
            widen_operand_types(&[DataType::Date32, DataType::Int32]),
            None
        );
        assert_eq!(
            widen_operand_types(&[
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            ]),
            None
        );
    }

    #[test]
    fn widened_numeric_stores_into_floats_but_not_into_ltz() {
        assert!(widened_stores(&DataType::Int32, &DataType::Float64));
        assert!(widened_stores(
            &DataType::Float64,
            &DataType::Decimal128(10, 2)
        ));
        assert!(widened_stores(&DataType::Null, &DataType::Float64));
        assert!(!widened_stores(&DataType::Utf8, &DataType::Float64));
        assert!(!widened_stores(&DataType::Int32, &DataType::Boolean));
        assert!(!widened_stores(
            &DataType::Float64,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
    }

    #[test]
    fn branch_shapes_follow_arity() {
        assert!(matches!(
            branch_shape("coalesce", 3),
            Some(BranchShape::All)
        ));
        assert!(matches!(branch_shape("nvl", 2), Some(BranchShape::All)));
        assert!(matches!(
            branch_shape("nullif", 2),
            Some(BranchShape::First)
        ));
        assert!(matches!(
            branch_shape("if", 3),
            Some(BranchShape::AfterFirst)
        ));
        assert!(matches!(
            branch_shape("NVL2", 3),
            Some(BranchShape::AfterFirst)
        ));
        assert!(branch_shape("nvl", 3).is_none());
        assert!(branch_shape("concat", 2).is_none());
    }
}
