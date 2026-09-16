use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

fn decimal_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    }
}

fn integer_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some(0),
        DataType::Int16 | DataType::UInt16 => Some(1),
        DataType::Int32 | DataType::UInt32 => Some(2),
        DataType::Int64 | DataType::UInt64 => Some(3),
        _ => None,
    }
}

fn for_type_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 | DataType::UInt64 => Some((20, 0)),
        _ => None,
    }
}

fn wider_decimal(first: (u8, i8), second: (u8, i8)) -> (u8, i8) {
    let scale = first.1.max(second.1);
    let range =
        (i32::from(first.0) - i32::from(first.1)).max(i32::from(second.0) - i32::from(second.1));
    let mut precision = range + i32::from(scale);
    let mut scale = i32::from(scale);
    if precision > 38 {
        scale -= precision - 38;
        precision = 38;
        scale = scale.max(0);
    }
    (
        u8::try_from(precision).unwrap_or(38),
        i8::try_from(scale).unwrap_or(38),
    )
}

fn decimal_wider_than(decimal: (u8, i8), fractional: &DataType) -> bool {
    let range = i32::from(decimal.0) - i32::from(decimal.1);
    match fractional {
        DataType::Float64 => range >= 30 && decimal.1 <= 15,
        _ => range >= 14 && decimal.1 <= 7,
    }
}

fn list_element(data_type: &DataType) -> Option<&Arc<Field>> {
    match data_type {
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            Some(child)
        }
        _ => None,
    }
}

fn is_spark_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn wider_pair(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if is_spark_string(left) && is_spark_string(right) {
        return Some(DataType::Utf8);
    }
    if *left == DataType::Null {
        return Some(right.clone());
    }
    if *right == DataType::Null {
        return Some(left.clone());
    }
    if let (Some(first), Some(second)) = (decimal_parts(left), decimal_parts(right)) {
        let (precision, scale) = wider_decimal(first, second);
        return Some(DataType::Decimal128(precision, scale));
    }
    if let (Some(first), Some(second)) = (for_type_decimal(left), decimal_parts(right)) {
        let (precision, scale) = wider_decimal(first, second);
        return Some(DataType::Decimal128(precision, scale));
    }
    if let (Some(first), Some(second)) = (decimal_parts(left), for_type_decimal(right)) {
        let (precision, scale) = wider_decimal(first, second);
        return Some(DataType::Decimal128(precision, scale));
    }
    let fractional_decimal = match (decimal_parts(left), decimal_parts(right)) {
        (Some(decimal), None)
            if matches!(
                right,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) =>
        {
            Some((decimal, right))
        }
        (None, Some(decimal))
            if matches!(
                left,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) =>
        {
            Some((decimal, left))
        }
        _ => None,
    };
    if let Some((decimal, fractional)) = fractional_decimal {
        return Some(if decimal_wider_than(decimal, fractional) {
            DataType::Decimal128(decimal.0, decimal.1)
        } else {
            fractional.clone()
        });
    }
    if *left == DataType::Float64 || *right == DataType::Float64 {
        return Some(DataType::Float64);
    }
    if *left == DataType::Float32 || *right == DataType::Float32 {
        let other = if *left == DataType::Float32 {
            right
        } else {
            left
        };
        if integer_rank(other).is_some() || *other == DataType::Float16 {
            return Some(DataType::Float32);
        }
    }
    if *left == DataType::Float16 || *right == DataType::Float16 {
        let other = if *left == DataType::Float16 {
            right
        } else {
            left
        };
        if integer_rank(other).is_some() {
            return Some(DataType::Float16);
        }
    }
    if let (Some(left_rank), Some(right_rank)) = (integer_rank(left), integer_rank(right)) {
        if left_rank >= right_rank {
            return Some(left.clone());
        }
        return Some(right.clone());
    }
    if let (Some(left_child), Some(right_child)) = (list_element(left), list_element(right)) {
        let element = wider_pair(left_child.data_type(), right_child.data_type())?;
        return Some(DataType::List(Arc::new(Field::new(
            "element",
            element,
            left_child.is_nullable() || right_child.is_nullable(),
        ))));
    }
    None
}

pub(crate) fn spark_common_element(types: &[DataType]) -> Option<DataType> {
    let mut common: Option<DataType> = None;
    for data_type in types {
        if *data_type == DataType::Null {
            continue;
        }
        common = Some(match common {
            None => data_type.clone(),
            Some(existing) => wider_pair(&existing, data_type)?,
        });
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_element_follows_spark_widening() {
        let cases: &[(&[DataType], Option<DataType>)] = &[
            (&[DataType::Int32, DataType::Int32], Some(DataType::Int32)),
            (&[DataType::Int32, DataType::Int64], Some(DataType::Int64)),
            (&[DataType::Int8, DataType::Int16], Some(DataType::Int16)),
            (
                &[DataType::Int32, DataType::Decimal128(2, 1)],
                Some(DataType::Decimal128(11, 1)),
            ),
            (
                &[DataType::Decimal128(2, 1), DataType::Decimal128(3, 2)],
                Some(DataType::Decimal128(3, 2)),
            ),
            (
                &[DataType::Decimal128(38, 10), DataType::Decimal128(38, 10)],
                Some(DataType::Decimal128(38, 10)),
            ),
            (&[DataType::Int32, DataType::Null], Some(DataType::Int32)),
            (&[DataType::Null], None),
            (&[], None),
            (&[DataType::Utf8, DataType::Int32], None),
            (
                &[DataType::Float64, DataType::Decimal128(10, 2)],
                Some(DataType::Float64),
            ),
            (
                &[DataType::Int32, DataType::Float32],
                Some(DataType::Float32),
            ),
            (&[DataType::Utf8, DataType::Utf8View], Some(DataType::Utf8)),
            (
                &[DataType::Utf8View, DataType::LargeUtf8],
                Some(DataType::Utf8),
            ),
        ];
        for (inputs, expected) in cases {
            assert_eq!(&spark_common_element(inputs), expected, "{inputs:?}");
        }
    }
}
