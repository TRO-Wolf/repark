use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::DataFusionError;

#[must_use]
pub(crate) fn widen_full(left: &DataType, right: &DataType) -> Option<DataType> {
    widen_inner(left, right, true)
}

fn unwrap_transparent(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, values) => unwrap_transparent(values),
        DataType::RunEndEncoded(_, values) => unwrap_transparent(values.data_type()),
        other => other,
    }
}

fn widen_inner(left: &DataType, right: &DataType, strings: bool) -> Option<DataType> {
    let left = unwrap_transparent(left);
    let right = unwrap_transparent(right);
    if is_unrepresentable(left) || is_unrepresentable(right) {
        return None;
    }
    if left == right {
        return Some(left.clone());
    }
    if *left == DataType::Null {
        return Some(right.clone());
    }
    if *right == DataType::Null {
        return Some(left.clone());
    }
    scalar_widen(left, right, strings)
        .or_else(|| temporal_widen(left, right))
        .or_else(|| complex_widen(left, right, strings))
}

fn integral_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(1),
        DataType::Int16 => Some(2),
        DataType::Int32 => Some(3),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(4),
        _ => None,
    }
}

fn rank_to_integral(rank: u8) -> DataType {
    match rank {
        1 => DataType::Int8,
        2 => DataType::Int16,
        3 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn is_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float16 | DataType::Float32)
}

fn as_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn integral_to_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Int8 => Some((3, 0)),
        DataType::Int16 => Some((5, 0)),
        DataType::Int32 => Some((10, 0)),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some((20, 0)),
        _ => None,
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn timestamp_parts(data_type: &DataType) -> Option<(TimeUnit, Option<Arc<str>>)> {
    match data_type {
        DataType::Timestamp(unit, zone) => Some((*unit, zone.clone())),
        _ => None,
    }
}

fn is_time(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

fn is_unrepresentable(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Duration(_) | DataType::Union(_, _))
}

fn scalar_widen(left: &DataType, right: &DataType, strings: bool) -> Option<DataType> {
    if let (Some(left_rank), Some(right_rank)) = (integral_rank(left), integral_rank(right)) {
        return Some(rank_to_integral(left_rank.max(right_rank)));
    }
    if is_float(left) && is_float(right) {
        return Some(DataType::Float32);
    }
    let widen_to_double = |own: &DataType, other: &DataType| {
        matches!(own, DataType::Float64)
            && (is_float(other)
                || integral_rank(other).is_some()
                || as_decimal(other).is_some()
                || matches!(other, DataType::Float64))
    };
    if widen_to_double(left, right) || widen_to_double(right, left) {
        return Some(DataType::Float64);
    }
    if (is_float(left)
        && (integral_rank(right).is_some()
            || as_decimal(right).is_some()
            || matches!(right, DataType::Float64)))
        || (is_float(right)
            && (integral_rank(left).is_some()
                || as_decimal(left).is_some()
                || matches!(left, DataType::Float64)))
    {
        return Some(DataType::Float64);
    }
    if let (Some((p1, s1)), Some((p2, s2))) = (
        as_decimal(left).or_else(|| integral_to_decimal(left)),
        as_decimal(right).or_else(|| integral_to_decimal(right)),
    ) {
        if as_decimal(left).is_some() || as_decimal(right).is_some() {
            let (precision, scale) = decimal_wider(p1, s1, p2, s2)?;
            return Some(DataType::Decimal128(precision, scale));
        }
    }
    if is_text(left) && is_text(right) {
        return Some(DataType::Utf8);
    }
    if is_text(left) || is_text(right) {
        if !strings {
            return None;
        }
        let other = if is_text(left) { right } else { left };
        if integral_rank(other).is_some() {
            return Some(DataType::Int64);
        }
        if is_float(other) || matches!(other, DataType::Float64) || as_decimal(other).is_some() {
            return Some(DataType::Float64);
        }
        if is_date(other) {
            return Some(DataType::Date32);
        }
        if let Some((_, zone)) = timestamp_parts(other) {
            return Some(DataType::Timestamp(TimeUnit::Microsecond, zone));
        }
        if matches!(other, DataType::Boolean) {
            return Some(DataType::Boolean);
        }
        if is_binary(other) {
            return Some(DataType::Binary);
        }
        return None;
    }
    if is_binary(left) && is_binary(right) {
        return Some(DataType::Binary);
    }
    if is_time(left) && is_time(right) {
        return Some(left.clone());
    }
    None
}

pub(crate) fn decimal_wider(p1: u8, s1: i8, p2: u8, s2: i8) -> Option<(u8, i8)> {
    let scale = i32::from(s1).max(i32::from(s2));
    let range = (i32::from(p1) - i32::from(s1)).max(i32::from(p2) - i32::from(s2));
    if range > 38 {
        return None;
    }
    let capped_scale = scale.min(38 - range);
    let precision = range + capped_scale;
    if precision < 1 || precision > 38 {
        return None;
    }
    let precision = u8::try_from(precision).ok()?;
    let capped_scale = i8::try_from(capped_scale).ok()?;
    Some((precision, capped_scale))
}

fn finer_unit(first: TimeUnit, second: TimeUnit) -> TimeUnit {
    let rank = |unit: TimeUnit| match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    };
    if rank(first) >= rank(second) {
        first
    } else {
        second
    }
}

fn temporal_widen(left: &DataType, right: &DataType) -> Option<DataType> {
    if is_date(left) && is_date(right) {
        return Some(DataType::Date32);
    }
    match (timestamp_parts(left), timestamp_parts(right)) {
        (Some((_, zone)), None) if is_date(right) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, zone))
        }
        (None, Some((_, zone))) if is_date(left) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, zone))
        }
        (Some((left_unit, left_zone)), Some((right_unit, right_zone))) => {
            match (left_zone, right_zone) {
                (Some(zone), Some(_)) | (Some(zone), None) | (None, Some(zone)) => Some(
                    DataType::Timestamp(finer_unit(left_unit, right_unit), Some(zone)),
                ),
                (None, None) => Some(DataType::Timestamp(finer_unit(left_unit, right_unit), None)),
            }
        }
        _ => None,
    }
}

fn list_element(data_type: &DataType) -> Option<Arc<Field>> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(Arc::clone(field)),
        _ => None,
    }
}

fn complex_widen(left: &DataType, right: &DataType, strings: bool) -> Option<DataType> {
    if let (Some(left_field), Some(right_field)) = (list_element(left), list_element(right)) {
        let element = widen_inner(left_field.data_type(), right_field.data_type(), strings)?;
        return Some(DataType::List(Arc::new(Field::new(
            left_field.name(),
            element,
            left_field.is_nullable() || right_field.is_nullable(),
        ))));
    }
    if let (DataType::Struct(left_fields), DataType::Struct(right_fields)) = (left, right) {
        if left_fields.len() != right_fields.len() {
            return None;
        }
        let mut fields = Vec::with_capacity(left_fields.len());
        for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
            if left_field.name() != right_field.name() {
                return None;
            }
            let widened = widen_inner(left_field.data_type(), right_field.data_type(), strings)?;
            fields.push(Arc::new(Field::new(
                left_field.name(),
                widened,
                left_field.is_nullable() || right_field.is_nullable(),
            )));
        }
        return Some(DataType::Struct(fields.into()));
    }
    map_widen(left, right, strings)
}

fn map_entries(data_type: &DataType) -> Option<(Arc<Field>, Arc<Field>, bool)> {
    match data_type {
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => {
                Some((Arc::clone(&pair[0]), Arc::clone(&pair[1]), *sorted))
            }
            _ => None,
        },
        _ => None,
    }
}

fn map_widen(left: &DataType, right: &DataType, strings: bool) -> Option<DataType> {
    let (left_key, left_value, left_sorted) = map_entries(left)?;
    let (right_key, right_value, right_sorted) = map_entries(right)?;
    let key = widen_inner(left_key.data_type(), right_key.data_type(), false)?;
    let value = widen_inner(left_value.data_type(), right_value.data_type(), strings)?;
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new(
                    left_key.name(),
                    key,
                    left_key.is_nullable() || right_key.is_nullable(),
                )),
                Arc::new(Field::new(
                    left_value.name(),
                    value,
                    left_value.is_nullable() || right_value.is_nullable(),
                )),
            ]
            .into(),
        ),
        false,
    ));
    Some(DataType::Map(entries, left_sorted && right_sorted))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompareRefusal {
    BinaryOp,
    Ordering(DataType),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompareLeaf {
    pub(crate) path_a: Vec<String>,
    pub(crate) path_b: Vec<String>,
    pub(crate) type_a: DataType,
    pub(crate) type_b: DataType,
    pub(crate) common: DataType,
}

pub(crate) fn contains_map(data_type: &DataType) -> bool {
    let data_type = unwrap_transparent(data_type);
    match data_type {
        DataType::Map(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_map(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_map(field.data_type())),
        _ => false,
    }
}

pub(crate) fn compare_for_nullif(
    left: &DataType,
    right: &DataType,
) -> Result<Vec<CompareLeaf>, CompareRefusal> {
    compare_inner(left, right, true, Vec::new(), Vec::new())
}

fn compare_inner(
    left: &DataType,
    right: &DataType,
    strings: bool,
    path_a: Vec<String>,
    path_b: Vec<String>,
) -> Result<Vec<CompareLeaf>, CompareRefusal> {
    let left = unwrap_transparent(left);
    let right = unwrap_transparent(right);
    if *left == DataType::Null && *right == DataType::Null {
        return Ok(Vec::new());
    }
    if *left == DataType::Null || *right == DataType::Null {
        let common = if *left == DataType::Null {
            right.clone()
        } else {
            left.clone()
        };
        return Ok(vec![CompareLeaf {
            path_a,
            path_b,
            type_a: left.clone(),
            type_b: right.clone(),
            common,
        }]);
    }
    if matches!(left, DataType::Map(_, _)) || matches!(right, DataType::Map(_, _)) {
        let Some(common) = map_common_for_compare(left, right) else {
            return Err(CompareRefusal::BinaryOp);
        };
        if !matches!(left, DataType::Map(_, _)) || !matches!(right, DataType::Map(_, _)) {
            return Err(CompareRefusal::BinaryOp);
        }
        return Err(CompareRefusal::Ordering(common));
    }
    if let (Some(left_field), Some(right_field)) = (list_element(left), list_element(right)) {
        let Some(element) = widen_inner(left_field.data_type(), right_field.data_type(), false)
        else {
            return Err(CompareRefusal::BinaryOp);
        };
        if contains_map(&element) {
            let common = DataType::List(Arc::new(Field::new(
                left_field.name(),
                element,
                left_field.is_nullable() || right_field.is_nullable(),
            )));
            return Err(CompareRefusal::Ordering(common));
        }
        let common = widen_inner(left, right, false).ok_or(CompareRefusal::BinaryOp)?;
        return Ok(vec![CompareLeaf {
            path_a,
            path_b,
            type_a: left.clone(),
            type_b: right.clone(),
            common,
        }]);
    }
    if matches!(left, DataType::Struct(_)) || matches!(right, DataType::Struct(_)) {
        return compare_structs(left, right, path_a, path_b);
    }
    if list_element(left).is_some()
        || list_element(right).is_some()
        || matches!(left, DataType::Map(_, _))
        || matches!(right, DataType::Map(_, _))
    {
        return Err(CompareRefusal::BinaryOp);
    }
    let Some(common) = widen_inner(left, right, strings) else {
        return Err(CompareRefusal::BinaryOp);
    };
    Ok(vec![CompareLeaf {
        path_a,
        path_b,
        type_a: left.clone(),
        type_b: right.clone(),
        common,
    }])
}

fn map_common_for_compare(left: &DataType, right: &DataType) -> Option<DataType> {
    let (left_key, left_value, left_sorted) = map_entries(left)?;
    let (right_key, right_value, right_sorted) = map_entries(right)?;
    let key = widen_inner(left_key.data_type(), right_key.data_type(), false)?;
    let value = widen_inner(left_value.data_type(), right_value.data_type(), true)?;
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new(
                    left_key.name(),
                    key,
                    left_key.is_nullable() || right_key.is_nullable(),
                )),
                Arc::new(Field::new(
                    left_value.name(),
                    value,
                    left_value.is_nullable() || right_value.is_nullable(),
                )),
            ]
            .into(),
        ),
        false,
    ));
    Some(DataType::Map(entries, left_sorted && right_sorted))
}

fn compare_structs(
    left: &DataType,
    right: &DataType,
    path_a: Vec<String>,
    path_b: Vec<String>,
) -> Result<Vec<CompareLeaf>, CompareRefusal> {
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) = (left, right) else {
        return Err(CompareRefusal::BinaryOp);
    };
    if left_fields.len() != right_fields.len() {
        return Err(CompareRefusal::BinaryOp);
    }
    let mut leaves = Vec::new();
    for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
        let mut child_a = path_a.clone();
        child_a.push(left_field.name().clone());
        let mut child_b = path_b.clone();
        child_b.push(right_field.name().clone());
        let mut child = compare_inner(
            left_field.data_type(),
            right_field.data_type(),
            false,
            child_a,
            child_b,
        )?;
        leaves.append(&mut child);
    }
    Ok(leaves)
}

#[must_use]
pub(crate) fn spark_nvl_type_name(data_type: &DataType) -> String {
    match unwrap_transparent(data_type) {
        DataType::Null => "VOID".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_owned(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_owned(),
        DataType::Int32 | DataType::UInt32 => "INT".to_owned(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_owned(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_owned(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_owned(),
        DataType::Date32 | DataType::Date64 => "DATE".to_owned(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_owned(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_owned(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_owned(),
        DataType::Interval(_) | DataType::Duration(_) => "INTERVAL".to_owned(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_nvl_type_name(field.data_type()))
        }
        DataType::Struct(fields) => {
            let inner = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}",
                        field.name(),
                        spark_nvl_type_name(field.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{inner}>")
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(pair) if pair.len() == 2 => format!(
                "MAP<{}, {}>",
                spark_nvl_type_name(pair[0].data_type()),
                spark_nvl_type_name(pair[1].data_type())
            ),
            _ => "MAP".to_owned(),
        },
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) | DataType::Union(_, _) => {
            data_type.to_string()
        }
    }
}

pub(crate) fn wrong_num_args(spelling: &str, expected: usize, got: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{spelling}` requires {expected} parameters but \
         the actual number is {got}. Please, refer to \
         'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: 42605"
    ))
}

pub(crate) fn coalesce_data_diff_types(first: &DataType, second: &DataType) -> DataFusionError {
    let first = spark_nvl_type_name(first);
    let second = spark_nvl_type_name(second);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"coalesce(...)\" due to data type \
         mismatch: Input to `coalesce` should all be the same type, but it's (\"{first}\" or \
         \"{second}\"). SQLSTATE: 42K09"
    ))
}

pub(crate) fn if_data_diff_types(first: &DataType, second: &DataType) -> DataFusionError {
    let first = spark_nvl_type_name(first);
    let second = spark_nvl_type_name(second);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] Cannot resolve \"if(...)\" due to data type mismatch: \
         Input to `if` should all be the same type, but it's [\"{first}\", \"{second}\"]. SQLSTATE: \
         42K09"
    ))
}

pub(crate) fn binary_op_diff_types(
    left_sql: &str,
    right_sql: &str,
    first: &DataType,
    second: &DataType,
) -> DataFusionError {
    let first = spark_nvl_type_name(first);
    let second = spark_nvl_type_name(second);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve \"({left_sql} = {right_sql})\" due \
         to data type mismatch: the left and right operands of the binary operator have \
         incompatible types (\"{first}\" and \"{second}\"). SQLSTATE: 42K09"
    ))
}

pub(crate) fn invalid_ordering_type(
    left_sql: &str,
    right_sql: &str,
    data_type: &DataType,
) -> DataFusionError {
    let rendered = spark_nvl_type_name(data_type);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve \"({left_sql} = {right_sql})\" \
         due to data type mismatch: The `=` does not support ordering on type \"{rendered}\". \
         SQLSTATE: 42K09"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ltz() -> DataType {
        crate::instant_ts::ltz_timestamp_type()
    }

    fn ntz() -> DataType {
        crate::instant_ts::ntz_timestamp_type()
    }

    fn list_of(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("element", element, true)))
    }

    fn struct_of(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn map_of(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", key, false)),
                        Arc::new(Field::new("value", value, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn widen_matches_spark_pairs() {
        let date = DataType::Date32;
        let cases: Vec<(DataType, DataType, DataType)> = vec![
            (date.clone(), ltz(), ltz()),
            (date.clone(), ntz(), ntz()),
            (ltz(), ntz(), ltz()),
            (
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
            ),
            (DataType::Int32, DataType::Int64, DataType::Int64),
            (DataType::Int32, DataType::Float64, DataType::Float64),
            (
                DataType::Int32,
                DataType::Decimal128(10, 2),
                DataType::Decimal128(12, 2),
            ),
            (
                DataType::Decimal128(5, 2),
                DataType::Decimal128(10, 4),
                DataType::Decimal128(10, 4),
            ),
            (DataType::Utf8, DataType::Int32, DataType::Int64),
            (DataType::Utf8, date.clone(), DataType::Date32),
            (DataType::Null, DataType::Int32, DataType::Int32),
            (DataType::Null, DataType::Null, DataType::Null),
            (
                list_of(DataType::Int32),
                list_of(DataType::Int64),
                list_of(DataType::Int64),
            ),
            (
                struct_of(vec![("a", DataType::Int32), ("b", DataType::Utf8)]),
                struct_of(vec![("a", DataType::Int64), ("b", DataType::Utf8)]),
                struct_of(vec![("a", DataType::Int64), ("b", DataType::Utf8)]),
            ),
            (DataType::Int8, DataType::Int16, DataType::Int16),
            (DataType::Float32, DataType::Int32, DataType::Float64),
            (DataType::Float32, DataType::Float64, DataType::Float64),
            (DataType::Float32, DataType::Float32, DataType::Float32),
            (
                DataType::Int16,
                DataType::Decimal128(10, 2),
                DataType::Decimal128(10, 2),
            ),
            (
                DataType::Int64,
                DataType::Decimal128(10, 2),
                DataType::Decimal128(22, 2),
            ),
            (
                DataType::Float64,
                DataType::Decimal128(10, 2),
                DataType::Float64,
            ),
            (DataType::Utf8, DataType::Int8, DataType::Int64),
            (DataType::Utf8, DataType::Float32, DataType::Float64),
            (
                DataType::Utf8,
                DataType::Decimal128(10, 2),
                DataType::Float64,
            ),
            (DataType::Utf8, ltz(), ltz()),
            (DataType::Utf8, ntz(), ntz()),
            (DataType::Utf8, DataType::Boolean, DataType::Boolean),
            (DataType::Utf8, DataType::Binary, DataType::Binary),
            (DataType::Binary, DataType::Binary, DataType::Binary),
            (
                map_of(DataType::Utf8, DataType::Int32),
                map_of(DataType::Utf8, DataType::Int64),
                map_of(DataType::Utf8, DataType::Int64),
            ),
            (
                map_of(DataType::Int32, DataType::Utf8),
                map_of(DataType::Int64, DataType::Utf8),
                map_of(DataType::Int64, DataType::Utf8),
            ),
            (
                DataType::Decimal128(38, 8),
                DataType::Decimal128(32, 10),
                DataType::Decimal128(38, 8),
            ),
            (
                DataType::Decimal128(10, 10),
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 0),
            ),
        ];
        for (left, right, expected) in &cases {
            assert_eq!(
                widen_full(left, right).as_ref(),
                Some(expected),
                "widen {left} with {right}"
            );
            assert_eq!(
                widen_full(right, left).as_ref(),
                Some(expected),
                "widen {right} with {left}"
            );
        }
    }

    #[test]
    fn widen_refuses_spark_refusals() {
        let date = DataType::Date32;
        let cases: Vec<(DataType, DataType)> = vec![
            (DataType::Boolean, DataType::Int32),
            (list_of(DataType::Int32), DataType::Int32),
            (struct_of(vec![("a", DataType::Int32)]), DataType::Int32),
            (
                struct_of(vec![("a", DataType::Int32)]),
                struct_of(vec![("b", DataType::Int32)]),
            ),
            (
                struct_of(vec![("a", DataType::Int32)]),
                struct_of(vec![("a", DataType::Int32), ("b", DataType::Int32)]),
            ),
            (
                struct_of(vec![("a", DataType::Int32), ("b", DataType::Int32)]),
                struct_of(vec![("b", DataType::Int32), ("a", DataType::Int32)]),
            ),
            (DataType::Int32, date.clone()),
            (DataType::Float64, date.clone()),
            (DataType::Boolean, date.clone()),
            (DataType::Decimal128(10, 2), date),
            (
                map_of(DataType::Utf8, DataType::Int32),
                map_of(DataType::Int32, DataType::Int32),
            ),
            (DataType::Utf8, list_of(DataType::Int32)),
            (
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
            ),
        ];
        for (left, right) in &cases {
            assert_eq!(widen_full(left, right), None, "widen {left} with {right}");
            assert_eq!(widen_full(right, left), None, "widen {right} with {left}");
        }
    }

    #[test]
    fn tight_widen_bars_strings_inside_complex() {
        assert_eq!(widen_inner(&DataType::Utf8, &DataType::Int32, false), None);
        assert_eq!(
            widen_inner(&DataType::Utf8, &DataType::Utf8, false),
            Some(DataType::Utf8)
        );
        assert_eq!(
            widen_inner(&DataType::Int32, &DataType::Int64, false),
            Some(DataType::Int64)
        );
        assert_eq!(widen_inner(&DataType::Date32, &ltz(), false), Some(ltz()));
        assert_eq!(
            widen_inner(&list_of(DataType::Utf8), &list_of(DataType::Int32), false),
            None
        );
    }

    #[test]
    fn decimal_wider_follows_spark() {
        assert_eq!(decimal_wider(10, 0, 10, 2), Some((12, 2)));
        assert_eq!(decimal_wider(5, 2, 10, 4), Some((10, 4)));
        assert_eq!(decimal_wider(20, 0, 10, 2), Some((22, 2)));
        assert_eq!(decimal_wider(38, 8, 32, 10), Some((38, 8)));
        assert_eq!(decimal_wider(10, 10, 38, 0), Some((38, 0)));
        assert_eq!(decimal_wider(38, 0, 10, 0), Some((38, 0)));
    }

    #[test]
    fn compare_classifies_like_spark() {
        assert!(matches!(
            compare_for_nullif(&list_of(DataType::Int32), &DataType::Int32),
            Err(CompareRefusal::BinaryOp)
        ));
        assert!(matches!(
            compare_for_nullif(&DataType::Boolean, &DataType::Int32),
            Err(CompareRefusal::BinaryOp)
        ));
        assert!(matches!(
            compare_for_nullif(&DataType::Int32, &DataType::Date32),
            Err(CompareRefusal::BinaryOp)
        ));
        let ordering = compare_for_nullif(
            &map_of(DataType::Utf8, DataType::Int32),
            &map_of(DataType::Utf8, DataType::Int64),
        );
        assert!(matches!(ordering, Err(CompareRefusal::Ordering(_))));
        assert!(matches!(
            compare_for_nullif(&map_of(DataType::Utf8, DataType::Int32), &DataType::Int32),
            Err(CompareRefusal::BinaryOp)
        ));
        assert!(matches!(
            compare_for_nullif(&list_of(DataType::Utf8), &list_of(DataType::Int32)),
            Err(CompareRefusal::BinaryOp)
        ));
        let leaves = compare_for_nullif(&DataType::Date32, &ltz()).expect("date with timestamp");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].common, ltz());
        let positional = compare_for_nullif(
            &struct_of(vec![("a", DataType::Int32)]),
            &struct_of(vec![("b", DataType::Int32)]),
        )
        .expect("positional struct compare");
        assert_eq!(positional.len(), 1);
        assert_eq!(positional[0].path_a, vec!["a".to_owned()]);
        assert_eq!(positional[0].path_b, vec!["b".to_owned()]);
        assert!(matches!(
            compare_for_nullif(
                &struct_of(vec![("a", DataType::Int32)]),
                &struct_of(vec![("a", DataType::Int32), ("b", DataType::Int32),]),
            ),
            Err(CompareRefusal::BinaryOp)
        ));
        assert_eq!(
            compare_for_nullif(&DataType::Null, &DataType::Null).expect("nulls"),
            Vec::new()
        );
    }

    #[test]
    fn type_names_match_spark_messages() {
        assert_eq!(spark_nvl_type_name(&DataType::Null), "VOID");
        assert_eq!(spark_nvl_type_name(&ltz()), "TIMESTAMP");
        assert_eq!(spark_nvl_type_name(&ntz()), "TIMESTAMP_NTZ");
        assert_eq!(
            spark_nvl_type_name(&DataType::Decimal128(10, 2)),
            "DECIMAL(10,2)"
        );
        assert_eq!(spark_nvl_type_name(&list_of(DataType::Int32)), "ARRAY<INT>");
        assert_eq!(
            spark_nvl_type_name(&map_of(DataType::Utf8, DataType::Int64)),
            "MAP<STRING, BIGINT>"
        );
        assert_eq!(
            spark_nvl_type_name(&struct_of(vec![("a", DataType::Int32)])),
            "STRUCT<a: INT>"
        );
    }

    #[test]
    fn error_shapes_carry_spark_classes() {
        let message = coalesce_data_diff_types(&DataType::Boolean, &DataType::Int32).to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"),
            "{message}"
        );
        assert!(message.contains("Input to `coalesce`"), "{message}");
        let message = if_data_diff_types(&DataType::Boolean, &DataType::Int32).to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"),
            "{message}"
        );
        assert!(message.contains("Input to `if`"), "{message}");
        let message =
            binary_op_diff_types("a", "b", &DataType::Int32, &DataType::Date32).to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
            "{message}"
        );
        let message =
            invalid_ordering_type("a", "b", &map_of(DataType::Utf8, DataType::Int32)).to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE]"),
            "{message}"
        );
        let message = wrong_num_args("nvl", 2, 3).to_string();
        assert!(
            message.contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]"),
            "{message}"
        );
        assert!(message.contains("`nvl` requires 2"), "{message}");
    }
}
