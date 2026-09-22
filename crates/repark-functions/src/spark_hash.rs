use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Decimal256Array, GenericListArray, LargeBinaryArray,
    LargeStringArray, OffsetSizeTrait, StringArray, StringViewArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Field, FieldRef, Int32Type, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

const HASH_SEED: u32 = 42;
const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

#[must_use]
pub fn hash_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkHash::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![hash_udf()]
}

#[must_use]
pub fn call_hash(args: Vec<Expr>) -> Expr {
    crate::expr_fn::call(hash_udf(), args)
}

#[derive(Debug)]
struct SparkHash {
    signature: Signature,
}

impl SparkHash {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkHash {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkHash {}

impl Hash for SparkHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn mix(hash: u32, value: u32) -> u32 {
    let mixed = value.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    (hash ^ mixed)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64)
}

fn fmix(hash: u32, length: u32) -> u32 {
    let mut out = hash ^ length;
    out ^= out >> 16;
    out = out.wrapping_mul(0x85eb_ca6b);
    out ^= out >> 13;
    out = out.wrapping_mul(0xc2b2_ae35);
    out ^= out >> 16;
    out
}

fn hash_int(value: i32, seed: u32) -> u32 {
    fmix(mix(seed, value.cast_unsigned()), 4)
}

fn hash_long(value: i64, seed: u32) -> u32 {
    let bits = value.cast_unsigned();
    #[allow(clippy::cast_possible_truncation)]
    let (low, high) = (bits as u32, (bits >> 32) as u32);
    fmix(mix(mix(seed, low), high), 8)
}

fn hash_bytes_murmur(data: &[u8], seed: u32) -> u32 {
    let mut hash = seed;
    let full = data.len() / 4 * 4;
    for chunk in data[..full].chunks_exact(4) {
        let word = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        hash = mix(hash, word);
    }
    for byte in &data[full..] {
        hash = mix(hash, u32::from(*byte));
    }
    #[allow(clippy::cast_possible_truncation)]
    fmix(hash, data.len() as u32)
}

fn hash_double(value: f64, seed: u32) -> u32 {
    if value == 0.0 {
        hash_long(0, seed)
    } else if value.is_nan() {
        hash_long(0x7ff8_0000_0000_0000u64.cast_signed(), seed)
    } else {
        hash_long(value.to_bits().cast_signed(), seed)
    }
}

fn hash_float(value: f32, seed: u32) -> u32 {
    hash_double(f64::from(value), seed)
}

fn hash_decimal_compact(unscaled: i64, seed: u32) -> u32 {
    hash_long(unscaled, seed)
}

fn plan_hash(arg_types: &[DataType]) -> Result<()> {
    if arg_types.is_empty() {
        return exec_err!("'hash' expects at least one argument, got 0");
    }
    Ok(())
}

fn hash_text_value(array: &ArrayRef, row: usize, seed: u32) -> Result<u32> {
    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs utf8 values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row).as_bytes(), seed))
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs utf8 values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row).as_bytes(), seed))
        }
        DataType::Utf8View => {
            let values = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs utf8 values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row).as_bytes(), seed))
        }
        DataType::Binary => {
            let values = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs binary values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row), seed))
        }
        DataType::LargeBinary => {
            let values = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs binary values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row), seed))
        }
        _ => {
            let values = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs binary values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(values.value(row), seed))
        }
    }
}

fn hash_time_value(array: &ArrayRef, row: usize, seed: u32) -> u32 {
    match array.data_type() {
        DataType::Date32 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Date32Type>();
            hash_int(values.value(row), seed)
        }
        DataType::Date64 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Date64Type>();
            #[allow(clippy::cast_possible_truncation)]
            let days = values.value(row).div_euclid(86_400_000) as i32;
            hash_int(days, seed)
        }
        _ => {
            use datafusion::arrow::datatypes::{
                TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
            };
            let unit = match array.data_type() {
                DataType::Timestamp(unit, _) => unit,
                _ => &TimeUnit::Microsecond,
            };
            let micros = match unit {
                TimeUnit::Second => {
                    let values = array.as_primitive::<TimestampSecondType>();
                    values.value(row).saturating_mul(1_000_000)
                }
                TimeUnit::Millisecond => {
                    let values = array.as_primitive::<TimestampMillisecondType>();
                    values.value(row).saturating_mul(1_000)
                }
                TimeUnit::Microsecond => {
                    let values = array.as_primitive::<TimestampMicrosecondType>();
                    values.value(row)
                }
                TimeUnit::Nanosecond => {
                    let values = array.as_primitive::<TimestampNanosecondType>();
                    values.value(row).div_euclid(1_000)
                }
            };
            hash_long(micros, seed)
        }
    }
}

fn hash_array_value(array: &ArrayRef, row: usize, seed: u32) -> Result<u32> {
    match array.data_type() {
        DataType::Null => Ok(seed),
        DataType::Boolean => {
            let values = array.as_boolean();
            Ok(hash_int(i32::from(values.value(row)), seed))
        }
        DataType::Int8 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Int8Type>();
            Ok(hash_int(i32::from(values.value(row)), seed))
        }
        DataType::Int16 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Int16Type>();
            Ok(hash_int(i32::from(values.value(row)), seed))
        }
        DataType::Int32 => {
            let values = array.as_primitive::<Int32Type>();
            Ok(hash_int(values.value(row), seed))
        }
        DataType::Int64 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            Ok(hash_long(values.value(row), seed))
        }
        DataType::UInt8 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::UInt8Type>();
            Ok(hash_int(i32::from(values.value(row)), seed))
        }
        DataType::UInt16 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::UInt16Type>();
            Ok(hash_int(i32::from(values.value(row)), seed))
        }
        DataType::UInt32 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
            Ok(hash_int(values.value(row).cast_signed(), seed))
        }
        DataType::UInt64 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
            Ok(hash_long(values.value(row).cast_signed(), seed))
        }
        DataType::Float32 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
            Ok(hash_float(values.value(row), seed))
        }
        DataType::Float64 => {
            let values = array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            Ok(hash_double(values.value(row), seed))
        }
        DataType::Decimal32(_, _) => {
            let values = array.as_primitive::<Decimal32Type>();
            Ok(hash_decimal_compact(i64::from(values.value(row)), seed))
        }
        DataType::Decimal64(_, _) => {
            let values = array.as_primitive::<Decimal64Type>();
            Ok(hash_decimal_compact(values.value(row), seed))
        }
        DataType::Decimal128(_, _) => {
            let values = array.as_primitive::<Decimal128Type>();
            match i64::try_from(values.value(row)) {
                Ok(compact) => Ok(hash_decimal_compact(compact, seed)),
                Err(_) => Ok(hash_bytes_murmur(&values.value(row).to_le_bytes(), seed)),
            }
        }
        DataType::Decimal256(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs decimal256 values".to_owned(),
                    )
                })?;
            Ok(hash_bytes_murmur(&values.value(row).to_le_bytes(), seed))
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => hash_text_value(array, row, seed),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            Ok(hash_time_value(array, row, seed))
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => hash_nested_value(array, row, seed),
        _ => {
            let shaped = cast(array.as_ref(), &DataType::Utf8)?;
            hash_array_value(&shaped, row, seed)
        }
    }
}

fn hash_nested_value(array: &ArrayRef, row: usize, seed: u32) -> Result<u32> {
    use datafusion::arrow::array::{MapArray, StructArray};
    match array.data_type() {
        DataType::List(_) => {
            let lists = datafusion::arrow::array::as_list_array(array.as_ref());
            fold_list_elements::<i32>(lists, row, seed)
        }
        DataType::LargeList(_) => {
            let lists = datafusion::arrow::array::as_large_list_array(array.as_ref());
            fold_list_elements::<i64>(lists, row, seed)
        }
        DataType::FixedSizeList(_, _) => {
            let lists = datafusion::arrow::array::as_fixed_size_list_array(array.as_ref());
            let width = usize::try_from(lists.value_length()).unwrap_or(0);
            let values = lists.values();
            let mut hash = seed;
            for index in 0..width {
                let element = row * width + index;
                if !values.is_null(element) {
                    hash = hash_array_value(values, element, hash)?;
                }
            }
            Ok(hash)
        }
        DataType::Struct(_) => {
            let structs = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "hash needs struct values".to_owned(),
                    )
                })?;
            let mut hash = seed;
            for column in structs.columns() {
                if !column.is_null(row) {
                    hash = hash_array_value(column, row, hash)?;
                }
            }
            Ok(hash)
        }
        DataType::Map(_, _) => {
            let maps = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Execution("hash needs map values".to_owned())
            })?;
            let offsets = maps.offsets();
            let entries = maps.entries();
            let mut hash = seed;
            let start = usize::try_from(offsets[row]).unwrap_or(0);
            let end = usize::try_from(offsets[row + 1]).unwrap_or(0);
            for index in start..end {
                if !entries.is_null(index) {
                    for column in entries.columns() {
                        if !column.is_null(index) {
                            hash = hash_array_value(column, index, hash)?;
                        }
                    }
                }
            }
            Ok(hash)
        }
        _ => exec_err!("'hash' does not nest {array:?}"),
    }
}

fn fold_list_elements<O: OffsetSizeTrait>(
    lists: &GenericListArray<O>,
    row: usize,
    seed: u32,
) -> Result<u32> {
    let offsets = lists.offsets();
    let start = offsets[row].as_usize();
    let end = offsets[row + 1].as_usize();
    let values = lists.values();
    let mut hash = seed;
    for index in start..end {
        if !values.is_null(index) {
            hash = hash_array_value(values, index, hash)?;
        }
    }
    Ok(hash)
}

fn hash_column_values(array: &ArrayRef, hashes: &mut [u32]) -> Result<()> {
    for (row, hash) in hashes.iter_mut().enumerate() {
        if !array.is_null(row) {
            *hash = hash_array_value(array, row, *hash)?;
        }
    }
    Ok(())
}

impl ScalarUDFImpl for SparkHash {
    crate::shim_udf_boilerplate!("hash");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_hash(&declared)?;
        Ok(Arc::new(Field::new("hash", DataType::Int32, false)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.is_empty() {
            return exec_err!("'hash' expects at least one argument, got 0");
        }
        let parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        Ok(format!("hash({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_hash(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            number_rows,
            ..
        } = args;
        if arg_values.is_empty() {
            return exec_err!("'hash' expects at least one argument, got 0");
        }
        let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
        let mut hashes = vec![HASH_SEED; number_rows];
        for array in &arrays {
            hash_column_values(array, &mut hashes)?;
        }
        let out: Vec<i32> = hashes.iter().map(|hash| (*hash).cast_signed()).collect();
        Ok(ColumnarValue::Array(Arc::new(
            datafusion::arrow::array::Int32Array::from(out),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn int_values(ctx: &SessionContext, sql: &str) -> Vec<i32> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let mut out = Vec::new();
        for batch in &batches {
            out.extend(
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|cell| cell.expect("hash never answers NULL")),
            );
        }
        out
    }

    async fn split_two_partitions(ctx: &SessionContext, sql: &str) -> Vec<i32> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(2))
            .unwrap_or_else(|error| panic!("repartition {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let mut out = Vec::new();
        for batch in &batches {
            out.extend(
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|cell| cell.expect("hash never answers NULL")),
            );
        }
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn hash_answers_fixture_ints() {
        let ctx = ctx();
        assert_eq!(int_values(&ctx, "SELECT hash(1)").await, vec![-559_580_957]);
        assert_eq!(
            int_values(&ctx, "SELECT hash(CAST(1 AS BIGINT))").await,
            vec![-1_712_319_331]
        );
        assert_eq!(int_values(&ctx, "SELECT hash(NULL)").await, vec![42]);
        assert_eq!(
            int_values(&ctx, "SELECT hash(true)").await,
            vec![-559_580_957]
        );
        assert_eq!(
            int_values(&ctx, "SELECT hash(CAST(0.0 AS DOUBLE))").await,
            vec![-1_670_924_195]
        );
        assert_eq!(
            int_values(&ctx, "SELECT hash(CAST(-0.0 AS DOUBLE))").await,
            vec![-1_670_924_195]
        );
    }

    #[tokio::test]
    async fn hash_matches_single_partition_across_two_partitions() {
        let ctx = ctx();
        let sql = "SELECT hash(n) FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)";
        let mut single = int_values(&ctx, sql).await;
        single.sort_unstable();
        assert_eq!(split_two_partitions(&ctx, sql).await, single);
    }
}
