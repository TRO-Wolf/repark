use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::i256;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Field, FieldRef, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};

#[must_use]
pub fn max_min_by_udaf(find_max: bool) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(MaxMinBy::new(find_max)))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MaxMinBy {
    signature: Signature,
    find_max: bool,
}

impl MaxMinBy {
    fn new(find_max: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            find_max,
        }
    }

    fn wrong_args(&self, actual: usize) -> DataFusionError {
        DataFusionError::Plan(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{}` requires 2 parameters but the actual \
             number is {actual}. Please, refer to \
             'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. SQLSTATE: \
             42605",
            self.name()
        ))
    }

    fn spark_display(&self, params: &AggregateFunctionParams) -> String {
        let args: Vec<String> = params.args.iter().map(unqualified_name).collect();
        format!("{}({})", self.name(), args.join(", "))
    }
}

pub(crate) fn unqualified_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => column.name.clone(),
        _ => expr.schema_name().to_string(),
    }
}

impl AggregateUDFImpl for MaxMinBy {
    fn name(&self) -> &str {
        if self.find_max { "max_by" } else { "min_by" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(self.wrong_args(arg_types.len()));
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let fields = acc_args.expr_fields;
        if fields.len() != 2 {
            return Err(self.wrong_args(fields.len()));
        }
        Ok(Box::new(MaxMinByAccumulator::new(
            self.find_max,
            fields[1].data_type().clone(),
            fields[0].data_type().clone(),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields.len() != 2 {
            return Err(self.wrong_args(args.input_fields.len()));
        }
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "ord"),
                args.input_fields[1].data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "val"),
                args.input_fields[0].data_type().clone(),
                true,
            )),
        ])
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(self.spark_display(params))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OrdKey<'a> {
    Bool(bool),
    Int(i128),
    Big(i256),
    Float(f64),
    Utf8(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> OrdKey<'a> {
    pub(crate) fn cmp(&self, other: &Self) -> Result<Ordering> {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => Ok(left.cmp(right)),
            (Self::Int(left), Self::Int(right)) => Ok(left.cmp(right)),
            (Self::Big(left), Self::Big(right)) => Ok(left.cmp(right)),
            (Self::Float(left), Self::Float(right)) => Ok(left.total_cmp(right)),
            (Self::Utf8(left), Self::Utf8(right)) => Ok(left.cmp(right)),
            (Self::Bytes(left), Self::Bytes(right)) => Ok(left.cmp(right)),
            _ => Err(DataFusionError::Execution(
                "max_by/min_by ordering keys have mismatched types".to_string(),
            )),
        }
    }

    pub(crate) fn from_scalar(value: &'a ScalarValue) -> Result<Option<Self>> {
        match value {
            ScalarValue::Null => Ok(None),
            ScalarValue::Boolean(value) => Ok(value.map(Self::Bool)),
            ScalarValue::Int8(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Int16(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Int32(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Int64(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::UInt8(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::UInt16(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::UInt32(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::UInt64(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Float16(value) => Ok(value.map(|v| Self::Float(f64::from(v.to_f32())))),
            ScalarValue::Float32(value) => Ok(value.map(|v| Self::Float(f64::from(v)))),
            ScalarValue::Float64(value) => Ok(value.map(Self::Float)),
            ScalarValue::Decimal32(value, _, _) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Decimal64(value, _, _) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Decimal128(value, _, _) => Ok(value.map(Self::Int)),
            ScalarValue::Decimal256(value, _, _) => Ok(value.map(Self::Big)),
            ScalarValue::Date32(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Date64(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Time32Second(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Time32Millisecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Time64Microsecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Time64Nanosecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::TimestampSecond(value, _) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::TimestampMillisecond(value, _) => {
                Ok(value.map(|v| Self::Int(i128::from(v))))
            }
            ScalarValue::TimestampMicrosecond(value, _) => {
                Ok(value.map(|v| Self::Int(i128::from(v))))
            }
            ScalarValue::TimestampNanosecond(value, _) => {
                Ok(value.map(|v| Self::Int(i128::from(v))))
            }
            ScalarValue::DurationSecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::DurationMillisecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::DurationMicrosecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::DurationNanosecond(value) => Ok(value.map(|v| Self::Int(i128::from(v)))),
            ScalarValue::Utf8(value)
            | ScalarValue::Utf8View(value)
            | ScalarValue::LargeUtf8(value) => Ok(value.as_deref().map(Self::Utf8)),
            ScalarValue::Binary(value)
            | ScalarValue::BinaryView(value)
            | ScalarValue::LargeBinary(value)
            | ScalarValue::FixedSizeBinary(_, value) => Ok(value.as_deref().map(Self::Bytes)),
            _ => Err(DataFusionError::Execution(format!(
                "max_by/min_by ordering over {} is not supported",
                value.data_type()
            ))),
        }
    }

    pub(crate) fn from_array(array: &'a ArrayRef, row: usize) -> Result<Option<Self>> {
        if array.data_type() == &DataType::Null || array.is_null(row) {
            return Ok(None);
        }
        match array.data_type() {
            DataType::Boolean => Ok(Some(Self::Bool(array.as_boolean().value(row)))),
            DataType::Int8 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Int8Type>().value(row),
            )))),
            DataType::Int16 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Int16Type>().value(row),
            )))),
            DataType::Int32 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Int32Type>().value(row),
            )))),
            DataType::Int64 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Int64Type>().value(row),
            )))),
            DataType::UInt8 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<UInt8Type>().value(row),
            )))),
            DataType::UInt16 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<UInt16Type>().value(row),
            )))),
            DataType::UInt32 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<UInt32Type>().value(row),
            )))),
            DataType::UInt64 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<UInt64Type>().value(row),
            )))),
            DataType::Float16 => Ok(Some(Self::Float(f64::from(
                array.as_primitive::<Float16Type>().value(row).to_f32(),
            )))),
            DataType::Float32 => Ok(Some(Self::Float(f64::from(
                array.as_primitive::<Float32Type>().value(row),
            )))),
            DataType::Float64 => Ok(Some(Self::Float(
                array.as_primitive::<Float64Type>().value(row),
            ))),
            DataType::Decimal32(_, _) => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Decimal32Type>().value(row),
            )))),
            DataType::Decimal64(_, _) => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Decimal64Type>().value(row),
            )))),
            DataType::Decimal128(_, _) => Ok(Some(Self::Int(
                array.as_primitive::<Decimal128Type>().value(row),
            ))),
            DataType::Decimal256(_, _) => Ok(Some(Self::Big(
                array.as_primitive::<Decimal256Type>().value(row),
            ))),
            DataType::Date32 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Date32Type>().value(row),
            )))),
            DataType::Date64 => Ok(Some(Self::Int(i128::from(
                array.as_primitive::<Date64Type>().value(row),
            )))),
            DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => temporal_key(array, row),
            DataType::Utf8 => Ok(Some(Self::Utf8(array.as_string::<i32>().value(row)))),
            DataType::LargeUtf8 => Ok(Some(Self::Utf8(array.as_string::<i64>().value(row)))),
            DataType::Utf8View => Ok(Some(Self::Utf8(array.as_string_view().value(row)))),
            DataType::Binary => Ok(Some(Self::Bytes(array.as_binary::<i32>().value(row)))),
            DataType::LargeBinary => Ok(Some(Self::Bytes(array.as_binary::<i64>().value(row)))),
            DataType::BinaryView => Ok(Some(Self::Bytes(array.as_binary_view().value(row)))),
            DataType::FixedSizeBinary(_) => {
                Ok(Some(Self::Bytes(array.as_fixed_size_binary().value(row))))
            }
            other => Err(DataFusionError::Execution(format!(
                "max_by/min_by ordering over {other} is not supported"
            ))),
        }
    }
}

fn temporal_key(array: &ArrayRef, row: usize) -> Result<Option<OrdKey<'_>>> {
    use arrow::datatypes::TimeUnit;
    let micros64 = matches!(array.data_type(), DataType::Time64(TimeUnit::Microsecond));
    let seconds32 = matches!(array.data_type(), DataType::Time32(TimeUnit::Second));
    let stamp = match array.data_type() {
        DataType::Timestamp(unit, _) => Some(*unit),
        _ => None,
    };
    let span = match array.data_type() {
        DataType::Duration(unit) => Some(*unit),
        _ => None,
    };
    if seconds32 {
        return Ok(Some(OrdKey::Int(i128::from(
            array.as_primitive::<Time32SecondType>().value(row),
        ))));
    }
    if micros64 {
        return Ok(Some(OrdKey::Int(i128::from(
            array.as_primitive::<Time64MicrosecondType>().value(row),
        ))));
    }
    if let Some(unit) = stamp {
        return Ok(Some(OrdKey::Int(i128::from(match unit {
            TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
            TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
            TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
            TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
        }))));
    }
    if let Some(unit) = span {
        return Ok(Some(OrdKey::Int(i128::from(match unit {
            TimeUnit::Second => array.as_primitive::<DurationSecondType>().value(row),
            TimeUnit::Millisecond => array.as_primitive::<DurationMillisecondType>().value(row),
            TimeUnit::Microsecond => array.as_primitive::<DurationMicrosecondType>().value(row),
            TimeUnit::Nanosecond => array.as_primitive::<DurationNanosecondType>().value(row),
        }))));
    }
    match array.data_type() {
        DataType::Time32(_) => Ok(Some(OrdKey::Int(i128::from(
            array.as_primitive::<Time32MillisecondType>().value(row),
        )))),
        DataType::Time64(_) => Ok(Some(OrdKey::Int(i128::from(
            array.as_primitive::<Time64NanosecondType>().value(row),
        )))),
        other => Err(DataFusionError::Execution(format!(
            "max_by/min_by ordering over {other} is not supported"
        ))),
    }
}

#[derive(Debug)]
struct MaxMinByAccumulator {
    find_max: bool,
    ord_type: DataType,
    value_type: DataType,
    best_ord: Option<ScalarValue>,
    best_val: Option<ScalarValue>,
}

impl MaxMinByAccumulator {
    fn new(find_max: bool, ord_type: DataType, value_type: DataType) -> Self {
        Self {
            find_max,
            ord_type,
            value_type,
            best_ord: None,
            best_val: None,
        }
    }

    fn consider(&mut self, ord: ScalarValue, key: OrdKey, val: ScalarValue) -> Result<()> {
        let take = match &self.best_ord {
            None => true,
            Some(best) => {
                let best_key = OrdKey::from_scalar(best)?.ok_or_else(|| {
                    DataFusionError::Internal("max_by/min_by incumbent ord is null".to_string())
                })?;
                let order = best_key.cmp(&key)?;
                (self.find_max && order == Ordering::Less)
                    || (!self.find_max && order == Ordering::Greater)
            }
        };
        if take {
            self.best_ord = Some(ord);
            self.best_val = Some(val);
        }
        Ok(())
    }

    fn null_of(data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let ords = &values[1];
        for row in 0..values[0].len() {
            let Some(key) = OrdKey::from_array(ords, row)? else {
                continue;
            };
            let ord = ScalarValue::try_from_array(ords, row)?;
            let val = ScalarValue::try_from_array(&values[0], row)?;
            self.consider(ord, key, val)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.best_val {
            Some(value) => Ok(value.clone()),
            None => Self::null_of(&self.value_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.best_ord
                .clone()
                .unwrap_or(Self::null_of(&self.ord_type)?),
            self.best_val
                .clone()
                .unwrap_or(Self::null_of(&self.value_type)?),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for row in 0..states[0].len() {
            if states[0].is_null(row) {
                continue;
            }
            let key = OrdKey::from_array(&states[0], row)?.ok_or_else(|| {
                DataFusionError::Internal("max_by/min_by state ord is null".to_string())
            })?;
            let ord = ScalarValue::try_from_array(&states[0], row)?;
            let val = ScalarValue::try_from_array(&states[1], row)?;
            self.consider(ord, key, val)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(max_min_by_udaf(true).as_ref().clone());
        ctx.register_udaf(max_min_by_udaf(false).as_ref().clone());
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().expect("one batch")
    }

    #[tokio::test]
    async fn max_by_skips_null_ord_and_keeps_null_value() {
        let batch = batch(
            &ctx(),
            "SELECT max_by(k, v) FROM (VALUES ('a', 10), ('b', NULL), ('c', 30)) AS t(k, v)",
        )
        .await;
        assert_eq!(batch.schema().field(0).name(), "max_by(k, v)");
        let value = ScalarValue::try_from_array(batch.column(0), 0).expect("value");
        assert_eq!(value, ScalarValue::Utf8(Some("c".to_string())));
    }

    #[tokio::test]
    async fn min_by_picks_first_of_ties_and_null_when_all_ord_null() {
        let batch = batch(
            &ctx(),
            "SELECT min_by(k, v), min_by(k, g) AS n FROM (VALUES ('a', 10, NULL), ('b', 10, NULL)) AS t(k, v, g)",
        )
        .await;
        assert_eq!(batch.schema().field(0).name(), "min_by(k, v)");
        let first = ScalarValue::try_from_array(batch.column(0), 0).expect("first");
        assert_eq!(first, ScalarValue::Utf8(Some("a".to_string())));
        assert!(batch.column(1).is_null(0));
    }

    #[tokio::test]
    async fn three_args_raise_spark_wrong_num_args() {
        let error = ctx()
            .sql("SELECT max_by(k, v, 2) FROM (VALUES ('a', 1)) AS t(k, v)")
            .await
            .expect_err("three args must not plan");
        assert!(
            error
                .to_string()
                .contains("[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]"),
            "got {error}"
        );
        let error = ctx()
            .sql("SELECT min_by(k) FROM (VALUES ('a', 1)) AS t(k, v)")
            .await
            .expect_err("one arg must not plan");
        assert!(
            error
                .to_string()
                .contains("requires 2 parameters but the actual number is 1"),
            "got {error}"
        );
    }

    fn accumulator(find_max: bool) -> MaxMinByAccumulator {
        MaxMinByAccumulator::new(find_max, DataType::Int64, DataType::Utf8)
    }

    fn input(values: Vec<Option<&str>>, ords: Vec<Option<i64>>) -> Vec<ArrayRef> {
        use arrow::array::{Int64Array, StringArray};
        let values: Vec<Option<String>> =
            values.into_iter().map(|v| v.map(str::to_string)).collect();
        vec![
            Arc::new(StringArray::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(ords)) as ArrayRef,
        ]
    }

    fn merged(find_max: bool, left: &[ArrayRef], right: &[ArrayRef]) -> ScalarValue {
        let mut first = accumulator(find_max);
        first.update_batch(left).expect("left update");
        let mut second = accumulator(find_max);
        second.update_batch(right).expect("right update");
        let left_state = first.state().expect("left state");
        let right_state = second.state().expect("right state");
        let states: Vec<ArrayRef> = (0..2)
            .map(|column| {
                ScalarValue::iter_to_array(vec![
                    left_state[column].clone(),
                    right_state[column].clone(),
                ])
                .expect("state column")
            })
            .collect();
        let mut merged = accumulator(find_max);
        merged.merge_batch(&states).expect("merge");
        merged.evaluate().expect("evaluate")
    }

    #[tokio::test]
    async fn merge_batch_matches_single_partition() {
        let left = input(vec![Some("a"), Some("b")], vec![Some(10), Some(20)]);
        let right = input(vec![Some("c"), None], vec![Some(30), None]);
        assert_eq!(
            merged(true, &left, &right),
            ScalarValue::Utf8(Some("c".to_string()))
        );
        assert_eq!(
            merged(false, &left, &right),
            ScalarValue::Utf8(Some("a".to_string()))
        );
    }

    #[tokio::test]
    async fn merge_batch_keeps_null_value_and_skips_empty_partial() {
        let left = input(vec![None], vec![None]);
        let right = input(vec![None], vec![Some(1)]);
        assert_eq!(merged(true, &left, &right), ScalarValue::Utf8(None));
    }
}
