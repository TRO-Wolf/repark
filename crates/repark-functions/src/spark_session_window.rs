use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_time_window::{
    micros_to_timestamp_array, parse_session_gap_parts, timestamp_micros_batch, window_fields,
};

pub const SESSION_FUNCTION_NAME: &str = "session_window";
pub const SESSION_OUTPUT_NAME: &str = "session_window";
pub const SESSION_END_NAME: &str = "__repark_session_end__";
pub const SESSION_TS_NAME: &str = "__repark_ts_micros__";
pub const SESSION_ASSEMBLE_NAME: &str = "__repark_session_assemble__";

pub const SESSION_TS_COLUMN: &str = "__repark_session_ts__";
pub const SESSION_GAP_COLUMN: &str = "__repark_session_gap_col__";
pub const SESSION_END_TS_COLUMN: &str = "__repark_session_end_ts__";
pub const SESSION_PREV_TS_COLUMN: &str = "__repark_session_prev_ts__";
pub const SESSION_PREV_GAP_COLUMN: &str = "__repark_session_prev_gap__";
pub const SESSION_PREV_END_COLUMN: &str = "__repark_session_prev_end__";
pub const SESSION_NEW_COLUMN: &str = "__repark_session_new__";
pub const SESSION_INDEX_COLUMN: &str = "__repark_session_idx__";
pub const SESSION_START_COLUMN: &str = "__repark_session_start__";
pub const SESSION_END_COLUMN: &str = "__repark_session_endm__";

#[must_use]
pub fn session_window_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSessionWindow::new()))
}

#[must_use]
pub fn session_end_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionEnd::new()))
}

#[must_use]
pub fn session_ts_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionTsMicros::new()))
}

#[must_use]
pub fn session_assemble_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionAssemble::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        session_window_udf(),
        session_end_udf(),
        session_ts_udf(),
        session_assemble_udf(),
    ]
}

fn fallback_time_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn time_argument_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.first() {
        Some(typed @ DataType::Timestamp(_, _)) => Ok(typed.clone()),
        Some(DataType::Date32) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Some(other) => exec_err!("'session_window' timeColumn must be a TIMESTAMP, got {other}"),
        None => exec_err!("'session_window' requires a time column"),
    }
}

fn gap_argument_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.get(1) {
        Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null) => {
            Ok(DataType::Utf8)
        }
        Some(other) => {
            exec_err!("'session_window' gapDuration must be a duration string, got {other}")
        }
        None => exec_err!("'session_window' requires a gap duration"),
    }
}

#[derive(Debug)]
struct SparkSessionWindow {
    signature: Signature,
}

impl SparkSessionWindow {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSessionWindow {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSessionWindow {}

impl Hash for SparkSessionWindow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkSessionWindow {
    fn name(&self) -> &str {
        SESSION_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let time_type = time_argument_type(arg_types).unwrap_or_else(|_| fallback_time_type());
        Ok(DataType::Struct(window_fields(&time_type)))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let inputs: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let time_type = time_argument_type(&inputs).unwrap_or_else(|_| fallback_time_type());
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(window_fields(&time_type)),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'session_window' requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![
            time_argument_type(arg_types)?,
            gap_argument_type(arg_types)?,
        ])
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("'session_window' groups rows and has no per-row value; use it in GROUP BY")
    }
}

#[derive(Debug)]
struct SessionEnd {
    signature: Signature,
}

impl SessionEnd {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SessionEnd {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SessionEnd {}

impl Hash for SessionEnd {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn add_months_to_timestamp_micros(stamp: i64, months: i32, extra_micros: i64) -> Option<i64> {
    let moment = DateTime::from_timestamp_micros(stamp)?;
    let shifted = crate::datetime::spark_add_months(moment.date_naive(), months)?;
    shifted
        .and_time(moment.time())
        .and_utc()
        .timestamp_micros()
        .checked_add(extra_micros)
}

fn read_session_gaps(gaps: &ArrayRef) -> Result<Vec<Option<(i32, i64)>>> {
    if matches!(gaps.data_type(), DataType::Null) {
        return Ok(vec![None; gaps.len()]);
    }
    let strings = gaps.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(format!(
            "'{SESSION_END_NAME}' gapDuration must be a duration string",
        ))
    })?;
    let mut cache: HashMap<&str, (i32, i64)> = HashMap::new();
    let mut parsed: Vec<Option<(i32, i64)>> = Vec::with_capacity(strings.len());
    for row in 0..strings.len() {
        if strings.is_null(row) {
            parsed.push(None);
            continue;
        }
        let text = strings.value(row);
        let parts = if let Some(parts) = cache.get(text) {
            *parts
        } else {
            let parts = parse_session_gap_parts(text)?;
            cache.insert(text, parts);
            parts
        };
        parsed.push(Some(parts));
    }
    Ok(parsed)
}

impl ScalarUDFImpl for SessionEnd {
    fn name(&self) -> &str {
        SESSION_END_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        time_argument_type(arg_types).or_else(|_| Ok(fallback_time_type()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let inputs: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let time_type = time_argument_type(&inputs).unwrap_or_else(|_| fallback_time_type());
        Ok(Arc::new(Field::new(self.name(), time_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'{}' requires 2 arguments, got {}",
                SESSION_END_NAME,
                arg_types.len()
            );
        }
        Ok(vec![
            time_argument_type(arg_types)?,
            gap_argument_type(&[arg_types[0].clone(), arg_types[1].clone()])?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(field) = args.arg_fields.first() else {
            return exec_err!("'{}' requires a time column", SESSION_END_NAME);
        };
        let time_type = time_argument_type(std::slice::from_ref(field.data_type()))?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 2 {
            return exec_err!("'{}' requires 2 arguments", SESSION_END_NAME);
        }
        let instants = timestamp_micros_batch(&time_type, &arrays[0])?;
        let gaps = read_session_gaps(&arrays[1])?;
        let time_nulls = arrays[0].nulls();
        let mut ends: Vec<i64> = Vec::with_capacity(instants.len());
        let mut valid: Vec<bool> = Vec::with_capacity(instants.len());
        for (row, (instant, gap)) in instants.iter().zip(gaps.iter()).enumerate() {
            if time_nulls.is_some_and(|nulls| nulls.is_null(row)) {
                ends.push(0);
                valid.push(false);
                continue;
            }
            let stop = match gap {
                None => None,
                Some((months, micros)) if *months < 0 || (*months == 0 && *micros <= 0) => None,
                Some((0, micros)) => Some(instant.checked_add(*micros).ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "'session_window' session end is out of range".to_string(),
                    )
                })?),
                Some((months, micros)) => Some(
                    add_months_to_timestamp_micros(*instant, *months, *micros).ok_or_else(
                        || {
                            datafusion::common::DataFusionError::Execution(
                                "'session_window' session end is out of range".to_string(),
                            )
                        },
                    )?,
                ),
            };
            match stop {
                None => {
                    ends.push(0);
                    valid.push(false);
                }
                Some(stop) => {
                    ends.push(stop);
                    valid.push(true);
                }
            }
        }
        let nulls: NullBuffer = valid.iter().copied().collect();
        let nulls = if nulls.null_count() == 0 {
            None
        } else {
            Some(nulls)
        };
        Ok(ColumnarValue::Array(micros_to_timestamp_array(
            &time_type, ends, nulls,
        )?))
    }
}

#[derive(Debug)]
struct SessionTsMicros {
    signature: Signature,
}

impl SessionTsMicros {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SessionTsMicros {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SessionTsMicros {}

impl Hash for SessionTsMicros {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SessionTsMicros {
    fn name(&self) -> &str {
        SESSION_TS_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "'{}' requires 1 argument, got {}",
                SESSION_TS_NAME,
                arg_types.len()
            );
        }
        Ok(vec![
            time_argument_type(&[arg_types[0].clone()]).map(|_| arg_types[0].clone())?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(field) = args.arg_fields.first() else {
            return exec_err!("'{}' requires a time column", SESSION_TS_NAME);
        };
        let time_type = time_argument_type(std::slice::from_ref(field.data_type()))?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(time) = arrays.first() else {
            return exec_err!("'{}' requires a time column", SESSION_TS_NAME);
        };
        let instants = timestamp_micros_batch(&time_type, time)?;
        let nulls = time.nulls().filter(|nulls| nulls.null_count() > 0).cloned();
        Ok(ColumnarValue::Array(Arc::new(
            datafusion::arrow::array::Int64Array::new(
                datafusion::arrow::buffer::ScalarBuffer::from(instants),
                nulls,
            ),
        )))
    }
}

#[derive(Debug)]
struct SessionAssemble {
    signature: Signature,
}

impl SessionAssemble {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SessionAssemble {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SessionAssemble {}

impl Hash for SessionAssemble {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn assemble_end_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.first() {
        Some(typed @ DataType::Timestamp(_, _)) => Ok(typed.clone()),
        Some(other) => {
            exec_err!(
                "'{}' start must be a TIMESTAMP, got {other}",
                SESSION_ASSEMBLE_NAME
            )
        }
        None => exec_err!("'{}' requires a start column", SESSION_ASSEMBLE_NAME),
    }
}

impl ScalarUDFImpl for SessionAssemble {
    fn name(&self) -> &str {
        SESSION_ASSEMBLE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let time_type = assemble_end_type(arg_types).unwrap_or_else(|_| fallback_time_type());
        Ok(DataType::Struct(window_fields(&time_type)))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let inputs: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let time_type = assemble_end_type(&inputs).unwrap_or_else(|_| fallback_time_type());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(window_fields(&time_type)),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "'{}' requires 2 arguments, got {}",
                SESSION_ASSEMBLE_NAME,
                arg_types.len()
            );
        }
        let time_type = assemble_end_type(arg_types)?;
        match arg_types.get(1) {
            Some(DataType::Int64) => Ok(vec![time_type, DataType::Int64]),
            Some(typed @ DataType::Timestamp(_, _)) => Ok(vec![time_type, typed.clone()]),
            Some(other) => exec_err!(
                "'{}' end must be Int64 micros or a TIMESTAMP, got {other}",
                SESSION_ASSEMBLE_NAME
            ),
            None => exec_err!("'{}' requires an end column", SESSION_ASSEMBLE_NAME),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(field) = args.arg_fields.first() else {
            return exec_err!("'{}' requires a start column", SESSION_ASSEMBLE_NAME);
        };
        let time_type = assemble_end_type(std::slice::from_ref(field.data_type()))?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 2 {
            return exec_err!("'{}' requires 2 arguments", SESSION_ASSEMBLE_NAME);
        }
        let starts = arrays[0].clone();
        if !matches!(starts.data_type(), DataType::Timestamp(_, _)) {
            return exec_err!(
                "'{}' start must be a TIMESTAMP, got {}",
                SESSION_ASSEMBLE_NAME,
                starts.data_type()
            );
        }
        let end_array = match arrays[1].data_type() {
            DataType::Timestamp(_, _) => arrays[1].clone(),
            DataType::Int64 => {
                let ends = arrays[1]
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "'{SESSION_ASSEMBLE_NAME}' end must be Int64 micros",
                        ))
                    })?;
                let mut end_values: Vec<i64> = Vec::with_capacity(ends.len());
                let mut end_valid: Vec<bool> = Vec::with_capacity(ends.len());
                for row in 0..ends.len() {
                    if ends.is_null(row) {
                        end_values.push(0);
                        end_valid.push(false);
                    } else {
                        end_values.push(ends.value(row));
                        end_valid.push(true);
                    }
                }
                let end_nulls: NullBuffer = end_valid.iter().copied().collect();
                let end_nulls = if end_nulls.null_count() == 0 {
                    None
                } else {
                    Some(end_nulls)
                };
                micros_to_timestamp_array(&time_type, end_values, end_nulls)?
            }
            other => {
                return exec_err!(
                    "'{}' end must be Int64 micros or a TIMESTAMP, got {other}",
                    SESSION_ASSEMBLE_NAME
                );
            }
        };
        let nulls = starts
            .nulls()
            .filter(|nulls| nulls.null_count() > 0)
            .cloned();
        Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
            window_fields(&time_type),
            vec![starts, end_array],
            nulls,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty(), "expected rows for {sql}");
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
        }
    }

    #[tokio::test]
    async fn gap_end_adds_the_gap_to_the_stamp() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', '5 minutes') AS g, \
             __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', '1 hour') AS h",
        )
        .await;
        let ends = produced
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
        assert!(!produced.column(0).is_null(0));
        assert_eq!(ends.value(0), 1_704_103_950_000_000_000);
        let hours = produced
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(hours.value(0), 1_704_107_250_000_000_000);
    }

    #[tokio::test]
    async fn bad_gap_carries_the_condition() {
        let ctx = ctx();
        let message = sql_error(
            &ctx,
            "SELECT __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', '10 parsecs') AS g",
        )
        .await;
        assert!(
            message.contains("[CANNOT_PARSE_INTERVAL]"),
            "expected the condition, got {message}"
        );
    }

    #[tokio::test]
    async fn null_zero_and_negative_gaps_are_null_ends() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', CAST(NULL AS STRING)) AS n, \
             __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', '0 seconds') AS z, \
             __repark_session_end__(TIMESTAMP '2024-01-01 10:07:30', '-1 minute') AS m, \
             __repark_session_end__(CAST(NULL AS TIMESTAMP), '5 minutes') AS t",
        )
        .await;
        for column in produced.columns() {
            assert!(column.is_null(0), "drop-row gaps stay null");
        }
    }

    #[tokio::test]
    async fn month_gap_answers_a_calendar_end() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT __repark_session_end__(TIMESTAMP '2026-09-15 10:07:30', '1 month') AS g",
        )
        .await;
        let ends = produced
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
        assert!(!produced.column(0).is_null(0));
        assert_eq!(ends.value(0), 1_792_058_850_000_000_000);
    }

    #[tokio::test]
    async fn ts_micros_reads_timestamps() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT __repark_ts_micros__(TIMESTAMP '2024-01-01 10:07:30') AS m",
        )
        .await;
        let micros = produced
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0);
        assert_eq!(micros, 1_704_103_200_000_000 + 450_000_000);
    }

    #[tokio::test]
    async fn assemble_builds_start_end_struct() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT __repark_session_assemble__(TIMESTAMP '2024-01-01 10:07:30', 1704103950000000) AS w",
        )
        .await;
        let structs = produced
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let names: Vec<&str> = structs
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, vec!["start", "end"]);
        let ends = structs
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(ends.value(0), 1_704_103_950_000_000_000);
    }

    #[tokio::test]
    async fn bare_session_call_refuses_loud() {
        let ctx = ctx();
        let message = sql_error(
            &ctx,
            "SELECT session_window(TIMESTAMP '2024-01-01 10:00:00', '5 minutes') AS w",
        )
        .await;
        assert!(
            message.contains("GROUP BY"),
            "expected the grouping refusal, got {message}"
        );
    }
}
