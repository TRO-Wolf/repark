use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use chrono::{DateTime, NaiveDateTime, TimeZone};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, TimestampMicrosecondType,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::ansi::{SparkAnsiConfig, spark_ansi_enabled_from_options};
use crate::instant_ts::{ltz_timestamp_type, ntz_timestamp_type, to_timestamp_udf};
use crate::java_datetime::{
    FormatPlan, compile_java_pattern, parse_wall_or_null, plan_format_column,
};
use crate::session_time_zone::session_time_zone_from_options;

#[must_use]
pub(crate) fn to_timestamp_ltz_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToTimestampLtz::new()))
}

#[must_use]
pub(crate) fn to_timestamp_ntz_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToTimestampNtz::new()))
}

#[must_use]
pub(crate) fn try_to_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTryToTimestamp::new()))
}

#[derive(Debug)]
struct SparkToTimestampLtz {
    signature: Signature,
}

impl SparkToTimestampLtz {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkToTimestampLtz {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToTimestampLtz {}

impl Hash for SparkToTimestampLtz {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToTimestampLtz {
    crate::shim_udf_boilerplate!("to_timestamp_ltz");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name(), ltz_timestamp_type(), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() > 2 {
            return exec_err!(
                "'to_timestamp_ltz' expects 1 or 2 arguments, got {}",
                args.args.len()
            );
        }
        to_timestamp_udf().invoke_with_args(args)
    }
}

#[derive(Debug)]
struct SparkToTimestampNtz {
    signature: Signature,
}

impl SparkToTimestampNtz {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkToTimestampNtz {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToTimestampNtz {}

impl Hash for SparkToTimestampNtz {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToTimestampNtz {
    crate::shim_udf_boilerplate!("to_timestamp_ntz");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ntz_timestamp_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name(), ntz_timestamp_type(), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!(
                "'to_timestamp_ntz' expects 1 or 2 arguments, got {}",
                args.args.len()
            );
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() == 2 && is_utf8_type(arrays[0].data_type()) {
            return ntz_with_format(&arrays[0], &arrays[1], &args);
        }
        ntz_single(&arrays[0], &args)
    }
}

#[derive(Debug)]
struct SparkTryToTimestamp {
    signature: Signature,
}

impl SparkTryToTimestamp {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTryToTimestamp {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTryToTimestamp {}

impl Hash for SparkTryToTimestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTryToTimestamp {
    crate::shim_udf_boilerplate!("try_to_timestamp");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name(), ltz_timestamp_type(), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() > 2 {
            return exec_err!(
                "'try_to_timestamp' expects 1 or 2 arguments, got {}",
                args.args.len()
            );
        }
        session_zone(args.config_options.as_ref())?;
        let shadow = without_ansi(&args);
        if args.args.len() == 1 {
            let arrays = ColumnarValue::values_to_arrays(&args.args)?;
            if is_utf8_type(arrays[0].data_type()) {
                let texts = utf8_strings(&arrays[0])?;
                return Ok(ColumnarValue::Array(ltz_of_strings(&texts, &shadow, true)?));
            }
        }
        to_timestamp_udf().invoke_with_args(shadow)
    }
}

fn without_ansi(args: &ScalarFunctionArgs) -> ScalarFunctionArgs {
    let mut options = (*args.config_options).clone();
    options
        .extensions
        .insert(SparkAnsiConfig { enabled: false });
    ScalarFunctionArgs {
        args: args.args.clone(),
        arg_fields: args.arg_fields.clone(),
        number_rows: args.number_rows,
        return_field: args.return_field.clone(),
        config_options: Arc::new(options),
    }
}

fn is_utf8_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn session_zone(options: &ConfigOptions) -> Result<Tz> {
    session_time_zone_from_options(options)
        .parse::<Tz>()
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone {:?} could not be resolved at query time ({error})",
                session_time_zone_from_options(options)
            ))
        })
}

fn forward_to_timestamp(values: ArrayRef, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let rows = values.len();
    let field: FieldRef = Arc::new(Field::new("to_timestamp_ntz", DataType::Utf8, true));
    let return_field: FieldRef =
        Arc::new(Field::new("to_timestamp_ntz", ltz_timestamp_type(), true));
    let forward = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(values)],
        arg_fields: vec![field],
        number_rows: rows,
        return_field,
        config_options: Arc::clone(&args.config_options),
    };
    to_timestamp_udf().invoke_with_args(forward)
}

fn retarget_to_ntz(error: &DataFusionError) -> DataFusionError {
    DataFusionError::Execution(
        error
            .to_string()
            .replace("\"TIMESTAMP\"", "\"TIMESTAMP_NTZ\""),
    )
}

fn ltz_of_strings(
    texts: &StringArray,
    args: &ScalarFunctionArgs,
    tolerate: bool,
) -> Result<ArrayRef> {
    let values: ArrayRef = Arc::new(texts.clone());
    let rows = values.len();
    let field: FieldRef = Arc::new(Field::new("to_timestamp_ntz", DataType::Utf8, true));
    let return_field: FieldRef =
        Arc::new(Field::new("to_timestamp_ntz", ltz_timestamp_type(), true));
    let forward = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(values)],
        arg_fields: vec![field],
        number_rows: rows,
        return_field,
        config_options: Arc::clone(&args.config_options),
    };
    match to_timestamp_udf().invoke_with_args(forward) {
        Ok(produced) => {
            let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&produced))?;
            if !tolerate {
                return Ok(Arc::clone(&arrays[0]));
            }
            let out = arrays[0].as_primitive::<TimestampMicrosecondType>();
            if out.null_count() == texts.null_count() {
                return Ok(Arc::clone(&arrays[0]));
            }
            salvage_strings(texts, args)
        }
        Err(error) => {
            if !tolerate {
                return Err(error);
            }
            salvage_strings(texts, args)
        }
    }
}

fn salvage_strings(texts: &StringArray, args: &ScalarFunctionArgs) -> Result<ArrayRef> {
    let mut builder = TimestampMicrosecondArray::builder(texts.len());
    for row in 0..texts.len() {
        if texts.is_null(row) {
            builder.append_null();
            continue;
        }
        let one: ArrayRef = Arc::new(StringArray::from(vec![Some(texts.value(row))]));
        match forward_to_timestamp(one, args) {
            Ok(produced) => {
                let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(&produced))?;
                let micros = arrays[0].as_primitive::<TimestampMicrosecondType>();
                if micros.is_null(0) {
                    builder.append_null();
                } else {
                    builder.append_value(micros.value(0));
                }
            }
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish().with_timezone("UTC")))
}

fn wall_without_zone(text: &str) -> &str {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return text;
    }
    let bytes = trimmed.as_bytes();
    if bytes[bytes.len() - 1] == b']'
        && let Some(start) = trimmed.find('[')
    {
        return trimmed[..start].trim_end();
    }
    if bytes[bytes.len() - 1] == b'Z' || bytes[bytes.len() - 1] == b'z' {
        return trimmed[..trimmed.len() - 1].trim_end();
    }
    for width in [6_usize, 5, 3] {
        if trimmed.len() < width {
            continue;
        }
        let tail = &trimmed[trimmed.len() - width..];
        let head = tail.as_bytes();
        if head[0] != b'+' && head[0] != b'-' {
            continue;
        }
        if !head[1].is_ascii_digit() || !head[2].is_ascii_digit() {
            continue;
        }
        let zoned = match width {
            3 => true,
            5 => head[3].is_ascii_digit() && head[4].is_ascii_digit(),
            _ => head[3] == b':' && head[4].is_ascii_digit() && head[5].is_ascii_digit(),
        };
        let wall = &trimmed[..trimmed.len() - width];
        let has_time = wall
            .bytes()
            .any(|byte| byte == b':' || byte == b'T' || byte == b't' || byte == b' ');
        if zoned && has_time {
            return wall.trim_end();
        }
    }
    text
}

fn unlocalize_to_walls(ltz: &ArrayRef, zone: Tz) -> Result<ArrayRef> {
    let instants = cast(ltz.as_ref(), &ltz_timestamp_type())?;
    let instants = instants.as_primitive::<TimestampMicrosecondType>();
    let mut builder = TimestampMicrosecondArray::builder(instants.len());
    for row in 0..instants.len() {
        if instants.is_null(row) {
            builder.append_null();
            continue;
        }
        let utc = DateTime::from_timestamp_micros(instants.value(row)).ok_or_else(|| {
            DataFusionError::Execution(
                "[CAST_INVALID_INPUT] NTZ instant is out of the supported range. SQLSTATE: 22018"
                    .to_string(),
            )
        })?;
        let wall = zone.from_utc_datetime(&utc.naive_utc()).naive_local();
        builder.append_value(wall.and_utc().timestamp_micros());
    }
    Ok(Arc::new(builder.finish()))
}

fn all_null_ntz(rows: usize) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; rows]))
}

fn ntz_single(input: &ArrayRef, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let zone = session_zone(args.config_options.as_ref())?;
    match input.data_type() {
        DataType::Null => Ok(ColumnarValue::Array(all_null_ntz(input.len()))),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let texts = cast(input.as_ref(), &DataType::Utf8)?;
            let texts = texts.as_string::<i32>();
            let mut stripped =
                StringBuilder::with_capacity(texts.len(), texts.get_array_memory_size());
            for row in 0..texts.len() {
                if texts.is_null(row) {
                    stripped.append_null();
                } else {
                    stripped.append_value(wall_without_zone(texts.value(row)));
                }
            }
            let stripped = stripped.finish();
            let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
            let instants =
                ltz_of_strings(&stripped, args, !ansi).map_err(|error| retarget_to_ntz(&error))?;
            Ok(ColumnarValue::Array(unlocalize_to_walls(&instants, zone)?))
        }
        DataType::Date32 | DataType::Date64 => {
            let days = cast(input.as_ref(), &DataType::Date32)?;
            let days = days.as_primitive::<Date32Type>();
            let mut builder = TimestampMicrosecondArray::builder(days.len());
            for row in 0..days.len() {
                if days.is_null(row) {
                    builder.append_null();
                    continue;
                }
                match Date32Type::to_naive_date_opt(days.value(row)) {
                    Some(date) => match date.and_hms_opt(0, 0, 0) {
                        Some(midnight) => {
                            builder.append_value(midnight.and_utc().timestamp_micros());
                        }
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        DataType::Timestamp(_, timezone) => {
            if timezone.is_some() {
                let instants = cast(input.as_ref(), &ltz_timestamp_type())?;
                Ok(ColumnarValue::Array(unlocalize_to_walls(&instants, zone)?))
            } else {
                Ok(ColumnarValue::Array(cast(
                    input.as_ref(),
                    &ntz_timestamp_type(),
                )?))
            }
        }
        _ => {
            let arrays = ColumnarValue::values_to_arrays(std::slice::from_ref(
                &forward_to_timestamp(Arc::clone(input), args)
                    .map_err(|error| retarget_to_ntz(&error))?,
            ))?;
            Ok(ColumnarValue::Array(unlocalize_to_walls(&arrays[0], zone)?))
        }
    }
}

fn utf8_strings(array: &ArrayRef) -> Result<StringArray> {
    let casted = cast(array.as_ref(), &DataType::Utf8)?;
    casted
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("utf8 cast did not yield Utf8".to_string()))
}

fn walls_with_format(
    input: &ArrayRef,
    formats: &ArrayRef,
    ansi: bool,
) -> Result<Vec<Option<NaiveDateTime>>> {
    let texts = utf8_strings(input)?;
    let format_texts = utf8_strings(formats)?;
    match plan_format_column(&format_texts)? {
        FormatPlan::AllNull => Ok(vec![None; texts.len()]),
        FormatPlan::Shared(pattern) => {
            let mut walls = Vec::with_capacity(texts.len());
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    walls.push(None);
                    continue;
                }
                walls.push(parse_wall_or_null(
                    texts.value(row),
                    &pattern,
                    ansi,
                    "try_to_timestamp",
                )?);
            }
            Ok(walls)
        }
        FormatPlan::PerRow => {
            let mut walls = Vec::with_capacity(texts.len());
            for row in 0..texts.len() {
                if texts.is_null(row) || format_texts.is_null(row) {
                    walls.push(None);
                    continue;
                }
                let pattern = compile_java_pattern(format_texts.value(row))?;
                walls.push(parse_wall_or_null(
                    texts.value(row),
                    &pattern,
                    ansi,
                    "try_to_timestamp",
                )?);
            }
            Ok(walls)
        }
    }
}

fn ntz_with_format(
    input: &ArrayRef,
    formats: &ArrayRef,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let ansi = spark_ansi_enabled_from_options(args.config_options.as_ref());
    let walls = walls_with_format(input, formats, ansi)?;
    let mut builder = TimestampMicrosecondArray::builder(walls.len());
    for wall in walls {
        match wall {
            Some(naive) => builder.append_value(naive.and_utc().timestamp_micros()),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_at(zone: &str) -> SessionContext {
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    fn ticks(table: &datafusion::arrow::record_batch::RecordBatch) -> i64 {
        table
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0)
    }

    #[tokio::test]
    async fn ntz_date_only_string_parses_midnight() {
        let midnight = chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
            .expect("midnight date")
            .and_hms_opt(0, 0, 0)
            .expect("midnight time")
            .and_utc()
            .timestamp_micros();
        let ctx = ctx_at("UTC");
        let table = ctx
            .sql("SELECT to_timestamp_ntz('2020-01-01') AS ts")
            .await
            .expect("ntz date-only")
            .collect()
            .await
            .expect("collect");
        assert_eq!(ticks(&table[0]), midnight);
    }

    #[tokio::test]
    async fn ltz_matches_to_timestamp_and_stays_ltz() {
        let ctx = ctx_at("America/New_York");
        let ltz = ctx
            .sql("SELECT to_timestamp_ltz('2016-12-31 00:12:00') AS ts")
            .await
            .expect("ltz")
            .collect()
            .await
            .expect("collect");
        let plain = ctx
            .sql("SELECT to_timestamp('2016-12-31 00:12:00') AS ts")
            .await
            .expect("plain")
            .collect()
            .await
            .expect("collect");
        assert_eq!(ltz[0].schema().field(0).data_type(), &ltz_timestamp_type());
        assert_eq!(ticks(&ltz[0]), ticks(&plain[0]));
        assert_eq!(ticks(&ltz[0]), 1_483_161_120_000_000);
    }

    #[tokio::test]
    async fn ntz_keeps_the_wall_in_every_session_zone() {
        for zone in ["UTC", "America/New_York"] {
            let ctx = ctx_at(zone);
            let batches = ctx
                .sql("SELECT to_timestamp_ntz('2016-12-31 00:12:00') AS ts")
                .await
                .expect("ntz")
                .collect()
                .await
                .expect("collect");
            assert_eq!(
                batches[0].schema().field(0).data_type(),
                &ntz_timestamp_type(),
                "{zone}"
            );
            assert_eq!(ticks(&batches[0]), 1_483_143_120_000_000, "{zone}");
        }
    }

    #[tokio::test]
    async fn ntz_parses_java_patterns_without_a_zone_shift() {
        let ctx = ctx_at("America/New_York");
        let batches = ctx
            .sql("SELECT to_timestamp_ntz('31/12/2016 10:30', 'dd/MM/yyyy HH:mm') AS ts")
            .await
            .expect("ntz")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type()
        );
        assert_eq!(ticks(&batches[0]), 1_483_180_200_000_000);
    }

    #[tokio::test]
    async fn ntz_drops_a_numeric_offset_and_keeps_the_wall() {
        let ctx = ctx_at("UTC");
        let batches = ctx
            .sql("SELECT to_timestamp_ntz('2016-12-31 00:12:00+02:00') AS ts")
            .await
            .expect("ntz")
            .collect()
            .await
            .expect("collect");
        assert_eq!(ticks(&batches[0]), 1_483_143_120_000_000);
    }

    #[tokio::test]
    async fn ltz_malformed_names_cast_invalid_input_under_ansi() {
        let ctx = ctx_at("UTC");
        let failure = ctx
            .sql("SELECT to_timestamp_ltz('garbage') AS ts")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("ansi garbage raises");
        assert!(
            failure.to_string().contains("[CAST_INVALID_INPUT]"),
            "{failure}"
        );
    }

    #[tokio::test]
    async fn ntz_malformed_names_the_ntz_target_under_ansi() {
        let ctx = ctx_at("UTC");
        let failure = ctx
            .sql("SELECT to_timestamp_ntz('garbage') AS ts")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("ansi garbage raises");
        let text = failure.to_string();
        assert!(text.contains("[CAST_INVALID_INPUT]"), "{text}");
        assert!(text.contains("\"TIMESTAMP_NTZ\""), "{text}");
    }

    #[tokio::test]
    async fn try_answers_null_on_garbage_under_ansi_on() {
        let ctx = ctx_at("UTC");
        for sql in [
            "SELECT try_to_timestamp('garbage') AS ts",
            "SELECT try_to_timestamp('2016-12-31', 'yyyy-MM-dd HH') AS ts",
        ] {
            let batches = ctx
                .sql(sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect("collect");
            assert_eq!(
                batches[0].schema().field(0).data_type(),
                &ltz_timestamp_type(),
                "{sql}"
            );
            assert!(batches[0].column(0).is_null(0), "{sql}");
        }
    }

    #[tokio::test]
    async fn try_keeps_valid_rows_beside_garbage() {
        let ctx = ctx_at("UTC");
        let batches = ctx
            .sql(
                "SELECT try_to_timestamp(s) AS ts FROM (VALUES ('2016-12-31 00:12:00'), \
                 ('garbage'), (NULL)) AS v(s)",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let micros = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(micros.value(0), 1_483_143_120_000_000);
        assert!(micros.is_null(1));
        assert!(micros.is_null(2));
    }

    #[tokio::test]
    async fn ntz_without_ansi_keeps_valid_rows_beside_garbage() {
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), "UTC");
        let config = crate::ansi::with_spark_ansi_config(config, false);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        let batches = ctx
            .sql(
                "SELECT to_timestamp_ntz(s) AS ts FROM (VALUES ('2016-12-31 00:12:00'), \
                 ('garbage'), (NULL)) AS v(s)",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type()
        );
        let micros = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(micros.value(0), 1_483_143_120_000_000);
        assert!(micros.is_null(1));
        assert!(micros.is_null(2));
    }

    #[tokio::test]
    async fn try_matches_to_timestamp_on_valid_input() {
        let ctx = ctx_at("America/New_York");
        let tried = ctx
            .sql("SELECT try_to_timestamp('31/12/2016 10:30', 'dd/MM/yyyy HH:mm') AS ts")
            .await
            .expect("try")
            .collect()
            .await
            .expect("collect");
        let plain = ctx
            .sql("SELECT to_timestamp('31/12/2016 10:30', 'dd/MM/yyyy HH:mm') AS ts")
            .await
            .expect("plain")
            .collect()
            .await
            .expect("collect");
        assert_eq!(ticks(&tried[0]), ticks(&plain[0]));
    }
}
