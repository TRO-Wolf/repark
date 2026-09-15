use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, TimeZone};
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use futures::StreamExt;

use crate::text_io::text_unsupported_column;
use crate::{Error, Result, engine_err};

const TEXT_PARTITION_WRITERS_CAP: usize = 256;

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

fn escape_partition_value(text: &str) -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out: Vec<u8> = Vec::with_capacity(text.len());
    for byte in text.bytes() {
        let escape = matches!(
            byte,
            b'"' | b'#'
                | b'%'
                | b'\''
                | b'*'
                | b'/'
                | b':'
                | b'='
                | b'?'
                | b'\\'
                | b'{'
                | b'['
                | b']'
                | b'^'
                | 0x00..=0x1F | 0x7F
        );
        if escape {
            out.push(b'%');
            out.push(HEX[usize::from(byte >> 4)]);
            out.push(HEX[usize::from(byte & 15)]);
        } else {
            out.push(byte);
        }
    }
    String::from_utf8(out).map_err(|error| {
        engine_err(DataFusionError::Internal(format!(
            "text partition escape produced invalid UTF-8: {error}"
        )))
    })
}

pub(crate) fn decimal_plain_text(scaled: i128, scale: i8) -> Result<String> {
    if scale <= 0 {
        let mut text = scaled.to_string();
        for _ in scale..0 {
            text.push('0');
        }
        return Ok(text);
    }
    let scale = usize::try_from(scale).map_err(|_| {
        engine_err(DataFusionError::Internal(format!(
            "text partition decimal scale out of range: {scale}"
        )))
    })?;
    let digits = scaled.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{:0>width$}", digits, width = scale + 1)
    } else {
        digits
    };
    let (head, tail) = padded.split_at(padded.len() - scale);
    let mut text = String::with_capacity(padded.len() + 2);
    if scaled < 0 {
        text.push('-');
    }
    text.push_str(head);
    text.push('.');
    text.push_str(tail);
    Ok(text)
}

fn date_text_from_days(days: i32) -> Result<String> {
    let absolute = i64::from(days) + 719_163;
    let within: i32 = absolute.try_into().map_err(|_| {
        engine_err(DataFusionError::Internal(format!(
            "text partition date out of range: {days} days"
        )))
    })?;
    NaiveDate::from_num_days_from_ce_opt(within)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| {
            engine_err(DataFusionError::Internal(format!(
                "text partition date out of range: {days} days"
            )))
        })
}

#[allow(clippy::cast_possible_truncation)]
fn timestamp_partition_text(value: i64, unit: TimeUnit, zone: Tz) -> Result<String> {
    let (secs, nanos) = match unit {
        TimeUnit::Second => (value, 0),
        TimeUnit::Millisecond => (
            value.div_euclid(1000),
            (value.rem_euclid(1000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            value.div_euclid(1_000_000),
            (value.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            value.div_euclid(1_000_000_000),
            value.rem_euclid(1_000_000_000) as u32,
        ),
    };
    let naive = chrono::DateTime::from_timestamp(secs, nanos)
        .map(|zoned| zoned.naive_utc())
        .ok_or_else(|| {
            engine_err(DataFusionError::Internal(
                "text partition timestamp out of range".to_string(),
            ))
        })?;
    let mut text = zone
        .from_utc_datetime(&naive)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    if nanos != 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        text.push('.');
        text.push_str(&fraction);
    }
    Ok(text)
}

enum PartitionColumnRef<'a> {
    Text(&'a StringArray),
    LargeText(&'a LargeStringArray),
    ViewText(&'a StringViewArray),
    Bool(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal32(&'a Decimal32Array, i8),
    Decimal64(&'a Decimal64Array, i8),
    Decimal128(&'a Decimal128Array, i8),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMillisecond(&'a TimestampMillisecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
}

macro_rules! downcast_partition_column {
    ($column:expr, $array:ty, $label:literal) => {
        $column.as_any().downcast_ref::<$array>().ok_or_else(|| {
            engine_err(DataFusionError::Internal(format!(
                "text partition column is not a {} array",
                $label
            )))
        })?
    };
}

fn resolve_partition_column<'a>(
    column: &'a dyn Array,
    name: &str,
    data_type: &DataType,
) -> Result<PartitionColumnRef<'a>> {
    let unsupported = || text_unsupported_column(name, data_type);
    match data_type {
        DataType::Utf8 => Ok(PartitionColumnRef::Text(downcast_partition_column!(
            column,
            StringArray,
            "Utf8"
        ))),
        DataType::LargeUtf8 => Ok(PartitionColumnRef::LargeText(downcast_partition_column!(
            column,
            LargeStringArray,
            "LargeUtf8"
        ))),
        DataType::Utf8View => Ok(PartitionColumnRef::ViewText(downcast_partition_column!(
            column,
            StringViewArray,
            "Utf8View"
        ))),
        DataType::Boolean => Ok(PartitionColumnRef::Bool(downcast_partition_column!(
            column,
            BooleanArray,
            "Boolean"
        ))),
        DataType::Int8 => Ok(PartitionColumnRef::Int8(downcast_partition_column!(
            column, Int8Array, "Int8"
        ))),
        DataType::Int16 => Ok(PartitionColumnRef::Int16(downcast_partition_column!(
            column, Int16Array, "Int16"
        ))),
        DataType::Int32 => Ok(PartitionColumnRef::Int32(downcast_partition_column!(
            column, Int32Array, "Int32"
        ))),
        DataType::Int64 => Ok(PartitionColumnRef::Int64(downcast_partition_column!(
            column, Int64Array, "Int64"
        ))),
        DataType::UInt8 => Ok(PartitionColumnRef::UInt8(downcast_partition_column!(
            column, UInt8Array, "UInt8"
        ))),
        DataType::UInt16 => Ok(PartitionColumnRef::UInt16(downcast_partition_column!(
            column,
            UInt16Array,
            "UInt16"
        ))),
        DataType::UInt32 => Ok(PartitionColumnRef::UInt32(downcast_partition_column!(
            column,
            UInt32Array,
            "UInt32"
        ))),
        DataType::UInt64 => Ok(PartitionColumnRef::UInt64(downcast_partition_column!(
            column,
            UInt64Array,
            "UInt64"
        ))),
        DataType::Float32 => Ok(PartitionColumnRef::Float32(downcast_partition_column!(
            column,
            Float32Array,
            "Float32"
        ))),
        DataType::Float64 => Ok(PartitionColumnRef::Float64(downcast_partition_column!(
            column,
            Float64Array,
            "Float64"
        ))),
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(..) => resolve_partition_temporal(column, name, data_type),
        _ => Err(unsupported()),
    }
}

fn resolve_partition_temporal<'a>(
    column: &'a dyn Array,
    name: &str,
    data_type: &DataType,
) -> Result<PartitionColumnRef<'a>> {
    let unsupported = || text_unsupported_column(name, data_type);
    match data_type {
        DataType::Decimal32(_, scale) => Ok(PartitionColumnRef::Decimal32(
            downcast_partition_column!(column, Decimal32Array, "Decimal32"),
            *scale,
        )),
        DataType::Decimal64(_, scale) => Ok(PartitionColumnRef::Decimal64(
            downcast_partition_column!(column, Decimal64Array, "Decimal64"),
            *scale,
        )),
        DataType::Decimal128(_, scale) => Ok(PartitionColumnRef::Decimal128(
            downcast_partition_column!(column, Decimal128Array, "Decimal128"),
            *scale,
        )),
        DataType::Date32 => Ok(PartitionColumnRef::Date32(downcast_partition_column!(
            column,
            Date32Array,
            "Date32"
        ))),
        DataType::Date64 => Ok(PartitionColumnRef::Date64(downcast_partition_column!(
            column,
            Date64Array,
            "Date64"
        ))),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(PartitionColumnRef::TimestampSecond(
            downcast_partition_column!(column, TimestampSecondArray, "Timestamp"),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(PartitionColumnRef::TimestampMillisecond(
                downcast_partition_column!(column, TimestampMillisecondArray, "Timestamp"),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(PartitionColumnRef::TimestampMicrosecond(
                downcast_partition_column!(column, TimestampMicrosecondArray, "Timestamp"),
            ))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(PartitionColumnRef::TimestampNanosecond(
                downcast_partition_column!(column, TimestampNanosecondArray, "Timestamp"),
            ))
        }
        _ => Err(unsupported()),
    }
}

macro_rules! render_row {
    ($values:expr, $row:expr, $render:expr) => {
        if $values.is_null($row) {
            Ok(None)
        } else {
            Ok(Some($render($values.value($row))))
        }
    };
}

fn render_partition_value(
    column: &PartitionColumnRef<'_>,
    row: usize,
    zone: Tz,
) -> Result<Option<String>> {
    match column {
        PartitionColumnRef::Text(values) => render_row!(values, row, |text: &str| text.to_string()),
        PartitionColumnRef::LargeText(values) => {
            render_row!(values, row, |text: &str| text.to_string())
        }
        PartitionColumnRef::ViewText(values) => {
            render_row!(values, row, |text: &str| text.to_string())
        }
        PartitionColumnRef::Bool(values) => render_row!(values, row, |flag: bool| {
            if flag {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }),
        PartitionColumnRef::Int8(values) => render_row!(values, row, |value: i8| value.to_string()),
        PartitionColumnRef::Int16(values) => {
            render_row!(values, row, |value: i16| value.to_string())
        }
        PartitionColumnRef::Int32(values) => {
            render_row!(values, row, |value: i32| value.to_string())
        }
        PartitionColumnRef::Int64(values) => {
            render_row!(values, row, |value: i64| value.to_string())
        }
        PartitionColumnRef::UInt8(values) => {
            render_row!(values, row, |value: u8| value.to_string())
        }
        PartitionColumnRef::UInt16(values) => {
            render_row!(values, row, |value: u16| value.to_string())
        }
        PartitionColumnRef::UInt32(values) => {
            render_row!(values, row, |value: u32| value.to_string())
        }
        PartitionColumnRef::UInt64(values) => {
            render_row!(values, row, |value: u64| value.to_string())
        }
        PartitionColumnRef::Float32(values) => {
            render_row!(values, row, |value: f32| value.to_string())
        }
        PartitionColumnRef::Float64(values) => {
            render_row!(values, row, |value: f64| value.to_string())
        }
        PartitionColumnRef::Decimal32(values, scale) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                decimal_plain_text(i128::from(values.value(row)), *scale).map(Some)
            }
        }
        PartitionColumnRef::Decimal64(values, scale) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                decimal_plain_text(i128::from(values.value(row)), *scale).map(Some)
            }
        }
        PartitionColumnRef::Decimal128(values, scale) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                decimal_plain_text(values.value(row), *scale).map(Some)
            }
        }
        PartitionColumnRef::Date32(_)
        | PartitionColumnRef::Date64(_)
        | PartitionColumnRef::TimestampSecond(_)
        | PartitionColumnRef::TimestampMillisecond(_)
        | PartitionColumnRef::TimestampMicrosecond(_)
        | PartitionColumnRef::TimestampNanosecond(_) => {
            render_partition_temporal(column, row, zone)
        }
    }
}

fn render_partition_temporal(
    column: &PartitionColumnRef<'_>,
    row: usize,
    zone: Tz,
) -> Result<Option<String>> {
    match column {
        PartitionColumnRef::Date32(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                date_text_from_days(values.value(row)).map(Some)
            }
        }
        PartitionColumnRef::Date64(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                let days = values.value(row).div_euclid(86_400_000);
                let days = i32::try_from(days).map_err(|_| {
                    engine_err(DataFusionError::Internal(format!(
                        "text partition date out of range: {days} millis"
                    )))
                })?;
                date_text_from_days(days).map(Some)
            }
        }
        PartitionColumnRef::TimestampSecond(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                timestamp_partition_text(values.value(row), TimeUnit::Second, zone).map(Some)
            }
        }
        PartitionColumnRef::TimestampMillisecond(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                timestamp_partition_text(values.value(row), TimeUnit::Millisecond, zone).map(Some)
            }
        }
        PartitionColumnRef::TimestampMicrosecond(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                timestamp_partition_text(values.value(row), TimeUnit::Microsecond, zone).map(Some)
            }
        }
        PartitionColumnRef::TimestampNanosecond(values) => {
            if values.is_null(row) {
                Ok(None)
            } else {
                timestamp_partition_text(values.value(row), TimeUnit::Nanosecond, zone).map(Some)
            }
        }
        _ => Err(engine_err(DataFusionError::Internal(
            "text partition temporal render reached a non-temporal column".to_string(),
        ))),
    }
}

fn partition_write_layout(
    schema: &DFSchema,
    partition_columns: &[String],
) -> Result<(Vec<usize>, usize, DataType)> {
    let mut partition_at: Vec<usize> = Vec::with_capacity(partition_columns.len());
    for name in partition_columns {
        let Some((index, _)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == name)
        else {
            return Err(Error::Analysis(format!(
                "text partition column {name:?} is not in the DataFrame columns"
            )));
        };
        partition_at.push(index);
    }
    let selected: HashSet<usize> = partition_at.iter().copied().collect();
    let mut body_at: Option<usize> = None;
    let mut remaining = 0usize;
    for index in 0..schema.fields().len() {
        if !selected.contains(&index) {
            remaining += 1;
            body_at = Some(index);
        }
    }
    let Some(body_at) = body_at else {
        return Err(Error::Analysis(
            "Text data source supports only a single column, and you have 0 columns.".to_string(),
        ));
    };
    if remaining != 1 {
        return Err(Error::Analysis(format!(
            "Text data source supports only a single column, and you have {remaining} columns."
        )));
    }
    let body_type = schema.fields()[body_at].data_type().clone();
    if !matches!(
        body_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(text_unsupported_column(
            schema.fields()[body_at].name(),
            &body_type,
        ));
    }
    Ok((partition_at, body_at, body_type))
}

pub(crate) fn render_partition_key(
    batch: &RecordBatch,
    row: usize,
    partition_at: &[usize],
    partition_columns: &[String],
    zone: Tz,
) -> Result<Vec<String>> {
    let mut key: Vec<String> = Vec::with_capacity(partition_at.len());
    for (position, column_at) in partition_at.iter().enumerate() {
        let column = batch.column(*column_at);
        let resolved = resolve_partition_column(
            column.as_ref(),
            &partition_columns[position],
            column.data_type(),
        )?;
        let raw = render_partition_value(&resolved, row, zone)?;
        let segment = match raw {
            None => format!("{}={HIVE_DEFAULT_PARTITION}", partition_columns[position]),
            Some(text) if text.is_empty() => {
                format!("{}={HIVE_DEFAULT_PARTITION}", partition_columns[position])
            }
            Some(text) => {
                format!(
                    "{}={}",
                    partition_columns[position],
                    escape_partition_value(&text)?
                )
            }
        };
        key.push(segment);
    }
    Ok(key)
}

fn flush_open_writers(open: &mut HashMap<Vec<String>, (BufWriter<File>, PathBuf)>) -> Result<()> {
    for (_, (mut writer, part)) in open.drain() {
        writer.flush().map_err(|error| {
            Error::Analysis(format!("text write to {} failed: {error}", part.display()))
        })?;
    }
    Ok(())
}

pub(crate) fn partition_leaf_writer<'a>(
    open: &'a mut HashMap<Vec<String>, (BufWriter<File>, PathBuf)>,
    touched: &mut HashMap<Vec<String>, u64>,
    parts: &mut HashMap<Vec<String>, PathBuf>,
    created: &mut HashSet<Vec<String>>,
    dir: &Path,
    key: &[String],
    tick: u64,
) -> Result<&'a mut (BufWriter<File>, PathBuf)> {
    if !open.contains_key(key) {
        if open.len() >= TEXT_PARTITION_WRITERS_CAP {
            let victim = touched
                .iter()
                .min_by_key(|(_, used)| **used)
                .map(|(key, _)| key.clone())
                .ok_or_else(|| {
                    engine_err(DataFusionError::Internal(
                        "text partition writer set is empty".to_string(),
                    ))
                })?;
            if let Some((mut writer, _)) = open.remove(&victim) {
                writer.flush().map_err(|error| {
                    Error::Analysis(format!("text partition write failed: {error}"))
                })?;
            }
            touched.remove(&victim);
        }
        if let Some(part) = parts.get(key) {
            let file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(part)
                .map_err(|error| {
                    Error::Analysis(format!(
                        "text write cannot append {}: {error}",
                        part.display()
                    ))
                })?;
            open.insert(key.to_owned(), (BufWriter::new(file), part.clone()));
        } else {
            let mut leaf = dir.to_path_buf();
            for segment in key {
                leaf.push(segment);
            }
            if !created.contains(key) {
                std::fs::create_dir_all(&leaf).map_err(|error| {
                    Error::Analysis(format!(
                        "text write cannot create directory {}: {error}",
                        leaf.display()
                    ))
                })?;
                created.insert(key.to_owned());
            }
            let part = leaf.join("part-00000.txt");
            let file = File::create(&part).map_err(|error| {
                Error::Analysis(format!(
                    "text write cannot create {}: {error}",
                    part.display()
                ))
            })?;
            parts.insert(key.to_owned(), part.clone());
            open.insert(key.to_owned(), (BufWriter::new(file), part));
        }
    }
    touched.insert(key.to_owned(), tick);
    open.get_mut(key).ok_or_else(|| {
        engine_err(DataFusionError::Internal(
            "text partition writer vanished".to_string(),
        ))
    })
}

#[allow(clippy::missing_errors_doc)]
pub async fn write_text_partitioned(
    frame: &DataFrame,
    dir: &Path,
    line_sep: &str,
    partition_columns: &[String],
    session_zone: &str,
) -> Result<()> {
    let (partition_at, body_at, body_type) =
        partition_write_layout(frame.schema(), partition_columns)?;
    if line_sep.is_empty() {
        return Err(Error::IllegalArgument(
            "requirement failed: 'lineSep' cannot be an empty string.".to_string(),
        ));
    }
    let zone: Tz = session_zone.parse().map_err(|error| {
        Error::Analysis(format!(
            "text partition write cannot parse session time zone {session_zone:?}: {error}"
        ))
    })?;
    std::fs::create_dir_all(dir).map_err(|error| {
        Error::Analysis(format!(
            "text write cannot create directory {}: {error}",
            dir.display()
        ))
    })?;
    let separator = line_sep.as_bytes();
    let mut stream = frame.clone().execute_stream().await.map_err(engine_err)?;
    let mut open: HashMap<Vec<String>, (BufWriter<File>, PathBuf)> = HashMap::new();
    let mut touched: HashMap<Vec<String>, u64> = HashMap::new();
    let mut parts: HashMap<Vec<String>, PathBuf> = HashMap::new();
    let mut created: HashSet<Vec<String>> = HashSet::new();
    let mut tick = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(engine_err)?;
        for row in 0..batch.num_rows() {
            let key = render_partition_key(&batch, row, &partition_at, partition_columns, zone)?;
            if !open.contains_key(&key) && open.len() >= TEXT_PARTITION_WRITERS_CAP {
                flush_open_writers(&mut open)?;
                let head = batch.slice(row, batch.num_rows() - row);
                let _spilled = crate::text_partition_fallback::append_remaining_sorted(
                    head,
                    stream,
                    crate::text_partition_fallback::PartitionTail {
                        partition_columns,
                        partition_at: &partition_at,
                        body_at,
                        body_type: &body_type,
                        dir,
                        separator,
                        parts: &mut parts,
                        created: &mut created,
                        zone,
                    },
                    frame.task_ctx(),
                )
                .await?;
                return Ok(());
            }
            tick += 1;
            let (writer, part) = partition_leaf_writer(
                &mut open,
                &mut touched,
                &mut parts,
                &mut created,
                dir,
                &key,
                tick,
            )?;
            write_partition_body_row(writer, &batch, body_at, &body_type, row, part, separator)?;
        }
    }
    flush_open_writers(&mut open)?;
    Ok(())
}

pub(crate) fn write_partition_body_row(
    writer: &mut BufWriter<File>,
    batch: &RecordBatch,
    body_at: usize,
    body_type: &DataType,
    row: usize,
    part: &Path,
    separator: &[u8],
) -> Result<()> {
    let column = batch.column(body_at);
    let present = !column.is_null(row);
    if present {
        match body_type {
            DataType::Utf8 => {
                let values = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        engine_err(DataFusionError::Internal(
                            "text write column is not a Utf8 array".to_string(),
                        ))
                    })?;
                writer
                    .write_all(values.value(row).as_bytes())
                    .map_err(|error| {
                        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                    })?;
            }
            DataType::LargeUtf8 => {
                let values = column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        engine_err(DataFusionError::Internal(
                            "text write column is not a LargeUtf8 array".to_string(),
                        ))
                    })?;
                writer
                    .write_all(values.value(row).as_bytes())
                    .map_err(|error| {
                        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                    })?;
            }
            DataType::Utf8View => {
                let values = column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        engine_err(DataFusionError::Internal(
                            "text write column is not a Utf8View array".to_string(),
                        ))
                    })?;
                writer
                    .write_all(values.value(row).as_bytes())
                    .map_err(|error| {
                        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
                    })?;
            }
            _ => {
                return Err(engine_err(DataFusionError::Internal(
                    "text partition body column changed type mid-stream".to_string(),
                )));
            }
        }
    }
    writer.write_all(separator).map_err(|error| {
        Error::Analysis(format!("text write to {} failed: {error}", part.display()))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session() -> crate::ReparkSession {
        crate::ReparkSession::builder().build().unwrap()
    }

    #[tokio::test]
    async fn text_partition_escape_covers_hive_set() {
        assert_eq!(
            escape_partition_value("a/b=c:d%e f").unwrap(),
            "a%2Fb%3Dc%3Ad%25e f"
        );
        assert_eq!(
            escape_partition_value("e#f?g*h\\i\"j'k{l}m[n]o^p").unwrap(),
            "e%23f%3Fg%2Ah%5Ci%22j%27k%7Bl}m%5Bn%5Do%5Ep"
        );
        assert_eq!(escape_partition_value("plainé}").unwrap(), "plainé}");
        assert_eq!(escape_partition_value("a\x01b\x7F").unwrap(), "a%01b%7F");
    }

    #[test]
    fn text_partition_decimal_renders_plain() {
        assert_eq!(decimal_plain_text(150, 2).unwrap(), "1.50");
        assert_eq!(decimal_plain_text(-5, 2).unwrap(), "-0.05");
        assert_eq!(decimal_plain_text(7, 0).unwrap(), "7");
    }

    #[tokio::test]
    async fn text_partition_write_fans_out_one_scan() {
        let session = test_session();
        let frame = session
            .sql("SELECT * FROM (VALUES ('a/b', 'v1'), ('c', 'v2'), (NULL, 'v3'), ('', 'v4')) AS t(k, value)")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        write_text_partitioned(&frame, &target, "\n", &[String::from("k")], "UTC")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("k=a%2Fb").join("part-00000.txt")).unwrap(),
            "v1\n"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("k=c").join("part-00000.txt")).unwrap(),
            "v2\n"
        );
        let default = target
            .join("k=__HIVE_DEFAULT_PARTITION__")
            .join("part-00000.txt");
        assert_eq!(std::fs::read_to_string(&default).unwrap(), "v3\nv4\n");
    }

    #[tokio::test]
    async fn text_partition_write_refuses_two_remaining() {
        let session = test_session();
        let frame = session
            .sql("SELECT 'x' AS k, 'a' AS value, 'b' AS extra")
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        let Err(error) =
            write_text_partitioned(&frame, &target, "\n", &[String::from("k")], "UTC").await
        else {
            panic!("a two-remaining frame must refuse the partitioned text write");
        };
        assert_eq!(
            error.to_string(),
            "Text data source supports only a single column, and you have 2 columns."
        );
        assert!(!target.exists());
    }

    #[test]
    fn text_partition_leaf_writer_appends_on_evict() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        std::fs::create_dir_all(&target).unwrap();
        let mut open: HashMap<Vec<String>, (BufWriter<File>, PathBuf)> = HashMap::new();
        let mut touched: HashMap<Vec<String>, u64> = HashMap::new();
        let mut parts: HashMap<Vec<String>, PathBuf> = HashMap::new();
        let mut created: HashSet<Vec<String>> = HashSet::new();
        for index in 0..TEXT_PARTITION_WRITERS_CAP {
            let key = vec![format!("k=k{index}")];
            partition_leaf_writer(
                &mut open,
                &mut touched,
                &mut parts,
                &mut created,
                &target,
                &key,
                u64::try_from(index).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(open.len(), TEXT_PARTITION_WRITERS_CAP);
        let first = vec![String::from("k=k0")];
        let extra = vec![String::from("k=extra")];
        partition_leaf_writer(
            &mut open,
            &mut touched,
            &mut parts,
            &mut created,
            &target,
            &extra,
            1000,
        )
        .unwrap();
        assert!(!open.contains_key(&first));
        let before = parts[&first].clone();
        let (writer, part) = partition_leaf_writer(
            &mut open,
            &mut touched,
            &mut parts,
            &mut created,
            &target,
            &first,
            1001,
        )
        .unwrap();
        assert_eq!(*part, before);
        writer.write_all(b"resumed\n").unwrap();
        for (_, (mut writer, _)) in open.drain() {
            writer.flush().unwrap();
        }
        assert_eq!(std::fs::read_to_string(&before).unwrap(), "resumed\n");
    }

    #[tokio::test]
    async fn text_partition_evicted_key_appends_to_same_part() {
        let session = test_session();
        let mut values: Vec<String> = Vec::new();
        for round in 0..4 {
            for key in 0..300 {
                values.push(format!("('k{key}', 'r{round}-{key}')"));
            }
        }
        let query = format!(
            "SELECT * FROM (VALUES {}) AS t(k, value)",
            values.join(", ")
        );
        let frame = session.sql(&query).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out");
        write_text_partitioned(&frame, &target, "\n", &[String::from("k")], "UTC")
            .await
            .unwrap();
        let mut leaves = 0usize;
        let mut files = 0usize;
        let mut rows = 0usize;
        for entry in std::fs::read_dir(&target).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_dir() {
                continue;
            }
            leaves += 1;
            let mut parts = 0usize;
            for leaf in std::fs::read_dir(entry.path()).unwrap() {
                let leaf = leaf.unwrap();
                if leaf.path().extension().is_some_and(|ext| ext == "txt") {
                    parts += 1;
                    let body = std::fs::read_to_string(leaf.path()).unwrap();
                    rows += body.lines().count();
                }
            }
            assert_eq!(parts, 1);
            files += parts;
        }
        assert_eq!(leaves, 300);
        assert_eq!(files, 300);
        assert_eq!(rows, 1200);
    }
}
