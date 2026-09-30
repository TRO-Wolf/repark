use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    NullBufferBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
};
use chrono::{FixedOffset, NaiveDateTime};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::fast::{DateDefaultState, TimestampDefaultState, wall_micros_bounds};
use super::render::{
    RenderValue, render_compiled_into, render_date_default_into, render_ntz_default_into,
    render_timestamp_default_into,
};
use super::{
    CompiledPattern, PatternKind, compile_write_pattern, micros_to_naive_wall, micros_to_wall_zone,
    pattern_failure_datafusion,
};
use crate::session_time_zone::{
    DEFAULT_SESSION_TIME_ZONE, canonical_session_zone_id, java_display_zone_id,
};

pub const WRITE_FORMAT_FUNCTION: &str = "repark_write_format_text";

const MICROS_PER_SECOND: i64 = 1_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;
const NANOS_PER_MICRO: u32 = 1_000;

enum FormatSpec {
    Default,
    Compiled(CompiledPattern),
}

struct FormatSpecs {
    timestamp: FormatSpec,
    ntz: FormatSpec,
    date: FormatSpec,
}

fn spec_from_value(text: Option<&str>, kind: PatternKind) -> Result<FormatSpec> {
    match text {
        None => Ok(FormatSpec::Default),
        Some(pattern) => compile_write_pattern(pattern, kind)
            .map(FormatSpec::Compiled)
            .map_err(|failure| pattern_failure_datafusion(&failure)),
    }
}

fn halve_backslashes(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(current) = characters.next() {
        if current == '\\' && characters.peek() == Some(&'\\') {
            characters.next();
        }
        output.push(current);
    }
    output
}

fn fraction_nanos(micros: i64) -> u32 {
    u32::try_from(micros.rem_euclid(MICROS_PER_SECOND)).unwrap_or(0) * NANOS_PER_MICRO
}

pub(crate) const CACHED_WINDOW_MICROS: i64 = 900_000_000;

const WINDOW_HALF_MICROS: i64 = CACHED_WINDOW_MICROS / 2;
const SHARED_ZONE_LIMIT: usize = 64;

#[derive(Debug)]
pub(crate) struct OffsetCache {
    zone: Tz,
    offset: Option<FixedOffset>,
    start: i64,
    end: i64,
    wall_lowest: i64,
    wall_highest: i64,
}

impl OffsetCache {
    pub(crate) fn new(zone: Tz) -> Self {
        let (wall_lowest, wall_highest) = wall_micros_bounds();
        Self {
            zone,
            offset: None,
            start: 0,
            end: 0,
            wall_lowest,
            wall_highest,
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_span_micros(&self) -> Option<i64> {
        self.offset.map(|_| self.end - self.start)
    }

    fn prove_window(&mut self, micros: i64, offset: FixedOffset) {
        let start = micros.saturating_sub(WINDOW_HALF_MICROS);
        let end = micros.saturating_add(WINDOW_HALF_MICROS);
        let ends_hold = micros_to_wall_zone(start, self.zone)
            .is_some_and(|(_, found)| found == offset)
            && micros_to_wall_zone(end.saturating_sub(1), self.zone)
                .is_some_and(|(_, found)| found == offset);
        if ends_hold {
            self.offset = Some(offset);
            self.start = start;
            self.end = end;
        } else {
            self.offset = None;
        }
    }

    fn refresh(&mut self, micros: i64) -> Option<(NaiveDateTime, FixedOffset)> {
        let (wall, offset) = micros_to_wall_zone(micros, self.zone)?;
        self.prove_window(micros, offset);
        Some((wall, offset))
    }

    pub(crate) fn resolve(&mut self, micros: i64) -> Option<(NaiveDateTime, FixedOffset)> {
        if let Some(offset) = self.offset
            && micros >= self.start
            && micros < self.end
        {
            let shift = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
            if let Some(shifted) = micros.checked_add(shift)
                && let Some(wall) = chrono::DateTime::from_timestamp_micros(shifted)
                    .map(|instant| instant.naive_utc())
            {
                return Some((wall, offset));
            }
        }
        self.refresh(micros)
    }

    fn refresh_micros(&mut self, micros: i64) -> Option<(i64, FixedOffset)> {
        let (_, offset) = micros_to_wall_zone(micros, self.zone)?;
        let shift = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
        let shifted = micros.checked_add(shift)?;
        self.prove_window(micros, offset);
        Some((shifted, offset))
    }

    pub(crate) fn resolve_micros(&mut self, micros: i64) -> Option<(i64, FixedOffset)> {
        if let Some(offset) = self.offset
            && micros >= self.start
            && micros < self.end
        {
            let shift = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
            if let Some(shifted) = micros.checked_add(shift)
                && shifted >= self.wall_lowest
                && shifted <= self.wall_highest
                && micros >= self.wall_lowest
                && micros <= self.wall_highest
            {
                return Some((shifted, offset));
            }
        }
        self.refresh_micros(micros)
    }
}

#[derive(Debug)]
pub(crate) enum ZoneResolver {
    Fixed {
        offset: FixedOffset,
        wall_lowest: i64,
        wall_highest: i64,
    },
    Cached(OffsetCache),
}

impl ZoneResolver {
    pub(crate) fn fixed(offset: FixedOffset) -> Self {
        let (wall_lowest, wall_highest) = wall_micros_bounds();
        Self::Fixed {
            offset,
            wall_lowest,
            wall_highest,
        }
    }

    pub(crate) fn resolve(&mut self, micros: i64) -> Option<(NaiveDateTime, FixedOffset)> {
        match self {
            Self::Fixed { offset, .. } => {
                let shift = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
                let shifted = micros.checked_add(shift)?;
                let wall = chrono::DateTime::from_timestamp_micros(shifted)?.naive_utc();
                Some((wall, *offset))
            }
            Self::Cached(cache) => cache.resolve(micros),
        }
    }

    pub(crate) fn resolve_micros(&mut self, micros: i64) -> Option<(i64, FixedOffset)> {
        match self {
            Self::Fixed {
                offset,
                wall_lowest,
                wall_highest,
            } => {
                let shift = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
                let shifted = micros.checked_add(shift)?;
                if shifted < *wall_lowest
                    || shifted > *wall_highest
                    || micros < *wall_lowest
                    || micros > *wall_highest
                {
                    return None;
                }
                Some((shifted, *offset))
            }
            Self::Cached(cache) => cache.resolve_micros(micros),
        }
    }
}

fn fixed_session_offset(canonical: &str) -> Option<FixedOffset> {
    if canonical == DEFAULT_SESSION_TIME_ZONE {
        return FixedOffset::east_opt(0);
    }
    let bytes = canonical.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    if bytes[0] != b'+' && bytes[0] != b'-' {
        return None;
    }
    if ![bytes[1], bytes[2], bytes[4], bytes[5]]
        .iter()
        .all(u8::is_ascii_digit)
    {
        return None;
    }
    let hours = u32::from(bytes[1] - b'0') * 10 + u32::from(bytes[2] - b'0');
    let minutes = u32::from(bytes[4] - b'0') * 10 + u32::from(bytes[5] - b'0');
    let Ok(seconds) = i32::try_from(hours * 3600 + minutes * 60) else {
        return None;
    };
    if bytes[0] == b'+' {
        FixedOffset::east_opt(seconds)
    } else {
        FixedOffset::west_opt(seconds)
    }
}

fn format_instant_into(
    buffer: &mut String,
    micros: i64,
    resolver: &mut ZoneResolver,
    zone_id: &str,
    spec: &FormatSpec,
) -> Result<bool> {
    let Some((wall, offset)) = resolver.resolve(micros) else {
        return Ok(false);
    };
    let nanos = fraction_nanos(micros);
    match spec {
        FormatSpec::Default => {
            render_timestamp_default_into(buffer, &wall, nanos, offset);
            Ok(true)
        }
        FormatSpec::Compiled(compiled) => {
            let value = RenderValue::Instant {
                wall,
                nanos,
                offset,
                zone_id,
            };
            render_compiled_into(compiled, &value, buffer)
                .map(|()| true)
                .map_err(DataFusionError::Execution)
        }
    }
}

fn format_wall_into(buffer: &mut String, micros: i64, spec: &FormatSpec) -> Result<bool> {
    let Some(wall) = micros_to_naive_wall(micros) else {
        return Ok(false);
    };
    let nanos = fraction_nanos(micros);
    match spec {
        FormatSpec::Default => {
            render_ntz_default_into(buffer, &wall, nanos);
            Ok(true)
        }
        FormatSpec::Compiled(compiled) => {
            let value = RenderValue::Wall { wall, nanos };
            render_compiled_into(compiled, &value, buffer)
                .map(|()| true)
                .map_err(DataFusionError::Execution)
        }
    }
}

fn format_date_into(buffer: &mut String, days: i32, spec: &FormatSpec) -> Result<bool> {
    let Some(absolute) = days.checked_add(719_163) else {
        return Ok(false);
    };
    let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(absolute) else {
        return Ok(false);
    };
    match spec {
        FormatSpec::Default => {
            render_date_default_into(buffer, date);
            Ok(true)
        }
        FormatSpec::Compiled(compiled) => {
            let value = RenderValue::Date { date };
            render_compiled_into(compiled, &value, buffer)
                .map(|()| true)
                .map_err(DataFusionError::Execution)
        }
    }
}

fn map_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => DataType::Utf8,
        DataType::Struct(fields) => DataType::Struct(map_fields(fields)),
        DataType::List(field) => DataType::List(map_field(field)),
        DataType::LargeList(field) => DataType::LargeList(map_field(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(map_field(field), *size),
        DataType::Map(field, sorted) => {
            let DataType::Struct(entries) = field.data_type() else {
                return data_type.clone();
            };
            let mapped: Vec<Arc<Field>> = entries
                .iter()
                .enumerate()
                .map(|(position, entry)| {
                    if position == 1 {
                        map_field(entry)
                    } else {
                        Arc::clone(entry)
                    }
                })
                .collect();
            let mapped_field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(mapped.into())),
            );
            DataType::Map(mapped_field, *sorted)
        }
        _ => data_type.clone(),
    }
}

fn map_fields(fields: &arrow::datatypes::Fields) -> arrow::datatypes::Fields {
    fields.iter().map(map_field).collect()
}

fn map_field(field: &Arc<Field>) -> Arc<Field> {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(map_data_type(field.data_type())),
    )
}

fn arrow_failed(error: &arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::Execution(format!("text timestamp write failed: {error}"))
}

struct FormatContext<'a> {
    specs: &'a FormatSpecs,
    resolver: ZoneResolver,
    zone_id: &'a str,
}

struct TextColumnBuilder {
    offsets: Vec<i32>,
    values: Vec<u8>,
    nulls: NullBufferBuilder,
}

impl TextColumnBuilder {
    fn with_capacity(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            offsets,
            values: Vec::with_capacity(bytes),
            nulls: NullBufferBuilder::new(rows),
        }
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.values.extend_from_slice(bytes);
        let Ok(end) = i32::try_from(self.values.len()) else {
            return Err(DataFusionError::Execution(
                "text timestamp write failed: output exceeds 2 GiB".to_string(),
            ));
        };
        self.offsets.push(end);
        self.nulls.append_non_null();
        Ok(())
    }

    fn append_null(&mut self) {
        let last = self.offsets[self.offsets.len() - 1];
        self.offsets.push(last);
        self.nulls.append_null();
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(self.offsets));
        let values = Buffer::from(self.values);
        let array = StringArray::try_new(offsets, values, self.nulls.finish())
            .map_err(|error| arrow_failed(&error))?;
        Ok(Arc::new(array))
    }
}

fn append_ltz_default(
    builder: &mut TextColumnBuilder,
    state: &mut TimestampDefaultState,
    staged: &mut [u8; 48],
    resolver: &mut ZoneResolver,
    ticks: i64,
) -> Result<()> {
    if let Some((wall, offset)) = resolver.resolve_micros(ticks) {
        let len = state.render(wall, Some(offset), staged);
        builder.append_bytes(&staged[..len])
    } else {
        builder.append_null();
        Ok(())
    }
}

fn format_ltz_default_column(
    micros: &arrow::array::PrimitiveArray<TimestampMicrosecondType>,
    resolver: &mut ZoneResolver,
) -> Result<ArrayRef> {
    let mut builder = TextColumnBuilder::with_capacity(micros.len(), micros.len() * 32);
    let mut state = TimestampDefaultState::new();
    let mut staged = [0u8; 48];
    let values = micros.values();
    if micros.null_count() == 0 {
        for ticks in values {
            append_ltz_default(&mut builder, &mut state, &mut staged, resolver, *ticks)?;
        }
    } else {
        for (row, ticks) in values.iter().enumerate() {
            if micros.is_null(row) {
                builder.append_null();
                continue;
            }
            append_ltz_default(&mut builder, &mut state, &mut staged, resolver, *ticks)?;
        }
    }
    builder.finish()
}

fn format_ntz_default_column(
    micros: &arrow::array::PrimitiveArray<TimestampMicrosecondType>,
) -> Result<ArrayRef> {
    let mut builder = TextColumnBuilder::with_capacity(micros.len(), micros.len() * 32);
    let mut state = TimestampDefaultState::new();
    let mut staged = [0u8; 48];
    let (lowest, highest) = wall_micros_bounds();
    let values = micros.values();
    if micros.null_count() == 0 {
        for ticks in values {
            if *ticks < lowest || *ticks > highest {
                builder.append_null();
                continue;
            }
            let len = state.render(*ticks, None, &mut staged);
            builder.append_bytes(&staged[..len])?;
        }
    } else {
        for (row, ticks) in values.iter().enumerate() {
            if micros.is_null(row) || *ticks < lowest || *ticks > highest {
                builder.append_null();
                continue;
            }
            let len = state.render(*ticks, None, &mut staged);
            builder.append_bytes(&staged[..len])?;
        }
    }
    builder.finish()
}

fn append_date_default(
    builder: &mut TextColumnBuilder,
    state: &mut DateDefaultState,
    staged: &mut [u8; 48],
    days: i64,
) -> Result<()> {
    if let Some(len) = state.render(days, staged) {
        builder.append_bytes(&staged[..len])
    } else {
        builder.append_null();
        Ok(())
    }
}

fn format_timestamp_column(
    array: &ArrayRef,
    zoned: bool,
    context: &mut FormatContext,
) -> Result<ArrayRef> {
    let micros = cast(
        array.as_ref(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let micros = micros.as_primitive::<TimestampMicrosecondType>();
    let spec = if zoned {
        &context.specs.timestamp
    } else {
        &context.specs.ntz
    };
    if matches!(spec, FormatSpec::Default) {
        return if zoned {
            format_ltz_default_column(micros, &mut context.resolver)
        } else {
            format_ntz_default_column(micros)
        };
    }
    let mut builder = StringBuilder::with_capacity(micros.len(), micros.len() * 32);
    let mut buffer = String::with_capacity(64);
    for row in 0..micros.len() {
        if micros.is_null(row) {
            builder.append_null();
            continue;
        }
        let ticks = micros.value(row);
        let rendered = if zoned {
            format_instant_into(
                &mut buffer,
                ticks,
                &mut context.resolver,
                context.zone_id,
                &context.specs.timestamp,
            )?
        } else {
            format_wall_into(&mut buffer, ticks, &context.specs.ntz)?
        };
        if rendered {
            builder.append_value(&buffer);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_date_default_column(days: &arrow::array::PrimitiveArray<Date32Type>) -> Result<ArrayRef> {
    let mut builder = TextColumnBuilder::with_capacity(days.len(), days.len() * 10);
    let mut state = DateDefaultState::new();
    let mut staged = [0u8; 48];
    let values = days.values();
    if days.null_count() == 0 {
        for day in values {
            append_date_default(&mut builder, &mut state, &mut staged, i64::from(*day))?;
        }
    } else {
        for (row, day) in values.iter().enumerate() {
            if days.is_null(row) {
                builder.append_null();
                continue;
            }
            append_date_default(&mut builder, &mut state, &mut staged, i64::from(*day))?;
        }
    }
    builder.finish()
}

fn format_date_column(array: &ArrayRef, context: &FormatContext) -> Result<ArrayRef> {
    let days = cast(array.as_ref(), &DataType::Date32)?;
    let days = days.as_primitive::<Date32Type>();
    if matches!(context.specs.date, FormatSpec::Default) {
        return format_date_default_column(days);
    }
    let mut builder = StringBuilder::with_capacity(days.len(), days.len() * 10);
    let mut buffer = String::with_capacity(16);
    for row in 0..days.len() {
        if days.is_null(row) {
            builder.append_null();
            continue;
        }
        if format_date_into(&mut buffer, days.value(row), &context.specs.date)? {
            builder.append_value(&buffer);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_date64_column(array: &ArrayRef, context: &FormatContext) -> Result<ArrayRef> {
    let array = array.as_primitive::<Date64Type>();
    if matches!(context.specs.date, FormatSpec::Default) {
        let mut builder = TextColumnBuilder::with_capacity(array.len(), array.len() * 10);
        let mut state = DateDefaultState::new();
        let mut staged = [0u8; 48];
        let values = array.values();
        if array.null_count() == 0 {
            for millis in values {
                append_date_default(
                    &mut builder,
                    &mut state,
                    &mut staged,
                    millis.div_euclid(MILLIS_PER_DAY),
                )?;
            }
        } else {
            for (row, millis) in values.iter().enumerate() {
                if array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                append_date_default(
                    &mut builder,
                    &mut state,
                    &mut staged,
                    millis.div_euclid(MILLIS_PER_DAY),
                )?;
            }
        }
        return builder.finish();
    }
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 10);
    let mut buffer = String::with_capacity(16);
    for row in 0..array.len() {
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        let days = array.value(row).div_euclid(MILLIS_PER_DAY);
        let Ok(days) = i32::try_from(days) else {
            builder.append_null();
            continue;
        };
        if format_date_into(&mut buffer, days, &context.specs.date)? {
            builder.append_value(&buffer);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_struct_column(array: &ArrayRef, context: &mut FormatContext) -> Result<ArrayRef> {
    let Some(structure) = array.as_struct_opt() else {
        return Ok(array.clone());
    };
    if structure.num_columns() == 0 {
        return Ok(array.clone());
    }
    let mut columns = Vec::with_capacity(structure.num_columns());
    for column in structure.columns() {
        columns.push(format_array(column.clone(), context)?);
    }
    let fields: Vec<Arc<Field>> = match array.data_type() {
        DataType::Struct(fields) => fields.iter().map(map_field).collect(),
        _ => structure
            .column_names()
            .iter()
            .zip(columns.iter())
            .map(|(name, column)| Arc::new(Field::new(*name, column.data_type().clone(), true)))
            .collect(),
    };
    let rebuilt = StructArray::try_new(fields.into(), columns, structure.nulls().cloned())
        .map_err(|error| arrow_failed(&error))?;
    Ok(Arc::new(rebuilt))
}

fn format_list_column(array: &ArrayRef, context: &mut FormatContext) -> Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = format_array(list.values().clone(), context)?;
        let field = match array.data_type() {
            DataType::List(field) => map_field(field),
            _ => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };
        let rebuilt =
            ListArray::try_new(field, list.offsets().clone(), values, list.nulls().cloned())
                .map_err(|error| arrow_failed(&error))?;
        return Ok(Arc::new(rebuilt));
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = format_array(list.values().clone(), context)?;
        let field = match array.data_type() {
            DataType::LargeList(field) => map_field(field),
            _ => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };
        let rebuilt =
            LargeListArray::try_new(field, list.offsets().clone(), values, list.nulls().cloned())
                .map_err(|error| arrow_failed(&error))?;
        return Ok(Arc::new(rebuilt));
    }
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = format_array(list.values().clone(), context)?;
        let field = match array.data_type() {
            DataType::FixedSizeList(field, _) => map_field(field),
            _ => Arc::new(Field::new("item", values.data_type().clone(), true)),
        };
        let rebuilt =
            FixedSizeListArray::try_new(field, list.value_length(), values, list.nulls().cloned())
                .map_err(|error| arrow_failed(&error))?;
        return Ok(Arc::new(rebuilt));
    }
    Ok(array.clone())
}

fn format_map_column(array: &ArrayRef, context: &mut FormatContext) -> Result<ArrayRef> {
    let Some(map) = array.as_map_opt() else {
        return Ok(array.clone());
    };
    let entries = map.entries();
    if entries.num_columns() != 2 {
        return Ok(array.clone());
    }
    let values = format_array(entries.column(1).clone(), context)?;
    let keys_field = entries.fields()[0].clone();
    let values_field = map_field(&entries.fields()[1]);
    let rebuilt_entries = StructArray::try_new(
        vec![keys_field, values_field].into(),
        vec![entries.column(0).clone(), values],
        entries.nulls().cloned(),
    )
    .map_err(|error| arrow_failed(&error))?;
    let field = match array.data_type() {
        DataType::Map(field, _) => Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(rebuilt_entries.data_type().clone()),
        ),
        _ => Arc::new(Field::new(
            "entries",
            rebuilt_entries.data_type().clone(),
            true,
        )),
    };
    let sorted = match array.data_type() {
        DataType::Map(_, sorted) => *sorted,
        _ => false,
    };
    let rebuilt = MapArray::try_new(
        field,
        map.offsets().clone(),
        rebuilt_entries,
        map.nulls().cloned(),
        sorted,
    )
    .map_err(|error| arrow_failed(&error))?;
    Ok(Arc::new(rebuilt))
}

fn format_array(array: ArrayRef, context: &mut FormatContext) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(_, zone) => format_timestamp_column(&array, zone.is_some(), context),
        DataType::Date32 => format_date_column(&array, context),
        DataType::Date64 => format_date64_column(&array, context),
        DataType::Struct(_) => format_struct_column(&array, context),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            format_list_column(&array, context)
        }
        DataType::Map(_, _) => format_map_column(&array, context),
        _ => Ok(array),
    }
}

#[derive(Debug)]
struct WriteFormatText {
    signature: Signature,
    caches: Mutex<HashMap<String, OffsetCache>>,
}

impl WriteFormatText {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
            caches: Mutex::new(HashMap::new()),
        }
    }

    fn take_resolver(&self, canonical: &str, zone_raw: &str) -> Result<ZoneResolver> {
        if let Some(offset) = fixed_session_offset(canonical) {
            return Ok(ZoneResolver::fixed(offset));
        }
        let zone = Tz::from_str(canonical).map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone {zone_raw:?} could not be resolved at query time ({error})"
            ))
        })?;
        let mut caches = self
            .caches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(ZoneResolver::Cached(
            caches
                .remove(canonical)
                .unwrap_or_else(|| OffsetCache::new(zone)),
        ))
    }

    fn park_resolver(&self, canonical: String, resolver: ZoneResolver) {
        let ZoneResolver::Cached(cache) = resolver else {
            return;
        };
        let mut caches = self
            .caches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if caches.len() >= SHARED_ZONE_LIMIT {
            caches.clear();
        }
        caches.insert(canonical, cache);
    }
}

impl PartialEq for WriteFormatText {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for WriteFormatText {}

impl Hash for WriteFormatText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for WriteFormatText {
    fn name(&self) -> &str {
        WRITE_FORMAT_FUNCTION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(first) = arg_types.first() else {
            return Err(DataFusionError::Plan(format!(
                "'{WRITE_FORMAT_FUNCTION}' expects (value, timestamp, ntz, date, zone), got 0 \
                 argument(s)"
            )));
        };
        Ok(map_data_type(first))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(first) = args.arg_fields.first() else {
            return Err(DataFusionError::Plan(format!(
                "'{WRITE_FORMAT_FUNCTION}' expects (value, timestamp, ntz, date, zone), got 0 \
                 argument(s)"
            )));
        };
        Ok(Arc::new(Field::new(
            first.name(),
            map_data_type(first.data_type()),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 5 {
            return Err(DataFusionError::Plan(format!(
                "'{WRITE_FORMAT_FUNCTION}' expects (value, timestamp, ntz, date, zone), got {} \
                 argument(s)",
                arg_types.len()
            )));
        }
        Ok(vec![
            arg_types[0].clone(),
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(value) = arrays.first() else {
            return Err(DataFusionError::Execution(format!(
                "'{WRITE_FORMAT_FUNCTION}' expects (value, timestamp, ntz, date, zone), got 0 \
                 argument(s)"
            )));
        };
        let pattern_at = |position: usize| -> Result<Option<String>> {
            let Some(source) = arrays.get(position) else {
                return Err(DataFusionError::Execution(format!(
                    "'{WRITE_FORMAT_FUNCTION}' expects (value, timestamp, ntz, date, zone), got \
                     {} argument(s)",
                    arrays.len()
                )));
            };
            let casted = cast(source.as_ref(), &DataType::Utf8)?;
            let strings = casted.as_string::<i32>();
            if strings.is_empty() || strings.is_null(0) {
                return Ok(None);
            }
            Ok(Some(strings.value(0).to_string()))
        };
        let timestamp = pattern_at(1)?.map(|text| halve_backslashes(&text));
        let ntz = pattern_at(2)?.map(|text| halve_backslashes(&text));
        let date = pattern_at(3)?.map(|text| halve_backslashes(&text));
        let zone_raw = pattern_at(4)?.ok_or_else(|| {
            DataFusionError::Execution(format!(
                "'{WRITE_FORMAT_FUNCTION}' expects a session zone id, got NULL"
            ))
        })?;
        let specs = FormatSpecs {
            timestamp: spec_from_value(timestamp.as_deref(), PatternKind::Timestamp)?,
            ntz: spec_from_value(ntz.as_deref(), PatternKind::TimestampNtz)?,
            date: spec_from_value(date.as_deref(), PatternKind::Date)?,
        };
        let canonical = canonical_session_zone_id(zone_raw.as_str());
        let display = java_display_zone_id(zone_raw.as_str());
        let mut context = FormatContext {
            specs: &specs,
            resolver: self.take_resolver(canonical.as_str(), zone_raw.as_str())?,
            zone_id: display.as_str(),
        };
        let formatted = format_array(value.clone(), &mut context)?;
        self.park_resolver(canonical, context.resolver);
        Ok(ColumnarValue::Array(formatted))
    }
}

#[must_use]
pub fn text_write_format_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(WriteFormatText::new()))
}

pub fn register_text_write_format(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(text_write_format_udf().as_ref().clone());
}
