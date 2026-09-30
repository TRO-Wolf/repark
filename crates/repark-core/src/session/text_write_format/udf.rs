use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    StringBuilder, StructArray,
};
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
use crate::session_time_zone::{canonical_session_zone_id, java_display_zone_id};

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

const SEARCH_SECONDS_CLAMP: i64 = 9_000_000_000_000;
const SEARCH_STEP_SECONDS: i64 = 518_400;
const SEARCH_PROBE_BUDGET: u32 = 12_288;

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

    fn probe(&self, seconds: i64, offset: FixedOffset) -> bool {
        let clamped = seconds.clamp(-SEARCH_SECONDS_CLAMP, SEARCH_SECONDS_CLAMP);
        micros_to_wall_zone(clamped * MICROS_PER_SECOND, self.zone)
            .is_some_and(|(_, found)| found == offset)
    }

    fn bound_above(&self, center: i64, offset: FixedOffset) -> i64 {
        let mut low = center;
        for _ in 0..SEARCH_PROBE_BUDGET {
            let high = low.saturating_add(SEARCH_STEP_SECONDS);
            if !self.probe(high, offset) {
                let mut keep = low;
                let mut drop = high;
                while drop - keep > 1 {
                    let middle = keep + (drop - keep) / 2;
                    if self.probe(middle, offset) {
                        keep = middle;
                    } else {
                        drop = middle;
                    }
                }
                return drop;
            }
            low = high;
        }
        low.saturating_add(1)
    }

    fn bound_below(&self, center: i64, offset: FixedOffset) -> i64 {
        let mut high = center;
        for _ in 0..SEARCH_PROBE_BUDGET {
            let low = high.saturating_sub(SEARCH_STEP_SECONDS);
            if !self.probe(low, offset) {
                let mut drop = low;
                let mut keep = high;
                while keep - drop > 1 {
                    let middle = drop + (keep - drop) / 2;
                    if self.probe(middle, offset) {
                        keep = middle;
                    } else {
                        drop = middle;
                    }
                }
                return keep;
            }
            high = low;
        }
        high
    }

    fn refresh(&mut self, micros: i64) -> Option<(NaiveDateTime, FixedOffset)> {
        let (wall, offset) = micros_to_wall_zone(micros, self.zone)?;
        let center = micros.div_euclid(MICROS_PER_SECOND);
        self.offset = Some(offset);
        self.start = self
            .bound_below(center, offset)
            .saturating_mul(MICROS_PER_SECOND);
        self.end = self
            .bound_above(center, offset)
            .saturating_mul(MICROS_PER_SECOND);
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
        let center = micros.div_euclid(MICROS_PER_SECOND);
        self.offset = Some(offset);
        self.start = self
            .bound_below(center, offset)
            .saturating_mul(MICROS_PER_SECOND);
        self.end = self
            .bound_above(center, offset)
            .saturating_mul(MICROS_PER_SECOND);
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

fn format_instant_into(
    buffer: &mut String,
    micros: i64,
    cache: &mut OffsetCache,
    zone_id: &str,
    spec: &FormatSpec,
) -> Result<bool> {
    let Some((wall, offset)) = cache.resolve(micros) else {
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
    cache: OffsetCache,
    zone_id: &'a str,
}

fn staged_text(staged: &[u8], len: usize) -> &str {
    debug_assert!(staged[..len].iter().all(u8::is_ascii));
    std::str::from_utf8(&staged[..len]).unwrap_or("")
}

fn append_ltz_default(
    builder: &mut StringBuilder,
    state: &mut TimestampDefaultState,
    staged: &mut [u8; 48],
    cache: &mut OffsetCache,
    ticks: i64,
) {
    match cache.resolve_micros(ticks) {
        Some((wall, offset)) => {
            let len = state.render(wall, Some(offset), staged);
            builder.append_value(staged_text(staged, len));
        }
        None => builder.append_null(),
    }
}

fn format_ltz_default_column(
    micros: &arrow::array::PrimitiveArray<TimestampMicrosecondType>,
    cache: &mut OffsetCache,
) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(micros.len(), micros.len() * 32);
    let mut state = TimestampDefaultState::new();
    let mut staged = [0u8; 48];
    let values = micros.values();
    if micros.null_count() == 0 {
        for ticks in values {
            append_ltz_default(&mut builder, &mut state, &mut staged, cache, *ticks);
        }
    } else {
        for (row, ticks) in values.iter().enumerate() {
            if micros.is_null(row) {
                builder.append_null();
                continue;
            }
            append_ltz_default(&mut builder, &mut state, &mut staged, cache, *ticks);
        }
    }
    Arc::new(builder.finish())
}

fn format_ntz_default_column(
    micros: &arrow::array::PrimitiveArray<TimestampMicrosecondType>,
) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(micros.len(), micros.len() * 32);
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
            builder.append_value(staged_text(&staged, len));
        }
    } else {
        for (row, ticks) in values.iter().enumerate() {
            if micros.is_null(row) || *ticks < lowest || *ticks > highest {
                builder.append_null();
                continue;
            }
            let len = state.render(*ticks, None, &mut staged);
            builder.append_value(staged_text(&staged, len));
        }
    }
    Arc::new(builder.finish())
}

fn append_date_default(
    builder: &mut StringBuilder,
    state: &mut DateDefaultState,
    staged: &mut [u8; 48],
    days: i64,
) {
    match state.render(days, staged) {
        Some(len) => builder.append_value(staged_text(staged, len)),
        None => builder.append_null(),
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
        return Ok(if zoned {
            format_ltz_default_column(micros, &mut context.cache)
        } else {
            format_ntz_default_column(micros)
        });
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
                &mut context.cache,
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

fn format_date_default_column(days: &arrow::array::PrimitiveArray<Date32Type>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(days.len(), days.len() * 10);
    let mut state = DateDefaultState::new();
    let mut staged = [0u8; 48];
    let values = days.values();
    if days.null_count() == 0 {
        for day in values {
            append_date_default(&mut builder, &mut state, &mut staged, i64::from(*day));
        }
    } else {
        for (row, day) in values.iter().enumerate() {
            if days.is_null(row) {
                builder.append_null();
                continue;
            }
            append_date_default(&mut builder, &mut state, &mut staged, i64::from(*day));
        }
    }
    Arc::new(builder.finish())
}

fn format_date_column(array: &ArrayRef, context: &FormatContext) -> Result<ArrayRef> {
    let days = cast(array.as_ref(), &DataType::Date32)?;
    let days = days.as_primitive::<Date32Type>();
    if matches!(context.specs.date, FormatSpec::Default) {
        return Ok(format_date_default_column(days));
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
        let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 10);
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
                );
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
                );
            }
        }
        return Ok(Arc::new(builder.finish()));
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
}

impl WriteFormatText {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
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
        let zone = Tz::from_str(canonical.as_str()).map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone {zone_raw:?} could not be resolved at query time ({error})"
            ))
        })?;
        let display = java_display_zone_id(zone_raw.as_str());
        let mut context = FormatContext {
            specs: &specs,
            cache: OffsetCache::new(zone),
            zone_id: display.as_str(),
        };
        format_array(value.clone(), &mut context).map(ColumnarValue::Array)
    }
}

#[must_use]
pub fn text_write_format_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(WriteFormatText::new()))
}

pub fn register_text_write_format(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(text_write_format_udf().as_ref().clone());
}

#[cfg(test)]
mod perf_probe {
    use super::*;
    use arrow::array::{Date32Array, TimestampMicrosecondArray};

    fn checksum(array: &ArrayRef) -> u64 {
        let strings = array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("utf8 output");
        let mut hash = 0u64;
        for row in 0..strings.len() {
            if strings.is_null(row) {
                hash = hash.wrapping_add(1);
                continue;
            }
            for byte in strings.value(row).as_bytes() {
                hash = hash.wrapping_mul(31).wrapping_add(u64::from(*byte));
            }
        }
        hash
    }

    fn probe_column(name: &str, array: &ArrayRef, zoned: bool, specs: &FormatSpecs) {
        let zone = Tz::from_str("America/New_York").expect("zone parses");
        let display = java_display_zone_id("America/New_York");
        let mut context = FormatContext {
            specs,
            cache: OffsetCache::new(zone),
            zone_id: display.leak(),
        };
        let started = std::time::Instant::now();
        let out = if zoned {
            format_timestamp_column(array, true, &mut context).expect("formats")
        } else if array.data_type() == &DataType::Date32 {
            format_date_column(array, &context).expect("formats")
        } else {
            format_timestamp_column(array, false, &mut context).expect("formats")
        };
        let elapsed = started.elapsed();
        let sum = checksum(&out);
        assert_eq!(out.len(), array.len());
        assert_ne!(sum, 0);
        let per = elapsed.as_nanos() / u128::try_from(array.len()).unwrap_or(1);
        println!("PROBE {name}: {elapsed:?} total, {per} ns/value, checksum {sum}");
    }

    fn default_specs() -> FormatSpecs {
        FormatSpecs {
            timestamp: FormatSpec::Default,
            ntz: FormatSpec::Default,
            date: FormatSpec::Default,
        }
    }

    #[test]
    fn probe_loop_stages_throughput() {
        let rows = 1_000_000i64;
        let base = 1_704_067_200_000_000i64;
        let values: Vec<i64> = (0..rows).map(|i| base + i * 31_536_789).collect();
        let array: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        let micros = array.as_primitive::<TimestampMicrosecondType>();
        let zone = Tz::from_str("America/New_York").expect("zone parses");
        let started = std::time::Instant::now();
        let mut builder = StringBuilder::with_capacity(micros.len(), micros.len() * 32);
        for row in 0..micros.len() {
            if micros.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value("2024-06-15T12:34:56.789-04:00");
            }
        }
        let out: ArrayRef = Arc::new(builder.finish());
        println!(
            "PROBE loop_only: {:?} total, checksum {}",
            started.elapsed(),
            checksum(&out)
        );
        let mut cache = OffsetCache::new(zone);
        let started = std::time::Instant::now();
        let mut total = 0i64;
        for row in 0..micros.len() {
            if let Some((wall, offset)) = cache.resolve(micros.value(row)) {
                total += i64::from(offset.local_minus_utc()) + wall.and_utc().timestamp();
            }
        }
        println!(
            "PROBE resolve_only: {:?} total, sink {total}",
            started.elapsed()
        );
        let (wall, offset) = micros_to_wall_zone(base, zone).expect("in range");
        let started = std::time::Instant::now();
        let mut builder = StringBuilder::with_capacity(micros.len(), micros.len() * 32);
        let mut buffer = String::with_capacity(64);
        for row in 0..micros.len() {
            if micros.is_null(row) {
                builder.append_null();
            } else {
                render_timestamp_default_into(&mut buffer, &wall, 789_000_000, offset);
                builder.append_value(&buffer);
            }
        }
        let out: ArrayRef = Arc::new(builder.finish());
        println!(
            "PROBE render_only: {:?} total, checksum {}",
            started.elapsed(),
            checksum(&out)
        );
    }

    #[test]
    fn probe_default_paths_throughput() {
        let rows = 1_000_000i64;
        let base = 1_704_067_200_000_000i64;
        let t1: Vec<i64> = (0..rows).map(|i| base + i * 31_536_789).collect();
        let t2: Vec<i64> = (0..rows).map(|i| base - i * 7_777_777_777).collect();
        let t3: Vec<i64> = (0..rows).map(|i| base + i * 1_000_003).collect();
        let days: Vec<i32> = (0..rows)
            .map(|i| i32::try_from(19_700 + (i % 20_000) - 10_000).expect("day fits"))
            .collect();
        let zoned = |values: Vec<i64>| -> ArrayRef {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
        };
        probe_column("ltz_t1", &zoned(t1.clone()), true, &default_specs());
        probe_column("ltz_t2", &zoned(t2), true, &default_specs());
        probe_column("ltz_t3", &zoned(t3), true, &default_specs());
        let ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(t1));
        probe_column("ntz_t1", &ntz, false, &default_specs());
        let dates: ArrayRef = Arc::new(Date32Array::from(days));
        probe_column("date_d", &dates, false, &default_specs());
    }
}
