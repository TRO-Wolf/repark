use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, StructArray, TimestampMicrosecondBuilder};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::datetime::localize_wall_micros_in_zone;
use crate::instant_ts::ltz_timestamp_type;
use crate::session_time_zone::session_time_zone_from_options;

pub const LOCALIZE_NESTED_NTZ_NAME: &str = "__repark_localize_nested_ntz__";

#[must_use]
pub fn localize_nested_ntz_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(LocalizeNestedNtz {
        signature: Signature::any(2, Volatility::Stable),
    }))
}

#[derive(Debug)]
struct LocalizeNestedNtz {
    signature: Signature,
}

impl PartialEq for LocalizeNestedNtz {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for LocalizeNestedNtz {}

impl Hash for LocalizeNestedNtz {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for LocalizeNestedNtz {
    fn name(&self) -> &str {
        LOCALIZE_NESTED_NTZ_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(
                "nested timestamp localization needs the source struct".to_string(),
            )
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let source = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(
                "nested timestamp localization needs the source struct".to_string(),
            )
        })?;
        let paths = parse_paths(&literal_paths(
            args.scalar_arguments.get(1).copied().flatten(),
        )?);
        let DataType::Struct(fields) = source.data_type() else {
            return plan_err!(
                "nested timestamp localization needs a struct, got {}",
                source.data_type()
            );
        };
        let retyped = retype_fields(fields, &paths)?;
        Ok(Arc::new(
            source
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(retyped)),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("nested timestamp localization takes a struct and a path list");
        }
        let zone_id = session_time_zone_from_options(args.config_options.as_ref());
        let zone = zone_id.parse::<Tz>().map_err(|error| {
            DataFusionError::Execution(format!(
                "session timezone {zone_id:?} could not be resolved at query time ({error})"
            ))
        })?;
        let paths = parse_paths(&paths_from_value(&args.args[1])?);
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let struct_array = arrays[0].as_struct_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "nested timestamp localization needs a struct, got {}",
                arrays[0].data_type()
            ))
        })?;
        let rewritten = rewrite_struct(struct_array, &paths, zone)?;
        if scalar && args.number_rows == 1 {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &rewritten, 0,
            )?));
        }
        Ok(ColumnarValue::Array(rewritten))
    }
}

fn literal_paths(value: Option<&ScalarValue>) -> Result<String> {
    if let Some(
        ScalarValue::Utf8(text) | ScalarValue::LargeUtf8(text) | ScalarValue::Utf8View(text),
    ) = value
    {
        return Ok(text.clone().unwrap_or_default());
    }
    plan_err!("nested timestamp paths must be a string literal")
}

fn paths_from_value(value: &ColumnarValue) -> Result<String> {
    match value {
        ColumnarValue::Scalar(scalar) => literal_paths(Some(scalar)),
        ColumnarValue::Array(_) => {
            plan_err!("nested timestamp paths must be a string literal")
        }
    }
}

fn parse_paths(text: &str) -> Vec<Vec<String>> {
    if text.is_empty() {
        return Vec::new();
    }
    text.split(',')
        .map(|path| path.split('.').map(str::to_string).collect())
        .collect()
}

fn is_naive_micros(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

fn retype_fields(fields: &Fields, paths: &[Vec<String>]) -> Result<Fields> {
    let mut seen = vec![false; paths.len()];
    let mut updated = Vec::with_capacity(fields.len());
    for field in fields {
        let mine = paths_at(field.name(), paths, &mut seen);
        updated.push(retype_field(field, &mine)?);
    }
    unseen(paths, &seen)?;
    Ok(updated.into())
}

fn retype_field(field: &FieldRef, paths: &[Vec<String>]) -> Result<FieldRef> {
    if paths.is_empty() {
        return Ok(Arc::clone(field));
    }
    if paths.iter().any(Vec::is_empty) {
        if paths.iter().any(|path| !path.is_empty()) {
            return plan_err!("unresolvable nested timestamp path at {}", field.name());
        }
        if !is_naive_micros(field.data_type()) {
            return plan_err!(
                "unresolvable nested timestamp path {}: not a naive microsecond timestamp",
                field.name()
            );
        }
        return Ok(Arc::new(
            field.as_ref().clone().with_data_type(ltz_timestamp_type()),
        ));
    }
    let DataType::Struct(children) = field.data_type() else {
        return plan_err!("unresolvable nested timestamp path at {}", field.name());
    };
    let nested = retype_fields(children, paths)?;
    Ok(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(nested)),
    ))
}

fn paths_at(name: &str, paths: &[Vec<String>], seen: &mut [bool]) -> Vec<Vec<String>> {
    let mut mine = Vec::new();
    for (index, path) in paths.iter().enumerate() {
        if path.first().map(String::as_str) == Some(name) {
            seen[index] = true;
            if path.len() == 1 {
                mine.push(Vec::new());
            } else {
                mine.push(path[1..].to_vec());
            }
        }
    }
    mine
}

fn unseen(paths: &[Vec<String>], seen: &[bool]) -> Result<()> {
    for (path, found) in paths.iter().zip(seen) {
        if !found {
            return plan_err!("unresolvable nested timestamp path {}", path.join("."));
        }
    }
    Ok(())
}

fn rewrite_struct(array: &StructArray, paths: &[Vec<String>], zone: Tz) -> Result<ArrayRef> {
    let mut seen = vec![false; paths.len()];
    let mut fields = Vec::with_capacity(array.fields().len());
    let mut columns = Vec::with_capacity(array.fields().len());
    for (index, field) in array.fields().iter().enumerate() {
        let mine = paths_at(field.name(), paths, &mut seen);
        let column = Arc::clone(array.column(index));
        let (updated, child) = rewrite_child(field, &column, &mine, zone)?;
        fields.push(updated);
        columns.push(child);
    }
    unseen(paths, &seen)?;
    let rebuilt =
        StructArray::try_new(fields.into(), columns, array.nulls().cloned()).map_err(|error| {
            DataFusionError::Execution(format!(
                "nested timestamp localization could not rebuild the struct ({error})"
            ))
        })?;
    Ok(Arc::new(rebuilt))
}

fn rewrite_child(
    field: &FieldRef,
    column: &ArrayRef,
    paths: &[Vec<String>],
    zone: Tz,
) -> Result<(FieldRef, ArrayRef)> {
    if paths.is_empty() {
        return Ok((Arc::clone(field), Arc::clone(column)));
    }
    if paths.iter().any(Vec::is_empty) {
        if paths.iter().any(|path| !path.is_empty()) {
            return plan_err!("unresolvable nested timestamp path at {}", field.name());
        }
        if !is_naive_micros(column.data_type()) {
            return exec_err!(
                "unresolvable nested timestamp path {}: not a naive microsecond timestamp",
                field.name()
            );
        }
        let localized = localize_column(column, zone)?;
        let updated = Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(localized.data_type().clone()),
        );
        return Ok((updated, localized));
    }
    let nested = column.as_struct_opt().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "unresolvable nested timestamp path at {}",
            field.name()
        ))
    })?;
    let rewritten = rewrite_struct(nested, paths, zone)?;
    let updated = Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(rewritten.data_type().clone()),
    );
    Ok((updated, rewritten))
}

fn localize_column(array: &ArrayRef, zone: Tz) -> Result<ArrayRef> {
    let values = array
        .as_primitive_opt::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "nested timestamp localization needs naive microseconds".to_string(),
            )
        })?;
    let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
            continue;
        }
        match localize_wall_micros_in_zone(values.value(row), zone) {
            Some(tick) => builder.append_value(tick),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish().with_timezone("UTC")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::array::{Int32Array, TimestampMicrosecondArray};
    use chrono::NaiveDate;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionConfig;

    use crate::instant_ts::to_timestamp_udf;
    use crate::session_time_zone::with_session_time_zone;

    fn wall_micros(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micro: u32,
    ) -> i64 {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_micro_opt(hour, minute, second, micro))
            .expect("wall")
            .and_utc()
            .timestamp_micros()
    }

    fn config_at(zone: &str) -> Arc<ConfigOptions> {
        let config = with_session_time_zone(SessionConfig::new(), zone);
        Arc::clone(config.options())
    }

    fn naive_field(name: &str, nullable: bool) -> FieldRef {
        Arc::new(Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable,
        ))
    }

    fn int_field(name: &str) -> FieldRef {
        Arc::new(Field::new(name, DataType::Int32, true))
    }

    fn struct_field(name: &str, children: Vec<FieldRef>, nullable: bool) -> FieldRef {
        Arc::new(Field::new(
            name,
            DataType::Struct(children.into()),
            nullable,
        ))
    }

    fn stamp_array(ticks: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(ticks.to_vec()))
    }

    fn struct_array(
        fields: Vec<FieldRef>,
        columns: Vec<ArrayRef>,
        valid: Option<Vec<bool>>,
    ) -> StructArray {
        let nulls = valid.map(datafusion::arrow::buffer::NullBuffer::from);
        StructArray::try_new(fields.into(), columns, nulls).expect("struct")
    }

    fn invoke(array: StructArray, paths: &str, zone: &str, scalar: bool) -> Result<ColumnarValue> {
        let source = Arc::new(Field::new("st", array.data_type().clone(), true));
        let paths_field = Arc::new(Field::new("paths", DataType::Utf8, false));
        let input = if scalar {
            ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(array)))
        } else {
            ColumnarValue::Array(Arc::new(array))
        };
        let rows = match &input {
            ColumnarValue::Array(values) => values.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let udf = localize_nested_ntz_udf();
        let paths_literal = ScalarValue::Utf8(Some(paths.to_string()));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&source), Arc::clone(&paths_field)],
            scalar_arguments: &[None, Some(&paths_literal)],
        })?;
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![input, ColumnarValue::Scalar(paths_literal)],
            arg_fields: vec![source, paths_field],
            number_rows: rows,
            return_field,
            config_options: config_at(zone),
        })
    }

    fn output_struct(value: &ColumnarValue) -> StructArray {
        if let ColumnarValue::Array(array) = value {
            return array.as_struct().clone();
        }
        let ColumnarValue::Scalar(ScalarValue::Struct(array)) = value else {
            panic!("struct output, got {value:?}");
        };
        array.as_ref().clone()
    }

    fn leaf_tick(array: &StructArray, path: &[&str], row: usize) -> (DataType, Option<i64>) {
        let mut current = array.clone();
        for (depth, name) in path.iter().enumerate() {
            let index = current
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .expect("field");
            if depth + 1 == path.len() {
                let column = Arc::clone(current.column(index));
                let data_type = column.data_type().clone();
                let ticks =
                    column.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
                let tick = if ticks.is_null(row) {
                    None
                } else {
                    Some(ticks.value(row))
                };
                return (data_type, tick);
            }
            current = current.column(index).as_struct().clone();
        }
        unreachable!("path")
    }

    fn top_level_tick(ticks: i64, zone: &str) -> Option<i64> {
        let array = TimestampMicrosecondArray::from(vec![Some(ticks)]);
        let field = Arc::new(Field::new("v", array.data_type().clone(), true));
        let udf = to_timestamp_udf();
        let value = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(array))],
                arg_fields: vec![Arc::clone(&field)],
                number_rows: 1,
                return_field: Arc::new(Field::new("to_timestamp", ltz_timestamp_type(), true)),
                config_options: config_at(zone),
            })
            .expect("to_timestamp");
        let arrays = ColumnarValue::values_to_arrays(&[value]).expect("array");
        let ticks =
            arrays[0].as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
        if ticks.is_null(0) {
            None
        } else {
            Some(ticks.value(0))
        }
    }

    #[test]
    fn one_leaf_matches_to_timestamp_in_three_zones() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let fields = vec![naive_field("v", true), int_field("n")];
        for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
            let array = struct_array(
                fields.clone(),
                vec![
                    stamp_array(&[Some(wall)]),
                    Arc::new(Int32Array::from(vec![Some(7)])),
                ],
                None,
            );
            let value = invoke(array, "v", zone, false).expect(zone);
            let rebuilt = output_struct(&value);
            let (data_type, tick) = leaf_tick(&rebuilt, &["v"], 0);
            assert_eq!(data_type, ltz_timestamp_type(), "{zone}");
            assert_eq!(tick, top_level_tick(wall, zone), "{zone}");
            assert_eq!(
                tick,
                localize_wall_micros_in_zone(wall, zone.parse().expect("zone")),
                "{zone}"
            );
        }
    }

    #[test]
    fn null_struct_null_inner_and_null_leaf_stay_null() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let leaf = naive_field("v", true);
        let null_row = struct_array(
            vec![leaf.clone(), int_field("n")],
            vec![
                stamp_array(&[Some(wall)]),
                Arc::new(Int32Array::from(vec![Some(7)])),
            ],
            Some(vec![false]),
        );
        let null_struct = output_struct(&invoke(null_row, "v", "UTC", false).expect("null struct"));
        assert!(null_struct.is_null(0));

        let inner = struct_field("inner", vec![leaf.clone()], true);
        let null_inner = struct_array(
            vec![inner],
            vec![Arc::new(struct_array(
                vec![leaf.clone()],
                vec![stamp_array(&[Some(wall)])],
                Some(vec![false]),
            ))],
            None,
        );
        let rebuilt =
            output_struct(&invoke(null_inner, "inner.v", "UTC", false).expect("null inner"));
        assert!(rebuilt.column(0).is_null(0));

        let null_leaf = struct_array(vec![leaf], vec![stamp_array(&[None])], None);
        let rebuilt = output_struct(&invoke(null_leaf, "v", "UTC", false).expect("null leaf"));
        let (data_type, tick) = leaf_tick(&rebuilt, &["v"], 0);
        assert_eq!(data_type, ltz_timestamp_type());
        assert_eq!(tick, None);
    }

    #[test]
    fn depth_three_leaf_is_localized() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let leaf = struct_array(
            vec![naive_field("v", true)],
            vec![stamp_array(&[Some(wall)])],
            None,
        );
        let middle = struct_array(
            vec![struct_field("b", vec![naive_field("v", true)], true)],
            vec![Arc::new(leaf)],
            None,
        );
        let outer = struct_array(
            vec![struct_field(
                "a",
                vec![struct_field("b", vec![naive_field("v", true)], true)],
                true,
            )],
            vec![Arc::new(middle)],
            None,
        );
        let rebuilt =
            output_struct(&invoke(outer, "a.b.v", "America/New_York", false).expect("depth"));
        let (data_type, tick) = leaf_tick(&rebuilt, &["a", "b", "v"], 0);
        assert_eq!(data_type, ltz_timestamp_type());
        assert_eq!(tick, top_level_tick(wall, "America/New_York"));
    }

    #[test]
    fn twenty_fields_pass_the_others_by_reference() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for index in 0..20 {
            let name = format!("f{index:02}");
            if index == 7 {
                fields.push(naive_field(&name, true));
                columns.push(stamp_array(&[Some(wall)]));
            } else {
                fields.push(int_field(&name));
                columns.push(Arc::new(Int32Array::from(vec![Some(index)])));
            }
        }
        let original_columns = columns.clone();
        let array = struct_array(fields, columns, None);
        let rebuilt = output_struct(&invoke(array, "f07", "Asia/Kolkata", false).expect("wide"));
        let (data_type, tick) = leaf_tick(&rebuilt, &["f07"], 0);
        assert_eq!(data_type, ltz_timestamp_type());
        assert_eq!(tick, top_level_tick(wall, "Asia/Kolkata"));
        for (index, column) in original_columns.iter().enumerate() {
            if index == 7 {
                continue;
            }
            assert!(
                Arc::ptr_eq(rebuilt.column(index), column),
                "field {index} was copied"
            );
        }
    }

    #[test]
    fn not_null_member_and_metadata_are_kept() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let mut leaf = Field::new("v", DataType::Timestamp(TimeUnit::Microsecond, None), false);
        leaf.set_metadata(HashMap::from([("origin".to_string(), "wall".to_string())]));
        let leaf = Arc::new(leaf);
        let array = struct_array(
            vec![Arc::clone(&leaf)],
            vec![stamp_array(&[Some(wall)])],
            None,
        );
        let value = invoke(array, "v", "UTC", false).expect("meta");
        let rebuilt = output_struct(&value);
        let field = rebuilt.fields()[0].as_ref();
        assert!(!field.is_nullable());
        assert_eq!(
            field.metadata().get("origin").map(String::as_str),
            Some("wall")
        );
        assert_eq!(field.data_type(), &ltz_timestamp_type());
    }

    #[test]
    fn scalar_struct_stays_a_scalar() {
        let wall = wall_micros(2026, 1, 2, 3, 4, 5, 123_456);
        let array = struct_array(
            vec![naive_field("v", true)],
            vec![stamp_array(&[Some(wall)])],
            None,
        );
        let value = invoke(array, "v", "UTC", true).expect("scalar");
        assert!(matches!(
            value,
            ColumnarValue::Scalar(ScalarValue::Struct(_))
        ));
        let rebuilt = output_struct(&value);
        assert_eq!(leaf_tick(&rebuilt, &["v"], 0).1, Some(wall));
    }

    #[test]
    fn an_unresolvable_path_is_a_plan_error() {
        let field = struct_field("st", vec![naive_field("v", true)], true);
        let paths = Arc::new(Field::new("paths", DataType::Utf8, false));
        let literal = ScalarValue::Utf8(Some("missing".to_string()));
        let error = localize_nested_ntz_udf()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[field, paths],
                scalar_arguments: &[None, Some(&literal)],
            })
            .expect_err("missing path");
        assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains("unresolvable nested timestamp path")
        );
    }

    #[test]
    fn gap_and_overlap_walls_match_the_top_level_path() {
        let cases = [
            ("America/New_York", wall_micros(2026, 3, 8, 2, 30, 0, 0)),
            ("America/New_York", wall_micros(2026, 11, 1, 1, 30, 0, 0)),
            ("Australia/Lord_Howe", wall_micros(2026, 10, 4, 2, 0, 0, 0)),
            ("Australia/Lord_Howe", wall_micros(2026, 4, 5, 1, 30, 0, 0)),
        ];
        for (zone, wall) in cases {
            let array = struct_array(
                vec![naive_field("v", true)],
                vec![stamp_array(&[Some(wall)])],
                None,
            );
            let rebuilt = output_struct(&invoke(array, "v", zone, false).expect(zone));
            assert_eq!(
                leaf_tick(&rebuilt, &["v"], 0).1,
                top_level_tick(wall, zone),
                "{zone} {wall}"
            );
        }
    }
}
