use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    make_array, new_null_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::prelude::SessionContext;

pub const ZONE_SEAM_NAME: &str = "__repark_zone_seam__";
pub const TRY_ZONE_SEAM_NAME: &str = "__repark_try_zone_seam__";

static TO_UTC: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(SparkUtcShift::new(
        datafusion_spark::function::datetime::to_utc_timestamp(),
    )))
});
static FROM_UTC: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(SparkUtcShift::new(
        datafusion_spark::function::datetime::from_utc_timestamp(),
    )))
});
static ZONE_SEAM: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkZoneSeam::new(false))));
static TRY_ZONE_SEAM: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkZoneSeam::new(true))));

#[must_use]
pub fn zone_seam_udf(try_cast: bool) -> Arc<ScalarUDF> {
    if try_cast {
        Arc::clone(&TRY_ZONE_SEAM)
    } else {
        Arc::clone(&ZONE_SEAM)
    }
}

#[must_use]
pub fn to_utc_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::clone(&TO_UTC)
}

#[must_use]
pub fn from_utc_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::clone(&FROM_UTC)
}

pub fn register(ctx: &SessionContext) {
    ctx.register_udf(to_utc_timestamp_udf().as_ref().clone());
    ctx.register_udf(from_utc_timestamp_udf().as_ref().clone());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leaf {
    Wall,
    Text,
    Day,
    Seconds,
    Instant,
    RefusedSeconds,
    RefusedWall,
}

fn is_instant(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
    )
}

fn is_wall(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn seam_leaf(source: &DataType, target: &DataType) -> Option<Leaf> {
    if is_instant(source) && is_wall(target) {
        return Some(Leaf::Wall);
    }
    if (is_instant(source) || is_wall(source)) && is_text(target) {
        return Some(Leaf::Text);
    }
    if is_instant(source) && matches!(target, DataType::Date32) {
        return Some(Leaf::Day);
    }
    if is_instant(source) && matches!(target, DataType::Int64) {
        return Some(Leaf::Seconds);
    }
    if (is_wall(source) || is_text(source) || matches!(source, DataType::Date32))
        && is_instant(target)
    {
        return Some(Leaf::Instant);
    }
    if is_wall(source) && matches!(target, DataType::Int64) {
        return Some(Leaf::RefusedSeconds);
    }
    if matches!(source, DataType::Int64) && is_wall(target) {
        return Some(Leaf::RefusedWall);
    }
    None
}

fn is_container(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

fn converted_field(source: &Field, target: &Field) -> (Field, bool) {
    match partial_type(source.data_type(), target.data_type()) {
        Some(partial) => (Field::new(source.name(), partial, true), true),
        None => (source.clone(), false),
    }
}

fn partial_type(source: &DataType, target: &DataType) -> Option<DataType> {
    match (source, target) {
        (DataType::Struct(sources), DataType::Struct(targets))
            if sources.len() == targets.len() =>
        {
            let mut found = false;
            let fields: Vec<Field> = sources
                .iter()
                .zip(targets.iter())
                .map(|(source, target)| {
                    let (field, converted) = converted_field(source, target);
                    found |= converted;
                    field
                })
                .collect();
            found.then(|| DataType::Struct(Fields::from(fields)))
        }
        (DataType::List(source), DataType::List(target)) => {
            let (field, converted) = converted_field(source, target);
            converted.then(|| DataType::List(Arc::new(field)))
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => {
            let (field, converted) = converted_field(source, target);
            converted.then(|| DataType::LargeList(Arc::new(field)))
        }
        (DataType::FixedSizeList(source, size), DataType::FixedSizeList(target, other))
            if size == other =>
        {
            let (field, converted) = converted_field(source, target);
            converted.then(|| DataType::FixedSizeList(Arc::new(field), *size))
        }
        (DataType::Map(source, sorted), DataType::Map(target, _)) => {
            let (DataType::Struct(sources), DataType::Struct(targets)) =
                (source.data_type(), target.data_type())
            else {
                return None;
            };
            if sources.len() != 2 || targets.len() != 2 {
                return None;
            }
            let (value, converted) = converted_field(&sources[1], &targets[1]);
            let entries = DataType::Struct(Fields::from(vec![sources[0].as_ref().clone(), value]));
            converted.then(|| {
                DataType::Map(
                    Arc::new(Field::new(source.name(), entries, source.is_nullable())),
                    *sorted,
                )
            })
        }
        (source, target) if !is_container(source) => {
            seam_leaf(source, target).map(|_| target.clone())
        }
        _ => None,
    }
}

fn refuses(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Struct(sources), DataType::Struct(targets)) => sources
            .iter()
            .zip(targets.iter())
            .any(|(source, target)| refuses(source.data_type(), target.data_type())),
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target))
        | (DataType::FixedSizeList(source, _), DataType::FixedSizeList(target, _))
        | (DataType::Map(source, _), DataType::Map(target, _)) => {
            refuses(source.data_type(), target.data_type())
        }
        (source, target) => matches!(
            seam_leaf(source, target),
            Some(Leaf::RefusedSeconds | Leaf::RefusedWall)
        ),
    }
}

fn refusal(try_cast: bool, source: &DataType, target: &DataType) -> DataFusionError {
    let keyword = if try_cast { "TRY_CAST" } else { "CAST" };
    let source = crate::cast_map::spark_sql_name(source);
    let target = crate::cast_map::spark_sql_name(target);
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{keyword}(value AS \
         {target})\" due to data type mismatch: cannot cast \"{source}\" to \"{target}\". \
         SQLSTATE: 42K09"
    ))
}

fn seam_call(value: Expr, target: &DataType, try_cast: bool) -> Option<Expr> {
    let carrier = ScalarValue::try_new_null(target).ok()?;
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        zone_seam_udf(try_cast),
        vec![value, Expr::Literal(carrier, None)],
    )))
}

fn container_cast(
    value: &Expr,
    field: &FieldRef,
    schema: &DFSchema,
    try_cast: bool,
) -> Option<Expr> {
    let source = value.get_type(schema).ok()?;
    if !is_container(&source) {
        return None;
    }
    let partial = partial_type(&source, field.data_type())?;
    let seamed = seam_call(value.clone(), &partial, try_cast)?;
    if &partial == field.data_type() {
        return Some(seamed);
    }
    Some(if try_cast {
        Expr::TryCast(TryCast::new_from_field(Box::new(seamed), Arc::clone(field)))
    } else {
        Expr::Cast(Cast::new_from_field(Box::new(seamed), Arc::clone(field)))
    })
}

fn leaf_try_cast(cast: &TryCast, schema: &DFSchema) -> Option<Expr> {
    let source = cast.expr.get_type(schema).ok()?;
    let target = cast.field.data_type();
    if matches!(source, DataType::Int64) && is_instant(target) {
        let seconds = Expr::TryCast(TryCast::new(
            cast.expr.clone(),
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
        ));
        return Some(Expr::TryCast(TryCast::new_from_field(
            Box::new(seconds),
            Arc::clone(&cast.field),
        )));
    }
    match seam_leaf(&source, target)? {
        Leaf::Text | Leaf::Day | Leaf::Seconds => seam_call(*cast.expr.clone(), target, true),
        Leaf::Instant if !is_text(&source) => seam_call(*cast.expr.clone(), target, true),
        _ => None,
    }
}

fn map_cast(function: &ScalarFunction, schema: &DFSchema) -> Option<Expr> {
    let [value, target, rest @ ..] = function.args.as_slice() else {
        return None;
    };
    let Expr::Literal(ScalarValue::Utf8(Some(text)), _) = target else {
        return None;
    };
    let target_type: DataType = text.parse().ok()?;
    let try_cast = matches!(
        rest.first(),
        Some(Expr::Literal(ScalarValue::Boolean(Some(true)), _))
    );
    let source = value.get_type(schema).ok()?;
    let partial = partial_type(&source, &target_type)?;
    let mut args = function.args.clone();
    args[0] = seam_call(value.clone(), &partial, try_cast)?;
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&function.func),
        args,
    )))
}

#[must_use]
pub(crate) fn rewrite_zone_seam(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    match expr {
        Expr::Cast(cast) => container_cast(&cast.expr, &cast.field, schema, false),
        Expr::TryCast(cast) => container_cast(&cast.expr, &cast.field, schema, true)
            .or_else(|| leaf_try_cast(cast, schema)),
        Expr::ScalarFunction(function)
            if function.func.name() == crate::cast_map::CAST_MAP_NAME =>
        {
            map_cast(function, schema)
        }
        _ => None,
    }
}

fn masked(column: &ArrayRef, parent: Option<&NullBuffer>) -> Result<ArrayRef> {
    let Some(parent) = parent else {
        return Ok(Arc::clone(column));
    };
    let nulls = NullBuffer::union(Some(parent), column.nulls());
    let data = column.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}

fn leaf_udf(leaf: Leaf, source: &DataType, try_cast: bool) -> Option<Arc<ScalarUDF>> {
    match leaf {
        Leaf::Wall => Some(crate::timestamp_ntz_cast::timestamp_ntz_cast_udf(try_cast)),
        Leaf::Text => Some(crate::timestamp_cast::spark_timestamp_to_string_udf()),
        Leaf::Day => Some(crate::timestamp_cast::spark_timestamp_to_date_udf()),
        Leaf::Seconds => Some(crate::timestamp_cast::spark_epoch_seconds_floor_udf()),
        Leaf::Instant if try_cast && is_text(source) => {
            Some(crate::timestamp_ltz_ntz::try_to_timestamp_udf())
        }
        Leaf::Instant => Some(crate::instant_ts::to_timestamp_udf()),
        Leaf::RefusedSeconds | Leaf::RefusedWall => None,
    }
}

fn convert_leaf(
    array: &ArrayRef,
    target: &DataType,
    try_cast: bool,
    config: &Arc<ConfigOptions>,
) -> Result<ArrayRef> {
    let source = array.data_type();
    let udf = seam_leaf(source, target)
        .and_then(|leaf| leaf_udf(leaf, source, try_cast))
        .ok_or_else(|| refusal(try_cast, source, target))?;
    let arg_field: FieldRef = Arc::new(Field::new("value", source.clone(), true));
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: std::slice::from_ref(&arg_field),
        scalar_arguments: &[None],
    })?;
    let produced = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(array))],
            arg_fields: vec![arg_field],
            number_rows: array.len(),
            return_field,
            config_options: Arc::clone(config),
        })?
        .into_array(array.len())?;
    if produced.data_type() == target {
        return Ok(produced);
    }
    Ok(cast(&produced, target)?)
}

fn convert(
    array: &ArrayRef,
    target: &DataType,
    try_cast: bool,
    config: &Arc<ConfigOptions>,
) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| {
                    let column = masked(column, structs.nulls())?;
                    convert(&column, field.data_type(), try_cast, config)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                structs.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_list::<i32>();
            let values = convert(list.values(), field.data_type(), try_cast, config)?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            let list = array.as_list::<i64>();
            let values = convert(list.values(), field.data_type(), try_cast, config)?;
            Ok(Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::FixedSizeList(_, _), DataType::FixedSizeList(field, size)) => {
            let list = array.as_fixed_size_list();
            let values = convert(list.values(), field.data_type(), try_cast, config)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(entries, sorted)) => {
            let map = array.as_map();
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Internal(format!(
                    "'{ZONE_SEAM_NAME}' needs map entries, got {entries}"
                )));
            };
            let values = convert(map.values(), fields[1].data_type(), try_cast, config)?;
            let pairs =
                StructArray::try_new(fields.clone(), vec![Arc::clone(map.keys()), values], None)?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                map.offsets().clone(),
                pairs,
                map.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => convert_leaf(array, target, try_cast, config),
    }
}

#[derive(Debug)]
struct SparkZoneSeam {
    try_cast: bool,
    signature: Signature,
}

impl SparkZoneSeam {
    fn new(try_cast: bool) -> Self {
        Self {
            try_cast,
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkZoneSeam {
    fn eq(&self, other: &Self) -> bool {
        self.try_cast == other.try_cast
    }
}

impl Eq for SparkZoneSeam {}

impl Hash for SparkZoneSeam {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkZoneSeam {
    fn name(&self) -> &str {
        if self.try_cast {
            TRY_ZONE_SEAM_NAME
        } else {
            ZONE_SEAM_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.get(1).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("'{}' expects a value and a target", self.name()))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [value, target] = args.arg_fields else {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects a value and a target",
                self.name()
            )));
        };
        if refuses(value.data_type(), target.data_type()) {
            return Err(refusal(
                self.try_cast,
                value.data_type(),
                target.data_type(),
            ));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            target.data_type().clone(),
            value.is_nullable() || self.try_cast,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target = args.return_field.data_type().clone();
        let Some(value) = args.args.first() else {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects a value and a target",
                self.name()
            )));
        };
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(convert(
                array,
                &target,
                self.try_cast,
                &args.config_options,
            )?)),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let converted = convert(&array, &target, self.try_cast, &args.config_options)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &converted, 0,
                )?))
            }
        }
    }
}

fn shifts_first(data_type: &DataType) -> bool {
    is_wall(data_type) || matches!(data_type, DataType::Date32 | DataType::Null)
}

#[derive(Debug)]
struct SparkUtcShift {
    inner: Arc<ScalarUDF>,
    signature: Signature,
}

impl SparkUtcShift {
    fn new(inner: Arc<ScalarUDF>) -> Self {
        Self {
            inner,
            signature: Signature::user_defined(Volatility::Stable),
        }
    }

    fn instant_field(field: &FieldRef) -> FieldRef {
        Arc::new(Field::new(
            field.name(),
            crate::instant_ts::ltz_timestamp_type(),
            true,
        ))
    }
}

impl PartialEq for SparkUtcShift {
    fn eq(&self, other: &Self) -> bool {
        self.inner.name() == other.inner.name()
    }
}

impl Eq for SparkUtcShift {}

impl Hash for SparkUtcShift {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkUtcShift {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let fields: Vec<FieldRef> = arg_types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                let data_type = if index == 0 && shifts_first(data_type) {
                    crate::instant_ts::ltz_timestamp_type()
                } else {
                    data_type.clone()
                };
                Arc::new(Field::new(format!("arg_{index}"), data_type, true))
            })
            .collect();
        let coerced = fields_with_udf(&fields, self.inner.as_ref())?;
        Ok(coerced
            .iter()
            .zip(arg_types)
            .enumerate()
            .map(|(index, (field, original))| {
                if index == 0 && shifts_first(original) {
                    original.clone()
                } else {
                    field.data_type().clone()
                }
            })
            .collect())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(format!(
            "'{}' answers its type from return_field_from_args",
            self.name()
        )))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let fields: Vec<FieldRef> = args
            .arg_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index == 0 && shifts_first(field.data_type()) {
                    Self::instant_field(field)
                } else {
                    Arc::clone(field)
                }
            })
            .collect();
        self.inner.return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: args.scalar_arguments,
        })
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return self.inner.invoke_with_args(args);
        };
        let source = first.data_type();
        if !shifts_first(&source) {
            return self.inner.invoke_with_args(args);
        }
        let values = first.to_array(args.number_rows)?;
        let instants = if matches!(source, DataType::Null) {
            new_null_array(&crate::instant_ts::ltz_timestamp_type(), values.len())
        } else {
            convert_leaf(
                &values,
                &crate::instant_ts::ltz_timestamp_type(),
                false,
                &args.config_options,
            )?
        };
        args.args[0] = ColumnarValue::Array(instants);
        args.arg_fields[0] = Self::instant_field(&args.arg_fields[0]);
        self.inner.invoke_with_args(args)
    }
}
