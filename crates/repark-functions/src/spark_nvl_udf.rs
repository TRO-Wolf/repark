use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use datafusion::arrow::compute::kernels::nullif::nullif as arrow_nullif;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::cast_map::spark_cast_ansi_zoned;
use crate::session_time_zone::session_time_zone_from_options;
use crate::spark_nvl::{coalesce_data_diff_types, if_data_diff_types, widen_full, wrong_num_args};

#[must_use]
pub fn nvl_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl::new("nvl")))
}

#[must_use]
pub fn ifnull_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl::new("ifnull")))
}

#[must_use]
pub fn nvl2_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl2::new()))
}

#[must_use]
pub fn nullif_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNullif::new()))
}

#[must_use]
pub fn nullif_fexpr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNullif::new_fexpr()))
}

#[must_use]
pub fn zeroifnull_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkZeroIfNull::new()))
}

#[must_use]
pub fn nullifzero_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNullIfZero::new()))
}

#[must_use]
pub fn nullif_pick_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(NullIfPick::new()))
}

#[must_use]
pub fn nvl_cast_udf(target: &DataType) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvlCast::new(target.clone())))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        nvl_udf(),
        ifnull_udf(),
        nvl2_udf(),
        nullif_udf(),
        zeroifnull_udf(),
        nullifzero_udf(),
        nullif_pick_udf(),
    ]
}

#[must_use]
pub fn nvl_expr(first: Expr, second: Expr) -> Expr {
    crate::expr_fn::call(nvl_udf(), vec![first, second])
}

#[must_use]
pub fn ifnull_expr(first: Expr, second: Expr) -> Expr {
    crate::expr_fn::call(ifnull_udf(), vec![first, second])
}

#[must_use]
pub fn nvl2_expr(test: Expr, first: Expr, second: Expr) -> Expr {
    crate::expr_fn::call(nvl2_udf(), vec![test, first, second])
}

#[must_use]
pub fn nullif_expr(first: Expr, second: Expr) -> Expr {
    crate::expr_fn::call(nullif_udf(), vec![first, second])
}

#[must_use]
pub fn zeroifnull_expr(arg: Expr) -> Expr {
    crate::expr_fn::call(zeroifnull_udf(), vec![arg])
}

#[must_use]
pub fn nullifzero_expr(arg: Expr) -> Expr {
    crate::expr_fn::call(nullifzero_udf(), vec![arg])
}

#[must_use]
pub fn nvl_cast_expr(value: Expr, target: &DataType) -> Expr {
    crate::expr_fn::call(nvl_cast_udf(target), vec![value])
}

#[must_use]
pub fn nvl_facade_udf(spelling: &'static str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl::new_facade(spelling)))
}

#[must_use]
pub fn nvl2_facade_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl2::new_facade()))
}

#[must_use]
pub fn zeroifnull_facade_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkZeroIfNull::new_facade()))
}

#[must_use]
pub fn nullifzero_facade_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNullIfZero::new_facade()))
}

#[allow(clippy::missing_errors_doc)]
pub fn nvl_family_facade_expr(name: &str, args: &[Expr]) -> Result<Expr, String> {
    let got = args.len();
    match name {
        "nvl" | "ifnull" => match args {
            [first, second] => Ok(crate::expr_fn::call(
                nvl_facade_udf(if name == "nvl" { "nvl" } else { "ifnull" }),
                vec![first.clone(), second.clone()],
            )),
            _ => Err(format!("expects 2 args, got {got}")),
        },
        "nvl2" => match args {
            [test, first, second] => Ok(crate::expr_fn::call(
                nvl2_facade_udf(),
                vec![test.clone(), first.clone(), second.clone()],
            )),
            _ => Err(format!("expects 3 args, got {got}")),
        },
        "nullif" => match args {
            [first, second] => Ok(nullif_expr(first.clone(), second.clone())),
            _ => Err(format!("expects 2 args, got {got}")),
        },
        "zeroifnull" => match args {
            [arg] => Ok(crate::expr_fn::call(
                zeroifnull_facade_udf(),
                vec![arg.clone()],
            )),
            _ => Err(format!("expects 1 args, got {got}")),
        },
        "nullifzero" => match args {
            [arg] => Ok(crate::expr_fn::call(
                nullifzero_facade_udf(),
                vec![arg.clone()],
            )),
            _ => Err(format!("expects 1 args, got {got}")),
        },
        _ => Err(format!("unknown nvl family name {name}")),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn nvl_family_expr(name: &str, args: &[Expr]) -> Result<Expr, String> {
    let got = args.len();
    match name {
        "nvl" | "ifnull" => match args {
            [first, second] => {
                if name == "nvl" {
                    Ok(nvl_expr(first.clone(), second.clone()))
                } else {
                    Ok(ifnull_expr(first.clone(), second.clone()))
                }
            }
            _ => Err(format!("expects 2 args, got {got}")),
        },
        "nvl2" => match args {
            [test, first, second] => Ok(nvl2_expr(test.clone(), first.clone(), second.clone())),
            _ => Err(format!("expects 3 args, got {got}")),
        },
        "nullif" => match args {
            [first, second] => Ok(nullif_expr(first.clone(), second.clone())),
            _ => Err(format!("expects 2 args, got {got}")),
        },
        "zeroifnull" => match args {
            [arg] => Ok(zeroifnull_expr(arg.clone())),
            _ => Err(format!("expects 1 args, got {got}")),
        },
        "nullifzero" => match args {
            [arg] => Ok(nullifzero_expr(arg.clone())),
            _ => Err(format!("expects 1 args, got {got}")),
        },
        _ => Err(format!("unknown nvl family name {name}")),
    }
}

fn materialize(values: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    let mut width = 0usize;
    let mut saw_array = false;
    for value in values {
        if let ColumnarValue::Array(array) = value {
            saw_array = true;
            width = width.max(array.len());
        }
    }
    if !saw_array {
        width = 1;
    }
    let mut arrays = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ColumnarValue::Scalar(scalar) => arrays.push(scalar.to_array_of_size(width)?),
            ColumnarValue::Array(array) => {
                if array.len() != width {
                    return exec_err!(
                        "nvl family got mismatched argument widths {} and {width}",
                        array.len()
                    );
                }
                arrays.push(Arc::clone(array));
            }
        }
    }
    Ok(arrays)
}

fn pick_rows(first: &ArrayRef, second: &ArrayRef, use_first: &[bool]) -> Result<ArrayRef> {
    let width = first.len();
    let combined = datafusion::arrow::compute::concat(&[first.as_ref(), second.as_ref()])?;
    let mut indices = Vec::with_capacity(width);
    for (row, take_first) in use_first.iter().enumerate() {
        let index = if *take_first { row } else { row + width };
        let index = u32::try_from(index).map_err(|_| {
            datafusion::common::DataFusionError::Execution("row index overflow".to_owned())
        })?;
        indices.push(index);
    }
    let indices = UInt32Array::from(indices);
    Ok(take(&combined, &indices, None)?)
}

fn row_is_null(array: &ArrayRef, row: usize) -> bool {
    matches!(array.data_type(), DataType::Null) || array.is_null(row)
}

fn first_present(first: &ArrayRef, second: &ArrayRef) -> Result<ArrayRef> {
    let mask: Vec<bool> = (0..first.len())
        .map(|row| !row_is_null(first, row))
        .collect();
    pick_rows(first, second, &mask)
}

fn pick_cast(
    first: &ArrayRef,
    second: &ArrayRef,
    use_first: &[bool],
    widen: &DataType,
    zone: &str,
) -> Result<ArrayRef> {
    let mut first_rows = Vec::new();
    let mut second_rows = Vec::new();
    for (row, take_first) in use_first.iter().enumerate() {
        let row = u32::try_from(row).map_err(|_| {
            datafusion::common::DataFusionError::Execution("row index overflow".to_owned())
        })?;
        if *take_first {
            first_rows.push(row);
        } else {
            second_rows.push(row);
        }
    }
    let taken_first = take(first.as_ref(), &UInt32Array::from(first_rows), None)?;
    let taken_second = take(second.as_ref(), &UInt32Array::from(second_rows), None)?;
    let now = Utc::now();
    let cast_first = spark_cast_ansi_zoned(&taken_first, widen, zone, now)?;
    let cast_second = spark_cast_ansi_zoned(&taken_second, widen, zone, now)?;
    let first_len = u32::try_from(cast_first.len()).map_err(|_| {
        datafusion::common::DataFusionError::Execution("row index overflow".to_owned())
    })?;
    let combined =
        datafusion::arrow::compute::concat(&[cast_first.as_ref(), cast_second.as_ref()])?;
    let mut first_pos = 0u32;
    let mut second_pos = 0u32;
    let mut indices = Vec::with_capacity(use_first.len());
    for take_first in use_first {
        if *take_first {
            indices.push(first_pos);
            first_pos += 1;
        } else {
            indices.push(first_len + second_pos);
            second_pos += 1;
        }
    }
    Ok(take(&combined, &UInt32Array::from(indices), None)?)
}

pub(crate) fn zero_scalar(data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int32 => Ok(ScalarValue::Int32(Some(0))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(0))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(0.0))),
        DataType::Decimal128(precision, scale) => {
            Ok(ScalarValue::Decimal128(Some(0), *precision, *scale))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(ScalarValue::Utf8(Some("0".to_owned())))
        }
        other => internal_err!("zeroifnull reached an unwidenable common type {other}"),
    }
}

fn case_when_present(test: Expr, first: Expr, second: Expr) -> Result<ExprSimplifyResult> {
    let built = CaseBuilder::new(
        None,
        vec![test.is_not_null()],
        vec![first],
        Some(Box::new(second)),
    )
    .end()?;
    Ok(ExprSimplifyResult::Simplified(built))
}

#[derive(Debug)]
pub(crate) struct SparkNvl {
    spelling: &'static str,
    signature: Signature,
    facade_built: bool,
}

impl SparkNvl {
    fn new(spelling: &'static str) -> Self {
        Self {
            spelling,
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: false,
        }
    }

    fn new_facade(spelling: &'static str) -> Self {
        Self {
            spelling,
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: true,
        }
    }

    pub(crate) fn is_facade_built(&self) -> bool {
        self.facade_built
    }
}

impl PartialEq for SparkNvl {
    fn eq(&self, other: &Self) -> bool {
        self.spelling == other.spelling
    }
}

impl Eq for SparkNvl {}

impl Hash for SparkNvl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.spelling.hash(state);
    }
}

impl ScalarUDFImpl for SparkNvl {
    fn name(&self) -> &str {
        self.spelling
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [first, second] = arg_types else {
            return Err(wrong_num_args(self.spelling, 2, arg_types.len()));
        };
        if let Some(declared) =
            crate::spark_nvl_core::core_declared(&datafusion::functions::core::nvl(), arg_types)
        {
            return Ok(declared);
        }
        Ok(widen_full(first, second).unwrap_or_else(|| first.clone()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [first, second] = args.arg_fields else {
            return Err(wrong_num_args(self.spelling, 2, args.arg_fields.len()));
        };
        if let Some(field) = crate::spark_nvl_core::core_field(
            &datafusion::functions::core::nvl(),
            self.spelling,
            &args,
        ) {
            return Ok(field);
        }
        let common = widen_full(first.data_type(), second.data_type())
            .unwrap_or_else(|| first.data_type().clone());
        let nullable = first.is_nullable() && second.is_nullable();
        Ok(Arc::new(Field::new(self.spelling, common, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(wrong_num_args(self.spelling, 2, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [_, _] = args.as_slice() else {
            return Err(wrong_num_args(self.spelling, 2, args.len()));
        };
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [first, second] = arrays.as_slice() else {
            return Err(wrong_num_args(self.spelling, 2, arrays.len()));
        };
        let widen = widen_full(first.data_type(), second.data_type())
            .ok_or_else(|| coalesce_data_diff_types(first.data_type(), second.data_type()))?;
        let mask: Vec<bool> = (0..first.len())
            .map(|row| !row_is_null(first, row))
            .collect();
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        Ok(ColumnarValue::Array(pick_cast(
            first, second, &mask, &widen, zone,
        )?))
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let [first, second] = args else {
            return None;
        };
        Some((vec![first], vec![second]))
    }

    fn short_circuits(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub(crate) struct SparkNvl2 {
    signature: Signature,
    facade_built: bool,
}

impl SparkNvl2 {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: false,
        }
    }

    fn new_facade() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: true,
        }
    }

    pub(crate) fn is_facade_built(&self) -> bool {
        self.facade_built
    }
}

impl PartialEq for SparkNvl2 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkNvl2 {}

impl Hash for SparkNvl2 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkNvl2 {
    crate::shim_udf_boilerplate!("nvl2");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, first, second] = arg_types else {
            return Err(wrong_num_args("nvl2", 3, arg_types.len()));
        };
        if let Some(declared) =
            crate::spark_nvl_core::core_declared(&datafusion::functions::core::nvl2(), arg_types)
        {
            return Ok(declared);
        }
        Ok(widen_full(first, second).unwrap_or_else(|| first.clone()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [_, first, second] = args.arg_fields else {
            return Err(wrong_num_args("nvl2", 3, args.arg_fields.len()));
        };
        if let Some(field) =
            crate::spark_nvl_core::core_field(&datafusion::functions::core::nvl2(), "nvl2", &args)
        {
            return Ok(field);
        }
        let common = widen_full(first.data_type(), second.data_type())
            .unwrap_or_else(|| first.data_type().clone());
        let nullable = first.is_nullable() || second.is_nullable();
        Ok(Arc::new(Field::new("nvl2", common, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(wrong_num_args("nvl2", 3, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [_, _, _] = args.as_slice() else {
            return Err(wrong_num_args("nvl2", 3, args.len()));
        };
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [test, first, second] = arrays.as_slice() else {
            return Err(wrong_num_args("nvl2", 3, arrays.len()));
        };
        let widen = widen_full(first.data_type(), second.data_type())
            .ok_or_else(|| if_data_diff_types(first.data_type(), second.data_type()))?;
        let mask: Vec<bool> = (0..test.len()).map(|row| !row_is_null(test, row)).collect();
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        Ok(ColumnarValue::Array(pick_cast(
            first, second, &mask, &widen, zone,
        )?))
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let [test, first, second] = args else {
            return None;
        };
        Some((vec![test], vec![first, second]))
    }

    fn short_circuits(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub(crate) struct SparkNullif {
    signature: Signature,
    fexpr_built: bool,
}

impl SparkNullif {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            fexpr_built: false,
        }
    }

    fn new_fexpr() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            fexpr_built: true,
        }
    }

    pub(crate) fn is_fexpr_built(&self) -> bool {
        self.fexpr_built
    }
}

impl PartialEq for SparkNullif {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkNullif {}

impl Hash for SparkNullif {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkNullif {
    crate::shim_udf_boilerplate!("nullif");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [first, _] = arg_types else {
            return Err(wrong_num_args("nullif", 2, arg_types.len()));
        };
        if let Some(declared) =
            crate::spark_nvl_core::core_declared(&datafusion::functions::core::nullif(), arg_types)
        {
            return Ok(declared);
        }
        Ok(first.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [first, _] = args.arg_fields else {
            return Err(wrong_num_args("nullif", 2, args.arg_fields.len()));
        };
        if let Some(field) = crate::spark_nvl_core::core_field(
            &datafusion::functions::core::nullif(),
            "nullif",
            &args,
        ) {
            return Ok(field);
        }
        Ok(Arc::new(Field::new(
            "nullif",
            first.data_type().clone(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(wrong_num_args("nullif", 2, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("nullif calls rewrite before execution and never invoke directly")
    }
}

#[derive(Debug)]
pub(crate) struct SparkZeroIfNull {
    signature: Signature,
    facade_built: bool,
}

impl SparkZeroIfNull {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: false,
        }
    }

    fn new_facade() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: true,
        }
    }

    pub(crate) fn is_facade_built(&self) -> bool {
        self.facade_built
    }
}

impl PartialEq for SparkZeroIfNull {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkZeroIfNull {}

impl Hash for SparkZeroIfNull {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkZeroIfNull {
    crate::shim_udf_boilerplate!("zeroifnull");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg] = arg_types else {
            return Err(wrong_num_args("zeroifnull", 1, arg_types.len()));
        };
        Ok(widen_full(arg, &DataType::Int32).unwrap_or_else(|| arg.clone()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [arg] = args.arg_fields else {
            return Err(wrong_num_args("zeroifnull", 1, args.arg_fields.len()));
        };
        let common = widen_full(arg.data_type(), &DataType::Int32)
            .unwrap_or_else(|| arg.data_type().clone());
        Ok(Arc::new(Field::new("zeroifnull", common, false)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(wrong_num_args("zeroifnull", 1, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [arg] = args.as_slice() else {
            return Err(wrong_num_args("zeroifnull", 1, args.len()));
        };
        let common = info.get_data_type(arg)?;
        let zero = Expr::Literal(zero_scalar(&common)?, None);
        case_when_present(arg.clone(), arg.clone(), zero)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [arg] = arrays.as_slice() else {
            return Err(wrong_num_args("zeroifnull", 1, arrays.len()));
        };
        let zero = zero_scalar(arg.data_type())?.to_array_of_size(arg.len())?;
        Ok(ColumnarValue::Array(first_present(arg, &zero)?))
    }
}

#[derive(Debug)]
pub(crate) struct SparkNullIfZero {
    signature: Signature,
    facade_built: bool,
}

impl SparkNullIfZero {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: false,
        }
    }

    fn new_facade() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            facade_built: true,
        }
    }

    pub(crate) fn is_facade_built(&self) -> bool {
        self.facade_built
    }
}

impl PartialEq for SparkNullIfZero {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkNullIfZero {}

impl Hash for SparkNullIfZero {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkNullIfZero {
    crate::shim_udf_boilerplate!("nullifzero");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg] = arg_types else {
            return Err(wrong_num_args("nullifzero", 1, arg_types.len()));
        };
        Ok(arg.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [arg] = args.arg_fields else {
            return Err(wrong_num_args("nullifzero", 1, args.arg_fields.len()));
        };
        Ok(Arc::new(Field::new(
            "nullifzero",
            arg.data_type().clone(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(wrong_num_args("nullifzero", 1, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("nullifzero calls rewrite before execution and never invoke directly")
    }
}

#[derive(Debug)]
struct NullIfPick {
    signature: Signature,
}

impl NullIfPick {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for NullIfPick {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NullIfPick {}

impl Hash for NullIfPick {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for NullIfPick {
    crate::shim_udf_boilerplate!("__repark_nullif_pick");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, value] = arg_types else {
            return Err(wrong_num_args("__repark_nullif_pick", 2, arg_types.len()));
        };
        Ok(value.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [_, value] = args.arg_fields else {
            return Err(wrong_num_args(
                "__repark_nullif_pick",
                2,
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            "__repark_nullif_pick",
            value.data_type().clone(),
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, value] = arg_types else {
            return Err(wrong_num_args("__repark_nullif_pick", 2, arg_types.len()));
        };
        Ok(vec![DataType::Boolean, value.clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [cond, value] = arrays.as_slice() else {
            return Err(wrong_num_args("__repark_nullif_pick", 2, arrays.len()));
        };
        let mask = cond
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "__repark_nullif_pick needs a boolean condition".to_owned(),
                )
            })?;
        Ok(ColumnarValue::Array(arrow_nullif(value.as_ref(), mask)?))
    }
}

#[derive(Debug)]
struct SparkNvlCast {
    target: DataType,
    signature: Signature,
}

impl SparkNvlCast {
    fn new(target: DataType) -> Self {
        Self {
            target,
            signature: Signature::user_defined(Volatility::Stable),
        }
    }
}

impl PartialEq for SparkNvlCast {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl Eq for SparkNvlCast {}

impl Hash for SparkNvlCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.target.hash(state);
    }
}

impl ScalarUDFImpl for SparkNvlCast {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "__repark_nvl_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(wrong_num_args("__repark_nvl_cast", 1, arg_types.len()));
        }
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [value] = args.arg_fields else {
            return Err(wrong_num_args(
                "__repark_nvl_cast",
                1,
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            "__repark_nvl_cast",
            self.target.clone(),
            value.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(wrong_num_args("__repark_nvl_cast", 1, arg_types.len()));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = materialize(&args.args)?;
        let [value] = arrays.as_slice() else {
            return Err(wrong_num_args("__repark_nvl_cast", 1, arrays.len()));
        };
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        Ok(ColumnarValue::Array(spark_cast_ansi_zoned(
            value,
            &self.target,
            zone,
            Utc::now(),
        )?))
    }
}
