use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float32Builder, Float64Builder};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column as DFColumn, DFSchema, exec_err};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DFResult;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::{ScalarFunction, WindowFunction};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprFunctionExt, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, SortExpr, Volatility, WindowFrame, WindowFunctionDefinition, lit,
};
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;

use crate::datafusion_to_py_err;
use crate::fence::fenced;

pub(crate) const MASK_NAME: &str = "__repark_is_duplicated__";
pub(crate) const KEY_NAME: &str = "__repark_is_duplicated_key__";
const INDEX_BASE: &str = "__repark_is_duplicated_index__";
const HELPER_BASE: &str = "__repark_is_duplicated_mask__";

#[derive(Debug)]
struct DupMask {
    signature: Signature,
}

impl DupMask {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl PartialEq for DupMask {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DupMask {}

impl Hash for DupMask {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl ScalarUDFImpl for DupMask {
    fn name(&self) -> &'static str {
        MASK_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("'{MASK_NAME}' requires 1 argument");
        };
        Ok(input.clone())
    }
}

#[derive(Debug)]
struct DupKey {
    signature: Signature,
}

impl DupKey {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for DupKey {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DupKey {}

impl Hash for DupKey {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl ScalarUDFImpl for DupKey {
    fn name(&self) -> &'static str {
        KEY_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("'{KEY_NAME}' requires 1 argument");
        };
        match input {
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar(normalize_scalar(scalar.clone())))
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(normalize_array(array))),
        }
    }
}

pub(crate) fn dup_mask_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DupMask::new()))
}

pub(crate) fn dup_key_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DupKey::new()))
}

fn normalize_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else {
        value + 0.0
    }
}

fn normalize_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value + 0.0
    }
}

fn normalize_scalar(scalar: ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::Float32(Some(value)) => ScalarValue::Float32(Some(normalize_f32(value))),
        ScalarValue::Float64(Some(value)) => ScalarValue::Float64(Some(normalize_f64(value))),
        other => other,
    }
}

#[allow(clippy::missing_panics_doc)]
fn normalize_array(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => {
            let values = array.as_primitive::<Float32Type>();
            let mut builder = Float32Builder::with_capacity(values.len());
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(normalize_f32(values.value(row)));
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let values = array.as_primitive::<Float64Type>();
            let mut builder = Float64Builder::with_capacity(values.len());
            for row in 0..values.len() {
                if values.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(normalize_f64(values.value(row)));
                }
            }
            Arc::new(builder.finish())
        }
        _ => Arc::clone(array),
    }
}

fn mask_inner(node: &Expr) -> Option<Expr> {
    match node {
        Expr::ScalarFunction(ScalarFunction { func, args })
            if func.name() == MASK_NAME && args.len() == 1 =>
        {
            Some(args[0].clone())
        }
        _ => None,
    }
}

fn collect_masks(node: &Expr, found: &mut Vec<Expr>) {
    let _ = node.clone().transform(|step| {
        if mask_inner(&step).is_some() {
            found.push(step.clone());
        }
        Ok(Transformed::no(step))
    });
}

fn contains_mask(node: &Expr) -> bool {
    let mut found = Vec::new();
    collect_masks(node, &mut found);
    !found.is_empty()
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
fn substitute_masks(expr: Expr, helpers: &HashMap<Expr, String>) -> DFResult<Expr> {
    expr.transform(|step| {
        if let Some(name) = helpers.get(&step) {
            Ok(Transformed::yes(Expr::Column(DFColumn::new_unqualified(
                name,
            ))))
        } else {
            Ok(Transformed::no(step))
        }
    })
    .map(|done| done.data)
}

fn fresh_name(schema: &DFSchema, base: &str) -> String {
    if !schema.has_column_with_unqualified_name(base) {
        return base.to_string();
    }
    let mut counter = 1u32;
    loop {
        let candidate = format!("{base}_{counter}");
        if !schema.has_column_with_unqualified_name(&candidate) {
            return candidate;
        }
        counter += 1;
    }
}

fn order_key_columns_usable(keys: &[SortExpr], schema: &DFSchema) -> bool {
    let mut usable = true;
    for key in keys {
        let _ = key.expr.clone().transform(|step| {
            match &step {
                Expr::Column(column) => {
                    if schema.index_of_column(column).is_err() {
                        usable = false;
                    }
                }
                Expr::WindowFunction(_)
                | Expr::AggregateFunction(_)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_) => {
                    usable = false;
                }
                _ => {}
            }
            Ok(Transformed::no(step))
        });
    }
    usable
}

fn input_order_keys(plan: &LogicalPlan, schema: &DFSchema) -> Vec<SortExpr> {
    let mut current = plan;
    loop {
        match current {
            LogicalPlan::Sort(sort) => {
                if order_key_columns_usable(&sort.expr, schema) {
                    return sort.expr.clone();
                }
                return Vec::new();
            }
            LogicalPlan::Filter(filter) => current = &filter.input,
            LogicalPlan::Projection(projection) => current = &projection.input,
            LogicalPlan::SubqueryAlias(alias) => current = &alias.input,
            LogicalPlan::Limit(limit) => current = &limit.input,
            _ => return Vec::new(),
        }
    }
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
fn row_index_expr(order_keys: Vec<SortExpr>) -> DFResult<Expr> {
    Expr::from(WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(row_number_udwf()),
        Vec::new(),
    ))
    .partition_by(vec![lit(1)])
    .order_by(order_keys)
    .window_frame(WindowFrame::new(None))
    .build()
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
fn expand_root(
    staged: DataFrame,
    root: Expr,
    helper_count: &mut usize,
) -> DFResult<(DataFrame, Expr, Vec<String>)> {
    let mut frame = staged;
    let mut current = root;
    let mut helpers = Vec::new();
    loop {
        let mut found = Vec::new();
        collect_masks(&current, &mut found);
        let mut round = Vec::new();
        for marker in &found {
            if let Some(inner) = mask_inner(marker)
                && !contains_mask(&inner)
                && !round.contains(marker)
            {
                round.push(marker.clone());
            }
        }
        if round.is_empty() {
            break;
        }
        let mut mapping = HashMap::new();
        for marker in &round {
            let slot = *helper_count;
            *helper_count += 1;
            let name = fresh_name(frame.schema(), &format!("{HELPER_BASE}_{slot}"));
            if let Some(inner) = mask_inner(marker) {
                frame = frame.with_column(&name, inner)?;
                mapping.insert(marker.clone(), name.clone());
                helpers.push(name);
            }
        }
        current = substitute_masks(current, &mapping)?;
    }
    Ok((frame, current, helpers))
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub(crate) fn filter_frame(frame: &DataFrame, predicate: Expr) -> PyResult<DataFrame> {
    fenced!("is_duplicated.filter_frame", {
        if !contains_mask(&predicate) {
            return frame
                .clone()
                .filter(predicate)
                .map_err(datafusion_to_py_err);
        }
        let index = fresh_name(frame.schema(), INDEX_BASE);
        let order_keys = input_order_keys(frame.logical_plan(), frame.schema());
        let indexed = frame
            .clone()
            .with_column(
                &index,
                row_index_expr(order_keys).map_err(datafusion_to_py_err)?,
            )
            .map_err(datafusion_to_py_err)?;
        let (staged, rewritten, helpers) =
            expand_root(indexed, predicate, &mut 0).map_err(datafusion_to_py_err)?;
        let mut drops = helpers
            .iter()
            .map(DFColumn::new_unqualified)
            .collect::<Vec<_>>();
        drops.push(DFColumn::new_unqualified(&index));
        staged
            .filter(rewritten)
            .map_err(datafusion_to_py_err)?
            .sort(vec![
                Expr::Column(DFColumn::new_unqualified(&index)).sort(true, true),
            ])
            .map_err(datafusion_to_py_err)?
            .drop_columns(&drops)
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
pub(crate) fn select_frame(frame: &DataFrame, exprs: Vec<Expr>) -> PyResult<DataFrame> {
    fenced!("is_duplicated.select_frame", {
        if !exprs.iter().any(contains_mask) {
            return frame.clone().select(exprs).map_err(datafusion_to_py_err);
        }
        let index = fresh_name(frame.schema(), INDEX_BASE);
        let order_keys = input_order_keys(frame.logical_plan(), frame.schema());
        let mut staged = frame
            .clone()
            .with_column(
                &index,
                row_index_expr(order_keys).map_err(datafusion_to_py_err)?,
            )
            .map_err(datafusion_to_py_err)?;
        let mut helper_count = 0;
        let mut rewritten = Vec::with_capacity(exprs.len() + 1);
        for expr in exprs {
            let (next, one, _) =
                expand_root(staged, expr, &mut helper_count).map_err(datafusion_to_py_err)?;
            staged = next;
            rewritten.push(one);
        }
        rewritten.push(Expr::Column(DFColumn::new_unqualified(&index)));
        staged
            .select(rewritten)
            .map_err(datafusion_to_py_err)?
            .sort(vec![
                Expr::Column(DFColumn::new_unqualified(&index)).sort(true, true),
            ])
            .map_err(datafusion_to_py_err)?
            .drop_columns(&[DFColumn::new_unqualified(&index)])
            .map_err(datafusion_to_py_err)
    })
}

#[cfg(test)]
mod dup_key_tests {
    use super::*;

    use arrow::array::{BooleanArray, Float32Array, Float64Array, StringArray};

    fn array_value(udf: &ScalarUDF, array: ArrayRef) -> ArrayRef {
        let data_type = array.data_type().clone();
        let scalar_args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(arrow::datatypes::Field::new("r", data_type, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        };
        match udf.invoke_with_args(scalar_args).expect("invoke") {
            ColumnarValue::Array(done) => done,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }
    }

    #[test]
    fn names_are_stable() {
        assert_eq!(dup_mask_udf().name(), MASK_NAME);
        assert_eq!(dup_key_udf().name(), KEY_NAME);
        assert_eq!(dup_mask_udf().signature().volatility, Volatility::Immutable);
        assert_eq!(dup_key_udf().signature().volatility, Volatility::Immutable);
    }

    #[test]
    fn mask_returns_its_input() {
        let udf = dup_mask_udf();
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        let done = array_value(&udf, Arc::clone(&input));
        assert_eq!(done.as_ref(), input.as_ref());
    }

    #[test]
    fn key_normalizes_signed_zero_and_nan() {
        let udf = dup_key_udf();
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            Some(0.0),
            Some(-0.0),
            None,
        ]));
        let done = array_value(&udf, input);
        let values = done.as_primitive::<Float64Type>();
        assert!(!values.is_null(0));
        assert_eq!(values.value(0).to_bits(), 1.0f64.to_bits());
        assert_eq!(values.value(1).to_bits(), f64::NAN.to_bits());
        assert_eq!(values.value(2).to_bits(), 0.0f64.to_bits());
        assert_eq!(values.value(3).to_bits(), 0.0f64.to_bits());
        assert!(values.is_null(4));
        let input32: ArrayRef = Arc::new(Float32Array::from(vec![Some(-0.0f32), Some(f32::NAN)]));
        let done32 = array_value(&udf, input32);
        let values32 = done32.as_primitive::<Float32Type>();
        assert_eq!(values32.value(0).to_bits(), 0.0f32.to_bits());
        assert_eq!(values32.value(1).to_bits(), f32::NAN.to_bits());
    }

    #[test]
    fn key_passes_other_types_through() {
        let udf = dup_key_udf();
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let done = array_value(&udf, Arc::clone(&input));
        assert_eq!(done.as_ref(), input.as_ref());
    }

    #[test]
    fn key_normalizes_scalars() {
        assert_eq!(
            normalize_scalar(ScalarValue::Float64(Some(-0.0))),
            ScalarValue::Float64(Some(0.0))
        );
        assert_eq!(
            normalize_scalar(ScalarValue::Float32(Some(f32::NAN))),
            ScalarValue::Float32(Some(f32::NAN))
        );
        assert_eq!(
            normalize_scalar(ScalarValue::Int32(Some(7))),
            ScalarValue::Int32(Some(7))
        );
    }
}
