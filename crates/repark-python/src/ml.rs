//! Native ML fit bindings stream DataFusion batches into `repark-ml` accumulators.
//! Python does not train. Fits retain parameters and use O(batch + p² + k·p) memory.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, LargeListArray, ListArray, RecordBatch,
};
use arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use repark_ml::{
    KMeansPass, LinearRegressionAccumulator, MAX_FEATURES, MlError, fit_kmeans_lloyd,
    fit_logistic_irls, random_center_indices, validate_init_mode,
    validate_linear_regression_params,
};
use tokio::runtime::Runtime;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced;
use crate::{IllegalArgumentException, UnsupportedOperationException};

/// Map an ML error to a PySpark-shaped Python exception.
#[allow(clippy::needless_pass_by_value)]
fn ml_to_py_err(err: MlError) -> PyErr {
    let message = err.to_string();
    match &err {
        MlError::Unsupported(_)
        | MlError::ElasticNetUnsupported { .. }
        | MlError::StandardizationUnsupported
        | MlError::KMeansInitModeDefault
        | MlError::KMeansInitModeUnsupported { .. } => {
            UnsupportedOperationException::new_err(message)
        }
        _ => IllegalArgumentException::new_err(message),
    }
}

/// Open `execute_stream` on a clone of the held plan without collecting it.
fn open_stream(plan: &DataFrame, runtime: &Runtime) -> Result<SendableRecordBatchStream, MlError> {
    runtime
        .block_on(plan.clone().execute_stream())
        .map_err(|err| MlError::IllegalArgument(format!("execute_stream: {err}")))
}

/// Drain a stream and invoke `on_batch` for each batch.
fn for_each_batch<F>(
    runtime: &Runtime,
    mut stream: SendableRecordBatchStream,
    mut on_batch: F,
) -> Result<(), MlError>
where
    F: FnMut(&RecordBatch) -> Result<(), MlError>,
{
    loop {
        match runtime.block_on(stream.next()) {
            None => break,
            Some(Ok(batch)) => on_batch(&batch)?,
            Some(Err(err)) => {
                return Err(MlError::IllegalArgument(format!("stream batch: {err}")));
            }
        }
    }
    Ok(())
}

fn column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, MlError> {
    batch.column_by_name(name).ok_or_else(|| {
        MlError::IllegalArgument(format!(
            "repark.ml fit: column {name:?} not found in batch schema"
        ))
    })
}

fn scalar_f64(
    array: &dyn Array,
    index: usize,
    what: &'static str,
    row_offset: u64,
) -> Result<f64, MlError> {
    if array.is_null(index) {
        return Err(MlError::NonFinite { what, row_offset });
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        let value = values.value(index);
        if !value.is_finite() {
            return Err(MlError::NonFinite { what, row_offset });
        }
        return Ok(value);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        // Estimators consume f64; Int64 values beyond its exact range can round.
        #[allow(clippy::cast_precision_loss)]
        return Ok(values.value(index) as f64);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(f64::from(values.value(index)));
    }
    Err(MlError::IllegalArgument(format!(
        "repark.ml fit: {what} column has unsupported type {:?} (need float64/int)",
        array.data_type()
    )))
}

fn read_f64_values(
    values: &dyn Array,
    expected_width: Option<usize>,
    row_offset: u64,
) -> Result<Vec<f64>, MlError> {
    let len = values.len();
    if let Some(expected) = expected_width
        && len != expected
    {
        return Err(MlError::FeatureWidthMismatch {
            expected,
            actual: len,
        });
    }
    if len > MAX_FEATURES {
        return Err(MlError::FeatureDimTooLarge {
            actual: len,
            limit: MAX_FEATURES,
        });
    }
    // An empty list is valid for an intercept-only design.
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(len);
    if let Some(floats) = values.as_any().downcast_ref::<Float64Array>() {
        for index in 0..len {
            if floats.is_null(index) {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset,
                });
            }
            let value = floats.value(index);
            if !value.is_finite() {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset,
                });
            }
            out.push(value);
        }
        return Ok(out);
    }
    if let Some(ints) = values.as_any().downcast_ref::<Int64Array>() {
        for index in 0..len {
            if ints.is_null(index) {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset,
                });
            }
            #[allow(clippy::cast_precision_loss)]
            let casted = ints.value(index) as f64;
            out.push(casted);
        }
        return Ok(out);
    }
    if let Some(ints) = values.as_any().downcast_ref::<Int32Array>() {
        for index in 0..len {
            if ints.is_null(index) {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset,
                });
            }
            out.push(f64::from(ints.value(index)));
        }
        return Ok(out);
    }
    Err(MlError::IllegalArgument(format!(
        "repark.ml fit: list value type {:?} unsupported (need float64/int)",
        values.data_type()
    )))
}

fn dense_features_at(
    array: &dyn Array,
    index: usize,
    expected_width: Option<usize>,
    row_offset: u64,
) -> Result<Vec<f64>, MlError> {
    if array.is_null(index) {
        return Err(MlError::NonFinite {
            what: "feature",
            row_offset,
        });
    }
    match array.data_type() {
        DataType::FixedSizeList(_, _) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .ok_or_else(|| MlError::IllegalArgument("FixedSizeList downcast failed".into()))?;
            read_f64_values(list.value(index).as_ref(), expected_width, row_offset)
        }
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| MlError::IllegalArgument("List downcast failed".into()))?;
            read_f64_values(list.value(index).as_ref(), expected_width, row_offset)
        }
        DataType::LargeList(_) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| MlError::IllegalArgument("LargeList downcast failed".into()))?;
            read_f64_values(list.value(index).as_ref(), expected_width, row_offset)
        }
        DataType::Float64 | DataType::Int64 | DataType::Int32 => {
            let value = scalar_f64(array, index, "feature", row_offset)?;
            let width = expected_width.unwrap_or(1);
            if width != 1 {
                return Err(MlError::FeatureWidthMismatch {
                    expected: width,
                    actual: 1,
                });
            }
            Ok(vec![value])
        }
        other => {
            // Native estimators require dense List or FixedSizeList input.
            let sparse_hint = match other {
                DataType::Struct(fields)
                    if fields.iter().any(|field| field.name() == "size")
                        && fields.iter().any(|field| field.name() == "indices")
                        && fields.iter().any(|field| field.name() == "values") =>
                {
                    " — sparse VectorUDT {size,indices,values} is not accepted by native \
                     estimators; use dense VectorAssembler (sparseOutput=False) or densify \
                     before fit (ext densify caps apply only on repark.ml.ext)"
                }
                DataType::Struct(_) => {
                    " — struct features unsupported; native estimators need dense \
                     List/FixedSizeList of float64 (sparse VectorUDT densify is not on \
                     the native path)"
                }
                _ => "",
            };
            Err(MlError::IllegalArgument(format!(
                "repark.ml fit: features column type {other:?} unsupported \
                 (need List/FixedSizeList of float64, or a scalar numeric column)\
                 {sparse_hint}"
            )))
        }
    }
}

fn discover_feature_width(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
) -> Result<usize, MlError> {
    Ok(discover_feature_width_and_count(plan, runtime, features_col)?.0)
}

/// Infer feature width from the first non-null row and count valid rows.
fn discover_feature_width_and_count(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
) -> Result<(usize, u64), MlError> {
    let stream = open_stream(plan, runtime)?;
    let mut width = None;
    let mut num_valid = 0_u64;
    for_each_batch(runtime, stream, |batch| {
        let features = column_by_name(batch, features_col)?;
        for index in 0..batch.num_rows() {
            if features.is_null(index) {
                continue;
            }
            // The first valid row fixes the feature width.
            let row = dense_features_at(features.as_ref(), index, width, num_valid)?;
            if width.is_none() {
                width = Some(row.len());
            }
            num_valid += 1;
        }
        Ok(())
    })?;
    let num_features = width
        .ok_or_else(|| MlError::EmptyDesign("no non-null feature rows to infer width".into()))?;
    Ok((num_features, num_valid))
}

fn stream_xy_into_ols(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
    label_col: &str,
    num_features: usize,
    acc: &mut LinearRegressionAccumulator,
) -> Result<(), MlError> {
    let stream = open_stream(plan, runtime)?;
    let mut row_offset = 0_u64;
    for_each_batch(runtime, stream, |batch| {
        let features_arr = column_by_name(batch, features_col)?;
        let label_arr = column_by_name(batch, label_col)?;
        for index in 0..batch.num_rows() {
            let features =
                dense_features_at(features_arr.as_ref(), index, Some(num_features), row_offset)?;
            let label = scalar_f64(label_arr.as_ref(), index, "label", row_offset)?;
            acc.observe_row(&features, label)?;
            row_offset += 1;
        }
        Ok(())
    })
}

/// Fit linear regression from streamed batches and return model parameters.
///
/// # Errors
/// Returns `IllegalArgumentException` for invalid parameters, schema, values, or stream errors.
/// Returns `UnsupportedOperationException` when elastic net or standardization is requested.
#[pyfunction]
#[pyo3(signature = (
    frame,
    features_col,
    label_col,
    fit_intercept=true,
    elastic_net_param=0.0,
    standardization=false,
))]
#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
pub fn fit_linear_regression(
    py: Python<'_>,
    frame: PyRef<'_, PyDataFrame>,
    features_col: String,
    label_col: String,
    fit_intercept: bool,
    elastic_net_param: f64,
    standardization: bool,
) -> PyResult<Py<PyDict>> {
    fenced!("ml.fit_linear_regression", {
        validate_linear_regression_params(elastic_net_param, standardization)
            .map_err(ml_to_py_err)?;
        // Clone Rust-owned state before releasing the GIL; PyRef is not Send.
        let plan = frame.inner().clone();
        let runtime: Arc<Runtime> = frame.runtime_handle();
        let solution = py.detach(|| {
            let num_features = discover_feature_width(&plan, runtime.as_ref(), &features_col)
                .map_err(ml_to_py_err)?;
            let mut acc = LinearRegressionAccumulator::new(num_features, fit_intercept)
                .map_err(ml_to_py_err)?;
            stream_xy_into_ols(
                &plan,
                runtime.as_ref(),
                &features_col,
                &label_col,
                num_features,
                &mut acc,
            )
            .map_err(ml_to_py_err)?;
            acc.finish().map_err(ml_to_py_err)
        })?;

        let dict = PyDict::new(py);
        dict.set_item("coefficients", solution.coefficients)?;
        dict.set_item("intercept", solution.intercept)?;
        dict.set_item("fit_intercept", solution.fit_intercept)?;
        dict.set_item("num_features", solution.num_features)?;
        dict.set_item("num_rows", solution.num_rows)?;
        Ok(dict.unbind())
    })
}

/// Fit logistic regression with multi-pass IRLS over the streamed plan.
///
/// # Errors
/// Returns `IllegalArgumentException` for invalid tolerance, schema, values, or stream errors.
#[pyfunction]
#[pyo3(signature = (
    frame,
    features_col,
    label_col,
    fit_intercept=true,
    max_iter=100,
    tol=1e-6,
))]
#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
pub fn fit_logistic_regression(
    py: Python<'_>,
    frame: PyRef<'_, PyDataFrame>,
    features_col: String,
    label_col: String,
    fit_intercept: bool,
    max_iter: usize,
    tol: f64,
) -> PyResult<Py<PyDict>> {
    fenced!("ml.fit_logistic_regression", {
        let plan = frame.inner().clone();
        let runtime: Arc<Runtime> = frame.runtime_handle();
        let solution = py.detach(|| {
            let (num_features, num_valid) =
                discover_feature_width_and_count(&plan, runtime.as_ref(), &features_col)
                    .map_err(ml_to_py_err)?;
            let mut solution =
                fit_logistic_irls(num_features, fit_intercept, max_iter, tol, |acc| {
                    let stream = open_stream(&plan, runtime.as_ref())?;
                    let mut row_offset = 0_u64;
                    for_each_batch(runtime.as_ref(), stream, |batch| {
                        let features_arr = column_by_name(batch, &features_col)?;
                        let label_arr = column_by_name(batch, &label_col)?;
                        for index in 0..batch.num_rows() {
                            let features = dense_features_at(
                                features_arr.as_ref(),
                                index,
                                Some(num_features),
                                row_offset,
                            )?;
                            let label = scalar_f64(label_arr.as_ref(), index, "label", row_offset)?;
                            acc.observe_row(&features, label)?;
                            row_offset += 1;
                        }
                        Ok(())
                    })
                })
                .map_err(ml_to_py_err)?;
            // Zero iterations infer and count valid feature rows without inspecting labels or
            // running IRLS.
            if solution.iterations == 0 && solution.num_rows == 0 {
                solution.num_rows = num_valid;
            }
            Ok::<_, PyErr>(solution)
        })?;

        let dict = PyDict::new(py);
        dict.set_item("coefficients", solution.coefficients)?;
        dict.set_item("intercept", solution.intercept)?;
        dict.set_item("fit_intercept", solution.fit_intercept)?;
        dict.set_item("num_features", solution.num_features)?;
        dict.set_item("num_rows", solution.num_rows)?;
        dict.set_item("iterations", solution.iterations)?;
        dict.set_item("converged", solution.converged)?;
        Ok(dict.unbind())
    })
}

/// Count valid feature rows and infer their width.
fn kmeans_count_valid(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
) -> Result<(usize, u64), MlError> {
    let stream = open_stream(plan, runtime)?;
    let mut width = None;
    let mut num_valid = 0_u64;
    for_each_batch(runtime, stream, |batch| {
        let features_arr = column_by_name(batch, features_col)?;
        for index in 0..batch.num_rows() {
            if features_arr.is_null(index) {
                continue;
            }
            let row = dense_features_at(features_arr.as_ref(), index, width, num_valid)?;
            if width.is_none() {
                width = Some(row.len());
            }
            num_valid += 1;
        }
        Ok(())
    })?;
    let num_features =
        width.ok_or_else(|| MlError::EmptyDesign("KMeans: no non-null feature rows".into()))?;
    Ok((num_features, num_valid))
}

/// Materialize initial centers from valid feature rows.
fn kmeans_materialize_centers(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
    num_features: usize,
    indices: &[u64],
    k: usize,
) -> Result<Vec<Vec<f64>>, MlError> {
    let mut centers: Vec<Option<Vec<f64>>> = vec![None; k];
    let stream = open_stream(plan, runtime)?;
    let mut valid_index = 0_u64;
    let mut remaining = k;
    for_each_batch(runtime, stream, |batch| {
        if remaining == 0 {
            return Ok(());
        }
        let features_arr = column_by_name(batch, features_col)?;
        for index in 0..batch.num_rows() {
            if features_arr.is_null(index) {
                continue;
            }
            let features = dense_features_at(
                features_arr.as_ref(),
                index,
                Some(num_features),
                valid_index,
            )?;
            if let Some(slot) = indices.iter().position(|&picked| picked == valid_index) {
                centers[slot] = Some(features);
                remaining = remaining.saturating_sub(1);
            }
            valid_index += 1;
        }
        Ok(())
    })?;
    centers
        .into_iter()
        .enumerate()
        .map(|(slot, center)| {
            center.ok_or_else(|| {
                MlError::IllegalArgument(format!(
                    "KMeans failed to materialize initial center slot {slot}"
                ))
            })
        })
        .collect()
}

/// Run one Lloyd pass over streamed feature rows.
fn kmeans_stream_pass(
    plan: &DataFrame,
    runtime: &Runtime,
    features_col: &str,
    num_features: usize,
    pass: &mut KMeansPass,
) -> Result<(), MlError> {
    let stream = open_stream(plan, runtime)?;
    let mut row_offset = 0_u64;
    for_each_batch(runtime, stream, |batch| {
        let features_arr = column_by_name(batch, features_col)?;
        for index in 0..batch.num_rows() {
            if features_arr.is_null(index) {
                continue;
            }
            let features =
                dense_features_at(features_arr.as_ref(), index, Some(num_features), row_offset)?;
            pass.observe_row(&features)?;
            row_offset += 1;
        }
        Ok(())
    })
}

/// Fit KMeans with Lloyd iterations. Only random initialization is supported.
///
/// # Errors
/// Returns `IllegalArgumentException` for invalid dimensions, schema, values, or stream errors.
/// Returns `UnsupportedOperationException` for unsupported initialization modes.
#[pyfunction]
#[pyo3(signature = (
    frame,
    features_col,
    k=2,
    max_iter=20,
    seed=42,
    init_mode="random",
))]
#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
pub fn fit_kmeans(
    py: Python<'_>,
    frame: PyRef<'_, PyDataFrame>,
    features_col: String,
    k: usize,
    max_iter: usize,
    seed: i64,
    init_mode: &str,
) -> PyResult<Py<PyDict>> {
    fenced!("ml.fit_kmeans", {
        validate_init_mode(init_mode).map_err(ml_to_py_err)?;
        let plan = frame.inner().clone();
        let runtime: Arc<Runtime> = frame.runtime_handle();
        let solution = py.detach(|| {
            let (num_features, num_valid) =
                kmeans_count_valid(&plan, runtime.as_ref(), &features_col).map_err(ml_to_py_err)?;
            #[allow(clippy::cast_sign_loss)]
            let seed_u64 = seed as u64;
            let indices = random_center_indices(num_valid, k, seed_u64).map_err(ml_to_py_err)?;
            let initial = kmeans_materialize_centers(
                &plan,
                runtime.as_ref(),
                &features_col,
                num_features,
                &indices,
                k,
            )
            .map_err(ml_to_py_err)?;
            let mut solution = fit_kmeans_lloyd(initial, max_iter, |pass: &mut KMeansPass| {
                kmeans_stream_pass(&plan, runtime.as_ref(), &features_col, num_features, pass)
            })
            .map_err(ml_to_py_err)?;
            // Zero iterations return validated centers without running Lloyd.
            if max_iter == 0 && solution.num_rows == 0 {
                solution.num_rows = num_valid;
            }
            Ok::<_, PyErr>(solution)
        })?;

        let dict = PyDict::new(py);
        dict.set_item("centers", solution.centers)?;
        dict.set_item("num_features", solution.num_features)?;
        dict.set_item("k", solution.k)?;
        dict.set_item("num_rows", solution.num_rows)?;
        dict.set_item("iterations", solution.iterations)?;
        Ok(dict.unbind())
    })
}

/// Register ML pyfunctions on the native module.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(fit_linear_regression, module)?)?;
    module.add_function(wrap_pyfunction!(fit_logistic_regression, module)?)?;
    module.add_function(wrap_pyfunction!(fit_kmeans, module)?)?;
    Ok(())
}
