mod build;
mod cells;
mod infer;

use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyList, PyTuple, PyType};

use crate::cdf_infer::build::build_column;
use crate::cdf_infer::cells::{Cdf, Cell, Ctx, extract_cell};
use crate::cdf_infer::infer::infer_column_type;

const STREAM_CAPSULE: &CStr = c"arrow_array_stream";

const SCHEMA_CAPSULE: &CStr = c"arrow_schema";

#[pyclass(name = "PyCdfArrowExport", module = "repark._native")]
pub struct PyCdfArrowExport {
    batch: RecordBatch,
}

#[pymethods]
impl PyCdfArrowExport {
    #[allow(clippy::wrong_self_convention)]
    #[allow(clippy::needless_pass_by_value)]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(self.batch.clone())),
            self.batch.schema(),
        ));
        PyCapsule::new_with_value_and_destructor(
            py,
            FFI_ArrowArrayStream::new(reader),
            STREAM_CAPSULE,
            |ffi_stream, _ctx| drop(ffi_stream),
        )
    }
}

fn read_schema(schema_obj: &Bound<'_, PyAny>) -> PyResult<Option<SchemaRef>> {
    if schema_obj.is_none() {
        return Ok(None);
    }
    let capsule = schema_obj.call_method0("__arrow_c_schema__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let pointer = capsule.pointer_checked(Some(SCHEMA_CAPSULE))?;
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    let schema = Schema::try_from(ffi_schema)
        .map_err(|err| PyValueError::new_err(format!("cdf schema import refused: {err}")))?;
    Ok(Some(Arc::new(schema)))
}

fn build_batch<'py>(
    names: &[String],
    columns: &[Vec<Cell<'py>>],
    schema: Option<SchemaRef>,
    cx: &Ctx<'py>,
) -> Result<Option<RecordBatch>, PyErr> {
    let batch_schema = if let Some(schema) = schema {
        schema
    } else {
        let mut fields: Vec<Field> = Vec::with_capacity(names.len());
        for (index, name) in names.iter().enumerate() {
            let data_type = match infer_column_type(&columns[index], cx) {
                Ok(data_type) => data_type,
                Err(Cdf::Fallback) => return Ok(None),
                Err(Cdf::Err(err)) => return Err(err),
            };
            fields.push(Field::new(name.as_str(), data_type, true));
        }
        Arc::new(Schema::new(Fields::from(fields)))
    };
    if batch_schema.fields().len() != columns.len() {
        return Ok(None);
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (index, field) in batch_schema.fields().iter().enumerate() {
        let slots: Vec<Option<&Cell<'py>>> = columns[index].iter().map(Some).collect();
        match build_column(&slots, field.data_type(), cx, 0) {
            Ok(array) => arrays.push(array),
            Err(Cdf::Fallback) => return Ok(None),
            Err(Cdf::Err(err)) => return Err(err),
        }
    }
    let Ok(batch) = RecordBatch::try_new(batch_schema, arrays) else {
        return Ok(None);
    };
    Ok(Some(batch))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::fn_params_excessive_bools)]
pub fn cdf_arrow_export<'py>(
    py: Python<'py>,
    names: Vec<String>,
    rows: Bound<'py, PyList>,
    schema: Option<Bound<'py, PyAny>>,
    session_tz_utc: bool,
    timestamp_ntz: bool,
    infer_dict_as_struct: bool,
    legacy_first_element: bool,
    decimal_prec: i64,
) -> PyResult<Option<PyCdfArrowExport>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut seen_names = std::collections::HashSet::with_capacity(names.len());
    if names.iter().any(|name| !seen_names.insert(name)) {
        return Ok(None);
    }
    let schema = match schema {
        Some(schema_obj) => read_schema(&schema_obj)?,
        None => None,
    };
    if let Some(schema_ref) = &schema
        && schema_ref.fields().len() != names.len()
    {
        return Ok(None);
    }
    let decimal_type = py
        .import("decimal")?
        .getattr("Decimal")?
        .cast_into::<PyType>()?;
    let cx = Ctx {
        decimal_type,
        session_tz_utc,
        timestamp_ntz,
        infer_dict_as_struct,
        legacy_first_element,
        decimal_prec,
    };
    let mut columns: Vec<Vec<Cell<'py>>> = (0..names.len())
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    for row in rows.iter() {
        let Ok(tuple) = row.cast::<PyTuple>() else {
            return Ok(None);
        };
        if tuple.len() != names.len() {
            return Ok(None);
        }
        for (index, item) in tuple.iter().enumerate() {
            match extract_cell(&item, &cx, 0) {
                Ok(cell) => columns[index].push(cell),
                Err(Cdf::Fallback) => return Ok(None),
                Err(Cdf::Err(err)) => return Err(err),
            }
        }
    }
    match build_batch(&names, &columns, schema, &cx)? {
        Some(batch) => Ok(Some(PyCdfArrowExport { batch })),
        None => Ok(None),
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyCdfArrowExport>()?;
    module.add_function(wrap_pyfunction!(cdf_arrow_export, module)?)?;
    Ok(())
}
