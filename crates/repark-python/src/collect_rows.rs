use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyDict, PyList, PyTuple};

use crate::fence::fenced;

const SCHEMA_CAPSULE: &CStr = c"arrow_schema";

const ARRAY_CAPSULE: &CStr = c"arrow_array";

enum ColumnSource<'py> {
    Native(ArrayRef),
    Supplied(Bound<'py, PyList>),
}

fn natively_convertible(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Null
            | ArrowDataType::Boolean
            | ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
            | ArrowDataType::Float32
            | ArrowDataType::Float64
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Utf8View
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::BinaryView
    )
}

fn downcast_miss(array: &ArrayRef) -> PyErr {
    PyValueError::new_err(format!(
        "collect fast path could not downcast a {} column",
        array.data_type()
    ))
}

macro_rules! native_cell {
    ($py:expr, $array:expr, $index:expr, $arrow:ty) => {{
        let values = $array
            .as_any()
            .downcast_ref::<$arrow>()
            .ok_or_else(|| downcast_miss($array))?;
        values.value($index).into_bound_py_any($py)
    }};
}

fn cell_to_python<'py>(
    py: Python<'py>,
    array: &ArrayRef,
    index: usize,
) -> PyResult<Bound<'py, PyAny>> {
    if array.is_null(index) {
        return py.None().into_bound_py_any(py);
    }
    match array.data_type() {
        ArrowDataType::Null => py.None().into_bound_py_any(py),
        ArrowDataType::Boolean => native_cell!(py, array, index, BooleanArray),
        ArrowDataType::Int8 => native_cell!(py, array, index, Int8Array),
        ArrowDataType::Int16 => native_cell!(py, array, index, Int16Array),
        ArrowDataType::Int32 => native_cell!(py, array, index, Int32Array),
        ArrowDataType::Int64 => native_cell!(py, array, index, Int64Array),
        ArrowDataType::UInt8 => native_cell!(py, array, index, UInt8Array),
        ArrowDataType::UInt16 => native_cell!(py, array, index, UInt16Array),
        ArrowDataType::UInt32 => native_cell!(py, array, index, UInt32Array),
        ArrowDataType::UInt64 => native_cell!(py, array, index, UInt64Array),
        ArrowDataType::Float32 => {
            let values = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| downcast_miss(array))?;
            f64::from(values.value(index)).into_bound_py_any(py)
        }
        ArrowDataType::Float64 => native_cell!(py, array, index, Float64Array),
        ArrowDataType::Utf8 => native_cell!(py, array, index, StringArray),
        ArrowDataType::LargeUtf8 => native_cell!(py, array, index, LargeStringArray),
        ArrowDataType::Utf8View => native_cell!(py, array, index, StringViewArray),
        ArrowDataType::Binary => native_cell!(py, array, index, BinaryArray),
        ArrowDataType::LargeBinary => native_cell!(py, array, index, LargeBinaryArray),
        ArrowDataType::BinaryView => native_cell!(py, array, index, BinaryViewArray),
        other => Err(PyValueError::new_err(format!(
            "collect fast path reached an unsupported Arrow type: {other}"
        ))),
    }
}

fn import_record_batch(batch: &Bound<'_, PyAny>) -> PyResult<StructArray> {
    let exported = batch.call_method0("__arrow_c_array__")?;
    let pair = exported.cast::<PyTuple>()?;
    let schema_item = pair.get_item(0)?;
    let array_item = pair.get_item(1)?;
    let schema_pointer = schema_item
        .cast::<PyCapsule>()?
        .pointer_checked(Some(SCHEMA_CAPSULE))?
        .as_ptr()
        .cast::<FFI_ArrowSchema>();
    let array_pointer = array_item
        .cast::<PyCapsule>()?
        .pointer_checked(Some(ARRAY_CAPSULE))?
        .as_ptr()
        .cast::<FFI_ArrowArray>();
    let (schema, array) = unsafe {
        (
            std::ptr::replace(schema_pointer, FFI_ArrowSchema::empty()),
            std::ptr::replace(array_pointer, FFI_ArrowArray::empty()),
        )
    };
    let data = unsafe { from_ffi(array, &schema) }
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    if !matches!(data.data_type(), ArrowDataType::Struct(_)) {
        return Err(PyValueError::new_err(
            "collect fast path expected a struct-typed record batch export",
        ));
    }
    Ok(StructArray::from(data))
}

fn row_tuple<'py>(
    py: Python<'py>,
    sources: &[ColumnSource<'py>],
    index: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let mut cells: Vec<Bound<'py, PyAny>> = Vec::with_capacity(sources.len());
    for source in sources {
        match source {
            ColumnSource::Native(array) => cells.push(cell_to_python(py, array, index)?),
            ColumnSource::Supplied(values) => cells.push(values.get_item(index)?),
        }
    }
    PyTuple::new(py, cells)
}

fn column_sources<'py>(
    struct_array: &StructArray,
    supplied: &Bound<'py, PyDict>,
    row_count: usize,
) -> PyResult<Option<Vec<ColumnSource<'py>>>> {
    let mut sources: Vec<ColumnSource<'py>> = Vec::with_capacity(struct_array.num_columns());
    for position in 0..struct_array.num_columns() {
        if let Some(values) = supplied.get_item(position)? {
            let values = values.cast_into::<PyList>()?;
            if values.len() != row_count {
                return Err(PyValueError::new_err(
                    "a supplied collect column does not match the batch row count",
                ));
            }
            sources.push(ColumnSource::Supplied(values));
            continue;
        }
        let column = struct_array.column(position);
        if !natively_convertible(column.data_type()) {
            return Ok(None);
        }
        sources.push(ColumnSource::Native(Arc::clone(column)));
    }
    Ok(Some(sources))
}

#[pyfunction]
pub fn rows_from_record_batch<'py>(
    py: Python<'py>,
    batch: &Bound<'py, PyAny>,
    supplied: &Bound<'py, PyDict>,
) -> PyResult<Option<Bound<'py, PyList>>> {
    fenced!("collect_rows.rows_from_record_batch", {
        let struct_array = import_record_batch(batch)?;
        let row_count = struct_array.len();
        let Some(sources) = column_sources(&struct_array, supplied, row_count)? else {
            return Ok(None);
        };
        let mut rows: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(row_count);
        for index in 0..row_count {
            rows.push(row_tuple(py, &sources, index)?);
        }
        Ok(Some(PyList::new(py, rows)?))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(rows_from_record_batch, module)?)?;
    Ok(())
}
