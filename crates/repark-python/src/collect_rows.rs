use std::ffi::CStr;
use std::os::raw::{c_char, c_long};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
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

fn too_long(kind: &str, length: usize) -> PyErr {
    PyValueError::new_err(format!(
        "collect fast path reached a {kind} of {length} bytes, past this platform's limit"
    ))
}

fn owned(py: Python<'_>, pointer: *mut ffi::PyObject) -> PyResult<Bound<'_, PyAny>> {
    unsafe { Bound::from_owned_ptr_or_err(py, pointer) }
}

fn int_cell(py: Python<'_>, value: i64) -> PyResult<Bound<'_, PyAny>> {
    owned(py, unsafe { ffi::PyLong_FromLongLong(value) })
}

fn uint_cell(py: Python<'_>, value: u64) -> PyResult<Bound<'_, PyAny>> {
    owned(py, unsafe { ffi::PyLong_FromUnsignedLongLong(value) })
}

fn float_cell(py: Python<'_>, value: f64) -> PyResult<Bound<'_, PyAny>> {
    owned(py, unsafe { ffi::PyFloat_FromDouble(value) })
}

fn bool_cell(py: Python<'_>, value: bool) -> PyResult<Bound<'_, PyAny>> {
    owned(py, unsafe { ffi::PyBool_FromLong(c_long::from(value)) })
}

fn text_cell<'py>(py: Python<'py>, value: &str) -> PyResult<Bound<'py, PyAny>> {
    let length =
        ffi::Py_ssize_t::try_from(value.len()).map_err(|_| too_long("string", value.len()))?;
    owned(py, unsafe {
        ffi::PyUnicode_FromStringAndSize(value.as_ptr().cast::<c_char>(), length)
    })
}

fn bytes_cell<'py>(py: Python<'py>, value: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let length = ffi::Py_ssize_t::try_from(value.len())
        .map_err(|_| too_long("binary value", value.len()))?;
    owned(py, unsafe {
        ffi::PyBytes_FromStringAndSize(value.as_ptr().cast::<c_char>(), length)
    })
}

macro_rules! values_of {
    ($array:expr, $arrow:ty) => {
        $array
            .as_any()
            .downcast_ref::<$arrow>()
            .ok_or_else(|| downcast_miss($array))?
    };
}

fn cell_to_python<'py>(
    py: Python<'py>,
    array: &ArrayRef,
    index: usize,
) -> PyResult<Bound<'py, PyAny>> {
    if array.is_null(index) {
        return Ok(py.None().into_bound(py));
    }
    match array.data_type() {
        ArrowDataType::Null => Ok(py.None().into_bound(py)),
        ArrowDataType::Boolean => bool_cell(py, values_of!(array, BooleanArray).value(index)),
        ArrowDataType::Int8 => int_cell(py, i64::from(values_of!(array, Int8Array).value(index))),
        ArrowDataType::Int16 => int_cell(py, i64::from(values_of!(array, Int16Array).value(index))),
        ArrowDataType::Int32 => int_cell(py, i64::from(values_of!(array, Int32Array).value(index))),
        ArrowDataType::Int64 => int_cell(py, values_of!(array, Int64Array).value(index)),
        ArrowDataType::UInt8 => {
            uint_cell(py, u64::from(values_of!(array, UInt8Array).value(index)))
        }
        ArrowDataType::UInt16 => {
            uint_cell(py, u64::from(values_of!(array, UInt16Array).value(index)))
        }
        ArrowDataType::UInt32 => {
            uint_cell(py, u64::from(values_of!(array, UInt32Array).value(index)))
        }
        ArrowDataType::UInt64 => uint_cell(py, values_of!(array, UInt64Array).value(index)),
        ArrowDataType::Float32 => {
            float_cell(py, f64::from(values_of!(array, Float32Array).value(index)))
        }
        ArrowDataType::Float64 => float_cell(py, values_of!(array, Float64Array).value(index)),
        ArrowDataType::Utf8 => text_cell(py, values_of!(array, StringArray).value(index)),
        ArrowDataType::LargeUtf8 => text_cell(py, values_of!(array, LargeStringArray).value(index)),
        ArrowDataType::Utf8View => text_cell(py, values_of!(array, StringViewArray).value(index)),
        ArrowDataType::Binary => bytes_cell(py, values_of!(array, BinaryArray).value(index)),
        ArrowDataType::LargeBinary => {
            bytes_cell(py, values_of!(array, LargeBinaryArray).value(index))
        }
        ArrowDataType::BinaryView => {
            bytes_cell(py, values_of!(array, BinaryViewArray).value(index))
        }
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

fn slot(position: usize) -> PyResult<ffi::Py_ssize_t> {
    ffi::Py_ssize_t::try_from(position).map_err(|_| too_long("row block", position))
}

fn row_tuple<'py>(
    py: Python<'py>,
    sources: &[ColumnSource<'py>],
    index: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let width = slot(sources.len())?;
    let tuple = owned(py, unsafe { ffi::PyTuple_New(width) })?;
    for (position, source) in sources.iter().enumerate() {
        let cell = match source {
            ColumnSource::Native(array) => cell_to_python(py, array, index)?,
            ColumnSource::Supplied(values) => values.get_item(index)?,
        };
        let filled =
            unsafe { ffi::PyTuple_SetItem(tuple.as_ptr(), slot(position)?, cell.into_ptr()) };
        if filled != 0 {
            return Err(PyErr::fetch(py));
        }
    }
    Ok(tuple.cast_into::<PyTuple>()?)
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
        let rows = owned(py, unsafe { ffi::PyList_New(slot(row_count)?) })?;
        for index in 0..row_count {
            let row = row_tuple(py, &sources, index)?;
            let filled =
                unsafe { ffi::PyList_SetItem(rows.as_ptr(), slot(index)?, row.into_ptr()) };
            if filled != 0 {
                return Err(PyErr::fetch(py));
            }
        }
        Ok(Some(rows.cast_into::<PyList>()?))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(rows_from_record_batch, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, UInt64Array};

    use super::*;

    fn cell<'py>(py: Python<'py>, array: &ArrayRef, index: usize) -> Bound<'py, PyAny> {
        cell_to_python(py, array, index).expect("the fast path converts this cell")
    }

    #[test]
    fn a_null_cpython_allocation_is_an_error_and_never_a_panic() {
        Python::attach(|py| {
            let refused = owned(py, unsafe { ffi::PyTuple_New(-1) });
            let error = refused.expect_err("a NULL return is an Err, not a panic");
            assert!(
                !error.to_string().is_empty(),
                "the error CPython set is surfaced, not discarded"
            );
        });
    }

    #[test]
    fn a_successful_allocation_passes_through_the_same_check() {
        Python::attach(|py| {
            let value =
                owned(py, unsafe { ffi::PyLong_FromLongLong(7) }).expect("a non-NULL return is Ok");
            assert_eq!(value.extract::<i64>().expect("an int"), 7);
        });
    }

    #[test]
    fn every_scalar_constructor_is_the_checked_one_and_keeps_its_value() {
        Python::attach(|py| {
            assert!(
                bool_cell(py, true)
                    .expect("bool")
                    .extract::<bool>()
                    .expect("bool")
            );
            assert_eq!(
                int_cell(py, -9)
                    .expect("int")
                    .extract::<i64>()
                    .expect("int"),
                -9
            );
            assert_eq!(
                uint_cell(py, u64::MAX)
                    .expect("uint")
                    .extract::<u64>()
                    .expect("uint"),
                u64::MAX
            );
            assert!(
                (float_cell(py, 1.5)
                    .expect("float")
                    .extract::<f64>()
                    .expect("float")
                    - 1.5)
                    .abs()
                    < f64::EPSILON
            );
            assert_eq!(
                text_cell(py, "sí")
                    .expect("text")
                    .extract::<String>()
                    .expect("text"),
                "sí"
            );
            assert_eq!(
                bytes_cell(py, &[0, 255])
                    .expect("bytes")
                    .extract::<Vec<u8>>()
                    .expect("bytes"),
                vec![0, 255]
            );
        });
    }

    #[test]
    fn an_empty_string_and_an_embedded_nul_survive_the_length_form() {
        Python::attach(|py| {
            assert_eq!(
                text_cell(py, "")
                    .expect("empty")
                    .extract::<String>()
                    .expect("empty"),
                ""
            );
            assert_eq!(
                text_cell(py, "a\0b")
                    .expect("nul")
                    .extract::<String>()
                    .expect("nul"),
                "a\0b"
            );
        });
    }

    #[test]
    fn a_null_slot_is_python_none_and_a_value_slot_is_not() {
        Python::attach(|py| {
            let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None]));
            assert_eq!(cell(py, &array, 0).extract::<i64>().expect("an int"), 3);
            assert!(cell(py, &array, 1).is_none(), "a null Arrow slot is None");
        });
    }

    #[test]
    fn a_row_tuple_keeps_column_order_and_width() {
        Python::attach(|py| {
            let ids: ArrayRef = Arc::new(Int32Array::from(vec![Some(11), Some(22)]));
            let counts: ArrayRef = Arc::new(UInt64Array::from(vec![Some(7), None]));
            let sources = vec![
                ColumnSource::Native(Arc::clone(&ids)),
                ColumnSource::Native(Arc::clone(&counts)),
            ];
            let row = row_tuple(py, &sources, 0).expect("a row builds");
            assert_eq!(row.len(), 2, "one cell per column");
            assert_eq!(
                row.get_item(0)
                    .expect("cell 0")
                    .extract::<i64>()
                    .expect("an int"),
                11
            );
            assert_eq!(
                row.get_item(1)
                    .expect("cell 1")
                    .extract::<u64>()
                    .expect("a uint"),
                7
            );
            let second = row_tuple(py, &sources, 1).expect("the second row builds");
            assert!(
                second.get_item(1).expect("cell 1").is_none(),
                "the null slot stays None inside the tuple"
            );
        });
    }

    #[test]
    fn an_unsupported_arrow_type_refuses_rather_than_panicking() {
        Python::attach(|py| {
            let array: ArrayRef = Arc::new(arrow::array::Date32Array::from(vec![Some(1)]));
            let error = cell_to_python(py, &array, 0).expect_err("Date32 is not on the fast path");
            assert!(
                error.to_string().contains("unsupported Arrow type"),
                "loud refusal, got {error}"
            );
        });
    }
}
