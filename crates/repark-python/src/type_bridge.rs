use std::borrow::Cow;
use std::ffi::CStr;

use arrow::datatypes::{DataType as ArrowDataType, Schema};
use arrow::ffi::FFI_ArrowSchema;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyDict, PyList, PyString};

use repark_spark::type_table::{
    self, DEFAULT_COLLATION, SparkDataType, SparkField, TypeTableError,
};

const SCHEMA_CAPSULE: &CStr = c"arrow_schema";

fn table_error_to_py(error: &TypeTableError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn spark_field_to_py(py: Python<'_>, field: &SparkField) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item(intern!(py, "kind"), intern!(py, "field"))?;
    dict.set_item(intern!(py, "name"), field.name.as_str())?;
    dict.set_item(intern!(py, "type"), spark_type_to_py(py, &field.data_type)?)?;
    dict.set_item(intern!(py, "nullable"), field.nullable)?;
    match &field.metadata {
        Some(metadata) => dict.set_item(intern!(py, "metadata"), metadata.as_str())?,
        None => dict.set_item(intern!(py, "metadata"), py.None())?,
    }
    Ok(dict.unbind().into_any())
}

fn spark_type_to_py(py: Python<'_>, data_type: &SparkDataType) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    let kind = intern!(py, "kind");
    match data_type {
        SparkDataType::Null => dict.set_item(kind, intern!(py, "null"))?,
        SparkDataType::SparkString { collation } => {
            dict.set_item(kind, intern!(py, "string"))?;
            dict.set_item(intern!(py, "collation"), collation.as_ref())?;
        }
        SparkDataType::Char { length } => {
            dict.set_item(kind, intern!(py, "char"))?;
            dict.set_item(intern!(py, "length"), *length)?;
        }
        SparkDataType::Varchar { length } => {
            dict.set_item(kind, intern!(py, "varchar"))?;
            dict.set_item(intern!(py, "length"), *length)?;
        }
        SparkDataType::Binary => dict.set_item(kind, intern!(py, "binary"))?,
        SparkDataType::Boolean => dict.set_item(kind, intern!(py, "boolean"))?,
        SparkDataType::Date => dict.set_item(kind, intern!(py, "date"))?,
        SparkDataType::Timestamp => dict.set_item(kind, intern!(py, "timestamp"))?,
        SparkDataType::TimestampNtz => dict.set_item(kind, intern!(py, "timestamp_ntz"))?,
        SparkDataType::Time { precision } => {
            dict.set_item(kind, intern!(py, "time"))?;
            dict.set_item(intern!(py, "precision"), *precision)?;
        }
        SparkDataType::Decimal { precision, scale } => {
            dict.set_item(kind, intern!(py, "decimal"))?;
            dict.set_item(intern!(py, "precision"), *precision)?;
            dict.set_item(intern!(py, "scale"), *scale)?;
        }
        SparkDataType::Double => dict.set_item(kind, intern!(py, "double"))?,
        SparkDataType::Float => dict.set_item(kind, intern!(py, "float"))?,
        SparkDataType::Byte => dict.set_item(kind, intern!(py, "byte"))?,
        SparkDataType::Integer => dict.set_item(kind, intern!(py, "integer"))?,
        SparkDataType::Long => dict.set_item(kind, intern!(py, "long"))?,
        SparkDataType::Short => dict.set_item(kind, intern!(py, "short"))?,
        SparkDataType::CalendarInterval => {
            dict.set_item(kind, intern!(py, "calendar_interval"))?;
        }
        SparkDataType::DayTimeInterval { start, end } => {
            dict.set_item(kind, intern!(py, "day_time_interval"))?;
            dict.set_item(intern!(py, "start"), start.as_str())?;
            dict.set_item(intern!(py, "end"), end.as_str())?;
        }
        SparkDataType::YearMonthInterval { start, end } => {
            dict.set_item(kind, intern!(py, "year_month_interval"))?;
            dict.set_item(intern!(py, "start"), start.as_str())?;
            dict.set_item(intern!(py, "end"), end.as_str())?;
        }
        SparkDataType::Variant => dict.set_item(kind, intern!(py, "variant"))?,
        SparkDataType::Array {
            element,
            contains_null,
        } => {
            dict.set_item(kind, intern!(py, "array"))?;
            dict.set_item(intern!(py, "element"), spark_type_to_py(py, element)?)?;
            dict.set_item(intern!(py, "contains_null"), *contains_null)?;
        }
        SparkDataType::Map {
            key,
            value,
            value_contains_null,
        } => {
            dict.set_item(kind, intern!(py, "map"))?;
            dict.set_item(intern!(py, "key"), spark_type_to_py(py, key)?)?;
            dict.set_item(intern!(py, "value"), spark_type_to_py(py, value)?)?;
            dict.set_item(intern!(py, "value_contains_null"), *value_contains_null)?;
        }
        SparkDataType::Struct(fields) => {
            dict.set_item(kind, intern!(py, "struct"))?;
            let items = PyList::empty(py);
            for field in fields {
                items.append(spark_field_to_py(py, field)?)?;
            }
            dict.set_item(intern!(py, "fields"), items)?;
        }
        SparkDataType::Field(field) => {
            return spark_field_to_py(py, field);
        }
    }
    Ok(dict.unbind().into_any())
}

fn dict_required<'py>(
    dict: &Bound<'py, PyDict>,
    key: &Bound<'py, PyString>,
) -> PyResult<Bound<'py, PyAny>> {
    dict.get_item(key)?.ok_or_else(|| {
        PyValueError::new_err(format!(
            "type-table descriptor is missing key {:?}",
            key.to_str().unwrap_or_default()
        ))
    })
}

fn spark_field_from_py(dict: &Bound<'_, PyDict>) -> PyResult<SparkField> {
    let py = dict.py();
    let name: String = dict_required(dict, intern!(py, "name"))?.extract()?;
    let inner = dict_required(dict, intern!(py, "type"))?;
    let data_type = spark_type_from_py(&inner)?;
    let nullable: bool = dict_required(dict, intern!(py, "nullable"))?.extract()?;
    let metadata_obj = dict_required(dict, intern!(py, "metadata"))?;
    let metadata = if metadata_obj.is_none() {
        None
    } else {
        Some(metadata_obj.extract::<String>()?)
    };
    Ok(SparkField {
        name,
        data_type,
        nullable,
        metadata,
    })
}

fn spark_type_from_py(obj: &Bound<'_, PyAny>) -> PyResult<SparkDataType> {
    let dict = obj.cast::<PyDict>()?;
    let py = dict.py();
    let kind_obj = dict_required(dict, intern!(py, "kind"))?;
    let kind: Cow<'_, str> = kind_obj.extract()?;
    Ok(match kind.as_ref() {
        "null" => SparkDataType::Null,
        "string" => {
            let collation_obj = dict_required(dict, intern!(py, "collation"))?;
            let collation: Cow<'_, str> = collation_obj.extract()?;
            SparkDataType::SparkString {
                collation: if collation == DEFAULT_COLLATION {
                    Cow::Borrowed(DEFAULT_COLLATION)
                } else {
                    Cow::Owned(collation.into_owned())
                },
            }
        }
        "char" => SparkDataType::Char {
            length: dict_required(dict, intern!(py, "length"))?.extract()?,
        },
        "varchar" => SparkDataType::Varchar {
            length: dict_required(dict, intern!(py, "length"))?.extract()?,
        },
        "binary" => SparkDataType::Binary,
        "boolean" => SparkDataType::Boolean,
        "date" => SparkDataType::Date,
        "timestamp" => SparkDataType::Timestamp,
        "timestamp_ntz" => SparkDataType::TimestampNtz,
        "time" => SparkDataType::Time {
            precision: dict_required(dict, intern!(py, "precision"))?.extract()?,
        },
        "decimal" => SparkDataType::Decimal {
            precision: dict_required(dict, intern!(py, "precision"))?.extract()?,
            scale: dict_required(dict, intern!(py, "scale"))?.extract()?,
        },
        "double" => SparkDataType::Double,
        "float" => SparkDataType::Float,
        "byte" => SparkDataType::Byte,
        "integer" => SparkDataType::Integer,
        "long" => SparkDataType::Long,
        "short" => SparkDataType::Short,
        "calendar_interval" => SparkDataType::CalendarInterval,
        "day_time_interval" => SparkDataType::DayTimeInterval {
            start: dict_required(dict, intern!(py, "start"))?.extract()?,
            end: dict_required(dict, intern!(py, "end"))?.extract()?,
        },
        "year_month_interval" => SparkDataType::YearMonthInterval {
            start: dict_required(dict, intern!(py, "start"))?.extract()?,
            end: dict_required(dict, intern!(py, "end"))?.extract()?,
        },
        "variant" => SparkDataType::Variant,
        "array" => SparkDataType::Array {
            element: Box::new(spark_type_from_py(&dict_required(
                dict,
                intern!(py, "element"),
            )?)?),
            contains_null: dict_required(dict, intern!(py, "contains_null"))?.extract()?,
        },
        "map" => SparkDataType::Map {
            key: Box::new(spark_type_from_py(&dict_required(
                dict,
                intern!(py, "key"),
            )?)?),
            value: Box::new(spark_type_from_py(&dict_required(
                dict,
                intern!(py, "value"),
            )?)?),
            value_contains_null: dict_required(dict, intern!(py, "value_contains_null"))?
                .extract()?,
        },
        "struct" => {
            let fields_obj = dict_required(dict, intern!(py, "fields"))?;
            let fields_list = fields_obj.cast::<PyList>()?;
            let mut fields: Vec<SparkField> = Vec::with_capacity(fields_list.len());
            for item in fields_list.iter() {
                fields.push(spark_field_from_py(item.cast::<PyDict>()?)?);
            }
            SparkDataType::Struct(fields)
        }
        "field" => SparkDataType::Field(Box::new(spark_field_from_py(dict)?)),
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported type-table descriptor kind {other:?}"
            )));
        }
    })
}

fn arrow_type_from_capsule(capsule: &Bound<'_, PyCapsule>) -> PyResult<ArrowDataType> {
    let pointer = capsule.pointer_checked(Some(SCHEMA_CAPSULE))?;
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    ArrowDataType::try_from(ffi_schema)
        .map_err(|error| PyValueError::new_err(format!("arrow type import refused: {error}")))
}

fn arrow_schema_from_capsule(capsule: &Bound<'_, PyCapsule>) -> PyResult<Schema> {
    let pointer = capsule.pointer_checked(Some(SCHEMA_CAPSULE))?;
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    Schema::try_from(ffi_schema)
        .map_err(|error| PyValueError::new_err(format!("arrow schema import refused: {error}")))
}

fn arrow_type_capsule<'py>(
    py: Python<'py>,
    data_type: &ArrowDataType,
) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi_schema = FFI_ArrowSchema::try_from(data_type)
        .map_err(|error| PyValueError::new_err(format!("arrow type export refused: {error}")))?;
    PyCapsule::new_with_value_and_destructor(py, ffi_schema, SCHEMA_CAPSULE, |ffi, _ctx| {
        drop(ffi);
    })
}

#[pyfunction]
fn spark_descriptor_from_ddl(py: Python<'_>, text: &str) -> PyResult<Py<PyAny>> {
    let data_type = type_table::parse_ddl(text).map_err(|error| table_error_to_py(&error))?;
    spark_type_to_py(py, &data_type)
}

#[pyfunction]
fn spark_descriptor_from_arrow_type(
    py: Python<'_>,
    capsule: &Bound<'_, PyCapsule>,
) -> PyResult<Py<PyAny>> {
    let arrow_type = arrow_type_from_capsule(capsule)?;
    spark_type_to_py(py, &type_table::spark_type_from_arrow(&arrow_type))
}

#[pyfunction]
fn spark_descriptor_from_arrow_schema(
    py: Python<'_>,
    capsule: &Bound<'_, PyCapsule>,
) -> PyResult<Py<PyAny>> {
    let schema = arrow_schema_from_capsule(capsule)?;
    spark_type_to_py(py, &type_table::spark_struct_from_arrow(&schema))
}

#[pyfunction]
fn arrow_type_capsule_from_descriptor<'py>(
    py: Python<'py>,
    descriptor: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let data_type = spark_type_from_py(descriptor)?;
    let arrow_type =
        type_table::arrow_type_from_spark(&data_type).map_err(|error| table_error_to_py(&error))?;
    arrow_type_capsule(py, &arrow_type)
}

#[pyfunction]
fn sql_token_to_arrow_capsule<'py>(
    py: Python<'py>,
    sql_type: &str,
) -> PyResult<(Option<Bound<'py, PyCapsule>>, i64, i64)> {
    let data_type =
        type_table::sql_type_from_token(sql_type).map_err(|error| table_error_to_py(&error))?;
    match type_table::arrow_type_from_spark(&data_type) {
        Ok(arrow_type) => Ok((Some(arrow_type_capsule(py, &arrow_type)?), 0, 0)),
        Err(TypeTableError::DecimalScale { precision, scale }) => Ok((None, precision, scale)),
        Err(error) => Err(table_error_to_py(&error)),
    }
}

#[pyfunction]
fn simple_string_from_descriptor(descriptor: &Bound<'_, PyAny>) -> PyResult<String> {
    let data_type = spark_type_from_py(descriptor)?;
    Ok(type_table::simple_string(&data_type))
}

#[pyfunction]
fn engine_token_from_descriptor(descriptor: &Bound<'_, PyAny>) -> PyResult<String> {
    let data_type = spark_type_from_py(descriptor)?;
    Ok(type_table::engine_token(&data_type))
}

#[pyfunction]
fn ddl_token_from_descriptor(descriptor: &Bound<'_, PyAny>) -> PyResult<String> {
    let data_type = spark_type_from_py(descriptor)?;
    Ok(type_table::ddl_token(&data_type))
}

#[pyfunction]
fn sql_marker_from_descriptor(descriptor: &Bound<'_, PyAny>) -> PyResult<String> {
    let data_type = spark_type_from_py(descriptor)?;
    Ok(type_table::sql_marker_token(&data_type))
}

#[pyfunction]
fn struct_field_ddl_from_descriptor(descriptor: &Bound<'_, PyAny>) -> PyResult<String> {
    let data_type = spark_type_from_py(descriptor)?;
    Ok(type_table::struct_field_ddl(&data_type))
}

#[pyfunction]
fn csv_sql_cast_token(engine_type: &str) -> String {
    type_table::csv_sql_cast_token(engine_type)
}

#[pyfunction]
fn csv_rung_descriptor(
    py: Python<'_>,
    rung: &str,
    precision: Option<i64>,
    scale: Option<i64>,
    timestamp_ntz: bool,
) -> PyResult<Py<PyAny>> {
    spark_type_to_py(
        py,
        &type_table::csv_rung_type(rung, precision, scale, timestamp_ntz),
    )
}

#[pyfunction]
fn default_timestamp_descriptor(py: Python<'_>, timestamp_ntz: bool) -> PyResult<Py<PyAny>> {
    spark_type_to_py(py, &type_table::default_timestamp_type(timestamp_ntz))
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(spark_descriptor_from_ddl, module)?)?;
    module.add_function(wrap_pyfunction!(spark_descriptor_from_arrow_type, module)?)?;
    module.add_function(wrap_pyfunction!(
        spark_descriptor_from_arrow_schema,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        arrow_type_capsule_from_descriptor,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(sql_token_to_arrow_capsule, module)?)?;
    module.add_function(wrap_pyfunction!(simple_string_from_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(engine_token_from_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(ddl_token_from_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(sql_marker_from_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(struct_field_ddl_from_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(csv_sql_cast_token, module)?)?;
    module.add_function(wrap_pyfunction!(csv_rung_descriptor, module)?)?;
    module.add_function(wrap_pyfunction!(default_timestamp_descriptor, module)?)?;
    Ok(())
}
