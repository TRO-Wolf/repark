use std::cell::RefCell;
use std::collections::HashMap;

use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList,
    PyMemoryView, PyString, PyTime, PyTuple, PyType,
};

pub(crate) enum Cdf {
    Fallback,
    Err(PyErr),
}

impl From<PyErr> for Cdf {
    fn from(error: PyErr) -> Self {
        Cdf::Err(error)
    }
}

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Ctx<'py> {
    pub decimal_type: Bound<'py, PyType>,
    pub bool_type: Bound<'py, PyType>,
    pub int_type: Bound<'py, PyType>,
    pub float_type: Bound<'py, PyType>,
    pub str_type: Bound<'py, PyType>,
    pub bytes_type: Bound<'py, PyType>,
    pub bytearray_type: Bound<'py, PyType>,
    pub memoryview_type: Bound<'py, PyType>,
    pub list_type: Bound<'py, PyType>,
    pub tuple_type: Bound<'py, PyType>,
    pub dict_type: Bound<'py, PyType>,
    pub datetime_type: Bound<'py, PyType>,
    pub date_type: Bound<'py, PyType>,
    pub time_type: Bound<'py, PyType>,
    pub timezone_type: Bound<'py, PyType>,
    pub epoch_date: Bound<'py, PyAny>,
    pub utcoffset_cache: RefCell<HashMap<usize, (Py<PyAny>, i64)>>,
    pub session_tz_utc: bool,
    pub timestamp_ntz: bool,
    pub infer_dict_as_struct: bool,
    pub legacy_first_element: bool,
    pub decimal_prec: i64,
}

pub(crate) enum CellKind<'py> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(PyBackedStr),
    Bin(Vec<u8>),
    Date(i32),
    Dt {
        wall_us: i64,
        off_us: Option<i64>,
    },
    Dec {
        unscaled: i128,
    },
    List(Vec<Cell<'py>>),
    Tup(Vec<Cell<'py>>),
    Row {
        fields: Vec<String>,
        vals: Vec<Cell<'py>>,
    },
    Dict(Vec<(Cell<'py>, Cell<'py>)>),
    Fill,
}

pub(crate) struct Cell<'py> {
    pub obj: Option<Bound<'py, PyAny>>,
    pub kind: CellKind<'py>,
}

pub(crate) const MAX_DEPTH: u32 = 100;

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let adjusted = i64::from(year) - i64::from(month <= 2);
    let era = if adjusted >= 0 {
        adjusted
    } else {
        adjusted - 399
    } / 400;
    let yoe = adjusted - era * 400;
    let mp = (i64::from(month) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(crate) fn py_str(obj: &Bound<'_, PyAny>) -> Result<PyBackedStr, Cdf> {
    let rendered = obj.str()?;
    Ok(rendered.try_into()?)
}

fn extract_decimal<'py>(obj: &Bound<'py, PyAny>, cx: &Ctx<'py>) -> Result<CellKind<'py>, Cdf> {
    let parts = obj.call_method0("as_tuple")?;
    let neg = parts.get_item(0)?.extract::<i64>()? != 0;
    let mut digits = Vec::new();
    for digit in parts.get_item(1)?.try_iter()? {
        digits.push(digit?.extract::<u8>()?);
    }
    let exponent_obj = parts.get_item(2)?;
    if exponent_obj.is_instance_of::<PyString>() {
        return Err(Cdf::Fallback);
    }
    let exp = exponent_obj.extract::<i64>()?;
    let int_digits = i64::try_from(digits.len()).map_err(|_| Cdf::Fallback)? + exp;
    if int_digits > 20 {
        return Err(Cdf::Fallback);
    }
    if int_digits + 18 > cx.decimal_prec {
        return Err(Cdf::Fallback);
    }
    if exp + 18 < 0 {
        let need_zero = usize::try_from(-(exp + 18)).map_err(|_| Cdf::Fallback)?;
        let tail: &[u8] = if need_zero >= digits.len() {
            &digits[..]
        } else {
            &digits[digits.len() - need_zero..]
        };
        if tail.iter().any(|digit| *digit != 0) {
            return Err(Cdf::Fallback);
        }
    }
    let mut mantissa: i128 = 0;
    for digit in &digits {
        mantissa = mantissa
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(*digit)))
            .ok_or(Cdf::Fallback)?;
    }
    let shift = exp + 18;
    let scaled = if shift >= 0 {
        mantissa
            .checked_mul(
                10_i128
                    .checked_pow(u32::try_from(shift).map_err(|_| Cdf::Fallback)?)
                    .ok_or(Cdf::Fallback)?,
            )
            .ok_or(Cdf::Fallback)?
    } else {
        mantissa / 10_i128.pow(u32::try_from(-shift).map_err(|_| Cdf::Fallback)?)
    };
    Ok(CellKind::Dec {
        unscaled: if neg { -scaled } else { scaled },
    })
}

fn extract_datetime<'py>(dt: &Bound<'py, PyDateTime>, cx: &Ctx<'py>) -> Result<CellKind<'py>, Cdf> {
    if dt.get_type().as_ptr() != cx.datetime_type.as_ptr() {
        return extract_datetime_subclass(dt);
    }
    let py = dt.py();
    let year = dt.getattr(pyo3::intern!(py, "year"))?.extract::<i32>()?;
    let month = dt.getattr(pyo3::intern!(py, "month"))?.extract::<u32>()?;
    let day = dt.getattr(pyo3::intern!(py, "day"))?.extract::<u32>()?;
    let hour = dt.getattr(pyo3::intern!(py, "hour"))?.extract::<i64>()?;
    let minute = dt.getattr(pyo3::intern!(py, "minute"))?.extract::<i64>()?;
    let second = dt.getattr(pyo3::intern!(py, "second"))?.extract::<i64>()?;
    let micro = dt
        .getattr(pyo3::intern!(py, "microsecond"))?
        .extract::<i64>()?;
    let wall_us = days_from_civil(year, month, day) * 86_400_000_000
        + (hour * 3600 + minute * 60 + second) * 1_000_000
        + micro;
    let tzinfo = dt.getattr(pyo3::intern!(py, "tzinfo"))?;
    let off_us = if tzinfo.is_none() {
        None
    } else {
        Some(datetime_utcoffset_us(&tzinfo, dt, cx)?)
    };
    Ok(CellKind::Dt { wall_us, off_us })
}

fn extract_datetime_subclass<'py>(dt: &Bound<'py, PyDateTime>) -> Result<CellKind<'py>, Cdf> {
    let parts = dt.call_method0(pyo3::intern!(dt.py(), "timetuple"))?;
    let days = days_from_civil(
        parts.get_item(0)?.extract::<i32>()?,
        parts.get_item(1)?.extract::<u32>()?,
        parts.get_item(2)?.extract::<u32>()?,
    );
    let wall_us = days * 86_400_000_000
        + (parts.get_item(3)?.extract::<i64>()? * 3600
            + parts.get_item(4)?.extract::<i64>()? * 60
            + parts.get_item(5)?.extract::<i64>()?)
            * 1_000_000
        + dt.getattr(pyo3::intern!(dt.py(), "microsecond"))?
            .extract::<i64>()?;
    let tzinfo = dt.getattr(pyo3::intern!(dt.py(), "tzinfo"))?;
    let off_us = if tzinfo.is_none() {
        None
    } else {
        Some(utcoffset_call_us(&tzinfo, dt)?)
    };
    Ok(CellKind::Dt { wall_us, off_us })
}

fn datetime_utcoffset_us(
    tzinfo: &Bound<'_, PyAny>,
    dt: &Bound<'_, PyDateTime>,
    cx: &Ctx<'_>,
) -> Result<i64, Cdf> {
    if tzinfo.get_type().as_ptr() != cx.timezone_type.as_ptr() {
        return utcoffset_call_us(tzinfo, dt);
    }
    let key = tzinfo.as_ptr() as usize;
    if let Some((_, us)) = cx.utcoffset_cache.borrow().get(&key) {
        return Ok(*us);
    }
    let us = utcoffset_call_us(tzinfo, dt)?;
    cx.utcoffset_cache
        .borrow_mut()
        .insert(key, (tzinfo.clone().unbind(), us));
    Ok(us)
}

fn utcoffset_call_us(tzinfo: &Bound<'_, PyAny>, dt: &Bound<'_, PyDateTime>) -> Result<i64, Cdf> {
    let offset = tzinfo.call_method1("utcoffset", (dt.clone().into_any(),))?;
    if offset.is_none() {
        return Err(Cdf::Fallback);
    }
    let delta = offset.cast::<PyDelta>().map_err(|_| Cdf::Fallback)?;
    let py = delta.py();
    Ok(
        (delta.getattr(pyo3::intern!(py, "days"))?.extract::<i64>()? * 86_400
            + delta
                .getattr(pyo3::intern!(py, "seconds"))?
                .extract::<i64>()?)
            * 1_000_000
            + delta
                .getattr(pyo3::intern!(py, "microseconds"))?
                .extract::<i64>()?,
    )
}

pub(crate) fn extract_cell<'py>(
    obj: &Bound<'py, PyAny>,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<Cell<'py>, Cdf> {
    if depth > MAX_DEPTH {
        return Err(Cdf::Fallback);
    }
    let kind = classify(obj, cx, depth)?;
    Ok(Cell {
        obj: if matches!(kind, CellKind::Str(_) | CellKind::Null) {
            None
        } else {
            Some(obj.clone())
        },
        kind,
    })
}

fn classify<'py>(obj: &Bound<'py, PyAny>, cx: &Ctx<'py>, depth: u32) -> Result<CellKind<'py>, Cdf> {
    if obj.is_none() {
        return Ok(CellKind::Null);
    }
    if let Ok(value) = obj.cast::<PyBool>() {
        return Ok(CellKind::Bool(value.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        let value = obj.extract::<i64>().map_err(|_| Cdf::Fallback)?;
        return Ok(CellKind::Int(value));
    }
    if obj.is_instance_of::<PyFloat>() {
        let value = obj.extract::<f64>()?;
        if value.is_infinite() {
            return Err(Cdf::Fallback);
        }
        return Ok(CellKind::Float(value));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(CellKind::Str(obj.extract::<PyBackedStr>()?));
    }
    if let Ok(value) = obj.cast::<PyBytes>() {
        return Ok(CellKind::Bin(value.as_bytes().to_vec()));
    }
    if let Ok(value) = obj.cast::<PyByteArray>() {
        return Ok(CellKind::Bin(value.to_vec()));
    }
    if let Ok(value) = obj.cast::<PyMemoryView>() {
        let bytes = value.call_method0("tobytes")?;
        return Ok(CellKind::Bin(bytes.extract::<Vec<u8>>()?));
    }
    if let Ok(value) = obj.cast::<PyDateTime>() {
        let name = obj.get_type().name()?;
        if name.to_str()? == "NaTType" {
            return Err(Cdf::Fallback);
        }
        return extract_datetime(value, cx);
    }
    if obj.is_instance_of::<PyTime>() {
        return Ok(CellKind::Str(py_str(obj)?));
    }
    if let Ok(value) = obj.cast::<PyDate>() {
        if value.get_type().as_ptr() == cx.date_type.as_ptr() {
            let delta = obj.sub(cx.epoch_date.clone())?;
            let days = delta
                .getattr(pyo3::intern!(obj.py(), "days"))?
                .extract::<i64>()?;
            return Ok(CellKind::Date(days.try_into().map_err(|_| Cdf::Fallback)?));
        }
        let parts = value.call_method0(pyo3::intern!(obj.py(), "timetuple"))?;
        let days = days_from_civil(
            parts.get_item(0)?.extract::<i32>()?,
            parts.get_item(1)?.extract::<u32>()?,
            parts.get_item(2)?.extract::<u32>()?,
        );
        return Ok(CellKind::Date(days.try_into().map_err(|_| Cdf::Fallback)?));
    }
    if obj.is_instance(&cx.decimal_type)? {
        return extract_decimal(obj, cx);
    }
    if let Ok(value) = obj.cast::<PyList>() {
        return Ok(CellKind::List(extract_seq(value.iter(), cx, depth)?));
    }
    if let Ok(value) = obj.cast::<PyTuple>() {
        return Ok(CellKind::Tup(extract_seq(value.iter(), cx, depth)?));
    }
    if obj.get_type().name()?.to_str()? == "Row" {
        let module = obj.get_type().getattr("__module__")?;
        if module.extract::<String>()?.starts_with("repark") {
            return extract_row(obj, cx, depth);
        }
    }
    if let Ok(value) = obj.cast::<PyDict>() {
        let mut pairs = Vec::with_capacity(value.len());
        for (key, val) in value.iter() {
            pairs.push((
                extract_cell(&key, cx, depth + 1)?,
                extract_cell(&val, cx, depth + 1)?,
            ));
        }
        return Ok(CellKind::Dict(pairs));
    }
    Err(Cdf::Fallback)
}

fn extract_seq<'py>(
    items: impl Iterator<Item = Bound<'py, PyAny>>,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<Vec<Cell<'py>>, Cdf> {
    let mut cells = Vec::new();
    for item in items {
        cells.push(extract_cell(&item, cx, depth + 1)?);
    }
    Ok(cells)
}

fn extract_row<'py>(
    obj: &Bound<'py, PyAny>,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<CellKind<'py>, Cdf> {
    let fields_obj = obj.getattr("__fields__")?;
    let mut fields = Vec::new();
    for name in fields_obj.try_iter()? {
        fields.push(name?.extract::<String>()?);
    }
    let mut vals = Vec::new();
    for value in obj.try_iter()? {
        vals.push(extract_cell(&value?, cx, depth + 1)?);
    }
    if fields.len() != vals.len() {
        return Err(Cdf::Fallback);
    }
    Ok(CellKind::Row { fields, vals })
}
