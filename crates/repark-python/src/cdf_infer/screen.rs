use pyo3::Bound;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyMemoryView,
    PyString, PyTime, PyTuple,
};

use arrow::datatypes::DataType;

use crate::cdf_infer::cells::{Cdf, Ctx, MAX_DEPTH};

pub(crate) const TAG_NULL: u16 = 1;
const TAG_BOOL: u16 = 1 << 1;
const TAG_INT: u16 = 1 << 2;
const TAG_FLOAT: u16 = 1 << 3;
const TAG_STR: u16 = 1 << 4;
const TAG_BIN: u16 = 1 << 5;
const TAG_DATE: u16 = 1 << 6;
const TAG_DT: u16 = 1 << 7;
const TAG_DEC: u16 = 1 << 8;
const TAG_LIST: u16 = 1 << 9;
const TAG_TUP: u16 = 1 << 10;
const TAG_ROW: u16 = 1 << 11;
const TAG_DICT: u16 = 1 << 12;

const MERGE_TAGS: u16 = TAG_BOOL | TAG_INT | TAG_FLOAT | TAG_DEC | TAG_DATE | TAG_DT;

#[derive(Clone, Copy, Default)]
pub(crate) struct ColumnScreen {
    kinds: u16,
    first: u16,
    elem_merge: u16,
}

impl ColumnScreen {
    pub(crate) fn record(&mut self, tag: u16, elem_merge: u16) {
        self.kinds |= tag;
        self.elem_merge |= elem_merge;
        if self.first == 0 && tag != TAG_NULL {
            self.first = tag;
        }
    }
}

fn tag_seq<'py>(
    items: impl Iterator<Item = Bound<'py, PyAny>>,
    cx: &Ctx<'py>,
    depth: u32,
    elem_merge: &mut u16,
) -> Result<(), Cdf> {
    for item in items {
        let mut inner = 0u16;
        let tag = tag_cell(&item, cx, depth + 1, &mut inner)?;
        *elem_merge |= tag & MERGE_TAGS;
    }
    Ok(())
}

fn tag_dict(dict: &Bound<'_, PyDict>, cx: &Ctx<'_>, depth: u32) -> Result<(), Cdf> {
    for (key, value) in dict.iter() {
        let mut inner = 0u16;
        let key_tag = tag_cell(&key, cx, depth + 1, &mut inner)?;
        let value_tag = tag_cell(&value, cx, depth + 1, &mut inner)?;
        if cx.infer_dict_as_struct
            && (key_tag == TAG_NULL || (key_tag != TAG_STR && value_tag != TAG_NULL))
        {
            return Err(Cdf::Fallback);
        }
    }
    Ok(())
}

fn tag_row<'py>(obj: &Bound<'py, PyAny>, cx: &Ctx<'py>, depth: u32) -> Result<bool, Cdf> {
    if obj.get_type().name()?.to_str()? != "Row" {
        return Ok(false);
    }
    let module = obj.get_type().getattr("__module__")?;
    if !module.extract::<String>()?.starts_with("repark") {
        return Ok(false);
    }
    for value in obj.try_iter()? {
        let mut inner = 0u16;
        tag_cell(&value?, cx, depth + 1, &mut inner)?;
    }
    Ok(true)
}

fn tag_slow<'py>(
    obj: &Bound<'py, PyAny>,
    cx: &Ctx<'py>,
    depth: u32,
    elem_merge: &mut u16,
) -> Result<u16, Cdf> {
    if obj.cast::<PyBool>().is_ok() {
        return Ok(TAG_BOOL);
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(TAG_INT);
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(TAG_FLOAT);
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(TAG_STR);
    }
    if obj.cast::<PyBytes>().is_ok()
        || obj.cast::<PyByteArray>().is_ok()
        || obj.cast::<PyMemoryView>().is_ok()
    {
        return Ok(TAG_BIN);
    }
    if obj.cast::<PyDateTime>().is_ok() {
        return Ok(TAG_DT);
    }
    if obj.is_instance_of::<PyTime>() {
        return Ok(TAG_STR);
    }
    if obj.cast::<PyDate>().is_ok() {
        return Ok(TAG_DATE);
    }
    if obj.is_instance(&cx.decimal_type)? {
        return Ok(TAG_DEC);
    }
    if let Ok(list) = obj.cast::<PyList>() {
        tag_seq(list.iter(), cx, depth, elem_merge)?;
        return Ok(TAG_LIST);
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        tag_seq(tuple.iter(), cx, depth, &mut 0)?;
        return Ok(TAG_TUP);
    }
    if tag_row(obj, cx, depth)? {
        return Ok(TAG_ROW);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        tag_dict(dict, cx, depth)?;
        return Ok(TAG_DICT);
    }
    Err(Cdf::Fallback)
}

pub(crate) fn tag_cell<'py>(
    obj: &Bound<'py, PyAny>,
    cx: &Ctx<'py>,
    depth: u32,
    elem_merge: &mut u16,
) -> Result<u16, Cdf> {
    if depth > MAX_DEPTH {
        return Err(Cdf::Fallback);
    }
    if obj.is_none() {
        return Ok(TAG_NULL);
    }
    let tp = obj.get_type().as_ptr();
    if tp == cx.bool_type.as_ptr() {
        return Ok(TAG_BOOL);
    }
    if tp == cx.int_type.as_ptr() {
        return Ok(TAG_INT);
    }
    if tp == cx.float_type.as_ptr() {
        return Ok(TAG_FLOAT);
    }
    if tp == cx.str_type.as_ptr() {
        return Ok(TAG_STR);
    }
    if tp == cx.bytes_type.as_ptr()
        || tp == cx.bytearray_type.as_ptr()
        || tp == cx.memoryview_type.as_ptr()
    {
        return Ok(TAG_BIN);
    }
    if tp == cx.datetime_type.as_ptr() {
        return Ok(TAG_DT);
    }
    if tp == cx.time_type.as_ptr() {
        return Ok(TAG_STR);
    }
    if tp == cx.date_type.as_ptr() {
        return Ok(TAG_DATE);
    }
    if tp == cx.decimal_type.as_ptr() {
        return Ok(TAG_DEC);
    }
    if tp == cx.list_type.as_ptr()
        && let Ok(list) = obj.cast::<PyList>()
    {
        tag_seq(list.iter(), cx, depth, elem_merge)?;
        return Ok(TAG_LIST);
    }
    if tp == cx.tuple_type.as_ptr()
        && let Ok(tuple) = obj.cast::<PyTuple>()
    {
        tag_seq(tuple.iter(), cx, depth, &mut 0)?;
        return Ok(TAG_TUP);
    }
    if tp == cx.dict_type.as_ptr()
        && let Ok(dict) = obj.cast::<PyDict>()
    {
        tag_dict(dict, cx, depth)?;
        return Ok(TAG_DICT);
    }
    tag_slow(obj, cx, depth, elem_merge)
}

fn inferred_buildable(first: u16) -> u16 {
    match first {
        TAG_BOOL => TAG_NULL | TAG_BOOL,
        TAG_INT => TAG_NULL | TAG_INT,
        TAG_FLOAT => TAG_NULL | TAG_INT | TAG_FLOAT,
        TAG_BIN => TAG_NULL | TAG_BIN | TAG_STR,
        TAG_DATE => TAG_NULL | TAG_DATE | TAG_DT,
        TAG_DT => TAG_NULL | TAG_DT,
        TAG_DEC => TAG_NULL | TAG_DEC,
        _ => u16::MAX,
    }
}

fn field_buildable(data_type: &DataType) -> u16 {
    match data_type {
        DataType::Boolean => TAG_NULL | TAG_BOOL,
        DataType::Int64 => TAG_NULL | TAG_INT,
        DataType::Float64 => TAG_NULL | TAG_INT | TAG_FLOAT,
        DataType::Binary => TAG_NULL | TAG_BIN | TAG_STR,
        DataType::Date32 => TAG_NULL | TAG_DATE | TAG_DT,
        DataType::Timestamp(..) => TAG_NULL | TAG_DT,
        DataType::Decimal128(38, 18) => TAG_NULL | TAG_DEC,
        _ => u16::MAX,
    }
}

pub(crate) fn screen_accepts_inferred(screen: ColumnScreen) -> bool {
    if screen.kinds == TAG_NULL {
        return true;
    }
    if (screen.kinds & MERGE_TAGS).count_ones() >= 2 {
        return false;
    }
    if screen.first == TAG_LIST && screen.elem_merge.count_ones() >= 2 {
        return false;
    }
    screen.kinds & !inferred_buildable(screen.first) == 0
}

pub(crate) fn screen_accepts_field(screen: ColumnScreen, data_type: &DataType) -> bool {
    screen.kinds & !field_buildable(data_type) == 0
}
