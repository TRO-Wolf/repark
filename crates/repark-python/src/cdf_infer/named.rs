use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyType};

use crate::cdf_infer::cells::{Cdf, Cell, CellKind, extract_cell};
use crate::cdf_infer::screen::{
    ColumnScreen, TAG_NULL, screen_accepts_field, screen_accepts_inferred, tag_cell,
};
use crate::cdf_infer::{PyCdfArrowExport, build_batch, make_ctx, read_schema};

fn mapping_names(mapping: &Bound<'_, PyDict>, sorted: bool) -> Option<Vec<String>> {
    let mut keys: Vec<String> = Vec::with_capacity(mapping.len());
    for key in mapping.keys().iter() {
        keys.push(key.extract::<String>().ok()?);
    }
    if sorted {
        keys.sort();
    }
    Some(keys)
}

fn dict_key_union_order(mappings: &[Bound<'_, PyDict>]) -> Option<Vec<String>> {
    let mut names = mapping_names(mappings.first()?, true)?;
    let mut seen: HashSet<String> = names.iter().cloned().collect();
    for mapping in mappings.iter().skip(1) {
        for key in mapping_names(mapping, true)? {
            if seen.insert(key.clone()) {
                names.push(key);
            }
        }
    }
    Some(names)
}

fn strict_bind(
    mappings: &[Bound<'_, PyDict>],
    schema_names: Option<&[String]>,
) -> Option<(Vec<String>, Vec<String>, Vec<usize>)> {
    let source = mapping_names(mappings.first()?, false)?;
    let Some(schema) = schema_names else {
        let permutation: Vec<usize> = (0..source.len()).collect();
        return Some((source.clone(), source, permutation));
    };
    if schema.len() != source.len() {
        return None;
    }
    {
        let mut seen = HashSet::with_capacity(source.len());
        if source.iter().any(|name| !seen.insert(name)) {
            return None;
        }
        let mut seen = HashSet::with_capacity(schema.len());
        if schema.iter().any(|name| !seen.insert(name)) {
            return None;
        }
    }
    let source_set: HashSet<&str> = source.iter().map(String::as_str).collect();
    let schema_set: HashSet<&str> = schema.iter().map(String::as_str).collect();
    if source_set == schema_set {
        let index_by_name: HashMap<&str, usize> = source
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect();
        let permutation: Vec<usize> = schema
            .iter()
            .map(|name| index_by_name[name.as_str()])
            .collect();
        Some((schema.to_vec(), source, permutation))
    } else if source_set.is_disjoint(&schema_set) {
        let permutation: Vec<usize> = (0..source.len()).collect();
        Some((schema.to_vec(), source, permutation))
    } else {
        None
    }
}

fn strict_key_set_matches(mappings: &[Bound<'_, PyDict>], source: &[String]) -> bool {
    let source_set: HashSet<&str> = source.iter().map(String::as_str).collect();
    for mapping in mappings.iter().skip(1) {
        if mapping.len() != source.len() {
            return false;
        }
        for key in mapping.keys().iter() {
            let Ok(name) = key.extract::<String>() else {
                return false;
            };
            if !source_set.contains(name.as_str()) {
                return false;
            }
        }
    }
    true
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::fn_params_excessive_bools)]
pub fn cdf_arrow_export_named<'py>(
    py: Python<'py>,
    is_row: bool,
    rows: Bound<'py, PyList>,
    schema_names: Option<Vec<String>>,
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
    let cx = make_ctx(
        py,
        session_tz_utc,
        timestamp_ntz,
        infer_dict_as_struct,
        legacy_first_element,
        decimal_prec,
    )?;
    let mut mappings: Vec<Bound<'py, PyDict>> = Vec::with_capacity(rows.len());
    if is_row {
        let row_type = py
            .import("repark.spark.row")?
            .getattr("Row")?
            .cast_into::<PyType>()?;
        for element in rows.iter() {
            if element.get_type().as_ptr() != row_type.as_ptr() {
                return Ok(None);
            }
            let Ok(mapping) = element.call_method0("asDict")?.cast_into::<PyDict>() else {
                return Ok(None);
            };
            mappings.push(mapping);
        }
    } else {
        for element in rows.iter() {
            if element.get_type().as_ptr() != cx.dict_type.as_ptr() {
                return Ok(None);
            }
            let Ok(mapping) = element.cast_into::<PyDict>() else {
                return Ok(None);
            };
            mappings.push(mapping);
        }
    }
    let schema = match schema {
        Some(schema_obj) => read_schema(&schema_obj)?,
        None => None,
    };
    let (names, lookup_names) = if !is_row && schema.is_none() && schema_names.is_none() {
        let Some(union) = dict_key_union_order(&mappings) else {
            return Ok(None);
        };
        (union.clone(), union)
    } else if !is_row && schema.is_some() {
        let names = schema.as_ref().map_or_else(Vec::new, |schema_ref| {
            schema_ref
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<String>>()
        });
        (names.clone(), names)
    } else {
        let Some((names, source, permutation)) = strict_bind(&mappings, schema_names.as_deref())
        else {
            return Ok(None);
        };
        if !strict_key_set_matches(&mappings, &source) {
            return Ok(None);
        }
        let lookup: Vec<String> = permutation
            .iter()
            .map(|index| source[*index].clone())
            .collect();
        (names, lookup)
    };
    let mut seen_names = HashSet::with_capacity(names.len());
    if names.iter().any(|name| !seen_names.insert(name)) {
        return Ok(None);
    }
    if let Some(schema_ref) = &schema
        && schema_ref.fields().len() != names.len()
    {
        return Ok(None);
    }
    let lookup_keys: Vec<Bound<'py, PyString>> = lookup_names
        .iter()
        .map(|name| PyString::new(py, name))
        .collect();
    let mut screens = vec![ColumnScreen::default(); names.len()];
    for mapping in &mappings {
        for (index, key) in lookup_keys.iter().enumerate() {
            let mut elem_merge = 0u16;
            let tag = match mapping.get_item(key)? {
                Some(item) => match tag_cell(&item, &cx, 0, &mut elem_merge) {
                    Ok(tag) => tag,
                    Err(Cdf::Fallback) => return Ok(None),
                    Err(Cdf::Err(err)) => return Err(err),
                },
                None => TAG_NULL,
            };
            screens[index].record(tag, elem_merge);
        }
    }
    match &schema {
        Some(schema_ref) => {
            for (index, field) in schema_ref.fields().iter().enumerate() {
                if !screen_accepts_field(screens[index], field.data_type()) {
                    return Ok(None);
                }
            }
        }
        None => {
            for screen in &screens {
                if !screen_accepts_inferred(*screen) {
                    return Ok(None);
                }
            }
        }
    }
    let mut columns: Vec<Vec<Cell<'py>>> = (0..names.len())
        .map(|_| Vec::with_capacity(mappings.len()))
        .collect();
    for mapping in &mappings {
        for (index, key) in lookup_keys.iter().enumerate() {
            let cell = match mapping.get_item(key)? {
                Some(item) => match extract_cell(&item, &cx, 0) {
                    Ok(cell) => cell,
                    Err(Cdf::Fallback) => return Ok(None),
                    Err(Cdf::Err(err)) => return Err(err),
                },
                None => Cell {
                    obj: None,
                    kind: CellKind::Null,
                },
            };
            columns[index].push(cell);
        }
    }
    match build_batch(&names, &columns, schema, &cx)? {
        Some(batch) => Ok(Some(PyCdfArrowExport { batch })),
        None => Ok(None),
    }
}
