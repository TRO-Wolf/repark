use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};

use crate::cdf_infer::cells::{Cdf, Cell, CellKind, Ctx, extract_cell};
use crate::cdf_infer::screen::{
    ColumnScreen, TAG_NULL, screen_accepts_field, screen_accepts_inferred, tag_cell,
};
use crate::cdf_infer::{PyCdfArrowExport, build_batch, make_ctx, read_schema};

type Mappings<'py> = Vec<Bound<'py, PyDict>>;

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
    source: &[String],
    schema_names: Option<&[String]>,
) -> Option<(Vec<String>, Vec<usize>)> {
    let Some(schema) = schema_names else {
        let permutation: Vec<usize> = (0..source.len()).collect();
        return Some((source.to_vec(), permutation));
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
        Some((schema.to_vec(), permutation))
    } else if source_set.is_disjoint(&schema_set) {
        let permutation: Vec<usize> = (0..source.len()).collect();
        Some((schema.to_vec(), permutation))
    } else {
        None
    }
}

fn row_key_sets_match<'py>(
    py: Python<'py>,
    elements: &Bound<'py, PyList>,
    source: &HashSet<&str>,
    source_fields: &Bound<'py, PyTuple>,
) -> PyResult<bool> {
    let attr = pyo3::intern!(py, "_Row__field_names");
    for element in elements.iter().skip(1) {
        let Ok(fields) = element.getattr(attr)?.cast_into::<PyTuple>() else {
            return Ok(false);
        };
        if fields.eq(source_fields)? {
            continue;
        }
        if fields.len() != source.len() {
            return Ok(false);
        }
        let mut all_known = true;
        for field in fields.iter() {
            let Ok(name) = field.extract::<String>() else {
                return Ok(false);
            };
            if !source.contains(name.as_str()) {
                all_known = false;
                break;
            }
        }
        if !all_known {
            return Ok(false);
        }
    }
    Ok(true)
}

fn collect_row_mappings<'py>(
    py: Python<'py>,
    rows: &Bound<'py, PyList>,
) -> PyResult<Option<(Mappings<'py>, Vec<String>)>> {
    let row_type = py
        .import("repark.spark.row")?
        .getattr("Row")?
        .cast_into::<PyType>()?;
    for element in rows.iter() {
        if element.get_type().as_ptr() != row_type.as_ptr() {
            return Ok(None);
        }
    }
    let Ok(first_mapping) = rows
        .get_item(0)?
        .call_method0("asDict")?
        .cast_into::<PyDict>()
    else {
        return Ok(None);
    };
    let Some(source) = mapping_names(&first_mapping, false) else {
        return Ok(None);
    };
    let Ok(source_fields) = rows
        .get_item(0)?
        .getattr(pyo3::intern!(py, "_Row__field_names"))?
        .cast_into::<PyTuple>()
    else {
        return Ok(None);
    };
    let source_set: HashSet<&str> = source.iter().map(String::as_str).collect();
    if !row_key_sets_match(py, rows, &source_set, &source_fields)? {
        return Ok(None);
    }
    let mut mappings: Mappings<'py> = Vec::with_capacity(rows.len());
    mappings.push(first_mapping);
    for element in rows.iter().skip(1) {
        let Ok(mapping) = element.call_method0("asDict")?.cast_into::<PyDict>() else {
            return Ok(None);
        };
        mappings.push(mapping);
    }
    Ok(Some((mappings, source)))
}

fn collect_dict_mappings<'py>(
    rows: &Bound<'py, PyList>,
    strict: bool,
) -> Option<(Mappings<'py>, Option<Vec<String>>)> {
    let mut mappings: Mappings<'py> = Vec::with_capacity(rows.len());
    for element in rows.iter() {
        let Ok(mapping) = element.cast_into::<PyDict>() else {
            return None;
        };
        mappings.push(mapping);
    }
    let source = if strict {
        Some(
            mappings
                .first()
                .and_then(|mapping| mapping_names(mapping, false))?,
        )
    } else {
        None
    };
    Some((mappings, source))
}

fn resolve_lookup(
    strict: bool,
    is_row: bool,
    source_names: Option<&[String]>,
    schema: Option<&arrow::datatypes::SchemaRef>,
    schema_names: Option<&[String]>,
    mappings: &[Bound<'_, PyDict>],
) -> Option<(Vec<String>, Vec<String>)> {
    if strict {
        let source = source_names?;
        let (names, permutation) = strict_bind(source, schema_names)?;
        let lookup: Vec<String> = permutation
            .iter()
            .map(|index| source[*index].clone())
            .collect();
        return Some((names, lookup));
    }
    if is_row {
        return None;
    }
    if let Some(schema_ref) = schema {
        let names = schema_ref
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<String>>();
        return Some((names.clone(), names));
    }
    let union = dict_key_union_order(mappings)?;
    Some((union.clone(), union))
}

fn tag_named(
    mappings: &[Bound<'_, PyDict>],
    lookup_keys: &[Bound<'_, PyString>],
    strict: bool,
    cx: &Ctx<'_>,
) -> PyResult<Option<Vec<ColumnScreen>>> {
    let mut screens = vec![ColumnScreen::default(); lookup_keys.len()];
    for mapping in mappings {
        if strict && mapping.len() != lookup_keys.len() {
            return Ok(None);
        }
        for (index, key) in lookup_keys.iter().enumerate() {
            let mut elem_merge = 0u16;
            let Some(item) = mapping.get_item(key)? else {
                if strict {
                    return Ok(None);
                }
                screens[index].record(TAG_NULL, 0);
                continue;
            };
            match tag_cell(&item, cx, 0, &mut elem_merge) {
                Ok(tag) => screens[index].record(tag, elem_merge),
                Err(Cdf::Fallback) => return Ok(None),
                Err(Cdf::Err(err)) => return Err(err),
            }
        }
    }
    Ok(Some(screens))
}

fn extract_named<'py>(
    mappings: &[Bound<'py, PyDict>],
    lookup_keys: &[Bound<'py, PyString>],
    cx: &Ctx<'py>,
) -> PyResult<Option<Vec<Vec<Cell<'py>>>>> {
    let mut columns: Vec<Vec<Cell<'py>>> = (0..lookup_keys.len())
        .map(|_| Vec::with_capacity(mappings.len()))
        .collect();
    for mapping in mappings {
        for (index, key) in lookup_keys.iter().enumerate() {
            let cell = match mapping.get_item(key)? {
                Some(item) => match extract_cell(&item, cx, 0) {
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
    Ok(Some(columns))
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
    let schema = match schema {
        Some(schema_obj) => read_schema(&schema_obj)?,
        None => None,
    };
    let strict = is_row || (schema_names.is_some() && schema.is_none());
    let (mappings, source_names) = if is_row {
        let Some((collected, source)) = collect_row_mappings(py, &rows)? else {
            return Ok(None);
        };
        (collected, Some(source))
    } else {
        let Some(collected) = collect_dict_mappings(&rows, strict) else {
            return Ok(None);
        };
        collected
    };
    let Some((names, lookup_names)) = resolve_lookup(
        strict,
        is_row,
        source_names.as_deref(),
        schema.as_ref(),
        schema_names.as_deref(),
        &mappings,
    ) else {
        return Ok(None);
    };
    if names.is_empty() {
        return Ok(None);
    }
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
    let Some(screens) = tag_named(&mappings, &lookup_keys, strict, &cx)? else {
        return Ok(None);
    };
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
    let Some(columns) = extract_named(&mappings, &lookup_keys, &cx)? else {
        return Ok(None);
    };
    match build_batch(&names, &columns, schema, &cx)? {
        Some(batch) => Ok(Some(PyCdfArrowExport { batch })),
        None => Ok(None),
    }
}
