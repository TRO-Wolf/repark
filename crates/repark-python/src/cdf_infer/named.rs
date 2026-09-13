use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};

use crate::cdf_infer::cells::{Cdf, Cell, CellKind, Ctx, extract_cell};
use crate::cdf_infer::screen::{
    ColumnScreen, TAG_NULL, screen_accepts_field, screen_accepts_inferred, tag_cell,
};
use crate::cdf_infer::{PyCdfArrowExport, build_batch, make_ctx, read_schema};

type Mappings<'py> = Vec<Bound<'py, PyDict>>;

type RowIndexMaps = (Vec<Vec<usize>>, Vec<usize>);

enum CollectedRows<'py> {
    Mappings(Mappings<'py>),
    Fields {
        field_tuples: Vec<Bound<'py, PyTuple>>,
        value_tuples: Vec<Bound<'py, PyTuple>>,
    },
}

enum NamedCells<'py> {
    Mappings(Mappings<'py>),
    Rows {
        value_tuples: Vec<Bound<'py, PyTuple>>,
        index_maps: Vec<Vec<usize>>,
        row_map: Vec<usize>,
    },
}

impl<'py> NamedCells<'py> {
    fn row_count(&self) -> usize {
        match self {
            Self::Mappings(mappings) => mappings.len(),
            Self::Rows { value_tuples, .. } => value_tuples.len(),
        }
    }

    fn width_matches(&self, row: usize, expected: usize) -> bool {
        match self {
            Self::Mappings(mappings) => mappings[row].len() == expected,
            Self::Rows { .. } => true,
        }
    }

    fn item(
        &self,
        row: usize,
        column: usize,
        lookup_keys: &[Bound<'py, PyString>],
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self {
            Self::Mappings(mappings) => mappings[row].get_item(&lookup_keys[column]),
            Self::Rows {
                value_tuples,
                index_maps,
                row_map,
            } => Ok(Some(
                value_tuples[row].get_item(index_maps[row_map[row]][column])?,
            )),
        }
    }
}

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

fn field_name_order(fields: &Bound<'_, PyTuple>) -> Option<Vec<String>> {
    let mut names: Vec<String> = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        let name = field.extract::<String>().ok()?;
        if !names.contains(&name) {
            names.push(name);
        }
    }
    Some(names)
}

fn row_field_tuples<'py>(
    py: Python<'py>,
    elements: &Bound<'py, PyList>,
    source: &HashSet<&str>,
    source_fields: &Bound<'py, PyTuple>,
) -> PyResult<Option<Vec<Bound<'py, PyTuple>>>> {
    let attr = pyo3::intern!(py, "_Row__field_names");
    let mut field_tuples: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(elements.len());
    field_tuples.push(source_fields.clone());
    for element in elements.iter().skip(1) {
        let Ok(fields) = element.getattr(attr)?.cast_into::<PyTuple>() else {
            return Ok(None);
        };
        if !fields.eq(source_fields)? {
            if fields.len() != source.len() {
                return Ok(None);
            }
            let mut all_known = true;
            for field in fields.iter() {
                let Ok(name) = field.extract::<String>() else {
                    return Ok(None);
                };
                if !source.contains(name.as_str()) {
                    all_known = false;
                    break;
                }
            }
            if !all_known {
                return Ok(None);
            }
        }
        field_tuples.push(fields);
    }
    Ok(Some(field_tuples))
}

fn collect_row_dicts<'py>(
    rows: &Bound<'py, PyList>,
) -> PyResult<Option<(Mappings<'py>, Vec<String>)>> {
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

fn collect_row_cells<'py>(
    py: Python<'py>,
    rows: &Bound<'py, PyList>,
) -> PyResult<Option<(CollectedRows<'py>, Vec<String>)>> {
    let row_type = py
        .import("repark.spark.row")?
        .getattr("Row")?
        .cast_into::<PyType>()?;
    for element in rows.iter() {
        if element.get_type().as_ptr() != row_type.as_ptr() {
            return Ok(None);
        }
    }
    let Ok(source_fields) = rows
        .get_item(0)?
        .getattr(pyo3::intern!(py, "_Row__field_names"))?
        .cast_into::<PyTuple>()
    else {
        return Ok(None);
    };
    let Some(source) = field_name_order(&source_fields) else {
        let Some((mappings, source)) = collect_row_dicts(rows)? else {
            return Ok(None);
        };
        return Ok(Some((CollectedRows::Mappings(mappings), source)));
    };
    let source_set: HashSet<&str> = source.iter().map(String::as_str).collect();
    let Some(field_tuples) = row_field_tuples(py, rows, &source_set, &source_fields)? else {
        return Ok(None);
    };
    let values_attr = pyo3::intern!(py, "_Row__field_values");
    let mut value_tuples: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(rows.len());
    let mut index_route = true;
    for (element, fields) in rows.iter().zip(field_tuples.iter()) {
        match element.getattr(values_attr)?.cast_into::<PyTuple>() {
            Ok(values) if values.len() == fields.len() => value_tuples.push(values),
            _ => {
                index_route = false;
                break;
            }
        }
    }
    if !index_route {
        let Some((mappings, source)) = collect_row_dicts(rows)? else {
            return Ok(None);
        };
        return Ok(Some((CollectedRows::Mappings(mappings), source)));
    }
    Ok(Some((
        CollectedRows::Fields {
            field_tuples,
            value_tuples,
        },
        source,
    )))
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

fn build_row_cells<'py>(
    field_tuples: &[Bound<'py, PyTuple>],
    lookup_names: &[String],
) -> PyResult<Option<RowIndexMaps>> {
    let mut index_maps: Vec<(Bound<'py, PyTuple>, Vec<usize>)> = Vec::new();
    let mut row_map: Vec<usize> = Vec::with_capacity(field_tuples.len());
    for fields in field_tuples {
        let mut found: Option<usize> = None;
        for (index, (cached, _)) in index_maps.iter().enumerate() {
            if fields.eq(cached)? {
                found = Some(index);
                break;
            }
        }
        let map_index = if let Some(index) = found {
            index
        } else {
            let mut last_pos: HashMap<String, usize> = HashMap::with_capacity(fields.len());
            for (position, field) in fields.iter().enumerate() {
                let Ok(name) = field.extract::<String>() else {
                    return Ok(None);
                };
                last_pos.insert(name, position);
            }
            if last_pos.len() != lookup_names.len()
                || lookup_names.iter().any(|name| !last_pos.contains_key(name))
            {
                return Ok(None);
            }
            index_maps.push((
                fields.clone(),
                lookup_names
                    .iter()
                    .map(|name| last_pos[name.as_str()])
                    .collect(),
            ));
            index_maps.len() - 1
        };
        row_map.push(map_index);
    }
    Ok(Some((
        index_maps
            .into_iter()
            .map(|(_, index_map)| index_map)
            .collect(),
        row_map,
    )))
}

fn resolve_lookup(
    strict: bool,
    source_names: Option<&[String]>,
    schema: Option<&arrow::datatypes::SchemaRef>,
    schema_names: Option<&[String]>,
    collected: &CollectedRows<'_>,
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
    if let Some(schema_ref) = schema {
        let names = schema_ref
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<String>>();
        return Some((names.clone(), names));
    }
    let CollectedRows::Mappings(mappings) = collected else {
        return None;
    };
    let union = dict_key_union_order(mappings)?;
    Some((union.clone(), union))
}

fn tag_named(
    cells: &NamedCells<'_>,
    lookup_keys: &[Bound<'_, PyString>],
    strict: bool,
    cx: &Ctx<'_>,
) -> PyResult<Option<Vec<ColumnScreen>>> {
    let mut screens = vec![ColumnScreen::default(); lookup_keys.len()];
    for row in 0..cells.row_count() {
        if strict && !cells.width_matches(row, lookup_keys.len()) {
            return Ok(None);
        }
        for (index, screen) in screens.iter_mut().enumerate() {
            let mut elem_merge = 0u16;
            let Some(item) = cells.item(row, index, lookup_keys)? else {
                if strict {
                    return Ok(None);
                }
                screen.record(TAG_NULL, 0);
                continue;
            };
            match tag_cell(&item, cx, 0, &mut elem_merge) {
                Ok(tag) => screen.record(tag, elem_merge),
                Err(Cdf::Fallback) => return Ok(None),
                Err(Cdf::Err(err)) => return Err(err),
            }
        }
    }
    Ok(Some(screens))
}

fn extract_named<'py>(
    cells: &NamedCells<'py>,
    lookup_keys: &[Bound<'py, PyString>],
    cx: &Ctx<'py>,
) -> PyResult<Option<Vec<Vec<Cell<'py>>>>> {
    let mut columns: Vec<Vec<Cell<'py>>> = (0..lookup_keys.len())
        .map(|_| Vec::with_capacity(cells.row_count()))
        .collect();
    for row in 0..cells.row_count() {
        for (index, column) in columns.iter_mut().enumerate() {
            let cell = match cells.item(row, index, lookup_keys)? {
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
            column.push(cell);
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
    let (collected, source_names) = if is_row {
        let Some((collected, source)) = collect_row_cells(py, &rows)? else {
            return Ok(None);
        };
        (collected, Some(source))
    } else {
        let Some((mappings, source)) = collect_dict_mappings(&rows, strict) else {
            return Ok(None);
        };
        (CollectedRows::Mappings(mappings), source)
    };
    let Some((names, lookup_names)) = resolve_lookup(
        strict,
        source_names.as_deref(),
        schema.as_ref(),
        schema_names.as_deref(),
        &collected,
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
    let cells = match collected {
        CollectedRows::Mappings(mappings) => NamedCells::Mappings(mappings),
        CollectedRows::Fields {
            field_tuples,
            value_tuples,
        } => {
            let Some((index_maps, row_map)) = build_row_cells(&field_tuples, &lookup_names)? else {
                return Ok(None);
            };
            NamedCells::Rows {
                value_tuples,
                index_maps,
                row_map,
            }
        }
    };
    let lookup_keys: Vec<Bound<'py, PyString>> = lookup_names
        .iter()
        .map(|name| PyString::new(py, name))
        .collect();
    let Some(screens) = tag_named(&cells, &lookup_keys, strict, &cx)? else {
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
    let Some(columns) = extract_named(&cells, &lookup_keys, &cx)? else {
        return Ok(None);
    };
    match build_batch(&names, &columns, schema, &cx)? {
        Some(batch) => Ok(Some(PyCdfArrowExport { batch })),
        None => Ok(None),
    }
}
