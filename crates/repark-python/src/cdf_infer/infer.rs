use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use crate::cdf_infer::cells::{Cdf, Cell, CellKind, Ctx};

pub(crate) fn default_timestamp(cx: &Ctx<'_>) -> DataType {
    if cx.timestamp_ntz {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    } else {
        DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        )
    }
}

fn merge_kind(cell: &Cell<'_>) -> Option<u8> {
    match cell.kind {
        CellKind::Bool(_) => Some(0),
        CellKind::Int(_) => Some(1),
        CellKind::Float(_) => Some(2),
        CellKind::Dec { .. } => Some(3),
        CellKind::Date(_) => Some(4),
        CellKind::Dt { .. } => Some(5),
        _ => None,
    }
}

fn refuse_scalar_merge(col: &[Cell<'_>]) -> Result<(), Cdf> {
    let mut kinds = 0u8;
    for cell in col {
        if let Some(kind) = merge_kind(cell) {
            kinds |= 1 << kind;
            if kinds.count_ones() >= 2 {
                return Err(Cdf::Fallback);
            }
        }
    }
    Ok(())
}

fn refuse_list_element_merge(col: &[Cell<'_>]) -> Result<(), Cdf> {
    let mut kinds = 0u8;
    for cell in col {
        if let CellKind::List(items) = &cell.kind {
            for item in items {
                if let Some(kind) = merge_kind(item) {
                    kinds |= 1 << kind;
                    if kinds.count_ones() >= 2 {
                        return Err(Cdf::Fallback);
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn list_field(child: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", child, true)))
}

fn map_field(key: DataType, value: DataType) -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ])),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

fn map_child(entries: &FieldRef, index: usize) -> Result<DataType, Cdf> {
    match entries.data_type() {
        DataType::Struct(fields) => fields
            .get(index)
            .map(|field| field.data_type().clone())
            .ok_or(Cdf::Fallback),
        _ => Err(Cdf::Fallback),
    }
}

fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

pub(crate) fn merge_types(left: &DataType, right: &DataType, depth: u32) -> Result<DataType, Cdf> {
    if depth > 100 {
        return Err(Cdf::Fallback);
    }
    if left == right {
        return Ok(left.clone());
    }
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    if matches!(right, DataType::Null) {
        return Ok(left.clone());
    }
    match (left, right) {
        (DataType::List(left_child), DataType::List(right_child)) => Ok(list_field(merge_types(
            left_child.data_type(),
            right_child.data_type(),
            depth + 1,
        )?)),
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            let mut fields: Vec<Field> = Vec::new();
            let mut seen: Vec<&str> = Vec::new();
            for left_field in left_fields {
                seen.push(left_field.name().as_str());
                if let Some(right_field) = right_fields
                    .iter()
                    .find(|candidate| candidate.name() == left_field.name())
                {
                    fields.push(Field::new(
                        left_field.name().as_str(),
                        merge_types(left_field.data_type(), right_field.data_type(), depth + 1)?,
                        true,
                    ));
                } else {
                    fields.push((**left_field).clone());
                }
            }
            for right_field in right_fields {
                if !seen.contains(&right_field.name().as_str()) {
                    fields.push((**right_field).clone());
                }
            }
            Ok(DataType::Struct(Fields::from(fields)))
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            let key = merge_types(
                &map_child(left_entries, 0)?,
                &map_child(right_entries, 0)?,
                depth + 1,
            )?;
            let value = merge_types(
                &map_child(left_entries, 1)?,
                &map_child(right_entries, 1)?,
                depth + 1,
            )?;
            Ok(map_field(key, value))
        }
        _ => {
            if matches!(left, DataType::Utf8 | DataType::LargeUtf8) {
                if is_nested(right) {
                    return Err(Cdf::Fallback);
                }
                return Ok(left.clone());
            }
            if matches!(right, DataType::Utf8 | DataType::LargeUtf8) {
                if is_nested(left) {
                    return Err(Cdf::Fallback);
                }
                return Ok(right.clone());
            }
            Err(Cdf::Fallback)
        }
    }
}

fn dict_key_name<'c>(cell: &'c Cell<'_>) -> Result<&'c str, Cdf> {
    match &cell.kind {
        CellKind::Str(name) => Ok(name.as_str()),
        _ => Err(Cdf::Fallback),
    }
}

pub(crate) fn struct_from_dict_samples<'py>(
    samples: &[&[(Cell<'py>, Cell<'py>)]],
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<DataType, Cdf> {
    let mut order: Vec<String> = Vec::new();
    let mut types: Vec<DataType> = Vec::new();
    for pairs in samples {
        for (key, value) in *pairs {
            if matches!(key.kind, CellKind::Null) {
                return Err(Cdf::Fallback);
            }
            if matches!(value.kind, CellKind::Null) {
                continue;
            }
            let name = dict_key_name(key)?;
            let inferred = nested_infer(value, cx, depth + 1)?;
            if let Some(index) = order.iter().position(|existing| existing == name) {
                types[index] = merge_types(&types[index], &inferred, depth + 1)?;
            } else {
                order.push(name.to_owned());
                types.push(inferred);
            }
        }
    }
    let fields: Vec<Field> = order
        .into_iter()
        .zip(types)
        .map(|(name, data_type)| Field::new(name, data_type, true))
        .collect();
    Ok(DataType::Struct(Fields::from(fields)))
}

fn map_from_dict_sample<'py>(
    pairs: &[(Cell<'py>, Cell<'py>)],
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<DataType, Cdf> {
    if pairs.is_empty() {
        return Ok(map_field(DataType::Utf8, DataType::Utf8));
    }
    let mut value_types: Vec<DataType> = Vec::new();
    for (_key, value) in pairs {
        if matches!(value.kind, CellKind::Null) {
            continue;
        }
        value_types.push(nested_infer(value, cx, depth + 1)?);
    }
    let value_type = match value_types.split_first() {
        None => DataType::Utf8,
        Some((first, rest)) => {
            if rest.iter().all(|candidate| candidate == first) {
                first.clone()
            } else {
                DataType::Utf8
            }
        }
    };
    Ok(map_field(
        nested_infer(&pairs[0].0, cx, depth + 1)?,
        value_type,
    ))
}

fn dict_pairs_is_sparse(pairs: &[(Cell<'_>, Cell<'_>)]) -> bool {
    if pairs.len() != 3 {
        return false;
    }
    let mut size = false;
    let mut indices = false;
    let mut values = false;
    for (key, value) in pairs {
        match &key.kind {
            CellKind::Str(name) if name.as_str() == "size" => {
                size = matches!(value.kind, CellKind::Int(_));
            }
            CellKind::Str(name) if name.as_str() == "indices" => {
                indices = matches!(value.kind, CellKind::List(_) | CellKind::Tup(_));
            }
            CellKind::Str(name) if name.as_str() == "values" => {
                values = matches!(value.kind, CellKind::List(_) | CellKind::Tup(_));
            }
            _ => {}
        }
    }
    size && indices && values
}

pub(crate) fn nested_infer<'py>(
    cell: &Cell<'py>,
    cx: &Ctx<'py>,
    depth: u32,
) -> Result<DataType, Cdf> {
    if depth > 100 {
        return Err(Cdf::Fallback);
    }
    match &cell.kind {
        CellKind::Null | CellKind::Str(_) => Ok(DataType::Utf8),
        CellKind::Bool(_) => Ok(DataType::Boolean),
        CellKind::Int(_) => Ok(DataType::Int64),
        CellKind::Float(_) => Ok(DataType::Float64),
        CellKind::Bin(_) => Ok(DataType::Binary),
        CellKind::Date(_) => Ok(DataType::Date32),
        CellKind::Dt { .. } => Ok(default_timestamp(cx)),
        CellKind::Dec { .. } => Ok(DataType::Decimal128(38, 18)),
        CellKind::List(items) => {
            if !cx.infer_dict_as_struct {
                let element = items
                    .iter()
                    .find(|item| !matches!(item.kind, CellKind::Null));
                return Ok(list_field(match element {
                    Some(item) => nested_infer(item, cx, depth + 1)?,
                    None => DataType::Utf8,
                }));
            }
            let non_null: Vec<&Cell<'py>> = items
                .iter()
                .filter(|item| !matches!(item.kind, CellKind::Null))
                .collect();
            if non_null.is_empty() {
                return Ok(list_field(DataType::Null));
            }
            if non_null
                .iter()
                .all(|item| matches!(item.kind, CellKind::Dict(_)))
            {
                let mut dict_items: Vec<&[(Cell<'py>, Cell<'py>)]> = Vec::new();
                if cx.legacy_first_element {
                    if let CellKind::Dict(pairs) = &non_null[0].kind {
                        dict_items.push(pairs.as_slice());
                    }
                } else {
                    for item in &non_null {
                        if let CellKind::Dict(pairs) = &item.kind {
                            dict_items.push(pairs.as_slice());
                        }
                    }
                }
                return Ok(list_field(struct_from_dict_samples(
                    &dict_items,
                    cx,
                    depth + 1,
                )?));
            }
            if cx.legacy_first_element {
                return Ok(list_field(nested_infer(non_null[0], cx, depth + 1)?));
            }
            let mut merged = nested_infer(non_null[0], cx, depth + 1)?;
            for item in &non_null[1..] {
                merged = merge_types(&merged, &nested_infer(item, cx, depth + 1)?, depth + 1)?;
            }
            Ok(list_field(merged))
        }
        CellKind::Tup(items) => {
            let fields: Vec<Field> = items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    Ok(Field::new(
                        format!("_{}", index + 1),
                        nested_infer(item, cx, depth + 1)?,
                        true,
                    ))
                })
                .collect::<Result<_, Cdf>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        CellKind::Row { fields, vals } => {
            let children: Vec<Field> = fields
                .iter()
                .zip(vals.iter())
                .map(|(name, value)| {
                    Ok(Field::new(
                        name.as_str(),
                        nested_infer(value, cx, depth + 1)?,
                        true,
                    ))
                })
                .collect::<Result<_, Cdf>>()?;
            Ok(DataType::Struct(Fields::from(children)))
        }
        CellKind::Dict(pairs) => {
            if cx.infer_dict_as_struct {
                let sample: Vec<&[(Cell<'py>, Cell<'py>)]> = vec![pairs.as_slice()];
                struct_from_dict_samples(&sample, cx, depth + 1)
            } else {
                map_from_dict_sample(pairs, cx, depth + 1)
            }
        }
        CellKind::Fill => Err(Cdf::Fallback),
    }
}

fn infer_dict_column<'py>(
    col: &[Cell<'py>],
    sample: &Cell<'py>,
    cx: &Ctx<'py>,
) -> Result<DataType, Cdf> {
    let CellKind::Dict(sample_pairs) = &sample.kind else {
        return Err(Cdf::Fallback);
    };
    if dict_pairs_is_sparse(sample_pairs) {
        return Ok(DataType::Struct(Fields::from(vec![
            Field::new("size", DataType::Int32, true),
            Field::new("indices", list_field(DataType::Int32), true),
            Field::new("values", list_field(DataType::Float64), true),
        ])));
    }
    if cx.infer_dict_as_struct {
        let mut dict_samples: Vec<&[(Cell<'py>, Cell<'py>)]> = Vec::new();
        for cell in col {
            if let CellKind::Dict(pairs) = &cell.kind {
                dict_samples.push(pairs.as_slice());
            }
        }
        if dict_samples.is_empty() {
            dict_samples.push(sample_pairs.as_slice());
        }
        return struct_from_dict_samples(&dict_samples, cx, 0);
    }
    let mut map_sample = sample;
    let needs_witness = sample_pairs.is_empty()
        || sample_pairs
            .iter()
            .all(|(_k, v)| matches!(v.kind, CellKind::Null));
    if needs_witness {
        for cell in col {
            if let CellKind::Dict(pairs) = &cell.kind
                && pairs
                    .iter()
                    .any(|(_k, v)| !matches!(v.kind, CellKind::Null))
            {
                map_sample = cell;
                break;
            }
        }
        if let CellKind::Dict(pairs) = &map_sample.kind
            && pairs.is_empty()
        {
            for cell in col {
                if let CellKind::Dict(candidate) = &cell.kind
                    && !candidate.is_empty()
                {
                    map_sample = cell;
                    break;
                }
            }
        }
    }
    nested_infer(map_sample, cx, 0)
}

fn infer_list_column<'py>(
    col: &[Cell<'py>],
    sample: &Cell<'py>,
    cx: &Ctx<'py>,
) -> Result<DataType, Cdf> {
    refuse_list_element_merge(col)?;
    if let CellKind::List(items) = &sample.kind
        && !items.is_empty()
        && items
            .iter()
            .all(|item| matches!(item.kind, CellKind::Float(_)))
    {
        return Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            i32::try_from(items.len()).map_err(|_| Cdf::Fallback)?,
        ));
    }
    if cx.infer_dict_as_struct {
        let mut dict_elements: Vec<&[(Cell<'py>, Cell<'py>)]> = Vec::new();
        let mut saw_non_dict = false;
        'scan: for cell in col {
            if let CellKind::List(items) = &cell.kind {
                for item in items {
                    if matches!(item.kind, CellKind::Null) {
                        continue;
                    }
                    if let CellKind::Dict(pairs) = &item.kind {
                        dict_elements.push(pairs.as_slice());
                        if cx.legacy_first_element {
                            break;
                        }
                    } else {
                        saw_non_dict = true;
                    }
                }
                if cx.legacy_first_element && !dict_elements.is_empty() {
                    break 'scan;
                }
            }
        }
        if !dict_elements.is_empty() && !saw_non_dict {
            return Ok(list_field(struct_from_dict_samples(&dict_elements, cx, 0)?));
        }
        if saw_non_dict {
            let mut element_types: Vec<DataType> = Vec::new();
            'rows: for cell in col {
                if let CellKind::List(items) = &cell.kind {
                    for item in items {
                        if matches!(item.kind, CellKind::Null) {
                            continue;
                        }
                        element_types.push(nested_infer(item, cx, 0)?);
                        if cx.legacy_first_element {
                            break;
                        }
                    }
                    if cx.legacy_first_element && !element_types.is_empty() {
                        break 'rows;
                    }
                }
            }
            if !element_types.is_empty() {
                let mut merged = element_types[0].clone();
                for element_type in &element_types[1..] {
                    merged = merge_types(&merged, element_type, 0)?;
                }
                return Ok(list_field(merged));
            }
        }
    }
    let CellKind::List(first_items) = &sample.kind else {
        return Err(Cdf::Fallback);
    };
    let mut element = first_items
        .iter()
        .find(|item| !matches!(item.kind, CellKind::Null));
    if element.is_none() {
        for cell in col {
            if let CellKind::List(items) = &cell.kind
                && let Some(found) = items
                    .iter()
                    .find(|item| !matches!(item.kind, CellKind::Null))
            {
                element = Some(found);
                break;
            }
        }
    }
    Ok(list_field(match element {
        Some(item) => nested_infer(item, cx, 0)?,
        None => DataType::Utf8,
    }))
}

pub(crate) fn infer_column_type<'py>(col: &[Cell<'py>], cx: &Ctx<'py>) -> Result<DataType, Cdf> {
    let Some(sample) = col.iter().find(|cell| !matches!(cell.kind, CellKind::Null)) else {
        return Ok(DataType::Utf8);
    };
    match &sample.kind {
        CellKind::Bool(_) => {
            refuse_scalar_merge(col)?;
            Ok(DataType::Boolean)
        }
        CellKind::Int(_) => {
            refuse_scalar_merge(col)?;
            Ok(DataType::Int64)
        }
        CellKind::Float(_) => {
            refuse_scalar_merge(col)?;
            Ok(DataType::Float64)
        }
        CellKind::List(_) => infer_list_column(col, sample, cx),
        CellKind::Row { .. } | CellKind::Tup(_) => nested_infer(sample, cx, 0),
        CellKind::Dict(_) => infer_dict_column(col, sample, cx),
        CellKind::Dt { .. } => {
            refuse_scalar_merge(col)?;
            Ok(default_timestamp(cx))
        }
        CellKind::Date(_) => {
            refuse_scalar_merge(col)?;
            Ok(DataType::Date32)
        }
        CellKind::Dec { .. } => {
            refuse_scalar_merge(col)?;
            Ok(DataType::Decimal128(38, 18))
        }
        CellKind::Bin(_) => Ok(DataType::Binary),
        CellKind::Str(_) | CellKind::Null => Ok(DataType::Utf8),
        CellKind::Fill => Err(Cdf::Fallback),
    }
}
