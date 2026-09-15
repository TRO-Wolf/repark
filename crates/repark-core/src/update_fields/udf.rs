use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StructArray, make_array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

const UPDATE_FIELDS: &str = "update_fields";
const OP_WITH: &str = "with";
const OP_DROP: &str = "drop";

#[must_use]
pub fn update_fields_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(UpdateFields::new()))
}

#[derive(Debug)]
struct UpdateFields {
    signature: Signature,
}

impl UpdateFields {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for UpdateFields {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for UpdateFields {}

impl Hash for UpdateFields {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

struct PlanEdit {
    drop: bool,
    path: Vec<String>,
    value: Option<FieldRef>,
    value_repr: String,
}

struct ExecEdit {
    drop: bool,
    path: Vec<String>,
    value: Option<(FieldRef, ArrayRef)>,
}

fn utf8_literal(scalar: Option<&ScalarValue>) -> Result<&str> {
    match scalar {
        Some(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
        ) => Ok(text),
        other => {
            plan_err!("{UPDATE_FIELDS} edit tags and paths must be Utf8 literals, got {other:?}")
        }
    }
}

fn utf8_argument(value: &ColumnarValue) -> Result<&str> {
    match value {
        ColumnarValue::Scalar(scalar) => utf8_literal(Some(scalar)),
        ColumnarValue::Array(array) => {
            plan_err!("{UPDATE_FIELDS} edit tags and paths must be Utf8 literals, got {array:?}")
        }
    }
}

fn scalar_at<'a>(scalars: &'a [Option<&'a ScalarValue>], index: usize) -> Result<&'a str> {
    utf8_literal(scalars.get(index).copied().flatten())
}

fn decode_path(text: &str) -> Vec<String> {
    text.split('.').map(str::to_string).collect()
}

fn decode_plan_edits(args: &ReturnFieldArgs) -> Result<Vec<PlanEdit>> {
    let mut edits = Vec::new();
    let mut index = 1;
    while index < args.scalar_arguments.len() {
        let op = scalar_at(args.scalar_arguments, index)?;
        let path = scalar_at(args.scalar_arguments, index + 1)?;
        match op {
            OP_WITH => {
                let value_field = args.arg_fields.get(index + 2).cloned().ok_or_else(|| {
                    exec_datafusion_err!("{UPDATE_FIELDS} '{OP_WITH}' edit needs a value argument")
                })?;
                let value_repr = match args.scalar_arguments.get(index + 2).copied().flatten() {
                    Some(scalar) => scalar.to_string(),
                    None => value_field.name().clone(),
                };
                edits.push(PlanEdit {
                    drop: false,
                    path: decode_path(path),
                    value: Some(value_field),
                    value_repr,
                });
                index += 3;
            }
            OP_DROP => {
                edits.push(PlanEdit {
                    drop: true,
                    path: decode_path(path),
                    value: None,
                    value_repr: String::new(),
                });
                index += 2;
            }
            other => {
                return plan_err!(
                    "{UPDATE_FIELDS} edit tag must be '{OP_WITH}' or '{OP_DROP}', got '{other}'"
                );
            }
        }
    }
    Ok(edits)
}

fn decode_exec_edits(args: &ScalarFunctionArgs) -> Result<Vec<ExecEdit>> {
    let mut edits = Vec::new();
    let mut index = 1;
    while index < args.args.len() {
        let op = utf8_argument(&args.args[index])?;
        let path = utf8_argument(&args.args[index + 1])?;
        match op {
            OP_WITH => {
                let value_field = args.arg_fields.get(index + 2).cloned().ok_or_else(|| {
                    exec_datafusion_err!("{UPDATE_FIELDS} '{OP_WITH}' edit needs a value argument")
                })?;
                let value_array = args
                    .args
                    .get(index + 2)
                    .ok_or_else(|| {
                        exec_datafusion_err!(
                            "{UPDATE_FIELDS} '{OP_WITH}' edit needs a value argument"
                        )
                    })?
                    .to_array(args.number_rows)?;
                edits.push(ExecEdit {
                    drop: false,
                    path: decode_path(path),
                    value: Some((value_field, value_array)),
                });
                index += 3;
            }
            OP_DROP => {
                edits.push(ExecEdit {
                    drop: true,
                    path: decode_path(path),
                    value: None,
                });
                index += 2;
            }
            other => {
                return plan_err!(
                    "{UPDATE_FIELDS} edit tag must be '{OP_WITH}' or '{OP_DROP}', got '{other}'"
                );
            }
        }
    }
    Ok(edits)
}

fn call_text(source: &str, edits: &[PlanEdit]) -> String {
    let mut text = format!("{UPDATE_FIELDS}({source}");
    for edit in edits {
        if edit.drop {
            text.push_str(", dropfield()");
        } else {
            let _ = std::fmt::Write::write_fmt(
                &mut text,
                format_args!(", WithField({})", edit.value_repr),
            );
        }
    }
    text.push(')');
    text
}

fn case_insensitive_eq(left: &str, right: &str) -> bool {
    left.to_lowercase() == right.to_lowercase()
}

fn sibling_names<'a>(names: impl Iterator<Item = &'a str>) -> String {
    names
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn mask_with_parent_nulls(child: ArrayRef, parent_nulls: Option<&NullBuffer>) -> Result<ArrayRef> {
    let Some(valid) = parent_nulls else {
        return Ok(child);
    };
    if valid.null_count() == 0 {
        return Ok(child);
    }
    let data = child.to_data();
    if data.data_type() == &DataType::Null {
        return Ok(child);
    }
    let combined = match data.nulls() {
        Some(existing) => NullBuffer::new(existing.inner() & valid.inner()),
        None => valid.clone(),
    };
    Ok(make_array(
        data.into_builder().nulls(Some(combined)).build()?,
    ))
}

fn edit_field_name(name: &str, index: usize) -> String {
    if name.is_empty() {
        format!("col{index}")
    } else {
        name.to_string()
    }
}

fn cannot_drop_all_fields(call: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS] Cannot resolve \"{call}\" due to data type \
         mismatch: Cannot drop all fields in struct. SQLSTATE: 42K09"
    ))
}

pub(crate) fn spark_sql_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(..) => "TIMESTAMP".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Null => "VOID".to_string(),
        other => other.to_string().to_uppercase(),
    }
}

fn apply_type_edits(out: &mut Vec<FieldRef>, edits: &[PlanEdit], call: &str) -> Result<()> {
    for edit in edits {
        apply_type_edit(out, edit, call)?;
    }
    Ok(())
}

fn apply_type_edit(out: &mut Vec<FieldRef>, edit: &PlanEdit, call: &str) -> Result<()> {
    let head = edit
        .path
        .first()
        .ok_or_else(|| exec_datafusion_err!("{UPDATE_FIELDS} edit path must not be empty"))?;
    if edit.path.len() > 1 {
        let sub = PlanEdit {
            drop: edit.drop,
            path: edit.path[1..].to_vec(),
            value: edit.value.clone(),
            value_repr: edit.value_repr.clone(),
        };
        let mut matched = false;
        for field in out.iter_mut() {
            if !case_insensitive_eq(field.name(), head) {
                continue;
            }
            matched = true;
            let DataType::Struct(children) = field.data_type() else {
                return plan_err!(
                    "[FIELD_NOT_FOUND] No such struct field `{}` in {}. SQLSTATE: 42704",
                    edit.path[1],
                    sibling_names(out.iter().map(|item| item.name().as_str()))
                );
            };
            let mut child_vec: Vec<FieldRef> = children.iter().cloned().collect();
            apply_type_edits(&mut child_vec, std::slice::from_ref(&sub), call)?;
            if child_vec.is_empty() {
                return Err(cannot_drop_all_fields(call));
            }
            *field = Arc::new(Field::new(
                field.name().clone(),
                DataType::Struct(Fields::from(child_vec)),
                field.is_nullable(),
            ));
        }
        if !matched && !edit.drop {
            return plan_err!(
                "[FIELD_NOT_FOUND] No such struct field `{head}` in {}. SQLSTATE: 42704",
                sibling_names(out.iter().map(|item| item.name().as_str()))
            );
        }
        return Ok(());
    }
    if edit.drop {
        out.retain(|field| !case_insensitive_eq(field.name(), head));
        return Ok(());
    }
    let Some(value) = &edit.value else {
        return plan_err!("{UPDATE_FIELDS} '{OP_WITH}' edit needs a value argument");
    };
    let mut matched = false;
    for (index, field) in out.iter_mut().enumerate() {
        if case_insensitive_eq(field.name(), head) {
            *field = Arc::new(Field::new(
                edit_field_name(head, index),
                value.data_type().clone(),
                value.is_nullable(),
            ));
            matched = true;
        }
    }
    if !matched {
        out.push(Arc::new(Field::new(
            edit_field_name(head, out.len()),
            value.data_type().clone(),
            value.is_nullable(),
        )));
    }
    Ok(())
}

fn apply_exec_edits(
    out: &mut Vec<(FieldRef, ArrayRef)>,
    edits: &[ExecEdit],
    call: &str,
) -> Result<()> {
    for edit in edits {
        apply_exec_edit(out, edit, call)?;
    }
    Ok(())
}

fn apply_exec_edit(out: &mut Vec<(FieldRef, ArrayRef)>, edit: &ExecEdit, call: &str) -> Result<()> {
    let head = edit
        .path
        .first()
        .ok_or_else(|| exec_datafusion_err!("{UPDATE_FIELDS} edit path must not be empty"))?;
    if edit.path.len() > 1 {
        let sub = ExecEdit {
            drop: edit.drop,
            path: edit.path[1..].to_vec(),
            value: edit.value.clone(),
        };
        let mut matched = false;
        for (field, array) in out.iter_mut() {
            if !case_insensitive_eq(field.name(), head) {
                continue;
            }
            matched = true;
            let Some(child) = array.as_struct_opt() else {
                return exec_err!(
                    "[FIELD_NOT_FOUND] No such struct field `{}` in {}. SQLSTATE: 42704",
                    edit.path[1],
                    sibling_names(out.iter().map(|(item, _)| item.name().as_str()))
                );
            };
            let mut child_pairs: Vec<(FieldRef, ArrayRef)> = child
                .fields()
                .iter()
                .cloned()
                .zip(child.columns().iter().cloned())
                .collect();
            apply_exec_edits(&mut child_pairs, std::slice::from_ref(&sub), call)?;
            if child_pairs.is_empty() {
                return Err(cannot_drop_all_fields(call));
            }
            let child_nulls = child.nulls().cloned();
            let child_fields: Vec<FieldRef> =
                child_pairs.iter().map(|(item, _)| item.clone()).collect();
            let mut child_columns: Vec<ArrayRef> = Vec::with_capacity(child_pairs.len());
            for (_, array) in &child_pairs {
                child_columns.push(mask_with_parent_nulls(array.clone(), child_nulls.as_ref())?);
            }
            *array = Arc::new(StructArray::try_new(
                Fields::from(child_fields.clone()),
                child_columns,
                child.nulls().cloned(),
            )?);
            *field = Arc::new(Field::new(
                field.name().clone(),
                DataType::Struct(Fields::from(child_fields)),
                field.is_nullable(),
            ));
        }
        if !matched && !edit.drop {
            return exec_err!(
                "[FIELD_NOT_FOUND] No such struct field `{head}` in {}. SQLSTATE: 42704",
                sibling_names(out.iter().map(|(item, _)| item.name().as_str()))
            );
        }
        return Ok(());
    }
    if edit.drop {
        out.retain(|(field, _)| !case_insensitive_eq(field.name(), head));
        return Ok(());
    }
    let Some((value_field, value_array)) = &edit.value else {
        return exec_err!("{UPDATE_FIELDS} '{OP_WITH}' edit needs a value argument");
    };
    let mut matched = false;
    for (index, (field, array)) in out.iter_mut().enumerate() {
        if case_insensitive_eq(field.name(), head) {
            *field = Arc::new(Field::new(
                edit_field_name(head, index),
                value_field.data_type().clone(),
                value_field.is_nullable(),
            ));
            *array = value_array.clone();
            matched = true;
        }
    }
    if !matched {
        out.push((
            Arc::new(Field::new(
                edit_field_name(head, out.len()),
                value_field.data_type().clone(),
                value_field.is_nullable(),
            )),
            value_array.clone(),
        ));
    }
    Ok(())
}

impl ScalarUDFImpl for UpdateFields {
    fn name(&self) -> &str {
        UPDATE_FIELDS
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        exec_err!("{UPDATE_FIELDS} return_type is not used; return_field_from_args is")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(first) = args.arg_fields.first() else {
            return plan_err!("{UPDATE_FIELDS} takes a struct argument first");
        };
        let edits = decode_plan_edits(&args)?;
        let call = call_text(first.name(), &edits);
        let DataType::Struct(fields) = first.data_type() else {
            return plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{call}\" due to data \
                 type mismatch: The first parameter requires the \"STRUCT\" type, however \"{}\" \
                 has the type \"{}\". SQLSTATE: 42K09",
                first.name(),
                spark_sql_type(first.data_type())
            );
        };
        let mut out: Vec<FieldRef> = fields.iter().cloned().collect();
        apply_type_edits(&mut out, &edits, &call)?;
        if out.is_empty() {
            return Err(cannot_drop_all_fields(&call));
        }
        Ok(Arc::new(Field::new(
            first.name(),
            DataType::Struct(Fields::from(out)),
            first.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return exec_err!("{UPDATE_FIELDS} takes a struct argument first");
        };
        let array = first.to_array(args.number_rows)?;
        let Some(struct_array) = array.as_struct_opt() else {
            return exec_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] The first parameter requires the \
                 \"STRUCT\" type"
            );
        };
        let edits = decode_exec_edits(&args)?;
        let mut out: Vec<(FieldRef, ArrayRef)> = struct_array
            .fields()
            .iter()
            .cloned()
            .zip(struct_array.columns().iter().cloned())
            .collect();
        apply_exec_edits(&mut out, &edits, UPDATE_FIELDS)?;
        if out.is_empty() {
            return Err(cannot_drop_all_fields(UPDATE_FIELDS));
        }
        let parent_nulls = struct_array.nulls().cloned();
        let fields: Vec<FieldRef> = out.iter().map(|(field, _)| field.clone()).collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(out.len());
        for (_, array) in &out {
            columns.push(mask_with_parent_nulls(
                array.clone(),
                parent_nulls.as_ref(),
            )?);
        }
        let result =
            StructArray::try_new(Fields::from(fields), columns, struct_array.nulls().cloned())?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
