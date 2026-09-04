use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::lit;

const NULL_MASK_FIELD: &str = "repark_null_mask_field";

pub(super) fn null_mask_field(parent: Expr, nested_name: &str) -> Expr {
    ScalarUDF::from(NullMaskField::new()).call(vec![parent, lit(nested_name)])
}

pub(super) fn null_mask_extractable(parent_type: &DataType) -> bool {
    matches!(parent_type, DataType::Struct(_))
}

#[derive(Debug)]
struct NullMaskField {
    signature: Signature,
}

impl NullMaskField {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for NullMaskField {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NullMaskField {}

impl Hash for NullMaskField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        NULL_MASK_FIELD.hash(state);
    }
}

impl ScalarUDFImpl for NullMaskField {
    fn name(&self) -> &str {
        NULL_MASK_FIELD
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("{NULL_MASK_FIELD} types itself from return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nested_name = literal_field_name(args.scalar_arguments.get(1).copied().flatten())?;
        let Some(parent) = args.arg_fields.first() else {
            return exec_err!("{NULL_MASK_FIELD} was called with no struct argument");
        };
        let child = struct_child_field(parent.data_type(), nested_name)?;
        Ok(Arc::new(child.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [parent, nested_name] = args.args.as_slice() else {
            return exec_err!(
                "{NULL_MASK_FIELD} takes 2 arguments, got {}",
                args.args.len()
            );
        };
        let nested_name = match nested_name {
            ColumnarValue::Scalar(scalar) => literal_field_name(Some(scalar))?,
            ColumnarValue::Array(_) => return exec_err!("{NULL_MASK_FIELD} needs a literal name"),
        };
        let array = parent.to_array(args.number_rows)?;
        let Some(parent) = array.as_struct_opt() else {
            return exec_err!(
                "{NULL_MASK_FIELD} needs a struct argument, got {}",
                array.data_type()
            );
        };
        let Some(child) = parent.column_by_name(nested_name) else {
            return exec_err!("{NULL_MASK_FIELD} found no field {nested_name} on the struct");
        };
        match parent.nulls() {
            Some(nulls) if nulls.null_count() > 0 => {
                let parent_is_null = BooleanArray::new(!nulls.inner(), None);
                Ok(ColumnarValue::Array(nullif(
                    child.as_ref(),
                    &parent_is_null,
                )?))
            }
            _ => Ok(ColumnarValue::Array(Arc::clone(child))),
        }
    }
}

fn literal_field_name(scalar: Option<&ScalarValue>) -> Result<&str> {
    match scalar {
        Some(ScalarValue::Utf8(Some(name)) | ScalarValue::Utf8View(Some(name))) => Ok(name),
        other => exec_err!("{NULL_MASK_FIELD} needs a literal Utf8 field name, got {other:?}"),
    }
}

fn struct_child_field(parent_type: &DataType, nested_name: &str) -> Result<FieldRef> {
    let DataType::Struct(fields) = parent_type else {
        return exec_err!("{NULL_MASK_FIELD} needs a struct argument, got {parent_type}");
    };
    fields
        .iter()
        .find(|field| field.name() == nested_name)
        .cloned()
        .ok_or_else(|| {
            exec_datafusion_err!("{NULL_MASK_FIELD} found no field {nested_name} on {parent_type}")
        })
}
