use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::spark_math::unexpected_input_type;

const DEFAULT_TOKENS: [&str; 4] = ["X", "x", "n", "NULL"];

#[must_use]
pub fn mask_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkMask::new()))
}

#[must_use]
pub fn call_mask(args: Vec<Expr>) -> Expr {
    crate::expr_fn::call(crate::string::mask_udf(), args)
}

#[derive(Debug)]
struct SparkMask {
    signature: Signature,
}

impl SparkMask {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkMask {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkMask {}

impl Hash for SparkMask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_mask_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn mask_param_error(position: usize, got: &DataType) -> DataFusionError {
    let ordinal = ["first", "second", "third", "fourth", "fifth"]
        .get(position)
        .unwrap_or(&"last");
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"mask(<expr>)\" due to \
         data type mismatch: The {ordinal} parameter requires the \"STRING\" type, however \
         the argument has the type \"{}\". SQLSTATE: 42K09",
        crate::spark_math::spark_type_name(got)
    ))
}

fn plan_mask(arg_types: &[DataType]) -> Result<()> {
    if arg_types.is_empty() || arg_types.len() > 5 {
        return exec_err!(
            "'mask' expects (col[, upper[, lower[, digit[, other]]]]), got {} argument(s)",
            arg_types.len()
        );
    }
    if !is_mask_text(&arg_types[0]) {
        return Err(unexpected_input_type(
            "mask",
            "STRING",
            &arg_types[0],
            "first",
        ));
    }
    for (position, data_type) in arg_types.iter().enumerate().skip(1) {
        if !is_mask_text(data_type) {
            return Err(mask_param_error(position, data_type));
        }
    }
    Ok(())
}

fn is_digit_char(value: char) -> bool {
    value.is_ascii_digit() || (!value.is_ascii() && value.is_numeric())
}

impl ScalarUDFImpl for SparkMask {
    crate::shim_udf_boilerplate!("mask");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_mask(&declared)?;
        Ok(Arc::new(Field::new("mask", DataType::Utf8, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.is_empty() || args.len() > 5 {
            return exec_err!(
                "'mask' expects (col[, upper[, lower[, digit[, other]]]]), got {} argument(s)",
                args.len()
            );
        }
        let mut parts: Vec<String> = args.iter().map(crate::expr_fn::spark_expr_token).collect();
        while parts.len() < 5 {
            parts.push(DEFAULT_TOKENS[parts.len() - 1].to_owned());
        }
        Ok(format!("mask({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_mask(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            number_rows,
            ..
        } = args;
        if arg_values.is_empty() || arg_values.len() > 5 {
            return exec_err!(
                "'mask' expects (col[, upper[, lower[, digit[, other]]]]), got {} argument(s)",
                arg_values.len()
            );
        }
        let first = arg_values[0].to_array(number_rows)?;
        if first.data_type() == &DataType::Null {
            let mut out = StringBuilder::new();
            for _ in 0..number_rows {
                out.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        let values = shaped_mask_text(&first)?;
        let defaults: [Option<char>; 4] = [Some('X'), Some('x'), Some('n'), None];
        let mut texts: Vec<Option<StringArray>> = Vec::with_capacity(4);
        let mut null_marked = [false; 4];
        for (position, arg) in arg_values.iter().skip(1).enumerate() {
            let array = arg.to_array(number_rows)?;
            if array.data_type() == &DataType::Null {
                null_marked[position] = true;
                texts.push(None);
            } else {
                texts.push(shaped_mask_optional(&array)?);
            }
        }
        let mut out = StringBuilder::new();
        for row in 0..number_rows {
            if values.is_null(row) {
                out.append_null();
                continue;
            }
            let pick = |position: usize| -> Option<char> {
                if null_marked.get(position).copied().unwrap_or(true) {
                    return None;
                }
                if let Some(array) = texts.get(position).and_then(|cell| cell.as_ref()) {
                    if array.is_null(row) {
                        return None;
                    }
                    if let Some(first) = array.value(row).chars().next() {
                        return Some(first);
                    }
                    return None;
                }
                defaults.get(position).copied().flatten()
            };
            let mut masked = String::with_capacity(values.value(row).len());
            for char in values.value(row).chars() {
                let substitute = if char.is_uppercase() {
                    pick(0)
                } else if char.is_lowercase() {
                    pick(1)
                } else if is_digit_char(char) {
                    pick(2)
                } else {
                    pick(3)
                };
                masked.push(substitute.unwrap_or(char));
            }
            out.append_value(masked);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn shaped_mask_text(array: &ArrayRef) -> Result<StringArray> {
    shaped_mask_optional(array)?.ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("mask needs utf8 values".to_owned())
    })
}

fn shaped_mask_optional(array: &ArrayRef) -> Result<Option<StringArray>> {
    if array.data_type() == &DataType::Null {
        return Ok(None);
    }
    let shaped = cast(array.as_ref(), &DataType::Utf8)?;
    shaped
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution("mask needs utf8 values".to_owned())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn strings_of(ctx: &SessionContext, sql: &str) -> Vec<Option<String>> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let mut out = Vec::new();
        for batch in &batches {
            out.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string output")
                    .iter()
                    .map(|cell| cell.map(str::to_owned)),
            );
        }
        out
    }

    #[tokio::test]
    async fn mask_defaults_and_overrides() {
        let ctx = ctx();
        assert_eq!(
            strings_of(&ctx, "SELECT mask('abcd-EFG-123')").await,
            vec![Some("xxxx-XXX-nnn".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT mask('abcd-EFG-123', 'Y', 'y', 'd', '*')").await,
            vec![Some("yyyy*YYY*ddd".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT mask('abcd-EFG-123', 'Y')").await,
            vec![Some("xxxx-YYY-nnn".to_owned())]
        );
        assert_eq!(
            strings_of(&ctx, "SELECT mask('abcd-EFG-123', NULL, NULL, NULL, 'o')").await,
            vec![Some("abcdoEFGo123".to_owned())]
        );
        assert_eq!(strings_of(&ctx, "SELECT mask(NULL)").await, vec![None]);
    }
}
