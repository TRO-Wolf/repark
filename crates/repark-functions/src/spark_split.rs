use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, ListArray, StringArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

fn split_arg_token(expr: &Expr) -> String {
    match expr {
        Expr::Cast(cast) => split_arg_token(&cast.expr),
        _ => crate::expr_fn::spark_expr_token(expr),
    }
}
use regex::Regex;

#[must_use]
pub fn split_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSplit::new()))
}

#[derive(Debug)]
struct SparkSplit {
    signature: Signature,
}

impl SparkSplit {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSplit {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSplit {}

impl Hash for SparkSplit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_stringy(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

fn unexpected_input_type(argument: &str, expected: &str, got: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"split(<expr>, ...)\" due \
         to data type mismatch: The {argument} parameter requires the {expected} type, however \
         the argument has the type \"{}\".",
        crate::collection::spark_type_name(got)
    ))
}

fn plan_split(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 2 && arg_types.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "'split' expects (str, pattern[, limit]), got {} argument(s)",
            arg_types.len()
        )));
    }
    if !is_stringy(&arg_types[0]) {
        return Err(unexpected_input_type("first", "\"STRING\"", &arg_types[0]));
    }
    if !matches!(
        arg_types[1],
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
    ) {
        return Err(unexpected_input_type("second", "\"STRING\"", &arg_types[1]));
    }
    if arg_types.len() == 3
        && !matches!(
            arg_types[2],
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Null
        )
    {
        return Err(unexpected_input_type("third", "\"INT\"", &arg_types[2]));
    }
    Ok(())
}

fn split_return() -> DataType {
    DataType::List(Arc::new(Field::new("element", DataType::Utf8, false)))
}

impl ScalarUDFImpl for SparkSplit {
    crate::shim_udf_boilerplate!("split");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        plan_split(arg_types)?;
        Ok(split_return())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        plan_split(&declared)?;
        let folded = args
            .scalar_arguments
            .iter()
            .all(|scalar| matches!(scalar, Some(value) if !value.is_null()));
        Ok(Arc::new(Field::new("split", split_return(), !folded)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 2 && args.len() != 3 {
            return exec_err!(
                "'split' expects (str, pattern[, limit]), got {} argument(s)",
                args.len()
            );
        }
        let mut parts: Vec<String> = args.iter().map(split_arg_token).collect();
        if parts.len() == 2 {
            parts.push("-1".to_owned());
        }
        Ok(format!("split({})", parts.join(", ")))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_split(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            return_field,
            ..
        } = args;
        if arg_values.len() != 2 && arg_values.len() != 3 {
            return exec_err!("'split' expects (str, pattern[, limit])");
        }
        let DataType::List(element) = return_field.data_type() else {
            return exec_err!("split needs a list return");
        };
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) = &arg_values[1] {
            return split_scalar_pattern(&arg_values, pattern, element);
        }
        let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
        let strings = shaped_utf8(&arrays[0])?;
        let patterns = shaped_utf8(&arrays[1])?;
        let limits = if arrays.len() > 2 {
            Some(shaped_int32(&arrays[2])?)
        } else {
            None
        };
        let row_count = strings.len();
        let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
        offsets.push(0);
        let mut validity: Vec<bool> = Vec::with_capacity(row_count);
        let mut any_null = false;
        let mut values = datafusion::arrow::array::StringBuilder::new();
        let mut value_count = 0usize;
        let mut compiled = PatternCache::default();
        for row in 0..row_count {
            let limit_null = limits.as_ref().is_some_and(|limits| limits.is_null(row));
            if strings.is_null(row) || patterns.is_null(row) || limit_null {
                validity.push(false);
                any_null = true;
                offsets.push(fit_i32(value_count)?);
                continue;
            }
            validity.push(true);
            let limit = limits.as_ref().map_or(-1, |limits| limits.value(row));
            let pattern = compiled.resolve(patterns.value(row))?;
            let pieces = split_row(strings.value(row), &pattern, limit)?;
            for piece in pieces {
                values.append_value(piece);
                value_count += 1;
            }
            offsets.push(fit_i32(value_count)?);
        }
        let nulls = if any_null {
            Some(NullBuffer::from(validity))
        } else {
            None
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            Arc::clone(element),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values.finish()),
            nulls,
        )?)))
    }
}

fn shaped_utf8(array: &ArrayRef) -> Result<StringArray> {
    if array.data_type() == &DataType::Utf8 {
        return array
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| DataFusionError::Execution("split needs utf8 values".to_owned()));
    }
    let shaped = cast(array.as_ref(), &DataType::Utf8)?;
    shaped
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("split needs utf8 values".to_owned()))
}

fn shaped_int32(array: &ArrayRef) -> Result<Int32Array> {
    if array.data_type() == &DataType::Int32 {
        return array
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .ok_or_else(|| DataFusionError::Execution("split needs int32 limits".to_owned()));
    }
    let shaped = cast(array.as_ref(), &DataType::Int32)?;
    shaped
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("split needs int32 limits".to_owned()))
}

fn fit_i32(value: usize) -> Result<i32> {
    i32::try_from(value)
        .map_err(|_| DataFusionError::Execution("split row count does not fit i32".to_owned()))
}

#[derive(Clone)]
enum SplitPattern {
    Literal(String),
    Regex(Regex),
}

fn is_plain_literal(translated: &str) -> bool {
    !translated.chars().any(|character| {
        matches!(
            character,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        )
    })
}

fn resolve_pattern(translated: &str) -> Result<SplitPattern> {
    if is_plain_literal(translated) {
        return Ok(SplitPattern::Literal(translated.to_owned()));
    }
    let regex = Regex::new(translated).map_err(|error| {
        DataFusionError::Execution(format!(
            "invalid regular expression '{translated}': {error}"
        ))
    })?;
    Ok(SplitPattern::Regex(regex))
}

#[derive(Default)]
struct PatternCache {
    entries: HashMap<String, SplitPattern>,
    order: std::collections::VecDeque<String>,
}

impl PatternCache {
    fn resolve(&mut self, pattern: &str) -> Result<SplitPattern> {
        if let Some(hit) = self.entries.get(pattern) {
            if let Some(position) = self.order.iter().position(|key| key == pattern) {
                self.order.remove(position);
                self.order.push_back(pattern.to_owned());
            }
            return Ok(hit.clone());
        }
        let translated = crate::spark_regexp::translate_java_pattern(pattern)?;
        let resolved = resolve_pattern(&translated)?;
        if self.entries.len() >= 64
            && let Some(oldest) = self.order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.order.push_back(pattern.to_owned());
        self.entries.insert(pattern.to_owned(), resolved.clone());
        Ok(resolved)
    }
}

fn split_scalar_pattern(
    arg_values: &[ColumnarValue],
    pattern: &str,
    element: &FieldRef,
) -> Result<ColumnarValue> {
    let translated = crate::spark_regexp::translate_java_pattern(pattern)?;
    let resolved = resolve_pattern(&translated)?;
    let mut shape_args = Vec::with_capacity(arg_values.len() - 1);
    shape_args.push(arg_values[0].clone());
    if arg_values.len() > 2 {
        shape_args.push(arg_values[2].clone());
    }
    let arrays = ColumnarValue::values_to_arrays(&shape_args)?;
    let strings = shaped_utf8(&arrays[0])?;
    let limits = if arrays.len() > 1 {
        Some(shaped_int32(&arrays[1])?)
    } else {
        None
    };
    let row_count = strings.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let mut values = datafusion::arrow::array::StringBuilder::new();
    let mut value_count = 0usize;
    for row in 0..row_count {
        let limit_null = limits.as_ref().is_some_and(|limits| limits.is_null(row));
        if strings.is_null(row) || limit_null {
            validity.push(false);
            any_null = true;
            offsets.push(fit_i32(value_count)?);
            continue;
        }
        validity.push(true);
        let limit = limits.as_ref().map_or(-1, |limits| limits.value(row));
        let pieces = split_row(strings.value(row), &resolved, limit)?;
        for piece in pieces {
            values.append_value(piece);
            value_count += 1;
        }
        offsets.push(fit_i32(value_count)?);
    }
    let nulls = if any_null {
        Some(NullBuffer::from(validity))
    } else {
        None
    };
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::clone(element),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values.finish()),
        nulls,
    )?)))
}

fn split_row<'text>(
    text: &'text str,
    pattern: &SplitPattern,
    limit: i32,
) -> Result<Vec<&'text str>> {
    if text.is_empty() {
        return Ok(vec![text]);
    }
    if let SplitPattern::Literal(literal) = pattern {
        if literal.is_empty() {
            let bound = if limit > 0 {
                usize::try_from(limit).unwrap_or(usize::MAX)
            } else {
                usize::MAX
            };
            let mut pieces: Vec<&str> = Vec::new();
            let mut offset = 0;
            for piece in text.split("").filter(|piece| !piece.is_empty()) {
                if pieces.len() + 1 == bound {
                    pieces.push(&text[offset..]);
                    return Ok(pieces);
                }
                offset += piece.len();
                pieces.push(piece);
            }
            return Ok(pieces);
        }
        if limit > 0 {
            let bound = usize::try_from(limit).unwrap_or(usize::MAX);
            return Ok(text.splitn(bound, literal.as_str()).collect());
        }
        return Ok(text.split(literal.as_str()).collect());
    }
    let SplitPattern::Regex(regex) = pattern else {
        return Ok(vec![text]);
    };
    let max_matches = if limit > 0 {
        usize::try_from(limit - 1).unwrap_or(usize::MAX)
    } else {
        usize::MAX
    };
    let found = crate::spark_regexp::collect_matches_up_to(text, regex, max_matches)?;
    let usable = found.len();
    let mut pieces = Vec::with_capacity(usable + 1);
    let mut cursor = 0;
    for (start, end) in found.iter().take(usable) {
        pieces.push(&text[cursor..*start]);
        cursor = *end;
    }
    pieces.push(&text[cursor..]);
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn pieces_of(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let lists = batches[0].column(0).as_list::<i32>();
        assert_eq!(lists.len(), 1);
        let row = lists.value(0);
        let values = row
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string pieces");
        values
            .iter()
            .map(|piece| piece.unwrap_or("NULL").to_owned())
            .collect()
    }

    #[tokio::test]
    async fn split_regex_limit_shapes() {
        let ctx = ctx();
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a,b', ',')").await,
            vec!["a", "b"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a,b,,c', ',', 2)").await,
            vec!["a", "b,,c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a,b,,c', ',', 0)").await,
            vec!["a", "b", "", "c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a.b', '.')").await,
            vec!["", "", "", ""]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('abc', '')").await,
            vec!["a", "b", "c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a,b,,c', '', 2)").await,
            vec!["a", ",b,,c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a,b,,c', '', 1)").await,
            vec!["a,b,,c"]
        );
        assert_eq!(pieces_of(&ctx, "SELECT split('', ',')").await, vec![""]);
        assert_eq!(
            pieces_of(&ctx, "SELECT split('a1b22c', '[0-9]+')").await,
            vec!["a", "b", "c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split('aXbxc', '(?i)x')").await,
            vec!["a", "b", "c"]
        );
        assert_eq!(
            pieces_of(&ctx, "SELECT split(123, '2')").await,
            vec!["1", "3"]
        );
    }

    #[tokio::test]
    async fn split_result_type_has_empty_contains_and_plan_nullability() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT split('a,b', ',')")
            .await
            .expect("plan split")
            .collect()
            .await
            .expect("execute split");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Utf8, false)))
        );
        let batches = ctx
            .sql("SELECT split(NULL, ',')")
            .await
            .expect("plan null split")
            .collect()
            .await
            .expect("execute null split");
        assert!(batches[0].column(0).is_null(0));
    }
}
