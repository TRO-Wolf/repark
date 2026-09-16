use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn like_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkLike {
        signature: Signature::user_defined(Volatility::Immutable),
        name: "like",
        case_insensitive: false,
    }))
}

#[must_use]
pub fn ilike_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkLike {
        signature: Signature::user_defined(Volatility::Immutable),
        name: "ilike",
        case_insensitive: true,
    }))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![like_udf(), ilike_udf()]
}

#[derive(Debug)]
struct SparkLike {
    signature: Signature,
    name: &'static str,
    case_insensitive: bool,
}

impl PartialEq for SparkLike {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.case_insensitive == other.case_insensitive
    }
}

impl Eq for SparkLike {}

impl Hash for SparkLike {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.case_insensitive.hash(state);
    }
}

fn invalid_escape() -> DataFusionError {
    DataFusionError::Plan(
        "[INVALID_ESCAPE_CHAR] The escape character is not a single character.".to_string(),
    )
}

fn escape_from_scalar(scalar: &ScalarValue) -> Result<char> {
    let text = match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => v.clone(),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            return Err(invalid_escape());
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] The third parameter of `like` \
                 requires the \"STRING\" type, got {}",
                scalar.data_type()
            )));
        }
    };
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(character), None) => Ok(character),
        _ => Err(invalid_escape()),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Token {
    Any,
    Single,
    Char(char),
}

fn tokens(pattern: &str, escape: char) -> Result<Vec<Token>> {
    let mut out = Vec::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(character) = chars.next() {
        if character == escape {
            match chars.next() {
                Some(escaped) => out.push(Token::Char(escaped)),
                None => {
                    return Err(DataFusionError::Execution(format!(
                        "[INVALID_FORMAT.ESC_AT_THE_END] The format is invalid: '{pattern}'. \
                         The escape character is not allowed to end with. SQLSTATE: 42601"
                    )));
                }
            }
            continue;
        }
        match character {
            '%' => out.push(Token::Any),
            '_' => out.push(Token::Single),
            literal => out.push(Token::Char(literal)),
        }
    }
    Ok(out)
}

fn chars_equal(left: char, right: char, insensitive: bool) -> bool {
    if !insensitive {
        return left == right;
    }
    if left == right {
        return true;
    }
    left.to_lowercase().eq(right.to_lowercase())
}

fn like_match(text: &str, pattern: &[Token], insensitive: bool) -> bool {
    let text: Vec<char> = text.chars().collect();
    let (mut cursor, mut mark) = (0usize, 0usize);
    let (mut star, mut resume) = (usize::MAX, 0usize);
    while cursor < text.len() {
        if mark < pattern.len()
            && match pattern[mark] {
                Token::Single => true,
                Token::Char(character) => chars_equal(character, text[cursor], insensitive),
                Token::Any => false,
            }
        {
            cursor += 1;
            mark += 1;
        } else if mark < pattern.len() && pattern[mark] == Token::Any {
            star = mark;
            resume = cursor;
            mark += 1;
        } else if star != usize::MAX {
            mark = star + 1;
            resume += 1;
            cursor = resume;
        } else {
            return false;
        }
    }
    while mark < pattern.len() && pattern[mark] == Token::Any {
        mark += 1;
    }
    mark == pattern.len()
}

impl ScalarUDFImpl for SparkLike {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let _ = arg_types;
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        if let Some(Some(scalar)) = args.scalar_arguments.get(2) {
            escape_from_scalar(scalar)?;
        }
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name, DataType::Boolean, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{}` requires 2 or 3 parameters but \
                 the actual number is {}",
                self.name,
                arg_types.len()
            )));
        }
        arg_types
            .iter()
            .map(|data_type| {
                if data_type == &DataType::Null
                    || matches!(
                        data_type,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    )
                {
                    Ok(DataType::Utf8)
                } else {
                    Err(DataFusionError::Plan(format!(
                        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \
                         \"{}(<expr>)\" due to data type mismatch: parameter requires the \
                         \"STRING\" type, however the argument has the type \"{data_type}\".",
                        self.name
                    )))
                }
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let escape = match args.args.get(2) {
            None => '\\',
            Some(ColumnarValue::Scalar(scalar)) => escape_from_scalar(scalar)?,
            Some(_) => {
                return Err(DataFusionError::Plan(format!(
                    "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"{}(<expr>)\" due \
                     to data type mismatch: the input \"escape\" should be a foldable \"STRING\" \
                     expression that is a single character.",
                    self.name
                )));
            }
        };
        let arrays = ColumnarValue::values_to_arrays(&args.args[..2])?;
        let text = cast(&arrays[0], &DataType::Utf8View)?;
        let pattern = cast(&arrays[1], &DataType::Utf8View)?;
        let text = text.as_string_view();
        let pattern = pattern.as_string_view();
        let mut values: Vec<Option<bool>> = Vec::with_capacity(text.len());
        for row in 0..text.len() {
            if text.is_null(row) || pattern.is_null(row) {
                values.push(None);
                continue;
            }
            let tokens = tokens(pattern.value(row), escape)?;
            values.push(Some(like_match(
                text.value(row),
                &tokens,
                self.case_insensitive,
            )));
        }
        Ok(ColumnarValue::Array(
            Arc::new(BooleanArray::from(values)) as ArrayRef
        ))
    }
}
