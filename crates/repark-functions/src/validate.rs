//! Spark `validate_utf8` / `try_validate_utf8` / `assert_true` — the refuse-or-pass-through trio.
//!
//! All three share one shape: inspect a value, hand it back unchanged when it is acceptable, and
//! **fail loudly** or yield NULL when it is not. None of them computes anything.
//!
//! **Why the UTF-8 pair needs a kernel at all.** An Arrow `Utf8` array cannot hold invalid UTF-8
//! — Rust's `&str` forbids it — so on a string column these functions are tautologies. The case
//! that matters is `Binary`, where the bytes have not been judged yet, and that is how
//! `datafusion-spark`'s `is_valid_utf8` already behaves (`X'61FF62'` → `false`). These follow it:
//! they accept binary, judge the bytes, and return the decoded string.
//!
//! Spark's own strings are `UTF8String` byte arrays that *can* carry invalid sequences, so a
//! Spark program can hit these on a STRING column where repark cannot. That is a structural
//! difference in the value representation, not a behaviour choice, and it is recorded here rather
//! than papered over: on binary input the three agree with Spark; on string input repark's answer
//! is trivially "valid" because an invalid string cannot exist to be passed in.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// ===========================================================================================
/// The kernels this module registers.
/// ===========================================================================================
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        validate_utf8_udf(),
        try_validate_utf8_udf(),
        assert_true_udf(),
    ]
}

/// Install them on a session. Called by [`crate::register_all`]; kept here rather than as a loop
/// in the crate root, which sits against its `check_lib_rs` ceiling.
pub(crate) fn register(ctx: &datafusion::prelude::SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

/// Spark `validate_utf8(expr)` — the input when it is valid UTF-8, an error otherwise.
#[must_use]
pub fn validate_utf8_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkValidateUtf8 {
        signature: byte_signature(),
        on_invalid: OnInvalid::Raise,
    }))
}

/// Spark `try_validate_utf8(expr)` — the input when it is valid UTF-8, NULL otherwise.
#[must_use]
pub fn try_validate_utf8_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkValidateUtf8 {
        signature: byte_signature(),
        on_invalid: OnInvalid::Null,
    }))
}

/// Spark `assert_true(condition[, message])` — NULL when true, an error otherwise.
#[must_use]
pub fn assert_true_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkAssertTrue {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
    }))
}

fn byte_signature() -> Signature {
    Signature::new(TypeSignature::UserDefined, Volatility::Immutable)
}

/// What an invalid sequence does — the ONLY difference between the two UTF-8 kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OnInvalid {
    Raise,
    Null,
}

#[derive(Debug)]
struct SparkValidateUtf8 {
    signature: Signature,
    on_invalid: OnInvalid,
}

impl PartialEq for SparkValidateUtf8 {
    fn eq(&self, other: &Self) -> bool {
        self.on_invalid == other.on_invalid
    }
}

impl Eq for SparkValidateUtf8 {}

impl Hash for SparkValidateUtf8 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.on_invalid.hash(state);
    }
}

impl ScalarUDFImpl for SparkValidateUtf8 {
    fn name(&self) -> &str {
        match self.on_invalid {
            OnInvalid::Raise => "validate_utf8",
            OnInvalid::Null => "try_validate_utf8",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        // Always nullable: `try_validate_utf8` yields NULL on invalid input even when the
        // argument itself is non-nullable.
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let Some(first) = arg_types.first() else {
            return exec_err!("'{}' expects one argument", self.name());
        };
        match first {
            // Binary is the case that can actually fail; keep it as-is so the bytes reach us.
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(vec![DataType::Binary])
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(vec![DataType::Utf8])
            }
            other => exec_err!(
                "'{}' expects a string or binary argument, got {other}",
                self.name()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(input) = arrays.first() else {
            return exec_err!("'{}' expects one argument", self.name());
        };

        // A Utf8 input is valid by construction — Arrow could not have built it otherwise.
        if matches!(input.data_type(), DataType::Utf8) {
            return Ok(ColumnarValue::Array(Arc::clone(input)));
        }

        let bytes = input.as_binary::<i32>();
        let mut values: Vec<Option<String>> = Vec::with_capacity(bytes.len());
        for row in 0..bytes.len() {
            if bytes.is_null(row) {
                values.push(None);
                continue;
            }
            match std::str::from_utf8(bytes.value(row)) {
                Ok(text) => values.push(Some(text.to_owned())),
                Err(error) => match self.on_invalid {
                    OnInvalid::Null => values.push(None),
                    OnInvalid::Raise => {
                        return Err(DataFusionError::Execution(format!(
                            "[INVALID_UTF8_STRING] Invalid UTF-8 byte sequence at index {}: \
                             use try_validate_utf8 to get NULL instead",
                            error.valid_up_to()
                        )));
                    }
                },
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

#[derive(Debug)]
struct SparkAssertTrue {
    signature: Signature,
}

impl PartialEq for SparkAssertTrue {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkAssertTrue {}

impl Hash for SparkAssertTrue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkAssertTrue {
    fn name(&self) -> &'static str {
        "assert_true"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let Some(first) = arg_types.first() else {
            return exec_err!("'assert_true' expects a boolean condition");
        };
        if !matches!(first, DataType::Boolean | DataType::Null) {
            return exec_err!("'assert_true' expects a boolean condition, got {first}");
        }
        let mut coerced = vec![DataType::Boolean];
        if arg_types.len() > 1 {
            coerced.push(DataType::Utf8);
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(condition) = arrays.first() else {
            return exec_err!("'assert_true' expects a boolean condition");
        };
        let flags = condition.as_boolean();
        let messages = arrays.get(1).map(AsArray::as_string::<i32>);

        for row in 0..flags.len() {
            // Spark raises on NULL as well as on false: only `true` passes.
            if flags.is_null(row) || !flags.value(row) {
                let message = messages
                    .and_then(|array| (!array.is_null(row)).then(|| array.value(row).to_owned()))
                    .unwrap_or_else(|| {
                        "'assert_true' failed: the condition was not true".to_owned()
                    });
                return Err(DataFusionError::Execution(message));
            }
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}
