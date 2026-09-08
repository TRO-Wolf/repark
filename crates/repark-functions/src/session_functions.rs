use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

const SESSION_IDENTITY: &str = "repark";

#[must_use]
pub fn user_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionUser::named("user")))
}

#[must_use]
pub fn current_user_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionUser::named("current_user")))
}

#[must_use]
pub fn session_user_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionUser::named("session_user")))
}

#[must_use]
pub fn version_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SessionVersion::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        user_udf(),
        current_user_udf(),
        session_user_udf(),
        version_udf(),
    ]
}

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionUser {
    name: &'static str,
    signature: Signature,
}

impl SessionUser {
    fn named(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::nullary(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SessionUser {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Utf8)
        } else {
            exec_err!(
                "'{}' requires 0 arguments, got {}",
                self.name,
                arg_types.len()
            )
        }
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                SESSION_IDENTITY.to_string(),
            ))))
        } else {
            exec_err!(
                "'{}' requires 0 arguments, got {}",
                self.name,
                args.args.len()
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionVersion {
    signature: Signature,
}

impl SessionVersion {
    fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Immutable),
        }
    }

    fn distribution() -> String {
        format!("repark-{}", env!("CARGO_PKG_VERSION"))
    }
}

impl ScalarUDFImpl for SessionVersion {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Utf8)
        } else {
            exec_err!("'version' requires 0 arguments, got {}", arg_types.len())
        }
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                Self::distribution(),
            ))))
        } else {
            exec_err!("'version' requires 0 arguments, got {}", args.args.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::config::ConfigOptions;
    use datafusion::execution::FunctionRegistry;

    use super::*;

    fn invoke(udf: &ScalarUDF, args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: Vec::new(),
            number_rows: 1,
            return_field: Field::new("f", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn scalar_text(value: ColumnarValue) -> String {
        match value {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) => text,
            other => panic!("expected a Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn session_user_spellings_answer_the_facade_identity() {
        for udf in [user_udf(), current_user_udf(), session_user_udf()] {
            assert_eq!(scalar_text(invoke(&udf, Vec::new()).unwrap()), "repark");
            assert_eq!(udf.return_type(&[]).unwrap(), DataType::Utf8);
        }
        assert_eq!(user_udf().name(), "user");
        assert_eq!(current_user_udf().name(), "current_user");
        assert_eq!(session_user_udf().name(), "session_user");
    }

    #[test]
    fn version_answers_the_repark_distribution_string() {
        assert_eq!(
            scalar_text(invoke(&version_udf(), Vec::new()).unwrap()),
            format!("repark-{}", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(version_udf().return_type(&[]).unwrap(), DataType::Utf8);
    }

    #[test]
    fn session_functions_refuse_arguments_loud() {
        let one = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))];
        for udf in functions() {
            assert!(invoke(&udf, one.clone()).is_err());
            assert!(udf.return_type(&[DataType::Int32]).is_err());
        }
    }

    #[test]
    fn register_installs_all_four_names() {
        let ctx = SessionContext::new();
        register(&ctx);
        for name in ["user", "current_user", "session_user", "version"] {
            assert!(ctx.udf(name).is_ok(), "{name} resolves after register");
        }
    }
}
