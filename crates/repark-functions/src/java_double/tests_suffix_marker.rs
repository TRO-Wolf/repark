use std::hash::{Hash, Hasher};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

#[derive(Debug)]
struct StubSuffixLiteral {
    signature: Signature,
}

impl StubSuffixLiteral {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for StubSuffixLiteral {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for StubSuffixLiteral {}

impl Hash for StubSuffixLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for StubSuffixLiteral {
    crate::shim_udf_boilerplate!("__repark_suffix_literal__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan("'__repark_suffix_literal__' expects one argument".to_string())
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan("'__repark_suffix_literal__' expects one argument".to_string())
        })?;
        Ok(Field::new(self.name(), first.data_type().clone(), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.first().cloned().ok_or_else(|| {
            DataFusionError::Execution(
                "'__repark_suffix_literal__' expects one argument".to_string(),
            )
        })
    }
}

async fn analyzed_text(sql: &str) -> String {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(StubSuffixLiteral::new()));
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    let plan = ctx.state().create_logical_plan(sql).await.expect("plan");
    let analyzed = crate::analyze_eagerly(&ctx.state(), plan).expect("analyze");
    analyzed.display_indent().to_string()
}

#[tokio::test]
async fn suffix_literal_cast_is_left_for_the_numeric_fold() {
    let text =
        analyzed_text("SELECT CAST(__repark_suffix_literal__('1e200') AS DOUBLE) AS v").await;
    assert!(
        text.contains("CAST(__repark_suffix_literal__(Utf8(\"1e200\")) AS Float64)"),
        "{text}"
    );
    assert!(!text.contains("__repark_parse_java_double__"), "{text}");
}

#[tokio::test]
async fn non_literal_string_cast_still_routes_to_the_parse_kernel() {
    let text = analyzed_text("SELECT CAST(repeat('1d', 1) AS DOUBLE) AS v").await;
    assert!(text.contains("__repark_parse_java_double__"), "{text}");
}
