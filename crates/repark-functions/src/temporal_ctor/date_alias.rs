use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
    expr::ScalarFunction,
    simplify::{ExprSimplifyResult, SimplifyContext},
    type_coercion::functions::fields_with_udf,
};
use datafusion_spark::function::datetime::date_add::SparkDateAdd;
use datafusion_spark::function::datetime::date_diff::SparkDateDiff;
use datafusion_spark::function::datetime::date_sub::SparkDateSub;

use super::adddiff::{timestampadd_udf, timestampdiff_udf};
use super::plan_error;

#[must_use]
pub fn dateadd_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateAddRoute::new()))
}

#[must_use]
pub fn date_add_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateOffset::add()))
}

#[must_use]
pub fn date_sub_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateOffset::sub()))
}

#[must_use]
pub fn datediff_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateDiffRoute::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        dateadd_udf(),
        datediff_udf(),
        date_add_udf(),
        date_sub_udf(),
    ]
}

fn routed(
    name: &str,
    two: &Arc<ScalarUDF>,
    three: &Arc<ScalarUDF>,
    len: usize,
) -> Result<Arc<ScalarUDF>> {
    match len {
        2 => Ok(Arc::clone(two)),
        3 => Ok(Arc::clone(three)),
        _ => Err(plan_error(format!(
            "'{name}' expects 2 or 3 arguments, got {len}"
        ))),
    }
}

fn coerce_pair(inner: &ScalarUDF, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let fields: Vec<FieldRef> = arg_types
        .iter()
        .map(|data_type| Arc::new(Field::new("arg", data_type.clone(), true)) as FieldRef)
        .collect();
    fields_with_udf(&fields, inner).map(|fields| {
        fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    })
}

fn coerce_routed(name: &str, two: &ScalarUDF, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    match arg_types.len() {
        2 => coerce_pair(two, arg_types),
        3 => Ok(arg_types.to_vec()),
        len => Err(plan_error(format!(
            "'{name}' expects 2 or 3 arguments, got {len}"
        ))),
    }
}

#[derive(Debug)]
struct DateAddRoute {
    signature: Signature,
    two: Arc<ScalarUDF>,
    three: Arc<ScalarUDF>,
}

impl DateAddRoute {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            two: date_add_udf(),
            three: timestampadd_udf(),
        }
    }
}

impl PartialEq for DateAddRoute {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DateAddRoute {}

impl Hash for DateAddRoute {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for DateAddRoute {
    crate::shim_udf_boilerplate!("dateadd");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.len() {
            2 => Ok(DataType::Date32),
            _ => self.three.return_type(arg_types),
        }
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<arrow::datatypes::FieldRef> {
        match args.arg_fields.len() {
            2 => self.two.return_field_from_args(args),
            _ => self.three.return_field_from_args(args),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_routed(self.name(), &self.two, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        routed(self.name(), &self.two, &self.three, args.args.len())?.invoke_with_args(args)
    }
}

#[derive(Debug)]
struct DateDiffRoute {
    signature: Signature,
    two: Arc<ScalarUDF>,
    three: Arc<ScalarUDF>,
}

impl DateDiffRoute {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            two: Arc::new(ScalarUDF::new_from_impl(SparkDateDiff::new())),
            three: timestampdiff_udf(),
        }
    }
}

impl PartialEq for DateDiffRoute {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DateDiffRoute {}

impl Hash for DateDiffRoute {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for DateDiffRoute {
    crate::shim_udf_boilerplate!("datediff");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.len() {
            2 => Ok(DataType::Int32),
            _ => self.three.return_type(arg_types),
        }
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<arrow::datatypes::FieldRef> {
        match args.arg_fields.len() {
            2 => self.two.return_field_from_args(args),
            _ => self.three.return_field_from_args(args),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_routed(self.name(), &self.two, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        routed(self.name(), &self.two, &self.three, args.args.len())?.invoke_with_args(args)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.len() == 2 {
            Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                ScalarFunction::new_udf(Arc::clone(&self.two), args),
            )))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }
}

#[derive(Debug)]
struct DateOffset {
    name: &'static str,
    signature: Signature,
    inner: Arc<ScalarUDF>,
}

impl DateOffset {
    fn add() -> Self {
        Self {
            name: "date_add",
            signature: Signature::user_defined(Volatility::Immutable),
            inner: Arc::new(ScalarUDF::new_from_impl(SparkDateAdd::new())),
        }
    }

    fn sub() -> Self {
        Self {
            name: "date_sub",
            signature: Signature::user_defined(Volatility::Immutable),
            inner: Arc::new(ScalarUDF::new_from_impl(SparkDateSub::new())),
        }
    }
}

impl PartialEq for DateOffset {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DateOffset {}

impl Hash for DateOffset {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for DateOffset {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [start, days] = arg_types else {
            return Err(plan_error(format!(
                "'{}' expects (start_date, num_days), got {} argument(s)",
                self.name(),
                arg_types.len()
            )));
        };
        if !matches!(start, DataType::Date32 | DataType::Null) {
            return Err(plan_error(format!(
                "'{}' cannot accept a start date of type {start}",
                self.name()
            )));
        }
        match days {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Null => Ok(vec![DataType::Date32, DataType::Int32]),
            other => Err(plan_error(format!(
                "'{}' num_days must be an integer, got {other}",
                self.name()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx_with(ansi: bool, zone: &str) -> SessionContext {
        let config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
        let config = crate::session_time_zone::with_session_time_zone(config, zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                out.push(
                    datafusion::arrow::util::display::array_value_to_string(column, row).unwrap(),
                );
            }
        }
        out
    }

    #[tokio::test]
    async fn dateadd_3arg_matches_timestampadd() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT dateadd('DAY', 1, TIMESTAMP'2024-03-10 01:30:00') AS v"
            )
            .await,
            vec!["2024-03-11T01:30:00Z".to_string()]
        );
    }

    #[tokio::test]
    async fn datediff_3arg_matches_timestampdiff() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT datediff('HOUR', TIMESTAMP'2024-03-10 01:30:00', TIMESTAMP'2024-03-11 01:00:00') AS v"
            )
            .await,
            vec!["23".to_string()]
        );
    }

    #[tokio::test]
    async fn dateadd_2arg_keeps_date_add() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT dateadd(DATE'2024-01-05', CAST(3 AS INT)) AS v"
            )
            .await,
            vec!["2024-01-08".to_string()]
        );
    }

    #[tokio::test]
    async fn datediff_2arg_keeps_date_diff() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(
                &ctx,
                "SELECT datediff(DATE'2024-01-05', DATE'2024-01-01') AS v"
            )
            .await,
            vec!["4".to_string()]
        );
    }

    #[tokio::test]
    async fn date_add_accepts_unsuffixed_literal_days() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT date_add(DATE'2024-01-05', 1) AS v").await,
            vec!["2024-01-06".to_string()]
        );
        assert_eq!(
            one(&ctx, "SELECT dateadd(DATE'2024-01-05', 1) AS v").await,
            vec!["2024-01-06".to_string()]
        );
    }

    #[tokio::test]
    async fn date_sub_accepts_unsuffixed_literal_days() {
        let ctx = ctx_with(true, "UTC");
        assert_eq!(
            one(&ctx, "SELECT date_sub(DATE'2024-01-05', 1) AS v").await,
            vec!["2024-01-04".to_string()]
        );
    }

    #[tokio::test]
    async fn date_add_refuses_text_days() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT date_add(DATE'2024-01-05', 'x') AS v")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("num_days must be an integer"), "{error}");
    }

    #[tokio::test]
    async fn dateadd_wrong_arity_fails_planning() {
        let ctx = ctx_with(true, "UTC");
        let error = ctx
            .sql("SELECT dateadd(DATE'2024-01-05') AS v")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("'dateadd' expects 2 or 3 arguments, got 1"),
            "{error}"
        );
    }
}
