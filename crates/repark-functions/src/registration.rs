use std::sync::Arc;

use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::prelude::SessionContext;

pub fn register_udf_families(ctx: &SessionContext) {
    crate::iceberg_system::register(ctx);
    crate::validate::register(ctx);
    crate::session_names::register(ctx);
    crate::try_invert::register(ctx);
    crate::temporal_ctor::register(ctx);
    crate::higher_order::register(ctx);
}

#[must_use]
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(crate::decimal_precision::SparkNegateNullDecimal),
        Arc::new(crate::spark_result_types::SparkIntegerLiteral),
        Arc::new(crate::lambda_rebind::LambdaRebind),
        Arc::new(crate::decimal_precision::SparkDecimalPrecision),
        Arc::new(crate::decimal_spark::SparkDecimalRewrite),
        Arc::new(crate::spark_nullability::SparkNullability),
        Arc::new(crate::integer_spark::SparkIntegerOverflow),
        Arc::new(crate::analyzer::SparkExprSemantics),
        Arc::new(crate::int_to_binary::IntToBinaryCast),
        Arc::new(crate::java_double::SparkFloatStringify),
    ];
    rules.extend(crate::cardinality::analyzer_rules());
    rules.push(crate::csv::fold::CsvFold::rule());
    rules.push(crate::time_family::time_cast_guard_rule());
    rules.push(crate::instant_ts::ltz_timestamp_cast_rule());
    rules.push(crate::temporal_ctor::interval_string_cast_rule());
    rules.push(Arc::new(TypeCoercion::new()));
    rules.push(Arc::new(crate::lambda_rebind::LambdaRebind));
    rules.push(Arc::new(crate::analyzer::time_window::SparkTimeWindow));
    rules.push(Arc::new(
        crate::analyzer::time_window::SparkWindowTimeGrouping,
    ));
    rules.push(Arc::new(crate::analyzer::time_window::SparkSessionWindow));
    rules.push(Arc::new(crate::generator::GeneratorRewrite));
    rules.push(grouping_rule());
    rules
}

#[must_use]
pub fn grouping_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(crate::grouping::ResolveGroupingId)
}
