use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, ScalarFunction};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::from_csv::FROM_CSV_UDF;
use super::schema_of_csv::{SCHEMA_OF_CSV_UDF, infer_csv_schema};
use super::{options_from_entries, options_from_map, spark_type_name, string_map_entries};

#[derive(Debug)]
pub(crate) struct CsvFold;

impl CsvFold {
    pub(crate) fn rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
        Arc::new(Self)
    }
}

fn literal_text(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    match scalar {
        datafusion::common::ScalarValue::Utf8(value)
        | datafusion::common::ScalarValue::LargeUtf8(value) => value.clone(),
        datafusion::common::ScalarValue::Utf8View(Some(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn csv_display(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => column.name.clone(),
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => text.clone(),
        Expr::Literal(ScalarValue::Null, _) => "NULL".to_string(),
        _ => expr.schema_name().to_string(),
    }
}

fn unexpected_null() -> DataFusionError {
    DataFusionError::Plan(
        "[DATATYPE_MISMATCH.UNEXPECTED_NULL] Cannot resolve \"schema_of_csv\" due to data type \
         mismatch: The csv must not be null. SQLSTATE: 42K09"
            .to_string(),
    )
}

fn unexpected_input_type(data_type: datafusion::arrow::datatypes::DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"schema_of_csv\" due to data \
         type mismatch: The first parameter requires the \"STRING\" type, however the input has \
         the type \"{}\". SQLSTATE: 42K09",
        spark_type_name(&data_type)
    ))
}

fn non_foldable_input(arg: &Expr) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve \"schema_of_csv({})\" due to data \
         type mismatch: the input `csv` should be a foldable \"STRING\" expression; however, got \
         \"{}\". SQLSTATE: 42K09",
        csv_display(arg),
        csv_display(arg)
    ))
}

fn non_map_function() -> DataFusionError {
    DataFusionError::Plan(
        "[INVALID_OPTIONS.NON_MAP_FUNCTION] Must use the `map()` function for options.".to_string(),
    )
}

fn fold_literal_text(scalar: &ScalarValue) -> Result<String> {
    match scalar {
        ScalarValue::Utf8(text) | ScalarValue::LargeUtf8(text) => match text {
            Some(value) => Ok(value.clone()),
            None => Err(unexpected_null()),
        },
        ScalarValue::Utf8View(Some(value)) => Ok(value.to_string()),
        ScalarValue::Utf8View(None) | ScalarValue::Null => Err(unexpected_null()),
        _ => Err(unexpected_input_type(scalar.data_type())),
    }
}

fn fold_cast_text(cast: &Cast) -> Result<String> {
    let Expr::Literal(scalar, _) = cast.expr.as_ref() else {
        return Err(non_foldable_input(&cast.expr));
    };
    if matches!(
        scalar,
        ScalarValue::Null | ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)
    ) {
        return Err(unexpected_null());
    }
    if !matches!(
        cast.field.data_type(),
        datafusion::arrow::datatypes::DataType::Utf8
            | datafusion::arrow::datatypes::DataType::LargeUtf8
            | datafusion::arrow::datatypes::DataType::Utf8View
    ) {
        return Err(unexpected_input_type(scalar.data_type()));
    }
    match scalar {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Ok(value.clone()),
        ScalarValue::Utf8View(Some(value)) => Ok(value.to_string()),
        _ => Ok(scalar.to_string()),
    }
}

fn peel_nonnull(arg: &Expr) -> &Expr {
    match arg {
        Expr::ScalarFunction(function)
            if function.func.name() == crate::decimal_cast::SPARK_NONNULL_NAME
                && function.args.len() == 1 =>
        {
            &function.args[0]
        }
        _ => arg,
    }
}

fn fold_schema_text(arg: &Expr) -> Result<String> {
    match peel_nonnull(arg) {
        Expr::Literal(scalar, _) => fold_literal_text(scalar),
        Expr::Cast(cast) => fold_cast_text(cast),
        _ => Err(non_foldable_input(arg)),
    }
}

fn fold_schema_options(arg: &Expr) -> Result<Option<super::CsvOptions>> {
    if let Some(raw) = options_pairs(arg) {
        return options_from_entries(&raw).map(Some);
    }
    match peel_nonnull(arg) {
        Expr::Literal(scalar @ ScalarValue::Map(_), _) => options_from_map(scalar).map(Some),
        Expr::Literal(_, _) => Err(non_map_function()),
        _ => Ok(None),
    }
}

fn fold_schema_of_csv(function: &ScalarFunction) -> Result<Option<Expr>> {
    if function.args.len() < 1 || function.args.len() > 2 {
        return Ok(None);
    }
    let text = fold_schema_text(&function.args[0])?;
    let options = if function.args.len() == 2 {
        match fold_schema_options(&function.args[1])? {
            Some(options) => options,
            None => return Ok(None),
        }
    } else {
        super::CsvOptions::default()
    };
    let schema = infer_csv_schema(&text, &options)?;
    Ok(Some(
        Expr::Literal(ScalarValue::Utf8(Some(schema)), None)
            .alias(format!("schema_of_csv({text})")),
    ))
}

fn check_from_csv_options(arg: &Expr) -> Result<()> {
    if let Some(raw) = options_pairs(arg) {
        options_from_entries(&raw)?;
        return Ok(());
    }
    match peel_nonnull(arg) {
        Expr::Literal(scalar @ ScalarValue::Map(_), _) => options_from_map(scalar).map(|_| ()),
        Expr::Literal(_, _) => Err(non_map_function()),
        _ => Ok(()),
    }
}

fn fold_from_csv(function: &ScalarFunction) -> Result<Option<Expr>> {
    let schema = match &function.args[1] {
        Expr::Literal(_, _) => None,
        Expr::Alias(alias) if matches!(alias.expr.as_ref(), Expr::Literal(_, _)) => {
            Some(alias.expr.as_ref().clone())
        }
        _ => return Err(super::non_string_literal_schema()),
    };
    if function.args.len() == 3 {
        check_from_csv_options(&function.args[2])?;
    }
    match schema {
        Some(literal) => {
            let mut args = function.args.clone();
            args[1] = literal;
            Ok(Some(Expr::ScalarFunction(ScalarFunction {
                func: Arc::clone(&function.func),
                args,
            })))
        }
        None => Ok(None),
    }
}

fn fold_call(function: &ScalarFunction) -> Result<Option<Expr>> {
    if function.func.name() == SCHEMA_OF_CSV_UDF {
        return fold_schema_of_csv(function);
    }
    if function.func.name() == FROM_CSV_UDF && (2..=3).contains(&function.args.len()) {
        return fold_from_csv(function);
    }
    Ok(None)
}

fn fold_csv_call(expr: Expr) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(function) => match fold_call(&function)? {
            Some(folded) => Ok(Transformed::yes(folded)),
            None => Ok(Transformed::no(Expr::ScalarFunction(function))),
        },
        Expr::Alias(alias) => {
            if let Expr::Alias(inner) = alias.expr.as_ref() {
                if matches!(inner.expr.as_ref(), Expr::Literal(_, _))
                    && inner.name.starts_with("schema_of_csv(")
                    && alias.name.starts_with("schema_of_csv(")
                {
                    return Ok(Transformed::yes(Expr::Alias(inner.clone())));
                }
            }
            let Expr::ScalarFunction(function) = alias.expr.as_ref() else {
                return Ok(Transformed::no(Expr::Alias(alias)));
            };
            match fold_call(function)? {
                Some(folded) if function.func.name() == SCHEMA_OF_CSV_UDF => {
                    Ok(Transformed::yes(folded))
                }
                Some(folded) => Ok(Transformed::yes(folded.alias(alias.name.clone()))),
                None => Ok(Transformed::no(Expr::Alias(alias))),
            }
        }
        _ => Ok(Transformed::no(expr)),
    }
}

fn options_pairs(expr: &Expr) -> Option<HashMap<String, String>> {
    match peel_nonnull(expr) {
        Expr::Literal(datafusion::common::ScalarValue::Map(map), _) => {
            if map.is_empty() {
                return Some(HashMap::new());
            }
            let entries = map.value(0);
            string_map_entries(entries.column(0), entries.column(1)).ok()
        }
        Expr::ScalarFunction(function) if function.func.name() == "create_map" => {
            let mut args = function.args.iter();
            let mut raw = HashMap::new();
            loop {
                match (args.next(), args.next()) {
                    (Some(key), Some(value)) => {
                        let Expr::Literal(key, _) = key else {
                            return None;
                        };
                        let Expr::Literal(value, _) = value else {
                            return None;
                        };
                        raw.insert(literal_text(key)?, literal_text(value)?);
                    }
                    (None, None) => break,
                    _ => return None,
                }
            }
            Some(raw)
        }
        Expr::ScalarFunction(function) if function.func.name() == "map" => {
            let paired_arrays = function.args.len() == 2
                && function.args.iter().all(|arg| {
                    matches!(arg, Expr::ScalarFunction(inner) if inner.func.name() == "make_array")
                });
            if paired_arrays {
                let mut lists = Vec::with_capacity(2);
                for arg in &function.args {
                    let Expr::ScalarFunction(inner) = arg else {
                        return None;
                    };
                    let mut values = Vec::with_capacity(inner.args.len());
                    for item in &inner.args {
                        let Expr::Literal(scalar, _) = item else {
                            return None;
                        };
                        values.push(literal_text(scalar)?);
                    }
                    lists.push(values);
                }
                if lists[0].len() != lists[1].len() {
                    return None;
                }
                return Some(
                    lists[0]
                        .iter()
                        .cloned()
                        .zip(lists[1].iter().cloned())
                        .collect(),
                );
            }
            if function.args.len() < 2 || function.args.len() % 2 != 0 {
                return None;
            }
            let mut args = function.args.iter();
            let mut raw = HashMap::new();
            loop {
                match (args.next(), args.next()) {
                    (Some(key), Some(value)) => {
                        let Expr::Literal(key, _) = key else {
                            return None;
                        };
                        let Expr::Literal(value, _) = value else {
                            return None;
                        };
                        raw.insert(literal_text(key)?, literal_text(value)?);
                    }
                    (None, None) => break,
                    _ => return None,
                }
            }
            Some(raw)
        }
        _ => None,
    }
}

impl AnalyzerRule for CsvFold {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let transformed = plan.transform_up(|plan| {
            let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().cloned().collect();
            let mapped = plan.map_expressions(|expr| expr.transform_up(fold_csv_call))?;
            if !mapped.transformed {
                return Ok(Transformed::no(mapped.data));
            }
            if matches!(
                mapped.data,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            ) {
                let expressions = mapped.data.expressions();
                return mapped
                    .data
                    .with_new_exprs(expressions, inputs)
                    .map(Transformed::yes);
            }
            Ok(mapped)
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "csv_fold"
    }
}
