use std::collections::HashSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ExprSchema;
use datafusion::common::config::ConfigOptions;
use datafusion::common::plan_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct LambdaRebind;

impl AnalyzerRule for LambdaRebind {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let packed = plan
            .map_expressions(|expr| {
                refuse_lambda_arity(&expr)?;
                pack_unreferenced_params(expr)
            })
            .data()?;
        let resolved = packed.resolve_lambda_variables().data()?;
        let schema = resolved.schema().clone();
        resolved
            .map_expressions(|expr| {
                check_aggregate_merge(&expr, schema.as_ref())?;
                Ok(Transformed::no(expr))
            })
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "lambda_rebind"
    }
}

fn refuse_lambda_arity(expr: &Expr) -> Result<()> {
    expr.apply(|node| match node {
        Expr::HigherOrderFunction(hof) => {
            check_lambda_params(hof)?;
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(())
}

fn check_lambda_params(hof: &HigherOrderFunction) -> Result<()> {
    let mut lambdas = hof.args.iter().filter_map(|arg| match arg {
        Expr::Lambda(lambda) => Some(lambda.params.len()),
        _ => None,
    });
    match hof.func.name() {
        "aggregate" => {
            if let Some(merge) = lambdas.next()
                && merge < 2
            {
                return arity_mismatch(merge, 2);
            }
        }
        "zip_with" | "transform_keys" | "transform_values" | "map_filter" => {
            if let Some(only) = lambdas.next()
                && lambdas.next().is_none()
                && only < 2
            {
                return arity_mismatch(only, 2);
            }
        }
        "map_zip_with" => {
            if let Some(only) = lambdas.next()
                && lambdas.next().is_none()
                && only < 3
            {
                return arity_mismatch(only, 3);
            }
        }
        _ => {}
    }
    Ok(())
}

fn arity_mismatch(user: usize, expected: usize) -> Result<()> {
    plan_err!(
        "[INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH] Invalid lambda function call. A \
         higher order function expects {user} arguments, but got {expected}."
    )
}

fn check_aggregate_merge(expr: &Expr, schema: &dyn ExprSchema) -> Result<()> {
    expr.apply(|node| match node {
        Expr::HigherOrderFunction(hof) if hof.func.name() == "aggregate" => {
            check_merge_types(hof, schema)?;
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(())
}

fn check_merge_types(hof: &HigherOrderFunction, schema: &dyn ExprSchema) -> Result<()> {
    let mut lambdas = hof.args.iter().filter_map(|arg| match arg {
        Expr::Lambda(lambda) => Some(lambda),
        _ => None,
    });
    let Some(merge) = lambdas.next() else {
        return Ok(());
    };
    let Some(initial) = hof
        .args
        .get(1)
        .filter(|arg| !matches!(arg, Expr::Lambda(_)))
    else {
        return Ok(());
    };
    let Ok((_, initial_field)) = initial.to_field(schema) else {
        return Ok(());
    };
    let Ok((_, merge_field)) = merge.body.to_field(schema) else {
        return Ok(());
    };
    if initial_field.data_type() == merge_field.data_type() {
        return Ok(());
    }
    plan_err!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"aggregate\" due to data type \
         mismatch: The third parameter requires the \"{}\" type, however the merge lambda has \
         the type \"{}\".",
        spark_type_name(initial_field.data_type()),
        spark_type_name(merge_field.data_type())
    )
}

fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => String::from("VOID"),
        DataType::Boolean => String::from("BOOLEAN"),
        DataType::Int8 => String::from("TINYINT"),
        DataType::Int16 => String::from("SMALLINT"),
        DataType::Int32 => String::from("INT"),
        DataType::Int64 => String::from("BIGINT"),
        DataType::Float32 => String::from("FLOAT"),
        DataType::Float64 => String::from("DOUBLE"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => String::from("STRING"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => String::from("BINARY"),
        DataType::Date32 => String::from("DATE"),
        DataType::Timestamp(_, _) => String::from("TIMESTAMP"),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_type_name(field.data_type()))
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(entries) if entries.len() == 2 => format!(
                "MAP<{},{}>",
                spark_type_name(entries[0].data_type()),
                spark_type_name(entries[1].data_type())
            ),
            _ => String::from("MAP"),
        },
        DataType::Struct(fields) => {
            let rendered: Vec<String> = fields
                .iter()
                .map(|field| format!("{}:{}", field.name(), spark_type_name(field.data_type())))
                .collect();
            format!("STRUCT<{}>", rendered.join(","))
        }
        DataType::Dictionary(_, values) => spark_type_name(values),
        other => other.to_string(),
    }
}

fn pack_unreferenced_params(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_down(|node| match node {
        Expr::HigherOrderFunction(hof) => pack_hof(hof),
        _ => Ok(Transformed::no(node)),
    })
}

fn pack_hof(hof: HigherOrderFunction) -> Result<Transformed<Expr>> {
    let mut changed = false;
    let mut args = Vec::with_capacity(hof.args.len());
    for arg in hof.args {
        match arg {
            Expr::Lambda(lambda) if lambda.params.len() >= 2 => {
                let referenced = referenced_params(&lambda.body)?;
                if lambda.params.iter().all(|name| referenced.contains(name)) {
                    args.push(Expr::Lambda(lambda));
                } else {
                    let kept =
                        crate::higher_order::hof_keep::keep_call(*lambda.body, &lambda.params);
                    args.push(Expr::Lambda(Lambda::new(lambda.params, kept)));
                    changed = true;
                }
            }
            _ => args.push(arg),
        }
    }
    Ok(Transformed::new(
        Expr::HigherOrderFunction(HigherOrderFunction::new(hof.func, args)),
        changed,
        TreeNodeRecursion::Continue,
    ))
}

fn referenced_params(body: &Expr) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    body.apply(|node| match node {
        Expr::LambdaVariable(var) => {
            names.insert(var.name.clone());
            Ok(TreeNodeRecursion::Continue)
        }
        Expr::Lambda(_) => Ok(TreeNodeRecursion::Jump),
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(names)
}
