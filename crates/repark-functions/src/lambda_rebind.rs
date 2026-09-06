use std::collections::HashSet;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion::logical_expr::{Expr, LogicalPlan, lit};
use datafusion::optimizer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct LambdaRebind;

impl AnalyzerRule for LambdaRebind {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let packed = plan.map_expressions(pack_unreferenced_params).data()?;
        packed.resolve_lambda_variables().data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "lambda_rebind"
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
                    let kept = keep_params(*lambda.body, &lambda.params);
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

fn keep_params(body: Expr, params: &[String]) -> Expr {
    let mut args = Vec::with_capacity(1 + params.len() * 2);
    args.push(lit("__hof_body"));
    args.push(body);
    for (index, name) in params.iter().enumerate() {
        args.push(lit(format!("__hof_p{index}")));
        args.push(Expr::LambdaVariable(LambdaVariable::new(
            name.clone(),
            None,
        )));
    }
    let packed = datafusion::functions::expr_fn::named_struct(args);
    datafusion::functions::core::get_field().call(vec![packed, lit("__hof_body")])
}
