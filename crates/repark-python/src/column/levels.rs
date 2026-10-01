use datafusion::logical_expr::Expr;
use pyo3::prelude::*;

#[pyclass(name = "PyColumn", module = "repark._native", from_py_object)]
pub struct PyColumn {
    pub(super) expr: Expr,
    pub(super) expr_levels: usize,
    pub(super) df_levels: usize,
    pub(super) plan_levels: usize,
}

impl PyColumn {
    pub(crate) fn from_expr(expr: Expr) -> Self {
        let (expression_depth, plan_depth) = crate::deep_stack::survey_expression(&expr);
        Self {
            expr,
            expr_levels: expression_depth,
            df_levels: expression_depth,
            plan_levels: plan_depth,
        }
    }

    pub(crate) fn from_sql_text(expr: Expr) -> Self {
        let (expression_depth, plan_depth) = crate::deep_stack::survey_expression(&expr);
        Self {
            expr,
            expr_levels: expression_depth,
            df_levels: 0,
            plan_levels: plan_depth,
        }
    }

    pub(crate) fn input_levels<'a>(
        inputs: impl IntoIterator<Item = &'a PyColumn>,
    ) -> (usize, usize, usize) {
        let mut levels = (0, 0, 0);
        for input in inputs {
            levels.0 = levels.0.max(input.expr_levels);
            levels.1 = levels.1.max(input.df_levels);
            levels.2 = levels.2.max(input.plan_levels);
        }
        levels
    }

    pub(crate) fn combine<'a>(
        expr: Expr,
        inputs: impl IntoIterator<Item = &'a PyColumn>,
        added: usize,
    ) -> Self {
        Self::combine_levels(expr, Self::input_levels(inputs), added)
    }

    pub(crate) fn combine_levels(expr: Expr, levels: (usize, usize, usize), added: usize) -> Self {
        Self {
            expr,
            expr_levels: levels.0.saturating_add(added),
            df_levels: levels.1.saturating_add(added),
            plan_levels: levels.2,
        }
    }

    pub(crate) fn combine_surveyed<'a>(
        expr: Expr,
        inputs: impl IntoIterator<Item = &'a PyColumn>,
    ) -> Self {
        Self::surveyed_levels(expr, Self::input_levels(inputs))
    }

    pub(crate) fn surveyed_levels(expr: Expr, levels: (usize, usize, usize)) -> Self {
        let (expression_depth, surveyed_plan) = crate::deep_stack::survey_expression(&expr);
        let grown = expression_depth.saturating_sub(levels.0);
        let shrunk = levels.0.saturating_sub(expression_depth);
        Self {
            expr,
            expr_levels: expression_depth,
            df_levels: levels.1.saturating_add(grown).saturating_sub(shrunk),
            plan_levels: levels.2.max(surveyed_plan),
        }
    }

    pub(crate) fn grown_read<T>(&self, read: impl FnOnce(&Expr) -> T) -> T {
        let need = crate::deep_stack::clone_need_bytes(self.plan_levels, self.expr_levels);
        crate::deep_stack::grown_sync(need, || read(&self.expr))
    }
}
