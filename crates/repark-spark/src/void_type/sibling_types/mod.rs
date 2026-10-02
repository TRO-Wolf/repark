use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName};
use repark_core::CatalogRegistry;

use super::ltz_values_store::{
    literal_source_type, load_table_schema, nvl_coalesce_text, probe_source_type,
};
use super::select_values_arms::{ArmMap, SiblingProvenance, SourceCell};
use super::source_leaves::{is_string_type, leaf_type};
use crate::catalog_ops::name_parts;
use crate::write_to_branch::qualify_table_parts;

pub(crate) struct SiblingJudge<'ctx, 'arms, 'ast> {
    ctx: &'ctx SessionContext,
    catalogs: &'ctx CatalogRegistry,
    arms: &'arms [ArmMap<'ast>],
    case_insensitive: bool,
    probes: HashMap<String, Option<DataType>>,
    tables: HashMap<String, Option<ArrowSchema>>,
    plans: HashMap<usize, ArmPlan>,
    skip: Option<Vec<bool>>,
}

#[derive(Clone)]
enum ArmPlan {
    Failed,
    Ambiguous,
    Unresolved,
    Typed(Vec<DataType>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SiblingStatus {
    Datetime,
    Other,
    Unknown,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArmPosition {
    Skip,
    Judge,
}

impl<'ctx, 'arms, 'ast> SiblingJudge<'ctx, 'arms, 'ast> {
    pub(crate) fn new(
        ctx: &'ctx SessionContext,
        catalogs: &'ctx CatalogRegistry,
        arms: &'arms [ArmMap<'ast>],
        case_insensitive: bool,
    ) -> Self {
        Self {
            ctx,
            catalogs,
            arms,
            case_insensitive,
            probes: HashMap::new(),
            tables: HashMap::new(),
            plans: HashMap::new(),
            skip: None,
        }
    }

    pub(crate) async fn skip_string(&mut self, position: usize, value: &Expr) -> bool {
        if let Some(data_type) = static_source_type(value) {
            if !is_string_type(&data_type) {
                return false;
            }
        } else {
            if !self
                .skip_map()
                .await
                .get(position)
                .copied()
                .unwrap_or(false)
            {
                return false;
            }
            let probed = self.probed_type(value).await;
            if !probed.is_some_and(|data_type| is_string_type(&data_type)) {
                return false;
            }
            return true;
        }
        self.skip_map()
            .await
            .get(position)
            .copied()
            .unwrap_or(false)
    }

    async fn skip_map(&mut self) -> &[bool] {
        if self.skip.is_none() {
            let arms = self.arms;
            let mut width = arms
                .iter()
                .map(|arm| arm.positions.len())
                .max()
                .unwrap_or(0);
            let mut skip = vec![false; width];
            let mut unresolved_pass = false;
            for (index, arm) in arms.iter().enumerate() {
                if arm.positions.is_empty() {
                    if arm.arm_sql.is_none() {
                        continue;
                    }
                    match self.arm_plan(index).await {
                        ArmPlan::Typed(types) => {
                            if types.len() > width {
                                skip.resize(types.len(), false);
                                width = types.len();
                            }
                            for (position, data_type) in types.iter().enumerate() {
                                if is_datetime_type(data_type) {
                                    skip[position] = true;
                                }
                            }
                        }
                        ArmPlan::Unresolved => unresolved_pass = true,
                        ArmPlan::Ambiguous | ArmPlan::Failed => {}
                    }
                    continue;
                }
                for (position, flag) in skip.iter_mut().enumerate().take(arm.positions.len()) {
                    if *flag {
                        continue;
                    }
                    if self.arm_position(index, position).await == ArmPosition::Skip {
                        *flag = true;
                    }
                }
            }
            if unresolved_pass || self.planned_ambiguous_arm() {
                skip.fill(true);
            }
            self.skip = Some(skip);
        }
        self.skip.as_deref().unwrap_or(&[])
    }

    async fn arm_position(&mut self, index: usize, position: usize) -> ArmPosition {
        let arms = self.arms;
        let mut unknown = false;
        if let Some(cells) = arms[index].positions.get(position) {
            for cell in cells {
                match self.values_column_status(cell).await {
                    SiblingStatus::Datetime => return ArmPosition::Skip,
                    SiblingStatus::Unknown => unknown = true,
                    SiblingStatus::Other => {}
                }
            }
        }
        if let Some(entries) = arms[index].provenance.get(position) {
            for entry in entries {
                match self.provenance_status(entry).await {
                    SiblingStatus::Datetime => return ArmPosition::Skip,
                    SiblingStatus::Unknown => unknown = true,
                    SiblingStatus::Other => {}
                }
            }
        }
        let cells_empty = arms[index]
            .positions
            .get(position)
            .is_none_or(Vec::is_empty);
        let provenance_empty = arms[index]
            .provenance
            .get(position)
            .is_none_or(Vec::is_empty);
        if (unknown || (cells_empty && provenance_empty)) && arms[index].arm_sql.is_some() {
            match self.arm_plan(index).await {
                ArmPlan::Typed(types) => match types.get(position) {
                    Some(data_type) if is_datetime_type(data_type) => return ArmPosition::Skip,
                    _ => return ArmPosition::Judge,
                },
                ArmPlan::Ambiguous | ArmPlan::Unresolved => return ArmPosition::Skip,
                ArmPlan::Failed => {}
            }
        }
        ArmPosition::Judge
    }

    async fn values_column_status(&mut self, cell: &SourceCell<'_>) -> SiblingStatus {
        let mut saw_datetime = false;
        let mut unknown = false;
        for row in cell.rows {
            let Some(expr) = row.content.get(cell.column) else {
                continue;
            };
            let typed = match static_source_type(expr) {
                Some(data_type) => Some(data_type),
                None => self.probed_type(expr).await,
            };
            match typed {
                Some(data_type) if is_datetime_type(&data_type) => saw_datetime = true,
                Some(DataType::Null) => {}
                Some(_) => return SiblingStatus::Other,
                None => unknown = true,
            }
        }
        if saw_datetime && !unknown {
            SiblingStatus::Datetime
        } else if unknown {
            SiblingStatus::Unknown
        } else {
            SiblingStatus::Other
        }
    }

    async fn provenance_status(&mut self, entry: &SiblingProvenance<'_>) -> SiblingStatus {
        match entry {
            SiblingProvenance::Table { table, column } => {
                match self.table_column_type(table, column).await {
                    Some(data_type) if is_datetime_type(&data_type) => SiblingStatus::Datetime,
                    Some(_) => SiblingStatus::Other,
                    None => SiblingStatus::Unknown,
                }
            }
            SiblingProvenance::Expr { expr } => {
                if let Some(data_type) = static_source_type(expr) {
                    if is_datetime_type(&data_type) {
                        return SiblingStatus::Datetime;
                    }
                    return SiblingStatus::Other;
                }
                match self.probed_type(expr).await {
                    Some(data_type) if is_datetime_type(&data_type) => SiblingStatus::Datetime,
                    Some(_) => SiblingStatus::Other,
                    None => SiblingStatus::Unknown,
                }
            }
        }
    }

    async fn table_column_type(&mut self, table: &ObjectName, column: &str) -> Option<DataType> {
        let parts = qualify_table_parts(self.ctx, name_parts(table));
        if parts.len() < 3 {
            return None;
        }
        let key = parts.join(".");
        if !self.tables.contains_key(&key) {
            let schema = load_table_schema(self.catalogs, &parts).await;
            self.tables.insert(key.clone(), schema);
        }
        let insensitive = self.case_insensitive;
        self.tables.get(&key)?.as_ref().and_then(|schema| {
            schema
                .fields()
                .iter()
                .find(|field| {
                    if insensitive {
                        field.name().eq_ignore_ascii_case(column)
                    } else {
                        field.name() == column
                    }
                })
                .map(|field| field.data_type().clone())
        })
    }

    async fn probed_type(&mut self, value: &Expr) -> Option<DataType> {
        let select = nvl_coalesce_text(value).unwrap_or_else(|| super::probe_text(value));
        if let Some(typed) = self.probes.get(&select) {
            return typed.clone();
        }
        let typed = probe_source_type(self.ctx, &select, self.case_insensitive)
            .await
            .ok()
            .flatten();
        self.probes.insert(select, typed.clone());
        typed
    }

    fn planned_ambiguous_arm(&self) -> bool {
        self.plans
            .values()
            .any(|plan| matches!(plan, ArmPlan::Ambiguous))
    }

    async fn arm_plan(&mut self, index: usize) -> ArmPlan {
        if let Some(plan) = self.plans.get(&index) {
            return plan.clone();
        }
        let arms = self.arms;
        let plan = match arms[index].arm_sql.as_deref() {
            Some(sql) => self.plan_arm_sql(sql).await,
            None => ArmPlan::Failed,
        };
        self.plans.insert(index, plan.clone());
        plan
    }

    async fn plan_arm_sql(&self, sql: &str) -> ArmPlan {
        let state = self.ctx.state();
        let session_dialect = state.config().options().sql_parser.dialect;
        let dialect = crate::dialect_for_executing_parse(sql, session_dialect);
        let Ok(mut statement) = state.sql_to_statement(sql, &dialect) else {
            return ArmPlan::Failed;
        };
        if let datafusion::sql::parser::Statement::Statement(inner) = &mut statement {
            crate::spark_ast::apply_pregate_judge_rewrites(sql, inner, self.case_insensitive);
        }
        match repark_core::column_resolution::plan_statement_with_column_repair(
            &state,
            statement,
            self.case_insensitive,
        )
        .await
        {
            Ok(plan) => typed_plan(&plan),
            Err(error) if is_ambiguity(&error) => ArmPlan::Ambiguous,
            Err(error) if is_parse_error(&error) => ArmPlan::Failed,
            Err(error) if is_resolution_error(&error) => ArmPlan::Unresolved,
            Err(_) => ArmPlan::Failed,
        }
    }
}

fn typed_plan(plan: &LogicalPlan) -> ArmPlan {
    ArmPlan::Typed(
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect(),
    )
}

fn static_source_type(value: &Expr) -> Option<DataType> {
    literal_source_type(value).or_else(|| leaf_type(value))
}

fn is_datetime_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

fn is_ambiguity(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::SchemaError(inner, _) => {
            matches!(inner.as_ref(), SchemaError::AmbiguousReference { .. })
        }
        DataFusionError::Plan(payload) => payload.starts_with("[AMBIGUOUS_REFERENCE]"),
        DataFusionError::Diagnostic(_, inner) | DataFusionError::Context(_, inner) => {
            is_ambiguity(inner)
        }
        DataFusionError::Shared(inner) => is_ambiguity(inner),
        DataFusionError::Collection(errors) => errors.iter().any(is_ambiguity),
        _ => false,
    }
}

fn is_parse_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::SQL(..) => true,
        DataFusionError::Diagnostic(_, inner) => is_parse_error(inner),
        DataFusionError::Collection(errors) => errors.iter().any(is_parse_error),
        _ => false,
    }
}

fn is_resolution_error(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::SchemaError(inner, _) => {
            matches!(inner.as_ref(), SchemaError::FieldNotFound { .. })
        }
        DataFusionError::Plan(payload) => is_resolution_payload(payload),
        DataFusionError::Diagnostic(detail, inner) => {
            is_unknown_table_text(&detail.message) || is_resolution_error(inner)
        }
        DataFusionError::Context(_, inner) => is_resolution_error(inner),
        DataFusionError::Shared(inner) => is_resolution_error(inner),
        DataFusionError::Collection(errors) => errors.iter().any(is_resolution_error),
        _ => false,
    }
}

fn is_resolution_payload(payload: &str) -> bool {
    is_unresolved_tag(payload)
        || payload.starts_with("Invalid function '")
        || is_unknown_table_text(payload)
}

fn is_unresolved_tag(payload: &str) -> bool {
    payload.starts_with("[UNRESOLVED_COLUMN")
        || payload.starts_with("[UNRESOLVED_ROUTINE")
        || payload.starts_with("[TABLE_OR_VIEW_NOT_FOUND")
}

fn is_unknown_table_text(text: &str) -> bool {
    text.starts_with("table '") && text.ends_with("' not found")
}

#[cfg(test)]
mod tests;
