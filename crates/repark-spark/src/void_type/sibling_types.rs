use std::collections::HashMap;
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Cte, Expr, ObjectName, ObjectNamePart, Select, Visit, Visitor,
};
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
        let typed = probe_source_type(self.ctx, &select).await.ok().flatten();
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
        let Ok(statement) = state.sql_to_statement(sql, &dialect) else {
            return ArmPlan::Failed;
        };
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
            Err(_) => ArmPlan::Unresolved,
        }
    }
}

pub(crate) fn unmapped_arm<'arm>(
    select: &Select,
    scope: &[&Cte],
    case_insensitive: bool,
) -> ArmMap<'arm> {
    ArmMap {
        positions: Vec::new(),
        provenance: Vec::new(),
        arm_sql: scoped_arm_sql(select, scope, case_insensitive),
    }
}

pub(crate) fn scoped_arm_sql(
    select: &Select,
    scope: &[&Cte],
    case_insensitive: bool,
) -> Option<String> {
    if scope.is_empty() {
        return Some(select.to_string());
    }
    let mut kept: Vec<&Cte> = Vec::with_capacity(scope.len());
    for cte in scope {
        let name = cte.alias.name.value.as_str();
        if let Some(position) = kept
            .iter()
            .position(|kept| names_equal(&kept.alias.name.value, name, case_insensitive))
        {
            kept.remove(position);
        }
        kept.push(*cte);
    }
    if has_case_twins(&kept) {
        return None;
    }
    let mut sql = String::from(if scope_needs_recursive(&kept, case_insensitive) {
        "WITH RECURSIVE "
    } else {
        "WITH "
    });
    for (position, cte) in kept.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&cte.to_string());
    }
    sql.push(' ');
    sql.push_str(&select.to_string());
    Some(sql)
}

fn has_case_twins(kept: &[&Cte]) -> bool {
    kept.iter().enumerate().any(|(index, first)| {
        kept.iter().skip(index + 1).any(|second| {
            first.alias.name.value != second.alias.name.value
                && first
                    .alias
                    .name
                    .value
                    .eq_ignore_ascii_case(&second.alias.name.value)
        })
    })
}

fn scope_needs_recursive(kept: &[&Cte], case_insensitive: bool) -> bool {
    kept.iter().enumerate().any(|(index, cte)| {
        let mut seen = RelationNames { names: Vec::new() };
        let _ = cte.query.visit(&mut seen);
        seen.names.iter().any(|name| {
            kept.iter()
                .skip(index)
                .any(|kept| names_equal(&kept.alias.name.value, name, case_insensitive))
        })
    })
}

fn names_equal(first: &str, second: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        first.eq_ignore_ascii_case(second)
    } else {
        first == second
    }
}

struct RelationNames {
    names: Vec<String>,
}

impl Visitor for RelationNames {
    type Break = std::convert::Infallible;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let [ObjectNamePart::Identifier(ident)] = relation.0.as_slice() {
            self.names.push(ident.value.clone());
        }
        ControlFlow::Continue(())
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
        DataFusionError::Diagnostic(_, inner) => is_ambiguity(inner),
        DataFusionError::Collection(errors) => errors.iter().any(is_ambiguity),
        error => error.to_string().contains("[AMBIGUOUS_REFERENCE]"),
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

#[cfg(test)]
mod tests {
    use datafusion::common::Column;
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::super::select_values_arms::{ArmMap, resolve_insert_arms};
    use super::*;

    fn arms_of(sql: &str) -> Vec<String> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        resolve_insert_arms(&source, true)
            .iter()
            .map(|arm| {
                format!(
                    "{}:{}",
                    arm.positions.len(),
                    arm.arm_sql.clone().unwrap_or_default()
                )
            })
            .collect()
    }

    #[test]
    fn stars_over_tables_keep_no_cells_and_carry_the_arm_select() {
        let star = arms_of(
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM src",
        );
        assert_eq!(star.len(), 2);
        assert!(star[1].starts_with("0:SELECT * FROM src"), "{star:?}");
        let qualified = arms_of(
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT s.* FROM src AS s",
        );
        assert_eq!(qualified.len(), 2);
        assert!(
            qualified[1].starts_with("0:SELECT s.* FROM src AS s"),
            "{qualified:?}"
        );
        let mixed = arms_of(
            "INSERT INTO t SELECT * FROM (VALUES (1, 2)) AS v(a, b) JOIN src ON v.a = src.id",
        );
        assert_eq!(mixed.len(), 1);
        assert!(mixed[0].starts_with("0:SELECT * FROM"), "{mixed:?}");
    }

    #[test]
    fn static_typing_reads_literals_and_casts_but_defers_functions() {
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t VALUES ('x', CAST('x' AS STRING), concat('a', 'b'), \
             TIMESTAMP '2020-01-01 10:00:00', make_timestamp(2020, 1, 1, 0, 0, 0))",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let SetExpr::Values(values) = source.body.as_ref() else {
            panic!("want a VALUES source");
        };
        let typed: Vec<Option<DataType>> = values.rows[0]
            .content
            .iter()
            .map(static_source_type)
            .collect();
        assert_eq!(typed[0], Some(DataType::Utf8), "{typed:?}");
        assert_eq!(typed[1], Some(DataType::Utf8), "{typed:?}");
        assert_eq!(typed[2], None, "{typed:?}");
        assert_eq!(
            typed[3],
            Some(DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "{typed:?}"
        );
        assert_eq!(typed[4], None, "{typed:?}");
    }

    #[test]
    fn ambiguity_matches_only_the_typed_error_and_the_tag() {
        let typed = DataFusionError::SchemaError(
            Box::new(SchemaError::AmbiguousReference {
                field: Box::new(Column::from_name("tsc")),
            }),
            Box::new(None),
        );
        assert!(is_ambiguity(&typed));
        let wrapped = DataFusionError::Collection(vec![typed]);
        assert!(is_ambiguity(&wrapped));
        assert!(is_ambiguity(&DataFusionError::Plan(
            "Error during planning: [AMBIGUOUS_REFERENCE] Reference `tsc` is ambiguous".to_string(),
        )));
        assert!(!is_ambiguity(&DataFusionError::Plan(
            "Reference `tsc` is ambiguous, could be expensive".to_string()
        )));
        assert!(!is_ambiguity(&DataFusionError::Plan(
            "table ambiguous_tsc not found".to_string()
        )));
        assert!(!is_ambiguity(&DataFusionError::Plan(
            "type_coercion".to_string()
        )));
    }

    #[tokio::test]
    async fn function_cells_type_through_the_shared_probe() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t SELECT * FROM (VALUES (1, concat('a', 'b'))) AS v(a, b)",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert_eq!(judge.probed_type(&value).await, Some(DataType::Utf8));
        assert_eq!(judge.probed_type(&value).await, Some(DataType::Utf8));
        assert_eq!(judge.probes.len(), 1);
    }

    #[tokio::test]
    async fn function_string_beside_timestamp_skips_but_beside_bigint_judges() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        for (sql, position, want) in [
            (
                "INSERT INTO t SELECT * FROM (VALUES (1, concat('2020-01-01', ' 10:00:00'))) AS v(a, b) UNION ALL SELECT 2, TIMESTAMP '2021-02-02 02:02:02'",
                1,
                true,
            ),
            (
                "INSERT INTO t SELECT * FROM (VALUES (1, '2020-01-01 10:00:00')) AS v(a, b) UNION ALL SELECT 2, 7L",
                1,
                false,
            ),
        ] {
            let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
            let Statement::Insert(insert) = statements.swap_remove(0) else {
                panic!("want an INSERT statement");
            };
            let source = insert.source.unwrap();
            let arms = resolve_insert_arms(&source, true);
            let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
            let cell = &arms[0].positions[position][0];
            let value = cell.rows[0].content[cell.column].clone();
            assert_eq!(judge.skip_string(position, &value).await, want, "{sql}");
        }
    }

    #[tokio::test]
    async fn unresolvable_arms_leave_cells_unjudged_for_the_analyzer() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM missing_table",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        assert_eq!(arms.len(), 2);
        assert!(arms[1].positions.is_empty());
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
    }

    #[tokio::test]
    async fn arms_without_sql_keep_their_judgement() {
        let ctx = SessionContext::new();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b)",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let mut arms = resolve_insert_arms(&source, true);
        arms.push(ArmMap {
            positions: vec![Vec::new(), Vec::new()],
            provenance: vec![Vec::new(), Vec::new()],
            arm_sql: None,
        });
        assert_eq!(arms.len(), 2);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(!judge.skip_string(1, &value).await);
    }

    fn mem_ctx() -> SessionContext {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::memory::MemTable;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]);
        let table =
            MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
        ctx.register_table("t", Arc::new(table))
            .expect("memory table registers");
        ctx
    }

    fn mem_ctx_upper() -> SessionContext {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::memory::MemTable;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("ID", DataType::Int32, false),
            Field::new("C", DataType::Utf8, false),
        ]);
        let table =
            MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
        ctx.register_table("t", Arc::new(table))
            .expect("memory table registers");
        ctx
    }

    fn mem_ctx_join() -> SessionContext {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::memory::MemTable;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        for name in ["t", "u"] {
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("c", DataType::Utf8, false),
            ]);
            let table =
                MemTable::try_new(Arc::new(schema), vec![Vec::new()]).expect("memory table builds");
            ctx.register_table(name, Arc::new(table))
                .expect("memory table registers");
        }
        ctx
    }

    #[tokio::test]
    async fn quoted_case_in_expressions_resolves_through_column_repair() {
        let ctx = mem_ctx();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(\"S\") FROM t",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(!judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
    }

    #[tokio::test]
    async fn genuinely_missing_columns_in_expressions_stay_unresolved() {
        let ctx = mem_ctx();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(nosuch) FROM t",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
    }

    #[tokio::test]
    async fn lowercase_refs_over_uppercase_columns_stay_judged() {
        let ctx = mem_ctx_upper();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(c) FROM t",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(!judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
    }

    #[tokio::test]
    async fn case_sensitive_exact_uppercase_refs_stay_judged() {
        let ctx = mem_ctx_upper();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT ID, UPPER(C) FROM t",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, false);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, false);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(!judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Typed(_))));
    }

    #[tokio::test]
    async fn case_sensitive_missing_uppercase_refs_stay_unresolved() {
        let ctx = mem_ctx();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, UPPER(C) FROM t",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, false);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, false);
        let cell = &arms[0].positions[1][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Unresolved)));
    }

    #[tokio::test]
    async fn ambiguous_arms_unjudge_values_positions_at_other_positions() {
        let ctx = mem_ctx_join();
        let catalogs = CatalogRegistry::new();
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO g SELECT v.a, t.c FROM (VALUES ('x', 2)) AS v(a, b) JOIN t ON v.b = t.id JOIN u ON t.id = u.id WHERE c = c",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        assert_eq!(arms.len(), 1);
        let mut judge = SiblingJudge::new(&ctx, &catalogs, &arms, true);
        let cell = &arms[0].positions[0][0];
        let value = cell.rows[0].content[cell.column].clone();
        assert!(judge.skip_string(0, &value).await);
        assert!(matches!(judge.plans.get(&0), Some(ArmPlan::Ambiguous)));
    }

    #[test]
    fn scoped_arms_carry_their_cte_definitions() {
        let scoped = arms_of(
            "INSERT INTO t WITH xv AS (SELECT * FROM strtab) SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM xv",
        );
        assert_eq!(scoped.len(), 2);
        assert!(scoped[1].starts_with("0:WITH "), "{scoped:?}");
        assert!(scoped[1].contains("xv AS"), "{scoped:?}");
        assert!(scoped[1].ends_with("SELECT * FROM xv"), "{scoped:?}");
        let bare = arms_of(
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT * FROM src",
        );
        assert_eq!(bare.len(), 2);
        assert!(bare[1].starts_with("0:SELECT * FROM src"), "{bare:?}");
    }

    #[test]
    fn scoped_arms_dedupe_shadowed_names_and_flag_recursion() {
        let shadowed = arms_of(
            "INSERT INTO t WITH v(a, b) AS (VALUES (1, 2)) SELECT * FROM v UNION ALL (WITH v(a, b) AS (VALUES (3, 4)) SELECT * FROM v)",
        );
        assert_eq!(shadowed.len(), 2);
        assert!(shadowed[1].starts_with("2:WITH "), "{shadowed:?}");
        assert!(shadowed[1].contains("VALUES (3, 4)"), "{shadowed:?}");
        assert!(!shadowed[1].contains("VALUES (1, 2)"), "{shadowed:?}");
        let recursive =
            arms_of("INSERT INTO t WITH RECURSIVE v AS (SELECT * FROM v) SELECT * FROM v");
        assert_eq!(recursive.len(), 1);
        assert!(
            recursive[0].starts_with("0:WITH RECURSIVE "),
            "{recursive:?}"
        );
    }

    #[test]
    fn scoped_arms_with_case_twins_carry_no_sql() {
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t WITH xv AS (VALUES (1)) SELECT * FROM xv UNION ALL (WITH XV AS (VALUES (2)) SELECT * FROM XV)",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, false);
        assert_eq!(arms.len(), 2);
        assert_eq!(arms[1].positions.len(), 1);
        assert!(arms[1].arm_sql.is_none());
        let merged = arms_of(
            "INSERT INTO t WITH xv AS (VALUES (1)) SELECT * FROM xv UNION ALL (WITH XV AS (VALUES (2)) SELECT * FROM XV)",
        );
        assert_eq!(merged.len(), 2);
        assert!(merged[1].starts_with("1:WITH "), "{merged:?}");
        assert!(merged[1].contains("VALUES (2)"), "{merged:?}");
        assert!(!merged[1].contains("VALUES (1)"), "{merged:?}");
    }
}
