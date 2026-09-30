use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Select};
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
            let mut star_pass = false;
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
                        ArmPlan::Ambiguous => star_pass = true,
                        ArmPlan::Failed => {}
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
            if star_pass {
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
                ArmPlan::Ambiguous => return ArmPosition::Skip,
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

    async fn arm_plan(&mut self, index: usize) -> ArmPlan {
        if let Some(plan) = self.plans.get(&index) {
            return plan.clone();
        }
        let arms = self.arms;
        let plan = match arms[index].arm_sql.as_deref() {
            Some(sql) => match self.ctx.sql(sql).await {
                Ok(frame) => ArmPlan::Typed(
                    frame
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.data_type().clone())
                        .collect(),
                ),
                Err(error) if is_ambiguity(&error) => ArmPlan::Ambiguous,
                Err(_) => ArmPlan::Failed,
            },
            None => ArmPlan::Failed,
        };
        self.plans.insert(index, plan.clone());
        plan
    }
}

pub(crate) fn unmapped_arm(select: &Select) -> ArmMap<'_> {
    ArmMap {
        positions: Vec::new(),
        provenance: Vec::new(),
        arm_sql: Some(select.to_string()),
    }
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
        _ => error
            .to_string()
            .split(|searched: char| !(searched.is_alphanumeric() || searched == '_'))
            .any(|word| word.eq_ignore_ascii_case("ambiguous")),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::super::select_values_arms::resolve_insert_arms;
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
    fn ambiguity_matches_the_typed_error_and_the_bare_word_only() {
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
    async fn arms_that_cannot_plan_keep_their_judgement() {
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
        assert!(!judge.skip_string(1, &value).await);
        assert!(matches!(judge.plans.get(&1), Some(ArmPlan::Failed)));
    }
}
