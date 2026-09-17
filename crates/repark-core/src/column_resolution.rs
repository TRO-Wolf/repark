use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::ops::ControlFlow;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, SchemaError, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, AssignmentTarget, Expr as SqlExpr, Ident, ObjectNamePart, Statement, Value,
    VisitMut, VisitorMut,
};

pub const SPARK_SQL_CASE_SENSITIVE_KEY: &str = "spark.sql.caseSensitive";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnResolutionConfig {
    pub case_insensitive: bool,
}

impl ConfigExtension for ColumnResolutionConfig {
    const PREFIX: &'static str = "repark.column_resolution";
}

impl ExtensionOptions for ColumnResolutionConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: case sensitivity is set with \
             `{SPARK_SQL_CASE_SENSITIVE_KEY}` on the session builder; change it at runtime with \
             `SET spark.sql.caseSensitive`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_column_case_sensitive(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \
             \"{SPARK_SQL_CASE_SENSITIVE_KEY}\" is invalid. \
             {SPARK_SQL_CASE_SENSITIVE_KEY} should be boolean, but was {raw}"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn column_case_sensitive_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_CASE_SENSITIVE_KEY) {
        Some(raw) => parse_column_case_sensitive(raw),
        None => Ok(false),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_runtime_column_case_sensitive(raw: &str) -> Result<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{raw}' in the config \
             \"{SPARK_SQL_CASE_SENSITIVE_KEY}\" is invalid. It should be a/an 'boolean' value. \
             SQLSTATE: 22022"
        )))
    }
}

#[must_use]
pub fn with_column_resolution_config(
    config: datafusion::prelude::SessionConfig,
    case_insensitive: bool,
) -> datafusion::prelude::SessionConfig {
    config.with_option_extension(ColumnResolutionConfig { case_insensitive })
}

#[must_use]
pub fn column_resolution_is_case_insensitive(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<ColumnResolutionConfig>()
        .is_some_and(|extension| extension.case_insensitive)
}

#[allow(clippy::missing_errors_doc)]
pub async fn plan_statement_with_column_repair(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
) -> Result<LogicalPlan> {
    if !column_resolution_is_case_insensitive(state.config().options()) {
        return state.statement_to_plan(statement).await;
    }
    let datafusion::sql::parser::Statement::Statement(mut inner) = statement else {
        return state.statement_to_plan(statement).await;
    };
    let written = written_references(&inner);
    let mut repaired: HashSet<(Option<String>, String)> = HashSet::new();
    for _ in 0..64 {
        let attempt = datafusion::sql::parser::Statement::Statement(inner.clone());
        match state.statement_to_plan(attempt).await {
            Ok(plan) => {
                audit_plan_for_ambiguity(&plan, &written)?;
                return Ok(plan);
            }
            Err(error) => {
                let Some((missing, valid)) = missing_column(&error) else {
                    return Err(error);
                };
                let key = (
                    missing.relation.as_ref().map(ToString::to_string),
                    missing.name.clone(),
                );
                if !repaired.insert(key) {
                    return Err(error);
                }
                match repair_missing(&mut inner, missing, valid)? {
                    RepairOutcome::Repaired => {}
                    RepairOutcome::Missing => return Err(error),
                }
            }
        }
    }
    state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner))
        .await
}

#[allow(clippy::missing_errors_doc)]
pub async fn sql_with_column_repair(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    let dialect = ctx.state().config().options().sql_parser.dialect;
    let statement = ctx.state().sql_to_statement(sql, &dialect)?;
    let plan = plan_statement_with_column_repair(&ctx.state(), statement).await?;
    ctx.execute_logical_plan(plan).await
}

fn missing_column(error: &DataFusionError) -> Option<(&Column, &[Column])> {
    match error {
        DataFusionError::SchemaError(inner, _) => match inner.as_ref() {
            SchemaError::FieldNotFound {
                field,
                valid_fields,
            } => Some((field, valid_fields)),
            _ => None,
        },
        DataFusionError::Diagnostic(_, inner) => missing_column(inner),
        DataFusionError::Collection(errors) => errors.iter().find_map(missing_column),
        _ => None,
    }
}

enum RepairOutcome {
    Repaired,
    Missing,
}

fn repair_missing(
    statement: &mut Statement,
    missing: &Column,
    valid_fields: &[Column],
) -> Result<RepairOutcome> {
    let mut candidates: Vec<&Column> = Vec::new();
    for candidate in valid_fields {
        if !candidate.name.eq_ignore_ascii_case(&missing.name) {
            continue;
        }
        match (&missing.relation, &candidate.relation) {
            (None, _) => {}
            (Some(written), Some(candidate)) => {
                if !qualifier_matches(written, candidate) {
                    continue;
                }
            }
            (Some(_), None) => continue,
        }
        candidates.push(candidate);
    }
    dedup_candidates(&mut candidates);
    if candidates.is_empty() {
        return Ok(RepairOutcome::Missing);
    }
    if candidates.len() > 1 {
        return Err(DataFusionError::Plan(ambiguous_message(
            missing,
            &candidates,
        )));
    }
    let canonical = candidates.remove(0);
    let mut repair = CaseRepair {
        missing_relation: missing
            .relation
            .as_ref()
            .map(|relation| relation.table().to_string()),
        missing_name: missing.name.clone(),
        canonical_qualifier: canonical
            .relation
            .as_ref()
            .map(|relation| relation.table().to_string()),
        canonical_name: canonical.name.clone(),
        changed: false,
    };
    let _ = statement.visit(&mut repair);
    if repair.changed {
        Ok(RepairOutcome::Repaired)
    } else {
        Ok(RepairOutcome::Missing)
    }
}

fn written_references(statement: &Statement) -> (HashSet<String>, HashSet<(String, String)>) {
    struct Collector {
        bare: HashSet<String>,
        qualified: HashSet<(String, String)>,
    }
    impl VisitorMut for Collector {
        type Break = std::convert::Infallible;
        fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
            match expr {
                SqlExpr::Identifier(ident) => {
                    self.bare.insert(ident.value.clone());
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    let name = parts[parts.len() - 1].value.clone();
                    let qualifier = parts[parts.len() - 2].value.clone();
                    self.qualified.insert((qualifier, name));
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    let mut collector = Collector {
        bare: HashSet::new(),
        qualified: HashSet::new(),
    };
    let mut owned = statement.clone();
    let _ = owned.visit(&mut collector);
    (collector.bare, collector.qualified)
}

fn audit_plan_for_ambiguity(
    plan: &LogicalPlan,
    written: &(HashSet<String>, HashSet<(String, String)>),
) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        let mut schema = DFSchema::empty();
        for input in node.inputs() {
            schema.merge(input.schema());
        }
        for expr in node.expressions() {
            expr.apply(|leaf| {
                if let Expr::Column(column) = leaf {
                    audit_column(column, &schema, written)?;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

fn audit_column(
    column: &Column,
    schema: &DFSchema,
    written: &(HashSet<String>, HashSet<(String, String)>),
) -> Result<()> {
    let columns = schema.columns();
    let mut honored: Vec<&Column> = Vec::new();
    let mut unqualified: Vec<&Column> = Vec::new();
    for valid in &columns {
        if !valid.name.eq_ignore_ascii_case(&column.name) {
            continue;
        }
        unqualified.push(valid);
        match (&column.relation, &valid.relation) {
            (None, _) => honored.push(valid),
            (Some(written), Some(candidate)) => {
                if qualifier_matches(written, candidate) {
                    honored.push(valid);
                }
            }
            (Some(_), None) => {}
        }
    }
    dedup_candidates(&mut honored);
    dedup_candidates(&mut unqualified);
    let written_qualified = column.relation.as_ref().is_some_and(|relation| {
        written.1.iter().any(|(qualifier, name)| {
            qualifier.eq_ignore_ascii_case(relation.table())
                && name.eq_ignore_ascii_case(&column.name)
        })
    });
    if column.relation.is_some() && honored.len() > 1 && written_qualified {
        let reference = Column::new_unqualified(column.name.clone());
        return Err(DataFusionError::Plan(ambiguous_message(
            &reference, &honored,
        )));
    }
    let mut spellings = HashSet::new();
    for valid in &unqualified {
        spellings.insert(valid.name.as_str());
    }
    let refers = written
        .0
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&column.name));
    if spellings.len() > 1 && refers {
        let reference = Column::new_unqualified(column.name.clone());
        return Err(DataFusionError::Plan(ambiguous_message(
            &reference,
            &unqualified,
        )));
    }
    Ok(())
}

fn dedup_candidates(candidates: &mut Vec<&Column>) {
    let mut seen: HashSet<(Option<String>, String)> = HashSet::new();
    candidates.retain(|candidate| {
        seen.insert((
            candidate.relation.as_ref().map(ToString::to_string),
            candidate.name.clone(),
        ))
    });
}

fn qualifier_matches(written: &TableReference, candidate: &TableReference) -> bool {
    written.table().eq_ignore_ascii_case(candidate.table())
        && part_matches(written.schema(), candidate.schema())
        && part_matches(written.catalog(), candidate.catalog())
}

fn part_matches(first: Option<&str>, second: Option<&str>) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(first), Some(second)) => first.eq_ignore_ascii_case(second),
        _ => false,
    }
}

fn ambiguous_message(missing: &Column, candidates: &[&Column]) -> String {
    let reference = match &missing.relation {
        Some(qualifier) => format!("`{}`.`{}`", qualifier.table(), missing.name),
        None => format!("`{}`", missing.name),
    };
    let tables = candidates
        .iter()
        .map(|candidate| {
            candidate
                .relation
                .as_ref()
                .map(|relation| relation.table().to_string())
        })
        .collect::<Vec<_>>();
    let uniform = tables.windows(2).all(|pair| pair[0] == pair[1]);
    let options = candidates
        .iter()
        .zip(tables.iter())
        .map(|(candidate, table)| match table {
            Some(own) if !uniform => format!("`{own}`.`{}`", candidate.name),
            _ => format!("`{}`", candidate.name),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: [{options}].")
}

struct CaseRepair {
    missing_relation: Option<String>,
    missing_name: String,
    canonical_qualifier: Option<String>,
    canonical_name: String,
    changed: bool,
}

impl CaseRepair {
    fn matches_missing(&self, qualifier: Option<&str>, name: &str) -> bool {
        if name != self.missing_name {
            return false;
        }
        match (&self.missing_relation, qualifier) {
            (None, None) => true,
            (Some(written), Some(found)) => written == found,
            _ => false,
        }
    }

    fn rewrite_ident(&mut self, ident: &mut Ident) {
        if ident.value == self.canonical_name {
            return;
        }
        ident.value.clone_from(&self.canonical_name);
        self.changed = true;
    }

    fn rewrite_qualifier(&mut self, ident: &mut Ident) {
        let Some(canonical) = self.canonical_qualifier.clone() else {
            return;
        };
        if ident.value != canonical {
            ident.value = canonical;
            self.changed = true;
        }
    }
}

impl VisitorMut for CaseRepair {
    type Break = std::convert::Infallible;

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        match expr {
            SqlExpr::Identifier(ident) => {
                if self.missing_relation.is_none() && ident.value == self.missing_name {
                    self.rewrite_ident(ident);
                }
            }
            SqlExpr::CompoundIdentifier(parts)
                if parts.len() == 2
                    && self.matches_missing(Some(parts[0].value.as_str()), &parts[1].value) =>
            {
                self.rewrite_qualifier(&mut parts[0]);
                self.rewrite_ident(&mut parts[1]);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Insert(insert) => {
                for column in &mut insert.columns {
                    rewrite_object_name(column, self);
                }
            }
            Statement::Update(update) => {
                for assignment in &mut update.assignments {
                    if let AssignmentTarget::ColumnName(name) = &mut assignment.target {
                        rewrite_object_name(name, self);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn part_value(part: &ObjectNamePart) -> Option<&str> {
    match part {
        ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
        ObjectNamePart::Function(_) => None,
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn rewrite_fragment_case(
    sql: &str,
    scopes: &[(&str, &[String])],
    case_insensitive: bool,
) -> Result<String, DataFusionError> {
    if !case_insensitive {
        return Ok(sql.to_string());
    }
    let dialect = datafusion::sql::sqlparser::dialect::DatabricksDialect {};
    let mut expr = datafusion::sql::sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|error| DataFusionError::SQL(Box::new(error), None))?
        .parse_expr()
        .map_err(|error| DataFusionError::SQL(Box::new(error), None))?;
    let mut repair = FragmentRepair {
        scopes,
        depth: 0,
        field_depth: 0,
        error: None,
    };
    let _ = expr.visit(&mut repair);
    if let Some(error) = repair.error {
        return Err(error);
    }
    Ok(expr.to_string())
}

struct FragmentRepair<'a> {
    scopes: &'a [(&'a str, &'a [String])],
    depth: usize,
    field_depth: usize,
    error: Option<DataFusionError>,
}

impl FragmentRepair<'_> {
    fn candidates(&self, qualifier: Option<&str>, name: &str) -> Vec<(String, String)> {
        let mut found: Vec<(String, String)> = Vec::new();
        for (alias, fields) in self.scopes {
            let alias = alias.trim_matches('"');
            if let Some(qualifier) = qualifier
                && !alias.eq_ignore_ascii_case(qualifier)
            {
                continue;
            }
            for field in *fields {
                if field.eq_ignore_ascii_case(name) {
                    found.push((alias.to_string(), field.clone()));
                }
            }
        }
        found.dedup();
        found
    }

    fn rewrite_ident(&mut self, qualifier: Option<&str>, ident: &mut Ident) {
        if self.error.is_some() {
            return;
        }
        let mut matches = self.candidates(qualifier, &ident.value);
        if matches.is_empty() {
            return;
        }
        if matches.len() > 1 {
            let missing = match qualifier {
                Some(scope) => Column::new(
                    Some(TableReference::bare(scope.to_string())),
                    ident.value.clone(),
                ),
                None => Column::new_unqualified(ident.value.clone()),
            };
            let columns = matches
                .iter()
                .map(|(scope, field)| {
                    Column::new(Some(TableReference::bare(scope.clone())), field.clone())
                })
                .collect::<Vec<_>>();
            let refs = columns.iter().collect::<Vec<_>>();
            self.error = Some(DataFusionError::Plan(ambiguous_message(&missing, &refs)));
            return;
        }
        let (_, canonical) = matches.remove(0);
        if ident.value != canonical {
            ident.value = canonical;
        }
    }
}

impl VisitorMut for FragmentRepair<'_> {
    type Break = ();

    fn pre_visit_query(
        &mut self,
        _query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(
        &mut self,
        _query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::CompoundFieldAccess { root, access_chain } = expr {
            self.field_depth += 1;
            if self.depth > 0 || self.error.is_some() {
                return ControlFlow::Continue(());
            }
            if let SqlExpr::Value(value) = root.as_mut()
                && let Value::DoubleQuotedString(qualifier) = &value.value
                && let [AccessExpr::Dot(field)] = access_chain.as_mut_slice()
                && let SqlExpr::Identifier(ident) = field
            {
                self.rewrite_ident(Some(qualifier.as_str()), ident);
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.depth > 0 || self.error.is_some() {
            return ControlFlow::Continue(());
        }
        match expr {
            SqlExpr::Identifier(ident) => {
                if self.field_depth == 0 {
                    self.rewrite_ident(None, ident);
                }
            }
            SqlExpr::CompoundIdentifier(parts) => {
                if self.field_depth == 0 && parts.len() == 2 {
                    let qualifier = parts[0].value.clone();
                    let name = parts[1].value.clone();
                    if self
                        .candidates(Some(qualifier.as_str()), name.as_str())
                        .is_empty()
                    {
                        return ControlFlow::Continue(());
                    }
                    self.rewrite_ident(Some(qualifier.as_str()), &mut parts[1]);
                }
            }
            SqlExpr::CompoundFieldAccess { .. } => {
                self.field_depth = self.field_depth.saturating_sub(1);
            }
            _ => {}
        }
        if self.error.is_some() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn rewrite_object_name(
    name: &mut datafusion::sql::sqlparser::ast::ObjectName,
    repair: &mut CaseRepair,
) {
    if name.0.len() == 1 {
        let ObjectNamePart::Identifier(ident) = &mut name.0[0] else {
            return;
        };
        if repair.missing_relation.is_none() && ident.value == repair.missing_name {
            repair.rewrite_ident(ident);
        }
        return;
    }
    if name.0.len() == 2 {
        let qualifier = part_value(&name.0[0]).unwrap_or_default().to_string();
        let column = part_value(&name.0[1]).unwrap_or_default().to_string();
        if part_value(&name.0[0]).is_some()
            && part_value(&name.0[1]).is_some()
            && repair.matches_missing(Some(qualifier.as_str()), column.as_str())
        {
            if let ObjectNamePart::Identifier(qualifier) = &mut name.0[0] {
                repair.rewrite_qualifier(qualifier);
            }
            if let ObjectNamePart::Identifier(column) = &mut name.0[1] {
                repair.rewrite_ident(column);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;

    fn repair_state(case_insensitive: bool) -> SessionState {
        let mut config = datafusion::prelude::SessionConfig::new();
        config.options_mut().sql_parser.enable_ident_normalization = false;
        let config = with_column_resolution_config(config, case_insensitive);
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build()
    }

    fn mixed_state(case_insensitive: bool) -> SessionState {
        let state = repair_state(case_insensitive);
        let schema = Arc::new(Schema::new(vec![
            Field::new("userId", DataType::Int64, true),
            Field::new("eventName", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx.state()
    }

    async fn plan_names(state: &SessionState, sql: &str) -> Vec<String> {
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(sql, &dialect).unwrap();
        plan_statement_with_column_repair(state, statement)
            .await
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    async fn plan_error(state: &SessionState, sql: &str) -> String {
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(sql, &dialect).unwrap();
        plan_statement_with_column_repair(state, statement)
            .await
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn carrier_absent_means_sensitive() {
        let options = datafusion::prelude::SessionConfig::new().options().clone();
        assert!(!column_resolution_is_case_insensitive(&options));
    }

    #[test]
    fn builder_map_parses_and_refuses_garbage() {
        let mut map = HashMap::new();
        assert!(!column_case_sensitive_from_config_map(&map).unwrap());
        map.insert(SPARK_SQL_CASE_SENSITIVE_KEY.to_string(), "true".to_string());
        assert!(column_case_sensitive_from_config_map(&map).unwrap());
        map.insert(SPARK_SQL_CASE_SENSITIVE_KEY.to_string(), "nope".to_string());
        assert!(column_case_sensitive_from_config_map(&map).is_err());
    }

    #[test]
    fn runtime_parse_is_strict_boolean() {
        assert!(parse_runtime_column_case_sensitive("true").unwrap());
        assert!(!parse_runtime_column_case_sensitive("FALSE").unwrap());
        assert!(parse_runtime_column_case_sensitive("1").is_err());
    }

    #[tokio::test]
    async fn wrong_case_select_filters_orders_and_reads_rows() {
        let state = mixed_state(true);
        assert_eq!(
            plan_names(&state, "SELECT USERID, EVENTNAME FROM t ORDER BY USERID").await,
            vec!["userId".to_string(), "eventName".to_string()]
        );
        let ctx = SessionContext::new_with_state(state);
        let batches = sql_with_column_repair(
            &ctx,
            "SELECT USERID FROM t WHERE EVENTNAME = 'b' ORDER BY USERID",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(column.len(), 1);
        assert_eq!(column.value(0), 2);
    }

    #[tokio::test]
    async fn quoted_wrong_case_and_qualified_resolve() {
        let state = mixed_state(true);
        assert_eq!(
            plan_names(&state, "SELECT `USERID` FROM t").await,
            vec!["userId".to_string()]
        );
        assert_eq!(
            plan_names(&state, "SELECT T.USERID FROM t AS T").await,
            vec!["userId".to_string()]
        );
    }

    #[tokio::test]
    async fn group_by_wrong_case_groups() {
        let state = mixed_state(true);
        assert_eq!(
            plan_names(
                &state,
                "SELECT EVENTNAME, COUNT(*) AS c FROM t GROUP BY EVENTNAME ORDER BY EVENTNAME"
            )
            .await,
            vec!["eventName".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn case_only_collision_raises_the_spark_sentence() {
        let state = repair_state(true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ID", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new_with_state(state.clone());
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let error = plan_error(&ctx.state(), "SELECT id FROM t").await;
        assert!(
            error.contains(
                "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `ID`]."
            ),
            "unexpected message: {error}"
        );
    }

    #[tokio::test]
    async fn join_collision_on_bare_reference_raises() {
        let state = repair_state(true);
        let ctx = SessionContext::new_with_state(state);
        for (name, value) in [("amb_l", "a"), ("amb_r", "A")] {
            let schema = Arc::new(Schema::new(vec![Field::new(value, DataType::Int32, true)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![Some(1)]))])
                    .unwrap();
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        }
        let error = plan_error(
            &ctx.state(),
            "SELECT a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A",
        )
        .await;
        assert!(
            error.contains(
                "[AMBIGUOUS_REFERENCE] Reference `a` is ambiguous, could be: [`amb_l`.`a`, `amb_r`.`A`]."
            ),
            "unexpected message: {error}"
        );
        assert_eq!(
            plan_names(
                &ctx.state(),
                "SELECT amb_l.a FROM amb_l JOIN amb_r ON amb_l.a = amb_r.A"
            )
            .await,
            vec!["a".to_string()]
        );
    }

    #[tokio::test]
    async fn sensitive_session_returns_the_native_error() {
        let state = mixed_state(false);
        let error = plan_error(&state, "SELECT USERID FROM t").await;
        assert!(error.contains("USERID"), "unexpected message: {error}");
        assert_eq!(
            plan_names(&state, "SELECT userId FROM t").await,
            vec!["userId".to_string()]
        );
    }

    #[test]
    fn fragment_rewrite_resolves_against_known_scopes() {
        let target = vec!["userId".to_string(), "eventName".to_string()];
        let source = vec!["userid".to_string(), "eventname".to_string()];
        let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
        assert_eq!(
            rewrite_fragment_case("USERID = 2", &[scopes[0]], true).unwrap(),
            "userId = 2".to_string()
        );
        assert_eq!(
            rewrite_fragment_case("t.USERID = s.userid AND t.EVENTNAME = 'x'", &scopes, true)
                .unwrap(),
            "t.userId = s.userid AND t.eventName = 'x'".to_string()
        );
        assert_eq!(
            rewrite_fragment_case("USERID = 2", &[scopes[0]], false).unwrap(),
            "USERID = 2".to_string()
        );
        assert!(
            rewrite_fragment_case("nope = 2", &[scopes[0]], true)
                .unwrap()
                .contains("nope")
        );
    }

    #[test]
    fn fragment_rewrite_skips_subqueries_and_flags_collisions() {
        let target = vec!["userId".to_string()];
        let source = vec!["USERID".to_string(), "userId".to_string()];
        let scopes = [("t", target.as_slice()), ("s", source.as_slice())];
        assert_eq!(
            rewrite_fragment_case("x IN (SELECT USERID FROM other)", &[scopes[0]], true).unwrap(),
            "x IN (SELECT USERID FROM other)".to_string()
        );
        let error = rewrite_fragment_case("USERID = s.USERID", &scopes, true).unwrap_err();
        assert!(error.to_string().contains("[AMBIGUOUS_REFERENCE]"));
    }

    #[tokio::test]
    async fn missing_column_stays_missing() {
        let state = mixed_state(true);
        let error = plan_error(&state, "SELECT nope FROM t").await;
        assert!(error.contains("nope"), "unexpected message: {error}");
    }
}
