use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

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

#[allow(clippy::missing_errors_doc)]
pub async fn plan_statement_with_column_repair(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
    case_insensitive: bool,
) -> Result<LogicalPlan> {
    if !case_insensitive {
        return state.statement_to_plan(statement).await;
    }
    let datafusion::sql::parser::Statement::Statement(mut inner) = statement else {
        return state.statement_to_plan(statement).await;
    };
    let written = written_references(&inner);
    match state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
        .await
    {
        Ok(plan) => {
            audit_plan_for_ambiguity(&plan, &written)?;
            Ok(plan)
        }
        Err(error) => {
            if let Some(field) = missing_ambiguity(&error) {
                if let Some(spark) =
                    spark_ambiguous_for_unresolved(state, &inner, &written, field).await
                {
                    return Err(spark);
                }
                return Err(error);
            }
            let Some(valid) = missing_fields(&error) else {
                return Err(error);
            };
            if !fold_statement(&mut inner, valid, &written)? {
                return Err(error);
            }
            let plan = state
                .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner))
                .await?;
            audit_plan_for_ambiguity(&plan, &written)?;
            Ok(plan)
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn sql_with_column_repair(
    ctx: &SessionContext,
    sql: &str,
    case_insensitive: bool,
) -> Result<DataFrame> {
    let dialect = ctx.state().config().options().sql_parser.dialect;
    let statement = ctx.state().sql_to_statement(sql, &dialect)?;
    let plan = plan_statement_with_column_repair(&ctx.state(), statement, case_insensitive).await?;
    ctx.execute_logical_plan(plan).await
}

fn missing_fields(error: &DataFusionError) -> Option<&[Column]> {
    match error {
        DataFusionError::SchemaError(inner, _) => match inner.as_ref() {
            SchemaError::FieldNotFound { valid_fields, .. } => Some(valid_fields),
            _ => None,
        },
        DataFusionError::Diagnostic(_, inner) => missing_fields(inner),
        DataFusionError::Collection(errors) => errors.iter().find_map(missing_fields),
        _ => None,
    }
}

fn missing_ambiguity(error: &DataFusionError) -> Option<&Column> {
    match error {
        DataFusionError::SchemaError(inner, _) => match inner.as_ref() {
            SchemaError::AmbiguousReference { field } => Some(field),
            _ => None,
        },
        DataFusionError::Diagnostic(_, inner) => missing_ambiguity(inner),
        DataFusionError::Collection(errors) => errors.iter().find_map(missing_ambiguity),
        _ => None,
    }
}

struct WrittenRefs {
    bare: HashSet<String>,
    qualified: HashSet<(String, String)>,
    projection: HashSet<String>,
}

fn written_references(statement: &Statement) -> WrittenRefs {
    struct Collector {
        bare: HashSet<String>,
        qualified: HashSet<(String, String)>,
        projection: HashSet<String>,
    }
    impl VisitorMut for Collector {
        type Break = std::convert::Infallible;
        fn pre_visit_query(
            &mut self,
            query: &mut datafusion::sql::sqlparser::ast::Query,
        ) -> ControlFlow<Self::Break> {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                for item in &select.projection {
                    match item {
                        datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(
                            SqlExpr::Identifier(ident),
                        )
                        | datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias {
                            expr: SqlExpr::Identifier(ident),
                            ..
                        } => {
                            self.projection.insert(ident.value.clone());
                        }
                        _ => {}
                    }
                }
            }
            ControlFlow::Continue(())
        }
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
        projection: HashSet::new(),
    };
    let mut owned = statement.clone();
    let _ = owned.visit(&mut collector);
    WrittenRefs {
        bare: collector.bare,
        qualified: collector.qualified,
        projection: collector.projection,
    }
}

fn audit_plan_for_ambiguity(plan: &LogicalPlan, written: &WrittenRefs) -> Result<()> {
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

fn audit_column(column: &Column, schema: &DFSchema, written: &WrittenRefs) -> Result<()> {
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
    if column.relation.is_some() && honored.len() > 1 {
        let requested = written
            .qualified
            .iter()
            .find(|(qualifier, name)| {
                column
                    .relation
                    .as_ref()
                    .is_some_and(|relation| qualifier.eq_ignore_ascii_case(relation.table()))
                    && name.eq_ignore_ascii_case(&column.name)
            })
            .map(|(qualifier, name)| (Some(qualifier.as_str()), name.as_str()));
        if let Some((qualifier, name)) = requested {
            let candidates = honored
                .iter()
                .map(|valid| {
                    (
                        valid
                            .relation
                            .as_ref()
                            .map(|relation| relation.table().to_string()),
                        valid.name.clone(),
                    )
                })
                .collect::<Vec<_>>();
            return Err(DataFusionError::Plan(ambiguous_message(
                qualifier,
                name,
                &candidates,
            )));
        }
    }
    let mut spellings = HashSet::new();
    for valid in &unqualified {
        spellings.insert(valid.name.as_str());
    }
    let refers = written
        .bare
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&column.name));
    if spellings.len() > 1 && refers {
        let requested = written
            .bare
            .iter()
            .find(|name| name.eq_ignore_ascii_case(&column.name))
            .map_or(column.name.as_str(), String::as_str);
        let candidates = unqualified
            .iter()
            .map(|valid| {
                (
                    valid
                        .relation
                        .as_ref()
                        .map(|relation| relation.table().to_string()),
                    valid.name.clone(),
                )
            })
            .collect::<Vec<_>>();
        return Err(DataFusionError::Plan(ambiguous_message(
            None,
            requested,
            &candidates,
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

fn ambiguous_message(
    qualifier: Option<&str>,
    requested: &str,
    candidates: &[(Option<String>, String)],
) -> String {
    let reference = match qualifier {
        Some(scope) => format!("`{scope}`.`{requested}`"),
        None => format!("`{requested}`"),
    };
    let mut seen: HashSet<Option<&str>> = HashSet::new();
    let mut options: Vec<String> = Vec::new();
    for (scope, _) in candidates {
        if !seen.insert(scope.as_deref()) {
            continue;
        }
        match scope {
            Some(own) => options.push(format!("`{own}`.`{requested}`")),
            None => options.push(format!("`{requested}`")),
        }
    }
    let options = options.join(", ");
    format!(
        "[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: [{options}]. SQLSTATE: 42702"
    )
}

struct CaseFold<'a> {
    scopes: HashMap<String, Vec<(Option<String>, String)>>,
    aliases: Vec<HashSet<String>>,
    written: &'a WrittenRefs,
    changed: bool,
    error: Option<DataFusionError>,
}

impl<'a> CaseFold<'a> {
    fn new(valid_fields: &[Column], written: &'a WrittenRefs) -> Self {
        let mut scopes: HashMap<String, Vec<(Option<String>, String)>> = HashMap::new();
        for candidate in valid_fields {
            scopes
                .entry(candidate.name.to_ascii_lowercase())
                .or_default()
                .push((
                    candidate.relation.as_ref().map(ToString::to_string),
                    candidate.name.clone(),
                ));
        }
        Self {
            scopes,
            aliases: Vec::new(),
            written,
            changed: false,
            error: None,
        }
    }

    fn candidates(&self, qualifier: Option<&str>, name: &str) -> Vec<(Option<String>, String)> {
        let Some(entries) = self.scopes.get(&name.to_ascii_lowercase()) else {
            return Vec::new();
        };
        let mut found: Vec<(Option<String>, String)> = Vec::new();
        for (scope, stored) in entries {
            match (qualifier, scope) {
                (None, _) => {}
                (Some(written), Some(candidate)) => {
                    if !written.eq_ignore_ascii_case(candidate) {
                        continue;
                    }
                }
                (Some(_), None) => continue,
            }
            if !found.contains(&(scope.clone(), stored.clone())) {
                found.push((scope.clone(), stored.clone()));
            }
        }
        found
    }

    fn alias_names(&self, ident: &str) -> bool {
        self.aliases
            .iter()
            .any(|level| level.iter().any(|alias| alias == ident))
    }

    fn requested_spelling(&self, qualifier: Option<&str>, name: &str) -> (Option<String>, String) {
        if let Some(scope) = qualifier {
            return self
                .written
                .qualified
                .iter()
                .find(|(written_scope, written_name)| {
                    written_scope.eq_ignore_ascii_case(scope)
                        && written_name.eq_ignore_ascii_case(name)
                })
                .map(|(written_scope, written_name)| {
                    (Some(written_scope.clone()), written_name.clone())
                })
                .unwrap_or((Some(scope.to_string()), name.to_string()));
        }
        let spelling = self
            .written
            .projection
            .iter()
            .find(|written| written.eq_ignore_ascii_case(name))
            .cloned()
            .or_else(|| {
                self.written
                    .bare
                    .iter()
                    .find(|written| written.eq_ignore_ascii_case(name))
                    .cloned()
            })
            .unwrap_or_else(|| name.to_string());
        (None, spelling)
    }

    fn rewrite_ident(&mut self, qualifier: Option<&str>, ident: &mut Ident) {
        if self.error.is_some() || self.alias_names(ident.value.as_str()) {
            return;
        }
        let candidates = self.candidates(qualifier, ident.value.as_str());
        if candidates.len() > 1 {
            let (scope, spelling) = self.requested_spelling(qualifier, ident.value.as_str());
            self.error = Some(DataFusionError::Plan(ambiguous_message(
                scope.as_deref(),
                spelling.as_str(),
                candidates.as_slice(),
            )));
            return;
        }
        let Some((_, stored)) = candidates.into_iter().next() else {
            return;
        };
        let current = if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        };
        if stored != current {
            ident.value = stored;
            ident.quote_style = Some('`');
            self.changed = true;
        }
    }
}

impl VisitorMut for CaseFold<'_> {
    type Break = std::convert::Infallible;

    fn pre_visit_query(
        &mut self,
        query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        let mut level: HashSet<String> = HashSet::new();
        if let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
            for item in &select.projection {
                if let datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias {
                    alias, ..
                } = item
                {
                    level.insert(alias.value.clone());
                }
            }
        }
        self.aliases.push(level);
        ControlFlow::Continue(())
    }

    fn post_visit_query(
        &mut self,
        _query: &mut datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        self.aliases.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if self.error.is_some() {
            return ControlFlow::Continue(());
        }
        match expr {
            SqlExpr::Identifier(ident) => {
                self.rewrite_ident(None, ident);
            }
            SqlExpr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualifier = parts[parts.len() - 2].value.clone();
                let last = parts.len() - 1;
                self.rewrite_ident(Some(qualifier.as_str()), &mut parts[last]);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        if self.error.is_some() {
            return ControlFlow::Continue(());
        }
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
    unqualified_scope: Option<&str>,
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
        unqualified_scope,
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
    unqualified_scope: Option<&'a str>,
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
            if qualifier.is_none()
                && let Some(home) = self.unqualified_scope
                && !alias.eq_ignore_ascii_case(home)
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
            let candidates = matches
                .iter()
                .map(|(scope, field)| (Some(scope.clone()), field.clone()))
                .collect::<Vec<_>>();
            self.error = Some(DataFusionError::Plan(ambiguous_message(
                qualifier,
                ident.value.as_str(),
                candidates.as_slice(),
            )));
            return;
        }
        let (_, stored) = matches.remove(0);
        let current = if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        };
        if stored != current {
            ident.value = stored;
            ident.quote_style = Some('`');
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
    fold: &mut CaseFold,
) {
    if name.0.is_empty() {
        return;
    }
    let last = name.0.len() - 1;
    let qualifier = if last > 0 {
        part_value(&name.0[last - 1]).map(str::to_string)
    } else {
        None
    };
    let ObjectNamePart::Identifier(ident) = &mut name.0[last] else {
        return;
    };
    fold.rewrite_ident(qualifier.as_deref(), ident);
}

fn rewrite_join_usings(query: &mut datafusion::sql::sqlparser::ast::Query, fold: &mut CaseFold) {
    use datafusion::sql::sqlparser::ast::{JoinConstraint, JoinOperator, SetExpr, TableFactor};
    fn tables(factor: &mut TableFactor, fold: &mut CaseFold) {
        if let TableFactor::Derived { subquery, .. } = factor {
            rewrite_join_usings(subquery, fold);
        }
    }
    fn constraint(operator: &mut JoinOperator) -> Option<&mut JoinConstraint> {
        match operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::Left(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::Right(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint)
            | JoinOperator::CrossJoin(constraint)
            | JoinOperator::Semi(constraint)
            | JoinOperator::LeftSemi(constraint)
            | JoinOperator::RightSemi(constraint)
            | JoinOperator::Anti(constraint)
            | JoinOperator::LeftAnti(constraint)
            | JoinOperator::RightAnti(constraint) => Some(constraint),
            _ => None,
        }
    }
    fn walk_body(body: &mut SetExpr, fold: &mut CaseFold) {
        match body {
            SetExpr::Select(select) => {
                for from in &mut select.from {
                    tables(&mut from.relation, fold);
                    for join in &mut from.joins {
                        tables(&mut join.relation, fold);
                        if let Some(JoinConstraint::Using(columns)) =
                            constraint(&mut join.join_operator)
                        {
                            for column in columns {
                                if let [part] = column.0.as_mut_slice()
                                    && let ObjectNamePart::Identifier(ident) = part
                                {
                                    fold.rewrite_ident(None, ident);
                                }
                                if fold.error.is_some() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            SetExpr::Query(query) => rewrite_join_usings(query, fold),
            SetExpr::SetOperation { left, right, .. } => {
                walk_body(left, fold);
                walk_body(right, fold);
            }
            _ => {}
        }
    }
    walk_body(&mut query.body, fold);
    if let Some(with) = query.with.as_mut() {
        for table in &mut with.cte_tables {
            rewrite_join_usings(&mut table.query, fold);
        }
    }
}

async fn spark_ambiguous_for_unresolved(
    state: &SessionState,
    statement: &Statement,
    written: &WrittenRefs,
    field: &Column,
) -> Option<DataFusionError> {
    let mut options: Vec<(Option<String>, String)> = Vec::new();
    let mut tables = direct_tables(statement);
    tables.sort_by(|first, second| first.0.cmp(&second.0));
    tables.dedup();
    for (display, reference) in tables {
        let Ok(provider) = state.schema_for_ref(reference.clone()) else {
            continue;
        };
        let Ok(Some(table)) = provider.table(reference.table()).await else {
            continue;
        };
        for candidate in table.schema().fields() {
            if !candidate.name().eq_ignore_ascii_case(&field.name) {
                continue;
            }
            if let Some(qualifier) = field.relation.as_ref()
                && !display.eq_ignore_ascii_case(qualifier.table())
            {
                continue;
            }
            if !options.iter().any(|(scope, stored)| {
                scope.as_deref() == Some(display.as_str()) && stored == candidate.name()
            }) {
                options.push((Some(display.clone()), candidate.name().clone()));
            }
        }
    }
    if options.len() < 2 {
        return None;
    }
    let requested = written
        .projection
        .iter()
        .find(|spelling| spelling.eq_ignore_ascii_case(&field.name))
        .cloned()
        .or_else(|| {
            written
                .bare
                .iter()
                .find(|spelling| spelling.eq_ignore_ascii_case(&field.name))
                .cloned()
        })
        .unwrap_or_else(|| field.name.clone());
    let qualifier = field
        .relation
        .as_ref()
        .map(|relation| relation.table().to_string());
    let spelling = qualifier
        .as_ref()
        .and_then(|scope| {
            written
                .qualified
                .iter()
                .find(|(written_scope, written_name)| {
                    written_scope.eq_ignore_ascii_case(scope)
                        && written_name.eq_ignore_ascii_case(&field.name)
                })
                .map(|(_, written_name)| written_name.clone())
        })
        .unwrap_or(requested);
    Some(DataFusionError::Plan(ambiguous_message(
        qualifier.as_deref(),
        spelling.as_str(),
        options.as_slice(),
    )))
}

fn direct_tables(statement: &Statement) -> Vec<(String, TableReference)> {
    use datafusion::sql::sqlparser::ast::{TableFactor, Visit, Visitor};
    struct Collector {
        tables: Vec<(String, TableReference)>,
    }
    impl Visitor for Collector {
        type Break = std::convert::Infallible;
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, alias, .. } = factor {
                let mut parts: Vec<String> = Vec::new();
                for part in &name.0 {
                    let Some(value) = part_value(part) else {
                        return ControlFlow::Continue(());
                    };
                    parts.push(value.to_string());
                }
                let reference = match parts.len() {
                    1 => TableReference::bare(parts[0].clone()),
                    2 => TableReference::partial(parts[0].clone(), parts[1].clone()),
                    3 => TableReference::full(parts[0].clone(), parts[1].clone(), parts[2].clone()),
                    _ => return ControlFlow::Continue(()),
                };
                let display = alias.as_ref().map_or_else(
                    || parts[parts.len() - 1].clone(),
                    |alias| alias.name.value.clone(),
                );
                self.tables.push((display, reference));
            }
            ControlFlow::Continue(())
        }
    }
    let mut collector = Collector { tables: Vec::new() };
    let _ = statement.visit(&mut collector);
    collector.tables
}

fn fold_statement(
    statement: &mut Statement,
    valid_fields: &[Column],
    written: &WrittenRefs,
) -> Result<bool> {
    let mut fold = CaseFold::new(valid_fields, written);
    let _ = statement.visit(&mut fold);
    if let Statement::Query(query) = statement {
        rewrite_join_usings(query, &mut fold);
    }
    if let Some(error) = fold.error {
        return Err(error);
    }
    Ok(fold.changed)
}

#[cfg(test)]
mod tests;
