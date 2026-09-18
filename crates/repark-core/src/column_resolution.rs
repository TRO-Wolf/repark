use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, SchemaError, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Ident, ObjectNamePart, Statement, Value, VisitMut, VisitorMut,
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
    let first = state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
        .await;
    let catalog_options = &state.config().options().catalog;
    let defaults = [
        catalog_options.default_catalog.clone(),
        catalog_options.default_schema.clone(),
    ];
    let mut error = match first {
        Ok(plan) => {
            if plan_has_upper_ascii_field(&plan) {
                audit_plan_for_ambiguity(&plan, &written_references(&inner, defaults))?;
            }
            return Ok(plan);
        }
        Err(error) => error,
    };
    let written = written_references(&inner, defaults);
    let mut known: Option<fold::Known> = None;
    let mut seen: HashSet<(Option<String>, String)> = HashSet::new();
    loop {
        if let Some(field) = missing_ambiguity(&error) {
            if let Some(spark) =
                spark_ambiguous_for_unresolved(state, &inner, &written, field).await
            {
                return Err(spark);
            }
            return Err(error);
        }
        let Some((field, valid)) = missing_field(&error) else {
            return Err(error);
        };
        let miss = (
            field.relation.as_ref().map(ToString::to_string),
            field.name.clone(),
        );
        if !seen.insert(miss) {
            return Err(error);
        }
        let catalog = match known.take() {
            Some(catalog) => catalog,
            None => fold::Known::with_tables(catalog_fields(state, &inner).await),
        };
        let catalog = known.insert(catalog);
        catalog.absorb(valid);
        if !fold::fold_statement(&mut inner, catalog, &written)? {
            return Err(error);
        }
        match state
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
            .await
        {
            Ok(plan) => {
                audit_plan_for_ambiguity(&plan, &written)?;
                return Ok(plan);
            }
            Err(next) => error = next,
        }
    }
}

async fn catalog_fields(
    state: &SessionState,
    statement: &Statement,
) -> HashMap<Vec<String>, Vec<String>> {
    use datafusion::sql::sqlparser::ast::{ObjectName, Visit, Visitor};
    struct Relations {
        names: Vec<Vec<String>>,
    }
    impl Visitor for Relations {
        type Break = std::convert::Infallible;
        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
            if let Some(parts) = fold::normalized_parts(relation)
                && !self.names.contains(&parts)
            {
                self.names.push(parts);
            }
            ControlFlow::Continue(())
        }
    }
    let mut relations = Relations { names: Vec::new() };
    let _ = statement.visit(&mut relations);
    let mut tables = HashMap::new();
    for parts in relations.names {
        let reference = match parts.as_slice() {
            [table] => TableReference::bare(table.clone()),
            [schema, table] => TableReference::partial(schema.clone(), table.clone()),
            [catalog, schema, table] => {
                TableReference::full(catalog.clone(), schema.clone(), table.clone())
            }
            _ => continue,
        };
        let Ok(provider) = state.schema_for_ref(reference.clone()) else {
            continue;
        };
        let Ok(Some(table)) = provider.table(reference.table()).await else {
            continue;
        };
        let fields = table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        tables.insert(parts, fields);
    }
    tables
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

fn missing_field(error: &DataFusionError) -> Option<(&Column, &[Column])> {
    match error {
        DataFusionError::SchemaError(inner, _) => match inner.as_ref() {
            SchemaError::FieldNotFound {
                field,
                valid_fields,
            } => Some((field.as_ref(), valid_fields.as_slice())),
            _ => None,
        },
        DataFusionError::Diagnostic(_, inner) => missing_field(inner),
        DataFusionError::Collection(errors) => errors.iter().find_map(missing_field),
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
    relations: Vec<(String, Vec<String>)>,
    defaults: [String; 2],
}

impl WrittenRefs {
    fn relation_parts(&self, reference: &TableReference) -> Vec<String> {
        let parts = self
            .relations
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(reference.table()))
            .map_or_else(|| reference_parts(reference), |(_, parts)| parts.clone());
        self.visible(parts)
    }

    fn visible(&self, parts: Vec<String>) -> Vec<String> {
        match parts.as_slice() {
            [catalog, schema, table]
                if catalog.eq_ignore_ascii_case(&self.defaults[0])
                    && schema.eq_ignore_ascii_case(&self.defaults[1]) =>
            {
                vec![table.clone()]
            }
            _ => parts,
        }
    }
}

fn written_references(statement: &Statement, defaults: [String; 2]) -> WrittenRefs {
    struct Collector {
        bare: HashSet<String>,
        qualified: HashSet<(String, String)>,
        projection: HashSet<String>,
        relations: Vec<(String, Vec<String>)>,
    }
    impl datafusion::sql::sqlparser::ast::Visitor for Collector {
        type Break = std::convert::Infallible;
        fn pre_visit_query(
            &mut self,
            query: &datafusion::sql::sqlparser::ast::Query,
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
        fn pre_visit_table_factor(
            &mut self,
            factor: &datafusion::sql::sqlparser::ast::TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, alias, .. } = factor
            {
                let written = name
                    .0
                    .iter()
                    .filter_map(|part| part_value(part).map(str::to_string))
                    .collect::<Vec<_>>();
                let entry = match alias {
                    Some(alias) => (alias.name.value.clone(), vec![alias.name.value.clone()]),
                    None => (written.last().cloned().unwrap_or_default(), written),
                };
                self.relations.push(entry);
            }
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
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
        relations: Vec::new(),
    };
    let _ = datafusion::sql::sqlparser::ast::Visit::visit(statement, &mut collector);
    WrittenRefs {
        bare: collector.bare,
        qualified: collector.qualified,
        projection: collector.projection,
        relations: collector.relations,
        defaults,
    }
}

type Twins<'a> = HashMap<String, Vec<(Option<&'a TableReference>, &'a str)>>;

fn audit_plan_for_ambiguity(plan: &LogicalPlan, written: &WrittenRefs) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        let twins = input_twins(node);
        if twins.is_empty() {
            return Ok(TreeNodeRecursion::Continue);
        }
        for expr in node.expressions() {
            expr.apply(|leaf| {
                if let Expr::Column(column) = leaf {
                    audit_column(column, &twins, written)?;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

fn plan_has_upper_ascii_field(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        found = node
            .schema()
            .fields()
            .iter()
            .any(|field| has_upper_ascii(field.name()))
            || node.inputs().iter().any(|input| {
                input
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| has_upper_ascii(field.name()))
            });
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

fn has_upper_ascii(name: &str) -> bool {
    name.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn input_twins(node: &LogicalPlan) -> Twins<'_> {
    let inputs = node.inputs();
    let mut index: Twins<'_> = HashMap::new();
    if !inputs.iter().any(|input| {
        input
            .schema()
            .fields()
            .iter()
            .any(|field| has_upper_ascii(field.name()))
    }) {
        return index;
    }
    for input in inputs {
        for (qualifier, field) in input.schema().iter() {
            index
                .entry(field.name().to_ascii_lowercase())
                .or_default()
                .push((qualifier, field.name().as_str()));
        }
    }
    index.retain(|_, fields| fields.iter().any(|(_, name)| *name != fields[0].1));
    index
}

fn audit_column(column: &Column, twins: &Twins<'_>, written: &WrittenRefs) -> Result<()> {
    let Some(fields) = twins.get(&column.name.to_ascii_lowercase()) else {
        return Ok(());
    };
    let qualified = column.relation.as_ref().and_then(|relation| {
        written.qualified.iter().find(|(qualifier, name)| {
            qualifier.eq_ignore_ascii_case(relation.table())
                && name.eq_ignore_ascii_case(&column.name)
        })
    });
    let bare = written
        .bare
        .iter()
        .find(|name| name.eq_ignore_ascii_case(&column.name));
    let (qualifier, requested) = match (qualified, bare) {
        (Some((qualifier, name)), _) => (Some(qualifier.as_str()), name.as_str()),
        (None, Some(name)) => (None, name.as_str()),
        (None, None) => return Ok(()),
    };
    let matching = fields
        .iter()
        .filter(
            |(candidate, _)| match (qualifier, column.relation.as_ref()) {
                (Some(_), Some(relation)) => {
                    candidate.is_some_and(|candidate| qualifier_matches(relation, candidate))
                }
                _ => true,
            },
        )
        .collect::<Vec<_>>();
    if matching.len() < 2 || matching.iter().all(|(_, name)| *name == matching[0].1) {
        return Ok(());
    }
    let scopes = matching
        .iter()
        .map(|(candidate, _)| {
            candidate
                .map(|candidate| written.relation_parts(candidate))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    Err(DataFusionError::Plan(ambiguous_message(
        qualifier, requested, &scopes,
    )))
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

fn reference_parts(reference: &TableReference) -> Vec<String> {
    match reference {
        TableReference::Bare { table } => vec![table.to_string()],
        TableReference::Partial { schema, table } => vec![schema.to_string(), table.to_string()],
        TableReference::Full {
            catalog,
            schema,
            table,
        } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
    }
}

fn quoted(parts: &[String], requested: &str) -> String {
    parts
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(requested))
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

fn ambiguous_message(qualifier: Option<&str>, requested: &str, scopes: &[Vec<String>]) -> String {
    let reference = match qualifier {
        Some(scope) => quoted(&[scope.to_string()], requested),
        None => quoted(&[], requested),
    };
    let options = scopes
        .iter()
        .map(|scope| quoted(scope, requested))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: [{options}]. SQLSTATE: 42704"
    )
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
                .map(|(scope, _)| vec![scope.clone()])
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
    let scopes = options
        .into_iter()
        .map(|(scope, _)| scope.into_iter().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    Some(DataFusionError::Plan(ambiguous_message(
        qualifier.as_deref(),
        spelling.as_str(),
        &scopes,
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

mod fold;

#[cfg(test)]
mod tests;
