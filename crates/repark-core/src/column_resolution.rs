use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Column, SchemaError, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
#[cfg(test)]
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Expr as SqlExpr, Ident, ObjectNamePart, Statement, Value, VisitMut, VisitorMut,
};
use repark_common::spark_error;

#[allow(clippy::missing_errors_doc)]
pub async fn plan_statement_with_column_repair(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
    case_insensitive: bool,
) -> Result<LogicalPlan> {
    let bytes = stack::stack_bytes_for(&statement);
    stack::on_grown_stack(bytes, plan_with_repair(state, statement, case_insensitive)).await
}

async fn finish_with_display(
    state: &SessionState,
    original: Box<Statement>,
    folded: Box<Statement>,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut respelled = folded.clone();
    let (folded, plan) = if inner_scopes::respell_inner_scopes(&original, &mut respelled) {
        let statement = datafusion::sql::parser::Statement::Statement(respelled.clone());
        match Box::pin(plan_with_repair(state, statement, true)).await {
            Ok(replanned) => (respelled, replanned),
            Err(_) => (folded, plan),
        }
    } else {
        (folded, plan)
    };
    let planned = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let Some(rewritten) = display::display_rewrite(&original, &folded, &planned) else {
        return display::keep_ref_qualifiers(&original, plan);
    };
    let replanned = Box::pin(plan_with_repair(
        state,
        datafusion::sql::parser::Statement::Statement(Box::new(rewritten)),
        true,
    ))
    .await?;
    display::keep_ref_qualifiers(&original, replanned)
}

fn boxed_finish(
    state: &SessionState,
    original: Box<Statement>,
    folded: Box<Statement>,
    plan: LogicalPlan,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<LogicalPlan>> + '_>> {
    Box::pin(finish_with_display(state, original, folded, plan))
}

fn boxed_case_sensitive(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<LogicalPlan>> + '_>> {
    Box::pin(plan_case_sensitive(state, statement))
}

async fn plan_case_sensitive(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
) -> Result<LogicalPlan> {
    if let datafusion::sql::parser::Statement::Statement(inner) = &statement {
        strict_case_guard(state, inner).await?;
    }
    let mut exact = state.clone();
    exact
        .config_mut()
        .options_mut()
        .sql_parser
        .enable_ident_normalization = false;
    exact
        .statement_to_plan(statement)
        .await
        .map_err(stamp_unresolved_column)
}

async fn strict_case_guard(state: &SessionState, inner: &Statement) -> Result<()> {
    if !display::should_strict_check(inner) {
        return Ok(());
    }
    let Some((name, alias)) = display::strict_single_table(inner) else {
        return Ok(());
    };
    let Some(parts) = fold::normalized_parts(&name) else {
        return Ok(());
    };
    let Some(fields) = catalog_fields(state, inner).await.get(&parts).cloned() else {
        return Ok(());
    };
    let relation = alias.as_deref().unwrap_or_else(|| {
        name.0
            .last()
            .and_then(|part| part_value(part))
            .unwrap_or_default()
    });
    display::strict_case_check(&fields, relation, inner)
}

async fn plan_with_repair(
    state: &SessionState,
    statement: datafusion::sql::parser::Statement,
    case_insensitive: bool,
) -> Result<LogicalPlan> {
    if !case_insensitive {
        return boxed_case_sensitive(state, statement).await;
    }
    let datafusion::sql::parser::Statement::Statement(mut inner) = statement else {
        return state
            .statement_to_plan(statement)
            .await
            .map_err(stamp_unresolved_column);
    };
    let original = inner.clone();
    let mut first = state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
        .await;
    let twin = matches!(&first, Err(error) if twins::is_unique_name_error(error));
    if twin && twins::respell_case_twins(&mut inner) {
        first = state
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
            .await;
    }
    let catalog_options = &state.config().options().catalog;
    let defaults = [
        catalog_options.default_catalog.clone(),
        catalog_options.default_schema.clone(),
    ];
    let mut error = match first {
        Ok(plan) => {
            if plan_has_upper_ascii_field(&plan) {
                ambiguity::audit_plan_for_ambiguity(&plan, &written_references(&inner, defaults))?;
            }
            return boxed_finish(state, original, inner, plan).await;
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
            return Err(stamp_unresolved_column(error));
        }
        let Some((field, valid)) = missing_field(&error) else {
            return Err(stamp_unresolved_column(error));
        };
        let miss = (
            field.relation.as_ref().map(ToString::to_string),
            field.name.clone(),
        );
        if !seen.insert(miss) {
            return Err(stamp_unresolved_column(error));
        }
        let catalog = match known.take() {
            Some(catalog) => catalog,
            None => fold::Known::with_tables(catalog_fields(state, &inner).await),
        };
        let catalog = known.insert(catalog);
        catalog.absorb(valid);
        if !fold::fold_statement(&mut inner, catalog, &written)? {
            return Err(stamp_unresolved_column(error));
        }
        match state
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(inner.clone()))
            .await
        {
            Ok(plan) => {
                ambiguity::audit_plan_for_ambiguity(&plan, &written)?;
                return boxed_finish(state, original, inner, plan).await;
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

fn stamp_unresolved_column(error: DataFusionError) -> DataFusionError {
    let Some((field, valid)) = missing_field(&error) else {
        return error;
    };
    if valid.is_empty() {
        return error;
    }
    let missing = field.name.as_str();
    let column_name = match field.relation.as_ref() {
        Some(relation) => quoted(&reference_parts(relation), missing),
        None => format!("`{missing}`"),
    };
    let shown = valid
        .iter()
        .filter(|column| !crate::frame_names::is_scratch_relation(&column.name))
        .collect::<Vec<_>>();
    if shown.is_empty() {
        return error;
    }
    let suggestions = shown
        .iter()
        .map(|column| {
            let candidate = column.name.as_str();
            format!("`{candidate}`")
        })
        .collect::<Vec<String>>()
        .join(", ");
    DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", column_name.as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    ))
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
    outer_bare: HashSet<String>,
    outer_qualified: HashSet<(String, String)>,
    projection: HashSet<String>,
    relations: Vec<(String, Vec<String>)>,
    views: HashSet<String>,
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

#[derive(Default)]
struct WrittenRefsCollector {
    bare: HashSet<String>,
    qualified: HashSet<(String, String)>,
    outer_bare: HashSet<String>,
    outer_qualified: HashSet<(String, String)>,
    projection: HashSet<String>,
    relations: Vec<(String, Vec<String>)>,
    ctes: HashSet<String>,
    named: Vec<(String, String)>,
    scoped: usize,
    barriers: Vec<usize>,
    cte_bodies: HashSet<*const datafusion::sql::sqlparser::ast::Query>,
}

impl WrittenRefsCollector {
    fn outer(&self) -> bool {
        self.scoped <= self.barriers.last().copied().unwrap_or(0)
    }
}

impl datafusion::sql::sqlparser::ast::Visitor for WrittenRefsCollector {
    type Break = std::convert::Infallible;
    fn pre_visit_query(
        &mut self,
        query: &datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        for cte in query.with.iter().flat_map(|with| &with.cte_tables) {
            self.ctes.insert(cte.alias.name.value.to_ascii_lowercase());
            self.cte_bodies
                .insert(std::ptr::from_ref(cte.query.as_ref()));
        }
        if self.cte_bodies.contains(&std::ptr::from_ref(query)) {
            self.scoped += 1;
        }
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
    fn post_visit_query(
        &mut self,
        query: &datafusion::sql::sqlparser::ast::Query,
    ) -> ControlFlow<Self::Break> {
        if self.cte_bodies.contains(&std::ptr::from_ref(query)) {
            self.scoped = self.scoped.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(
        &mut self,
        factor: &datafusion::sql::sqlparser::ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        if matches!(
            factor,
            datafusion::sql::sqlparser::ast::TableFactor::Derived { .. }
        ) {
            self.scoped += 1;
        }
        if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, alias, .. } = factor {
            let written = name
                .0
                .iter()
                .filter_map(|part| part_value(part).map(str::to_string))
                .collect::<Vec<_>>();
            self.named.push((
                written.last().cloned().unwrap_or_default(),
                alias.as_ref().map_or_else(
                    || written.last().cloned().unwrap_or_default(),
                    |alias| alias.name.value.clone(),
                ),
            ));
            let entry = match alias {
                Some(alias) => (alias.name.value.clone(), vec![alias.name.value.clone()]),
                None => (written.last().cloned().unwrap_or_default(), written),
            };
            self.relations.push(entry);
        }
        ControlFlow::Continue(())
    }
    fn post_visit_table_factor(
        &mut self,
        factor: &datafusion::sql::sqlparser::ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        if matches!(
            factor,
            datafusion::sql::sqlparser::ast::TableFactor::Derived { .. }
        ) {
            self.scoped = self.scoped.saturating_sub(1);
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if inner_scopes::is_expression_subquery(expr) {
            self.barriers.push(self.scoped);
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        if inner_scopes::is_expression_subquery(expr) {
            self.barriers.pop();
        }
        let outer = self.outer();
        match expr {
            SqlExpr::Identifier(ident) => {
                self.bare.insert(ident.value.clone());
                if outer {
                    self.outer_bare.insert(ident.value.clone());
                }
            }
            SqlExpr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let name = parts[parts.len() - 1].value.clone();
                let qualifier = parts[parts.len() - 2].value.clone();
                self.qualified.insert((qualifier.clone(), name.clone()));
                if outer {
                    self.outer_qualified.insert((qualifier, name));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn written_references(statement: &Statement, defaults: [String; 2]) -> WrittenRefs {
    let mut collector = WrittenRefsCollector::default();
    let _ = datafusion::sql::sqlparser::ast::Visit::visit(statement, &mut collector);
    WrittenRefs {
        bare: collector.bare,
        qualified: collector.qualified,
        outer_bare: collector.outer_bare,
        outer_qualified: collector.outer_qualified,
        projection: collector.projection,
        relations: collector.relations,
        views: collector
            .named
            .into_iter()
            .filter(|(written, _)| {
                !written.is_empty() && !collector.ctes.contains(&written.to_ascii_lowercase())
            })
            .map(|(_, visible)| visible.to_ascii_lowercase())
            .collect(),
        defaults,
    }
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
    spark_error::message(
        spark_error::AMBIGUOUS_REFERENCE,
        &[("reference", &reference), ("options", &options)],
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
    Ok(repark_iceberg::write::sql_text::render_for_reparse(
        &mut expr,
    ))
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

mod ambiguity;
mod display;
mod fold;
mod fold_text;
mod inner_scopes;
mod scope_fields;
mod stack;
mod twins;

pub use fold_text::fold_query_text;
pub use stack::{GrownStack, on_grown_stack_with, remaining_stack, run_on_grown_stack};

#[cfg(test)]
mod tests;
