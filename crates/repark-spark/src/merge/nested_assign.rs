use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, MergeAction, MergeClause, MergeInsertExpr,
    MergeInsertKind, ObjectName, ObjectNamePart, TableFactor,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::update_cast::{incompatible_update_message, store_assignment_cast_sql};

mod expand;
mod render;
#[cfg(test)]
mod tests;

pub(crate) struct AssignmentScope {
    pub(crate) qualifiers: Vec<Vec<String>>,
    pub(crate) sql_qualifier: String,
    pub(crate) column_prefix: Option<String>,
    pub(crate) value_qualifiers: Vec<Vec<String>>,
    pub(crate) probe_from: String,
    pub(crate) case_sensitive: bool,
}

pub(crate) fn name_suffixes(parts: &[String]) -> Vec<Vec<String>> {
    (0..parts.len())
        .map(|start| parts[start..].to_vec())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Field(String),
    MapValue(String),
}

#[derive(Debug, Clone)]
struct ResolvedKey {
    column: String,
    steps: Vec<Step>,
}

impl ResolvedKey {
    fn pretty(&self) -> String {
        let mut out = self.column.clone();
        for step in &self.steps {
            match step {
                Step::Field(name) => {
                    out.push('.');
                    out.push_str(name);
                }
                Step::MapValue(key) => {
                    out.push('[');
                    out.push_str(key);
                    out.push(']');
                }
            }
        }
        out
    }

    fn sql(&self, scope: &AssignmentScope) -> String {
        let mut out = format!(
            "{}.{}",
            scope.sql_qualifier,
            render::quote_if_needed(&self.column)
        );
        for step in &self.steps {
            match step {
                Step::Field(name) => {
                    out.push('.');
                    out.push_str(&render::backtick(name));
                }
                Step::MapValue(key) => {
                    out.push('[');
                    out.push_str(&render::string_literal(key));
                    out.push(']');
                }
            }
        }
        out
    }
}

struct Keyed<'a> {
    key: ResolvedKey,
    value: &'a Expr,
    value_type: Option<DataType>,
    sql: String,
}

fn folds(schema: &ArrowSchema, key: &ResolvedKey) -> bool {
    !key.steps.is_empty()
        || matches!(
            find_field(schema.fields(), &key.column).map(|field| field.data_type()),
            Some(DataType::Struct(_))
        )
}

fn key_parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect()
}

fn find_field<'a>(fields: &'a Fields, name: &str) -> Option<&'a Arc<Field>> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .or_else(|| {
            fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
        })
}

fn source_field<'a>(
    fields: &'a Fields,
    name: &str,
    case_sensitive: bool,
) -> Option<&'a Arc<Field>> {
    if case_sensitive {
        return fields.iter().find(|field| field.name() == name);
    }
    find_field(fields, name)
}

fn has_required_field(fields: &Fields) -> bool {
    fields.iter().any(|field| {
        !field.is_nullable()
            || matches!(field.data_type(), DataType::Struct(inner) if has_required_field(inner))
    })
}

fn unqualified<'p>(
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    parts: &'p [String],
) -> &'p [String] {
    let skip = scope
        .qualifiers
        .iter()
        .filter(|qualifier| {
            parts.len() > qualifier.len()
                && qualifier
                    .iter()
                    .zip(parts)
                    .all(|(expected, part)| expected.eq_ignore_ascii_case(part))
                && find_field(schema.fields(), &parts[qualifier.len()]).is_some()
        })
        .map(Vec::len)
        .max()
        .unwrap_or(0);
    &parts[skip..]
}

fn map_value_type(entries: &Field) -> Option<&DataType> {
    match entries.data_type() {
        DataType::Struct(pair) => pair.get(1).map(|value| value.data_type()),
        _ => None,
    }
}

fn resolve_key(schema: &ArrowSchema, parts: &[String]) -> Result<Option<ResolvedKey>> {
    let Some((first, rest)) = parts.split_first() else {
        return Ok(None);
    };
    let Some(root) = find_field(schema.fields(), first) else {
        return Ok(None);
    };
    let mut key = ResolvedKey {
        column: root.name().clone(),
        steps: Vec::with_capacity(rest.len()),
    };
    let mut current = root.data_type().clone();
    for part in rest {
        let (step, next) = match &current {
            DataType::Struct(fields) => {
                let child = find_field(fields, part)
                    .ok_or_else(|| render::field_not_found(part, fields))?;
                (Step::Field(child.name().clone()), child.data_type().clone())
            }
            DataType::List(element) | DataType::LargeList(element) => {
                let DataType::Struct(fields) = element.data_type() else {
                    return Err(render::unexpected_index_type(&key.pretty(), part));
                };
                let child = find_field(fields, part)
                    .ok_or_else(|| render::field_not_found(part, fields))?;
                (
                    Step::Field(child.name().clone()),
                    DataType::List(Arc::new(Field::new(
                        "element",
                        child.data_type().clone(),
                        true,
                    ))),
                )
            }
            DataType::Map(entries, _) => {
                let value = map_value_type(entries)
                    .cloned()
                    .ok_or_else(|| render::invalid_extract(&key.pretty(), &current))?;
                (Step::MapValue(part.clone()), value)
            }
            other => return Err(render::invalid_extract(&key.pretty(), other)),
        };
        key.steps.push(step);
        current = next;
    }
    Ok(Some(key))
}

fn resolve_target(
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    name: &ObjectName,
) -> Result<Option<ResolvedKey>> {
    let Some(parts) = key_parts(name) else {
        return Ok(None);
    };
    resolve_key(schema, unqualified(schema, scope, &parts))
}

fn pretty_assignment(
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    name: &ObjectName,
    key: Option<&ResolvedKey>,
    value: &Expr,
) -> String {
    let written = || {
        key_parts(name).map_or_else(
            || name.to_string(),
            |parts| unqualified(schema, scope, &parts).join("."),
        )
    };
    let key = match key {
        Some(key) if !key.steps.is_empty() => key.pretty(),
        _ => written(),
    };
    format!(
        "{key} = {}",
        render::pretty_expr(value, &scope.value_qualifiers)
    )
}

async fn probe_type(
    ctx: &SessionContext,
    scope: &AssignmentScope,
    value: &Expr,
) -> Option<DataType> {
    let sql = format!("SELECT ({value}) FROM {} LIMIT 0", scope.probe_from);
    let frame = ctx.sql(&sql).await.ok()?;
    frame
        .schema()
        .fields()
        .first()
        .map(|field| field.data_type().clone())
}

struct Aligner {
    errors: Vec<String>,
    case_sensitive: bool,
}

impl Aligner {
    fn apply(
        &mut self,
        path: &[String],
        column_sql: &str,
        data_type: &DataType,
        assigned: &[(usize, &Keyed<'_>)],
    ) -> Result<String> {
        let (exact, nested): (Vec<_>, Vec<_>) = assigned
            .iter()
            .partition(|(offset, keyed)| *offset == keyed.key.steps.len());
        if exact.len() > 1 {
            let values = exact
                .iter()
                .map(|(_, keyed)| keyed.value.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            self.errors.push(format!(
                "Multiple assignments for '{}': {values}",
                render::quoted_column_path(path)
            ));
            return Ok(column_sql.to_string());
        }
        if !exact.is_empty() && !nested.is_empty() {
            let conflicting = exact
                .iter()
                .chain(nested.iter())
                .map(|(_, keyed)| keyed.sql.clone())
                .collect::<Vec<_>>()
                .join(", ");
            self.errors.push(format!(
                "Conflicting assignments for '{}': {conflicting}",
                render::quoted_column_path(path)
            ));
            return Ok(column_sql.to_string());
        }
        if let [(_, only)] = exact.as_slice() {
            return leaf_sql(only, data_type, path, self.case_sensitive);
        }
        if nested.is_empty() {
            return Ok(column_sql.to_string());
        }
        let DataType::Struct(fields) = data_type else {
            self.errors.push(format!(
                "Updating nested fields is only supported for StructType but '{}' is of type {}",
                render::quoted_column_path(path),
                render::scala_type(data_type)
            ));
            return Ok(column_sql.to_string());
        };
        let mut members = Vec::with_capacity(fields.len());
        for field in fields {
            let name = render::string_literal(field.name());
            let child_sql = format!("get_field({column_sql}, {name})");
            let child: Vec<(usize, &Keyed<'_>)> = nested
                .iter()
                .filter(|(offset, keyed)| {
                    matches!(&keyed.key.steps[*offset], Step::Field(step) if step == field.name())
                })
                .map(|(offset, keyed)| (offset + 1, *keyed))
                .collect();
            let mut child_path = path.to_vec();
            child_path.push(field.name().clone());
            let rebuilt = self.apply(&child_path, &child_sql, field.data_type(), &child)?;
            members.push(format!("{name}, {rebuilt}"));
        }
        Ok(format!("named_struct({})", members.join(", ")))
    }
}

fn leaf_sql(
    keyed: &Keyed<'_>,
    target: &DataType,
    path: &[String],
    case_sensitive: bool,
) -> Result<String> {
    let value_sql = keyed.value.to_string();
    match &keyed.value_type {
        Some(source) => value_sql_for(&value_sql, source, target, path, case_sensitive),
        None => Ok(store_assignment_cast_sql(&value_sql, target)),
    }
}

fn names_match_exactly(source: &Fields, target: &Fields) -> bool {
    source.len() == target.len()
        && target.iter().all(|target_field| {
            source.iter().any(|source_field| {
                source_field.name() == target_field.name()
                    && match (source_field.data_type(), target_field.data_type()) {
                        (DataType::Struct(inner_source), DataType::Struct(inner_target)) => {
                            names_match_exactly(inner_source, inner_target)
                        }
                        _ => true,
                    }
            })
        })
}

fn value_sql_for(
    value_sql: &str,
    source: &DataType,
    target: &DataType,
    path: &[String],
    case_sensitive: bool,
) -> Result<String> {
    let (DataType::Struct(source_fields), DataType::Struct(target_fields)) = (source, target)
    else {
        if let Some(text) =
            incompatible_update_message("``", &render::backtick_path(path), source, target)
        {
            return Err(DataFusionError::Plan(text));
        }
        return Ok(store_assignment_cast_sql(value_sql, target));
    };
    let mut members = Vec::with_capacity(target_fields.len());
    let mut used = HashSet::new();
    for target_field in target_fields {
        let mut child_path = path.to_vec();
        child_path.push(target_field.name().clone());
        let source_field = source_field(source_fields, target_field.name(), case_sensitive)
            .ok_or_else(|| render::cannot_find_data(&child_path))?;
        used.insert(source_field.name().clone());
        let child_sql = format!(
            "get_field(({value_sql}), {})",
            render::string_literal(source_field.name())
        );
        let member = value_sql_for(
            &child_sql,
            source_field.data_type(),
            target_field.data_type(),
            &child_path,
            case_sensitive,
        )?;
        members.push(format!(
            "{}, {member}",
            render::string_literal(target_field.name())
        ));
    }
    let extra = source_fields
        .iter()
        .filter(|field| !used.contains(field.name()))
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    if !extra.is_empty() {
        return Err(render::extra_struct_fields(&extra, path));
    }
    if names_match_exactly(source_fields, target_fields) && !has_required_field(target_fields) {
        return Ok(store_assignment_cast_sql(value_sql, target));
    }
    Ok(format!(
        "CASE WHEN ({value_sql}) IS NULL THEN NULL ELSE named_struct({}) END",
        members.join(", ")
    ))
}

fn parse_expr(sql: &str) -> Result<Expr> {
    Parser::new(&DatabricksDialect {})
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_expr())
        .map_err(|error| DataFusionError::Plan(format!("nested assignment rebuild: {error}")))
}

fn column_sql(scope: &AssignmentScope, column: &str) -> String {
    let quoted = render::backtick(column);
    match &scope.column_prefix {
        Some(prefix) => format!("{prefix}.{quoted}"),
        None => quoted,
    }
}

pub(crate) async fn fold_nested_assignments(
    ctx: &SessionContext,
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    assignments: &[Assignment],
) -> Result<Option<Vec<Assignment>>> {
    let mut keys = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        keys.push(match &assignment.target {
            AssignmentTarget::ColumnName(name) => resolve_target(schema, scope, name)?,
            AssignmentTarget::Tuple(_) => None,
        });
    }
    let mut seen = HashSet::new();
    let affected: HashSet<String> = keys
        .iter()
        .flatten()
        .filter(|key| folds(schema, key) || (key.steps.is_empty() && !seen.insert(&key.column)))
        .map(|key| key.column.clone())
        .collect();
    if affected.is_empty() {
        return Ok(None);
    }
    let mut keyed = Vec::new();
    for (assignment, key) in assignments.iter().zip(&keys) {
        let Some(key) = key.as_ref().filter(|key| affected.contains(&key.column)) else {
            continue;
        };
        keyed.push(Keyed {
            key: key.clone(),
            value: &assignment.value,
            value_type: probe_type(ctx, scope, &assignment.value).await,
            sql: format!("{} = {}", key.sql(scope), assignment.value),
        });
    }
    let mut aligner = Aligner {
        errors: Vec::new(),
        case_sensitive: scope.case_sensitive,
    };
    let mut rebuilt = HashMap::new();
    for field in schema.fields() {
        if !affected.contains(field.name()) {
            continue;
        }
        let assigned: Vec<(usize, &Keyed<'_>)> = keyed
            .iter()
            .filter(|keyed| &keyed.key.column == field.name())
            .map(|keyed| (0, keyed))
            .collect();
        let sql = aligner.apply(
            std::slice::from_ref(field.name()),
            &column_sql(scope, field.name()),
            field.data_type(),
            &assigned,
        )?;
        rebuilt.insert(field.name().clone(), sql);
    }
    if !aligner.errors.is_empty() {
        let pretty = assignments
            .iter()
            .zip(&keys)
            .map(|(assignment, key)| match &assignment.target {
                AssignmentTarget::ColumnName(name) => {
                    pretty_assignment(schema, scope, name, key.as_ref(), &assignment.value)
                }
                AssignmentTarget::Tuple(_) => assignment.to_string(),
            })
            .collect::<Vec<_>>();
        return Err(render::row_level_refusal(&pretty, &aligner.errors));
    }
    let mut folded = Vec::with_capacity(assignments.len());
    let mut emitted = HashSet::new();
    for (assignment, key) in assignments.iter().zip(&keys) {
        match key.as_ref().filter(|key| affected.contains(&key.column)) {
            Some(key) => {
                if !emitted.insert(key.column.clone()) {
                    continue;
                }
                let sql = rebuilt.get(&key.column).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "nested assignment rebuild missed column `{}`",
                        key.column
                    ))
                })?;
                folded.push(Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::with_quote('`', key.column.clone())),
                    ])),
                    value: parse_expr(sql)?,
                });
            }
            None => folded.push(assignment.clone()),
        }
    }
    Ok(Some(folded))
}

fn refuse_nested_insert_keys(
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    insert: &MergeInsertExpr,
) -> Result<()> {
    let MergeInsertKind::Values(values) = &insert.kind else {
        return Ok(());
    };
    let [row] = values.rows.as_slice() else {
        return Ok(());
    };
    let mut keys = Vec::with_capacity(insert.columns.len());
    for column in &insert.columns {
        keys.push(resolve_target(schema, scope, column)?);
    }
    if insert.columns.len() != row.len() {
        return Ok(());
    }
    let nested = keys
        .iter()
        .zip(row.iter())
        .filter_map(|(key, value)| {
            key.as_ref()
                .filter(|key| !key.steps.is_empty())
                .map(|key| format!("{} = {value}", key.sql(scope)))
        })
        .collect::<Vec<_>>();
    let mut errors = Vec::new();
    if !nested.is_empty() {
        errors.push(format!(
            "INSERT assignment keys cannot be nested fields: {}",
            nested.join(", ")
        ));
    }
    errors.extend(repeated_insert_keys(&keys, row));
    if errors.is_empty() {
        return Ok(());
    }
    let pretty = insert
        .columns
        .iter()
        .zip(&keys)
        .zip(row.iter())
        .map(|((name, key), value)| pretty_assignment(schema, scope, name, key.as_ref(), value))
        .collect::<Vec<_>>();
    Err(render::row_level_refusal(&pretty, &errors))
}

fn repeated_insert_keys(keys: &[Option<ResolvedKey>], row: &[Expr]) -> Vec<String> {
    let mut columns: Vec<(&str, Vec<String>)> = Vec::new();
    for (key, value) in keys.iter().zip(row) {
        let Some(key) = key.as_ref().filter(|key| key.steps.is_empty()) else {
            continue;
        };
        match columns.iter_mut().find(|(column, _)| *column == key.column) {
            Some((_, values)) => values.push(value.to_string()),
            None => columns.push((&key.column, vec![value.to_string()])),
        }
    }
    columns
        .into_iter()
        .filter(|(_, values)| values.len() > 1)
        .map(|(column, values)| {
            format!(
                "Multiple assignments for '{}': {}",
                render::quoted_column_path(&[column.to_string()]),
                values.join(", ")
            )
        })
        .collect()
}

fn merge_needs_fold(clauses: &[MergeClause]) -> bool {
    clauses.iter().any(|clause| match &clause.action {
        MergeAction::Update(update) => !update.assignments.is_empty(),
        MergeAction::Insert(insert) => !insert.columns.is_empty(),
        MergeAction::Delete { .. } => false,
    })
}

pub(crate) fn repeats_a_column(
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    assignments: &[Assignment],
) -> bool {
    let mut seen = HashSet::new();
    assignments
        .iter()
        .any(|assignment| match &assignment.target {
            AssignmentTarget::ColumnName(name) => matches!(
                resolve_target(schema, scope, name),
                Ok(Some(key)) if key.steps.is_empty() && !seen.insert(key.column.clone())
            ),
            AssignmentTarget::Tuple(_) => false,
        })
}

fn factor_alias(factor: &TableFactor) -> Option<(String, String)> {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some((alias.name.value.clone(), alias.name.to_string())),
            None => name
                .0
                .last()
                .and_then(ObjectNamePart::as_ident)
                .map(|ident| (ident.value.clone(), ident.to_string())),
        },
        TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|alias| (alias.name.value.clone(), alias.name.to_string())),
        _ => None,
    }
}

async fn merge_target_schema(
    catalogs: &CatalogRegistry,
    table: &TableFactor,
) -> Option<ArrowSchema> {
    let TableFactor::Table { name, .. } = table else {
        return None;
    };
    let parts = crate::name_parts(name);
    let [catalog, namespace, table_name, ..] = parts.as_slice() else {
        return None;
    };
    let catalog = catalogs.get(catalog)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone());
    let loaded = catalog.load_table(&ident).await.ok()?;
    iceberg::arrow::schema_to_arrow_schema(loaded.metadata().current_schema()).ok()
}

pub(crate) async fn fold_merge_clauses(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &TableFactor,
    source: &TableFactor,
    clauses: &[MergeClause],
    schema_evolution: bool,
) -> Result<Option<Vec<MergeClause>>> {
    if !merge_needs_fold(clauses) {
        return Ok(None);
    }
    let Some(schema) = merge_target_schema(catalogs, table).await else {
        return Ok(None);
    };
    let Some((target_alias, target_rendered)) = factor_alias(table) else {
        return Ok(None);
    };
    let qualifiers = match table {
        TableFactor::Table {
            alias: None, name, ..
        } => name_suffixes(&crate::name_parts(name)),
        _ => vec![vec![target_alias.clone()]],
    };
    let mut value_qualifiers = qualifiers.clone();
    let source_alias = factor_alias(source);
    value_qualifiers.extend(source_alias.as_ref().map(|(alias, _)| vec![alias.clone()]));
    let has_star = clauses.iter().any(|clause| match &clause.action {
        MergeAction::Update(update) => {
            matches!(super::star_update(&update.assignments), Ok(Some(_)))
        }
        MergeAction::Insert(insert) => super::star_insert(insert),
        MergeAction::Delete { .. } => false,
    });
    let case_sensitive = !crate::spark_door_case_insensitive(ctx.state().config().options());
    if has_star && case_sensitive && !schema_evolution {
        expand::refuse_unwritten_star_columns(&schema, source)?;
    }
    let star = match source_alias.filter(|_| has_star && !schema_evolution) {
        Some((_, rendered)) => {
            expand::star_source(ctx, &schema, source, &rendered, case_sensitive).await
        }
        None => None,
    };
    let scope = AssignmentScope {
        qualifiers,
        sql_qualifier: target_alias,
        column_prefix: Some(target_rendered),
        value_qualifiers,
        probe_from: format!("{source} CROSS JOIN {table}"),
        case_sensitive,
    };
    let mut folded = clauses.to_vec();
    let mut changed = false;
    for clause in &mut folded {
        match &mut clause.action {
            MergeAction::Update(update) => {
                if let Some(star) = star
                    .as_ref()
                    .filter(|_| matches!(super::star_update(&update.assignments), Ok(Some(_))))
                {
                    update.assignments = star.assignments()?;
                    changed = true;
                }
                if let Some(assignments) =
                    fold_nested_assignments(ctx, &schema, &scope, &update.assignments).await?
                {
                    update.assignments = assignments;
                    changed = true;
                }
            }
            MergeAction::Insert(insert) => {
                refuse_nested_insert_keys(&schema, &scope, insert)?;
                if let Some(star) = star.as_ref().filter(|_| super::star_insert(insert)) {
                    star.expand_insert(insert)?;
                    changed = true;
                }
                changed |= expand::fold_insert_values(ctx, &schema, &scope, insert).await?;
            }
            MergeAction::Delete { .. } => {}
        }
    }
    Ok(changed.then_some(folded))
}
