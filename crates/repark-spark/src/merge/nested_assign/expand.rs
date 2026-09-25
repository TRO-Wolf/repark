use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::common::utils::datafusion_strsim::levenshtein;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, MergeInsertExpr, MergeInsertKind, ObjectName,
    ObjectNamePart, SelectItem, SetExpr, TableFactor,
};
use repark_common::spark_error;

use super::{AssignmentScope, parse_expr, probe_type, render, resolve_target, value_sql_for};

pub(super) struct StarSource {
    pairs: Vec<(String, String)>,
}

async fn source_fields(ctx: &SessionContext, source: &TableFactor) -> Option<Fields> {
    let frame = ctx
        .sql(&format!("SELECT * FROM {source} LIMIT 0"))
        .await
        .ok()?;
    Some(frame.schema().fields().clone())
}

fn needs_rebuild(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Struct(from), DataType::Struct(to)) => {
            from.len() != to.len()
                || from.iter().zip(to.iter()).any(|(source, target)| {
                    source.name() != target.name()
                        || needs_rebuild(source.data_type(), target.data_type())
                })
        }
        _ => false,
    }
}

fn unique_match<'a>(fields: &'a Fields, name: &str, case_sensitive: bool) -> Option<&'a Field> {
    let mut matches = fields.iter().filter(|field| {
        if case_sensitive {
            field.name() == name
        } else {
            field.name().eq_ignore_ascii_case(name)
        }
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first.as_ref())
}

fn written_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            parts.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

fn written_source_names(source: &TableFactor) -> Option<Vec<String>> {
    let TableFactor::Derived { subquery, .. } = source else {
        return None;
    };
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return None;
    };
    select.projection.iter().map(written_name).collect()
}

pub(super) fn refuse_unwritten_star_columns(
    schema: &ArrowSchema,
    source: &TableFactor,
) -> Result<()> {
    let Some(mut names) = written_source_names(source) else {
        return Ok(());
    };
    let Some(missing) = schema
        .fields()
        .iter()
        .find(|field| !names.iter().any(|name| name == field.name()))
    else {
        return Ok(());
    };
    let target = missing.name().to_lowercase();
    names.sort();
    names.sort_by_key(|name| levenshtein(&name.to_lowercase(), &target));
    let suggestions = names
        .iter()
        .map(|name| render::backtick(name))
        .collect::<Vec<_>>()
        .join(", ");
    Err(DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", render::backtick(missing.name()).as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    )))
}

pub(super) async fn star_source(
    ctx: &SessionContext,
    schema: &ArrowSchema,
    source: &TableFactor,
    source_alias: &str,
    case_sensitive: bool,
) -> Option<StarSource> {
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Struct(_)))
    {
        return None;
    }
    let fields = source_fields(ctx, source).await?;
    let mut pairs = Vec::with_capacity(schema.fields().len());
    let mut rebuild = false;
    for target in schema.fields() {
        let found = unique_match(&fields, target.name(), case_sensitive)?;
        rebuild |= needs_rebuild(found.data_type(), target.data_type());
        pairs.push((
            target.name().clone(),
            format!("{source_alias}.{}", render::backtick(found.name())),
        ));
    }
    rebuild.then_some(StarSource { pairs })
}

fn column_name(column: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::with_quote(
        '`',
        column.to_string(),
    ))])
}

impl StarSource {
    pub(super) fn assignments(&self) -> Result<Vec<Assignment>> {
        self.pairs
            .iter()
            .map(|(column, value)| {
                Ok(Assignment {
                    target: AssignmentTarget::ColumnName(column_name(column)),
                    value: parse_expr(value)?,
                })
            })
            .collect()
    }

    pub(super) fn expand_insert(&self, insert: &mut MergeInsertExpr) -> Result<()> {
        let MergeInsertKind::Values(values) = &mut insert.kind else {
            return Ok(());
        };
        let [only] = values.rows.as_mut_slice() else {
            return Ok(());
        };
        let row = self
            .pairs
            .iter()
            .map(|(_, value)| parse_expr(value))
            .collect::<Result<Vec<_>>>()?;
        insert.columns = self
            .pairs
            .iter()
            .map(|(column, _)| column_name(column))
            .collect();
        only.content = row;
        Ok(())
    }
}

pub(super) async fn fold_insert_values(
    ctx: &SessionContext,
    schema: &ArrowSchema,
    scope: &AssignmentScope,
    insert: &mut MergeInsertExpr,
) -> Result<bool> {
    let MergeInsertKind::Values(values) = &mut insert.kind else {
        return Ok(false);
    };
    let [row] = values.rows.as_mut_slice() else {
        return Ok(false);
    };
    if row.content.len() != insert.columns.len() {
        return Ok(false);
    }
    let mut changed = false;
    for (column, value) in insert.columns.iter().zip(row.content.iter_mut()) {
        let Some(key) = resolve_target(schema, scope, column)? else {
            continue;
        };
        if !key.steps.is_empty() {
            continue;
        }
        let Some(target) = schema
            .fields()
            .iter()
            .find(|field| field.name() == &key.column)
        else {
            continue;
        };
        let Some(source) = probe_type(ctx, scope, value).await else {
            continue;
        };
        if !needs_rebuild(&source, target.data_type()) {
            continue;
        }
        let rebuilt = value_sql_for(
            &value.to_string(),
            &source,
            target.data_type(),
            std::slice::from_ref(&key.column),
            scope.case_sensitive,
        )?;
        *value = parse_expr(&rebuilt)?;
        changed = true;
    }
    Ok(changed)
}
