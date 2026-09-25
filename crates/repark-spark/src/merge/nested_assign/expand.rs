use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Ident, MergeInsertExpr, MergeInsertKind, ObjectName,
    ObjectNamePart, TableFactor,
};

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

fn unique_match<'a>(fields: &'a Fields, name: &str) -> Option<&'a Field> {
    let mut matches = fields
        .iter()
        .filter(|field| field.name().eq_ignore_ascii_case(name));
    let first = matches.next()?;
    matches.next().is_none().then_some(first.as_ref())
}

pub(super) async fn star_source(
    ctx: &SessionContext,
    schema: &ArrowSchema,
    source: &TableFactor,
    source_alias: &str,
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
        let found = unique_match(&fields, target.name())?;
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
        )?;
        *value = parse_expr(&rebuilt)?;
        changed = true;
    }
    Ok(changed)
}
