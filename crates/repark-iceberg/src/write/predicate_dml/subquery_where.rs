use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Delete, FromTable, ObjectName, Statement, TableFactor, Update,
};

use super::{expression_contains_subquery, try_allowed_delete_in, try_allowed_update_in};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DmlSubqueryVerb {
    Delete,
    Update,
}

impl DmlSubqueryVerb {
    const fn verb(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
        }
    }

    const fn consequence(self) -> &'static str {
        match self {
            Self::Delete => "delete EVERY row of the table",
            Self::Update => "update EVERY row of the table",
        }
    }

    const fn merge_action(self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Update => "UPDATE SET <assignments>",
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_dml_subquery_predicate(
    statement: &Statement,
    identity_forms_execute: bool,
) -> Result<()> {
    let (verb, selection, target) = match statement {
        Statement::Delete(delete) => (
            DmlSubqueryVerb::Delete,
            delete.selection.as_ref(),
            delete_target(delete).map_or_else(|| "<table>".to_string(), ToString::to_string),
        ),
        Statement::Update(update) => (
            DmlSubqueryVerb::Update,
            update.selection.as_ref(),
            update_target(update).map_or_else(|| update.table.to_string(), ToString::to_string),
        ),
        _ => return Ok(()),
    };
    if identity_forms_execute
        && (try_allowed_delete_in(statement)?.is_some()
            || try_allowed_update_in(statement)?.is_some())
    {
        return Ok(());
    }
    let Some(selection) = selection else {
        return Ok(());
    };
    if !expression_contains_subquery(selection) {
        return Ok(());
    }
    Err(DataFusionError::Plan(dml_subquery_refusal_message(
        verb, &target,
    )))
}

fn delete_target(delete: &Delete) -> Option<&ObjectName> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    match &tables.first()?.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

fn update_target(update: &Update) -> Option<&ObjectName> {
    match &update.table.relation {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

fn dml_subquery_refusal_message(verb: DmlSubqueryVerb, table: &str) -> String {
    format!(
        "{verb_name} with a subquery in its WHERE clause is refused on `{table}`: subquery \
         predicates are silently mis-executed today — DataFusion's DML planner decorrelates \
         IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a semi-join and then \
         recovers NO filter for the Iceberg writer, so this statement would \
         {consequence} instead of only the matching ones (defect G3-E8, silent data loss). \
         Rewrite it as `MERGE INTO {table} AS target USING (<the subquery>) AS source \
         ON <join keys> WHEN MATCHED THEN {action}` — the RePark-owned MERGE executor never \
         crosses that seam, and it is the dbt adapter's proven vehicle. Support returns when the \
         underlying fix lands; non-subquery {verb_name} predicates are unaffected.",
        verb_name = verb.verb(),
        consequence = verb.consequence(),
        action = verb.merge_action(),
    )
}
