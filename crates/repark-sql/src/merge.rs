//! `MERGE INTO` — the ANSI door's thin lowering (design §2 Q4).
//!
//! Ownership is the point of the ruling. **Execution is shared and RePark-owned**:
//! [`repark_iceberg::write::merge::execute_merge`] does first-match-wins, the cardinality check,
//! the copy-on-write rewrite, and the commit — the fork's `TableProvider` DML path (ADR-0003) is
//! deliberately NOT used for MERGE. **Lowering is per-door**: this module maps sqlparser's
//! `Statement::Merge` onto the shared [`MergeSpec`], and the Spark door maps its own AST onto the
//! same type. There is no door→door edge, and the shared target TYPE is what turns any drift
//! between the two lowerings into a cross-door test failure (design §6 R3).
//!
//! What this door does NOT carry, by ruling:
//! * **The star forms.** `WHEN MATCHED THEN UPDATE SET *` and `THEN INSERT *` are Spark spellings
//!   that stock sqlparser cannot parse; the Spark door earns them with a token-level sentinel
//!   rewrite. Here they are parse-level absent, and the wrong-door sniff already steers. No
//!   sentinel machinery is duplicated.
//! * **`OUTPUT` / `RETURNING`.** They parse, and silently dropping the clause would hand back a
//!   result set the user never asked for — so they refuse loud, in the same message class the
//!   Spark door uses.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Merge, MergeAction, MergeClause, MergeClauseKind,
    MergeInsertKind, ObjectName, TableFactor,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::EngineContext;
use repark_iceberg::write::merge::{
    InsertAction, InsertClause, MatchedAction, MatchedClause, MergeSpec,
};

use crate::schema_ddl::{catalog_handle, name_parts};

/// ===========================================================================================
/// Lower and execute a parsed `MERGE INTO`.
/// ===========================================================================================
///
/// # Errors
/// An OUTPUT/RETURNING refusal, a malformed target/source/clause shape, the P11 or `MoR` guard
/// refusals already raised at the router head, or anything the shared executor surfaces
/// (including `MERGE_CARDINALITY_VIOLATION`).
pub(crate) async fn execute_merge(cx: &EngineContext<'_>, merge: &Merge) -> Result<DataFrame> {
    if merge.output.is_some() {
        return Err(DataFusionError::NotImplemented(
            "MERGE OUTPUT/RETURNING clauses are not supported — the statement returns no rows. \
             Read the affected rows back with a SELECT after the MERGE commits"
                .to_string(),
        ));
    }
    let (catalog_name, spec) = lower(&merge.table, &merge.source, &merge.on, &merge.clauses)?;
    let handle = catalog_handle(cx.catalogs, &catalog_name)?;
    repark_iceberg::write::merge::execute_merge(cx.ctx, handle, &spec).await?;
    cx.ctx.read_empty()
}

/// Lower the sqlparser pieces into the executor's plain-string spec, returning the catalog name
/// the target resolved to.
fn lower(
    table: &TableFactor,
    source: &TableFactor,
    on: &Expr,
    clauses: &[MergeClause],
) -> Result<(String, MergeSpec)> {
    let (target_name, target_alias) = target_table(table)?;
    let parts = name_parts(&target_name);
    let [catalog, namespace, table_name] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "MERGE INTO target must be a three-part `catalog.schema.table` name, got \
             `{target_name}` — this door does not resolve a default catalog"
        )));
    };
    let target_alias = match target_alias {
        Some(alias) => alias,
        None => bare_alias(&target_name)?,
    };
    let (source_from_sql, source_alias) = source_table(source)?;

    let mut matched = Vec::new();
    let mut not_matched = Vec::new();
    for clause in clauses {
        lower_clause(clause, &mut matched, &mut not_matched)?;
    }
    if matched.is_empty() && not_matched.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN clause".to_string(),
        ));
    }

    Ok((
        catalog.clone(),
        MergeSpec {
            target: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
            target_alias,
            source_from_sql,
            source_alias,
            on_sql: on.to_string(),
            matched,
            not_matched,
        },
    ))
}

/// Sort one WHEN clause into the matched / not-matched lists, validating the kind–action pairing.
///
/// Every expression is re-rendered to SQL verbatim (`ToString`), so quoting the user wrote is
/// preserved and resolves against the aliases chosen here.
fn lower_clause(
    clause: &MergeClause,
    matched: &mut Vec<MatchedClause>,
    not_matched: &mut Vec<InsertClause>,
) -> Result<()> {
    let predicate_sql = clause.predicate.as_ref().map(ToString::to_string);
    match (&clause.clause_kind, &clause.action) {
        (MergeClauseKind::Matched, MergeAction::Update(update)) => {
            matched.push(MatchedClause {
                predicate_sql,
                action: MatchedAction::Update {
                    assignments: lower_assignments(&update.assignments)?,
                },
            });
        }
        (MergeClauseKind::Matched, MergeAction::Delete { .. }) => {
            matched.push(MatchedClause {
                predicate_sql,
                action: MatchedAction::Delete,
            });
        }
        (MergeClauseKind::Matched, MergeAction::Insert(_)) => {
            return Err(DataFusionError::Plan(
                "INSERT is not a valid WHEN MATCHED action".to_string(),
            ));
        }
        (
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
            MergeAction::Insert(insert),
        ) => {
            let MergeInsertKind::Values(values) = &insert.kind else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE `INSERT ROW` is not supported; write INSERT (cols) VALUES (…)"
                        .to_string(),
                ));
            };
            let [row] = values.rows.as_slice() else {
                return Err(DataFusionError::Plan(
                    "MERGE INSERT expects exactly one VALUES row".to_string(),
                ));
            };
            if insert.columns.is_empty() {
                return Err(DataFusionError::Plan(
                    "MERGE INSERT requires an explicit column list: INSERT (a, b) VALUES (…)"
                        .to_string(),
                ));
            }
            not_matched.push(InsertClause {
                predicate_sql,
                action: InsertAction::Explicit {
                    columns: insert.columns.iter().map(object_name_column).collect(),
                    values_sql: row.iter().map(ToString::to_string).collect(),
                },
            });
        }
        (MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget, _) => {
            return Err(DataFusionError::Plan(
                "only INSERT is valid in a WHEN NOT MATCHED clause".to_string(),
            ));
        }
        (MergeClauseKind::NotMatchedBySource, _) => {
            return Err(DataFusionError::NotImplemented(
                "WHEN NOT MATCHED BY SOURCE is not supported yet — express it as a separate \
                 DELETE or UPDATE against the target"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// `SET col = expr` pairs; a qualified target (`t.col`) keeps only the column name.
fn lower_assignments(assignments: &[Assignment]) -> Result<Vec<(String, String)>> {
    if assignments.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE UPDATE requires at least one SET assignment".to_string(),
        ));
    }
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE UPDATE SET (a, b) = … tuple assignment is not supported".to_string(),
                ));
            };
            let column = name_parts(name).pop().ok_or_else(|| {
                DataFusionError::Plan(format!("cannot resolve SET target `{name}`"))
            })?;
            Ok((column, assignment.value.to_string()))
        })
        .collect()
}

/// The MERGE target: a plain table factor with an optional alias, rendered WITH its quoting so it
/// matches the references the user's ON/SET expressions re-render with.
fn target_table(factor: &TableFactor) -> Result<(ObjectName, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(DataFusionError::Plan(
            "MERGE INTO target must be a table, not a subquery".to_string(),
        ));
    };
    Ok((name.clone(), alias.as_ref().map(|a| a.name.to_string())))
}

/// The `USING` source: a table (aliased, or referenced by its bare name) or an aliased subquery.
fn source_table(factor: &TableFactor) -> Result<(String, String)> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let bare = bare_alias(name)?;
            Ok((
                name.to_string(),
                alias.as_ref().map_or(bare, |a| a.name.to_string()),
            ))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias = alias.as_ref().map(|a| a.name.to_string()).ok_or_else(|| {
                DataFusionError::Plan(
                    "a MERGE subquery source requires an alias (USING (SELECT …) AS s)".to_string(),
                )
            })?;
            Ok((format!("({subquery})"), alias))
        }
        other => Err(DataFusionError::Plan(format!(
            "unsupported MERGE source: `{other}`"
        ))),
    }
}

/// The default alias for an unaliased relation: its last name part, rendered WITH any quoting.
fn bare_alias(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .map(ToString::to_string)
        .ok_or_else(|| DataFusionError::Plan(format!("cannot resolve MERGE name `{name}`")))
}

/// An insert-column name, unquoted.
fn object_name_column(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map_or_else(|| name.to_string(), |ident| ident.value.clone())
}

#[cfg(test)]
mod tests;
