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
    MergeInsertExpr, MergeInsertKind, MergeUpdateExpr, ObjectName, TableFactor,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::EngineContext;
use repark_iceberg::write::merge::{
    InsertAction, InsertClause, MatchedAction, MatchedClause, MergeSpec,
};

use crate::schema_ddl::{catalog_handle, name_parts};

/// Oracle-style action sub-predicates (`UPDATE SET … WHERE` / `DELETE WHERE` /
/// `INSERT … WHERE`) are not Spark MERGE grammar. Copied into both doors.
const ORACLE_STYLE_SUB_PREDICATE_REFUSAL: &str = "Oracle-style `UPDATE SET … WHERE` / `DELETE WHERE` / `INSERT … WHERE` is not Spark MERGE \
     grammar; move the predicate into `WHEN MATCHED AND <cond>` / `WHEN NOT MATCHED AND <cond>`";

/// Verbatim A8 needle: MERGE INSERT without a column list.
const MERGE_INSERT_COLUMN_LIST_REQUIRED: &str =
    "MERGE INSERT requires an explicit column list: INSERT (a, b) VALUES (…)";

/// Spark analysis error-class: an unconditioned MATCHED clause that is not last of its kind.
const NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION: &str = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION: When there are more than one MATCHED clauses in a \
     MERGE statement, only the last MATCHED clause can omit the condition";

/// Spark analysis error-class: an unconditioned NOT MATCHED clause that is not last of its kind.
const NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION: &str = "NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION: When there are more than one NOT MATCHED \
     clauses in a MERGE statement, only the last NOT MATCHED clause can omit the condition";

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
        lower_clause(clause, &target_alias, &mut matched, &mut not_matched)?;
    }
    if matched.is_empty() && not_matched.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN clause".to_string(),
        ));
    }
    refuse_non_last_unconditional_clause(&matched, &not_matched)?;

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
    target_alias: &str,
    matched: &mut Vec<MatchedClause>,
    not_matched: &mut Vec<InsertClause>,
) -> Result<()> {
    let predicate_sql = clause.predicate.as_ref().map(ToString::to_string);
    match (&clause.clause_kind, &clause.action) {
        (MergeClauseKind::Matched, MergeAction::Update(update)) => {
            let MergeUpdateExpr {
                update_token: _,
                assignments,
                update_predicate,
                delete_predicate,
            } = update;
            refuse_oracle_style_sub_predicate(update_predicate.as_ref())?;
            refuse_oracle_style_sub_predicate(delete_predicate.as_ref())?;
            matched.push(MatchedClause {
                predicate_sql,
                action: MatchedAction::Update {
                    assignments: lower_assignments(assignments, target_alias)?,
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
            let MergeInsertExpr {
                insert_token: _,
                columns,
                kind_token: _,
                kind,
                insert_predicate,
            } = insert;
            refuse_oracle_style_sub_predicate(insert_predicate.as_ref())?;
            let MergeInsertKind::Values(values) = kind else {
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
            if columns.is_empty() {
                return Err(DataFusionError::Plan(
                    MERGE_INSERT_COLUMN_LIST_REQUIRED.to_string(),
                ));
            }
            let columns = columns
                .iter()
                .map(|name| resolve_merge_column(name, target_alias, "INSERT column"))
                .collect::<Result<Vec<_>>>()?;
            not_matched.push(InsertClause {
                predicate_sql,
                action: InsertAction::Explicit {
                    columns,
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

/// `SET col = expr` pairs. Accepts a bare column or `<target-alias>.column`; any other
/// qualifier or a three-or-more-part (nested) name is refused.
fn lower_assignments(
    assignments: &[Assignment],
    target_alias: &str,
) -> Result<Vec<(String, String)>> {
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
            let column = resolve_merge_column(name, target_alias, "SET target")?;
            Ok((column, assignment.value.to_string()))
        })
        .collect()
}

/// Loud Plan error when an Oracle-style action sub-predicate is present.
fn refuse_oracle_style_sub_predicate(predicate: Option<&Expr>) -> Result<()> {
    if predicate.is_some() {
        return Err(DataFusionError::Plan(
            ORACLE_STYLE_SUB_PREDICATE_REFUSAL.to_string(),
        ));
    }
    Ok(())
}

/// After clause collection: an unconditioned clause may only be last of its kind.
fn refuse_non_last_unconditional_clause(
    matched: &[MatchedClause],
    not_matched: &[InsertClause],
) -> Result<()> {
    if matched
        .iter()
        .enumerate()
        .any(|(index, clause)| clause.predicate_sql.is_none() && index + 1 < matched.len())
    {
        return Err(DataFusionError::Plan(
            NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION.to_string(),
        ));
    }
    if not_matched
        .iter()
        .enumerate()
        .any(|(index, clause)| clause.predicate_sql.is_none() && index + 1 < not_matched.len())
    {
        return Err(DataFusionError::Plan(
            NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION.to_string(),
        ));
    }
    Ok(())
}

/// Bare column, or `<target-alias>.column` (qualifier compared case-insensitively to the
/// statement's target alias). Three-or-more-part names are nested-field assignment.
fn resolve_merge_column(name: &ObjectName, target_alias: &str, construct: &str) -> Result<String> {
    let parts = name_parts(name);
    match parts.as_slice() {
        [] => Err(DataFusionError::Plan(format!(
            "cannot resolve {construct} `{name}`"
        ))),
        [column] => Ok(column.clone()),
        [qualifier, column] => {
            if qualifier.eq_ignore_ascii_case(unquoted_ident(target_alias)) {
                Ok(column.clone())
            } else {
                Err(DataFusionError::Plan(format!(
                    "{construct} qualifier `{qualifier}` is not the MERGE target alias `{target_alias}`"
                )))
            }
        }
        _ => Err(DataFusionError::Plan(
            "nested-field assignment is not supported".to_string(),
        )),
    }
}

/// Strip a matching pair of `"` or `` ` `` from a rendered identifier so alias comparison
/// uses the ident value, not the quote style.
fn unquoted_ident(rendered: &str) -> &str {
    let trimmed = rendered.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let quote = bytes[0];
        if matches!(quote, b'"' | b'`') && bytes[bytes.len() - 1] == quote {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
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

#[cfg(test)]
mod tests;
