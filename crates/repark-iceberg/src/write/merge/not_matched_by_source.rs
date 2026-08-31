//! `WHEN NOT MATCHED BY SOURCE` types and SQL fragments.

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::ManifestContentType;
use iceberg::table::Table;

use super::{
    FILE_PATH_COL, MergeSpec, MergeSql, POS_COL, iceberg_err, quote_ident,
    resolve_schema_field_name, store_assignment_then_sql,
};

/// One `WHEN NOT MATCHED BY SOURCE [AND …] THEN UPDATE/DELETE` clause.
#[derive(Debug, Clone)]
pub struct NotMatchedBySourceClause {
    /// The `AND …` predicate, if present (SQL-rendered).
    pub predicate_sql: Option<String>,
    /// What the clause does to a target row with no source match.
    pub action: NotMatchedBySourceAction,
}

/// The action of a `WHEN NOT MATCHED BY SOURCE` clause.
#[derive(Debug, Clone)]
pub enum NotMatchedBySourceAction {
    /// `UPDATE SET col = expr, …` — `(column, SQL expression)` pairs.
    Update {
        /// Assignments as `(target column, SQL expression)` pairs.
        assignments: Vec<(String, String)>,
    },
    /// `DELETE` — drop the unmatched-by-source row.
    Delete,
}

/// True when the spec carries at least one `WHEN NOT MATCHED BY SOURCE` clause.
#[must_use]
pub(super) fn is_present(spec: &MergeSpec) -> bool {
    !spec.not_matched_by_source.is_empty()
}

/// First-match clause id over unmatched-by-source rows; `NULL` when the row matched source.
#[must_use]
pub(super) fn clause_id_expr(sql: &MergeSql<'_>) -> String {
    let predicates: Vec<Option<&str>> = sql
        .spec
        .not_matched_by_source
        .iter()
        .map(|clause| clause.predicate_sql.as_deref())
        .collect();
    if predicates.is_empty() {
        return "CAST(NULL AS BIGINT)".to_string();
    }
    let mut branches = Vec::with_capacity(predicates.len() + 1);
    branches.push(format!("WHEN {} THEN NULL", sql.matched()));
    for (index, predicate) in predicates.iter().enumerate() {
        branches.push(format!(
            "WHEN {} THEN {index}",
            MergeSql::applies(*predicate)
        ));
    }
    format!("CASE {} END", branches.join(" "))
}

/// True when the unmatched-by-source row's first applicable clause is a DELETE.
#[must_use]
pub(super) fn delete_applies(sql: &MergeSql<'_>) -> String {
    action_applies(sql, |action| {
        matches!(action, NotMatchedBySourceAction::Delete)
    })
}

/// True when the unmatched-by-source row's first applicable clause is an UPDATE.
#[must_use]
pub(super) fn update_applies(sql: &MergeSql<'_>) -> String {
    action_applies(sql, |action| {
        matches!(action, NotMatchedBySourceAction::Update { .. })
    })
}

fn action_applies(
    sql: &MergeSql<'_>,
    predicate: impl Fn(&NotMatchedBySourceAction) -> bool,
) -> String {
    let ids: Vec<String> = sql
        .spec
        .not_matched_by_source
        .iter()
        .enumerate()
        .filter(|(_, clause)| predicate(&clause.action))
        .map(|(index, _)| index.to_string())
        .collect();
    if ids.is_empty() {
        "FALSE".to_string()
    } else if ids.len() == 1 {
        format!("COALESCE(({}) = {}, FALSE)", clause_id_expr(sql), ids[0])
    } else {
        format!(
            "COALESCE(({}) IN ({}), FALSE)",
            clause_id_expr(sql),
            ids.join(", ")
        )
    }
}

/// Per-clause assignment maps (column lower → SQL expr) for NMBS UPDATE clauses.
#[must_use]
pub(super) fn update_assignment_lookup(spec: &MergeSpec) -> Vec<Option<HashMap<String, &str>>> {
    spec.not_matched_by_source
        .iter()
        .map(|clause| match &clause.action {
            NotMatchedBySourceAction::Update { assignments } => {
                let mut map = HashMap::with_capacity(assignments.len());
                for (name, expr) in assignments {
                    map.insert(name.to_ascii_lowercase(), expr.as_str());
                }
                Some(map)
            }
            NotMatchedBySourceAction::Delete => None,
        })
        .collect()
}

/// ELSE expression for a rewrite column: NMBS UPDATE CASE, or the original target column.
#[must_use]
pub(super) fn rewrite_else(
    sql: &MergeSql<'_>,
    column: &str,
    original: &str,
    store_type: Option<&DataType>,
) -> String {
    let maps = update_assignment_lookup(sql.spec);
    let key = column.to_ascii_lowercase();
    let branches: Vec<String> = maps
        .iter()
        .enumerate()
        .filter_map(|(index, map_opt)| {
            let expr = map_opt.as_ref()?.get(&key)?;
            let then_expr = match store_type {
                Some(data_type) => store_assignment_then_sql(expr, data_type),
                None => (*expr).to_string(),
            };
            Some(format!("WHEN {index} THEN ({then_expr})"))
        })
        .collect();
    if branches.is_empty() {
        original.to_string()
    } else {
        format!(
            "CASE ({clause_id}) {} ELSE {original} END",
            branches.join(" "),
            clause_id = clause_id_expr(sql),
        )
    }
}

/// Combine MATCHED-delete and NMBS-delete predicates.
#[must_use]
pub(super) fn combined_delete_applies(matched_delete: &str, sql: &MergeSql<'_>) -> String {
    let nmbs = delete_applies(sql);
    if nmbs == "FALSE" {
        matched_delete.to_string()
    } else if matched_delete == "FALSE" {
        nmbs
    } else {
        format!("({matched_delete}) OR ({nmbs})")
    }
}

/// Merge-on-read work for unmatched-by-source rows (position deletes + UPDATE projection).
#[must_use]
pub(super) fn mor_work_sql(sql: &MergeSql<'_>, write_schema: &ArrowSchema) -> String {
    let ta = &sql.spec.target_alias;
    let projection = sql.rewrite_projection(write_schema);
    format!(
        "SELECT {ta}.\"{FILE_PATH_COL}\", {ta}.\"{POS_COL}\", \
         CAST(1 AS BIGINT) AS match_count, \
         CAST(1 AS BIGINT) AS is_mutated, \
         CASE WHEN ({updated}) THEN 1 ELSE 0 END AS is_update, \
         {projection} \
         FROM {target} LEFT JOIN {source} ON {on} \
         WHERE NOT ({matched}) AND ({clause_id}) IS NOT NULL",
        target = sql.target_from(),
        source = sql.source_from(),
        on = sql.spec.on_sql,
        matched = sql.matched(),
        updated = update_applies(sql),
        clause_id = clause_id_expr(sql),
    )
}

/// Every live data-file path in the current snapshot (full-table rewrite when NMBS is present).
pub(super) async fn all_current_data_file_paths(table: &Table) -> Result<Vec<String>> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(iceberg_err)?;
    let mut paths = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(iceberg_err)?;
        for entry in manifest.entries() {
            if entry.is_alive() {
                paths.push(entry.data_file().file_path().to_string());
            }
        }
    }
    Ok(paths)
}

/// Plan NMBS `UPDATE SET` expressions against the target only (Spark: source columns do not resolve).
pub(super) fn update_assignment_probe_sql(
    sql: &MergeSql<'_>,
    write_schema: &ArrowSchema,
) -> Result<Option<(String, Vec<String>)>> {
    let mut projections = Vec::new();
    let mut target_columns = Vec::new();
    for clause in &sql.spec.not_matched_by_source {
        let NotMatchedBySourceAction::Update { assignments } = &clause.action else {
            continue;
        };
        for (column, expr) in assignments {
            let Some(canonical) = resolve_schema_field_name(write_schema, column) else {
                return Err(DataFusionError::Internal(format!(
                    "MERGE UPDATE SET column `{column}` missing after validate_update_columns \
                     (executor bug)"
                )));
            };
            let alias = quote_ident(&format!("n{}", projections.len()));
            projections.push(format!("({expr}) AS {alias}"));
            target_columns.push(canonical.to_string());
        }
    }
    if projections.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        format!(
            "SELECT {projection} FROM {target}",
            projection = projections.join(", "),
            target = sql.target_from(),
        ),
        target_columns,
    )))
}

/// Every NMBS UPDATE SET column must exist once in the target schema.
pub(super) fn validate_update_columns(spec: &MergeSpec, write_schema: &ArrowSchema) -> Result<()> {
    for clause in &spec.not_matched_by_source {
        let NotMatchedBySourceAction::Update { assignments } = &clause.action else {
            continue;
        };
        let mut seen = std::collections::HashSet::with_capacity(assignments.len());
        for (column, _) in assignments {
            let Some(canonical) = resolve_schema_field_name(write_schema, column) else {
                return Err(DataFusionError::Plan(format!(
                    "MERGE UPDATE SET column `{column}` does not exist in the target table"
                )));
            };
            if !seen.insert(canonical.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "MERGE UPDATE SET names column `{column}` more than once \
                     (case-insensitive)"
                )));
            }
        }
    }
    Ok(())
}
