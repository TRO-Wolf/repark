use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::error::Result;
use iceberg::table::Table;

use super::insert::{store_assignment_then_sql, table_projection};
use super::{
    FILE_PATH_COL, MatchedAction, MergeSpec, POS_COL, cow_scratch, not_matched_by_source,
    quote_ident, row_lineage,
};

pub(super) struct MergeSql<'a> {
    pub(super) spec: &'a MergeSpec,
    pub(super) target_name: &'a str,
    pub(super) match_flag: &'a str,
    pub(super) carry_lineage: bool,
}

impl MergeSql<'_> {
    pub(super) fn target_from(&self) -> String {
        format!(
            "{} AS {}",
            cow_scratch::quote_scratch_name(self.target_name),
            self.spec.target_alias
        )
    }

    pub(super) fn source_from(&self) -> String {
        format!(
            "(SELECT *, TRUE AS {} FROM {} AS __repark_src_inner) AS {}",
            self.match_flag, self.spec.source_from_sql, self.spec.source_alias
        )
    }

    pub(super) fn matched(&self) -> String {
        format!("{}.{} IS NOT NULL", self.spec.source_alias, self.match_flag)
    }

    pub(super) fn applies(predicate_sql: Option<&str>) -> String {
        predicate_sql.map_or_else(|| "TRUE".to_string(), |p| format!("COALESCE(({p}), FALSE)"))
    }

    pub(super) fn clause_id_case(predicates: &[Option<&str>]) -> String {
        if predicates.is_empty() {
            return "CAST(NULL AS BIGINT)".to_string();
        }
        let branches: Vec<String> = predicates
            .iter()
            .enumerate()
            .map(|(index, predicate)| format!("WHEN {} THEN {index}", Self::applies(*predicate)))
            .collect();
        format!("CASE {} END", branches.join(" "))
    }

    #[cfg(test)]
    pub(super) fn prior_clauses_do_not_apply(predicates: &[Option<&str>], index: usize) -> String {
        if index == 0 {
            return "TRUE".to_string();
        }
        let clause_id = Self::clause_id_case(predicates);
        format!("(({clause_id}) IS NULL OR ({clause_id}) >= {index})")
    }

    #[cfg(test)]
    pub(super) fn prior_clauses_do_not_apply_legacy(
        predicates: &[Option<&str>],
        index: usize,
    ) -> String {
        let negations: Vec<String> = predicates[..index]
            .iter()
            .map(|predicate| format!("NOT {}", Self::applies(*predicate)))
            .collect();
        if negations.is_empty() {
            "TRUE".to_string()
        } else {
            negations.join(" AND ")
        }
    }

    pub(super) fn matched_predicates(&self) -> Vec<Option<&str>> {
        self.spec
            .matched
            .iter()
            .map(|clause| clause.predicate_sql.as_deref())
            .collect()
    }

    fn matched_clause_id_expr(&self) -> String {
        let predicates = self.matched_predicates();
        if predicates.is_empty() {
            return "CAST(NULL AS BIGINT)".to_string();
        }
        let mut branches = Vec::with_capacity(predicates.len() + 1);
        branches.push(format!("WHEN NOT ({}) THEN NULL", self.matched()));
        for (index, predicate) in predicates.iter().enumerate() {
            branches.push(format!("WHEN {} THEN {index}", Self::applies(*predicate)));
        }
        format!("CASE {} END", branches.join(" "))
    }

    fn update_assignment_lookup(&self) -> Vec<Option<HashMap<String, &str>>> {
        self.spec
            .matched
            .iter()
            .map(|clause| match &clause.action {
                MatchedAction::Update { assignments } => {
                    let mut map = HashMap::with_capacity(assignments.len());
                    for (name, expr) in assignments {
                        map.insert(name.to_ascii_lowercase(), expr.as_str());
                    }
                    Some(map)
                }
                MatchedAction::UpdateAll | MatchedAction::Delete => None,
            })
            .collect()
    }

    pub(super) fn rewrite_projection(&self, write_schema: &ArrowSchema) -> String {
        let maps = self.update_assignment_lookup();
        let user = write_schema
            .fields()
            .iter()
            .map(|field| {
                self.rewrite_column_with_maps(&maps, field.name(), Some(field.data_type()))
            })
            .collect::<Vec<_>>()
            .join(", ");
        row_lineage::maybe_append_lineage_projection(self, user)
    }

    pub(super) fn match_discovery_sql(&self) -> String {
        let ta = &self.spec.target_alias;
        format!(
            "SELECT {ta}.{FILE_PATH_COL}, {ta}.{POS_COL}, \
             count(*) AS match_count, \
             MAX(CASE WHEN ({mutated}) THEN 1 ELSE 0 END) AS is_mutated \
             FROM {source} JOIN {target} ON {on} \
             GROUP BY {ta}.{FILE_PATH_COL}, {ta}.{POS_COL}",
            target = self.target_from(),
            source = self.source_from(),
            on = self.spec.on_sql,
            mutated = self.mutated(),
        )
    }

    pub(super) fn mutated(&self) -> String {
        self.spec
            .matched
            .iter()
            .map(|clause| Self::applies(clause.predicate_sql.as_deref()))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    pub(super) fn matched_work_sql(&self, write_schema: &ArrowSchema) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        format!(
            "SELECT {ta}.{FILE_PATH_COL}, {ta}.{POS_COL}, \
             count(*) OVER (PARTITION BY {ta}.{FILE_PATH_COL}, {ta}.{POS_COL}) \
               AS match_count, \
             CASE WHEN ({mutated}) THEN 1 ELSE 0 END AS is_mutated, \
             CASE WHEN ({updated}) THEN 1 ELSE 0 END AS is_update, \
             {projection} \
             FROM {source} JOIN {target} ON {on}",
            target = self.target_from(),
            source = self.source_from(),
            on = self.spec.on_sql,
            mutated = self.mutated(),
            updated = self.update_applies(),
        )
    }

    pub(super) fn update_applies(&self) -> String {
        let update_ids: Vec<String> = self
            .spec
            .matched
            .iter()
            .enumerate()
            .filter(|(_, clause)| matches!(clause.action, MatchedAction::Update { .. }))
            .map(|(index, _)| index.to_string())
            .collect();
        if update_ids.is_empty() {
            "FALSE".to_string()
        } else if update_ids.len() == 1 {
            format!(
                "COALESCE(({}) = {}, FALSE)",
                self.matched_clause_id_expr(),
                update_ids[0]
            )
        } else {
            format!(
                "COALESCE(({}) IN ({}), FALSE)",
                self.matched_clause_id_expr(),
                update_ids.join(", ")
            )
        }
    }

    pub(super) fn rewrite_sql_allowlisted(
        &self,
        target_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let quoted_target = cow_scratch::quote_scratch_name(target_table_name);
        format!(
            "SELECT {projection} FROM {quoted_target} AS {ta} LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
        )
    }

    pub(super) fn rewrite_sql_path_semijoin(
        &self,
        target_table_name: &str,
        path_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let quoted_target = cow_scratch::quote_scratch_name(target_table_name);
        let quoted_paths = cow_scratch::quote_scratch_name(path_table_name);
        format!(
            "SELECT {projection} FROM {quoted_target} AS {ta} \
             INNER JOIN {quoted_paths} AS __repark_aff \
               ON {ta}.{FILE_PATH_COL} = __repark_aff.{path_col} \
             LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
            path_col = cow_scratch::AFFECTED_PATHS_COL,
        )
    }

    #[cfg(test)]
    pub(super) fn rewrite_column(&self, column: &str) -> String {
        let maps = self.update_assignment_lookup();
        self.rewrite_column_with_maps(&maps, column, None)
    }

    fn rewrite_column_with_maps(
        &self,
        maps: &[Option<HashMap<String, &str>>],
        column: &str,
        store_type: Option<&DataType>,
    ) -> String {
        let ta = &self.spec.target_alias;
        let quoted = quote_ident(column);
        let original = format!("{ta}.{quoted}");
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
        let else_expr = not_matched_by_source::rewrite_else(self, column, &original, store_type);
        if branches.is_empty() {
            format!("{else_expr} AS {quoted}")
        } else {
            format!(
                "CASE ({clause_id}) {} ELSE {else_expr} END AS {quoted}",
                branches.join(" "),
                clause_id = self.matched_clause_id_expr(),
            )
        }
    }

    pub(super) fn delete_applies(&self) -> String {
        let delete_ids: Vec<String> = self
            .spec
            .matched
            .iter()
            .enumerate()
            .filter(|(_, clause)| matches!(clause.action, MatchedAction::Delete))
            .map(|(index, _)| index.to_string())
            .collect();
        let matched_delete = if delete_ids.is_empty() {
            "FALSE".to_string()
        } else if delete_ids.len() == 1 {
            format!(
                "COALESCE(({}) = {}, FALSE)",
                self.matched_clause_id_expr(),
                delete_ids[0]
            )
        } else {
            format!(
                "COALESCE(({}) IN ({}), FALSE)",
                self.matched_clause_id_expr(),
                delete_ids.join(", ")
            )
        };
        not_matched_by_source::combined_delete_applies(&matched_delete, self)
    }

    pub(super) fn insert_sql(
        &self,
        index: usize,
        table: &Table,
        schema: &ArrowSchema,
    ) -> Result<String> {
        let clause = &self.spec.not_matched[index];
        let projection = table_projection(clause, table, schema, self.spec.case_insensitive)?;
        let predicates: Vec<Option<&str>> = self
            .spec
            .not_matched
            .iter()
            .map(|c| c.predicate_sql.as_deref())
            .collect();
        let clause_id = Self::clause_id_case(&predicates);
        Ok(format!(
            "SELECT {projection} FROM (SELECT {sa}.*, {ta}.{POS_COL} AS {NOT_MATCHED_POS_SENTINEL} \
             FROM {source} LEFT JOIN {target} ON {on}) AS {sa} \
             WHERE {NOT_MATCHED_POS_SENTINEL} IS NULL AND ({clause_id}) = {index}",
            source = self.source_from(),
            target = self.target_from(),
            on = self.spec.on_sql,
            ta = self.spec.target_alias,
            sa = self.spec.source_alias,
        ))
    }
}

const NOT_MATCHED_POS_SENTINEL: &str = "__repark_not_matched_pos";
