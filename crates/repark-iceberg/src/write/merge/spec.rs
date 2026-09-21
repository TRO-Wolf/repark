use iceberg::TableIdent;

use crate::write::merge::not_matched_by_source::NotMatchedBySourceClause;

#[derive(Debug, Clone)]
pub struct MergeSpec {
    pub target: TableIdent,
    pub target_alias: String,
    pub source_from_sql: String,
    pub source_alias: String,
    pub on_sql: String,
    pub matched: Vec<MatchedClause>,
    pub not_matched: Vec<InsertClause>,
    pub not_matched_by_source: Vec<NotMatchedBySourceClause>,
    pub commit_branch: Option<String>,
    pub case_insensitive: bool,
    pub schema_evolution: bool,
}

#[derive(Debug, Clone)]
pub struct MatchedClause {
    pub predicate_sql: Option<String>,
    pub action: MatchedAction,
}

#[derive(Debug, Clone)]
pub enum MatchedAction {
    Update { assignments: Vec<(String, String)> },
    UpdateAll,
    Delete,
}

#[derive(Debug, Clone)]
pub struct InsertClause {
    pub predicate_sql: Option<String>,
    pub action: InsertAction,
}

#[derive(Debug, Clone)]
pub enum InsertAction {
    Explicit {
        columns: Vec<String>,
        values_sql: Vec<String>,
    },
    All,
}
