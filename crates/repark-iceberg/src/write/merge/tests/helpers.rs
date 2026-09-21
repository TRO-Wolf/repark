use super::super::*;
use iceberg::NamespaceIdent;

pub(super) fn spec(matched: Vec<MatchedClause>, not_matched: Vec<InsertClause>) -> MergeSpec {
    MergeSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string()),
        target_alias: "t".to_string(),
        source_from_sql: "src".to_string(),
        source_alias: "s".to_string(),
        on_sql: "t.id = s.id".to_string(),
        matched,
        not_matched,
        not_matched_by_source: vec![],
        commit_branch: None,
        case_insensitive: true,
        schema_evolution: false,
    }
}

pub(super) fn update(predicate: Option<&str>, sets: &[(&str, &str)]) -> MatchedClause {
    MatchedClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: MatchedAction::Update {
            assignments: sets
                .iter()
                .map(|(c, e)| ((*c).to_string(), (*e).to_string()))
                .collect(),
        },
    }
}

pub(super) fn delete(predicate: Option<&str>) -> MatchedClause {
    MatchedClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: MatchedAction::Delete,
    }
}
