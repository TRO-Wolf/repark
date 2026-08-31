//! pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005
//! SQL-fragment pins for the unmatched-by-source arm.

use super::super::*;
use super::merge::{delete, spec, update};
use crate::write::merge::not_matched_by_source::{
    NotMatchedBySourceAction, NotMatchedBySourceClause, combined_delete_applies, delete_applies,
};

fn nmbs_delete(predicate: Option<&str>) -> NotMatchedBySourceClause {
    NotMatchedBySourceClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: NotMatchedBySourceAction::Delete,
    }
}

fn nmbs_update(predicate: Option<&str>, sets: &[(&str, &str)]) -> NotMatchedBySourceClause {
    NotMatchedBySourceClause {
        predicate_sql: predicate.map(ToString::to_string),
        action: NotMatchedBySourceAction::Update {
            assignments: sets
                .iter()
                .map(|(column, expr)| ((*column).to_string(), (*expr).to_string()))
                .collect(),
        },
    }
}

fn spec_nmbs(matched: Vec<MatchedClause>, nmbs: Vec<NotMatchedBySourceClause>) -> MergeSpec {
    let mut spec = spec(matched, vec![]);
    spec.not_matched_by_source = nmbs;
    spec
}

#[test]
fn nmbs_delete_or_s_into_rewrite_where() {
    let spec = spec_nmbs(vec![delete(None)], vec![nmbs_delete(None)]);
    let sql = MergeSql {
        spec: &spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let deleted = sql.delete_applies();
    assert!(
        deleted.contains("OR"),
        "MATCHED DELETE and NMBS DELETE must combine, got {deleted}"
    );
    assert_ne!(delete_applies(&sql), "FALSE");
    let combined = combined_delete_applies("FALSE", &sql);
    assert_ne!(combined, "FALSE");
}

#[test]
fn nmbs_update_projects_else_branch() {
    let spec = spec_nmbs(
        vec![update(None, &[("name", "s.name")])],
        vec![nmbs_update(None, &[("name", "'gone'")])],
    );
    let sql = MergeSql {
        spec: &spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
    };
    let name_case = sql.rewrite_column("name");
    assert!(
        name_case.contains("'gone'"),
        "NMBS UPDATE must appear in the rewrite ELSE, got {name_case}"
    );
}

#[test]
fn skip_cardinality_ignores_nmbs_clauses() {
    assert!(skip_cardinality(&spec_nmbs(
        vec![delete(None)],
        vec![nmbs_delete(None)]
    )));
    assert!(!skip_cardinality(&spec_nmbs(
        vec![],
        vec![nmbs_delete(None)]
    )));
    assert!(!skip_cardinality(&spec_nmbs(
        vec![update(None, &[("name", "s.name")])],
        vec![nmbs_delete(None)]
    )));
}
