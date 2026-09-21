use super::*;

const UPSERT: &str = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";

#[test]
fn the_clause_is_stripped_and_the_rest_parses_as_an_ordinary_merge() {
    let sql = format!("MERGE WITH SCHEMA EVOLUTION INTO c.n.t t USING v s ON t.id = s.id {UPSERT}");
    let stripped = strip_schema_evolution(&sql).expect("clause present");
    assert_eq!(
        stripped,
        format!("MERGE INTO c.n.t t USING v s ON t.id = s.id {UPSERT}")
    );
    assert!(crate::starts_with_merge(&stripped));
}

#[test]
fn case_whitespace_and_comments_between_the_keywords_are_ignored() {
    let sql = format!(
        "merge   with\n /* note */ schema\t-- why\n evolution INTO c.n.t t USING v s \
         ON t.id = s.id {UPSERT}"
    );
    let stripped = strip_schema_evolution(&sql).expect("clause present");
    assert!(stripped.to_ascii_uppercase().starts_with("MERGE"));
    assert!(!stripped.to_ascii_uppercase().contains("SCHEMA EVOLUTION"));
    assert!(stripped.contains("INTO c.n.t t USING v s"));
}

#[test]
fn a_plain_merge_is_left_alone() {
    let sql = format!("MERGE INTO c.n.t t USING v s ON t.id = s.id {UPSERT}");
    assert!(strip_schema_evolution(&sql).is_none());
}

#[test]
fn the_words_only_count_immediately_after_merge() {
    let sql = "MERGE INTO c.n.t t USING v s ON t.id = s.id \
               WHEN MATCHED THEN UPDATE SET data = 'with schema evolution'";
    assert!(strip_schema_evolution(sql).is_none());
    let cte = "WITH s AS (SELECT 1) MERGE INTO c.n.t t USING s ON t.id = s.id \
               WHEN MATCHED THEN DELETE";
    assert!(strip_schema_evolution(cte).is_none());
}

#[test]
fn a_column_or_value_named_evolution_survives() {
    let sql = "MERGE INTO c.n.t t USING v s ON t.id = s.id \
               WHEN MATCHED THEN UPDATE SET t.evolution = s.schema";
    assert!(strip_schema_evolution(sql).is_none());
}

#[test]
fn a_partial_clause_is_not_stripped() {
    assert!(strip_schema_evolution("MERGE WITH SCHEMA INTO c.n.t t USING v s ON 1=1").is_none());
    assert!(
        strip_schema_evolution("MERGE SCHEMA EVOLUTION INTO c.n.t t USING v s ON 1=1").is_none()
    );
    assert!(strip_schema_evolution("MERGE WITH EVOLUTION SCHEMA INTO c.n.t t USING v s").is_none());
}

#[test]
fn a_non_merge_statement_is_never_touched() {
    assert!(strip_schema_evolution("SELECT 1").is_none());
    assert!(strip_schema_evolution("INSERT INTO t SELECT 'with schema evolution' AS a").is_none());
}
