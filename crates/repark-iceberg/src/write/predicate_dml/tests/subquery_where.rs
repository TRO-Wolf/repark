use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::super::subquery_where::refuse_dml_subquery_predicate;

const IDENTITY_FORMS: [&str; 5] = [
    "UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s)",
    "DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
    "DELETE FROM ice.sales.t WHERE EXISTS (SELECT 1 FROM ice.sales.s WHERE s.id = t.id)",
    "DELETE FROM ice.sales.t WHERE id NOT IN (SELECT id FROM ice.sales.s)",
    "DELETE FROM ice.sales.t WHERE EXISTS (SELECT 1 FROM ice.sales.s WHERE s.id = 100)",
];

const REFUSED_EITHER_WAY: [&str; 6] = [
    "UPDATE ice.sales.t SET v = 5 WHERE id = (SELECT max(id) FROM ice.sales.s)",
    "UPDATE ice.sales.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.sales.s WHERE s.id = t.id)",
    "UPDATE ice.sales.t SET v = 5 WHERE id NOT IN (SELECT id FROM ice.sales.s)",
    "DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s) AND v > 25",
    "UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)",
    "DELETE FROM t WHERE id IN (SELECT id FROM s)",
];

const PASSES_EITHER_WAY: [&str; 6] = [
    "DELETE FROM ice.sales.t",
    "DELETE FROM ice.sales.t WHERE id = 2",
    "UPDATE ice.sales.t SET v = 5 WHERE id IN (1, 3)",
    "UPDATE ice.sales.t SET v = (SELECT max(v) FROM ice.sales.s) WHERE id = 2",
    "SELECT id FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
    "INSERT INTO ice.sales.t SELECT id, w FROM ice.sales.s WHERE id IN (SELECT id FROM ice.sales.t)",
];

const UPDATE_TEXT: &str = "UPDATE with a subquery in its WHERE clause is refused on \
     `ice.sales.t`: subquery predicates are silently mis-executed today — DataFusion's DML \
     planner decorrelates IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a \
     semi-join and then recovers NO filter for the Iceberg writer, so this statement would update \
     EVERY row of the table instead of only the matching ones (defect G3-E8, silent data loss). \
     Rewrite it as `MERGE INTO ice.sales.t AS target USING (<the subquery>) AS source ON <join \
     keys> WHEN MATCHED THEN UPDATE SET <assignments>` — the RePark-owned MERGE executor never \
     crosses that seam, and it is the dbt adapter's proven vehicle. Support returns when the \
     underlying fix lands; non-subquery UPDATE predicates are unaffected.";

const DELETE_TEXT: &str = "DELETE with a subquery in its WHERE clause is refused on \
     `ice.sales.t`: subquery predicates are silently mis-executed today — DataFusion's DML \
     planner decorrelates IN / NOT IN / EXISTS / ANY / ALL / correlated predicates into a \
     semi-join and then recovers NO filter for the Iceberg writer, so this statement would delete \
     EVERY row of the table instead of only the matching ones (defect G3-E8, silent data loss). \
     Rewrite it as `MERGE INTO ice.sales.t AS target USING (<the subquery>) AS source ON <join \
     keys> WHEN MATCHED THEN DELETE` — the RePark-owned MERGE executor never crosses that seam, \
     and it is the dbt adapter's proven vehicle. Support returns when the underlying fix lands; \
     non-subquery DELETE predicates are unaffected.";

fn parsed(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

fn refusal(sql: &str, identity_forms_execute: bool) -> String {
    refuse_dml_subquery_predicate(&parsed(sql), identity_forms_execute)
        .expect_err(sql)
        .to_string()
}

#[test]
fn the_identity_exemption_passes_the_allow_listed_forms_and_only_with_it() {
    for sql in IDENTITY_FORMS {
        refuse_dml_subquery_predicate(&parsed(sql), true)
            .unwrap_or_else(|error| panic!("{sql}: the exemption must pass it: {error}"));
        assert!(
            refusal(sql, false).contains("subquery predicates are silently mis-executed"),
            "{sql}"
        );
    }
}

#[test]
fn a_form_outside_the_allow_list_refuses_with_or_without_the_exemption() {
    for sql in REFUSED_EITHER_WAY {
        for identity_forms_execute in [true, false] {
            assert!(
                refusal(sql, identity_forms_execute)
                    .contains("subquery predicates are silently mis-executed"),
                "{sql}, exemption {identity_forms_execute}"
            );
        }
    }
}

#[test]
fn a_statement_with_no_subquery_in_a_dml_where_passes_with_or_without_the_exemption() {
    for sql in PASSES_EITHER_WAY {
        for identity_forms_execute in [true, false] {
            refuse_dml_subquery_predicate(&parsed(sql), identity_forms_execute).unwrap_or_else(
                |error| panic!("{sql}, exemption {identity_forms_execute}: {error}"),
            );
        }
    }
}

#[test]
fn the_refusal_text_is_the_g3_e8_text_for_both_verbs_and_the_fallback_target() {
    let update = refusal(
        "UPDATE ice.sales.t SET v = 5 WHERE id = (SELECT max(id) FROM ice.sales.s)",
        false,
    );
    assert_eq!(update, format!("Error during planning: {UPDATE_TEXT}"));
    let delete = refusal(
        "DELETE FROM ice.sales.t WHERE id = (SELECT max(id) FROM ice.sales.s)",
        false,
    );
    assert_eq!(delete, format!("Error during planning: {DELETE_TEXT}"));
    let fallback = refusal(
        "DELETE FROM (SELECT 1 AS id) AS x WHERE id IN (SELECT id FROM ice.sales.s)",
        false,
    );
    assert!(fallback.contains("is refused on `<table>`"), "{fallback}");
    assert!(
        fallback.contains("MERGE INTO <table> AS target"),
        "{fallback}"
    );
}
