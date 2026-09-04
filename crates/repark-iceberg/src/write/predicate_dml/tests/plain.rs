use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::super::plain::try_allowed_plain_identity;

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

#[test]
fn plain_where_delete_is_identity_dml() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id = 0",
    ))
    .expect("plain delete must parse")
    .expect("plain delete must be identity DML");
    assert_eq!(allowed.catalog_name, "ice");
    assert_eq!(allowed.spec.target.name(), "puredv");
    assert!(allowed.spec.assignments.is_none());
}

#[test]
fn plain_where_update_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "UPDATE ice.sales.puredv SET name = 'z' WHERE id = 0",
    ))
    .expect("plain update must parse");
    assert!(allowed.is_none());
}

#[test]
fn literal_in_list_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id IN (1, 2, 3)",
    ))
    .expect("in-list delete must parse");
    assert!(allowed.is_none());
}

#[test]
fn branch_selector_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.t.branch_b WHERE id = 0",
    ))
    .expect("branch delete must parse");
    assert!(allowed.is_none());
}

#[test]
fn subquery_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id IN (SELECT id FROM ice.sales.src)",
    ))
    .expect("subquery delete must parse");
    assert!(allowed.is_none());
}
