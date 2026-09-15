use super::super::*;
use super::merge::{merge_sql, spec, update};

use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

fn insert(columns: &[&str], values: &[&str]) -> InsertClause {
    InsertClause {
        predicate_sql: None,
        action: InsertAction::Explicit {
            columns: columns.iter().map(ToString::to_string).collect(),
            values_sql: values.iter().map(ToString::to_string).collect(),
        },
    }
}

#[test]
fn merge_internal_statements_parse_under_the_spark_dialect() {
    let schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let owned = spec(
        vec![update(None, &[("name", "s.name")])],
        vec![insert(&["id", "name"], &["s.id", "s.name"])],
    );
    let sql = merge_sql(&owned);
    let queries = [
        sql.match_discovery_sql(),
        sql.insert_sql(0, &schema).expect("insert"),
        sql.rewrite_sql_allowlisted("scoped_target", &schema),
        sql.rewrite_sql_path_semijoin("scratch", "aff_paths", &schema),
    ];
    for query in &queries {
        assert!(
            !query.contains('"'),
            "MERGE internal SQL must not carry double-quoted identifiers, got: {query}"
        );
        Parser::parse_sql(&DatabricksDialect {}, query)
            .expect("MERGE internal SQL must parse under the Spark dialect");
    }
}
