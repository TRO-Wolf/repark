use datafusion::sql::parser::{DFParser, Statement as DFStatement};

use super::refuse_wrapped_write;

const EXPLAIN_ANALYZE_TEXT: &str = "EXPLAIN ANALYZE is refused: its statement writes to a table, \
     and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same statement \
     still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.";

const PREPARE_TEXT: &str = "PREPARE `p` is refused: its statement writes to a table, and a \
     prepared write would run later, at EXECUTE. PREPARE of a query still works.";

const WRITES: [&str; 8] = [
    "DELETE FROM t WHERE id = 2",
    "DELETE FROM t WHERE id IN (SELECT id FROM s)",
    "UPDATE t SET v = 5 WHERE id = 2",
    "UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)",
    "INSERT INTO t VALUES (5, 50)",
    "INSERT INTO t SELECT id, w FROM s WHERE id IN (SELECT id FROM s)",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.w",
    "WITH c AS (SELECT 2 AS id) DELETE FROM t WHERE id IN (SELECT id FROM c)",
];

fn parsed(sql: &str) -> DFStatement {
    DFParser::parse_sql(sql).expect(sql).pop_front().expect(sql)
}

fn refusal(sql: &str) -> String {
    refuse_wrapped_write(&parsed(sql))
        .expect_err(sql)
        .to_string()
}

#[test]
fn an_explain_analyze_of_a_write_refuses_with_its_text() {
    for write in WRITES {
        for statement in [
            format!("EXPLAIN ANALYZE {write}"),
            format!("EXPLAIN ANALYZE VERBOSE {write}"),
            format!("EXPLAIN ANALYZE EXPLAIN ANALYZE {write}"),
            format!("EXPLAIN ANALYZE PREPARE p AS {write}"),
        ] {
            let text = refusal(&statement);
            assert!(text.contains(EXPLAIN_ANALYZE_TEXT), "{statement}: {text}");
        }
    }
}

#[test]
fn a_prepare_of_a_write_refuses_with_its_text() {
    for write in WRITES {
        for statement in [
            format!("PREPARE p AS {write}"),
            format!("PREPARE p AS EXPLAIN ANALYZE {write}"),
        ] {
            let text = refusal(&statement);
            assert!(text.contains(PREPARE_TEXT), "{statement}: {text}");
        }
    }
    let text = refusal("PREPARE p(INT) AS DELETE FROM t WHERE id = $1");
    assert!(text.contains(PREPARE_TEXT), "{text}");
    let named = refusal("PREPARE remove_one AS UPDATE t SET v = 1");
    assert!(named.contains("PREPARE `remove_one` is refused"), "{named}");
}

#[test]
fn a_query_a_plain_explain_and_a_bare_statement_answer_nothing() {
    let mut statements = vec![
        "SELECT * FROM t".to_string(),
        "EXPLAIN SELECT * FROM t".to_string(),
        "EXPLAIN ANALYZE SELECT * FROM t WHERE id IN (SELECT id FROM s)".to_string(),
        "EXPLAIN ANALYZE VERBOSE SELECT 1".to_string(),
        "PREPARE q AS SELECT v FROM t WHERE id = $1".to_string(),
        "EXECUTE p".to_string(),
        "EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO 'out' STORED AS PARQUET".to_string(),
        "PREPARE q AS CREATE VIEW v AS SELECT * FROM t".to_string(),
    ];
    for write in WRITES {
        statements.push(write.to_string());
        statements.push(format!("EXPLAIN {write}"));
        statements.push(format!("EXPLAIN VERBOSE {write}"));
        statements.push(format!("EXPLAIN ANALYZE EXPLAIN {write}"));
        statements.push(format!("PREPARE p AS EXPLAIN {write}"));
        statements.push(format!("EXPLAIN EXPLAIN ANALYZE {write}"));
    }
    for statement in statements {
        refuse_wrapped_write(&parsed(&statement)).expect(&statement);
    }
}

#[test]
fn neither_refusal_names_a_data_deleting_action() {
    for text in [EXPLAIN_ANALYZE_TEXT, PREPARE_TEXT] {
        let lowered = text.to_lowercase();
        for word in [
            "delete",
            "drop",
            "truncate",
            "purge",
            "expire",
            "remove",
            "overwrite",
        ] {
            assert!(!lowered.contains(word), "{word} in {text}");
        }
    }
    assert!(refusal("EXPLAIN ANALYZE DELETE FROM t").contains(EXPLAIN_ANALYZE_TEXT));
}
