use super::common::*;

use std::collections::HashSet;

use datafusion::prelude::DataFrame;
use repark_common::spark_error;

use crate::write_options::StatementWriteOptions;

struct NamedViews {
    names: Vec<&'static str>,
}

impl repark_core::TempViewSession for NamedViews {
    fn create_or_replace_temp_view_from(
        &self,
        _name: &str,
        _frame: &DataFrame,
    ) -> repark_common::Result<()> {
        Ok(())
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        let name = name.trim_matches('`').to_lowercase();
        Ok(self
            .names
            .contains(&name.as_str())
            .then(|| vec!["datafusion".to_string(), "public".to_string(), name.clone()]))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(self.names.iter().map(ToString::to_string).collect())
    }

    fn drop_temp_view(&self, _name: &str) -> repark_common::Result<bool> {
        Ok(false)
    }
}

const HEADS: [(&str, &str); 4] = [
    ("drop", "DROP TABLE {}"),
    ("truncate", "TRUNCATE TABLE {}"),
    ("delete", "DELETE FROM {} WHERE id = 1"),
    ("select", "SELECT * FROM {}"),
];

async fn outcome(views: &NamedViews, sql: &str) -> String {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    match crate::router::execute_in_session(
        &ctx,
        &catalogs,
        sql,
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        Some(views),
    )
    .await
    {
        Ok(frame) => match frame.collect().await {
            Ok(_) => "ok".to_string(),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

fn shown(message: &str) -> String {
    format!("{message:?}").trim_matches('"').to_string()
}

fn parse_refusal(near: &str) -> String {
    shown(&spark_error::message(
        spark_error::PARSE_SYNTAX_ERROR,
        &[("near", near)],
    ))
}

fn invalid_identifier(ident: &str) -> String {
    shown(&format!(
        "[INVALID_IDENTIFIER] The unquoted identifier {ident} is invalid and must be back quoted as: `{ident}`."
    ))
}

fn expected_refusal(head: &str, spelling: &str) -> Option<String> {
    let select = head == "select";
    match spelling {
        "\"t\"" | "ns.\"t\"" | "ice.ns.\"t\"" => Some(parse_refusal("'\"t\"'")),
        "'t'" if select => Some(parse_refusal("''t'': extra input ''t''")),
        "'t'" => Some(parse_refusal("''t''")),
        "ice.ns.'t'" if select => Some(parse_refusal("''t''")),
        "ice.ns.'t'" => Some(parse_refusal("'.'")),
        "\"ice\".ns.t" => Some(parse_refusal("'\"ice\"'")),
        "a, t" if !select => Some(parse_refusal("','")),
        "ü1" | "ｔ" => Some(invalid_identifier(spelling)),
        _ => None,
    }
}

const SPELLINGS: [&str; 16] = [
    "\"t\"",
    "'t'",
    "ns.\"t\"",
    "ice.ns.\"t\"",
    "ice.ns.'t'",
    "\"ice\".ns.t",
    "a, t",
    "ü1",
    "ｔ",
    "1t",
    "t -- c",
    "t/* c */",
    "t",
    "`t`",
    "`a``b`",
    "ns.t",
];

#[tokio::test]
async fn every_head_reads_a_table_identifier_with_spark_grammar() {
    for (head, template) in HEADS {
        for spelling in SPELLINGS {
            let sql = template.replace("{}", spelling);
            for views in [
                NamedViews { names: Vec::new() },
                NamedViews {
                    names: vec!["t", "1t", "ü1", "ｔ"],
                },
            ] {
                let outcome = outcome(&views, &sql).await;
                match expected_refusal(head, spelling) {
                    Some(refusal) => assert!(outcome.contains(&refusal), "{sql}: {outcome}"),
                    None => {
                        for refused in [
                            "PARSE_SYNTAX_ERROR",
                            "INVALID_IDENTIFIER",
                            "invalid table identifier",
                            "unexpected character",
                        ] {
                            assert!(!outcome.contains(refused), "{sql}: {outcome}");
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn the_grammar_refuses_a_target_on_heads_that_need_more_and_on_a_second_target() {
    let views = NamedViews { names: Vec::new() };
    for (sql, near) in [
        ("INSERT INTO t -- c VALUES (5, 'q')", "end of input"),
        ("UPDATE t -- c SET v = 'u'", "end of input"),
        ("ALTER TABLE t -- c ADD COLUMN z INT", "end of input"),
        ("INSERT INTO a, t VALUES (5, 'q')", "','"),
        ("MERGE INTO a, t USING s ON a.id = s.id", "','"),
        ("INSERT INTO ice.ns.'t' VALUES (1)", "''t''"),
        ("UPDATE ice.ns.'t' SET v = 'u'", "'.'"),
        ("SELECT * FROM a JOIN \"t\" ON 1 = 1", "'\"t\"'"),
        (
            "INSERT OVERWRITE t SELECT a.id, 'z' FROM a JOIN \"a\" x ON 1 = 1",
            "'\"a\"'",
        ),
        ("SELECT * FROM a, \"t\"", "'\"t\"'"),
        (
            "INSERT OVERWRITE t SELECT id, 'z' FROM (SELECT 1 AS k) q, \"a\"",
            "'\"a\"'",
        ),
        (
            "INSERT OVERWRITE t SELECT id, 'z' FROM (SELECT 1 AS k) q, 'a'",
            "''a''",
        ),
        (
            "DELETE FROM t WHERE id IN (SELECT 1 FROM a, \"a\")",
            "'\"a\"'",
        ),
        (
            "UPDATE t SET v = 'u' WHERE id IN (SELECT 1 FROM a, \"a\")",
            "'\"a\"'",
        ),
        (
            "MERGE INTO t USING (SELECT a.id FROM a, \"a\" x) s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "'\"a\"'",
        ),
    ] {
        let outcome = outcome(&views, sql).await;
        assert!(outcome.contains(&parse_refusal(near)), "{sql}: {outcome}");
    }
    for sql in [
        "SELECT TRIM(BOTH 'x' FROM \"xax\")",
        "SELECT 'a' IS DISTINCT FROM 'b'",
        "SELECT EXTRACT(YEAR FROM '2020-01-01')",
        "SELECT * FROM range(3)",
        "DELETE FROM t -- c WHERE id = 1",
        "SELECT t.id FROM a, `t`",
        "SELECT q.k FROM (SELECT 1 AS k) q, 1t",
        "SELECT t.id FROM a JOIN `t` ON a.id = t.id",
    ] {
        let outcome = outcome(&views, sql).await;
        for refused in ["PARSE_SYNTAX_ERROR", "INVALID_IDENTIFIER"] {
            assert!(!outcome.contains(refused), "{sql}: {outcome}");
        }
    }
    let unicode = "INSERT OVERWRITE t SELECT q.k, 'z' FROM (SELECT 1 AS k) q, ü1";
    let outcome = outcome(&views, unicode).await;
    assert!(
        outcome.contains(&invalid_identifier("ü1")),
        "{unicode}: {outcome}"
    );
}
