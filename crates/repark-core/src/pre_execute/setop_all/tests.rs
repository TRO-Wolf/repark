use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::config::Dialect as ParserDialect;
use datafusion::error::Result as DfResult;
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::dialect::{DatabricksDialect, Dialect, GenericDialect};

use super::{MultisetSetOperation, multiset_set_operation, refuse_multiset_set_operation};
use crate::dialect::{DataFusionDialect, EngineContext, SqlDialect};
use crate::{Error, ReparkSession};

const LEFT: &str = "SELECT a FROM (VALUES (1), (1), (1), (2), (2), (4)) AS l(a)";
const RIGHT: &str = "SELECT a FROM (VALUES (1), (2), (2), (3)) AS r(a)";

fn dialects() -> [Box<dyn Dialect>; 2] {
    [Box::new(GenericDialect {}), Box::new(DatabricksDialect {})]
}

fn shaped(op: &str) -> Vec<String> {
    let q = format!("SELECT a FROM l {op} SELECT a FROM r");
    vec![
        q.clone(),
        format!("WITH x AS ({q}) SELECT a FROM x"),
        format!("SELECT (SELECT count(*) FROM ({q}) x) AS c"),
        format!("SELECT a FROM r WHERE a IN ({q})"),
        format!("CREATE OR REPLACE TEMPORARY VIEW v AS {q}"),
        format!("CREATE VIEW ice.ns.v AS {q}"),
        format!("INSERT INTO ice.ns.t {q}"),
        format!("CREATE TABLE ice.ns.c USING iceberg AS {q}"),
        format!("MERGE INTO ice.ns.m t USING ({q}) s ON t.a = s.a WHEN NOT MATCHED THEN INSERT *"),
        format!("SELECT a FROM (SELECT 9 AS a UNION ALL ({q})) x"),
        format!("EXPLAIN {q}"),
    ]
}

#[test]
fn every_shape_and_spelling_of_a_multiset_operation_is_found_in_both_parser_dialects() {
    let spellings = [
        ("INTERSECT ALL", MultisetSetOperation::Intersect),
        ("intersect all", MultisetSetOperation::Intersect),
        ("Intersect All", MultisetSetOperation::Intersect),
        ("INTERSECT /* gap */ ALL", MultisetSetOperation::Intersect),
        ("EXCEPT ALL", MultisetSetOperation::Except),
        ("ExCePt AlL", MultisetSetOperation::Except),
        ("EXCEPT\n  ALL", MultisetSetOperation::Except),
        ("EXCEPT -- gap\n ALL", MultisetSetOperation::Except),
        ("MINUS ALL", MultisetSetOperation::Minus),
        ("MiNuS aLl", MultisetSetOperation::Minus),
    ];
    for dialect in dialects() {
        for (op, expected) in spellings {
            for sql in shaped(op) {
                assert_eq!(
                    multiset_set_operation(dialect.as_ref(), &sql),
                    Some(expected),
                    "{dialect:?}: {sql}"
                );
            }
        }
    }
}

#[test]
fn controls_and_text_that_only_mentions_the_words_are_not_found() {
    let mut controls: Vec<String> = [
        "UNION ALL",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "EXCEPT DISTINCT",
        "INTERSECT DISTINCT",
        "MINUS",
    ]
    .iter()
    .flat_map(|op| shaped(op))
    .collect();
    controls.extend(
        [
            "SELECT 'a INTERSECT ALL b' AS s",
            "SELECT 1 AS a /* EXCEPT ALL */",
            "SELECT 1 AS a -- MINUS ALL\n",
            "SELECT 1 AS \"except all\"",
            "SELECT a AS all_rows FROM l INTERSECT SELECT a FROM r",
            "SELECT a FROM l INTERSECT SELECT all_a FROM r",
            "SELECT a FROM l EXCEPT SELECT a FROM r WHERE a = ALL (SELECT 1)",
            "SELECT DISTINCT a FROM l ORDER BY ALL",
            "SELECT 1 AS minus",
        ]
        .map(str::to_string),
    );
    for dialect in dialects() {
        for sql in &controls {
            assert_eq!(
                multiset_set_operation(dialect.as_ref(), sql),
                None,
                "{dialect:?}: {sql}"
            );
        }
    }
    assert_eq!(
        multiset_set_operation(&DatabricksDialect {}, "SELECT 1 AS `intersect all`"),
        None
    );
    assert_eq!(
        multiset_set_operation(&GenericDialect {}, "SELECT 'unterminated INTERSECT ALL"),
        None
    );
}

#[test]
fn the_refusal_names_the_operator_the_distinct_form_the_workaround_and_the_card() {
    let expected = [
        (
            MultisetSetOperation::Intersect,
            "SQL INTERSECT ALL multiset semantics are not Spark-correct on this engine yet; use \
             INTERSECT for distinct bags, or number each side's duplicates with row_number() and \
             INTERSECT the numbered rows (SQL-SETOP-ALL-MULTISET-1)",
        ),
        (
            MultisetSetOperation::Except,
            "SQL EXCEPT ALL multiset semantics are not Spark-correct on this engine yet; use \
             EXCEPT for distinct bags, or number each side's duplicates with row_number() and \
             EXCEPT the numbered rows (SQL-SETOP-ALL-MULTISET-1)",
        ),
        (
            MultisetSetOperation::Minus,
            "SQL MINUS ALL multiset semantics are not Spark-correct on this engine yet; use \
             EXCEPT for distinct bags, or number each side's duplicates with row_number() and \
             EXCEPT the numbered rows (SQL-SETOP-ALL-MULTISET-1)",
        ),
    ];
    for (operation, text) in expected {
        assert_eq!(operation.refusal_message(), text);
    }
    let refused =
        refuse_multiset_set_operation(ParserDialect::Databricks, "SELECT 1 EXCEPT ALL SELECT 2")
            .unwrap_err();
    assert_eq!(
        unsupported_text(crate::engine_err(refused)),
        MultisetSetOperation::Except.refusal_message()
    );
    refuse_multiset_set_operation(ParserDialect::Generic, "SELECT 1 EXCEPT SELECT 2").unwrap();
}

fn unsupported_text(error: Error) -> String {
    assert_eq!(
        error.exception_class(),
        crate::ErrorClass::Unsupported,
        "{error:?}"
    );
    match error {
        Error::NotImplemented(message) => message,
        other => panic!("not an unsupported refusal: {other:?}"),
    }
}

#[derive(Default)]
struct Counting {
    calls: AtomicUsize,
}

#[async_trait(?Send)]
impl SqlDialect for Counting {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> DfResult<DataFrame> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        DataFusionDialect.execute(cx, query).await
    }

    async fn execute_with_write_options(
        &self,
        cx: EngineContext<'_>,
        query: &str,
        _options: &HashMap<String, String>,
    ) -> DfResult<DataFrame> {
        self.execute(cx, query).await
    }
}

fn session(counting: &Arc<Counting>, parser: &str, guarded: bool) -> ReparkSession {
    let builder = ReparkSession::builder()
        .config("datafusion.sql_parser.dialect", parser)
        .with_sql_dialect(Arc::clone(counting) as Arc<dyn SqlDialect>);
    let builder = if guarded {
        builder.with_local_fs_guard()
    } else {
        builder
    };
    builder.build().unwrap()
}

async fn count_of(session: &ReparkSession, sql: &str) -> i64 {
    let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
    let column = batches[0].column(0);
    let counts = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(counts.len(), 1);
    counts.value(0)
}

#[tokio::test]
async fn the_session_seat_refuses_before_the_door_and_lets_the_controls_through() {
    for (parser, guarded) in [("generic", false), ("generic", true), ("databricks", false)] {
        let counting = Arc::new(Counting::default());
        let session = session(&counting, parser, guarded);
        for (op, operation) in [
            ("INTERSECT ALL", MultisetSetOperation::Intersect),
            ("except all", MultisetSetOperation::Except),
            ("MINUS ALL", MultisetSetOperation::Minus),
        ] {
            let sql = format!("SELECT count(*) FROM ({LEFT} {op} {RIGHT}) x");
            let error = session.sql(&sql).await.unwrap_err();
            assert_eq!(
                unsupported_text(error),
                operation.refusal_message(),
                "{parser}"
            );
        }
        assert_eq!(counting.calls.load(Ordering::SeqCst), 0, "{parser}");
        let controls = [
            ("INTERSECT", 2),
            ("EXCEPT", 1),
            ("UNION ALL", 10),
            ("UNION", 4),
        ];
        for (op, expected) in controls {
            let sql = format!("SELECT count(*) FROM ({LEFT} {op} {RIGHT}) x");
            assert_eq!(count_of(&session, &sql).await, expected, "{parser} {op}");
        }
        assert_eq!(
            counting.calls.load(Ordering::SeqCst),
            controls.len(),
            "{parser}"
        );
    }
}

#[tokio::test]
async fn a_refused_insert_or_ctas_writes_nothing() {
    let counting = Arc::new(Counting::default());
    let session = session(&counting, "generic", false);
    session
        .sql("CREATE TABLE t (a BIGINT)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let insert = format!("INSERT INTO t {LEFT} INTERSECT ALL {RIGHT}");
    assert_eq!(
        unsupported_text(session.sql(&insert).await.unwrap_err()),
        MultisetSetOperation::Intersect.refusal_message()
    );
    assert_eq!(count_of(&session, "SELECT count(*) FROM t").await, 0);
    let ctas = format!("CREATE TABLE c AS {LEFT} EXCEPT ALL {RIGHT}");
    assert_eq!(
        unsupported_text(session.sql(&ctas).await.unwrap_err()),
        MultisetSetOperation::Except.refusal_message()
    );
    assert!(!session.context().table_exist("c").unwrap());
    let control = format!("INSERT INTO t {LEFT} EXCEPT {RIGHT}");
    session
        .sql(&control)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(count_of(&session, "SELECT count(*) FROM t").await, 1);
}
