use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::{DatabricksDialect, Dialect, GenericDialect};

use super::{
    MultisetSetOperations, insert_multiset_set_operations, is_lossless_round_trip,
    minus_all_as_except_all, minus_all_with,
};
use crate::ReparkSession;
use crate::dialect::{DataFusionDialect, EngineContext, SqlDialect};
use datafusion::config::Dialect as ParserDialect;

const LEFT: &str = "SELECT a FROM (VALUES (1), (1), (1), (2), (2), (4)) AS l(a)";
const RIGHT: &str = "SELECT a FROM (VALUES (1), (2), (2), (3)) AS r(a)";

fn dialects() -> [Box<dyn Dialect>; 2] {
    [Box::new(GenericDialect {}), Box::new(DatabricksDialect {})]
}

fn shaped(op: &str) -> Vec<String> {
    let q = format!("SELECT 'é' AS e, a FROM l {op} SELECT 'ü', a FROM r");
    vec![
        q.clone(),
        format!("WITH x AS ({q}) SELECT a FROM x"),
        format!("SELECT (SELECT count(*) FROM ({q}) x) AS c"),
        format!("SELECT a FROM r WHERE a IN (\n  {q})"),
        format!("CREATE OR REPLACE TEMPORARY VIEW v AS {q}"),
        format!("CREATE VIEW ice.ns.v AS {q}"),
        format!("INSERT INTO ice.ns.t {q}"),
        format!("CREATE TABLE ice.ns.c USING iceberg AS {q}"),
        format!("MERGE INTO ice.ns.m t USING ({q}) s ON t.a = s.a WHEN NOT MATCHED THEN INSERT *"),
        format!("SELECT a FROM (SELECT 9 AS a UNION ALL ({q})) x"),
        format!("EXPLAIN {q}"),
        format!("{q} MINUS ALL {q}"),
    ]
}

#[test]
fn every_shape_and_spelling_of_minus_all_becomes_except_all_in_both_parser_dialects() {
    let spellings = [
        "MINUS ALL",
        "minus all",
        "MiNuS aLl",
        "MINUS /* gap */ ALL",
        "MINUS\n  ALL",
        "MINUS -- gap\n ALL",
    ];
    for dialect in dialects() {
        for op in spellings {
            for sql in shaped(op) {
                let expected = sql
                    .replace(op, &format!("EXCEPT{}", &op[5..]))
                    .replace(" MINUS ALL ", " EXCEPT ALL ");
                let rewritten = minus_all_with(dialect.as_ref(), &sql);
                assert!(matches!(rewritten, Cow::Owned(_)), "{dialect:?}: {sql}");
                assert_eq!(rewritten, expected, "{dialect:?}: {sql}");
            }
        }
    }
}

#[test]
fn text_that_only_mentions_minus_all_is_unchanged() {
    let texts = [
        "SELECT 'a MINUS ALL b' AS s",
        "SELECT 1 AS a /* MINUS ALL */",
        "SELECT 1 AS a -- MINUS ALL\n",
        "SELECT a AS all_rows FROM l MINUS SELECT a FROM r",
        "SELECT a FROM l MINUS SELECT a AS all FROM r",
        "SELECT a FROM l INTERSECT ALL SELECT a FROM r",
        "SELECT a FROM l EXCEPT ALL SELECT a FROM r",
        "SELECT a FROM t WHERE a = ALL (SELECT minus FROM u)",
        "SELECT 'unterminated MINUS ALL",
    ];
    for dialect in dialects() {
        for sql in texts {
            assert!(
                matches!(minus_all_with(dialect.as_ref(), sql), Cow::Borrowed(_)),
                "{dialect:?}: {sql}"
            );
        }
    }
    assert!(matches!(
        minus_all_with(&DatabricksDialect {}, "SELECT 1 AS `minus all`"),
        Cow::Borrowed(_)
    ));
    assert!(matches!(
        minus_all_with(&GenericDialect {}, "SELECT 1 AS \"MINUS\" FROM \"ALL\""),
        Cow::Borrowed(_)
    ));
    let spark = "SELECT a FROM l MINUS ALL SELECT a FROM r";
    assert!(matches!(
        minus_all_as_except_all(ParserDialect::Generic, spark),
        Cow::Borrowed(_)
    ));
    assert_eq!(
        minus_all_as_except_all(ParserDialect::Databricks, spark),
        "SELECT a FROM l EXCEPT ALL SELECT a FROM r"
    );
}

#[test]
fn the_rule_sits_right_after_the_struct_set_operation_rule() {
    let rules = Analyzer::new().rules;
    assert!(insert_multiset_set_operations(rules.clone()).is_err());
    let guarded = crate::session::insert_struct_setop_after_coercion(rules).unwrap();
    let names = insert_multiset_set_operations(guarded)
        .unwrap()
        .iter()
        .map(|rule| rule.name().to_string())
        .collect::<Vec<_>>();
    let struct_rule = names.iter().position(|name| name == "struct_setop_names");
    let multiset = names.iter().position(|name| name == "setop_all_multiset");
    assert_eq!(multiset, struct_rule.map(|position| position + 1));
}

fn joins_of(plan: &LogicalPlan) -> Vec<JoinType> {
    let mut found = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Join(join) = node {
            found.push(join.join_type);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

async fn planned(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    ctx.state().create_logical_plan(sql).await.unwrap()
}

#[tokio::test]
async fn the_rewrite_keeps_the_join_schema_and_a_second_pass_is_the_identity() {
    let config = ConfigOptions::new();
    for op in ["INTERSECT ALL", "EXCEPT ALL"] {
        for sql in [
            format!("{LEFT} {op} {RIGHT}"),
            format!("SELECT (SELECT count(*) FROM ({LEFT} {op} {RIGHT}) x) AS c"),
            format!("SELECT a FROM ({LEFT}) WHERE a IN ({LEFT} {op} {RIGHT})"),
        ] {
            let plan = planned(&sql).await;
            assert!(!joins_of(&plan).is_empty(), "{sql}");
            let once = MultisetSetOperations
                .analyze(plan.clone(), &config)
                .unwrap();
            assert_eq!(once.schema(), plan.schema(), "{sql}");
            assert!(joins_of(&once).is_empty(), "{sql}\n{once}");
            assert!(format!("{once}").contains("Unnest"), "{once}");
            let twice = MultisetSetOperations
                .analyze(once.clone(), &config)
                .unwrap();
            assert_eq!(format!("{twice}"), format!("{once}"));
        }
    }
}

#[tokio::test]
async fn the_distinct_forms_and_a_semi_join_written_as_a_join_are_untouched() {
    let config = ConfigOptions::new();
    for sql in [
        format!("{LEFT} INTERSECT {RIGHT}"),
        format!("{LEFT} EXCEPT {RIGHT}"),
        format!("{LEFT} UNION ALL {RIGHT}"),
        "SELECT x.a FROM (VALUES (1), (1)) AS x(a) LEFT SEMI JOIN (VALUES (1)) AS y(a) \
         ON x.a IS NOT DISTINCT FROM y.a"
            .to_string(),
        "SELECT x.a FROM (VALUES (1), (1)) AS x(a) LEFT ANTI JOIN (VALUES (2)) AS y(a) \
         USING (a)"
            .to_string(),
    ] {
        let plan = planned(&sql).await;
        let analyzed = MultisetSetOperations
            .analyze(plan.clone(), &config)
            .unwrap();
        assert_eq!(format!("{analyzed}"), format!("{plan}"), "{sql}");
    }
}

#[derive(Default)]
struct Recording {
    queries: Mutex<Vec<String>>,
}

#[async_trait(?Send)]
impl SqlDialect for Recording {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> DfResult<DataFrame> {
        self.queries.lock().unwrap().push(query.to_string());
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

fn session(recording: &Arc<Recording>, parser: &str, guarded: bool) -> ReparkSession {
    let builder = ReparkSession::builder()
        .config("datafusion.sql_parser.dialect", parser)
        .with_sql_dialect(Arc::clone(recording) as Arc<dyn SqlDialect>);
    let builder = if guarded {
        builder.with_local_fs_guard()
    } else {
        builder
    };
    builder.build().unwrap()
}

async fn rows_of(session: &ReparkSession, sql: &str) -> Vec<String> {
    let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row) || column.data_type() == &DataType::Null {
                        "NULL".to_string()
                    } else {
                        array_value_to_string(column, row).unwrap()
                    }
                })
                .collect::<Vec<_>>();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

async fn count_of(session: &ReparkSession, sql: &str) -> i64 {
    let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
    let column = batches[0].column(0);
    let counts = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(counts.len(), 1);
    counts.value(0)
}

fn strings(rows: &[&str]) -> Vec<String> {
    rows.iter().map(ToString::to_string).collect()
}

#[tokio::test]
async fn every_door_answers_the_spark_multiset_and_only_spark_respells_minus_all() {
    for (parser, guarded) in [("generic", false), ("generic", true), ("databricks", false)] {
        let recording = Arc::new(Recording::default());
        let session = session(&recording, parser, guarded);
        let spark_door = parser == "databricks";
        let cells = [
            ("INTERSECT ALL", strings(&["1", "2", "2"])),
            ("intersect all", strings(&["1", "2", "2"])),
            ("EXCEPT ALL", strings(&["1", "1", "4"])),
            ("INTERSECT", strings(&["1", "2"])),
            ("EXCEPT", strings(&["4"])),
            ("UNION", strings(&["1", "2", "3", "4"])),
        ];
        for (op, expected) in cells {
            let sql = format!("SELECT a FROM ({LEFT} {op} {RIGHT}) x");
            assert_eq!(rows_of(&session, &sql).await, expected, "{parser} {op}");
        }
        for op in ["MINUS ALL", "MiNuS /* gap */ aLl"] {
            let sql = format!("SELECT a FROM ({LEFT} {op} {RIGHT}) x");
            if spark_door {
                assert_eq!(
                    rows_of(&session, &sql).await,
                    strings(&["1", "1", "4"]),
                    "{op}"
                );
            } else {
                let error = session.sql(&sql).await.unwrap_err().to_string();
                assert!(
                    error.contains("MINUS ALL not implemented"),
                    "{parser} {op}: {error}"
                );
            }
        }
        let seen = recording.queries.lock().unwrap().clone();
        if spark_door {
            assert_eq!(seen.len(), 8, "{parser}");
            assert!(seen[6].contains(" EXCEPT ALL "), "{}", seen[6]);
            assert!(seen[7].contains(" EXCEPT /* gap */ aLl "), "{}", seen[7]);
        } else {
            assert!(seen.len() >= 6, "{parser}");
            assert!(
                seen.iter().all(|query| !query.contains("EXCEPT /* gap */")),
                "{parser}: {seen:?}"
            );
        }
        let reversed = format!("SELECT a FROM ({RIGHT} EXCEPT ALL {LEFT}) x");
        assert_eq!(rows_of(&session, &reversed).await, strings(&["3"]));
        let nulls = "SELECT a, b FROM (VALUES (1, NULL), (1, NULL), (NULL, 'x'), (NULL, 'x'), \
                     (NULL, NULL)) AS l(a, b) INTERSECT ALL SELECT a, b FROM (VALUES \
                     (1, NULL), (NULL, 'x'), (NULL, 'x'), (NULL, 'x'), (2, 'y')) AS r(a, b)";
        assert_eq!(
            rows_of(&session, nulls).await,
            strings(&["1|NULL", "NULL|x", "NULL|x"])
        );
        let empty =
            format!("SELECT a FROM ({LEFT} EXCEPT ALL SELECT a FROM ({RIGHT}) WHERE a > 9) x");
        assert_eq!(rows_of(&session, &empty).await.len(), 6);
        let counted =
            format!("SELECT (SELECT count(*) FROM ({LEFT} INTERSECT ALL {RIGHT}) x) AS c");
        assert_eq!(count_of(&session, &counted).await, 3, "{parser}");
    }
}

#[tokio::test]
async fn an_insert_or_ctas_of_a_multiset_stores_the_spark_rows() {
    let recording = Arc::new(Recording::default());
    let session = session(&recording, "generic", false);
    session
        .sql("CREATE TABLE t (a BIGINT)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let insert = format!("INSERT INTO t {LEFT} INTERSECT ALL {RIGHT}");
    session.sql(&insert).await.unwrap().collect().await.unwrap();
    assert_eq!(
        rows_of(&session, "SELECT a FROM t").await,
        strings(&["1", "2", "2"])
    );
    let ctas = format!("CREATE TABLE c AS {LEFT} EXCEPT ALL {RIGHT}");
    session.sql(&ctas).await.unwrap().collect().await.unwrap();
    assert_eq!(
        rows_of(&session, "SELECT a FROM c").await,
        strings(&["1", "1", "4"])
    );
}

#[test]
fn non_operator_minus_all_identifiers_and_unparsable_sql_stay_untouched() {
    let unchanged = [
        "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
        "SELECT t.minus all FROM (SELECT 1 AS minus, 2 AS except) t",
        "SELECT minus /* c */ all FROM (SELECT 1 AS minus, 2 AS except) t",
        "SELECT minus all FROM (SELECT 1 AS minus) t",
        "WITH minus AS (SELECT 1 AS a) SELECT a FROM minus all",
        "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all",
        "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT `all`.a FROM minus all",
        "SELECT `minus` all FROM (SELECT 1 AS minus, 2 AS except) t",
        "SELECT 'minus all' AS s",
        "CREATE VIEW ice.ns.v AS SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
        "CREATE TABLE ice.ns.c USING iceberg AS SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
        "a IN (SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t)",
        "a IN (WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all)",
        "SELECT 1 FROM WHERE MINUS ALL",
        "SELECT 1 MINUS ALL FROM",
        "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1 FROM WHERE MINUS ALL",
    ];
    let dialect = DatabricksDialect {};
    for sql in unchanged {
        assert!(
            matches!(minus_all_with(&dialect, sql), Cow::Borrowed(_)),
            "{sql}"
        );
    }
    let mixed = "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except UNION ALL SELECT 1, 2) t \
                 MINUS /* gap */ ALL SELECT 1 AS all";
    assert_eq!(
        minus_all_with(&dialect, mixed),
        "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except UNION ALL SELECT 1, 2) t \
         EXCEPT /* gap */ ALL SELECT 1 AS all"
    );
    let expr = "a IN (SELECT 1 AS a UNION ALL SELECT 1 MINUS /* gap */ ALL SELECT 1 AS a)";
    assert_eq!(
        minus_all_with(&dialect, expr),
        "a IN (SELECT 1 AS a UNION ALL SELECT 1 EXCEPT /* gap */ ALL SELECT 1 AS a)"
    );
    let replace =
        "REPLACE TABLE ice.ns.c USING iceberg AS SELECT 1 AS a MINUS\n\tALL SELECT 1 AS a";
    assert_eq!(
        minus_all_with(&dialect, replace),
        "REPLACE TABLE ice.ns.c USING iceberg AS SELECT 1 AS a EXCEPT\n\tALL SELECT 1 AS a"
    );
    let cr_only = "SELECT 1 AS a UNION ALL SELECT 1\rMINUS\rALL\rSELECT 1 AS a";
    assert_eq!(
        minus_all_with(&dialect, cr_only),
        "SELECT 1 AS a UNION ALL SELECT 1\rEXCEPT\rALL\rSELECT 1 AS a"
    );
}

#[tokio::test]
async fn databricks_session_answers_minus_all_identifiers_and_mixed_operator() {
    let recording = Arc::new(Recording::default());
    let session = session(&recording, "databricks", false);
    for (sql, expected) in [
        (
            "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
            strings(&["1"]),
        ),
        (
            "SELECT t.minus all FROM (SELECT 1 AS minus, 2 AS except) t",
            strings(&["1"]),
        ),
        (
            "SELECT minus /* c */ all FROM (SELECT 1 AS minus, 2 AS except) t",
            strings(&["1"]),
        ),
        (
            "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all",
            strings(&["1"]),
        ),
        (
            "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT `all`.a FROM minus all",
            strings(&["1"]),
        ),
        (
            "SELECT `minus` all FROM (SELECT 1 AS minus, 2 AS except) t",
            strings(&["1"]),
        ),
        (
            "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except UNION ALL SELECT 1, 2) t \
             MINUS /* gap */ ALL SELECT 1 AS all",
            strings(&["1"]),
        ),
    ] {
        assert_eq!(rows_of(&session, sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn untyped_null_left_operand_matches_distinct_twin_schema_and_multiset_rows() {
    let recording = Arc::new(Recording::default());
    let session = session(&recording, "databricks", false);
    for rhs in [
        "SELECT CAST(1 AS INT) AS a",
        "SELECT CAST('x' AS VARCHAR) AS a",
        "SELECT CAST('2024-01-01' AS DATE) AS a",
        "SELECT CAST(1.5 AS DOUBLE) AS a",
        "SELECT make_array(1, 2) AS a",
    ] {
        let distinct_i = session
            .sql(&format!("SELECT NULL AS a INTERSECT {rhs}"))
            .await
            .unwrap();
        let all_i = session
            .sql(&format!("SELECT NULL AS a INTERSECT ALL {rhs}"))
            .await
            .unwrap();
        assert_eq!(
            all_i.schema().field(0).data_type(),
            &DataType::Null,
            "{rhs}"
        );
        assert_eq!(
            all_i.schema().field(0).data_type(),
            distinct_i.schema().field(0).data_type(),
            "{rhs}"
        );
        assert_eq!(
            rows_of(&session, &format!("SELECT NULL AS a INTERSECT ALL {rhs}")).await,
            Vec::<String>::new(),
            "{rhs}"
        );
        let distinct_e = session
            .sql(&format!("SELECT NULL AS a EXCEPT {rhs}"))
            .await
            .unwrap();
        let all_e = session
            .sql(&format!("SELECT NULL AS a EXCEPT ALL {rhs}"))
            .await
            .unwrap();
        assert_eq!(
            all_e.schema().field(0).data_type(),
            distinct_e.schema().field(0).data_type(),
            "{rhs}"
        );
        assert_eq!(
            rows_of(&session, &format!("SELECT NULL AS a EXCEPT ALL {rhs}")).await,
            strings(&["NULL"]),
            "{rhs}"
        );
    }
    let multi = "(SELECT CAST(1 AS INT) AS a, NULL AS b UNION ALL SELECT 1, NULL) \
                 INTERSECT ALL (SELECT CAST(1 AS INT) AS a, CAST(NULL AS VARCHAR) AS b)";
    assert_eq!(rows_of(&session, multi).await, strings(&["1|NULL"]));
    let dup_null = "(SELECT NULL AS a UNION ALL SELECT NULL) EXCEPT ALL \
                    (SELECT CAST('x' AS VARCHAR) AS a UNION ALL SELECT CAST(NULL AS VARCHAR))";
    assert_eq!(rows_of(&session, dup_null).await, strings(&["NULL"]));
}

#[test]
fn lossless_round_trip_classifier_allows_exact_widenings_and_refuses_lossy_pairs() {
    let lossless = [
        (DataType::Int32, DataType::Int64),
        (DataType::Int8, DataType::Int32),
        (DataType::Int16, DataType::Int64),
        (DataType::Float32, DataType::Float64),
        (DataType::Int32, DataType::Float64),
        (DataType::Decimal128(5, 2), DataType::Decimal128(10, 4)),
        (DataType::Int32, DataType::Decimal128(12, 2)),
        (DataType::Int64, DataType::Decimal128(22, 2)),
        (DataType::Utf8View, DataType::Utf8),
    ];
    for (out_type, key_type) in lossless {
        assert!(
            is_lossless_round_trip(&out_type, &key_type),
            "{out_type} -> {key_type}"
        );
    }
    let struct_str = DataType::Struct(vec![Arc::new(Field::new("x", DataType::Utf8, true))].into());
    let struct_int =
        DataType::Struct(vec![Arc::new(Field::new("x", DataType::Int32, true))].into());
    let list_str = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let list_int = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let lossy = [
        (DataType::Utf8, DataType::Int32),
        (DataType::Utf8, DataType::Int64),
        (DataType::Utf8, DataType::Float32),
        (DataType::Utf8, DataType::Float64),
        (DataType::Utf8, DataType::Decimal128(10, 2)),
        (DataType::Utf8, DataType::Date32),
        (
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        (DataType::Int32, DataType::Float32),
        (DataType::Int64, DataType::Float64),
        (DataType::Int64, DataType::Float32),
        (DataType::Decimal128(38, 10), DataType::Decimal128(38, 2)),
        (DataType::Int64, DataType::Decimal128(18, 2)),
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ),
        (struct_str, struct_int),
        (list_str, list_int),
    ];
    for (out_type, key_type) in lossy {
        assert!(
            !is_lossless_round_trip(&out_type, &key_type),
            "{out_type} -> {key_type}"
        );
    }
}

#[tokio::test]
async fn lossy_mixed_type_multiset_refuses_before_write_while_exact_widenings_answer() {
    let recording = Arc::new(Recording::default());
    let session = session(&recording, "databricks", false);
    session
        .sql("CREATE TABLE out_str (a VARCHAR)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for sql in [
        "(SELECT '01' AS a UNION ALL SELECT '01') EXCEPT ALL SELECT CAST(1 AS INT) AS a",
        "(SELECT '01' AS a UNION ALL SELECT '01') INTERSECT ALL SELECT CAST(1 AS INT) AS a",
        "(SELECT '1.00' AS a UNION ALL SELECT '1.00') EXCEPT ALL SELECT CAST(1.0 AS DOUBLE) AS a",
        "(SELECT '2024-01-01' AS a UNION ALL SELECT '2024-01-01') EXCEPT ALL \
         SELECT CAST('2024-01-01' AS DATE) AS a",
        "(SELECT CAST(9007199254740993 AS BIGINT) AS a UNION ALL SELECT 9007199254740993) \
         EXCEPT ALL SELECT CAST(9007199254740992.0 AS DOUBLE) AS a",
        "(SELECT CAST(16777217 AS INT) AS a UNION ALL SELECT 16777217) \
         EXCEPT ALL SELECT CAST(16777216.0 AS FLOAT) AS a",
        "INSERT INTO out_str (SELECT '01' AS a UNION ALL SELECT '01') \
         EXCEPT ALL SELECT CAST(1 AS INT) AS a",
    ] {
        let error = match session.sql(sql).await {
            Ok(frame) => frame
                .collect()
                .await
                .map_err(crate::engine_err)
                .unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(
            error.exception_class(),
            repark_common::ErrorClass::Analysis,
            "{sql}: {error}"
        );
        assert!(
            error.to_string().contains("SETOP-MIXED-TYPE-COERCION-1"),
            "{sql}: {error}"
        );
    }
    assert!(rows_of(&session, "SELECT a FROM out_str").await.is_empty());
    for (sql, expected) in [
        (
            "(SELECT CAST(1 AS INT) AS a UNION ALL SELECT 1) EXCEPT ALL \
             (SELECT CAST(1 AS BIGINT) AS a UNION ALL SELECT CAST(5000000000 AS BIGINT))",
            strings(&["1"]),
        ),
        (
            "(SELECT CAST(1.5 AS FLOAT) AS a UNION ALL SELECT CAST(1.5 AS FLOAT)) EXCEPT ALL \
             SELECT CAST(1.5 AS DOUBLE) AS a",
            strings(&["1.5"]),
        ),
    ] {
        assert_eq!(rows_of(&session, sql).await, expected, "{sql}");
    }
}

#[derive(Default)]
struct OwningRecording {
    queries: Mutex<Vec<String>>,
}

#[async_trait(?Send)]
impl SqlDialect for OwningRecording {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> DfResult<DataFrame> {
        self.queries.lock().unwrap().push(query.to_string());
        DataFusionDialect.execute(cx, "SELECT 1 AS a").await
    }

    fn owns_minus_all_rewrite(&self) -> bool {
        true
    }
}

#[test]
fn ddl_dml_fallback_respells_values_and_parenthesized_insert_by_name() {
    let dialect = DatabricksDialect {};
    let cases = [
        (
            "CREATE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "CREATE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) EXCEPT ALL VALUES (1)",
        ),
        (
            "CREATE OR REPLACE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) \
             MINUS ALL VALUES (1)",
            "CREATE OR REPLACE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) \
             EXCEPT ALL VALUES (1)",
        ),
        (
            "REPLACE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "REPLACE TABLE ice.ns.t USING iceberg AS VALUES (1), (1), (4) EXCEPT ALL VALUES (1)",
        ),
        (
            "CREATE TABLE ice.ns.t USING iceberg TBLPROPERTIES ('comment' = 'SELECT WITH') \
             AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "CREATE TABLE ice.ns.t USING iceberg TBLPROPERTIES ('comment' = 'SELECT WITH') \
             AS VALUES (1), (1), (4) EXCEPT ALL VALUES (1)",
        ),
        (
            "INSERT INTO ice.ns.t BY NAME (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) \
             MINUS ALL (SELECT 1 AS a)",
            "INSERT INTO ice.ns.t BY NAME (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) \
             EXCEPT ALL (SELECT 1 AS a)",
        ),
        (
            "INSERT OVERWRITE ice.ns.t BY NAME \
             (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) MINUS ALL (SELECT 1 AS a)",
            "INSERT OVERWRITE ice.ns.t BY NAME \
             (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) EXCEPT ALL (SELECT 1 AS a)",
        ),
        (
            "INSERT INTO ice.ns.t REPLACE WHERE a > 0 \
             (SELECT 1 AS a UNION ALL SELECT 1) MINUS ALL (SELECT 1 AS a)",
            "INSERT INTO ice.ns.t REPLACE WHERE a > 0 \
             (SELECT 1 AS a UNION ALL SELECT 1) EXCEPT ALL (SELECT 1 AS a)",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(minus_all_with(&dialect, input), expected, "{input}");
    }
    for untouched in [
        "CREATE TABLE ice.ns.t USING iceberg AS \
         (SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) s)",
        "INSERT INTO ice.ns.t BY NAME \
         (SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) s)",
    ] {
        assert!(
            matches!(minus_all_with(&dialect, untouched), Cow::Borrowed(_)),
            "{untouched}"
        );
    }
}

#[tokio::test]
async fn dialect_owning_minus_all_receives_raw_sql_even_under_local_fs_guard() {
    for guarded in [false, true] {
        let recording = Arc::new(OwningRecording::default());
        let builder = ReparkSession::builder()
            .config("datafusion.sql_parser.dialect", "databricks")
            .with_sql_dialect(Arc::clone(&recording) as Arc<dyn SqlDialect>);
        let builder = if guarded {
            builder.with_local_fs_guard()
        } else {
            builder
        };
        let session = builder.build().unwrap();
        let sql = "SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL SELECT 1 AS a";
        let _ = session.sql(sql).await.unwrap();
        let seen = recording.queries.lock().unwrap().clone();
        assert_eq!(seen, vec![sql.to_string()], "guarded={guarded}");
    }
}
