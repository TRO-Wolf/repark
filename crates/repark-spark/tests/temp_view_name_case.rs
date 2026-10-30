use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const COPY_ON_WRITE: &str = "TBLPROPERTIES ('format-version'='2')";
const MERGE_ON_READ: &str = "TBLPROPERTIES ('format-version'='2', \
     'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
     'write.merge.mode'='merge-on-read')";
const TABLE_READ: &str = "SELECT id FROM ice.ns.t ORDER BY id";
const SEEDED: &[i64] = &[1, 2, 3];
const VIEW_ROW: &[i64] = &[99];
const EXPECT_TABLE_NOT_VIEW: &str = "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]";
const TABLE_OPERATION: &str = "[UNSUPPORTED_FEATURE.TABLE_OPERATION]";
const VIEW_SPELLINGS: [&str; 4] = ["`T`", "T", "t", "`t`"];
const NAME_SPELLINGS: [&str; 4] = ["t", "T", "`T`", "`t`"];

#[derive(Clone, Copy)]
enum Refusal {
    ExpectsTable(&'static str),
    WrittenName(&'static str),
    TableOperation(&'static str),
}

#[derive(Clone, Copy)]
struct Route {
    template: &'static str,
    refusal: Refusal,
    written: &'static [i64],
}

const ROUTES: [Route; 7] = [
    Route {
        template: "DELETE FROM {} WHERE id = 2",
        refusal: Refusal::ExpectsTable("DELETE"),
        written: &[1, 3],
    },
    Route {
        template: "UPDATE {} SET id = 10 WHERE id = 1",
        refusal: Refusal::TableOperation("UPDATE TABLE"),
        written: &[2, 3, 10],
    },
    Route {
        template: "MERGE INTO {} AS x USING (SELECT 2 AS id) AS s ON x.id = s.id \
                   WHEN MATCHED THEN DELETE",
        refusal: Refusal::TableOperation("MERGE INTO TABLE"),
        written: &[1, 3],
    },
    Route {
        template: "INSERT INTO {} VALUES (40)",
        refusal: Refusal::ExpectsTable("INSERT"),
        written: &[1, 2, 3, 40],
    },
    Route {
        template: "INSERT OVERWRITE {} VALUES (50)",
        refusal: Refusal::ExpectsTable("INSERT"),
        written: &[50],
    },
    Route {
        template: "TRUNCATE TABLE {}",
        refusal: Refusal::WrittenName("TRUNCATE TABLE"),
        written: &[],
    },
    Route {
        template: "MERGE WITH SCHEMA EVOLUTION INTO {} AS x USING (SELECT 2 AS id) AS s \
                   ON x.id = s.id WHEN MATCHED THEN DELETE",
        refusal: Refusal::TableOperation("MERGE INTO TABLE"),
        written: &[1, 3],
    },
];
const SIX_ROUTES: usize = 6;
const MERGE_ROUTE: usize = 2;

fn routes_without_a_three_part_merge() -> impl Iterator<Item = &'static Route> {
    ROUTES[..SIX_ROUTES]
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != MERGE_ROUTE)
        .map(|(_, route)| route)
}

struct Seeded {
    session: ReparkSession,
    _warehouse: TempDir,
}

fn session(case_sensitive: bool) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("spark.sql.caseSensitive", case_sensitive.to_string())
        .build()
        .expect("session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, sql: &str) {
    attempt(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

async fn seeded(properties: &str, view: &str, case_sensitive: bool) -> Seeded {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session(case_sensitive);
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        &format!("CREATE TABLE ice.ns.t (id INT) USING iceberg {properties}"),
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t VALUES (1), (2), (3)").await;
    run(&session, "USE ice.ns").await;
    run(
        &session,
        &format!("CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id"),
    )
    .await;
    Seeded {
        session,
        _warehouse: warehouse,
    }
}

async fn ids(session: &ReparkSession, read: &str) -> Vec<i64> {
    let batches = attempt(session, read)
        .await
        .unwrap_or_else(|error| panic!("`{read}` must read: {error}"));
    let mut values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let column = cast(batch.column(0), &DataType::Int64).expect("int64 column");
            column.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    values.sort_unstable();
    values
}

fn backticked(segments: &[String]) -> String {
    segments
        .iter()
        .map(|segment| format!("`{}`", segment.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

async fn view_ids(session: &ReparkSession, name: &str) -> Vec<i64> {
    let home = session
        .resolve_temp_view_home_ref(name)
        .expect("temp view lookup")
        .unwrap_or_else(|| panic!("`{name}` must name the temporary view"));
    ids(session, &format!("SELECT id FROM {}", backticked(&home))).await
}

fn leaf(name: &str) -> String {
    name.trim_matches('`').to_string()
}

fn assert_refusal(sql: &str, error: &str, route: Route, name: &str) {
    let (token, shape) = match route.refusal {
        Refusal::ExpectsTable(operation) => (
            EXPECT_TABLE_NOT_VIEW,
            format!("'{operation}' expects a table but `"),
        ),
        Refusal::WrittenName(operation) => (
            EXPECT_TABLE_NOT_VIEW,
            format!(
                "'{operation}' expects a table but `{}` is a view",
                leaf(name)
            ),
        ),
        Refusal::TableOperation(operation) => {
            (TABLE_OPERATION, format!("does not support {operation}"))
        }
    };
    assert!(
        error.contains(token),
        "`{sql}` must refuse with {token}, got: {error}"
    );
    assert!(
        error.contains(&shape),
        "`{sql}` must carry `{shape}`, got: {error}"
    );
    if let Refusal::ExpectsTable(_) = route.refusal {
        let shown = error
            .split("expects a table but `")
            .nth(1)
            .and_then(|rest| rest.split('`').next())
            .unwrap_or_default();
        assert!(
            shown.eq_ignore_ascii_case("t"),
            "`{sql}` must name the view, got: {error}"
        );
    }
}

async fn assert_every_spelling_refuses(properties: &str, view: &str, routes: &[Route]) {
    for name in NAME_SPELLINGS {
        for route in routes {
            let seeded = seeded(properties, view, false).await;
            let sql = route.template.replace("{}", name);
            let outcome = attempt(&seeded.session, &sql).await;
            let Err(error) = outcome else {
                panic!(
                    "`{sql}` must refuse while the temporary view {view} holds the name; the \
                     table now reads {:?}",
                    ids(&seeded.session, TABLE_READ).await
                );
            };
            assert_refusal(&sql, &error, *route, name);
            assert_eq!(
                ids(&seeded.session, TABLE_READ).await,
                SEEDED,
                "`{sql}` must leave the table unchanged under view {view}"
            );
            assert_eq!(
                view_ids(&seeded.session, "t").await,
                VIEW_ROW,
                "the view {view} must still read its own row after `{sql}`"
            );
        }
    }
}

#[tokio::test]
async fn a_quoted_upper_case_view_holds_every_spelling_of_its_name() {
    assert_every_spelling_refuses(COPY_ON_WRITE, "`T`", &ROUTES).await;
}

#[tokio::test]
async fn an_unquoted_upper_case_view_holds_every_spelling_of_its_name() {
    assert_every_spelling_refuses(COPY_ON_WRITE, "T", &ROUTES[..SIX_ROUTES]).await;
}

#[tokio::test]
async fn a_lower_case_view_holds_every_spelling_of_its_name() {
    assert_every_spelling_refuses(COPY_ON_WRITE, "t", &ROUTES[..SIX_ROUTES]).await;
    assert_every_spelling_refuses(COPY_ON_WRITE, "`t`", &ROUTES[..SIX_ROUTES]).await;
}

#[tokio::test]
async fn a_quoted_upper_case_view_holds_its_name_on_merge_on_read() {
    assert_every_spelling_refuses(MERGE_ON_READ, "`T`", &ROUTES[..3]).await;
}

#[tokio::test]
async fn merge_with_schema_evolution_through_a_view_name_refuses_as_a_merge() {
    for view in VIEW_SPELLINGS {
        assert_every_spelling_refuses(COPY_ON_WRITE, view, &ROUTES[SIX_ROUTES..]).await;
    }
}

#[tokio::test]
async fn every_spelling_resolves_one_view_under_the_default_rule() {
    for view in VIEW_SPELLINGS {
        let seeded = seeded(COPY_ON_WRITE, view, false).await;
        for name in ["t", "T", "`T`", "`t`", "\"T\"", "\"t\""] {
            assert_eq!(
                view_ids(&seeded.session, name).await,
                VIEW_ROW,
                "`{name}` must read the view created as {view}"
            );
        }
        assert_eq!(
            seeded.session.list_temp_view_names().expect("names").len(),
            1,
            "the view {view} must be registered once"
        );
    }
}

#[tokio::test]
async fn a_view_replaced_or_dropped_under_another_spelling_is_the_same_view() {
    for view in VIEW_SPELLINGS {
        for other in VIEW_SPELLINGS {
            let seeded = seeded(COPY_ON_WRITE, view, false).await;
            run(
                &seeded.session,
                &format!("CREATE OR REPLACE TEMPORARY VIEW {other} AS SELECT 7 AS id"),
            )
            .await;
            assert_eq!(
                seeded.session.list_temp_view_names().expect("names").len(),
                1,
                "replacing {view} as {other} must keep one view"
            );
            assert_eq!(view_ids(&seeded.session, "T").await, vec![7]);
            run(&seeded.session, &format!("DROP VIEW {other}")).await;
            assert!(
                seeded
                    .session
                    .list_temp_view_names()
                    .expect("names")
                    .is_empty(),
                "dropping {other} must drop the view created as {view}"
            );
            assert_eq!(ids(&seeded.session, TABLE_READ).await, SEEDED);
        }
    }
}

#[tokio::test]
async fn a_data_frame_view_registered_under_a_quoted_name_holds_every_spelling() {
    let seeded = seeded(COPY_ON_WRITE, "w", false).await;
    let frame = seeded.session.sql("SELECT 99 AS id").await.expect("frame");
    seeded
        .session
        .create_or_replace_temp_view_from("\"T\"", &frame)
        .expect("register");
    for route in &ROUTES[..SIX_ROUTES] {
        let sql = route.template.replace("{}", "t");
        let error = attempt(&seeded.session, &sql)
            .await
            .expect_err("a write through the folded name must refuse");
        assert_refusal(&sql, &error, *route, "t");
        assert_eq!(ids(&seeded.session, TABLE_READ).await, SEEDED);
    }
    assert!(
        seeded
            .session
            .drop_temp_view("t")
            .expect("drop through the folded name")
    );
    assert_eq!(view_ids(&seeded.session, "w").await, VIEW_ROW);
}

async fn at_table_home(seeded: &Seeded) {
    run(
        &seeded.session,
        "SET datafusion.catalog.default_catalog = 'ice'",
    )
    .await;
    run(
        &seeded.session,
        "SET datafusion.catalog.default_schema = 'ns'",
    )
    .await;
}

#[tokio::test]
async fn a_two_part_table_name_writes_the_table_with_the_view_in_scope() {
    for route in routes_without_a_three_part_merge() {
        let seeded = seeded(COPY_ON_WRITE, "t", false).await;
        at_table_home(&seeded).await;
        let sql = route.template.replace("{}", "ns.t");
        run(&seeded.session, &sql).await;
        assert_eq!(
            ids(&seeded.session, TABLE_READ).await,
            route.written,
            "`{sql}` must write the table"
        );
        assert_eq!(
            view_ids(&seeded.session, "t").await,
            VIEW_ROW,
            "the view must still read its own row after `{sql}`"
        );
    }
}

#[tokio::test]
async fn under_case_sensitive_names_an_upper_case_view_does_not_hold_the_lower_case_table() {
    for view in ["T", "`T`"] {
        for name in ["t", "`t`"] {
            for route in routes_without_a_three_part_merge() {
                let seeded = seeded(COPY_ON_WRITE, view, true).await;
                at_table_home(&seeded).await;
                let sql = route.template.replace("{}", name);
                run(&seeded.session, &sql).await;
                assert_eq!(
                    ids(&seeded.session, TABLE_READ).await,
                    route.written,
                    "`{sql}` must write the table while only the view {view} exists"
                );
                assert_eq!(view_ids(&seeded.session, "T").await, VIEW_ROW);
            }
        }
        for name in ["T", "`T`"] {
            for route in &ROUTES[..SIX_ROUTES] {
                let seeded = seeded(COPY_ON_WRITE, view, true).await;
                let sql = route.template.replace("{}", name);
                let error = attempt(&seeded.session, &sql)
                    .await
                    .expect_err("a write through the exact view name must refuse");
                let token = match route.refusal {
                    Refusal::TableOperation(_) => TABLE_OPERATION,
                    _ => EXPECT_TABLE_NOT_VIEW,
                };
                assert!(
                    error.contains(token),
                    "`{sql}` must refuse with {token}, got: {error}"
                );
                assert_eq!(ids(&seeded.session, TABLE_READ).await, SEEDED);
                assert_eq!(view_ids(&seeded.session, "T").await, VIEW_ROW);
            }
        }
    }
}
