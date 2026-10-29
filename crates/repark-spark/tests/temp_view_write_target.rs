use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{OverwriteIntent, ReparkSession};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const COPY_ON_WRITE: &str = "TBLPROPERTIES ('format-version'='2')";
const MERGE_ON_READ: &str = "TBLPROPERTIES ('format-version'='2', \
     'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
     'write.merge.mode'='merge-on-read')";
const TABLE_READ: &str = "SELECT id FROM ice.ns.t ORDER BY id";
const QUALIFIED: &str = "ice.ns.t";
const SEEDED: &[i64] = &[1, 2, 3];
const VIEW_ROW: &[i64] = &[99];
const EXPECT_TABLE_NOT_VIEW: &str = "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]";
const TABLE_OPERATION: &str = "[UNSUPPORTED_FEATURE.TABLE_OPERATION]";

#[derive(Clone, Copy)]
enum Refusal {
    ExpectsTable(&'static str),
    TableOperation(&'static str),
}

#[derive(Clone, Copy)]
struct Route {
    template: &'static str,
    refusal: Refusal,
    written: &'static [i64],
}

const fn route(template: &'static str, refusal: Refusal, written: &'static [i64]) -> Route {
    Route {
        template,
        refusal,
        written,
    }
}

const DELETE: Route = route(
    "DELETE FROM {} WHERE id = 2",
    Refusal::ExpectsTable("DELETE"),
    &[1, 3],
);
const UPDATE: Route = route(
    "UPDATE {} SET id = 10 WHERE id = 1",
    Refusal::TableOperation("UPDATE TABLE"),
    &[2, 3, 10],
);
const MERGE: Route = route(
    "MERGE INTO {} AS x USING (SELECT 2 AS id) AS s ON x.id = s.id WHEN MATCHED THEN DELETE",
    Refusal::TableOperation("MERGE INTO TABLE"),
    &[1, 3],
);
const INSERT_VALUES: Route = route(
    "INSERT INTO {} VALUES (40)",
    Refusal::ExpectsTable("INSERT"),
    &[1, 2, 3, 40],
);
const INSERT_COLUMNS: Route = route(
    "INSERT INTO {} (id) VALUES (41)",
    Refusal::ExpectsTable("INSERT"),
    &[1, 2, 3, 41],
);
const INSERT_BY_NAME: Route = route(
    "INSERT INTO {} BY NAME SELECT 42 AS id",
    Refusal::ExpectsTable("INSERT"),
    &[1, 2, 3, 42],
);
const INSERT_TABLE_SELECT: Route = route(
    "INSERT INTO TABLE {} SELECT 43 AS id",
    Refusal::ExpectsTable("INSERT"),
    &[1, 2, 3, 43],
);
const INSERT_OVERWRITE_VALUES: Route = route(
    "INSERT OVERWRITE {} VALUES (50)",
    Refusal::ExpectsTable("INSERT"),
    &[50],
);
const INSERT_OVERWRITE_TABLE_SELECT: Route = route(
    "INSERT OVERWRITE TABLE {} SELECT 51 AS id",
    Refusal::ExpectsTable("INSERT"),
    &[51],
);
const TRUNCATE: Route = route(
    "TRUNCATE TABLE {}",
    Refusal::ExpectsTable("TRUNCATE TABLE"),
    &[],
);

const ROW_LEVEL: [Route; 3] = [DELETE, UPDATE, MERGE];
const INSERTS: [Route; 4] = [
    INSERT_VALUES,
    INSERT_COLUMNS,
    INSERT_BY_NAME,
    INSERT_TABLE_SELECT,
];
const OVERWRITES: [Route; 2] = [INSERT_OVERWRITE_VALUES, INSERT_OVERWRITE_TABLE_SELECT];
const EVERY_ROUTE: [Route; 10] = [
    DELETE,
    UPDATE,
    MERGE,
    INSERT_VALUES,
    INSERT_COLUMNS,
    INSERT_BY_NAME,
    INSERT_TABLE_SELECT,
    INSERT_OVERWRITE_VALUES,
    INSERT_OVERWRITE_TABLE_SELECT,
    TRUNCATE,
];

struct Seeded {
    session: ReparkSession,
    _warehouse: TempDir,
}

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
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

async fn seeded(properties: &str, view: Option<&str>) -> Seeded {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
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
    if let Some(view) = view {
        run(
            &session,
            &format!("CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id"),
        )
        .await;
    }
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

fn statement(route: Route, name: &str) -> String {
    route.template.replace("{}", name)
}

fn assert_refusal(sql: &str, error: &str, refusal: Refusal, shown: &str) {
    match refusal {
        Refusal::ExpectsTable(operation) => {
            assert!(
                error.contains(EXPECT_TABLE_NOT_VIEW),
                "`{sql}` must refuse with {EXPECT_TABLE_NOT_VIEW}, got: {error}"
            );
            let shape = format!("'{operation}' expects a table but `{shown}` is a view");
            assert!(
                error.contains(&shape),
                "`{sql}` must carry `{shape}`, got: {error}"
            );
        }
        Refusal::TableOperation(operation) => {
            assert!(
                error.contains(TABLE_OPERATION),
                "`{sql}` must refuse with {TABLE_OPERATION}, got: {error}"
            );
            let shape = format!("does not support {operation}");
            assert!(
                error.contains(&shape),
                "`{sql}` must carry `{shape}`, got: {error}"
            );
        }
    }
}

async fn assert_view_name_refuses(properties: &str, route: Route, name: &str, shown: &str) {
    let seeded = seeded(properties, Some("t")).await;
    let sql = statement(route, name);
    let outcome = attempt(&seeded.session, &sql).await;
    let Err(error) = outcome else {
        panic!(
            "`{sql}` must refuse while a temporary view holds the name; the table now reads {:?}",
            ids(&seeded.session, TABLE_READ).await
        );
    };
    assert_refusal(&sql, &error, route.refusal, shown);
    assert_eq!(
        ids(&seeded.session, TABLE_READ).await,
        SEEDED,
        "`{sql}` must leave the table unchanged"
    );
    assert_eq!(
        ids(&seeded.session, "SELECT id FROM t").await,
        VIEW_ROW,
        "the view must still read its own row after `{sql}`"
    );
}

async fn assert_writes_table(properties: &str, view: Option<&str>, route: Route, name: &str) {
    let seeded = seeded(properties, view).await;
    let sql = statement(route, name);
    run(&seeded.session, &sql).await;
    assert_eq!(
        ids(&seeded.session, TABLE_READ).await,
        route.written,
        "`{sql}` must write the table"
    );
    if let Some(view) = view {
        assert_eq!(
            ids(&seeded.session, &format!("SELECT id FROM {view}")).await,
            VIEW_ROW,
            "the view `{view}` must still read its own row after `{sql}`"
        );
    }
}

#[tokio::test]
async fn delete_update_and_merge_through_a_view_name_refuse_on_copy_on_write() {
    for route in ROW_LEVEL {
        assert_view_name_refuses(COPY_ON_WRITE, route, "t", "t").await;
    }
}

#[tokio::test]
async fn delete_update_and_merge_through_a_view_name_refuse_on_merge_on_read() {
    for route in ROW_LEVEL {
        assert_view_name_refuses(MERGE_ON_READ, route, "t", "t").await;
    }
}

#[tokio::test]
async fn insert_through_a_view_name_refuses() {
    for route in INSERTS {
        assert_view_name_refuses(COPY_ON_WRITE, route, "t", "t").await;
    }
}

#[tokio::test]
async fn insert_overwrite_through_a_view_name_refuses() {
    for route in OVERWRITES {
        assert_view_name_refuses(COPY_ON_WRITE, route, "t", "t").await;
    }
}

#[tokio::test]
async fn truncate_through_a_view_name_refuses() {
    assert_view_name_refuses(COPY_ON_WRITE, TRUNCATE, "t", "t").await;
}

#[tokio::test]
async fn a_quoted_view_name_refuses_every_route() {
    for route in EVERY_ROUTE {
        assert_view_name_refuses(COPY_ON_WRITE, route, "`t`", "t").await;
    }
}

#[tokio::test]
async fn an_upper_case_view_name_refuses_every_route() {
    for route in [
        DELETE,
        UPDATE,
        MERGE,
        INSERT_VALUES,
        INSERT_OVERWRITE_VALUES,
    ] {
        assert_view_name_refuses(COPY_ON_WRITE, route, "T", "t").await;
    }
    assert_view_name_refuses(COPY_ON_WRITE, TRUNCATE, "T", "T").await;
}

#[tokio::test]
async fn the_home_qualified_view_name_refuses_every_route() {
    let home = session()
        .temp_view_home()
        .expect("temp view home")
        .join(".");
    for route in EVERY_ROUTE {
        assert_view_name_refuses(COPY_ON_WRITE, route, &format!("{home}.t"), "t").await;
    }
    for route in ROW_LEVEL {
        assert_view_name_refuses(MERGE_ON_READ, route, &format!("{home}.t"), "t").await;
    }
}

#[tokio::test]
async fn every_route_writes_the_qualified_table_with_no_view_in_scope() {
    for route in EVERY_ROUTE {
        assert_writes_table(COPY_ON_WRITE, None, route, QUALIFIED).await;
    }
    for route in ROW_LEVEL {
        assert_writes_table(MERGE_ON_READ, None, route, QUALIFIED).await;
    }
}

#[tokio::test]
async fn a_view_with_another_name_does_not_block_the_write() {
    for route in EVERY_ROUTE {
        assert_writes_table(COPY_ON_WRITE, Some("w"), route, QUALIFIED).await;
    }
    for route in ROW_LEVEL {
        assert_writes_table(MERGE_ON_READ, Some("w"), route, QUALIFIED).await;
    }
}

#[tokio::test]
async fn a_qualified_table_name_writes_the_table_with_the_view_in_scope() {
    for route in EVERY_ROUTE {
        assert_writes_table(COPY_ON_WRITE, Some("t"), route, QUALIFIED).await;
    }
    for route in ROW_LEVEL {
        assert_writes_table(MERGE_ON_READ, Some("t"), route, QUALIFIED).await;
    }
}

#[tokio::test]
async fn a_view_name_with_write_options_still_holds_the_table() {
    let options = HashMap::from([(
        "snapshot-property.run_id".to_string(),
        "view-target".to_string(),
    )]);
    for route in EVERY_ROUTE {
        let seeded = seeded(COPY_ON_WRITE, Some("t")).await;
        let sql = statement(route, "t");
        let outcome = match seeded
            .session
            .sql_with_write_options(&sql, &options, OverwriteIntent::Session, false)
            .await
        {
            Ok(frame) => frame
                .collect()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        assert!(
            outcome.is_err(),
            "`{sql}` with write options must refuse while a temporary view holds the name"
        );
        assert_eq!(
            ids(&seeded.session, TABLE_READ).await,
            SEEDED,
            "`{sql}` with write options must leave the table unchanged"
        );
        assert_eq!(
            ids(&seeded.session, "SELECT id FROM t").await,
            VIEW_ROW,
            "the view must still read its own row after `{sql}` with write options"
        );
    }
}
