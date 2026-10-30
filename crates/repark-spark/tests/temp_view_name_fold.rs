use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::ReparkSession;
use repark_functions::case_sensitive::SparkCaseSensitiveConfig;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const SEEDED: &[i64] = &[1, 2, 3];
const VIEW_ROW: &[i64] = &[99];
const EXPECT_TABLE_NOT_VIEW: &str = "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]";
const TABLE_OPERATION: &str = "[UNSUPPORTED_FEATURE.TABLE_OPERATION]";

struct Route {
    template: &'static str,
    token: &'static str,
    written: &'static [i64],
    bare_target: bool,
}

const ROUTES: [Route; 6] = [
    Route {
        template: "DELETE FROM {} WHERE id = 2",
        token: EXPECT_TABLE_NOT_VIEW,
        written: &[1, 3],
        bare_target: true,
    },
    Route {
        template: "UPDATE {} SET id = 10 WHERE id = 1",
        token: TABLE_OPERATION,
        written: &[2, 3, 10],
        bare_target: true,
    },
    Route {
        template: "MERGE INTO {} AS x USING (SELECT 2 AS id) AS s ON x.id = s.id \
                   WHEN MATCHED THEN DELETE",
        token: TABLE_OPERATION,
        written: &[1, 3],
        bare_target: false,
    },
    Route {
        template: "INSERT INTO {} VALUES (40)",
        token: EXPECT_TABLE_NOT_VIEW,
        written: &[1, 2, 3, 40],
        bare_target: true,
    },
    Route {
        template: "INSERT OVERWRITE {} VALUES (50)",
        token: EXPECT_TABLE_NOT_VIEW,
        written: &[50],
        bare_target: true,
    },
    Route {
        template: "TRUNCATE TABLE {}",
        token: EXPECT_TABLE_NOT_VIEW,
        written: &[],
        bare_target: true,
    },
];

struct Seeded {
    session: ReparkSession,
    table: &'static str,
    _warehouse: TempDir,
}

fn set_case_sensitive(session: &ReparkSession, enabled: bool) {
    session
        .context()
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .extensions
        .insert(SparkCaseSensitiveConfig { enabled });
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

async fn seed(table: &'static str, view: &str, created: bool, looked_up: bool) -> Seeded {
    let warehouse = TempDir::new().expect("warehouse");
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.`{table}` (id INT) USING iceberg \
             TBLPROPERTIES ('format-version'='2')"
        ),
    )
    .await;
    run(
        &session,
        &format!("INSERT INTO ice.ns.`{table}` VALUES (1), (2), (3)"),
    )
    .await;
    run(&session, "USE ice.ns").await;
    set_case_sensitive(&session, created);
    run(
        &session,
        &format!("CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT 99 AS id"),
    )
    .await;
    set_case_sensitive(&session, looked_up);
    Seeded {
        session,
        table,
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

async fn table_ids(seeded: &Seeded) -> Vec<i64> {
    ids(
        &seeded.session,
        &format!("SELECT id FROM ice.ns.`{}`", seeded.table),
    )
    .await
}

fn held(seeded: &Seeded, name: &str) -> Option<Vec<String>> {
    seeded
        .session
        .resolve_temp_view_home_ref(name)
        .expect("temp view lookup")
}

async fn view_ids(seeded: &Seeded, name: &str) -> Vec<i64> {
    let home = held(seeded, name).unwrap_or_else(|| panic!("`{name}` must name the view"));
    let reference = home
        .iter()
        .map(|segment| format!("`{segment}`"))
        .collect::<Vec<_>>()
        .join(".");
    ids(&seeded.session, &format!("SELECT id FROM {reference}")).await
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

async fn assert_refuses(seeded: &Seeded, route: &Route, name: &str, view_name: &str) {
    let sql = route.template.replace("{}", name);
    let error = attempt(&seeded.session, &sql)
        .await
        .expect_err("a write through the held name must refuse");
    assert!(
        error.contains(route.token),
        "`{sql}` must refuse with {}, got: {error}",
        route.token
    );
    assert_eq!(
        table_ids(seeded).await,
        SEEDED,
        "`{sql}` must hold the table"
    );
    assert_eq!(view_ids(seeded, view_name).await, VIEW_ROW);
}

async fn assert_writes(seeded: &Seeded, route: &Route, name: &str) {
    at_table_home(seeded).await;
    let sql = route.template.replace("{}", name);
    run(&seeded.session, &sql).await;
    assert_eq!(
        table_ids(seeded).await,
        route.written,
        "`{sql}` must write the table"
    );
}

#[tokio::test]
async fn a_view_created_under_the_default_rule_keeps_its_folded_name_when_the_rule_turns_exact() {
    for view in ["`T`", "T", "t"] {
        let seeded = seed("t", view, false, true).await;
        assert_eq!(
            seeded.session.list_temp_view_names().expect("names"),
            vec!["t".to_string()],
            "{view} must be stored folded"
        );
        assert!(held(&seeded, "t").is_some());
        assert!(held(&seeded, "`t`").is_some());
        assert!(held(&seeded, "T").is_none());
        assert!(held(&seeded, "`T`").is_none());
        for route in &ROUTES {
            let seeded = seed("t", view, false, true).await;
            assert_refuses(&seeded, route, "t", "t").await;
            let sql = route.template.replace("{}", "`T`");
            assert!(attempt(&seeded.session, &sql).await.is_err());
            assert_eq!(table_ids(&seeded).await, SEEDED);
        }
    }
}

#[tokio::test]
async fn a_view_created_under_the_exact_rule_keeps_its_written_name_when_the_rule_turns_default() {
    for view in ["`T`", "T"] {
        let seeded = seed("t", view, true, false).await;
        assert_eq!(
            seeded.session.list_temp_view_names().expect("names"),
            vec!["T".to_string()],
            "{view} must be stored verbatim"
        );
        for name in ["t", "T", "`T`", "`t`"] {
            assert!(held(&seeded, name).is_none(), "{name} must not hold {view}");
        }
        for route in ROUTES.iter().filter(|route| route.bare_target) {
            let seeded = seed("t", view, true, false).await;
            assert_writes(&seeded, route, "t").await;
        }
    }
    let seeded = seed("t", "t", true, false).await;
    assert!(held(&seeded, "T").is_some());
    for route in &ROUTES {
        let seeded = seed("t", "t", true, false).await;
        assert_refuses(&seeded, route, "T", "t").await;
    }
}

#[tokio::test]
async fn a_non_ascii_view_name_folds_with_unicode_lower_casing() {
    for (view, stored) in [("`Ä`", "ä"), ("`ä`", "ä")] {
        let seeded = seed("ä", view, false, false).await;
        assert_eq!(
            seeded.session.list_temp_view_names().expect("names"),
            vec![stored.to_string()]
        );
        for name in ["`Ä`", "`ä`", "\"Ä\"", "\"ä\""] {
            assert_eq!(
                view_ids(&seeded, name).await,
                VIEW_ROW,
                "{name} under {view}"
            );
        }
        for name in ["`Ä`", "`ä`"] {
            for route in &ROUTES {
                let seeded = seed("ä", view, false, false).await;
                assert_refuses(&seeded, route, name, "`ä`").await;
            }
        }
    }
}

#[tokio::test]
async fn a_non_ascii_view_name_is_exact_under_the_case_sensitive_rule() {
    let seeded = seed("ä", "`Ä`", true, true).await;
    assert_eq!(
        seeded.session.list_temp_view_names().expect("names"),
        vec!["Ä".to_string()]
    );
    assert!(held(&seeded, "`ä`").is_none());
    for route in &ROUTES {
        let seeded = seed("ä", "`Ä`", true, true).await;
        assert_refuses(&seeded, route, "`Ä`", "`Ä`").await;
    }
    for route in ROUTES.iter().filter(|route| route.bare_target) {
        let seeded = seed("ä", "`Ä`", true, true).await;
        assert_writes(&seeded, route, "`ä`").await;
    }
}

#[tokio::test]
async fn a_data_frame_view_registered_under_a_non_ascii_name_is_stored_folded() {
    let seeded = seed("ä", "w", false, false).await;
    let frame = seeded.session.sql("SELECT 99 AS id").await.expect("frame");
    seeded
        .session
        .create_or_replace_temp_view_from("\"Ä\"", &frame)
        .expect("register");
    let mut names = seeded.session.list_temp_view_names().expect("names");
    names.sort();
    assert_eq!(names, vec!["w".to_string(), "ä".to_string()]);
    for route in &ROUTES {
        assert_refuses(&seeded, route, "`ä`", "`Ä`").await;
    }
    assert!(seeded.session.drop_temp_view("\"ä\"").expect("drop"));
    assert!(held(&seeded, "`Ä`").is_none());
    assert_eq!(table_ids(&seeded).await, SEEDED);
}
