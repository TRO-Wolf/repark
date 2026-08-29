//! Temp-view API, raw-context escape hatch, and PREPARE behavior pins.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use repark_core::{Error, ReparkSession};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, true),
        Field::new("close", DataType::Float64, false),
    ]))
}

fn rows() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec!["AAA", "AAA"])),
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(2)])),
            Arc::new(Float64Array::from(vec![100.0, 101.0])),
        ],
    )
    .unwrap()
}

fn keys(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

/// An Iceberg-catalog session with a tightened temp view `tight` and namespace `ice.sales`.
async fn native_ddl_sink_session() -> (tempfile::TempDir, ReparkSession) {
    let warehouse_dir = tempfile::TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::new().unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let batch = rows();
    session
        .register_record_batches_as_temp_view("tight", batch.schema(), vec![batch])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(&["symbol", "ts"]), true)
        .await
        .unwrap();
    (warehouse_dir, session)
}

// ===========================================================================================
// Temp-view API, raw-context escape hatch, and PREPARE behavior pins.
// ===========================================================================================

#[tokio::test]
async fn qualified_temp_view_name_refuses_and_persists_nothing() {
    // A qualified temp-view name must not bypass the SQL guard and persist into an Iceberg catalog.
    let (_dir, session) = native_ddl_sink_session().await;
    let tightened = session
        .context()
        .table_provider("tight")
        .await
        .expect("the tightened temp view is registered")
        .schema();
    let empty_tight = session
        .sql("SELECT * FROM tight LIMIT 0")
        .await
        .expect("plan a LIMIT 0 over the tightened view");

    let batch_err = session
        .create_or_replace_temp_view("ice.sales.v3", vec![rows()])
        .expect_err("a qualified temp-view name must refuse (batches overload)");
    let from_err = session
        .create_or_replace_temp_view_from("ice.sales.vlazy", &empty_tight)
        .expect_err("a qualified temp-view name must refuse (plan overload)");
    let batches_err = session
        .register_record_batches_as_temp_view("ice.sales.vempty", tightened, vec![])
        .expect_err("a qualified temp-view name must refuse (record-batches overload)");
    let materialize_err = session
        .materialize_dataframe_as_temp_view("ice.sales.vmat", empty_tight)
        .await
        .expect_err("a qualified temp-view name must refuse (materialize overload)");
    let drop_err = session
        .drop_temp_view("ice.sales.vmat")
        .expect_err("a qualified temp-view name must refuse (drop)");

    for (label, error) in [
        ("batches", batch_err),
        ("from-plan", from_err),
        ("record-batches", batches_err),
        ("materialize", materialize_err),
        ("drop", drop_err),
    ] {
        assert!(
            matches!(error, Error::Analysis(_)),
            "{label}: PySpark's class for a qualified temp-view name is AnalysisException, got \
             {error:?}"
        );
        assert!(
            error.to_string().contains("SESSION-LOCAL"),
            "{label}: the refusal must say why, got: {error}"
        );
    }
    // The unpublished half: nothing reached the catalog under ANY of the names.
    for name in [
        "ice.sales.v3",
        "ice.sales.vlazy",
        "ice.sales.vempty",
        "ice.sales.vmat",
    ] {
        assert!(
            !session.table_exists(name).await.unwrap(),
            "`{name}` must not exist — the temp-view API may never write a catalog"
        );
    }
}

#[tokio::test]
async fn set_default_catalog_cannot_move_a_temp_view_into_a_catalog() {
    // R6-1. Kills resolving a one-part temp-view name against the live default catalog.
    let (_dir, session) = native_ddl_sink_session().await;
    session
        .sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("SET default_catalog");
    session
        .sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("SET default_schema");

    session
        .create_or_replace_temp_view("vbare", vec![rows()])
        .expect("a one-part temp view must still register after the SET");
    assert!(
        !session.table_exists("ice.sales.vbare").await.unwrap(),
        "the SET must not move the registration into the Iceberg catalog"
    );
    assert!(
        session.table_exists("vbare").await.unwrap(),
        "`tableExists` on a one-part name asks the pinned temp-view home"
    );
    assert!(
        session
            .list_temp_view_names()
            .unwrap()
            .contains(&"vbare".to_string()),
        "the listing reads the pinned home too, not the live default catalog"
    );
    // Raw SQL retains DataFusion's live-default resolution; the home-qualified spelling reads it.
    assert!(session.sql("SELECT * FROM vbare").await.is_err());
    session
        .sql("SELECT * FROM datafusion.public.vbare")
        .await
        .expect("the view is in the pinned home")
        .collect()
        .await
        .expect("and it reads back");
}

#[tokio::test]
async fn context_sql_is_a_known_unguarded_hatch() {
    // R6-2. `context().sql` is the documented unguarded hatch and may persist tightened DDL.
    let (_dir, session) = native_ddl_sink_session().await;
    session
        .context()
        .sql("CREATE VIEW ice.sales.v_hatch AS SELECT * FROM tight LIMIT 0")
        .await
        .expect("KNOWN HATCH: the raw context has no pre-execute guard")
        .collect()
        .await
        .expect("KNOWN HATCH: and it executes");
    assert!(
        session.table_exists("ice.sales.v_hatch").await.unwrap(),
        "KNOWN HATCH: the raw context persisted the table the guarded doors refuse"
    );
    // The same statement on the guarded door refuses — the contrast is the point.
    let error = session
        .sql("CREATE VIEW ice.sales.v_door AS SELECT * FROM tight LIMIT 0")
        .await
        .expect_err("the product door refuses what the hatch allows");
    assert!(error.to_string().contains("tightenNulls"), "{error}");
    assert!(!session.table_exists("ice.sales.v_door").await.unwrap());
}

#[tokio::test]
async fn prepare_of_a_tightened_ddl_sink_is_inert_today() {
    // DataFusion 54.1 cannot execute prepared DDL. This pin fails if that compatibility floor moves.
    let (_dir, session) = native_ddl_sink_session().await;
    session
        .sql("PREPARE p_sink AS CREATE VIEW ice.sales.v_prepared AS SELECT * FROM tight LIMIT 0")
        .await
        .expect("PREPARE only stores the statement")
        .collect()
        .await
        .expect("collecting the PREPARE itself");
    assert!(
        !session.table_exists("ice.sales.v_prepared").await.unwrap(),
        "PREPARE must not publish the stored statement's target"
    );
    let executed = session.sql("EXECUTE p_sink").await;
    match executed {
        Ok(frame) => {
            let error = frame
                .collect()
                .await
                .expect_err("measured today: a prepared DDL cannot execute");
            assert!(
                error.to_string().contains("CreateView"),
                "the inert-today reason must stay visible: {error}"
            );
        }
        Err(error) => {
            // Equally pinned: refusing earlier is fine, persisting is not.
            assert!(!error.to_string().is_empty());
        }
    }
    assert!(
        !session.table_exists("ice.sales.v_prepared").await.unwrap(),
        "EXECUTE of a prepared tightened CREATE VIEW must persist nothing"
    );
}

#[tokio::test]
async fn a_catalog_over_the_build_time_default_is_not_a_temp_view_home() {
    // A catalog can replace the configured home provider. Identity checks must then refuse every
    // temp-view entry point before the required-schema payload reaches that catalog.
    let warehouse_dir = tempfile::TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .config("datafusion.catalog.default_catalog", "ice")
        .config("datafusion.catalog.default_schema", "sales")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();

    let batches_err = session
        .register_record_batches_as_temp_view("vempty", schema(), vec![])
        .expect_err("no session-local home left: the record-batches path must refuse");
    let batch_err = session
        .create_or_replace_temp_view("vbatch", vec![rows()])
        .expect_err("no session-local home left: the batches path must refuse");
    let drop_err = session
        .drop_temp_view("vempty")
        .expect_err("no session-local home left: drop must refuse too");
    let declare_err = session
        .declare_temp_view_sorted("vempty", &keys(&["symbol"]), true)
        .await
        .expect_err("no session-local home left: declare-sorted must refuse");
    let list_err = session
        .list_temp_view_names()
        .expect_err("listing must refuse rather than report a CATALOG's tables as temp views");
    let exists_err = session
        .table_exists("vempty")
        .await
        .expect_err("the one-part existence arm must refuse rather than probe the catalog");

    for (label, error) in [
        ("record-batches", batches_err),
        ("batches", batch_err),
        ("drop", drop_err),
        ("declare-sorted", declare_err),
        ("list", list_err),
        ("table_exists one-part", exists_err),
    ] {
        assert!(
            matches!(error, Error::Analysis(_)),
            "{label}: the class is AnalysisException, got {error:?}"
        );
        assert!(
            error.to_string().contains("SESSION-LOCAL"),
            "{label}: the refusal must say why, got: {error}"
        );
    }
    // The unpublished half: the S1 payload cannot reach the catalog by any of those names.
    for name in ["ice.sales.vempty", "ice.sales.vbatch"] {
        assert!(
            !session.table_exists(name).await.unwrap(),
            "`{name}` must not exist — the temp-view API may never write a catalog"
        );
    }
}

#[tokio::test]
async fn a_quoted_dotted_temp_view_name_round_trips_through_table_exists() {
    // Re-parsing a quoted dotted segment makes one identifier look qualified. Use the parsed segment.
    // Unquoted segments still use the existing case fold.
    let session = ReparkSession::new().unwrap();
    session
        .create_or_replace_temp_view("\"a.b\"", vec![rows()])
        .expect("a QUOTED dotted name is one identifier and is allowed");
    assert_eq!(session.list_temp_view_names().unwrap(), vec!["a.b"]);
    assert!(session.table_exists("\"a.b\"").await.unwrap());
    assert!(
        session.table_exists("`a.b`").await.unwrap(),
        "the backtick spelling reads the same identifier"
    );
    assert!(!session.table_exists("nope").await.unwrap());

    session
        .create_or_replace_temp_view("MyView", vec![rows()])
        .unwrap();
    assert!(session.table_exists("MyView").await.unwrap());
    assert!(session.table_exists("myview").await.unwrap());
    assert!(session.drop_temp_view("\"a.b\"").unwrap());
    assert!(!session.table_exists("\"a.b\"").await.unwrap());
}

#[tokio::test]
async fn set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read() {
    // Raw SQL reads follow the live default catalog. Product APIs keep the pinned temp-view home.
    // The write path must never follow `SET datafusion.catalog.default_catalog` into a catalog.
    let session = ReparkSession::new().unwrap();
    session.context().register_catalog(
        "mem",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    session
        .context()
        .catalog("mem")
        .unwrap()
        .register_schema(
            "public",
            Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
        )
        .unwrap();
    session
        .sql("SET datafusion.catalog.default_catalog = 'mem'")
        .await
        .expect("SET default_catalog");

    session
        .create_or_replace_temp_view("v2", vec![rows()])
        .expect("the write stays in the build-time home");
    assert!(session.table_exists("v2").await.unwrap());
    assert!(
        session
            .context()
            .catalog("mem")
            .unwrap()
            .schema("public")
            .unwrap()
            .table_names()
            .is_empty(),
        "the SET must not move the registration into the other catalog"
    );
    assert!(
        session.sql("SELECT * FROM v2").await.is_err(),
        "the READ path is DataFusion's and still follows the live default (disclosed, not fixed)"
    );
    session
        .sql("SELECT * FROM datafusion.public.v2")
        .await
        .expect("naming the home reads it back");

    // R7-1. Raw SQL follows the live default; product reads use the build-time home spelling.
    assert_eq!(
        session.temp_view_home().unwrap(),
        vec!["datafusion".to_string(), "public".to_string()],
        "the home a product read path prefixes with is the BUILD-time home, not the SET default"
    );
    assert_eq!(
        session.resolve_temp_view_home_ref("v2").unwrap(),
        Some(vec![
            "datafusion".to_string(),
            "public".to_string(),
            "v2".to_string()
        ]),
        "a view that exists in the home resolves to the home-qualified spelling under the SET"
    );
    assert_eq!(
        session.resolve_temp_view_home_ref("nope").unwrap(),
        None,
        "a name that is no temp view answers None, so the caller falls back to catalog rules"
    );
    assert_eq!(
        session.resolve_temp_view_home_ref("mem.public.v2").unwrap(),
        None,
        "a qualified name is not a temp-view spelling — it is not this resolver's question"
    );
    // The spelling the resolver hands back is accepted by the temp-view API as the SAME view.
    session
        .create_or_replace_temp_view("datafusion.public.v2", vec![rows()])
        .expect("the session's own home spelling is the home, not a qualified refusal");
    assert!(session.table_exists("v2").await.unwrap());
    assert!(session.drop_temp_view("datafusion.public.v2").unwrap());
    assert!(!session.table_exists("v2").await.unwrap());
}

#[tokio::test]
async fn a_catalog_over_the_home_refuses_the_read_spelling_too() {
    // R7-1 + R6-1. The read-side seam must refuse when a catalog takes the session-local home.
    let warehouse_dir = tempfile::TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .config("datafusion.catalog.default_catalog", "ice")
        .config("datafusion.catalog.default_schema", "sales")
        .build()
        .unwrap();
    assert_eq!(
        session.temp_view_home().unwrap(),
        vec!["ice".to_string(), "sales".to_string()],
        "before the catalog lands, the configured home is a real session-local schema"
    );
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    for error in [
        session.temp_view_home().unwrap_err(),
        session.resolve_temp_view_home_ref("v").unwrap_err(),
    ] {
        let message = error.to_string();
        assert!(
            matches!(error, Error::Analysis(_))
                && message.contains("no session-local temp-view home"),
            "the read seam must refuse with the same home check as the write seam, got: {message}"
        );
    }
}
