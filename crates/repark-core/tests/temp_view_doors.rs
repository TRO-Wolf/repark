//! SQM round 6 — the temp-view API as a catalog-write door (R6-1), the documented `context()`
//! escape hatch (R6-2), and the PREPARE class (R6-5). BASE-of-round = `68e98f4`.
//!
//! Split from `declared_sorted.rs` (which had reached its file-size ceiling) rather than grown
//! into it; the fixture below is this file's own, deliberately identical in shape to that file's
//! `native_ddl_sink_session` so the two read side by side.

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
// SQM round 6 — the temp-view API as a catalog-write door (R6-1), the documented `context()`
// hatch (R6-2), and PREPARE (R6-5). BASE-of-round = `68e98f4`.
// ===========================================================================================

#[tokio::test]
async fn qualified_temp_view_name_refuses_and_persists_nothing() {
    // R6-1 (S1). Kills: `replace_view` forwarding the raw name to
    // `SessionContext::register_table`, which resolves a qualified name into the target schema
    // provider — for an Iceberg catalog that PERSISTS a real table.
    //
    // MEASURED on BASE (`68e98f4`), same session shape as below:
    //
    // | call | BASE result | BASE `table_exists` |
    // |---|---|---|
    // | `register_record_batches_as_temp_view("ice.sales.vempty", <tightened schema>, [])` | **Ok** | **true** |
    // | `create_or_replace_temp_view_from("ice.sales.vlazy", <tightened LIMIT 0 frame>)` | **Ok** | **true** |
    // | `materialize_dataframe_as_temp_view("ice.sales.vmat", <tightened LIMIT 0 frame>)` | **Ok** | **true** |
    // | `create_or_replace_temp_view("ice.sales.v3", <non-empty batches>)` | Err (Iceberg "register_table does not support tables with data") | false |
    //
    // i.e. the temp-view API was a THIRD write door into an Iceberg catalog, carrying exactly
    // the `tightenNulls` `required: true` payload the pre-execute belt refuses on the SQL doors
    // — and it never went near a guard, because no statement was ever planned.
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
    // R6-1 (S1), the SET half. Kills: resolving a ONE-part temp-view name against the LIVE
    // `datafusion.catalog.default_catalog` (which is what `register_table(&str)` did).
    //
    // MEASURED on BASE (`68e98f4`): after `SET datafusion.catalog.default_catalog = 'ice'` +
    // `default_schema = 'sales'`, `create_or_replace_temp_view("vbare", <non-empty>)` returned
    // **Err(Iceberg "register_table does not support tables with data")** — the registration had
    // LEFT the session and hit the Iceberg schema provider; with an empty/lazy body it would have
    // persisted, exactly like the qualified spellings above. Either way the caller could no
    // longer create a temp view at all.
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
    // MEASURED and recorded rather than asserted-as-good: DataFusion resolves the name in a SQL
    // body against the LIVE default too, so `SELECT * FROM vbare` does NOT see the temp view
    // while the SET is in force (Spark would; DataFusion has no temp-view namespace to search
    // first). Naming the home explicitly finds it. Scoped out of R6-1, which is about the temp
    // view API never WRITING a catalog.
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
    // R6-2 (RULED: documentation, not a guard). `ReparkSession::context()` hands back the raw
    // DataFusion `SessionContext`; `context().sql` bypasses the pre-execute belt, so DDL through
    // it still persists a tightened schema. That is the DOCUMENTED behaviour (see the rustdoc on
    // `ReparkSession::context`) — closing it would mean wrapping DataFusion.
    //
    // This pin exists so the hatch cannot change silently: if a future round DOES guard it, this
    // test goes red and the ledger's claim has to move with it.
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
    // R6-5 (S3). `PREPARE` STORES a statement; the `sorted_view` guard runs on EXECUTED DDL
    // (`PreExecute::guard` on the planned statement), so a `CreateView` hidden inside a PREPARE
    // is never inspected. Is that a leak today? MEASURED on THIS head, native door:
    //
    // | step | result | persisted? |
    // |---|---|---|
    // | `PREPARE p_sink AS CREATE VIEW ice.sales.v_prepared AS SELECT * FROM tight LIMIT 0` | **Ok** | `table_exists` **false** |
    // | `.collect()` on that PREPARE | **Ok**, 0 batches | **false** |
    // | `EXECUTE p_sink` | Ok (plans) | — |
    // | `.collect()` on the EXECUTE | **Err** — `NotImplemented`: "Unsupported logical plan: CreateView" | **false** |
    //
    // So the class is inert TODAY: DataFusion 54.1 cannot execute a prepared DDL at all. Not a
    // guard, a measured floor — this node pins it, so the day `EXECUTE` starts running the
    // stored DDL, this test goes red instead of the leak going unnoticed.
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
    // R6-1 (S1), the BUILD-time half — the round-6 critic's red. Kills: pinning the temp-view
    // home to the CONFIGURED default catalog NAME and stopping there. `datafusion.*` builder
    // keys are first-class (`DATAFUSION_CONFIG_PREFIX`), so `default_catalog = ice` at BUILD
    // makes the pinned home name `ice.sales` — and `register_memory_catalog("ice")` then
    // REPLACES the provider that name resolves to with the Iceberg one. The name-pinned fix
    // therefore pinned the leak IN, not out.
    //
    // MEASURED on the name-only fix (this file's tree before the S1 patch), same session shape:
    //
    // | call | name-only fix | `table_exists("ice.sales.vempty")` |
    // |---|---|---|
    // | `register_record_batches_as_temp_view("vempty", <required schema>, [])` | **Ok** | **true** |
    // | persisted provider schema | `[("symbol", nullable=false), ("ts", true), ("close", false)]` — the `required: true` tighten payload, PERSISTED | |
    // | `list_temp_view_names()` | `["vempty"]` — simultaneously reported as a session temp view | |
    //
    // MEASURED after the S1 patch (this test): every temp-view entry point refuses
    // `Error::Analysis`, and the catalog stays empty. The home now snapshots the schema PROVIDER
    // and re-checks its identity live (`Arc::ptr_eq`) — MEASURED false after the catalog
    // registration, true for repeated lookups of an untouched home.
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
    // R6-1 follow-on (round-6 critic S3). Kills: `table_exists`'s one-part arm re-parsing an
    // ALREADY-parsed segment. `parse_table_identifier_segments` strips the quotes, so `"a.b"`
    // arrived at the name seam as the bare string `a.b` and was refused as "qualified" — an
    // allowed spelling that could be created, listed and dropped but never asked about.
    //
    // MEASURED before this fix: `create_or_replace_temp_view("\"a.b\"")` = Ok,
    // `list_temp_view_names()` = `["a.b"]`, `drop_temp_view("\"a.b\"")` = Ok(true), but
    // `table_exists("\"a.b\"")` = Err(Analysis "… is qualified …"). On BASE it was Ok(false)
    // (also wrong, and not an error). MEASURED now: all four agree.
    //
    // The second half kills dropping BASE's case folding: the segment path must lowercase an
    // UNQUOTED name exactly like `TableReference::parse_str` does, or `tableExists("MyView")`
    // would stop finding the view `createOrReplaceTempView("MyView")` registered as `myview`.
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
    // R6-1 disclosure, pinned rather than only narrated (round-6 critic S3). The write side is
    // now immune to `SET datafusion.catalog.default_catalog`; the READ side (DataFusion's own
    // bare-name resolution inside a SQL body) is untouched. So under a `SET` to ANY other
    // catalog — including a plain non-Iceberg one — a create-then-read-by-bare-name round trip
    // that worked on BASE now misses. This is a MEASURED behaviour CHANGE, not "the current
    // behaviour": it is the price of the write-side pin, and it is disclosed here and in the
    // round-6 ledger rather than left for a reader to trip over.
    //
    // MEASURED, both sides (BASE mechanism = `context().register_table(<raw &str>, provider)`,
    // the exact call BASE's `replace_view` made):
    //   BASE mech: register_table("v2") = Ok → landed in `mem.public` (`table_names() == ["v2"]`)
    //              → `SELECT * FROM v2` = Ok.
    //   FIXED:     create_or_replace_temp_view("v2") = Ok → landed in `datafusion.public`
    //              → `SELECT * FROM v2` = Err(Analysis "table 'mem.public.v2' not found")
    //              → `SELECT * FROM datafusion.public.v2` = Ok, `table_exists("v2")` = true.
    // Reachability is low: the facade's currentCatalog/setCurrentCatalog is facade-only state and
    // never issues this SET (`python/repark/src/repark/spark/catalog.py`), so only a raw
    // `spark.sql("SET datafusion...")` reaches it.
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
}
