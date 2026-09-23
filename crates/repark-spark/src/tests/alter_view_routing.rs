use super::super::*;
use super::common::*;
use super::describe_view_routing::{FaultCatalog, ViewFaults, ViewlessCatalog, register_catalog};
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::ErrorKind;

async fn collected_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &str,
) -> Vec<(i32, String)> {
    let batches = execute(ctx, catalogs, statement)
        .await
        .expect("query must plan")
        .collect()
        .await
        .expect("query must collect");
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("ids");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("names");
            (0..batch.num_rows())
                .map(move |index| (ids.value(index), names.value(index).to_string()))
        })
        .collect()
}

async fn assert_view_write_names_match(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &str,
) {
    let full_error = execute(ctx, catalogs, statement)
        .await
        .expect_err("three-part view target must refuse");
    let DataFusionError::Plan(expected) = full_error else {
        panic!("expected Plan refusal, got {full_error:?}");
    };
    crate::use_ddl::set_session_defaults(ctx, catalogs, "ice", "sales");
    let bare = statement.replace("ice.sales.v", "v");
    let bare_error = execute(ctx, catalogs, &bare)
        .await
        .expect_err("bare view target must refuse");
    let DataFusionError::Plan(bare_message) = bare_error else {
        panic!("expected Plan refusal, got {bare_error:?}");
    };
    assert_eq!(bare_message, expected);
    crate::use_ddl::set_session_defaults(ctx, catalogs, "ice", "");
    let two_part = statement.replace("ice.sales.v", "sales.v");
    let two_part_error = execute(ctx, catalogs, &two_part)
        .await
        .expect_err("two-part view target must refuse");
    let DataFusionError::Plan(two_part_message) = two_part_error else {
        panic!("expected Plan refusal, got {two_part_error:?}");
    };
    assert_eq!(two_part_message, expected);
}

#[tokio::test]
async fn insert_view_names_match_and_bare_table_commits() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    assert_view_write_names_match(&ctx, &catalogs, "INSERT INTO ice.sales.v VALUES (9, 'd')").await;
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
    run(&ctx, &catalogs, "INSERT INTO src VALUES (9, 'd')").await;
    assert_eq!(
        collected_rows(&ctx, &catalogs, "SELECT * FROM src ORDER BY id").await,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (9, "d".into())
        ]
    );
}

#[tokio::test]
async fn insert_by_name_view_names_match_and_table_control_commits() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.src AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    assert_view_write_names_match(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.v BY NAME SELECT 9 AS id, 'd' AS name",
    )
    .await;
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT INTO src BY NAME SELECT 9 AS id, 'd' AS name",
    )
    .await
    .expect_err("bare BY NAME retains its three-part requirement");
    let DataFusionError::Plan(message) = error else {
        panic!("expected Plan refusal, got {error:?}");
    };
    assert_eq!(
        message,
        "INSERT INTO src BY NAME needs a three-part Iceberg table name, got `src`"
    );
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.src BY NAME SELECT 9 AS id, 'd' AS name",
    )
    .await;
    assert_eq!(
        collected_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.src ORDER BY id").await,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (9, "d".into())
        ]
    );
}

#[tokio::test]
async fn delete_view_names_match_and_bare_table_rows_change() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    assert_view_write_names_match(&ctx, &catalogs, "DELETE FROM ice.sales.v WHERE id = 1").await;
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
    run(&ctx, &catalogs, "DELETE FROM src WHERE id = 1").await;
    assert_eq!(
        collected_rows(&ctx, &catalogs, "SELECT * FROM src ORDER BY id").await,
        vec![(2, "b".into()), (3, "c".into())]
    );
}

#[tokio::test]
async fn update_view_names_match_and_bare_table_rows_change() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    assert_view_write_names_match(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.v SET id = 9 WHERE id = 1",
    )
    .await;
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "ice", "sales");
    run(&ctx, &catalogs, "UPDATE src SET id = 9 WHERE id = 1").await;
    assert_eq!(
        collected_rows(&ctx, &catalogs, "SELECT * FROM src ORDER BY id").await,
        vec![(2, "b".into()), (3, "c".into()), (9, "a".into())]
    );
}

#[tokio::test]
async fn alter_viewless_catalog_matches_memory_missing_targets_and_redirects_tables() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let catalog = Arc::new(ViewlessCatalog {
        inner: catalogs["ice"].clone(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    for statement in [
        "ALTER VIEW {catalog}.sales.absent SET TBLPROPERTIES ('k'='v')",
        "ALTER VIEW {catalog}.sales.absent UNSET TBLPROPERTIES ('k')",
        "ALTER VIEW {catalog}.sales.absent UNSET TBLPROPERTIES IF EXISTS ('k')",
        "ALTER VIEW {catalog}.sales.absent RENAME TO {catalog}.sales.b",
    ] {
        let memory_sql = statement.replace("{catalog}", "ice");
        let viewless_sql = statement.replace("{catalog}", "fault");
        let memory_error = execute(&ctx, &catalogs, &memory_sql)
            .await
            .expect_err("memory catalog must refuse missing view");
        assert!(matches!(memory_error, DataFusionError::Plan(_)));
        let memory_error = memory_error.to_string();
        let viewless_error = execute(&ctx, &catalogs, &viewless_sql)
            .await
            .expect_err("viewless catalog must refuse missing view");
        assert!(matches!(viewless_error, DataFusionError::Plan(_)));
        let viewless_error = viewless_error.to_string();
        assert_eq!(viewless_error, memory_error.replace("`ice`", "`fault`"));
        let expected = if statement.contains("RENAME TO") {
            "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `fault`.`sales`.`absent` cannot be found. Verify the spelling and correctness of the schema and catalog. If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
        } else {
            "Error during planning: [UNSUPPORTED_FEATURE.CATALOG_OPERATION] The feature is not supported: Catalog `fault` does not support views. SQLSTATE: 0A000"
        };
        assert_eq!(viewless_error, expected);
    }
    for statement in [
        "ALTER VIEW fault.sales.t SET TBLPROPERTIES ('k'='v')",
        "ALTER VIEW fault.sales.t UNSET TBLPROPERTIES ('k')",
        "ALTER VIEW fault.sales.t UNSET TBLPROPERTIES IF EXISTS ('k')",
    ] {
        let error = execute(&ctx, &catalogs, statement)
            .await
            .expect_err("table name must refuse view property update");
        assert!(matches!(error, DataFusionError::Plan(_)));
        assert_eq!(
            error.to_string(),
            "Error during planning: [UNSUPPORTED_FEATURE.CATALOG_OPERATION] The feature is not supported: Catalog `fault` does not support views. SQLSTATE: 0A000"
        );
    }
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.t RENAME TO fault.sales.b",
    )
    .await
    .expect_err("table rename must redirect");
    assert!(matches!(error, DataFusionError::Plan(_)));
    let error = error.to_string();
    assert_eq!(
        error,
        "Error during planning: Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead."
    );
}

#[tokio::test]
async fn alter_view_load_failure_propagates_without_table_probe() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let table_exists_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: Some(ErrorKind::Unexpected),
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            table_exists_calls: Some(table_exists_calls.clone()),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.absent RENAME TO fault.sales.b",
    )
    .await
    .expect_err("load failure must propagate");
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External error, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected an Iceberg error");
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    let error = error.to_string();
    assert!(error.contains("injected load_view failure"), "{error}");
    assert!(!error.contains("TABLE_OR_VIEW_NOT_FOUND"), "{error}");
    assert!(!error.contains("CATALOG_OPERATION"), "{error}");
    assert_eq!(table_exists_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn alter_view_rename_view_exists_failure_propagates_without_rename() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    let rename_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            view_exists_failure: Some(ErrorKind::Unexpected),
            rename_view_calls: Some(rename_calls.clone()),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.v RENAME TO fault.sales.b",
    )
    .await
    .expect_err("existence failure must propagate");
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External error, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected an Iceberg error");
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    let error = error.to_string();
    assert!(error.contains("injected view_exists failure"), "{error}");
    assert_eq!(rename_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn alter_view_missing_rename_table_exists_failure_propagates() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            table_exists_failure: Some(ErrorKind::Unexpected),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.absent RENAME TO fault.sales.b",
    )
    .await
    .expect_err("table existence failure must propagate");
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External error, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected an Iceberg error");
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    let error = error.to_string();
    assert!(error.contains("injected table_exists failure"), "{error}");
    assert!(!error.contains("TABLE_OR_VIEW_NOT_FOUND"), "{error}");
}

#[tokio::test]
async fn alter_view_unset_if_exists_updates_only_when_a_key_is_present() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v TBLPROPERTIES ('k'='v','k2'='w') AS SELECT * FROM src",
    )
    .await;
    let update_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            update_view_calls: Some(update_calls.clone()),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.v UNSET TBLPROPERTIES IF EXISTS ('absent')",
    )
    .await;
    assert_eq!(update_calls.load(Ordering::SeqCst), 0);
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.v UNSET TBLPROPERTIES IF EXISTS ('k')",
    )
    .await;
    assert_eq!(update_calls.load(Ordering::SeqCst), 1);
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v".to_string());
    let view = catalogs["fault"]
        .load_view(&ident)
        .await
        .expect("updated view");
    assert!(!view.metadata().properties().contains_key("k"));
    assert_eq!(
        view.metadata().properties().get("k2"),
        Some(&"w".to_string())
    );
}

#[tokio::test]
async fn alter_view_set_overwrites_property_and_adds_another_once() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v TBLPROPERTIES ('k'='v') AS SELECT * FROM src",
    )
    .await;
    let update_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            update_view_calls: Some(update_calls.clone()),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW fault.sales.v SET TBLPROPERTIES ('k'='v2','j'='u')",
    )
    .await;
    assert_eq!(update_calls.load(Ordering::SeqCst), 1);
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v".to_string());
    let view = catalogs["fault"]
        .load_view(&ident)
        .await
        .expect("updated view");
    assert_eq!(
        view.metadata().properties().get("k"),
        Some(&"v2".to_string())
    );
    assert_eq!(
        view.metadata().properties().get("j"),
        Some(&"u".to_string())
    );
}

#[tokio::test]
async fn alter_view_bare_name_uses_session_defaults_and_commits_once() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    let update_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults {
            update_view_calls: Some(update_calls.clone()),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    run(&ctx, &catalogs, "USE fault.sales").await;
    run(&ctx, &catalogs, "ALTER VIEW v SET TBLPROPERTIES ('j'='u')").await;
    assert_eq!(update_calls.load(Ordering::SeqCst), 1);
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v".to_string());
    let view = catalogs["fault"]
        .load_view(&ident)
        .await
        .expect("updated view");
    assert_eq!(
        view.metadata().properties().get("j"),
        Some(&"u".to_string())
    );
}

#[tokio::test]
async fn view_create_drop_and_write_guard_complete_bare_names_from_use() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "CREATE VIEW v AS SELECT * FROM src").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v".to_string());
    assert!(
        catalogs["ice"]
            .view_exists(&ident)
            .await
            .expect("view exists")
    );
    let statements =
        Parser::parse_sql(&DatabricksDialect {}, "DROP VIEW v").expect("drop view statement");
    let Statement::Drop { names, .. } = &statements[0] else {
        panic!("expected DROP VIEW");
    };
    let error = crate::view_ddl::execute::refuse_view_write_target(&ctx, &catalogs, &names[0])
        .await
        .expect_err("view write target must refuse");
    assert_eq!(
        error.to_string(),
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ice`.`sales`.`v` cannot be found. Verify the spelling and correctness of the schema and catalog. If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    );
    run(&ctx, &catalogs, "DROP VIEW v").await;
    assert!(
        !catalogs["ice"]
            .view_exists(&ident)
            .await
            .expect("view removed")
    );
}

#[tokio::test]
async fn alter_view_router_keeps_the_tail_parser_refusal() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(&ctx, &catalogs, "ALTER VIEW ice.sales.v SET OTHER")
        .await
        .expect_err("malformed SET must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    let error = error.to_string();
    assert_eq!(
        error,
        "Error during planning: could not parse `ALTER VIEW`: expected TBLPROPERTIES after SET"
    );
}

#[tokio::test]
async fn alter_view_rename_rejects_cross_catalog_and_existing_destinations() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.b AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW ice.sales.v RENAME TO other.sales.b",
    )
    .await
    .expect_err("cross catalog move must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    let error = error.to_string();
    assert_eq!(
        error,
        "Error during planning: Cannot move view between catalogs: from=ice and to=other"
    );
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER VIEW ice.sales.v RENAME TO ice.sales.b",
    )
    .await
    .expect_err("existing view must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    let error = error.to_string();
    assert_eq!(
        error,
        "Error during planning: [VIEW_ALREADY_EXISTS] Cannot create view sales.b because it already exists.\nChoose a different name, drop or replace the existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects. SQLSTATE: 42P07"
    );
}

#[tokio::test]
async fn alter_view_rename_moves_the_view() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW ice.sales.v RENAME TO ice.sales.b",
    )
    .await;
    let source = TableIdent::new(NamespaceIdent::new("sales".to_string()), "v".to_string());
    let destination = TableIdent::new(NamespaceIdent::new("sales".to_string()), "b".to_string());
    assert!(
        !catalogs["ice"]
            .view_exists(&source)
            .await
            .expect("source existence")
    );
    assert!(
        catalogs["ice"]
            .view_exists(&destination)
            .await
            .expect("destination existence")
    );
}

#[tokio::test]
async fn alter_view_router_refuses_statement_write_options() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let options = crate::write_options::StatementWriteOptions {
        raw: vec![("write-format".to_string(), "parquet".to_string())],
        ..crate::write_options::StatementWriteOptions::empty()
    };
    let error = crate::router::execute_with_statement_options(
        &ctx,
        &catalogs,
        "ALTER VIEW ice.sales.v SET TBLPROPERTIES ('k'='v')",
        &std::collections::HashSet::<String>::new(),
        &options,
    )
    .await
    .expect_err("ALTER VIEW cannot take statement write options");
    assert!(matches!(error, DataFusionError::Plan(_)));
    let error = error.to_string();
    assert_eq!(
        error,
        "Error during planning: ALTER VIEW does not support write options (write-format); they are only honoured on Iceberg table writes (ICE-WRITE-OPTIONS-1)"
    );
}
