use super::super::*;
use super::common::*;
use super::describe_view_routing::{FaultCatalog, ViewFaults, ViewlessCatalog, register_catalog};
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::ErrorKind;

use crate::view_ddl::execute::execute_show_tblproperties;
use crate::view_ddl::parse::ShowTblpropertiesStatement;

fn statement(catalog: &str, name: &str) -> ShowTblpropertiesStatement {
    ShowTblpropertiesStatement {
        name: vec![catalog.to_string(), "sales".to_string(), name.to_string()],
        key: None,
    }
}

fn fault_catalog(
    inner: Arc<dyn Catalog>,
    view_failure: Option<ErrorKind>,
    view_calls: Arc<AtomicUsize>,
    faults: ViewFaults,
) -> Arc<FaultCatalog> {
    Arc::new(FaultCatalog {
        inner,
        table_failure: None,
        view_failure,
        view_calls,
        faults,
    })
}

fn assert_iceberg_error(error: &DataFusionError, kind: ErrorKind, message: &str) {
    let DataFusionError::External(inner) = error else {
        panic!("expected External, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected Iceberg error");
    assert_eq!(source.kind(), kind);
    assert_eq!(source.to_string(), message);
}

#[tokio::test]
async fn present_view_returns_reserved_and_stored_rows() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v TBLPROPERTIES ('k'='v', 'a'='b') AS SELECT * FROM src",
    )
    .await;
    let frame = execute_show_tblproperties(&ctx, &catalogs, statement("ice", "v"))
        .await
        .expect("view must be handled")
        .expect("view must answer");
    let batches = frame.collect().await.expect("collect view properties");
    assert_eq!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("key", &DataType::Utf8, false),
            ("value", &DataType::Utf8, false)
        ]
    );
    let rows = batches
        .iter()
        .flat_map(|batch| {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("keys");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("values");
            (0..batch.num_rows())
                .map(|index| {
                    (
                        keys.value(index).to_string(),
                        values.value(index).to_string(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (
                "location".to_string(),
                std::env::temp_dir()
                    .join("sales/v")
                    .to_string_lossy()
                    .into_owned()
            ),
            ("provider".to_string(), "iceberg".to_string()),
            ("format-version".to_string(), "1".to_string()),
            ("a".to_string(), "b".to_string()),
            ("k".to_string(), "v".to_string()),
        ]
    );
}

#[tokio::test]
async fn viewless_catalog_table_falls_through_after_one_view_probe() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let view_calls = Arc::new(AtomicUsize::new(0));
    let catalog = fault_catalog(
        Arc::new(ViewlessCatalog {
            inner: catalogs["ice"].clone(),
        }),
        None,
        view_calls.clone(),
        ViewFaults::default(),
    );
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let result = execute_show_tblproperties(&ctx, &catalogs, statement("fault", "t")).await;
    assert!(result.is_none());
    assert_eq!(view_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn missing_view_with_existing_table_falls_through() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let view_calls = Arc::new(AtomicUsize::new(0));
    let table_calls = Arc::new(AtomicUsize::new(0));
    let catalog = fault_catalog(
        catalogs["ice"].clone(),
        Some(ErrorKind::ViewNotFound),
        view_calls.clone(),
        ViewFaults {
            table_exists_calls: Some(table_calls.clone()),
            ..ViewFaults::default()
        },
    );
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let result = execute_show_tblproperties(&ctx, &catalogs, statement("fault", "t")).await;
    assert!(result.is_none());
    assert_eq!(view_calls.load(Ordering::SeqCst), 1);
    assert_eq!(table_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn viewless_catalog_missing_name_is_table_or_view_not_found() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let catalog = Arc::new(ViewlessCatalog {
        inner: catalogs["ice"].clone(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute_show_tblproperties(&ctx, &catalogs, statement("fault", "absent"))
        .await
        .expect("missing view must be handled")
        .expect_err("missing name must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `fault`.`sales`.`absent` cannot be found. Verify the spelling and correctness of the schema and catalog. If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    );
}

#[tokio::test]
async fn load_view_failure_preserves_iceberg_identity_without_table_probe() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let table_calls = Arc::new(AtomicUsize::new(0));
    let catalog = fault_catalog(
        catalogs["ice"].clone(),
        Some(ErrorKind::Unexpected),
        Arc::new(AtomicUsize::new(0)),
        ViewFaults {
            table_exists_calls: Some(table_calls.clone()),
            ..ViewFaults::default()
        },
    );
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute_show_tblproperties(&ctx, &catalogs, statement("fault", "absent"))
        .await
        .expect("view load failure must be handled")
        .expect_err("view load failure must propagate");
    assert_iceberg_error(
        &error,
        ErrorKind::Unexpected,
        "Unexpected => injected load_view failure",
    );
    assert_eq!(table_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn table_exists_failure_preserves_iceberg_identity() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let table_calls = Arc::new(AtomicUsize::new(0));
    let catalog = fault_catalog(
        catalogs["ice"].clone(),
        Some(ErrorKind::ViewNotFound),
        Arc::new(AtomicUsize::new(0)),
        ViewFaults {
            table_exists_failure: Some(ErrorKind::Unexpected),
            table_exists_calls: Some(table_calls.clone()),
            ..ViewFaults::default()
        },
    );
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute_show_tblproperties(&ctx, &catalogs, statement("fault", "absent"))
        .await
        .expect("table existence failure must be handled")
        .expect_err("table existence failure must propagate");
    assert_iceberg_error(
        &error,
        ErrorKind::Unexpected,
        "Unexpected => injected table_exists failure",
    );
    assert_eq!(table_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_name_and_missing_catalog_fall_through_before_view_probe() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let invalid = ShowTblpropertiesStatement {
        name: vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ],
        key: None,
    };
    assert!(
        execute_show_tblproperties(&ctx, &catalogs, invalid)
            .await
            .is_none()
    );
    assert!(
        execute_show_tblproperties(&ctx, &catalogs, statement("not_registered", "v"))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn router_preserves_show_parse_error_and_table_fallthrough() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES")
        .await
        .expect_err("missing name must refuse");
    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF"
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.t")
        .await
        .expect_err("table must reach upstream SHOW refusal");
    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: SHOW [VARIABLE] is not supported unless information_schema is enabled"
    );
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    let rows = execute(
        &ctx,
        &catalogs,
        "SHOW TBLPROPERTIES ice.sales.v ('provider')",
    )
    .await
    .expect("router must answer a view")
    .collect()
    .await
    .expect("collect view properties");
    assert_eq!(
        rows[0]
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("key", &DataType::Utf8, false),
            ("value", &DataType::Utf8, false)
        ]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].num_rows(), 1);
    let keys = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys");
    let values = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("values");
    assert_eq!(keys.value(0), "provider");
    assert_eq!(values.value(0), "iceberg");
}
