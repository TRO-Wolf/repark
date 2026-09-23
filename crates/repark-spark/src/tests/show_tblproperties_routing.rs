use super::super::*;
use super::common::*;
use super::describe_view_routing::{FaultCatalog, ViewFaults, ViewlessCatalog, register_catalog};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::view_ddl::execute::execute_show_tblproperties;
use crate::view_ddl::parse::ShowTblpropertiesStatement;

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
    let catalog = Arc::new(FaultCatalog {
        inner: Arc::new(ViewlessCatalog {
            inner: catalogs["ice"].clone(),
        }),
        table_failure: None,
        view_failure: None,
        view_calls: view_calls.clone(),
        faults: ViewFaults::default(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let result = execute_show_tblproperties(
        &ctx,
        &catalogs,
        ShowTblpropertiesStatement {
            name: vec!["fault".to_string(), "sales".to_string(), "t".to_string()],
            key: None,
        },
    )
    .await;
    assert!(result.is_none());
    assert_eq!(view_calls.load(Ordering::SeqCst), 1);
}
