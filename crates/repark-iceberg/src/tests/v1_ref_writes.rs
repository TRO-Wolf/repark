use std::collections::HashMap;

use iceberg::spec::{FormatVersion, Struct};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::merge_append_series::{NAMESPACE, memory_catalog, series_schema, synthetic_data_file};
use crate::write::{
    SnapshotRefKind, SnapshotRefRetention, commit_append_to, create_branch_on_empty_table,
    create_snapshot_ref,
};

fn ref_head(table: &iceberg::table::Table, name: &str) -> Option<i64> {
    table
        .metadata()
        .snapshot_for_ref(name)
        .map(|snapshot| snapshot.snapshot_id())
}

#[tokio::test]
async fn every_ref_write_on_a_v1_table_commits_and_survives_a_reload() {
    let warehouse = TempDir::new().unwrap();
    let catalog = memory_catalog(&warehouse).await;
    let ident = TableIdent::new(NamespaceIdent::new(NAMESPACE.to_string()), "t".to_string());
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name("t".to_string())
                .schema(series_schema())
                .format_version(FormatVersion::V1)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .unwrap();
    create_branch_on_empty_table(
        catalog.as_ref(),
        &ident,
        "b0",
        SnapshotRefRetention::default(),
    )
    .await
    .unwrap();
    let branched_empty = catalog.load_table(&ident).await.unwrap();
    assert!(ref_head(&branched_empty, "b0").is_some());
    assert!(ref_head(&branched_empty, "main").is_none());

    let file = synthetic_data_file(1, 0, Struct::empty());
    let seeded = commit_append_to(&catalog, &branched_empty, vec![file], Some("main"))
        .await
        .unwrap();
    let main_head = seeded.metadata().current_snapshot_id().unwrap();
    create_snapshot_ref(
        catalog.as_ref(),
        &ident,
        SnapshotRefKind::Branch,
        "b1",
        main_head,
    )
    .await
    .unwrap();
    create_snapshot_ref(
        catalog.as_ref(),
        &ident,
        SnapshotRefKind::Tag,
        "t1",
        main_head,
    )
    .await
    .unwrap();

    let fresh = catalog.load_table(&ident).await.unwrap();
    let file = synthetic_data_file(2, 0, Struct::empty());
    let branched = commit_append_to(&catalog, &fresh, vec![file], Some("b1"))
        .await
        .unwrap();
    let branch_head = ref_head(&branched, "b1").unwrap();
    assert_ne!(branch_head, main_head);

    let reloaded = catalog.load_table(&ident).await.unwrap();
    assert_eq!(reloaded.metadata().format_version(), FormatVersion::V1);
    assert_eq!(ref_head(&reloaded, "main"), Some(main_head));
    assert_eq!(ref_head(&reloaded, "b1"), Some(branch_head));
    assert_eq!(ref_head(&reloaded, "t1"), Some(main_head));
    assert!(ref_head(&reloaded, "b0").is_some());
}
