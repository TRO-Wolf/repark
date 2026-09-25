use std::collections::HashMap;

use iceberg::spec::{FormatVersion, Struct};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::merge_append_series::{NAMESPACE, memory_catalog, series_schema, synthetic_data_file};
use crate::write::{
    SnapshotRefKind, SnapshotRefRetention, commit_append_to, create_branch_on_empty_table,
    create_snapshot_ref,
};

const BRANCH: &str = "This feature is not implemented: BRANCH on the format v1 table ns.t is not \
                      supported: the Iceberg fork writes v1 metadata without its refs, so the new \
                      ref would be lost";
const TAG: &str = "This feature is not implemented: TAG on the format v1 table ns.t is not \
                   supported: the Iceberg fork writes v1 metadata without its refs, so the new ref \
                   would be lost";

#[tokio::test]
async fn every_ref_write_on_a_v1_table_refuses_before_it_commits() {
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
    let empty = catalog.load_table(&ident).await.unwrap();
    let error = create_branch_on_empty_table(
        catalog.as_ref(),
        &ident,
        "b0",
        SnapshotRefRetention::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), BRANCH);
    let file = synthetic_data_file(1, 0, Struct::empty());
    let error = commit_append_to(&catalog, &empty, vec![file], Some("b1"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), BRANCH);

    let file = synthetic_data_file(2, 0, Struct::empty());
    let seeded = commit_append_to(&catalog, &empty, vec![file], Some("main"))
        .await
        .unwrap();
    let snapshot_id = seeded.metadata().current_snapshot_id().unwrap();
    let error = create_snapshot_ref(
        catalog.as_ref(),
        &ident,
        SnapshotRefKind::Tag,
        "t1",
        snapshot_id,
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), TAG);

    let reloaded = catalog.load_table(&ident).await.unwrap();
    assert_eq!(reloaded.metadata().snapshots().count(), 1);
    for name in ["b0", "b1", "t1"] {
        assert!(
            reloaded.metadata().snapshot_for_ref(name).is_none(),
            "{name}"
        );
    }
}
