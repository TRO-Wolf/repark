use super::super::*;
use super::accept_any_refusals::{assert_illegal_argument, refusal};
use super::common::*;

const EMPTY_SUMMARY: [(&str, &str); 11] = [
    ("operation", "append"),
    ("manifests-created", "0"),
    ("manifests-kept", "0"),
    ("manifests-replaced", "0"),
    ("changed-partition-count", "0"),
    ("total-records", "0"),
    ("total-files-size", "0"),
    ("total-data-files", "0"),
    ("total-delete-files", "0"),
    ("total-position-deletes", "0"),
    ("total-equality-deletes", "0"),
];

async fn empty_table(ctx: &SessionContext, catalogs: &CatalogRegistry, version: u8) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.be (id BIGINT, data STRING) USING iceberg \
             TBLPROPERTIES ('format-version'='{version}')"
        ),
    )
    .await;
}

fn metadata_json(table: &iceberg::table::Table) -> serde_json::Value {
    let location = table.metadata_location().unwrap();
    let path = location.strip_prefix("file://").unwrap_or(location);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn assert_empty_append(snapshot: &serde_json::Value) {
    for (key, value) in EMPTY_SUMMARY {
        assert_eq!(snapshot["summary"][key], value, "{key}");
    }
    assert!(snapshot["summary"].get("added-data-files").is_none());
    assert_eq!(snapshot.get("parent-snapshot-id"), None);
}

#[tokio::test]
async fn create_branch_on_an_empty_table_commits_an_empty_append_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 2).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.be CREATE BRANCH b1").await;
    let table = load_sales_table(&catalogs, "be").await;
    let json = metadata_json(&table);
    assert_eq!(json["current-snapshot-id"], serde_json::json!(null));
    assert!(json.get("snapshot-log").is_none());
    assert_eq!(json["last-sequence-number"], 1);
    let snapshot = &json["snapshots"][0];
    assert_eq!(json["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["sequence-number"], 1);
    assert_empty_append(snapshot);
    assert_eq!(
        json["refs"],
        serde_json::json!({"b1": {"snapshot-id": snapshot["snapshot-id"], "type": "branch"}})
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.be.branch_b1").await,
        0
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.be").await, 0);

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.be.branch_b1 VALUES (1, 'a')",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.be.branch_b1").await,
        1
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.be").await, 0);
}

#[tokio::test]
async fn branch_variants_on_an_empty_table_each_commit_their_own_snapshot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 2).await;
    for sql in [
        "ALTER TABLE ice.sales.be CREATE BRANCH b1",
        "ALTER TABLE ice.sales.be CREATE BRANCH IF NOT EXISTS b1",
        "ALTER TABLE ice.sales.be CREATE BRANCH b2 WITH SNAPSHOT RETENTION 3 SNAPSHOTS",
        "ALTER TABLE ice.sales.be CREATE BRANCH IF NOT EXISTS b3",
        "ALTER TABLE ice.sales.be CREATE OR REPLACE BRANCH b4",
        "ALTER TABLE ice.sales.be CREATE BRANCH b5 RETAIN 7 DAYS \
         WITH SNAPSHOT RETENTION 2 SNAPSHOTS 3 DAYS",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let table = load_sales_table(&catalogs, "be").await;
    let json = metadata_json(&table);
    let snapshots = json["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 5);
    for snapshot in snapshots {
        assert_empty_append(snapshot);
    }
    let ids: std::collections::HashSet<_> = ["b1", "b2", "b3", "b4", "b5"]
        .iter()
        .map(|name| json["refs"][name]["snapshot-id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids.len(), 5);
    assert_eq!(json["refs"]["b2"]["min-snapshots-to-keep"], 3);
    assert_eq!(json["refs"]["b5"]["min-snapshots-to-keep"], 2);
    assert_eq!(json["refs"]["b5"]["max-snapshot-age-ms"], 259_200_000);
    assert_eq!(json["refs"]["b5"]["max-ref-age-ms"], 604_800_000);
    assert!(json["refs"].get("main").is_none());
    assert_eq!(json["current-snapshot-id"], serde_json::json!(null));
}

#[tokio::test]
async fn create_branch_main_on_an_empty_table_sets_the_current_snapshot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 2).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.be CREATE BRANCH main",
    )
    .await;
    let table = load_sales_table(&catalogs, "be").await;
    let json = metadata_json(&table);
    let snapshot_id = json["snapshots"][0]["snapshot-id"].clone();
    assert_empty_append(&json["snapshots"][0]);
    assert_eq!(json["current-snapshot-id"], snapshot_id);
    assert_eq!(json["snapshot-log"][0]["snapshot-id"], snapshot_id);
    assert_eq!(
        json["refs"],
        serde_json::json!({"main": {"snapshot-id": snapshot_id, "type": "branch"}})
    );
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.be CREATE BRANCH IF NOT EXISTS main",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.be CREATE TAG t1").await;
    assert_eq!(
        metadata_json(&load_sales_table(&catalogs, "be").await)["refs"]["t1"]["snapshot-id"],
        snapshot_id
    );
}

#[tokio::test]
async fn tags_and_replaces_on_an_empty_table_refuse_like_spark() {
    const TAG: &str = "Cannot complete create or replace tag operation on sales.be, main has no \
                       snapshot";
    const REPLACE: &str = "Cannot complete replace branch operation on sales.be, main has no \
                           snapshot";
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 2).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.be CREATE BRANCH b1").await;
    for (sql, expected) in [
        ("ALTER TABLE ice.sales.be CREATE TAG t1", TAG),
        ("ALTER TABLE ice.sales.be CREATE TAG IF NOT EXISTS t1", TAG),
        ("ALTER TABLE ice.sales.be CREATE OR REPLACE TAG t1", TAG),
        ("ALTER TABLE ice.sales.be CREATE TAG t1 RETAIN 3 DAYS", TAG),
        ("ALTER TABLE ice.sales.be REPLACE TAG t1", TAG),
        (
            "ALTER TABLE ice.sales.be CREATE OR REPLACE BRANCH b1",
            REPLACE,
        ),
        ("ALTER TABLE ice.sales.be REPLACE BRANCH b1", REPLACE),
        ("ALTER TABLE ice.sales.be REPLACE BRANCH nope", REPLACE),
        (
            "ALTER TABLE ice.sales.be CREATE BRANCH b1",
            "Ref b1 already exists",
        ),
    ] {
        assert_illegal_argument(&ctx, &catalogs, sql, expected).await;
    }
    let mapped = refusal(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.be CREATE BRANCH bx AS OF VERSION 123",
    )
    .await;
    assert!(
        mapped.to_string().contains("123"),
        "an explicit AS OF keeps the fork path: {mapped}"
    );
    let json = metadata_json(&load_sales_table(&catalogs, "be").await);
    assert_eq!(json["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(json["refs"].as_object().unwrap().len(), 1);
}

#[tokio::test]
async fn create_branch_on_a_table_with_a_snapshot_keeps_the_current_snapshot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 2).await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.be VALUES (1, 'a')").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.be CREATE BRANCH b1").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.be CREATE TAG t1").await;
    let table = load_sales_table(&catalogs, "be").await;
    let current = table.metadata().current_snapshot_id();
    assert_eq!(table.metadata().snapshots().count(), 1);
    for name in ["b1", "t1"] {
        assert_eq!(
            table
                .metadata()
                .snapshot_for_ref(name)
                .map(|snapshot| snapshot.snapshot_id()),
            current
        );
    }
}

pub(super) fn v1_ref_refusal(kind: &str, table: &str) -> String {
    format!(
        "This feature is not implemented: {kind} on the format v1 table {table} is not \
         supported: the Iceberg fork writes v1 metadata without its refs, so the new ref would be \
         lost"
    )
}

pub(super) fn metadata_file_count(table: &iceberg::table::Table) -> usize {
    let location = table.metadata().location();
    let directory =
        std::path::Path::new(location.strip_prefix("file://").unwrap_or(location)).join("metadata");
    std::fs::read_dir(directory)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".metadata.json")
        })
        .count()
}

#[tokio::test]
async fn branch_and_tag_on_a_v1_table_refuse_until_the_fork_keeps_v1_refs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    empty_table(&ctx, &catalogs, 1).await;
    let branch = v1_ref_refusal("BRANCH", "sales.be");
    let tag = v1_ref_refusal("TAG", "sales.be");
    let empty_tag = "Cannot complete create or replace tag operation on sales.be, main has no \
                     snapshot"
        .to_string();
    for (sql, expected) in [
        ("ALTER TABLE ice.sales.be CREATE BRANCH b1", &branch),
        (
            "ALTER TABLE ice.sales.be CREATE BRANCH IF NOT EXISTS b1",
            &branch,
        ),
        (
            "ALTER TABLE ice.sales.be CREATE OR REPLACE BRANCH b1",
            &branch,
        ),
        ("ALTER TABLE ice.sales.be CREATE TAG t1", &empty_tag),
    ] {
        let error = refusal(&ctx, &catalogs, sql).await;
        assert!(
            error.to_string().ends_with(expected.as_str()),
            "{sql}: {error}"
        );
    }
    run(&ctx, &catalogs, "INSERT INTO ice.sales.be VALUES (1, 'a')").await;
    let seeded = load_sales_table(&catalogs, "be").await;
    let snapshot_id = seeded.metadata().current_snapshot_id().unwrap();
    let files = metadata_file_count(&seeded);
    let as_of_branch =
        format!("ALTER TABLE ice.sales.be CREATE BRANCH b2 AS OF VERSION {snapshot_id}");
    let as_of_tag = format!("ALTER TABLE ice.sales.be CREATE TAG t2 AS OF VERSION {snapshot_id}");
    for (sql, expected) in [
        ("ALTER TABLE ice.sales.be CREATE BRANCH b1", &branch),
        (
            "ALTER TABLE ice.sales.be CREATE BRANCH IF NOT EXISTS b1",
            &branch,
        ),
        (
            "ALTER TABLE ice.sales.be CREATE OR REPLACE BRANCH b1",
            &branch,
        ),
        ("ALTER TABLE ice.sales.be REPLACE BRANCH b1", &branch),
        (
            "ALTER TABLE ice.sales.be REPLACE BRANCH main WITH SNAPSHOT RETENTION 2 SNAPSHOTS",
            &branch,
        ),
        (as_of_branch.as_str(), &branch),
        ("ALTER TABLE ice.sales.be CREATE TAG t1", &tag),
        ("ALTER TABLE ice.sales.be CREATE TAG IF NOT EXISTS t1", &tag),
        ("ALTER TABLE ice.sales.be CREATE OR REPLACE TAG t1", &tag),
        ("ALTER TABLE ice.sales.be REPLACE TAG t1", &tag),
        (as_of_tag.as_str(), &tag),
    ] {
        let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
        assert_eq!(&error.to_string(), expected, "{sql}");
    }
    let table = load_sales_table(&catalogs, "be").await;
    assert_eq!(metadata_file_count(&table), files);
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert!(table.metadata().snapshot_for_ref("b1").is_none());

    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.be REPLACE BRANCH main",
    )
    .await;
    assert_eq!(
        load_sales_table(&catalogs, "be")
            .await
            .metadata()
            .current_snapshot_id(),
        Some(snapshot_id)
    );
}
