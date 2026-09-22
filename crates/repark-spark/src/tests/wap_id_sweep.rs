use std::collections::HashSet;

use super::common::*;
use super::session_write_conf::{set_session_conf, unset_session_conf};
use super::wap_id::{
    WAP_DDL, ids, main_snapshot_id, ref_heads, seed, set_wap, snapshot_count, staged_snapshot_id,
};

#[tokio::test]
async fn wap_id_with_a_session_conf_stages_a_first_write_without_creating_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert!(
        ref_heads(&ctx, &catalogs).await.is_empty(),
        "no ref exists, main was never created"
    );
    assert!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t")
            .await
            .is_empty(),
        "with no main head the read answers empty"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        1,
        "the log holds only the staged snapshot"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    let table = load_sales_table(&catalogs, "t").await;
    let staged_snapshot = table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log");
    assert_eq!(
        staged_snapshot.parent_snapshot_id(),
        None,
        "the staged first write has no parent"
    );
    let props = &staged_snapshot.summary().additional_properties;
    assert_eq!(
        props.get("wap.id").map(String::as_str),
        Some("w1"),
        "the staged snapshot carries the wap id"
    );
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the staged snapshot carries the session stamp"
    );
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged first write added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("1"),
        "the staged first write totals one record"
    );
}

#[tokio::test]
async fn wap_id_with_session_and_statement_options_stages_partitioned_writes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.p (id INT, name STRING) USING iceberg PARTITIONED BY (id) \
         TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.p SELECT 1 AS id, 'a' AS name",
    )
    .await;
    let seed_id = load_sales_table(&catalogs, "p")
        .await
        .metadata()
        .snapshot_for_ref("main")
        .expect("main ref exists")
        .snapshot_id();

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.p SELECT 2 AS id, 'b' AS name",
    )
    .await;
    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w2"));
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.p SELECT 3 AS id, 'c' AS name",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    let mut found = time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.p").await;
    found.sort_unstable();
    assert_eq!(found, vec![1], "the staged rows are invisible on main");
    let table = load_sales_table(&catalogs, "p").await;
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("main")
            .expect("main ref exists")
            .snapshot_id(),
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        table.metadata().snapshots().count(),
        3,
        "the log holds the seed and the two staged snapshots"
    );
    for wap_id in ["w1", "w2"] {
        let mut hits = 0;
        for snapshot in table.metadata().snapshots() {
            if snapshot
                .summary()
                .additional_properties
                .get("wap.id")
                .is_some_and(|value| value == wap_id)
            {
                hits += 1;
                let staged_props = &snapshot.summary().additional_properties;
                assert_eq!(
                    staged_props.get("added-records").map(String::as_str),
                    Some("1"),
                    "the staged snapshot added one record"
                );
                assert_eq!(
                    staged_props.get("total-records").map(String::as_str),
                    Some("2"),
                    "the staged snapshot totals two records"
                );
            }
        }
        assert_eq!(
            hits, 1,
            "exactly one staged snapshot carries wap.id {wap_id}"
        );
    }
}

#[tokio::test]
async fn wap_id_with_statement_options_and_no_session_conf_stages() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the staged row is invisible on main"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the log holds the seed and the staged snapshot"
    );
    let staged = staged_snapshot_id(&catalogs, "w1").await;
    assert_ne!(staged, seed_id, "the staged snapshot is a new snapshot");
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(staged)
        .expect("staged snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("wap.id").map(String::as_str),
        Some("w1"),
        "the staged snapshot carries the wap id"
    );
    assert_eq!(
        props.get("team").map(String::as_str),
        Some("a"),
        "the staged snapshot carries the statement stamp"
    );
    assert_eq!(
        props.get("added-records").map(String::as_str),
        Some("1"),
        "the staged snapshot added one record"
    );
    assert_eq!(
        props.get("total-records").map(String::as_str),
        Some("2"),
        "the staged snapshot totals two records"
    );
}

#[tokio::test]
async fn wap_id_leaves_an_explicit_branch_write_on_the_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH feat",
    )
    .await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_feat SELECT 2 AS id, 'b' AS name",
    )
    .await;
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "main still reads only the seed row"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_feat").await,
        vec![1, 2],
        "the branch carries the seed and the new row"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the log holds the seed and the branch commit"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_conf_leaves_an_explicit_branch_write_on_the_branch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE BRANCH feat",
    )
    .await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_feat SELECT 2 AS id, 'b' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "main still reads only the seed row"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_feat").await,
        vec![1, 2],
        "the branch carries the seed and the new row"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the log holds the seed and the branch commit"
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshots().all(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key("wap.id")
        }),
        "no snapshot carries a staged wap id"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_conf_leaves_insert_overwrite_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![9],
        "the overwrite replaces main instead of staging"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the overwrite committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_ne!(head, seed_id, "main moved to the overwrite");
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(head)
        .expect("head snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the main snapshot carries the session stamp"
    );
    assert!(
        !props.contains_key("wap.id"),
        "the overwrite stamps no wap id"
    );
}

#[tokio::test]
async fn wap_id_leaves_insert_by_name_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT 'b' AS name, 2 AS id",
    )
    .await;
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the by-name append commits to main, not to a stage"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the by-name append committed one snapshot on main"
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshots().all(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key("wap.id")
        }),
        "no snapshot carries a staged wap id"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_conf_leaves_insert_by_name_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT 'b' AS name, 2 AS id",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "the by-name append commits to main, not to a stage"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the by-name append committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(head)
        .expect("head snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the main snapshot carries the session stamp"
    );
    assert!(
        !props.contains_key("wap.id"),
        "the by-name append stamps no wap id"
    );
}

#[tokio::test]
async fn wap_id_with_a_session_conf_leaves_delete_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;

    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 1").await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![2],
        "the delete commits to main, not to a stage"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        3,
        "seed, insert and delete each committed"
    );
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshots().all(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key("wap.id")
        }),
        "no snapshot carries a staged wap id"
    );
}

#[tokio::test]
async fn wap_id_leaves_merge_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING (SELECT 1 AS id, 'z' AS name) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the merge commits to main, not to a stage"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the merge committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_ne!(head, seed_id, "main moved to the merge commit");
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshots().all(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key("wap.id")
        }),
        "no snapshot carries a staged wap id"
    );
}

#[tokio::test]
async fn wap_id_leaves_truncate_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_wap(&ctx, None, Some("w1"));
    run(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.t").await;
    set_wap(&ctx, None, None);

    assert!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t")
            .await
            .is_empty(),
        "the truncate emptied main instead of staging"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the truncate committed one snapshot on main"
    );
    assert_ne!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main moved to the truncate commit"
    );
}

#[tokio::test]
async fn wap_id_leaves_ctas_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    set_wap(&ctx, None, Some("w1"));
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.c1 AS SELECT 1 AS id",
    )
    .await;
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.c2 AS SELECT 2 AS id",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.c1").await,
        vec![1],
        "the plain CTAS committed its rows"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.c2").await,
        vec![2],
        "the session-conf CTAS committed its rows"
    );
    for name in ["c1", "c2"] {
        let table = load_sales_table(&catalogs, name).await;
        assert_eq!(
            table.metadata().snapshots().count(),
            1,
            "the CTAS committed exactly one snapshot"
        );
        assert!(
            table.metadata().snapshots().all(|snapshot| {
                !snapshot
                    .summary()
                    .additional_properties
                    .contains_key("wap.id")
            }),
            "no snapshot carries a staged wap id"
        );
    }
}

#[tokio::test]
async fn wap_id_with_statement_options_leaves_insert_overwrite_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 9 AS id, 'z' AS name",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![9],
        "the overwrite replaces main instead of staging"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the overwrite committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_ne!(head, seed_id, "main moved to the overwrite");
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(head)
        .expect("head snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("team").map(String::as_str),
        Some("a"),
        "the main snapshot carries the statement stamp"
    );
    assert!(
        !props.contains_key("wap.id"),
        "the overwrite stamps no wap id"
    );
}

#[tokio::test]
async fn wap_id_with_statement_and_session_options_leaves_insert_overwrite_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 9 AS id, 'z' AS name",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![9],
        "the overwrite replaces main instead of staging"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the overwrite committed one snapshot on main"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_ne!(head, seed_id, "main moved to the overwrite");
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(head)
        .expect("head snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("team").map(String::as_str),
        Some("a"),
        "the main snapshot carries the statement stamp"
    );
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the main snapshot carries the session stamp"
    );
    assert!(
        !props.contains_key("wap.id"),
        "the overwrite stamps no wap id"
    );
}

#[tokio::test]
async fn wap_id_with_statement_options_leaves_ctas_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.c1 AS SELECT 1 AS id",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.c2 AS SELECT 2 AS id",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.c1").await,
        vec![1],
        "the options CTAS committed its rows"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.c2").await,
        vec![2],
        "the options and session-conf CTAS committed its rows"
    );
    for name in ["c1", "c2"] {
        let table = load_sales_table(&catalogs, name).await;
        assert_eq!(
            table.metadata().snapshots().count(),
            1,
            "the CTAS committed exactly one snapshot"
        );
        let props = &table
            .metadata()
            .snapshots()
            .next()
            .expect("one snapshot in the log")
            .summary()
            .additional_properties;
        assert_eq!(
            props.get("team").map(String::as_str),
            Some("a"),
            "the CTAS snapshot carries the statement stamp"
        );
        assert!(
            !props.contains_key("wap.id"),
            "no snapshot carries a staged wap id"
        );
    }
    let table = load_sales_table(&catalogs, "c2").await;
    let props = &table
        .metadata()
        .snapshots()
        .next()
        .expect("one snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the session-conf CTAS snapshot carries the session stamp"
    );
}

#[tokio::test]
async fn wap_id_with_statement_options_refuses_insert_by_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    let error = crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT 'b' AS name, 2 AS id",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .expect_err("a by-name append cannot honour statement options");
    set_wap(&ctx, None, None);

    assert!(
        error.to_string().contains("does not support write options"),
        "the refusal names the unsupported options: {error}"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the refused write changed nothing"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        1,
        "the refused write left only the seed snapshot"
    );
}

#[tokio::test]
async fn wap_id_with_statement_and_session_options_refuses_insert_by_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "snapshot-property.team".to_string(),
        "a".to_string(),
    )])
    .unwrap();
    set_wap(&ctx, None, Some("w1"));
    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    let error = crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT 'b' AS name, 2 AS id",
        &HashSet::<String>::new(),
        &options,
    )
    .await
    .expect_err("a by-name append cannot honour statement options");
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");
    set_wap(&ctx, None, None);

    assert!(
        error.to_string().contains("does not support write options"),
        "the refusal names the unsupported options: {error}"
    );
    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1],
        "the refused write changed nothing"
    );
    assert_eq!(
        main_snapshot_id(&catalogs).await,
        seed_id,
        "main still points at the seed snapshot"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        1,
        "the refused write left only the seed snapshot"
    );
}

#[tokio::test]
async fn session_snapshot_property_without_a_wap_id_commits_on_main() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed(&ctx, &catalogs, WAP_DDL).await;
    let seed_id = main_snapshot_id(&catalogs).await;

    set_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k", "v");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 2 AS id, 'b' AS name",
    )
    .await;
    unset_session_conf(&ctx, "spark.sql.iceberg.snapshot-property.k");

    assert_eq!(
        ids(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2],
        "without the id the conf changes nothing about the commit"
    );
    assert_eq!(
        snapshot_count(&catalogs).await,
        2,
        "the normal commit appended one snapshot"
    );
    let head = main_snapshot_id(&catalogs).await;
    assert_ne!(head, seed_id, "main moved to the new snapshot");
    assert_eq!(
        ref_heads(&ctx, &catalogs).await,
        vec![("main".to_string(), "BRANCH".to_string(), head)],
        "main alone exists and points at the new head"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let props = &table
        .metadata()
        .snapshot_by_id(head)
        .expect("head snapshot in the log")
        .summary()
        .additional_properties;
    assert_eq!(
        props.get("k").map(String::as_str),
        Some("v"),
        "the main snapshot carries the session stamp"
    );
    assert!(
        !props.contains_key("wap.id"),
        "no wap id stamp without the conf"
    );
}
