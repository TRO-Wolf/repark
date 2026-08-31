//! pins: dml-a-merge-not-matched-by-source/C-001, C-002, C-003, C-004, C-005, C-006, C-007
//! Spark-door `WHEN NOT MATCHED BY SOURCE` execute pins (COW + MOR).

use super::super::*;
use super::common::*;

const COW: &str = "'format-version' = '2', \
     'write.delete.mode' = 'copy-on-write', \
     'write.update.mode' = 'copy-on-write', \
     'write.merge.mode' = 'copy-on-write'";

const MOR: &str = "'format-version' = '2', \
     'write.delete.mode' = 'merge-on-read', \
     'write.update.mode' = 'merge-on-read', \
     'write.merge.mode' = 'merge-on-read'";

async fn seed_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &str,
    props: &str,
    rows: &[(i32, &str)],
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{name} (id INT, name STRING) USING iceberg \
             TBLPROPERTIES({props})"
        ),
    )
    .await;
    let values = rows
        .iter()
        .map(|(id, name)| format!("({id}, '{name}')"))
        .collect::<Vec<_>>()
        .join(", ");
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{name} VALUES {values}"),
    )
    .await;
}

async fn merge_nmbs(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, body: &str) {
    run(
        ctx,
        catalogs,
        &format!("MERGE INTO ice.sales.{table} AS t USING nmbs_src AS s ON t.id = s.id {body}"),
    )
    .await;
}

#[tokio::test]
async fn cow_nmbs_delete_only_keeps_matched_target() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(
        &ctx,
        &catalogs,
        "nmbs_del",
        COW,
        &[(1, "a"), (2, "b"), (3, "c")],
    )
    .await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    merge_nmbs(
        &ctx,
        &catalogs,
        "nmbs_del",
        "WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.nmbs_del").await,
        vec![(1, "a".to_string())]
    );
}

#[tokio::test]
async fn mor_nmbs_delete_only_writes_position_deletes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(
        &ctx,
        &catalogs,
        "nmbs_del_mor",
        MOR,
        &[(1, "a"), (2, "b"), (3, "c")],
    )
    .await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    merge_nmbs(
        &ctx,
        &catalogs,
        "nmbs_del_mor",
        "WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.nmbs_del_mor").await,
        vec![(1, "a".to_string())]
    );
    let table = load_sales_table(&catalogs, "nmbs_del_mor").await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let added = snapshot
        .summary()
        .additional_properties
        .get("added-position-deletes")
        .map(String::as_str);
    assert_eq!(
        added,
        Some("2"),
        "MOR NMBS DELETE must write two position deletes (matched MATCHED-DELETE control), got {:?}",
        snapshot.summary().additional_properties
    );
}

#[tokio::test]
async fn cow_and_mor_nmbs_update_rewrites_unmatched() {
    for (name, props) in [("nmbs_upd_cow", COW), ("nmbs_upd_mor", MOR)] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        seed_table(&ctx, &catalogs, name, props, &[(1, "a"), (2, "b")]).await;
        register_source(&ctx, "nmbs_src", &[(1, "aa")]);
        merge_nmbs(
            &ctx,
            &catalogs,
            name,
            "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET name = 'gone'",
        )
        .await;
        assert_eq!(
            table_rows(&ctx, &catalogs, &format!("ice.sales.{name}")).await,
            vec![(1, "a".to_string()), (2, "gone".to_string())],
            "{name}"
        );
    }
}

#[tokio::test]
async fn three_arms_cow_and_mor_match_spark() {
    for (name, props) in [("nmbs_3_cow", COW), ("nmbs_3_mor", MOR)] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        seed_table(&ctx, &catalogs, name, props, &[(1, "a"), (2, "b")]).await;
        register_source(&ctx, "nmbs_src", &[(1, "aa"), (4, "dd")]);
        merge_nmbs(
            &ctx,
            &catalogs,
            name,
            "WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )
        .await;
        assert_eq!(
            table_rows(&ctx, &catalogs, &format!("ice.sales.{name}")).await,
            vec![(1, "aa".to_string()), (4, "dd".to_string())],
            "{name}"
        );
    }
}

#[tokio::test]
async fn source_empty_nmbs_delete_wipes_cow_and_mor() {
    for (name, props) in [("nmbs_wipe_cow", COW), ("nmbs_wipe_mor", MOR)] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = setup(&wh).await;
        seed_table(
            &ctx,
            &catalogs,
            name,
            props,
            &[(1, "a"), (2, "b"), (3, "c")],
        )
        .await;
        register_source(&ctx, "nmbs_src", &[]);
        merge_nmbs(
            &ctx,
            &catalogs,
            name,
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )
        .await;
        assert_eq!(
            table_rows(&ctx, &catalogs, &format!("ice.sales.{name}")).await,
            Vec::<(i32, String)>::new(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn nmbs_first_match_update_then_delete() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(
        &ctx,
        &catalogs,
        "nmbs_fm",
        COW,
        &[(1, "a"), (2, "b"), (3, "c")],
    )
    .await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    merge_nmbs(
        &ctx,
        &catalogs,
        "nmbs_fm",
        "WHEN NOT MATCHED BY SOURCE AND t.name = 'b' THEN UPDATE SET name = 'x' \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.nmbs_fm").await,
        vec![(1, "a".to_string()), (2, "x".to_string())]
    );
}

#[tokio::test]
async fn nmbs_unconditional_not_last_raises_spark_class() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "nmbs_nl", COW, &[(1, "a")]).await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.nmbs_nl AS t USING nmbs_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN DELETE \
         WHEN NOT MATCHED BY SOURCE AND t.name = 'b' THEN UPDATE SET name = 'x'",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION"),
        "{err}"
    );
}

#[tokio::test]
async fn nmbs_plus_matched_update_dup_source_raises_cardinality() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "nmbs_card", COW, &[(1, "a"), (2, "b")]).await;
    register_source(&ctx, "nmbs_src", &[(1, "x"), (1, "y")]);
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.nmbs_card AS t USING nmbs_src AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("MERGE_CARDINALITY_VIOLATION"), "{err}");
}

#[tokio::test]
async fn nmbs_update_store_assignment_reuses_merge_gate() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_table(&ctx, &catalogs, "nmbs_sa", COW, &[(1, "a"), (2, "b")]).await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.nmbs_sa AS t USING nmbs_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET id = 'not-an-id'",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("INCOMPATIBLE_DATA_FOR_TABLE") || err.contains("not ANSI-store-assignable"),
        "{err}"
    );
}

#[tokio::test]
async fn adopted_v3_nmbs_merge_stays_refused() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nmbs_v3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES('format-version' = '3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.nmbs_v3 VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    register_source(&ctx, "nmbs_src", &[(1, "aa")]);
    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.nmbs_v3 AS t USING nmbs_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("V3-COW-1") || err.contains("row lineage"),
        "{err}"
    );
}
