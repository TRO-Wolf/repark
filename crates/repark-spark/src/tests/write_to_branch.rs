use super::super::*;
use super::common::*;

const _: &str = "pins: rp-5-fork-repin/C-004";

async fn seed_diverged(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    branch: &str,
) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE {table} AS SELECT * FROM src WHERE id <= 2"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE {table} CREATE BRANCH {branch}"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO {table}.branch_{branch} VALUES (10, 'ten')"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("DELETE FROM {table}.branch_{branch} WHERE id = 2"),
    )
    .await;
}

fn snaps(table: &iceberg::table::Table, branch: &str) -> (i64, i64, Option<i64>) {
    let main = table
        .metadata()
        .current_snapshot_id()
        .expect("main snapshot");
    let branch_id = table
        .metadata()
        .snapshot_for_ref(branch)
        .expect("branch snapshot")
        .snapshot_id();
    let parent = table
        .metadata()
        .snapshot_by_id(branch_id)
        .expect("branch snapshot row")
        .parent_snapshot_id();
    (main, branch_id, parent)
}

#[tokio::test]
async fn insert_values_on_diverged_branch_leaves_main_unmoved() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b VALUES (99, 'iv')",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 10, 99]
    );
}

#[tokio::test]
async fn insert_select_on_diverged_branch_leaves_main_unmoved() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t.branch_b SELECT 99 AS id, 'is' AS name",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 10, 99]
    );
}

#[tokio::test]
async fn update_on_diverged_branch_sees_branch_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t.branch_b SET name = 'u' WHERE id = 10",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 10]
    );
}

#[tokio::test]
async fn delete_on_diverged_branch_sees_branch_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.t.branch_b WHERE id = 10",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1]
    );
}

#[tokio::test]
async fn merge_on_diverged_branch_sees_branch_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t.branch_b t USING (SELECT 10 AS id, 'm' AS name) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![1, 10]
    );
}

#[tokio::test]
async fn overwrite_on_diverged_branch_replaces_branch_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_diverged(&ctx, &catalogs, "ice.sales.t", "b").await;
    let before = load_sales_table(&catalogs, "t").await;
    let (main_before, branch_before, _) = snaps(&before, "b");
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t.branch_b VALUES (77, 'ow')",
    )
    .await;
    let after = load_sales_table(&catalogs, "t").await;
    let (main_after, branch_after, parent) = snaps(&after, "b");
    assert_eq!(main_after, main_before);
    assert_ne!(branch_after, branch_before);
    assert_eq!(parent, Some(branch_before));
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t").await,
        vec![1, 2]
    );
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.t.branch_b").await,
        vec![77]
    );
}

#[tokio::test]
async fn write_to_tag_refuses_spark_shaped() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG v1").await;
    for (sql, needle) in [
        (
            "INSERT INTO ice.sales.t.tag_v1 VALUES (99, 'iv')",
            "Cannot write to table with time travel",
        ),
        (
            "INSERT INTO ice.sales.t.tag_v1 SELECT 99 AS id, 'is' AS name",
            "Cannot write to table with time travel",
        ),
        (
            "UPDATE ice.sales.t.tag_v1 SET name = 'u' WHERE id = 1",
            "Cannot modify table with time travel",
        ),
        (
            "DELETE FROM ice.sales.t.tag_v1 WHERE id = 1",
            "Cannot modify table with time travel",
        ),
        (
            "MERGE INTO ice.sales.t.tag_v1 t USING (SELECT 1 AS id, 'm' AS name) s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name",
            "Cannot modify table with time travel",
        ),
        (
            "INSERT OVERWRITE ice.sales.t.tag_v1 VALUES (77, 'ow')",
            "Cannot write to table with time travel",
        ),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("tag write must refuse");
        assert!(
            error.to_string().contains(needle),
            "for {sql:?} expected {needle:?}, got: {error}"
        );
    }
}

#[tokio::test]
async fn write_to_missing_branch_refuses_spark_shaped() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    for sql in [
        "INSERT INTO ice.sales.t.branch_nope VALUES (99, 'iv')",
        "INSERT INTO ice.sales.t.branch_nope SELECT 99 AS id, 'is' AS name",
        "UPDATE ice.sales.t.branch_nope SET name = 'u' WHERE id = 1",
        "DELETE FROM ice.sales.t.branch_nope WHERE id = 1",
        "MERGE INTO ice.sales.t.branch_nope t USING (SELECT 1 AS id, 'm' AS name) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name",
        "INSERT OVERWRITE ice.sales.t.branch_nope VALUES (77, 'ow')",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("missing branch must refuse");
        assert!(
            error
                .to_string()
                .contains("Cannot use branch (does not exist): nope"),
            "for {sql:?} got: {error}"
        );
    }
    let table = load_sales_table(&catalogs, "t").await;
    assert!(
        table.metadata().snapshot_for_ref("nope").is_none(),
        "missing-branch write must not create the branch"
    );
}
