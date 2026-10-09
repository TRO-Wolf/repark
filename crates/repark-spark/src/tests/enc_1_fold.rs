use std::collections::BTreeSet;

use super::super::*;
use super::common::*;
use super::wap_id::set_wap;

const KEY: &str = "SEKRETKEYVAL9f3a7";
const VERSIONS: [&str; 2] = ["2", "3"];
const MERGE_ON_READ: &str =
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read'";
const WAP_ENABLED: &str = ", 'write.wap.enabled' = 'true'";

#[derive(Debug, PartialEq, Eq)]
struct Frozen {
    snapshots: Vec<i64>,
    refs: String,
    pointer: Option<String>,
    listing: BTreeSet<String>,
}

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    properties: &'static str,
    partitioned: bool,
    before_key: &'static [&'static str],
    wap_branch: Option<&'static str>,
    wap_id: Option<&'static str>,
    statement: &'static str,
}

const PLAIN: Shape = Shape {
    name: "",
    properties: "",
    partitioned: false,
    before_key: &[],
    wap_branch: None,
    wap_id: None,
    statement: "",
};

const BRANCH: &[&str] = &["ALTER TABLE ice.sales.t CREATE BRANCH b1"];
const MERGE: &str = "MERGE INTO ice.sales.t t USING (SELECT 1 AS id, 'm' AS name, 'x' AS cat) s \
     ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT *";

fn listing(root: &std::path::Path) -> BTreeSet<String> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = BTreeSet::new();
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.display().to_string());
            }
        }
    }
    found
}

async fn freeze(
    ctx: &SessionContext,
    wh: &TempDir,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Frozen {
    let loaded = load_sales_table(catalogs, table).await;
    let metadata = loaded.metadata();
    let mut snapshots: Vec<i64> = metadata
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    snapshots.sort_unstable();
    let refs = outcome(
        ctx,
        catalogs,
        &format!("SELECT name, type, snapshot_id FROM ice.sales.{table}.refs ORDER BY name"),
    )
    .await
    .map(|batches| {
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .map(|table| table.to_string())
            .unwrap_or_default()
    })
    .unwrap_or_else(|error| error.to_string());
    Frozen {
        snapshots,
        refs,
        pointer: loaded.metadata_location().map(str::to_string),
        listing: listing(wh.path()),
    }
}

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    execute(ctx, catalogs, sql).await?.collect().await
}

fn refusal_faults(error: datafusion::error::DataFusionError, table: &str) -> Vec<String> {
    let mapped = repark_core::engine_err(error);
    let message = mapped.to_string();
    let mut faults = Vec::new();
    if !matches!(&mapped, repark_core::Error::NotImplemented(_)) {
        faults.push(format!("class is not Unsupported: {mapped:?}"));
    }
    let expected = format!(
        "Table {table} carries property 'encryption.key-id': RePark has no table encryption \
         and refuses to write plaintext into a table that asks for it (ENC-1)."
    );
    if message != expected {
        faults.push(format!("text differs: {message}"));
    }
    if message.contains(KEY) {
        faults.push(format!("text echoes the key value: {message}"));
    }
    faults
}

async fn add_key(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} SET TBLPROPERTIES ('encryption.key-id' = '{KEY}')"),
    )
    .await;
}

async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry, version: &str, shape: &Shape) {
    let partition = if shape.partitioned {
        " PARTITIONED BY (cat)"
    } else {
        ""
    };
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id INT, name STRING, cat STRING) USING iceberg{partition} \
             TBLPROPERTIES ('format-version' = '{version}'{})",
            shape.properties
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y')",
    )
    .await;
    for statement in shape.before_key {
        run(ctx, catalogs, statement).await;
    }
    add_key(ctx, catalogs, "t").await;
}

async fn shape_faults(version: &str, shape: &Shape) -> Vec<String> {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    seed(&ctx, &catalogs, version, shape).await;
    let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
    set_wap(&ctx, shape.wap_branch, shape.wap_id);
    let result = outcome(&ctx, &catalogs, shape.statement).await;
    set_wap(&ctx, None, None);
    let mut faults = match result {
        Ok(_) => vec!["the write ran".to_string()],
        Err(error) => refusal_faults(error, "sales.t"),
    };
    let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
    if after.snapshots != before.snapshots {
        faults.push("snapshots moved".to_string());
    }
    if after.refs != before.refs {
        faults.push(format!("refs moved:\n{}", after.refs));
    }
    if after.pointer != before.pointer {
        faults.push("metadata pointer moved".to_string());
    }
    let added: Vec<&String> = after.listing.difference(&before.listing).collect();
    if !added.is_empty() {
        faults.push(format!("files added: {added:?}"));
    }
    let removed = before.listing.difference(&after.listing).count();
    if removed != 0 {
        faults.push(format!("{removed} files removed"));
    }
    faults
        .into_iter()
        .map(|fault| format!("v{version} {}: {fault}", shape.name))
        .collect()
}

async fn assert_shapes_refuse(shapes: &[Shape]) {
    let mut faults = Vec::new();
    for version in VERSIONS {
        for shape in shapes {
            faults.extend(shape_faults(version, shape).await);
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_branch_target_writes_refuse() {
    assert_shapes_refuse(&[
        Shape {
            name: "branch_insert",
            before_key: BRANCH,
            statement: "INSERT INTO ice.sales.t.branch_b1 VALUES (4, 'd', 'x')",
            ..PLAIN
        },
        Shape {
            name: "branch_insert_select",
            before_key: BRANCH,
            statement: "INSERT INTO ice.sales.t.branch_b1 SELECT 4 AS id, 'd' AS name, 'x' AS cat",
            ..PLAIN
        },
        Shape {
            name: "branch_insert_backtick",
            before_key: BRANCH,
            statement: "INSERT INTO ice.sales.t.`branch_b1` VALUES (4, 'd', 'x')",
            ..PLAIN
        },
        Shape {
            name: "branch_update",
            before_key: BRANCH,
            statement: "UPDATE ice.sales.t.branch_b1 SET name = 'q' WHERE id = 1",
            ..PLAIN
        },
        Shape {
            name: "branch_update_mor",
            properties: MERGE_ON_READ,
            before_key: BRANCH,
            statement: "UPDATE ice.sales.t.branch_b1 SET name = 'q' WHERE id = 1",
            ..PLAIN
        },
        Shape {
            name: "branch_delete",
            before_key: BRANCH,
            statement: "DELETE FROM ice.sales.t.branch_b1 WHERE id = 1",
            ..PLAIN
        },
        Shape {
            name: "branch_delete_mor",
            properties: MERGE_ON_READ,
            before_key: BRANCH,
            statement: "DELETE FROM ice.sales.t.branch_b1 WHERE id = 1",
            ..PLAIN
        },
        Shape {
            name: "branch_delete_meta",
            partitioned: true,
            before_key: BRANCH,
            statement: "DELETE FROM ice.sales.t.branch_b1 WHERE cat = 'x'",
            ..PLAIN
        },
        Shape {
            name: "branch_delete_all",
            before_key: BRANCH,
            statement: "DELETE FROM ice.sales.t.branch_b1",
            ..PLAIN
        },
    ])
    .await;
}

#[tokio::test]
async fn keyed_table_wap_branch_writes_refuse() {
    assert_shapes_refuse(&[
        Shape {
            name: "wap_branch_insert",
            properties: WAP_ENABLED,
            wap_branch: Some("wb"),
            statement: "INSERT INTO ice.sales.t VALUES (4, 'd', 'x')",
            ..PLAIN
        },
        Shape {
            name: "wap_branch_update",
            properties: WAP_ENABLED,
            wap_branch: Some("wb"),
            statement: "UPDATE ice.sales.t SET name = 'q' WHERE id = 1",
            ..PLAIN
        },
        Shape {
            name: "wap_branch_delete",
            properties: WAP_ENABLED,
            wap_branch: Some("wb"),
            statement: "DELETE FROM ice.sales.t WHERE id = 1",
            ..PLAIN
        },
    ])
    .await;
}

#[tokio::test]
async fn keyed_table_wap_branch_refusals_leave_no_metadata_or_ref() {
    assert_shapes_refuse(&[
        Shape {
            name: "wap_branch_merge",
            properties: WAP_ENABLED,
            wap_branch: Some("wb"),
            statement: MERGE,
            ..PLAIN
        },
        Shape {
            name: "wap_branch_overwrite",
            properties: WAP_ENABLED,
            wap_branch: Some("wb"),
            statement: "INSERT OVERWRITE ice.sales.t VALUES (4, 'd', 'x')",
            ..PLAIN
        },
    ])
    .await;
}

#[tokio::test]
async fn keyed_table_wap_id_writes_refuse() {
    assert_shapes_refuse(&[
        Shape {
            name: "wap_id_insert",
            properties: WAP_ENABLED,
            wap_id: Some("w1"),
            statement: "INSERT INTO ice.sales.t VALUES (4, 'd', 'x')",
            ..PLAIN
        },
        Shape {
            name: "wap_id_insert_select",
            properties: WAP_ENABLED,
            wap_id: Some("w1"),
            statement: "INSERT INTO ice.sales.t SELECT 4 AS id, 'd' AS name, 'x' AS cat",
            ..PLAIN
        },
    ])
    .await;
}

#[tokio::test]
async fn keyed_table_stats_and_path_procedures_refuse() {
    assert_shapes_refuse(&[
        Shape {
            name: "compute_table_stats",
            statement: "CALL ice.system.compute_table_stats('sales.t')",
            ..PLAIN
        },
        Shape {
            name: "compute_table_stats_columns",
            statement: "CALL ice.system.compute_table_stats(table => 'sales.t', \
                        columns => array('id'))",
            ..PLAIN
        },
        Shape {
            name: "compute_partition_stats",
            partitioned: true,
            statement: "CALL ice.system.compute_partition_stats('sales.t')",
            ..PLAIN
        },
    ])
    .await;
}

#[tokio::test]
async fn keyed_table_rewrite_table_path_refuses() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        seed(&ctx, &catalogs, version, &PLAIN).await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let source = warehouse.path().display();
        let result = outcome(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_table_path(table => 'sales.t', \
                 source_prefix => '{source}', target_prefix => '/tmp/nowhere-enc1')"
            ),
        )
        .await;
        match result {
            Ok(_) => faults.push(format!("v{version}: the procedure ran")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_empty_table_branch_takes_no_first_write() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg TBLPROPERTIES \
                 ('format-version' = '{version}', 'encryption.key-id' = '{KEY}')"
            ),
        )
        .await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        match outcome(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH eb").await {
            Ok(_) => faults.push(format!("v{version}: CREATE BRANCH on the empty table ran")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if outcome(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t.branch_eb VALUES (1, 'a')",
        )
        .await
        .is_ok()
        {
            faults.push(format!("v{version}: the branch INSERT ran"));
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_run_maintenance_apply_rewrites_nothing() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.t (id INT, name STRING, cat STRING) USING iceberg \
                 TBLPROPERTIES ('format-version' = '{version}')"
            ),
        )
        .await;
        for index in 0..12 {
            run(
                &ctx,
                &catalogs,
                &format!("INSERT INTO ice.sales.t VALUES ({index}, 'a', 'x')"),
            )
            .await;
        }
        add_key(&ctx, &catalogs, "t").await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let result = outcome(
            &ctx,
            &catalogs,
            "CALL ice.system.run_maintenance(table => 'sales.t', dry_run => false, \
             target_file_size_bytes => 134217728, rewrite_manifests => true, \
             snapshot_retain_last => 1)",
        )
        .await;
        match result {
            Ok(_) => faults.push(format!("v{version}: the apply run returned rows")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_non_fast_forward_cherrypick_refuses() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg TBLPROPERTIES \
                 ('format-version' = '{version}', 'write.wap.enabled' = 'true')"
            ),
        )
        .await;
        run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1, 'a')").await;
        set_wap(&ctx, None, Some("w9"));
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t VALUES (40, 'staged')",
        )
        .await;
        set_wap(&ctx, None, None);
        let staged = super::wap_id::staged_snapshot_id(&catalogs, "w9").await;
        run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (2, 'b')").await;
        add_key(&ctx, &catalogs, "t").await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let result = outcome(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.cherrypick_snapshot('sales.t', {staged})"),
        )
        .await;
        match result {
            Ok(_) => faults.push(format!("v{version}: the cherry-pick ran")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_stale_handle_commit_refuses() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg TBLPROPERTIES \
                 ('format-version' = '{version}')"
            ),
        )
        .await;
        run(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1, 'a')").await;
        let stale = load_sales_table(&catalogs, "t").await;
        add_key(&ctx, &catalogs, "t").await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let catalog = catalogs.get("ice").expect("ice");
        let result = repark_iceberg::write::commit_append_with_summary(
            catalog,
            &stale,
            Vec::new(),
            &[],
            None,
        )
        .await;
        match result {
            Ok(_) => faults.push(format!("v{version}: the stale-handle commit ran")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_public_append_entry_refuses() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        seed(&ctx, &catalogs, version, &PLAIN).await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let frame = ctx
            .sql("SELECT 9 AS id, 'z' AS name, 'x' AS cat")
            .await
            .unwrap();
        let batches = frame.collect().await.unwrap();
        let catalog = catalogs.get("ice").expect("ice");
        let ident = TableIdent::from_strs(["sales", "t"]).unwrap();
        match repark_iceberg::write::append(catalog, &ident, batches).await {
            Ok(_) => faults.push(format!("v{version}: the public append ran")),
            Err(error) => faults.extend(refusal_faults(error, "sales.t")),
        }
        if freeze(&ctx, &warehouse, &catalogs, "t").await != before {
            faults.push(format!("v{version}: the table or its listing moved"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_ref_expiry_and_property_commits_still_run() {
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        seed(&ctx, &catalogs, version, &PLAIN).await;
        run(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.t CREATE BRANCH kept",
        )
        .await;
        run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE TAG marked").await;
        run(&ctx, &catalogs, "ALTER TABLE ice.sales.t DROP BRANCH kept").await;
        run(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.t SET TBLPROPERTIES ('comment' = 'still alterable')",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "CALL ice.system.expire_snapshots(table => 'sales.t', retain_last => 1)",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.t UNSET TBLPROPERTIES ('encryption.key-id')",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t VALUES (3, 'c', 'x')",
        )
        .await;
        assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
    }
}

#[tokio::test]
async fn unkeyed_twins_of_the_fold_shapes_still_write() {
    for version in VERSIONS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.t (id INT, name STRING, cat STRING) USING iceberg \
                 TBLPROPERTIES ('format-version' = '{version}', 'write.wap.enabled' = 'true', \
                 'encryption.keyid' = '{KEY}', 'encryption.key-id-x' = '{KEY}')"
            ),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y')",
        )
        .await;
        run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t.branch_b1 VALUES (4, 'd', 'x')",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "UPDATE ice.sales.t.branch_b1 SET name = 'q' WHERE id = 1",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "DELETE FROM ice.sales.t.branch_b1 WHERE id = 2",
        )
        .await;
        set_wap(&ctx, Some("wb"), None);
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t VALUES (5, 'e', 'x')",
        )
        .await;
        set_wap(&ctx, None, Some("w1"));
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.t VALUES (6, 'f', 'x')",
        )
        .await;
        set_wap(&ctx, None, None);
        run(
            &ctx,
            &catalogs,
            "CALL ice.system.compute_table_stats('sales.t')",
        )
        .await;
        assert_eq!(
            rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t.branch_b1").await,
            2
        );
        assert_eq!(
            rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t.branch_wb").await,
            3
        );
        assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    }
}
