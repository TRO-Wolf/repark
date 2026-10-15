use super::super::*;
use super::common::*;
use super::enc_1_fold::{Frozen, VERSIONS, add_key, freeze, refusal_faults};

async fn sweep_fixture(
    version: &str,
) -> (
    SessionContext,
    CatalogRegistry,
    TempDir,
    iceberg::table::Table,
) {
    let warehouse = TempDir::new().expect("temp warehouse");
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
    (ctx, catalogs, warehouse, stale)
}

async fn stage_stale_batch(
    ctx: &SessionContext,
    stale: &iceberg::table::Table,
) -> Vec<iceberg::spec::DataFile> {
    let batches = ctx
        .sql("SELECT 9 AS id, 'z' AS name")
        .await
        .expect("staging source frames")
        .collect()
        .await
        .expect("staging source collects");
    let staged = repark_iceberg::write::write_data_files(stale, batches)
        .await
        .expect("staging writes");
    assert!(!staged.is_empty(), "staging wrote no file");
    staged
}

fn check_swept(
    faults: &mut Vec<String>,
    version: &str,
    door: &str,
    result: datafusion::error::Result<iceberg::table::Table>,
    before: &Frozen,
    after: &Frozen,
) {
    match result {
        Ok(_) => faults.push(format!("v{version}: the keyed {door} ran")),
        Err(error) => faults.extend(refusal_faults(error, "sales.t")),
    }
    if after != before {
        faults.push(format!("v{version}: the refused {door} left a file behind"));
    }
}

#[tokio::test]
async fn keyed_table_refused_append_sweeps_its_staged_file() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result = repark_iceberg::write::commit_append(catalog, &stale, staged).await;
        let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
        check_swept(&mut faults, version, "append", result, &before, &after);
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_refused_branch_append_sweeps_its_staged_file() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result = repark_iceberg::write::commit_append_to(catalog, &stale, staged, None).await;
        let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
        check_swept(
            &mut faults,
            version,
            "branch append",
            result,
            &before,
            &after,
        );
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

async fn write_options_append_doors_sweep(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    warehouse: &TempDir,
    stale: &iceberg::table::Table,
    version: &str,
    before: &Frozen,
    faults: &mut Vec<String>,
) {
    let catalog = catalogs.get("ice").expect("ice catalog is registered");
    let staged = stage_stale_batch(ctx, stale).await;
    let result =
        repark_iceberg::write::commit_append_with_summary(catalog, stale, staged, &[], None).await;
    check_swept(
        faults,
        version,
        "staged append",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
    let batches = ctx
        .sql("SELECT 9 AS id, 'z' AS name")
        .await
        .expect("staging source frames")
        .collect()
        .await
        .expect("staging source collects");
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>));
    let result = repark_iceberg::write::append_staged_with_options(
        catalog,
        stale,
        stream,
        &[],
        &repark_iceberg::write::WriterStagingOverrides::none(),
        repark_iceberg::write::WriteConcurrency::default(),
        None,
    )
    .await;
    check_swept(
        faults,
        version,
        "streaming staged append",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
    let staged = stage_stale_batch(ctx, stale).await;
    let result =
        repark_iceberg::write::commit_replace_write_with_summary(catalog, stale, staged, &[]).await;
    check_swept(
        faults,
        version,
        "replace write",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
}

async fn write_options_overwrite_doors_sweep(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    warehouse: &TempDir,
    stale: &iceberg::table::Table,
    version: &str,
    before: &Frozen,
    faults: &mut Vec<String>,
) {
    let catalog = catalogs.get("ice").expect("ice catalog is registered");
    let staged = stage_stale_batch(ctx, stale).await;
    let result = repark_iceberg::write::commit_overwrite_replace_all_with_summary(
        catalog,
        stale,
        staged,
        None,
        &[],
        None,
    )
    .await;
    check_swept(
        faults,
        version,
        "replace-all overwrite",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
    let staged = stage_stale_batch(ctx, stale).await;
    let overwrite = repark_iceberg::write::StaticPartitionOverwrite {
        predicate: iceberg::expr::Predicate::AlwaysTrue,
        equalities: Vec::new(),
    };
    let result = repark_iceberg::write::commit_overwrite_by_row_filter_with_summary(
        catalog,
        stale,
        staged,
        overwrite,
        None,
        &[],
        None,
    )
    .await;
    check_swept(
        faults,
        version,
        "row-filter overwrite",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
    let staged = stage_stale_batch(ctx, stale).await;
    let result = repark_iceberg::write::commit_replace_partitions_with_summary(
        catalog,
        stale,
        staged,
        None,
        &[],
        None,
    )
    .await;
    check_swept(
        faults,
        version,
        "replace partitions",
        result,
        before,
        &freeze(ctx, warehouse, catalogs, "t").await,
    );
}

#[tokio::test]
async fn keyed_table_refused_write_options_doors_sweep_their_staged_files() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        write_options_append_doors_sweep(
            &ctx,
            &catalogs,
            &warehouse,
            &stale,
            version,
            &before,
            &mut faults,
        )
        .await;
        write_options_overwrite_doors_sweep(
            &ctx,
            &catalogs,
            &warehouse,
            &stale,
            version,
            &before,
            &mut faults,
        )
        .await;
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_refused_overwrite_sweeps_its_staged_files() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result =
            repark_iceberg::write::commit_overwrite_replace_all_to(catalog, &stale, staged, None)
                .await;
        check_swept(
            &mut faults,
            version,
            "replace-all overwrite",
            result,
            &before,
            &freeze(&ctx, &warehouse, &catalogs, "t").await,
        );
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result = repark_iceberg::write::commit_replace_write(catalog, &stale, staged).await;
        check_swept(
            &mut faults,
            version,
            "replace write",
            result,
            &before,
            &freeze(&ctx, &warehouse, &catalogs, "t").await,
        );
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_refused_partition_overwrite_sweeps_its_staged_files() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result = repark_iceberg::write::commit_overwrite_by_row_filter_to(
            catalog,
            &stale,
            staged,
            iceberg::expr::Predicate::AlwaysTrue,
            None,
        )
        .await;
        check_swept(
            &mut faults,
            version,
            "row-filter overwrite",
            result,
            &before,
            &freeze(&ctx, &warehouse, &catalogs, "t").await,
        );
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result =
            repark_iceberg::write::commit_replace_partitions_to(catalog, &stale, staged, None)
                .await;
        check_swept(
            &mut faults,
            version,
            "replace partitions",
            result,
            &before,
            &freeze(&ctx, &warehouse, &catalogs, "t").await,
        );
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn keyed_table_refused_overwrite_by_filter_sweeps_its_staged_file() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let (ctx, catalogs, warehouse, stale) = sweep_fixture(version).await;
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &stale).await;
        let result = repark_iceberg::write::commit_overwrite_by_filter_with_summary(
            catalog,
            &stale,
            staged,
            iceberg::expr::Predicate::AlwaysTrue,
            None,
            &[],
            repark_iceberg::write::FilterValidation::default(),
        )
        .await;
        let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
        check_swept(
            &mut faults,
            version,
            "overwrite by filter",
            result,
            &before,
            &after,
        );
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}

#[tokio::test]
async fn unkeyed_table_append_stores_its_staged_file() {
    let mut faults = Vec::new();
    for version in VERSIONS {
        let warehouse = TempDir::new().expect("temp warehouse");
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
        let catalog = catalogs.get("ice").expect("ice catalog is registered");
        let fresh = load_sales_table(&catalogs, "t").await;
        let before = freeze(&ctx, &warehouse, &catalogs, "t").await;
        let staged = stage_stale_batch(&ctx, &fresh).await;
        let stored =
            repark_iceberg::write::commit_append_with_summary(catalog, &fresh, staged, &[], None)
                .await;
        if stored.is_err() {
            faults.push(format!("v{version}: the unkeyed append refused"));
            continue;
        }
        let after = freeze(&ctx, &warehouse, &catalogs, "t").await;
        if after.snapshots.len() != before.snapshots.len() + 1 {
            faults.push(format!("v{version}: the unkeyed append added no snapshot"));
        }
        let added: Vec<&String> = after.listing.difference(&before.listing).collect();
        if added.is_empty() {
            faults.push(format!("v{version}: the unkeyed append stored no file"));
        }
    }
    assert!(faults.is_empty(), "{}", faults.join("\n"));
}
