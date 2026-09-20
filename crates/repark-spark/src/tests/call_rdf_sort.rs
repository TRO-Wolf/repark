use super::super::*;
use super::call::call_count;
use super::common::*;

async fn seed_unsorted(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id BIGINT, data STRING) USING iceberg"),
    )
    .await;
    for (id, data) in [(3_i64, "c"), (1, "a"), (8, "h"), (5, "e"), (2, "b")] {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({id}, '{data}')"),
        )
        .await;
    }
}

async fn data_file_paths(catalog: &dyn Catalog, ident: &TableIdent) -> Vec<String> {
    use futures::TryStreamExt;
    let table = catalog.load_table(ident).await.expect("load");
    let scan = table.scan().build().expect("scan");
    let tasks: Vec<iceberg::scan::FileScanTask> = scan
        .plan_files()
        .await
        .expect("plan_files")
        .try_collect()
        .await
        .expect("collect tasks");
    tasks
        .into_iter()
        .map(|task| task.data_file_path.to_string())
        .collect()
}

fn ids_in_file_order(uri: &str) -> Vec<Option<i64>> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = uri
        .strip_prefix("file://")
        .or_else(|| uri.strip_prefix("file:"))
        .unwrap_or(uri);
    let file = std::fs::File::open(path).expect("open rewritten data file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .build()
        .expect("build reader");
    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_primitive::<Int64Type>();
        ids.extend(column.iter());
    }
    ids
}

async fn sort_order_fields(catalog: &dyn Catalog, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("load");
    table.metadata().default_sort_order().fields.len()
}

#[tokio::test]
async fn call_rdf_sort_explicit_order_writes_descending_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "so").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "so".into());
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.so', strategy => 'sort', \
         sort_order => 'id DESC NULLS LAST', options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("sort rewrite must succeed")
    .collect()
    .await
    .expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 5);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 1);
    let files = data_file_paths(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(files.len(), 1, "rewrite-all must fuse the five files");
    assert_eq!(
        ids_in_file_order(&files[0]),
        vec![Some(8), Some(5), Some(3), Some(2), Some(1)],
        "a bin-pack rewrite would leave insertion order"
    );
    assert_eq!(
        sort_order_fields(catalogs["ice"].as_ref(), &ident).await,
        0,
        "the procedure must not record the one-shot order on the table"
    );
}

#[tokio::test]
async fn call_rdf_sort_without_sort_order_uses_the_table_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tbo (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.tbo WRITE ORDERED BY (id DESC NULLS LAST)",
    )
    .await;
    for (id, data) in [(3_i64, "c"), (1, "a"), (8, "h"), (5, "e"), (2, "b")] {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.tbo VALUES ({id}, '{data}')"),
        )
        .await;
    }
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "tbo".into());
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.tbo', strategy => 'sort', \
         options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("table-order sort rewrite must succeed")
    .collect()
    .await
    .expect("collect");
    let files = data_file_paths(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(files.len(), 1);
    assert_eq!(
        ids_in_file_order(&files[0]),
        vec![Some(8), Some(5), Some(3), Some(2), Some(1)],
        "the table's own DESC order must drive the rewrite"
    );
}

#[tokio::test]
async fn call_rdf_sort_on_unsorted_table_surfaces_the_fork_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "us").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "us".into());
    let before = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.us', strategy => 'sort')",
    )
    .await
    .expect_err("sort on an unsorted table must refuse");
    assert_eq!(
        error.to_string(),
        concat!(
            "External error: Cannot sort data without a valid sort order, ",
            "table 'sales.us' is unsorted and no sort order is provided"
        )
    );
    assert_eq!(
        count_planned_data_files(catalogs["ice"].as_ref(), &ident).await,
        before,
        "a refused CALL must not compact"
    );
}

#[tokio::test]
async fn call_rdf_binpack_with_sort_order_matches_javas_rewrite_mode_message() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "bp").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "bp".into());
    let before = count_planned_data_files(catalogs["ice"].as_ref(), &ident).await;
    for order in ["id ASC", "zorder(id, data)"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.bp', strategy => 'binpack', \
                 sort_order => '{order}')"
            ),
        )
        .await
        .expect_err("binpack plus a sort order must refuse");
        assert!(
            error
                .to_string()
                .contains("Cannot set rewrite mode, it has already been set to BIN-PACK"),
            "got: {error}"
        );
    }
    assert_eq!(
        count_planned_data_files(catalogs["ice"].as_ref(), &ident).await,
        before
    );
}

#[tokio::test]
async fn call_rdf_sort_order_without_strategy_is_consulted() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "ns").await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "ns".into());
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.ns', sort_order => 'nope ASC')",
    )
    .await
    .expect_err("an omitted strategy must still consult sort_order");
    assert!(
        error.to_string().contains("Cannot find field 'nope'"),
        "got: {error}"
    );
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.ns', \
         sort_order => 'id DESC NULLS LAST', options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("an omitted strategy with a sort order sorts")
    .collect()
    .await
    .expect("collect");
    let files = data_file_paths(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(files.len(), 1);
    assert_eq!(
        ids_in_file_order(&files[0]),
        vec![Some(8), Some(5), Some(3), Some(2), Some(1)]
    );
}

#[tokio::test]
async fn call_rdf_zorder_refusals_come_from_the_fork() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "zo").await;
    for (order, expected) in [
        (
            "zorder(nope)",
            concat!(
                "External error: Cannot find column 'nope' in table schema ",
                "(case sensitive = false): ",
                "struct<1: id: optional long, 2: data: optional string>"
            ),
        ),
        (
            "zorder()",
            "External error: Cannot ZOrder when no columns are specified",
        ),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.zo', strategy => 'sort', \
                 sort_order => '{order}')"
            ),
        )
        .await
        .expect_err("bad zorder must refuse");
        assert_eq!(error.to_string(), expected);
    }
}

#[tokio::test]
async fn call_rdf_sort_order_parse_refusals_match_java() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "pr").await;
    for (order, needle) in [
        (
            "id, zorder(data)",
            "Cannot mix identity sort columns and a Zorder sort expression: id, zorder(data)",
        ),
        ("id NULLS SIDEWAYS", "Unable to parse sortOrder: "),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.pr', strategy => 'sort', \
                 sort_order => '{order}')"
            ),
        )
        .await
        .expect_err("a malformed sort order must refuse");
        assert!(error.to_string().contains(needle), "got: {error}");
    }
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.pr', strategy => 'sort', \
         sort_order => 'bucket(4, id)')",
    )
    .await
    .expect_err("a transform sort term refuses on the CALL door");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "got: {error:?}"
    );
    assert_eq!(
        error.to_string(),
        concat!(
            "This feature is not implemented: CALL rewrite_data_files sort_order transform ",
            "`bucket(…)` is not supported yet — only identity sort columns and zorder(…) are ported"
        )
    );
}

#[tokio::test]
async fn call_rdf_layout_options_follow_the_rewriter() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_unsorted(&ctx, &catalogs, "lo").await;
    for (call, needle) in [
        (
            "strategy => 'binpack', options => map('shuffle-partitions-per-file', '4')",
            "Cannot use options [shuffle-partitions-per-file], they are not supported by the \
             action or the rewriter BIN-PACK",
        ),
        (
            "sort_order => 'id DESC', options => map('var-length-contribution', '4')",
            "Cannot use options [var-length-contribution], they are not supported by the action \
             or the rewriter SORT",
        ),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CALL ice.system.rewrite_data_files(table => 'sales.lo', {call})"),
        )
        .await
        .expect_err("an option the rewriter does not accept must refuse");
        assert!(error.to_string().contains(needle), "got: {error}");
    }
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.lo', strategy => 'sort', \
         sort_order => 'zorder(id, data)', options => map('rewrite-all', 'true', \
         'var-length-contribution', '4', 'max-output-size', '64', \
         'shuffle-partitions-per-file', '1', 'compression-factor', '1.0'))",
    )
    .await
    .expect("z-order accepts all four layout options")
    .collect()
    .await
    .expect("collect");
}
