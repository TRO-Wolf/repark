use super::super::*;
use super::call::call_count;
use super::common::*;

async fn seed_options_shape(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.opts (id INT, part INT) USING iceberg PARTITIONED BY (part)",
    )
    .await;
    for index in 1..=4 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.opts VALUES ({index}, 0)"),
        )
        .await;
    }
    for index in 101..=104 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.opts VALUES ({index}, 1)"),
        )
        .await;
    }
}

async fn seed_mor_shape(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.rpdopts (id INT, v STRING) USING iceberg TBLPROPERTIES \
         ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')",
    )
    .await;
    for id in 1..=6 {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.rpdopts VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
    run(ctx, catalogs, "DELETE FROM ice.sales.rpdopts WHERE id <= 3").await;
}

async fn rdf_options_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    options: &str,
) -> String {
    execute(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.rewrite_data_files(table => '{table}', options => map({options}))"
        ),
    )
    .await
    .expect_err("options cell must refuse")
    .to_string()
}

async fn rpd_options_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    options: &str,
) -> String {
    execute(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.rewrite_position_delete_files(table => 'sales.rpdopts', options => map({options}))"
        ),
    )
    .await
    .expect_err("options cell must refuse")
    .to_string()
}

#[tokio::test]
async fn call_rdf_options_unknown_key_names_binpack() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'foo', '1'").await;
    assert!(
        message.contains("Cannot use options [foo], they are not supported by the action or the rewriter BIN-PACK"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_unknown_keys_join_in_map_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'aaa', '1', 'bbb', '2'").await;
    assert!(
        message.contains("Cannot use options [aaa, bbb]"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_uppercase_key_is_unknown() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'MIN-INPUT-FILES', '1'").await;
    assert!(
        message.contains("Cannot use options [MIN-INPUT-FILES]"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_empty_key_is_unknown() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'', '1'").await;
    assert!(
        message.contains("Cannot use options [], they are not supported"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_bad_integer_reports_the_input() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message =
        rdf_options_error(&ctx, &catalogs, "sales.opts", "'min-input-files', 'abc'").await;
    assert!(
        message.contains("For input string: \"abc\""),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_negative_delete_threshold() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'delete-file-threshold', '-1'",
    )
    .await;
    assert!(
        message.contains("'delete-file-threshold' is set to -1 but must be >= 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_zero_min_input_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'min-input-files', '0'").await;
    assert!(
        message.contains("'min-input-files' is set to 0 but must be > 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_bad_job_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'rewrite-job-order', 'bogus'",
    )
    .await;
    assert!(
        message.contains("Invalid rewrite job order name: bogus"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_job_order_names_are_case_insensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    for name in ["none", "BYTES-DESC", "files-desc"] {
        execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('rewrite-job-order', '{name}'))"
            ),
        )
        .await
        .expect("job order spelling must validate");
    }
}

#[tokio::test]
async fn call_rdf_options_unknown_spec_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(&ctx, &catalogs, "sales.opts", "'output-spec-id', '99'").await;
    assert!(
        message.contains(
            "Cannot use output spec id 99 because the table does not contain a reference to this spec-id."
        ),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_current_spec_id_validates() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('output-spec-id', '0'))",
    )
    .await
    .expect("the current spec id must validate");
}

#[tokio::test]
async fn call_rdf_options_zero_max_commits_needs_enabled_progress() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'partial-progress.enabled', 'true', 'partial-progress.max-commits', '0'",
    )
    .await;
    assert!(
        message.contains(
            "Cannot set partial-progress.max-commits to 0, the value must be positive when partial-progress.enabled is true"
        ),
        "got: {message}"
    );
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('partial-progress.max-commits', '0'))",
    )
    .await
    .expect("max-commits without enabled progress must validate");
}

#[tokio::test]
async fn call_rdf_options_target_against_max() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'min-file-size-bytes', '0', 'max-file-size-bytes', '100000000', 'min-input-files', '2'",
    )
    .await;
    assert!(
        message.contains(
            "'target-file-size-bytes' (536870912) must be < 'max-file-size-bytes' (100000000)"
        ),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_negative_target() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'target-file-size-bytes', '-1'",
    )
    .await;
    assert!(
        message.contains("'target-file-size-bytes' is set to -1 but must be > 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_zero_concurrent_rewrites() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'max-concurrent-file-group-rewrites', '0'",
    )
    .await;
    assert!(
        message.contains(
            "Cannot set max-concurrent-file-group-rewrites to 0, the value must be positive."
        ),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_delete_ratio_bounds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'delete-ratio-threshold', '2'",
    )
    .await;
    assert!(
        message.contains("'delete-ratio-threshold' is set to 2.0 but must be <= 1"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_duplicate_key_matches_spark_map() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message = rdf_options_error(
        &ctx,
        &catalogs,
        "sales.opts",
        "'min-input-files', '1', 'min-input-files', '2'",
    )
    .await;
    assert!(
        message.contains("[DUPLICATED_MAP_KEY] Duplicate map key min-input-files was found"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_non_map_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => 'min-input-files')",
    )
    .await
    .expect_err("a non-map options value must refuse");
    assert!(
        error.to_string().contains("must be map(k, v, …)"),
        "got: {error}"
    );
}

#[tokio::test]
async fn call_rdf_options_bad_boolean_is_silently_false() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('rewrite-all', 'maybe'))",
    )
    .await
    .expect("a bad boolean parses as false");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 0);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 0);
}

#[tokio::test]
async fn call_rdf_options_empty_map_and_null_are_defaults() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    for options in ["map()", "NULL"] {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => {options})"
            ),
        )
        .await
        .expect("empty options must behave as defaults");
        let batches = frame.collect().await.expect("collect");
        assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 0);
    }
}

#[tokio::test]
async fn call_rdf_options_integer_literal_value_parses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('min-input-files', 9))",
    )
    .await
    .expect("an integer literal value must parse");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 0);
}

#[tokio::test]
async fn call_rdf_options_min_input_files_rewrites() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('min-input-files', '1'))",
    )
    .await
    .expect("rewrite with options");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 8);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.opts.files").await,
        2,
        "one output file per partition"
    );
}

#[tokio::test]
async fn call_rdf_options_where_composes_with_options() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_data_files(table => 'sales.opts', options => map('min-input-files', '1'), where => 'part = 1')",
    )
    .await
    .expect("filtered rewrite with options");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(call_count(&batches[0], "rewritten_data_files_count"), 4);
    assert_eq!(call_count(&batches[0], "added_data_files_count"), 1);
}

#[tokio::test]
async fn call_rpd_options_unknown_key_names_binpack() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message = rpd_options_error(&ctx, &catalogs, "'foo', '1'").await;
    assert!(
        message.contains("Cannot use options [foo], they are not supported by the action or the rewriter BIN-PACK"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_rejects_data_only_keys() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    for (options, key) in [
        ("'delete-file-threshold', '1'", "delete-file-threshold"),
        ("'delete-ratio-threshold', '0.5'", "delete-ratio-threshold"),
        ("'output-spec-id', '0'", "output-spec-id"),
        (
            "'use-starting-sequence-number', 'false'",
            "use-starting-sequence-number",
        ),
        (
            "'remove-dangling-deletes', 'true'",
            "remove-dangling-deletes",
        ),
        (
            "'partial-progress.max-failed-commits', '2'",
            "partial-progress.max-failed-commits",
        ),
    ] {
        let message = rpd_options_error(&ctx, &catalogs, options).await;
        assert!(
            message.contains(&format!("Cannot use options [{key}]")),
            "got: {message}"
        );
    }
}

#[tokio::test]
async fn call_rpd_options_bad_integer_reports_the_input() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message = rpd_options_error(&ctx, &catalogs, "'min-input-files', 'abc'").await;
    assert!(
        message.contains("For input string: \"abc\""),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_min_input_files_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let frame = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rpdopts', options => map('min-input-files', '1'))",
    )
    .await
    .expect("delete rewrite with options");
    let batches = frame.collect().await.expect("collect");
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "rewritten_delete_files_count",
            "added_delete_files_count",
            "rewritten_bytes_count",
            "added_bytes_count"
        ]
    );
}

#[tokio::test]
async fn call_rpd_options_rewrite_all_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.rpdopts', options => map('rewrite-all', 'true'))",
    )
    .await
    .expect("rewrite-all must validate on the delete procedure");
}

#[tokio::test]
async fn call_rpd_options_enabled_zero_max_commits_is_illegal_argument() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message = rpd_options_error(
        &ctx,
        &catalogs,
        "'partial-progress.enabled', 'true', 'partial-progress.max-commits', '0'",
    )
    .await;
    assert!(
        message.contains("Cannot set partial-progress.max-commits to 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_zero_concurrent_is_illegal_argument() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message =
        rpd_options_error(&ctx, &catalogs, "'max-concurrent-file-group-rewrites', '0'").await;
    assert!(
        message.contains("Cannot set max-concurrent-file-group-rewrites to 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_bogus_job_order_is_illegal_argument() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message = rpd_options_error(&ctx, &catalogs, "'rewrite-job-order', 'bogus'").await;
    assert!(
        message.contains("Invalid rewrite job order name: bogus"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_negative_min_size_needs_non_negative() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message =
        rdf_options_error(&ctx, &catalogs, "sales.opts", "'min-file-size-bytes', '-1'").await;
    assert!(
        message.contains("'min-file-size-bytes' is set to -1 but must be >= 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rdf_options_negative_max_size_fails_band() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_options_shape(&ctx, &catalogs).await;
    let message =
        rdf_options_error(&ctx, &catalogs, "sales.opts", "'max-file-size-bytes', '-1'").await;
    assert!(
        message.contains("must be < 'max-file-size-bytes' (-1)"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_negative_min_size_needs_non_negative() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    let message = rpd_options_error(&ctx, &catalogs, "'min-file-size-bytes', '-1'").await;
    assert!(
        message.contains("'min-file-size-bytes' is set to -1 but must be >= 0"),
        "got: {message}"
    );
}

#[tokio::test]
async fn call_rpd_options_unwired_keys_refuse_unsupported() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_mor_shape(&ctx, &catalogs).await;
    for (options, key) in [
        ("'rewrite-job-order', 'bytes-desc'", "rewrite-job-order"),
        (
            "'partial-progress.enabled', 'false'",
            "partial-progress.enabled",
        ),
        (
            "'partial-progress.max-commits', '3'",
            "partial-progress.max-commits",
        ),
        (
            "'max-concurrent-file-group-rewrites', '4'",
            "max-concurrent-file-group-rewrites",
        ),
    ] {
        let message = rpd_options_error(&ctx, &catalogs, options).await;
        assert!(
            message.contains(&format!(
                "CALL rewrite_position_delete_files option [{key}] is not supported in RePark \
                 (registry ICE-RDF-OPTIONS-1, 2026-09-17)"
            )),
            "got: {message}"
        );
    }
}
