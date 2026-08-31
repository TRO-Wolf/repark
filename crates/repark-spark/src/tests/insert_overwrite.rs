/// `INSERT OVERWRITE` maps to `InsertOp::Overwrite` → the provider's full-table replace.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn insert_overwrite_replaces_all() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (9, 'nine'), (10, 'ten')",
    )
    .await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id >= 9").await,
        2
    );
}

/// BUG-001 materialize path: non-empty column-list form still replaces (value pin).
#[tokio::test]
async fn insert_overwrite_column_list_nonempty_replaces_via_materialize() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (id, name) \
             SELECT id, name FROM src WHERE id = 2",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE id = 2 AND name = 'b'"
        )
        .await,
        1,
        "column-list materialize OW must keep the selected payload"
    );
}

/// A volatile source yields one row for the probe and zero rows for materialization.
#[tokio::test]
async fn insert_overwrite_source_becomes_empty_between_probe_and_exec_does_not_wipe() {
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Volatile gate: first invoke batch is true (probe sees rows); later invokes are false.
    #[derive(Debug)]
    struct ProbeThenEmpty {
        signature: Signature,
        invoke_count: Arc<AtomicUsize>,
    }
    impl PartialEq for ProbeThenEmpty {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }
    impl Eq for ProbeThenEmpty {}
    impl std::hash::Hash for ProbeThenEmpty {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.name().hash(state);
        }
    }
    impl ScalarUDFImpl for ProbeThenEmpty {
        fn name(&self) -> &'static str {
            "repark_probe_then_empty"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let pass = self.invoke_count.fetch_add(1, AtomicOrdering::SeqCst);
            let len = match &args.args[0] {
                ColumnarValue::Array(array) => array.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            // Pass 0 = probe path (LIMIT 1 filter eval); later = materialize.
            let keep = pass == 0;
            let flags: Vec<bool> = std::iter::repeat_n(keep, len).collect();
            Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::array::BooleanArray::from(flags),
            )))
        }
    }

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let prior = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await;
    assert_eq!(prior, 3, "fixture must start with three rows");

    // First invoke pass: return true for every row (probe LIMIT 1 sees a match).
    let invoke_count = Arc::new(AtomicUsize::new(0));
    let invoke_count_for_udf = Arc::clone(&invoke_count);
    ctx.register_udf(ScalarUDF::from(ProbeThenEmpty {
        signature: Signature::exact(vec![DataType::Int32], Volatility::Volatile),
        invoke_count: invoke_count_for_udf,
    }));

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT id, name FROM src WHERE repark_probe_then_empty(id)",
    )
    .await
    .expect_err("TOCTOU empty-after-nonempty-probe must refuse wipe, not succeed");
    let message = error.to_string();
    assert!(
        message.contains("became empty") || message.contains("BUG-001"),
        "must surface BUG-001 refuse-wipe class, got: {message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        prior,
        "source-becomes-empty TOCTOU must leave prior rows (must NOT wipe)"
    );
    assert!(
        invoke_count.load(AtomicOrdering::SeqCst) >= 2,
        "probe and materialize must both have evaluated the gate UDF"
    );
}

/// BUG-001 companion: honest empty overwrite still wipes.
#[tokio::test]
async fn empty_insert_overwrite_still_wipes_after_bug001_materialize() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "honest empty INSERT OVERWRITE must still wipe"
    );
}

/// Nullability tighten must only flip nullability — field + schema metadata (e.g.
#[test]
fn tighten_batch_nullability_preserves_field_and_schema_metadata() {
    let field_meta = HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]);
    let schema_meta = HashMap::from([("iceberg.schema".to_string(), "x".to_string())]);
    let field = Field::new("id", DataType::Int32, true).with_metadata(field_meta.clone());
    let schema = Arc::new(Schema::new_with_metadata(vec![field], schema_meta.clone()));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
    )
    .expect("batch");
    let out = tighten_batch_nullability(vec![batch]).expect("tighten");
    assert_eq!(out.len(), 1);
    let out_schema = out[0].schema();
    assert!(
        !out_schema.field(0).is_nullable(),
        "zero-null column must tighten to non-nullable"
    );
    assert_eq!(
        out_schema.field(0).metadata(),
        &field_meta,
        "field metadata must be preserved"
    );
    assert_eq!(
        out_schema.metadata(),
        &schema_meta,
        "schema metadata must be preserved"
    );
    // Column with a null stays nullable and still keeps metadata.
    let nullable_field = Field::new("name", DataType::Utf8, true).with_metadata(field_meta.clone());
    let nullable_schema = Arc::new(Schema::new(vec![nullable_field]));
    let nullable_batch = RecordBatch::try_new(
        nullable_schema,
        vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
    )
    .expect("nullable batch");
    let out_null = tighten_batch_nullability(vec![nullable_batch]).expect("tighten nulls");
    assert!(out_null[0].schema().field(0).is_nullable());
    assert_eq!(out_null[0].schema().field(0).metadata(), &field_meta);
}

/// Spark's `INSERT OVERWRITE TABLE t …` keyword form works identically to the bare form.
#[tokio::test]
async fn insert_overwrite_table_keyword_form() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE TABLE ice.sales.t SELECT * FROM src WHERE id = 1",
    )
    .await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}

/// Empty `INSERT OVERWRITE … SELECT … WHERE false` must replace the table.
#[tokio::test]
async fn empty_insert_overwrite_select_where_false_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM src WHERE false",
    )
    .await;

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "empty INSERT OVERWRITE must wipe all rows (not a silent no-op)"
    );
}

/// BUG-003: empty OW must use the **provider overwrite** wipe, not a `DELETE FROM` rewrite.
#[tokio::test]
async fn empty_insert_overwrite_mor_table_uses_overwrite_not_delete_shape() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.delete.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    assert!(
        !live_data_file_paths(&catalogs, "t").await.is_empty(),
        "precondition: CTAS must land data files"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM src WHERE false",
    )
    .await;

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "empty INSERT OVERWRITE must wipe all rows"
    );
    assert!(
        live_data_file_paths(&catalogs, "t").await.is_empty(),
        "provider overwrite wipe must remove live data files (DELETE rewrite would leave them)"
    );
    assert_eq!(
        delete_file_count(&catalogs, "t").await,
        0,
        "provider overwrite wipe must not commit position-delete files (DELETE MoR would)"
    );
    // Note: Iceberg may still stamp summary.operation = Delete for a full-file remove.
}

/// Empty OW must not wipe when the source launders types via CAST.
#[tokio::test]
async fn empty_insert_overwrite_cast_launder_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let err = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT CAST('x' AS INT) AS id, name FROM src WHERE false",
    )
    .await
    .expect_err("CAST-laundered empty OW must refuse wipe");
    assert!(
        err.to_string().contains("CAST") || err.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {err}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "CAST-launder empty OW must leave prior rows"
    );
}

/// Aggregate casts live in `Aggregate.aggr_expr`, not the wrapping projection.
#[tokio::test]
async fn empty_insert_overwrite_aggregate_cast_launder_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT max(CAST(a AS INT)) AS id, 'z' AS name FROM src2 WHERE false GROUP BY b",
    )
    .await
    .expect_err("Aggregate-hosted CAST launder must refuse the wipe");
    assert!(
        error.to_string().contains("CAST") || error.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "Aggregate-hosted CAST launder must leave prior rows"
    );

    // Control: the identical statement WITH rows fails at cast and keeps prior rows.
    let control = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT max(CAST(a AS INT)) AS id, 'z' AS name FROM src2 GROUP BY b",
    )
    .await
    .expect_err("non-empty Aggregate CAST must fail at cast");
    assert!(
        control.to_string().contains("Cast") || control.to_string().contains("cast"),
        "non-empty control must fail at cast, got: {control}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty Aggregate CAST must leave prior rows"
    );
}

/// Window casts live in `Window.window_expr`; this node shape must also refuse a fallible cast.
#[tokio::test]
async fn empty_insert_overwrite_window_cast_launder_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT max(CAST(a AS INT)) OVER (PARTITION BY b) AS id, b AS name \
             FROM src2 WHERE false",
    )
    .await
    .expect_err("Window-hosted CAST launder must refuse the wipe");
    assert!(
        error.to_string().contains("CAST") || error.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "Window-hosted CAST launder must leave prior rows"
    );

    let control = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT max(CAST(a AS INT)) OVER (PARTITION BY b) AS id, b AS name FROM src2",
    )
    .await
    .expect_err("non-empty Window CAST must fail at cast");
    assert!(
        control.to_string().contains("Cast") || control.to_string().contains("cast"),
        "non-empty control must fail at cast, got: {control}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty Window CAST must leave prior rows"
    );
}

/// Scalar-subquery expressions are not logical-plan children.
#[tokio::test]
async fn empty_insert_overwrite_scalar_subquery_cast_launder_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT (SELECT CAST(a AS INT) FROM src2 LIMIT 1) AS id, name FROM src WHERE false",
    )
    .await
    .expect_err("scalar-subquery CAST launder must refuse the wipe");
    assert!(
        error.to_string().contains("CAST") || error.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "scalar-subquery CAST launder must leave prior rows"
    );

    let control = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT (SELECT CAST(a AS INT) FROM src2 LIMIT 1) AS id, name FROM src",
    )
    .await
    .expect_err("non-empty scalar-subquery CAST must fail at cast");
    assert!(
        control.to_string().contains("Cast") || control.to_string().contains("cast"),
        "non-empty control must fail at cast, got: {control}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty scalar-subquery CAST must leave prior rows"
    );
}

/// Join-key casts can be skipped when the source is empty.
#[tokio::test]
async fn empty_insert_overwrite_join_key_cast_launder_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT s.id, s.name FROM src s JOIN src2 j ON CAST(j.a AS INT) = s.id WHERE false",
    )
    .await
    .expect_err("join-key CAST launder must refuse the wipe");
    assert!(
        error.to_string().contains("CAST") || error.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "join-key CAST launder must leave prior rows"
    );

    let control = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT s.id, s.name FROM src s JOIN src2 j ON CAST(j.a AS INT) = s.id",
    )
    .await
    .expect_err("non-empty join-key CAST must fail at cast");
    assert!(
        control.to_string().contains("Cast") || control.to_string().contains("cast"),
        "non-empty control must fail at cast, got: {control}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty join-key CAST must leave prior rows"
    );
}

/// A runtime-empty source does not evaluate its `Filter` predicate.
#[tokio::test]
async fn empty_insert_overwrite_runtime_empty_predicate_cast_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "stage", &[]);
    register_source(&ctx, "stage_loaded", &[(7, "zz")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, name FROM stage WHERE CAST(name AS INT) = 1",
    )
    .await
    .expect_err("runtime-empty source with a fallible WHERE cast must refuse the wipe");
    assert!(
        error.to_string().contains("CAST") || error.to_string().contains("cast"),
        "expected CAST-refusal diagnostic, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "runtime-empty predicate CAST must leave prior rows"
    );

    // Control: the same statement over a source.
    let control = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT id, name FROM stage_loaded WHERE CAST(name AS INT) = 1",
    )
    .await
    .expect_err("non-empty predicate CAST must fail at cast");
    assert!(
        control.to_string().contains("Cast") || control.to_string().contains("cast"),
        "non-empty control must fail at cast, got: {control}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty predicate CAST must leave prior rows"
    );
}

/// `CAST` is total because NULL casts to NULL for every target.
#[tokio::test]
async fn empty_insert_overwrite_null_literal_cast_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, CAST(NULL AS STRING) AS name \
             FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "a total NULL-literal cast must not block the wipe"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, CAST(NULL AS STRING) AS name FROM src",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "non-empty control of the same statement must succeed"
    );
}

/// Analyzer-inserted `Int32 → Utf8` coercion in a `Filter` cannot raise, so it remains allowed.
#[tokio::test]
async fn empty_insert_overwrite_predicate_coercion_cast_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    run(
        &ctx,
        &catalogs,
        // DF54: string/numeric comparisons coerce the string side to numeric.
        "INSERT OVERWRITE ice.sales.t SELECT id, name FROM src WHERE id > 99",
    )
    .await;

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "numeric WHERE comparison (no fallible cast) must not block the wipe"
    );
}

/// Analyzer-inserted `CAST` inside a `Projection` is infallible and must remain allowed.
#[tokio::test]
async fn empty_insert_overwrite_projection_coercion_cast_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, concat(name, id) AS name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "infallible coercion CAST in a Projection must not block the wipe"
    );

    // Control: the same statement WITH rows succeeds.
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, concat(name, id) AS name FROM src",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "non-empty control of the same statement must succeed"
    );
}

/// A user-written integer-to-string cast cannot raise, so the empty form must wipe.
#[tokio::test]
async fn empty_insert_overwrite_stringify_cast_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, CAST(id AS STRING) AS name \
             FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "infallible stringify CAST must not block the wipe"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, CAST(id AS STRING) AS name FROM src",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "non-empty control of the same statement must succeed"
    );
}

/// `TRY_CAST` is total, so the empty and non-empty forms agree: no asymmetry, no refusal.
#[tokio::test]
async fn empty_insert_overwrite_try_cast_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT TRY_CAST(a AS INT) AS id, b AS name FROM src2 WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "TRY_CAST cannot raise, so the empty form must wipe"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT TRY_CAST(a AS INT) AS id, b AS name FROM src2",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        1,
        "non-empty TRY_CAST control must succeed (NULL, not an error)"
    );
}

/// Ambiguity branch: an `INSERT OVERWRITE` column list.
#[tokio::test]
async fn empty_insert_overwrite_case_ambiguous_column_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("ID", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    ctx.register_batch("case_collide", batch).unwrap();

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE case_collide (id) SELECT 1 AS id WHERE false",
    )
    .await
    .expect_err("case-ambiguous column list must fail loud, not wipe");
    assert!(
        error.to_string().contains("ambiguous"),
        "error must name the ambiguity, got: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM case_collide").await,
        1,
        "ambiguous empty OW must leave prior rows"
    );
}

/// Empty OW column list resolves case-insensitively (Spark caseSensitive=false).
#[tokio::test]
async fn empty_insert_overwrite_column_list_case_insensitive_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (ID, NAME) SELECT id, name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "case-differing column list empty OW must wipe, not refuse"
    );
}

/// Hollow-pin close: empty computed source into a partitioned target still wipes.
#[tokio::test]
async fn empty_computed_insert_overwrite_into_partitioned_still_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    // A computed non-partition column is refused for a non-empty partitioned target.
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t \
             SELECT id, upper(name) AS name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "empty computed OW into partitioned table must wipe (not guard-refuse)"
    );
}

/// The `INSERT OVERWRITE TABLE` keyword form with an empty source also wipes.
#[tokio::test]
async fn empty_insert_overwrite_table_keyword_wipes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE TABLE ice.sales.t SELECT * FROM src WHERE id < 0",
    )
    .await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

/// Empty `INSERT INTO` must not wipe — zero rows appended, prior rows remain.
#[tokio::test]
async fn empty_insert_into_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT * FROM src WHERE false",
    )
    .await;

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "empty INSERT INTO is a no-op append, not a wipe"
    );
}

/// Empty static `INSERT OVERWRITE … PARTITION` drops only that partition.
/// pins: dml-b-insert-overwrite/C-001, C-004, C-005
#[tokio::test]
async fn empty_insert_overwrite_partition_refuses_full_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(2, "b".into()), (3, "c".into())],
        "empty static partition overwrite must drop only id=1"
    );
}

/// Empty `INSERT OVERWRITE … LIMIT 0` must wipe (not only `WHERE false` forms).
#[tokio::test]
async fn empty_insert_overwrite_limit_zero_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM src LIMIT 0",
    )
    .await;

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "LIMIT 0 INSERT OVERWRITE must wipe (same class as WHERE false)"
    );
}

/// Non-empty static `INSERT OVERWRITE … PARTITION` replaces only that partition.
/// pins: dml-b-insert-overwrite/C-001, C-005
#[tokio::test]
async fn insert_overwrite_partition_nonempty_refuses_whole_table_replace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT 'z' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "z".into()), (2, "b".into()), (3, "c".into())],
        "static partition overwrite must not whole-table replace"
    );
}

/// Empty INSERT OVERWRITE with incompatible source schema must fail loud and leave prior rows.
#[tokio::test]
async fn empty_insert_overwrite_incompatible_schema_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 'x' AS only_wrong WHERE false",
    )
    .await
    .expect_err("incompatible empty overwrite must fail, not wipe");
    let message = error.to_string();
    assert!(
        message.contains("Column count")
            || message.contains("column")
            || message.contains("schema")
            || message.contains("field"),
        "error must name schema/column mismatch, got: {message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "incompatible empty INSERT OVERWRITE must not wipe prior rows"
    );
}

/// Same-arity type mismatch empty OW must not wipe.
#[tokio::test]
async fn empty_insert_overwrite_type_mismatch_same_arity_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 'x' AS id, 'y' AS name WHERE false",
    )
    .await
    .expect_err("type-mismatch empty overwrite must fail, not wipe");
    let message = error.to_string();
    assert!(
        message.contains("assignment-compatible")
            || message.contains("type")
            || message.contains("refusing full-table wipe"),
        "error must name type assignment refusal, got: {message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "type-mismatch empty INSERT OVERWRITE must not wipe prior rows"
    );

    // Control: non-empty type mismatch still fails and leaves rows (asymmetry class pin).
    let nonempty_error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 'x' AS id, 'y' AS name",
    )
    .await
    .expect_err("non-empty type mismatch must still fail");
    let nonempty_message = nonempty_error.to_string();
    assert!(
        nonempty_message.contains("Cast")
            || nonempty_message.contains("cast")
            || nonempty_message.contains("Int32"),
        "non-empty must fail at cast, got: {nonempty_message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "failed non-empty type-mismatch INSERT OVERWRITE must leave prior rows"
    );
}

/// WITH-CTE empty INSERT OVERWRITE still wipes (probe wraps arbitrary Query Display).
#[tokio::test]
async fn empty_insert_overwrite_with_cte_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t WITH e AS (SELECT * FROM src WHERE false) SELECT * FROM e",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "WITH-CTE empty INSERT OVERWRITE must wipe"
    );
}

/// `INSERT OVERWRITE INTO` empty source must wipe — same class as bare `INSERT OVERWRITE`.
#[tokio::test]
async fn empty_insert_overwrite_into_keyword_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE INTO ice.sales.t SELECT * FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "INSERT OVERWRITE INTO empty must wipe"
    );
}

/// Column-list empty INSERT OVERWRITE must wipe (same class as bare SELECT *).
#[tokio::test]
async fn empty_insert_overwrite_column_list_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (id, name) SELECT id, name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "column-list empty INSERT OVERWRITE must wipe"
    );
}

/// Self-scan empty INSERT OVERWRITE must wipe (probe wraps source; DELETE target).
#[tokio::test]
async fn empty_insert_overwrite_self_scan_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM ice.sales.t WHERE false",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "self-scan empty INSERT OVERWRITE must wipe"
    );
}

/// ORDER BY … LIMIT 0 empty INSERT OVERWRITE must wipe (not only bare LIMIT 0).
#[tokio::test]
async fn empty_insert_overwrite_order_by_limit_zero_wipes_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT * FROM src ORDER BY id LIMIT 0",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        0,
        "ORDER BY … LIMIT 0 INSERT OVERWRITE must wipe"
    );
}

/// Column-list empty OW with wrong SELECT arity must not wipe.
#[tokio::test]
async fn empty_insert_overwrite_column_list_incompatible_does_not_wipe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (id, name) SELECT 'x' AS only_wrong WHERE false",
    )
    .await
    .expect_err("column-list incompatible empty overwrite must fail, not wipe");
    let message = error.to_string();
    assert!(
        message.contains("Column count")
            || message.contains("column")
            || message.contains("schema")
            || message.contains("field"),
        "error must name schema/column mismatch, got: {message}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "column-list incompatible empty OW must leave prior rows"
    );
}
