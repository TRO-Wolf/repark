/// BUG-001 hard rule: `MERGE` is never gated by the DELETE/UPDATE merge-on-read multi-spec valve.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn bug001_merge_mor_not_blocked_by_delete_update_valve() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mrg (id INT, category STRING, name STRING) USING iceberg \
             TBLPROPERTIES(\
               'write.merge.mode' = 'merge-on-read', \
               'write.delete.mode' = 'merge-on-read', \
               'write.update.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mrg VALUES (1, 'a', 'x'), (2, 'b', 'y')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.mrg ADD PARTITION FIELD category",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.mrg DROP PARTITION FIELD category",
    )
    .await;
    // Hazard shape for DELETE/UPDATE — but MERGE must still run.
    let table = load_sales_table(&catalogs, "mrg").await;
    assert!(table.metadata().partition_specs_iter().len() > 1);
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned()
            || table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty()
    );

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.mrg AS t USING (SELECT 1 AS id, 'z' AS name) AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.mrg WHERE id = 1 AND name = 'z'"
        )
        .await,
        1
    );
}

/// Classic upsert: matched rows take source values; unmatched source rows insert; others survive.
#[tokio::test]
async fn merge_upsert_updates_and_inserts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ]
    );
}

/// The literal source publish job MERGE text.
#[tokio::test]
async fn merge_star_forms_upsert() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    // The source carries an extra column the target lacks — Spark's star resolution ignores it.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("extra", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec!["bee", "dee"])),
            Arc::new(datafusion::arrow::array::BooleanArray::from(vec![
                true, false,
            ])),
        ],
    )
    .unwrap();
    ctx.register_batch("staging_view", batch).unwrap();

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS Target USING staging_view AS Source \
             ON Target.id = Source.id \
             WHEN MATCHED THEN UPDATE SET * \
             WHEN NOT MATCHED THEN INSERT *",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ]
    );
}

/// A star whose source cannot provide every target column errors up front, never silent NULL-fill.
#[tokio::test]
async fn merge_star_missing_source_column_errors() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![2, 4]))]).unwrap();
    ctx.register_batch("ids_only", batch).unwrap();

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING ids_only AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET *",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("missing from the source: `name`"),
        "expected the missing-column star error, got: {err}"
    );
}

/// `WHEN MATCHED AND <cond> THEN DELETE`: only the row passing the clause predicate is deleted.
#[tokio::test]
async fn merge_matched_delete_with_predicate() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (3, "cee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND s.name = 'bee' THEN DELETE",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".to_string()), (3, "c".to_string())]
    );
}

/// Clause declaration order is first-match-wins.
#[tokio::test]
async fn merge_clause_order_first_match_wins() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "x"), (3, "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND t.id = 2 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET name = 'u'",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".to_string()), (3, "u".to_string())]
    );
}

/// A target row matched by two source rows raises Spark's `MERGE_CARDINALITY_VIOLATION`.
#[tokio::test]
async fn merge_cardinality_violation_errors() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "p"), (2, "q")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string().contains("MERGE_CARDINALITY_VIOLATION"),
        "expected a cardinality violation, got: {err}"
    );
}

/// Insert-only MERGE takes the `fast_append` path.
#[tokio::test]
async fn merge_insert_only_appends_without_rewriting() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = id_file_pairs(&catalogs, "t").await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    let after = id_file_pairs(&catalogs, "t").await;
    for pair in &before {
        assert!(
            after.contains(pair),
            "pre-merge file for id {} must survive an insert-only merge",
            pair.0
        );
    }
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ]
    );
}

/// Copy-on-write granularity: only files containing a mutated row are rewritten.
#[tokio::test]
async fn merge_rewrites_only_affected_files() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(1, "one")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (10, 'j'), (11, 'k')",
    )
    .await;

    let before = id_file_pairs(&catalogs, "t").await;
    let touched: Vec<&String> = before
        .iter()
        .filter(|(id, _)| *id == 1)
        .map(|(_, file)| file)
        .collect();
    let untouched: Vec<&(i32, String)> = before
        .iter()
        .filter(|(_, file)| !touched.contains(&file))
        .collect();
    assert!(!untouched.is_empty(), "fixture needs an untouched file");

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;

    let after = id_file_pairs(&catalogs, "t").await;
    for pair in untouched {
        assert!(
            after.contains(pair),
            "file untouched by the merge must survive with its path: {pair:?}"
        );
    }
    for file in touched {
        assert!(
            !after.iter().any(|(_, f)| f == file),
            "the affected file must have been rewritten away: {file}"
        );
    }
    assert!(
        table_rows(&ctx, &catalogs, "ice.sales.t")
            .await
            .contains(&(1, "one".to_string()))
    );
}

/// MERGE into a just-created empty table (no snapshot yet): the whole source inserts.
#[tokio::test]
async fn merge_into_empty_table_inserts_all() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id INT NOT NULL, name STRING NOT NULL)",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(2, "bee".to_string())]
    );
}

/// Every MERGE commit is stamped with a unique `engine.operation-id` snapshot-summary property.
#[tokio::test]
async fn merge_stamps_operation_id_in_snapshot_summary() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;

    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let summary = table.metadata().current_snapshot().unwrap().summary();
    assert!(
        summary
            .additional_properties
            .contains_key(repark_iceberg::write::merge::OPERATION_ID_PROP),
        "MERGE snapshot summary must carry {}",
        repark_iceberg::write::merge::OPERATION_ID_PROP
    );
}

/// A bucket-partitioned merge-on-read MERGE through the SQL door matches Spark on the Arrow path.
#[tokio::test]
async fn merge_bucket_partitioned_mor_mode_runs_end_to_end() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // src = (1,'a'), (2,'b'), (3,'c').
    register_source(&ctx, "updates", &[(1, "drop"), (2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bkt_mor USING iceberg \
             PARTITIONED BY (bucket(4, id)) \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    // The MANIFEST-level file set, not the scan's `_file` column.
    let before = live_data_file_paths(&catalogs, "bkt_mor").await;
    assert!(!before.is_empty(), "the CTAS wrote at least one data file");

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.bkt_mor AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND s.name = 'drop' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.bkt_mor").await,
        vec![
            // id=1 is ABSENT — first-match-wins, as in the unpartitioned pin.
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
        "merge-on-read MERGE on a bucket-partitioned table must produce Spark's answer"
    );
    assert!(
        delete_file_count(&catalogs, "bkt_mor").await > 0,
        "the commit must carry position-delete files (a copy-on-write fallback carries none)"
    );
    let after = live_data_file_paths(&catalogs, "bkt_mor").await;
    assert!(
        before.is_subset(&after),
        "merge-on-read must leave every pre-merge data file live in the manifests: {before:?} \
             vs {after:?}"
    );
}

/// Runs merge-on-read end to end and requires position deletes without rewriting data files.
#[tokio::test]
async fn merge_merge_on_read_mode_runs_end_to_end() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // src = (1,'a'), (2,'b'), (3,'c').
    register_source(&ctx, "updates", &[(1, "drop"), (2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    // MANIFEST oracle, not the scan's `_file`.
    let before = live_data_file_paths(&catalogs, "t").await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND s.name = 'drop' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            // id=1 is ABSENT, and that absence is the first-match-wins proof.
            (2, "bee".to_string()), // the DELETE predicate did not hold ⇒ the UPDATE clause ran
            (3, "c".to_string()),   // untouched
            (4, "dee".to_string()), // not-matched INSERT
        ],
        "merge-on-read MERGE must produce Spark's answer — id=1 deleted, id=2 updated once"
    );
    assert!(
        delete_file_count(&catalogs, "t").await > 0,
        "the commit must carry position-delete files (a copy-on-write fallback carries none)"
    );
    let after = live_data_file_paths(&catalogs, "t").await;
    for file in &before {
        assert!(
            after.contains(file),
            "merge-on-read must leave every pre-merge data file live; `{file}` is gone"
        );
    }
}

/// An unparsable MERGE gets our targeted error, not DataFusion's opaque parse failure.
#[tokio::test]
async fn merge_unparsable_form_gets_targeted_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT ROW",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("MERGE INTO form"),
        "expected the targeted MERGE parse message, got: {err}"
    );
}

/// A MERGE carrying an MSSQL-style `OUTPUT` clause is rejected deterministically.
#[tokio::test]
async fn merge_output_clause_rejected() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE OUTPUT deleted.*",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("OUTPUT"),
        "expected the OUTPUT-clause rejection, got: {err}"
    );
}

/// A typo'd `UPDATE SET` column is an error, never a silent no-op.
#[tokio::test]
async fn merge_update_set_unknown_column_errors() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET nope_col = s.name",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("`nope_col` does not exist"),
        "expected the unknown-SET-column error, got: {err}"
    );
    // Case-differing spelling of a real column must APPLY, not refuse as unknown.
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET NAME = s.name",
    )
    .await;
    let got = table_rows(&ctx, &catalogs, "ice.sales.t").await;
    assert!(
        got.iter().any(|(id, name)| *id == 2 && name == "bee"),
        "case-insensitive SET NAME must update name, got {got:?}"
    );
}

/// Casefold-duplicate SET keys on the wire path must fail loud (not first-win).
#[tokio::test]
async fn merge_update_set_casefold_duplicate_errors_without_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = 'first', NAME = 'second'",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("more than once"),
        "expected casefold-duplicate SET error, got: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// Quoted, case-sensitive aliases survive lowering.
#[tokio::test]
async fn merge_quoted_alias_resolves() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS \"Tgt\" USING updates AS \"Src\" \
             ON \"Tgt\".id = \"Src\".id \
             WHEN MATCHED THEN UPDATE SET name = \"Src\".name",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
        ]
    );
}

/// A source that happens to carry a column named `__repark_matched` merges fine.
#[tokio::test]
async fn merge_source_with_reserved_flag_column_works() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("__repark_matched", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["bee"])),
            Arc::new(datafusion::arrow::array::BooleanArray::from(vec![false])),
        ],
    )
    .unwrap();
    ctx.register_batch("updates", batch).unwrap();
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
        ]
    );
}

// N-2b / G3 deferred cases: mirror Python corpus shapes that G-4's file ban blocked with N-2.

/// G3 pin 1 / N-2b.
#[tokio::test]
async fn merge_duplicate_source_keys_with_matched_raises() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Two source rows share id=2 — the match key — under a WHEN MATCHED UPDATE.
    register_source(&ctx, "updates", &[(2, "x"), (2, "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("MERGE_CARDINALITY_VIOLATION"),
        "expected MERGE_CARDINALITY_VIOLATION on duplicate source keys with MATCHED, got: {err}"
    );
    // Failed MERGE must leave the target untouched (no partial write).
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "cardinality failure must not mutate the target"
    );
}

/// G3 pin 2 / N-2b.
#[tokio::test]
async fn merge_duplicate_source_keys_insert_only_commits_both() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Two unmatched source rows share id=9; no WHEN MATCHED arm ⇒ both insert.
    register_source(&ctx, "updates", &[(9, "x"), (9, "y")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    let mut got = table_rows(&ctx, &catalogs, "ice.sales.t").await;
    // Order by (id, name) so the twin id=9 rows compare stably.
    got.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    assert_eq!(
        got,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (9, "x".to_string()),
            (9, "y".to_string()),
        ],
        "insert-only MERGE must commit BOTH duplicate-key source rows"
    );
}

/// G3 pin 3 / N-2b.
#[tokio::test]
async fn merge_matched_and_arm_order_update_then_delete() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.score_t (id INT NOT NULL, score INT NOT NULL) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.score_t VALUES (1, 10), (2, 20)",
    )
    .await;
    // Source carries the same ids with new scores (only the UPDATE arm uses source.score).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    ctx.register_batch("updates", batch).unwrap();

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.score_t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND t.score = 10 THEN UPDATE SET score = s.score \
             WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(
        score_table_rows(&ctx, &catalogs, "ice.sales.score_t").await,
        vec![(1, 100)],
        "id=1 updates (first arm); id=2 deletes (second arm) — first-match-wins"
    );
}

/// G3 pin 4 / N-2b.
#[tokio::test]
async fn merge_matched_and_threshold_update_or_delete() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.score_t (id INT NOT NULL, score INT NOT NULL) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.score_t VALUES (1, 10), (2, 20), (3, 5)",
    )
    .await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![100, 200, 50])),
        ],
    )
    .unwrap();
    ctx.register_batch("updates", batch).unwrap();

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.score_t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED AND t.score >= 15 THEN UPDATE SET score = s.score \
             WHEN MATCHED AND t.score < 15 THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, score) VALUES (s.id, s.score)",
    )
    .await;

    assert_eq!(
        score_table_rows(&ctx, &catalogs, "ice.sales.score_t").await,
        vec![(2, 200)],
        "id=2 (score>=15) updates; id=1 and id=3 (score<15) delete"
    );
}

// Spark-door MERGE lowering strictness pins.

/// M2 / r5 — Oracle-style `UPDATE SET … WHERE` refuses at the door (not silently dropped).
#[tokio::test]
async fn merge_oracle_style_update_where_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.r5_t (id INT NOT NULL, qty INT NOT NULL) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.r5_t VALUES (1, 100), (2, 200)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.r5_s (id INT NOT NULL, qty INT NOT NULL) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.r5_s VALUES (1, 0), (2, 7)",
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.r5_t AS t USING ice.sales.r5_s AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET qty = s.qty WHERE s.qty > 0",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("UPDATE SET … WHERE"),
        "expected Oracle-style UPDATE WHERE refusal, got: {err}"
    );
    assert_eq!(
        qty_table_rows(&ctx, &catalogs, "ice.sales.r5_t").await,
        vec![(1, 100), (2, 200)],
        "failed MERGE must leave rows untouched"
    );
}

/// M2 — Oracle-style `DELETE WHERE` on UPDATE SET refuses.
#[tokio::test]
async fn merge_oracle_style_delete_where_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name DELETE WHERE s.name = 'bee'",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("DELETE WHERE"),
        "expected Oracle-style DELETE WHERE refusal, got: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// M2 — Oracle-style `INSERT … WHERE` refuses.
#[tokio::test]
async fn merge_oracle_style_insert_where_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) WHERE s.id > 0",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("INSERT … WHERE"),
        "expected Oracle-style INSERT WHERE refusal, got: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// M3 / r7 — source-qualified SET target refuses and writes nothing.
#[tokio::test]
async fn merge_source_qualified_set_target_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(1, "new")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET s.name = 'hacked'",
    )
    .await
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("`s`"),
        "must name the received qualifier: {message}"
    );
    assert!(
        message.contains("target alias `t`"),
        "must name the target alias: {message}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// M3 — `t.addr.city` refuses with the nested-field needle.
#[tokio::test]
async fn merge_nested_field_set_target_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(1, "new")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.addr.city = s.name",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("nested-field assignment is not supported"),
        "expected nested-field refusal, got: {err}"
    );
}

/// M3 positive — `t.name = …` and bare `name = …` still lower and execute.
#[tokio::test]
async fn merge_target_qualified_and_bare_set_targets_execute() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
        ]
    );

    register_source(&ctx, "updates2", &[(3, "cee")]);
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates2 AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "cee".to_string()),
        ]
    );
}

/// M8 / r6 — column-list-less `INSERT VALUES` is refused with the ANSI needle.
#[tokio::test]
async fn merge_insert_without_column_list_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.r6_t (a STRING, b STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.r6_t VALUES ('a0', 'b0')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.r6_s (a STRING, b STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.r6_s VALUES ('a1', 'b1')",
    )
    .await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.r6_t AS t USING ice.sales.r6_s AS s ON t.a = s.a \
             WHEN NOT MATCHED THEN INSERT VALUES (s.b, s.a)",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("MERGE INSERT requires an explicit column list: INSERT (a, b) VALUES (…)"),
        "must copy the ANSI needle verbatim, got: {err}"
    );
}

/// M10 / r12 — unconditional MATCHED clause before a later MATCHED clause refuses.
#[tokio::test]
async fn merge_non_last_unconditional_matched_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(1, "b")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN MATCHED AND s.name = 'b' THEN UPDATE SET name = s.name",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION"),
        "expected Spark error-class wording, got: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// M10 — unconditional NOT MATCHED clause before a later NOT MATCHED clause refuses.
#[tokio::test]
async fn merge_non_last_unconditional_not_matched_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = table_rows(&ctx, &catalogs, "ice.sales.t").await;

    let err = execute(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
             WHEN NOT MATCHED AND s.name = 'dee' THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION"),
        "expected Spark error-class wording, got: {err}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        before,
        "failed MERGE must leave rows untouched"
    );
}

/// Read `(id, qty)` pairs sorted by id — local oracle for the M2 / r5 pin.
async fn qty_table_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, i32)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, qty FROM {table} ORDER BY id"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let quantities = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), quantities.value(index)));
        }
    }
    rows
}

/// Read `(id, score)` pairs sorted by id — local oracle for the two G3 score-arm pins.
async fn score_table_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, i32)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, score FROM {table} ORDER BY id"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), scores.value(index)));
        }
    }
    rows
}
