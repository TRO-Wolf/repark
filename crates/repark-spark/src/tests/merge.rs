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

/// The classic upsert (the source publish job's MERGE shape): matched rows take the source
/// values, unmatched source rows are inserted, untouched target rows survive — one commit.
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

/// The literal source publish job MERGE text — `UPDATE SET *` / `INSERT *` — end to end:
/// stars expand to every target column by name from the source (extra source columns
/// ignored), producing the same upsert as the explicit form.
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

/// A star whose source cannot provide every target column errors up front, naming the
/// missing column — never a silent NULL-fill.
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

/// `WHEN MATCHED AND <cond> THEN DELETE`: only the row passing the clause predicate is
/// deleted; a matched row failing it survives byte-identical (matched-but-no-clause path).
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

/// Clause declaration order is first-match-wins (Spark): a row captured by the first clause
/// never reaches the second, even when both predicates hold.
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

/// A target row matched by two source rows raises Spark's `MERGE_CARDINALITY_VIOLATION`
/// instead of picking one arbitrarily.
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

/// Insert-only MERGE takes the `fast_append` path: no target file is rewritten (every
/// pre-merge `(id, file)` pair survives identically) and only the new row is added.
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

/// Copy-on-write granularity: only files containing a mutated row are rewritten; a second
/// file whose rows the MERGE never touches keeps its exact path across the commit.
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

/// Every MERGE commit is stamped with a unique `engine.operation-id` snapshot-summary
/// property — the fork `ENGINE_CONTRACT` §8 ambiguous-commit mitigation.
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

/// PIN Y1-SQL (gate-retirement, second generation) — the lineage here is three deep:
/// `merge_non_identity_partition_transform_rejected` (retired by Group R when transform
/// copy-on-write MERGE landed) → `merge_bucket_partitioned_mor_mode_still_rejected` (R5/Group T:
/// merge-on-read RAN, but not on a transform table) → THIS, because Group Y proved and enabled
/// the composition. A `bucket(4, id)` + `write.merge.mode = 'merge-on-read'` table now RUNS a
/// three-clause MERGE end-to-end through the real user surface (CTAS `TBLPROPERTIES` → the SQL
/// MERGE router → the merge-on-read arm), producing Spark's answer on the Arrow path.
///
/// The physical assertions are the load-bearing half — routing this table to the copy-on-write
/// arm produces the SAME three rows: position-delete files MUST be committed, and EVERY
/// pre-merge data file must still be live.
#[tokio::test]
async fn merge_bucket_partitioned_mor_mode_runs_end_to_end() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // src = (1,'a'), (2,'b'), (3,'c'). Drop id=1, update id=2, insert id=4.
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
    // The MANIFEST-level file set, not the scan's `_file` column: this MERGE empties the
    // `bucket(4, 1) == bucket(4, 2)` file of every visible row, so a scan-based oracle would
    // call a perfectly correct merge-on-read commit a rewrite.
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

/// PIN T (SQL entry point) — `write.merge.mode = 'merge-on-read'` now RUNS end-to-end through
/// the real user surface: CTAS `TBLPROPERTIES` → the SQL MERGE router → the merge-on-read arm.
/// A three-clause MERGE (DELETE / UPDATE / INSERT) reads back exactly Spark's answer on the
/// Arrow path, position-delete files are committed, and EVERY pre-merge data file is still live
/// (merge-on-read never rewrites). This test REPLACES the retired `merge_mor_mode_rejected`
/// v1-limit pin — the limit it guarded is what Group T removed.
///
/// The `_file`-survival and delete-file assertions are the load-bearing half: routing this
/// table to the copy-on-write arm yields the SAME three rows and would pass a rows-only pin.
#[tokio::test]
async fn merge_merge_on_read_mode_runs_end_to_end() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // src = (1,'a'), (2,'b'), (3,'c'). Drop id=1, update id=2, insert id=4.
    register_source(&ctx, "updates", &[(1, "drop"), (2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') \
             AS SELECT * FROM src",
    )
    .await;
    // MANIFEST oracle, not the scan's `_file` (C-Y-1): a file whose every row is
    // position-deleted vanishes from `_file` while staying live in the manifests, so the
    // scan-based `before ⊆ after` would FALSE-RED on a correct MoR commit (and a pre-emptied
    // file absent from `before` would let a COW rewrite of it slip). `live_data_file_paths`
    // reads the manifests directly and closes both directions.
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
            // id=1 is ABSENT, and that absence is the first-match-wins proof: BOTH matched
            // clauses applied to it (the DELETE's `s.name = 'drop'` held AND the UPDATE is
            // unconditional), so only declaration order decides. Last-match-wins would have
            // left `(1, "drop")` here.
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

/// An unparsable MERGE (`BigQuery`'s `INSERT ROW`, which the Databricks dialect rejects)
/// gets OUR targeted error, not DataFusion's opaque parse failure from the passthrough.
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

/// A MERGE carrying an MSSQL-style `OUTPUT` clause (sqlparser parses it; we cannot honour
/// it) is rejected deterministically instead of executing the write and dropping the output.
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

/// A typo'd `UPDATE SET` column is an ERROR, never a silent no-op (audit BUG-006:
/// case-insensitive resolution still refuses names that match no schema field).
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
    // Case-differing spelling of a real column must APPLY (Spark caseSensitive=false), not
    // refuse as unknown — the prior exact-case pin used `NAME` and is now wrong.
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

/// Critic-octo P3C1-Q-001: casefold-duplicate SET keys on the wire path must fail loud
/// (not first-win). Deleting `validate_update_columns` in `execute_merge` must RED this pin.
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

/// Quoted, case-sensitive aliases survive lowering: the alias declaration in the generated
/// FROM keeps its quoting, so the user's quoted references in ON/SET resolve.
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

/// A source that happens to carry a column named `__repark_matched` merges fine — the match
/// sentinel is UUID-suffixed per execution, so no fixed source column can collide with it.
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
