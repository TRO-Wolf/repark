//! MW-6 — `CALL <catalog>.system.rewrite_manifests`.
//!
//! Every count here is measured on a live Spark 4.0.1 + Iceberg 1.10.0 oracle (2026-08-23); the
//! schema is also the Iceberg 1.10.0 jar's `OUTPUT_TYPE` constant. The oracle scenarios and the
//! numbers they produced are in `task/ledgers/completed/mw-6-rewrite-manifests-ledger.md`.

use iceberg::spec::ManifestContentType;

use super::super::*;
use super::common::*;

/// Spark's two columns, in Spark's order, with Spark's types and nullability.
///
/// Read from the Iceberg 1.10.0 jar (`RewriteManifestsProcedure.OUTPUT_TYPE`: two
/// `IntegerType` `StructField`s, `iconst_0` nullable) and confirmed by executing the procedure
/// on the live 4.0.1 oracle, whose result schema JSON carries `"nullable":false` on both.
fn assert_rewrite_manifests_schema_is_sparks(batch: &datafusion::arrow::array::RecordBatch) {
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["rewritten_manifests_count", "added_manifests_count"]
    );
    let types: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    assert_eq!(
        types,
        vec![DataType::Int32, DataType::Int32],
        "Spark declares both columns int, not bigint"
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable()),
        "Spark declares both rewrite_manifests columns NON-nullable"
    );
}

/// The current snapshot's manifest counts: `(data at current spec, delete at current spec, total)`.
async fn manifest_shape(catalog: &dyn Catalog, ident: &TableIdent) -> (usize, usize, usize) {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return (0, 0, 0);
    };
    let spec_id = metadata.default_partition_spec_id();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut data = 0;
    let mut deletes = 0;
    let entries = manifest_list.entries();
    for manifest in entries {
        if manifest.partition_spec_id != spec_id {
            continue;
        }
        match manifest.content {
            ManifestContentType::Data => data += 1,
            ManifestContentType::Deletes => deletes += 1,
        }
    }
    (data, deletes, entries.len())
}

async fn snapshot_count(catalog: &dyn Catalog, ident: &TableIdent) -> usize {
    catalog
        .load_table(ident)
        .await
        .expect("load table")
        .metadata()
        .snapshots()
        .count()
}

fn sales(table: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), table.into())
}

/// Seed `appends` single-row appends, each of which commits its own data manifest.
async fn seed_appends(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str, appends: i32) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, v STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '2')"
        ),
    )
    .await;
    for id in 1..=appends {
        run(
            ctx,
            catalogs,
            &format!("INSERT INTO ice.sales.{table} VALUES ({id}, 'v{id}')"),
        )
        .await;
    }
}

/// MW-6: five data manifests become one, and the counts are Spark's.
///
/// Oracle — live Spark 4.0.1 + Iceberg 1.10.0, five single-row appends into an unpartitioned v2
/// table:
///
/// ```text
/// manifests 5 → 1   rewritten_manifests_count=5   added_manifests_count=1
/// new snapshot summary: manifests-replaced=5  manifests-created=1  manifests-kept=0
/// ```
///
/// pins: mw-6-rewrite-manifests/C-001, C-002, C-003
#[tokio::test]
async fn call_rewrite_manifests_compacts_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_appends(&ctx, &catalogs, "man", 5).await;
    let ident = sales("man");
    let before = manifest_shape(catalogs["ice"].as_ref(), &ident).await;
    assert_eq!(
        before,
        (5, 0, 5),
        "fixture must strand five data manifests, else the rewrite below proves nothing"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.man").await;

    let result = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.man')",
    )
    .await
    .expect("rewrite_manifests CALL");
    let batches = result.collect().await.expect("collect result");
    let batch = &batches[0];
    assert_rewrite_manifests_schema_is_sparks(batch);
    assert_eq!(
        call_manifest_count(batch, "rewritten_manifests_count"),
        5,
        "Spark rewrote all five"
    );
    assert_eq!(
        call_manifest_count(batch, "added_manifests_count"),
        1,
        "Spark wrote one manifest — the whole table fits the 8 MB target"
    );

    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &ident).await,
        (1, 0, 1),
        "five manifests became one"
    );
    // A manifest rewrite re-groups entries; it never changes which rows are live.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.man").await,
        live_before,
        "the live row set must survive the rewrite"
    );
}

/// MW-6: nothing to rewrite is two zeros and NO new snapshot.
///
/// Oracle — live Spark 4.0.1: a second call on the freshly rewritten table returns `0, 0` and the
/// snapshot list does not grow. Spark's rule is `targetNumManifests == 1 && matching.size() == 1`.
/// Without the guard the fork rewrites the single manifest into itself and answers `1, 1`.
///
/// pins: mw-6-rewrite-manifests/C-004
#[tokio::test]
async fn call_rewrite_manifests_no_op_returns_zeros_and_commits_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_appends(&ctx, &catalogs, "noop", 5).await;
    let ident = sales("noop");
    run(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.noop')",
    )
    .await;
    let snapshots_before = snapshot_count(catalogs["ice"].as_ref(), &ident).await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.noop')",
    )
    .await
    .expect("second rewrite_manifests CALL")
    .collect()
    .await
    .expect("collect result");
    let batch = &batches[0];
    assert_rewrite_manifests_schema_is_sparks(batch);
    assert_eq!(call_manifest_count(batch, "rewritten_manifests_count"), 0);
    assert_eq!(call_manifest_count(batch, "added_manifests_count"), 0);
    assert_eq!(
        snapshot_count(catalogs["ice"].as_ref(), &ident).await,
        snapshots_before,
        "Spark commits no snapshot for a no-op rewrite"
    );
}

/// MW-6: a table with no snapshot answers zeros, where the fork action errors.
///
/// Oracle — live Spark 4.0.1: `CALL system.rewrite_manifests` on an empty table returns `0, 0`
/// and creates no snapshot. The fork's action fails `DataInvalid` on the same table, so this pin
/// holds the guard that runs before it.
///
/// pins: mw-6-rewrite-manifests/C-009
#[tokio::test]
async fn call_rewrite_manifests_on_a_table_with_no_snapshot_returns_zeros() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fresh (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.fresh')",
    )
    .await
    .expect("rewrite_manifests on an empty table must not error")
    .collect()
    .await
    .expect("collect result");
    let batch = &batches[0];
    assert_rewrite_manifests_schema_is_sparks(batch);
    assert_eq!(call_manifest_count(batch, "rewritten_manifests_count"), 0);
    assert_eq!(call_manifest_count(batch, "added_manifests_count"), 0);
    assert_eq!(
        snapshot_count(catalogs["ice"].as_ref(), &sales("fresh")).await,
        0,
        "the guard must not commit a snapshot"
    );
}

/// Seed a merge-on-read table whose DATA manifest count stays at one while delete manifests pile
/// up: one append, then `deletes` merge-on-read `DELETE`s, each writing only a position delete.
async fn seed_delete_manifests(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    deletes: i32,
) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT, v STRING) USING iceberg TBLPROPERTIES \
             ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')"
        ),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!(
            "INSERT INTO ice.sales.{table} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"
        ),
    )
    .await;
    for id in 1..=deletes {
        run(
            ctx,
            catalogs,
            &format!("DELETE FROM ice.sales.{table} WHERE id = {id}"),
        )
        .await;
    }
}

/// MW-6 / registry `MANIFEST-1`: zeros refuse while delete manifests stay uncompacted.
///
/// Oracle — live Spark 4.0.1, one data manifest plus delete manifests: Spark's second leg rewrites
/// the delete manifests (measured `2, 1` on a 1-data + 2-delete table). The fork carries every
/// delete manifest forward unchanged, so this engine could only answer `0, 0` — which reads as
/// "nothing to compact" while the delete manifests sit in the table. It refuses instead.
///
/// pins: mw-6-rewrite-manifests/C-005
#[tokio::test]
async fn call_rewrite_manifests_refuses_zeros_while_delete_manifests_stay() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_delete_manifests(&ctx, &catalogs, "del_man", 3).await;
    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &sales("del_man")).await,
        (1, 3, 4),
        "fixture must hold ONE data manifest and three delete manifests"
    );

    let error = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.del_man')",
    )
    .await
    .expect_err("zeros must refuse while Spark would compact the delete manifests");
    let message = error.to_string();
    assert!(
        message.contains("3 delete manifest"),
        "the refusal must count the delete manifests, got: {message}"
    );
}

/// MW-6 / registry `MANIFEST-1`: a working data leg runs and leaves the delete manifests.
///
/// Oracle — live Spark 4.0.1, five data manifests plus three delete manifests: `8, 2` and both
/// legs compacted. This engine reports its data leg only and the delete manifests stay, which is
/// the disclosure the registry row carries. The row set is identical either way.
///
/// pins: mw-6-rewrite-manifests/C-010
#[tokio::test]
async fn call_rewrite_manifests_reports_the_data_leg_and_leaves_delete_manifests() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mor_man (id INT, v STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mor_man VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    for id in 1..=3 {
        run(
            &ctx,
            &catalogs,
            &format!(
                "MERGE INTO ice.sales.mor_man AS t USING (SELECT {id} AS id, 'm{id}' AS v) AS s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v"
            ),
        )
        .await;
    }
    let ident = sales("mor_man");
    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &ident).await,
        (4, 3, 7),
        "each merge-on-read MERGE adds one data manifest and one delete manifest"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor_man").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.mor_man')",
    )
    .await
    .expect("rewrite_manifests CALL")
    .collect()
    .await
    .expect("collect result");
    let batch = &batches[0];
    assert_eq!(
        call_manifest_count(batch, "rewritten_manifests_count"),
        4,
        "the data leg only — Spark counts its delete leg here too"
    );
    assert_eq!(call_manifest_count(batch, "added_manifests_count"), 1);
    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &ident).await,
        (1, 3, 4),
        "the three delete manifests are carried forward untouched"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.mor_man").await,
        live_before,
        "the live row set must survive the rewrite"
    );
}

/// MW-6: only the CURRENT partition spec's manifests are rewritten, which is Spark's default.
///
/// Oracle — live Spark 4.0.1, a table appended twice, `ALTER TABLE … ADD PARTITION FIELD grp`,
/// then appended three more times: `rewritten_manifests_count=3`, `added_manifests_count=1`, and
/// the old-spec manifest survives (`manifests-kept=1`). `RewriteManifestsSparkAction` filters
/// `manifest.partitionSpecId() == spec.specId()` and its `spec` defaults to the current one.
///
/// pins: mw-6-rewrite-manifests/C-006
#[tokio::test]
async fn call_rewrite_manifests_rewrites_only_the_current_spec() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.evolve (id INT, grp STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    for id in 1..=2 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.evolve VALUES ({id}, 'a')"),
        )
        .await;
    }
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.evolve ADD PARTITION FIELD grp",
    )
    .await;
    for id in 3..=5 {
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.evolve VALUES ({id}, 'b')"),
        )
        .await;
    }
    let ident = sales("evolve");
    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &ident).await,
        (3, 0, 5),
        "fixture must hold three current-spec manifests and two at the old spec"
    );
    let live_before = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.evolve").await;

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.evolve')",
    )
    .await
    .expect("rewrite_manifests CALL")
    .collect()
    .await
    .expect("collect result");
    let batch = &batches[0];
    assert_eq!(
        call_manifest_count(batch, "rewritten_manifests_count"),
        3,
        "only the current spec's three manifests are rewritten"
    );
    assert_eq!(call_manifest_count(batch, "added_manifests_count"), 1);

    assert_eq!(
        manifest_shape(catalogs["ice"].as_ref(), &ident).await,
        (1, 0, 3),
        "the current spec merged to one manifest; both old-spec manifests are kept, as Spark \
         keeps them"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.evolve").await,
        live_before,
        "the live row set must survive the rewrite"
    );
}

/// MW-6: `spec_id` refuses loud; `use_caching` is accepted and changes nothing.
///
/// Oracle — live Spark 4.0.1 on a five-manifest table: `use_caching => true` and
/// `use_caching => false` both answered `5, 1`, the same as the bare call, so the option is a
/// Spark-side `DataFrame` cache and not a behaviour. `spec_id => 0` also answered `5, 1` there;
/// this engine refuses the argument instead of accepting one value of it (registry `MANIFEST-2`).
///
/// pins: mw-6-rewrite-manifests/C-007, C-008
#[tokio::test]
async fn call_rewrite_manifests_argument_surface_is_sparks() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_appends(&ctx, &catalogs, "args", 5).await;

    let refused = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.args', spec_id => 0)",
    )
    .await
    .expect_err("spec_id must refuse");
    assert!(
        refused.to_string().contains("spec_id"),
        "the refusal must name spec_id, got: {refused}"
    );
    // Positional #2 is the same argument, so it must refuse the same way.
    assert!(
        execute(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests('sales.args', true, 0)",
        )
        .await
        .is_err(),
        "positional spec_id must refuse too"
    );
    // A non-boolean use_caching refuses exactly as Spark's typed parameter does.
    assert!(
        execute(
            &ctx,
            &catalogs,
            "CALL ice.system.rewrite_manifests(table => 'sales.args', use_caching => 'yes')",
        )
        .await
        .is_err(),
        "use_caching takes a boolean literal"
    );

    let batches = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_manifests(table => 'sales.args', use_caching => true)",
    )
    .await
    .expect("use_caching is accepted")
    .collect()
    .await
    .expect("collect result");
    let batch = &batches[0];
    assert_rewrite_manifests_schema_is_sparks(batch);
    assert_eq!(
        call_manifest_count(batch, "rewritten_manifests_count"),
        5,
        "use_caching changes nothing about the answer"
    );
    assert_eq!(call_manifest_count(batch, "added_manifests_count"), 1);
}

/// Read an `Int32` result column as `i64` (the module's own copy of `call.rs`'s reader, which is
/// private to that module).
fn call_manifest_count(batch: &datafusion::arrow::array::RecordBatch, name: &str) -> i64 {
    let index = batch.schema().index_of(name).expect("column present");
    i64::from(
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 count column")
            .value(0),
    )
}
