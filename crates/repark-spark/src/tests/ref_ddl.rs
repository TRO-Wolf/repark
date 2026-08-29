/// CREATE OR REPLACE and bare REPLACE BRANCH|TAG preserve snapshot identities.
use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // create → replace → or-replace → tag matrix + id asserts
async fn branch_tag_replace_and_or_replace_round_trip() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let s1 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("CTAS snapshot");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let s2 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("insert snapshot");
    assert_ne!(s1, s2);

    // CREATE OR REPLACE when absent = create at s1.
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t CREATE OR REPLACE BRANCH audit AS OF VERSION {s1}"),
    )
    .await;
    let ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'audit' ORDER BY id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3], "OR REPLACE create pins s1");

    // Bare REPLACE re-pins to s2.
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t REPLACE BRANCH audit AS OF VERSION {s2}"),
    )
    .await;
    let ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'audit' ORDER BY id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4], "REPLACE re-pins to s2");

    // CREATE OR REPLACE when present = replace back to s1.
    run(
        &ctx,
        &catalogs,
        &format!("CREATE OR REPLACE BRANCH audit IN ice.sales.t AS OF VERSION {s1}"),
    )
    .await;
    let ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'audit' ORDER BY id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3], "OR REPLACE existing re-pins s1");

    // Tag replace path.
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t CREATE TAG t1 AS OF VERSION {s1}"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t REPLACE TAG t1 AS OF VERSION {s2}"),
    )
    .await;
    let tag_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 't1' ORDER BY id",
    )
    .await;
    assert_eq!(tag_ids, vec![1, 2, 3, 4], "REPLACE TAG re-pins s2");

    // Snapshot-id assert on the ref itself (not only row multiset).
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap();
    let audit_id = table
        .metadata()
        .snapshot_for_ref("audit")
        .expect("audit ref")
        .snapshot_id();
    let tag_id = table
        .metadata()
        .snapshot_for_ref("t1")
        .expect("t1 ref")
        .snapshot_id();
    assert_eq!(audit_id, s1, "audit branch snapshot_id after OR REPLACE");
    assert_eq!(tag_id, s2, "t1 tag snapshot_id after REPLACE");
}

/// RETAIN and WITH SNAPSHOT RETENTION populate the fork's `SnapshotRetention` fields.
#[tokio::test]
async fn branch_retention_clauses_round_trip() {
    use datafusion::arrow::array::{Array, AsArray};

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
        "ALTER TABLE ice.sales.t CREATE BRANCH keep RETAIN 7 DAYS \
             WITH SNAPSHOT RETENTION 3 SNAPSHOTS",
    )
    .await;
    // Tag RETAIN only.
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t CREATE TAG pin RETAIN 24 HOURS",
    )
    .await;

    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT name, type, max_reference_age_in_ms, min_snapshots_to_keep \
             FROM ice.sales.t.refs WHERE name IN ('keep', 'pin') ORDER BY name",
    )
    .await
    .expect("refs metadata")
    .collect()
    .await
    .unwrap();
    assert!(!batches.is_empty());
    let batch = &batches[0];
    let names = batch.column(0).as_string::<i32>();
    let types = batch.column(1).as_string::<i32>();
    let max_ref_age = batch
        .column(2)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    let min_snaps = batch
        .column(3)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    // ORDER BY name → keep, pin
    assert_eq!(names.value(0), "keep");
    assert_eq!(types.value(0), "BRANCH");
    assert_eq!(max_ref_age.value(0), 7 * 86_400_000);
    assert_eq!(min_snaps.value(0), 3);
    assert_eq!(names.value(1), "pin");
    assert_eq!(types.value(1), "TAG");
    assert_eq!(max_ref_age.value(1), 24 * 3_600_000);
    assert!(min_snaps.is_null(1), "tag has no min_snapshots_to_keep");
}

/// Write-to-branch targets refuse because commits are MAIN_BRANCH-only.
#[tokio::test]
async fn write_to_branch_refuses_loud_naming_fork_gap() {
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
        "ALTER TABLE ice.sales.t CREATE BRANCH audit",
    )
    .await;
    for sql in [
        "INSERT INTO ice.sales.t.audit SELECT 9 AS id, 'z' AS name",
        "INSERT INTO ice.sales.t.branch_audit SELECT 9 AS id, 'z' AS name",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("write-to-branch must STOP");
        let message = err.to_string();
        assert!(
            message.contains("MAIN_BRANCH") || message.contains("to_branch"),
            "must name fork gap for {sql:?}, got: {message}"
        );
        assert!(
            message.contains("not supported") || message.contains("NotImplemented"),
            "must be NotImplemented for {sql:?}, got: {message}"
        );
    }
    // Main-branch insert still works; audit ref still at CTAS snapshot (3 rows).
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS name",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
    let audit_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'audit' ORDER BY id",
    )
    .await;
    assert_eq!(
        audit_ids,
        vec![1, 2, 3],
        "refused write-to-branch must not advance the branch"
    );
}

/// DECLARED-DIVERGENCE pin for **`docs/spark-sql-iceberg-parity.md` §2.2 row REF-2** — the
/// idempotent `IF EXISTS` / `IF NOT EXISTS` spellings, and every other trailing clause, stay out
/// of the snapshot-ref DDL surface and refuse LOUD.
///
/// The divergence: Apache Spark's Iceberg extension accepts `CREATE BRANCH IF NOT EXISTS` and
/// `DROP BRANCH IF EXISTS`; this door refuses them. What the row exists to prevent is the
/// fail-open alternative — silently DROPPING the trailing clause, which inverts the statement's
/// meaning in both directions (an ignored `IF NOT EXISTS` turns a no-op into a hard failure; an
/// ignored `IF EXISTS` turns a tolerated miss into one).
///
/// This pin reds if the divergence silently disappears: every spelling below must still be an
/// error, and the three `ALTER TABLE` forms must still name the registry section in the message, so
/// implementing the forms (or dropping the doc pointer) forces this test and the registry row to
/// move together.
///
/// The leftover-token assertion binds the **dynamic** `(got word "…")` span, never the bare word:
/// the message's constant tail already contains `NOT` and `EXISTS`, so a bare `contains(leftover)`
/// would be satisfied by the static grammar text and would stay green if the interpolation were
/// deleted. Asserting the rendered span means redacting the interpolation reds every case.
#[tokio::test]
async fn ref_ddl_if_exists_spellings_and_trailing_clauses_refuse_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.refdecl AS SELECT * FROM src",
    )
    .await;

    // The ALTER TABLE forms reach the ref-DDL parser's trailing-token rejection, so their message
    // carries the supported grammar AND the registry pointer.
    for (sql, leftover) in [
        (
            "ALTER TABLE ice.sales.refdecl CREATE BRANCH IF NOT EXISTS b1",
            "NOT",
        ),
        (
            "ALTER TABLE ice.sales.refdecl DROP BRANCH IF EXISTS b1",
            "EXISTS",
        ),
        (
            "ALTER TABLE ice.sales.refdecl CREATE TAG t1 AS OF VERSION 1 RETAIN 7 DAYS EXTRA",
            "EXTRA",
        ),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a trailing clause must refuse, never be dropped silently")
            .to_string();
        assert!(
            error.contains("trailing clause after the supported form"),
            "the refusal must name the trailing clause ({sql}): {error}"
        );
        let rendered_leftover = format!("(got word {leftover:?})");
        assert!(
            error.contains(&rendered_leftover),
            "the refusal must name the leftover token in its dynamic slot, \
             {rendered_leftover} ({sql}): {error}"
        );
        assert!(
            error.contains("IF EXISTS / IF NOT EXISTS stay out"),
            "the refusal must name the known-but-unsupported spellings ({sql}): {error}"
        );
        assert!(
            error.contains("docs/spark-sql-iceberg-parity.md §2.2"),
            "the refusal must cite the registry row it defends ({sql}): {error}"
        );
    }

    // The top-level `… IN t` spellings break on the same clause earlier, in the `IN` requirement —
    // still a loud refusal naming the supported shape, never a silent create/drop.
    for sql in [
        "CREATE BRANCH IF NOT EXISTS b2 IN ice.sales.refdecl",
        "DROP TAG IF EXISTS t2 IN ice.sales.refdecl",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("the top-level IF-EXISTS spellings must refuse too")
            .to_string();
        assert!(
            error.contains("IN catalog.namespace.table"),
            "the refusal must name the supported shape ({sql}): {error}"
        );
    }

    // …and nothing was created or dropped by any of the refused statements.
    let refs = rows(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.refdecl.refs WHERE name <> 'main'",
    )
    .await;
    assert_eq!(refs, 0, "a refused ref DDL must not create or drop a ref");
}

/// A real two-part table literally named `branch_*` must not
/// false-refuse as write-to-branch; the `t.branch_x` form with a resolvable bare prefix
/// still STOPs loud (disambiguation by resolution, not raw-SQL shape).
#[tokio::test]
async fn two_part_branch_named_table_write_disambiguates_by_resolution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let mem_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int64,
            false,
        ),
    ]));
    let seed = RecordBatch::try_new(
        mem_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("seed batch");
    let branch_daily =
        datafusion::datasource::MemTable::try_new(mem_schema.clone(), vec![vec![seed]])
            .expect("mem table");
    ctx.register_table("branch_daily", Arc::new(branch_daily))
        .expect("register branch_daily");
    // Full two-part name resolves (default catalog `public` schema) → normal write path.
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO public.branch_daily SELECT 2 AS id",
    )
    .await
    .expect("real schema.branch_* table must not hit the write-to-branch refusal")
    .collect()
    .await
    .expect("insert into branch_daily");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM public.branch_daily").await,
        2,
        "insert must land in the real branch_daily table"
    );
    // Bare-prefix form still refuses loud once the prefix resolves as a table.
    let bare_t =
        datafusion::datasource::MemTable::try_new(mem_schema.clone(), vec![vec![]]).expect("mem t");
    ctx.register_table("t", Arc::new(bare_t))
        .expect("register t");
    let err = execute(&ctx, &catalogs, "INSERT INTO t.branch_audit SELECT 3 AS id")
        .await
        .expect_err("t.branch_x with a real bare prefix must STOP");
    let message = err.to_string();
    assert!(
        message.contains("MAIN_BRANCH") || message.contains("to_branch"),
        "must name the fork gap, got: {message}"
    );
    // Neither name resolving → planning's own error, NOT the branch refusal.
    let err = execute(
        &ctx,
        &catalogs,
        "INSERT INTO nosuch.branch_thing SELECT 4 AS id",
    )
    .await
    .expect_err("unresolvable target must still error");
    let message = err.to_string();
    assert!(
        !message.contains("MAIN_BRANCH") && !message.contains("to_branch"),
        "unresolvable two-part target must fall through to planning error, got: {message}"
    );
}

/// CREATE/DROP BRANCH|TAG via DDL, then time-travel read through the DDL-created ref.
/// Fork: `manage_snapshots.rs:90-145` (`create_branch` / `create_tag` / `remove_*`).
#[tokio::test]
async fn branch_tag_ddl_create_drop_round_trip() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    // Snapshot after CTAS (3 rows).
    let s1 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("CTAS creates a snapshot");

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let s2 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("insert snapshot");
    assert_ne!(s1, s2);

    // CREATE TAG at s1 via ALTER TABLE … AS OF VERSION.
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.t CREATE TAG tag_s1 AS OF VERSION {s1}"),
    )
    .await;
    // CREATE BRANCH at s2 via top-level CREATE … IN form.
    run(
        &ctx,
        &catalogs,
        &format!("CREATE BRANCH branch_s2 IN ice.sales.t AS OF VERSION {s2}"),
    )
    .await;

    let tag_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'tag_s1' ORDER BY id",
    )
    .await;
    assert_eq!(tag_ids, vec![1, 2, 3], "tag_s1 must pin CTAS snapshot");

    let branch_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'branch_s2' ORDER BY id",
    )
    .await;
    assert_eq!(
        branch_ids,
        vec![1, 2, 3, 4],
        "branch_s2 must pin insert snapshot"
    );

    // DROP via ALTER TABLE.
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t DROP TAG tag_s1").await;
    let err = execute(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.t VERSION AS OF 'tag_s1'",
    )
    .await
    .expect_err("dropped tag must not resolve");
    assert!(
        err.to_string().contains("tag_s1") || err.to_string().contains("unknown"),
        "got: {err}"
    );

    run(&ctx, &catalogs, "DROP BRANCH branch_s2 IN ice.sales.t").await;

    // Current read unaffected.
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
}

/// Ref DDL edge matrix — default AS OF = current, empty needs AS OF,
/// unknown snapshot / DROP main / kind mismatch refuse loud (wrong-target / wrong-snapshot).
#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat edge matrix of AS OF / DROP-target pins
async fn branch_tag_ddl_edge_matrix_as_of_and_drop_targets() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    // Schema-only empty: CREATE BRANCH without AS OF must refuse.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empty_ref (id BIGINT) USING iceberg",
    )
    .await;
    let empty_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.empty_ref CREATE BRANCH b1",
    )
    .await
    .expect_err("empty schema-only needs AS OF VERSION");
    assert!(
        empty_err.to_string().contains("AS OF VERSION"),
        "got: {empty_err}"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ref_edge AS SELECT * FROM src",
    )
    .await;
    let s1 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ref_edge".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("CTAS snapshot");
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ref_edge SELECT 9 AS id, 'z' AS name",
    )
    .await;
    let s2 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ref_edge".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("insert snapshot");
    assert_ne!(s1, s2);

    // Default (no AS OF) → current snapshot multiset.
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge CREATE BRANCH cur_default",
    )
    .await;
    let default_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.ref_edge VERSION AS OF 'cur_default' ORDER BY id",
    )
    .await;
    assert_eq!(
        default_ids,
        vec![1, 2, 3, 9],
        "CREATE BRANCH without AS OF must pin current snapshot"
    );

    // Explicit older AS OF still works (wrong-snapshot attack control).
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.ref_edge CREATE TAG old_s1 AS OF VERSION {s1}"),
    )
    .await;
    let old_ids = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.ref_edge VERSION AS OF 'old_s1' ORDER BY id",
    )
    .await;
    assert_eq!(old_ids, vec![1, 2, 3]);

    let unknown = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge CREATE BRANCH bad AS OF VERSION 999999999",
    )
    .await
    .expect_err("unknown snapshot");
    assert!(
        unknown.to_string().contains("999999999") || unknown.to_string().contains("not found"),
        "got: {unknown}"
    );

    let drop_main = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge DROP BRANCH main",
    )
    .await
    .expect_err("DROP main must refuse");
    assert!(drop_main.to_string().contains("main"), "got: {drop_main}");

    let kind_mismatch = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge DROP BRANCH old_s1",
    )
    .await
    .expect_err("DROP BRANCH on TAG must refuse");
    assert!(
        kind_mismatch.to_string().contains("tag") || kind_mismatch.to_string().contains("branch"),
        "got: {kind_mismatch}"
    );
    // Inverse kind mismatch: DROP TAG on a BRANCH.
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.ref_edge CREATE BRANCH br_kind AS OF VERSION {s2}"),
    )
    .await;
    let tag_on_branch = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge DROP TAG br_kind",
    )
    .await
    .expect_err("DROP TAG on BRANCH must refuse");
    assert!(
        tag_on_branch.to_string().contains("branch") || tag_on_branch.to_string().contains("tag"),
        "got: {tag_on_branch}"
    );
    // Tag still resolvable after kind-mismatch DROP attempt (not orphaned/deleted).
    let still = time_travel_id_multiset(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.ref_edge VERSION AS OF 'old_s1' ORDER BY id",
    )
    .await;
    assert_eq!(still, vec![1, 2, 3], "failed DROP must not remove the tag");

    // Duplicate CREATE BRANCH + DROP missing refuse.
    let dup = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge CREATE BRANCH cur_default",
    )
    .await
    .expect_err("duplicate branch");
    assert!(
        dup.to_string().contains("already exists") || dup.to_string().contains("cur_default"),
        "got: {dup}"
    );
    let missing = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ref_edge DROP TAG missing_tag_xyz",
    )
    .await
    .expect_err("missing tag");
    assert!(
        missing.to_string().contains("does not exist")
            || missing.to_string().contains("missing_tag_xyz"),
        "got: {missing}"
    );

    // CREATE BRANCH at older snapshot must not move main/current.
    let before_main = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ref_edge".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id();
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.ref_edge CREATE BRANCH side_old AS OF VERSION {s1}"),
    )
    .await;
    let after_main = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ref_edge".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        before_main, after_main,
        "CREATE BRANCH must not move the table's current snapshot"
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.ref_edge ORDER BY id"
        )
        .await,
        vec![1, 2, 3, 9]
    );
}

/// BRANCH sniff must not treat multipart table-name segments as DDL verbs.
///
/// Skip multipart table-name segments while matching true branch or tag DDL verbs.
#[test]
fn branch_sniff_skips_table_name_segments() {
    assert!(
        !starts_with_branch_or_tag_ddl(
            "ALTER TABLE ice.create.branch SET TBLPROPERTIES ('x' = 'y')"
        ),
        "table name create.branch must not look like BRANCH DDL"
    );
    assert!(
        !starts_with_branch_or_tag_ddl("ALTER TABLE ice.drop.tag SET LOCATION 's3://x'"),
        "table name drop.tag must not look like BRANCH DDL"
    );
    assert!(
        !starts_with_branch_or_tag_ddl("ALTER TABLE ice.replace.tag UNSET TBLPROPERTIES ('a')"),
        "table name replace.tag must not look like BRANCH DDL"
    );
    assert!(
        !starts_with_branch_or_tag_ddl("ALTER TABLE ice.sales.t RENAME TO create.branch"),
        "RENAME TO create.branch must not look like BRANCH DDL"
    );
    assert!(
        starts_with_branch_or_tag_ddl("ALTER TABLE ice.sales.t CREATE BRANCH audit"),
        "true positive CREATE BRANCH after table name"
    );
    assert!(
        starts_with_branch_or_tag_ddl("ALTER TABLE ice.sales.t REPLACE BRANCH audit"),
        "true positive REPLACE BRANCH after table name"
    );
    assert!(
        starts_with_branch_or_tag_ddl("ALTER TABLE ice.sales.t CREATE OR REPLACE BRANCH audit"),
        "true positive CREATE OR REPLACE BRANCH after table name"
    );
    assert!(
        starts_with_branch_or_tag_ddl("ALTER TABLE ice.create.branch CREATE BRANCH audit"),
        "true BRANCH DDL even when the table name itself contains create.branch"
    );
    assert!(
        starts_with_branch_or_tag_ddl("CREATE BRANCH audit IN ice.sales.t"),
        "top-level CREATE BRANCH still matches"
    );
    assert!(
        starts_with_branch_or_tag_ddl("CREATE OR REPLACE TAG t1 IN ice.sales.t"),
        "top-level CREATE OR REPLACE TAG still matches"
    );
}
