/// I1: VERSION/TIMESTAMP AS OF, branch, and tag pins, plus unknown-id loud error.
use super::super::*;
use super::common::*;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::tokenizer::Token;
use repark_core::SessionTimeZone;

#[tokio::test]
#[allow(clippy::too_many_lines)] // multi-snapshot matrix + error pins in one oracle
async fn time_travel_version_timestamp_branch_tag_and_errors() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    // Snapshot 1: CTAS three rows.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tt AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "tt".into());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s1 = table
        .metadata()
        .current_snapshot_id()
        .expect("s1 current snapshot");
    let s1_ts = table.metadata().snapshot_by_id(s1).unwrap().timestamp_ms();

    // Snapshot 2: append one row.
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tt SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2 = table
        .metadata()
        .current_snapshot_id()
        .expect("s2 current snapshot");

    // Snapshot 3: overwrite to a single row (distinct multiset).
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.tt SELECT 9 AS id, 'z' AS name",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s3 = table
        .metadata()
        .current_snapshot_id()
        .expect("s3 current snapshot");
    assert_ne!(s1, s2);
    assert_ne!(s2, s3);

    // Tag at s1, branch at s2 (test-support ManageSnapshots seam).
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx
        .manage_snapshots()
        .create_tag("tag_s1", s1)
        .create_branch("branch_s2", s2);
    let tx = action.apply(tx).expect("apply create ref");
    tx.commit(catalogs["ice"].as_ref())
        .await
        .expect("commit refs");

    // VERSION AS OF snapshot id.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt VERSION AS OF {s1}")
        )
        .await,
        vec![1, 2, 3]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt FOR SYSTEM_VERSION AS OF {s2}")
        )
        .await,
        vec![1, 2, 3, 4]
    );

    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let s2_ts = table.metadata().snapshot_by_id(s2).unwrap().timestamp_ms();
    let s3_ts = table.metadata().snapshot_by_id(s3).unwrap().timestamp_ms();
    assert!(s1_ts < s2_ts && s2_ts <= s3_ts);
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s1_ts}")
        )
        .await,
        vec![9]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s2_ts}")
        )
        .await,
        vec![9]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {s3_ts}")
        )
        .await,
        vec![9]
    );
    let mid = s1_ts + ((s2_ts - s1_ts) / 2).max(1);
    if mid < s2_ts {
        assert_eq!(
            time_travel_id_multiset(
                &ctx,
                &catalogs,
                &format!("SELECT id FROM ice.sales.tt TIMESTAMP AS OF {mid}")
            )
            .await,
            vec![9]
        );
    }
    // Earlier than first snapshot → loud error.
    let early_err = execute(
        &ctx,
        &catalogs,
        &format!(
            "SELECT * FROM ice.sales.tt TIMESTAMP AS OF {}",
            s1_ts.div_euclid(1000) - 3600
        ),
    )
    .await
    .expect_err("ts earlier than first snapshot must fail");
    assert!(
        early_err.to_string().contains("snapshot older than"),
        "got: {early_err}"
    );

    // Tag + branch refs via VERSION AS OF.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.tt VERSION AS OF 'tag_s1'"
        )
        .await,
        vec![1, 2, 3]
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.tt VERSION AS OF 'branch_s2'"
        )
        .await,
        vec![1, 2, 3, 4]
    );

    // Unknown snapshot id / ref → loud, naming it.
    let unknown_id = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.tt VERSION AS OF 999999999",
    )
    .await
    .expect_err("unknown snapshot id");
    assert!(
        unknown_id.to_string().contains("999999999"),
        "must name snapshot id, got: {unknown_id}"
    );
    let unknown_ref = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.tt VERSION AS OF 'no_such_ref'",
    )
    .await
    .expect_err("unknown ref");
    assert!(
        unknown_ref.to_string().contains("no_such_ref"),
        "must name ref, got: {unknown_ref}"
    );

    // Filter/projection composition on a pinned snapshot.
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.tt VERSION AS OF {s1} WHERE id >= 2")
        )
        .await,
        vec![2, 3]
    );

    // Current read still sees s3 (overwrite) after time-travel reads.
    assert_eq!(
        time_travel_id_multiset(&ctx, &catalogs, "SELECT id FROM ice.sales.tt").await,
        vec![9]
    );
}

/// Ephemeral `__repark_tt_*` names stay visible on the session default schema until taken down.
fn leftover_time_travel_views(ctx: &SessionContext) -> Vec<String> {
    let state = ctx.state();
    let catalog_options = &state.config_options().catalog;
    let catalog = ctx
        .catalog(&catalog_options.default_catalog)
        .expect("default catalog");
    let schema = catalog
        .schema(&catalog_options.default_schema)
        .expect("default schema");
    let mut names: Vec<String> = schema
        .table_names()
        .into_iter()
        .filter(|name| name.starts_with("__repark_tt_"))
        .collect();
    names.sort();
    names
}

/// Two-snapshot `ice.sales.leak` + the ids of its first and second snapshot.
async fn setup_time_travel_leak_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> (i64, i64) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.leak AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "leak".into());
    let first = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("first snapshot");
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.leak SELECT 4 AS id, 'd' AS name",
    )
    .await;
    let second = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("second snapshot");
    assert_ne!(first, second);
    (first, second)
}

/// I1 leak pin (H-1b): the rewrite's ephemeral views must NOT survive the statement.
#[tokio::test]
async fn time_travel_temp_views_do_not_survive_a_successful_statement() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let (first, second) = setup_time_travel_leak_table(&ctx, &catalogs).await;
    assert!(leftover_time_travel_views(&ctx).is_empty());

    // Repeat: the leak is per-statement, so accumulation is what a single-shot check would miss.
    for _ in 0..3 {
        let frame = execute(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.leak VERSION AS OF {first}"),
        )
        .await
        .expect("pinned read must plan");
        // Collected after `execute` returned — i.e.
        let rows: usize = frame
            .collect()
            .await
            .expect("pinned read must execute after release")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3, "the pinned read must still see only the CTAS rows");
    }

    // Two pins in ONE statement: both ephemeral names must go.
    let rows: usize = execute(
        &ctx,
        &catalogs,
        &format!(
            "SELECT a.id FROM ice.sales.leak VERSION AS OF {first} a \
             JOIN ice.sales.leak VERSION AS OF {second} b ON a.id = b.id"
        ),
    )
    .await
    .expect("two-pin join must plan")
    .collect()
    .await
    .expect("two-pin join must execute")
    .iter()
    .map(RecordBatch::num_rows)
    .sum();
    assert_eq!(rows, 3, "join of the 3-row pin against the 4-row pin");

    assert!(
        leftover_time_travel_views(&ctx).is_empty(),
        "time-travel temp views must be released, not left on the session: {:?}",
        leftover_time_travel_views(&ctx)
    );
}

/// I1 leak pin, ERROR half.
#[tokio::test]
async fn time_travel_temp_views_do_not_survive_a_failed_statement() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let (first, _second) = setup_time_travel_leak_table(&ctx, &catalogs).await;

    // 1.
    let mid_rewrite = execute(
        &ctx,
        &catalogs,
        &format!(
            "SELECT a.id FROM ice.sales.leak VERSION AS OF 999999999 a \
             JOIN ice.sales.leak VERSION AS OF {first} b ON a.id = b.id"
        ),
    )
    .await
    .expect_err("unknown snapshot id must fail the statement");
    assert!(
        mid_rewrite.to_string().contains("999999999"),
        "must still name the unresolvable snapshot id, got: {mid_rewrite}"
    );
    assert!(
        leftover_time_travel_views(&ctx).is_empty(),
        "a rewrite that failed half-way must release what it already registered: {:?}",
        leftover_time_travel_views(&ctx)
    );

    // 2.
    let planning = execute(
        &ctx,
        &catalogs,
        &format!("SELECT no_such_column FROM ice.sales.leak VERSION AS OF {first}"),
    )
    .await
    .expect_err("unknown column must fail planning");
    assert!(
        planning.to_string().contains("no_such_column"),
        "must still name the unknown column, got: {planning}"
    );
    assert!(
        leftover_time_travel_views(&ctx).is_empty(),
        "a statement that failed in planning must release its pinned views: {:?}",
        leftover_time_travel_views(&ctx)
    );
}

/// The `<n>` of an engine-minted `__repark_tt_<n>` name.
fn temp_view_sequence(name: &str) -> u64 {
    name.strip_prefix("__repark_tt_")
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("not an engine-minted temp-view name: {name}"))
}

/// I1 collision: Spark-door time travel must not disturb a reader-options registration.
#[tokio::test]
async fn time_travel_statement_pins_never_collide_with_a_reader_options_view() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let (first, _second) = setup_time_travel_leak_table(&ctx, &catalogs).await;
    assert!(leftover_time_travel_views(&ctx).is_empty());

    // The reader-options shape: `spark.read.option` reaches exactly this call.
    let table_parts = ["ice".to_string(), "sales".to_string(), "leak".to_string()];
    let reader_frame = repark_core::read_table_at(
        &ctx,
        &catalogs,
        &table_parts,
        &repark_core::TimeTravelSpec::SnapshotId(first),
        &repark_core::SessionTimeZone::default(),
    )
    .await
    .expect("the reader-options pinned read must plan");
    let reader_views = leftover_time_travel_views(&ctx);
    assert_eq!(
        reader_views.len(),
        1,
        "the reader-options path registers exactly one ephemeral view: {reader_views:?}"
    );

    // A Spark-door statement on the SAME session.
    let rows: usize = execute(
        &ctx,
        &catalogs,
        &format!("SELECT id FROM ice.sales.leak VERSION AS OF {first}"),
    )
    .await
    .expect("the statement's pinned read must plan")
    .collect()
    .await
    .expect("the statement's pinned read must execute")
    .iter()
    .map(RecordBatch::num_rows)
    .sum();
    assert_eq!(rows, 3, "the statement still sees only the CTAS rows");

    // 1.
    assert_eq!(
        leftover_time_travel_views(&ctx),
        reader_views,
        "a time-travel STATEMENT must release every name it minted and leave the reader-options \
         registration alone"
    );
    // The reader's frame still executes.
    let reader_rows: usize = reader_frame
        .collect()
        .await
        .expect("the reader-options frame must still execute")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        reader_rows, 3,
        "the reader-options frame is still the pinned snapshot"
    );

    // 2.
    let before_mint = temp_view_sequence(&repark_core::next_temp_view_name());
    let _ = execute(
        &ctx,
        &catalogs,
        &format!("SELECT id FROM ice.sales.leak VERSION AS OF {first}"),
    )
    .await
    .expect("the second statement must plan");
    let after_mint = temp_view_sequence(&repark_core::next_temp_view_name());
    assert!(
        after_mint > before_mint + 1,
        "the Spark door must mint from repark-core's counter, not one of its own: \
         {before_mint} → {after_mint}"
    );

    // Nothing accumulated across either statement.
    assert_eq!(leftover_time_travel_views(&ctx), reader_views);
}

#[test]
fn reader_spec_builtin_pins_refuse_loud() {
    use repark_core::{ReaderTimeTravel, TimeTravelSpec, resolve_reader_spec};

    let zone = SessionTimeZone::default();
    let pin = |opts: &ReaderTimeTravel| resolve_reader_spec(opts, &zone, false);
    let versioned = |version: &str| ReaderTimeTravel {
        version_as_of: Some(version.to_string()),
        ..Default::default()
    };
    let both = ReaderTimeTravel {
        version_as_of: Some("1".to_string()),
        timestamp_as_of: Some("2020-01-01".to_string()),
        ..Default::default()
    };
    let err = pin(&both).expect_err("version plus timestamp must refuse");
    assert!(
        err.to_string().contains("INVALID_TIME_TRAVEL_SPEC"),
        "got: {err}"
    );
    let legacy_version = ReaderTimeTravel {
        snapshot_id: Some(7),
        version_as_of: Some("8".to_string()),
        ..Default::default()
    };
    let err = pin(&legacy_version).expect_err("snapshot-id plus versionAsOf must refuse");
    assert!(err.to_string().contains("versionAsOf"), "got: {err}");
    let legacy_timestamp = ReaderTimeTravel {
        as_of_timestamp_ms: Some(1_750_000_000_000),
        timestamp_as_of: Some("2020-01-01".to_string()),
        ..Default::default()
    };
    let err = pin(&legacy_timestamp).expect_err("as-of-timestamp plus timestampAsOf must refuse");
    assert!(err.to_string().contains("timestampAsOf"), "got: {err}");
    let branch_version = ReaderTimeTravel {
        branch: Some("b0".to_string()),
        version_as_of: Some("1".to_string()),
        ..Default::default()
    };
    let err = pin(&branch_version).expect_err("branch plus versionAsOf must refuse");
    assert!(
        err.to_string().contains("Can't time travel in branch"),
        "got: {err}"
    );
    let err = resolve_reader_spec(&versioned("1"), &zone, true)
        .expect_err("versionAsOf inside a branch must refuse");
    assert!(
        err.to_string().contains("Can't time travel in branch"),
        "got: {err}"
    );
    assert_eq!(
        pin(&versioned("42")).unwrap(),
        Some(TimeTravelSpec::SnapshotId(42))
    );
    assert_eq!(
        pin(&versioned("audit")).unwrap(),
        Some(TimeTravelSpec::VersionRef("audit".to_string()))
    );
    let err = pin(&versioned("")).expect_err("empty versionAsOf must refuse");
    assert!(
        err.to_string().contains("Cannot find matching"),
        "got: {err}"
    );
}

#[test]
fn reader_spec_legacy_pins_still_resolve() {
    use repark_core::{ReaderTimeTravel, TimeTravelSpec, resolve_reader_spec};

    let zone = SessionTimeZone::default();
    let pin = |opts: &ReaderTimeTravel| resolve_reader_spec(opts, &zone, false);
    let stamped = ReaderTimeTravel {
        timestamp_as_of: Some("1750000000".to_string()),
        ..Default::default()
    };
    assert_eq!(
        pin(&stamped).unwrap(),
        Some(TimeTravelSpec::TimestampMs(1_750_000_000_000))
    );
    let bad_stamp = ReaderTimeTravel {
        timestamp_as_of: Some("not a ts".to_string()),
        ..Default::default()
    };
    let err = pin(&bad_stamp).expect_err("unparsable timestampAsOf must refuse");
    assert!(
        err.to_string()
            .contains("INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT"),
        "got: {err}"
    );
    let legacy_only = ReaderTimeTravel {
        snapshot_id: Some(7),
        ..Default::default()
    };
    assert_eq!(
        pin(&legacy_only).unwrap(),
        Some(TimeTravelSpec::SnapshotId(7))
    );
    let legacy_clash = ReaderTimeTravel {
        snapshot_id: Some(7),
        branch: Some("b0".to_string()),
        ..Default::default()
    };
    assert!(pin(&legacy_clash).is_err());
    let tagged = ReaderTimeTravel {
        tag: Some("t0".to_string()),
        ..Default::default()
    };
    assert_eq!(
        pin(&tagged).unwrap(),
        Some(TimeTravelSpec::VersionRef("t0".to_string()))
    );
    assert_eq!(pin(&ReaderTimeTravel::default()).unwrap(), None);
}

fn asof_tokens(text: &str) -> Vec<Token> {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    Tokenizer::new(&GenericDialect {}, text)
        .tokenize()
        .unwrap()
        .into_iter()
        .filter(|token| !matches!(token, Token::EOF))
        .collect()
}

async fn eval_one(
    ctx: &SessionContext,
    zone: &SessionTimeZone,
    text: &str,
) -> datafusion::error::Result<i64> {
    use repark_core::evaluate_sql_timestamp_asof;

    let tokens = asof_tokens(text);
    evaluate_sql_timestamp_asof(ctx, &tokens, zone).await
}

#[tokio::test]
async fn sql_timestamp_asof_evaluates_constants_in_session_zone() {
    let ctx = SessionContext::new();
    let zone = SessionTimeZone::default();
    assert_eq!(
        eval_one(&ctx, &zone, "1750000000").await.unwrap(),
        1_750_000_000_000
    );
    assert_eq!(
        eval_one(&ctx, &zone, "'2020-06-01 00:00:00'")
            .await
            .unwrap(),
        1_590_969_600_000
    );
    assert_eq!(
        eval_one(&ctx, &zone, "CAST('2020-06-01 00:00:00' AS TIMESTAMP)")
            .await
            .unwrap(),
        1_590_969_600_000
    );
    let before = chrono::Utc::now().timestamp_millis();
    let current = eval_one(&ctx, &zone, "current_timestamp()").await.unwrap();
    let after = chrono::Utc::now().timestamp_millis();
    assert!(before <= current && current <= after + 1_000);
    let err = eval_one(&ctx, &zone, "'not a ts'")
        .await
        .expect_err("garbage must refuse");
    assert!(
        err.to_string()
            .contains("INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT"),
        "got: {err}"
    );
    let err = eval_one(&ctx, &zone, "NULL")
        .await
        .expect_err("NULL must refuse");
    assert!(
        err.to_string()
            .contains("INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT"),
        "got: {err}"
    );
    let err = eval_one(&ctx, &zone, "id")
        .await
        .expect_err("a column reference must refuse");
    assert!(
        err.to_string().contains("cannot refer to any columns"),
        "got: {err}"
    );
    let err = eval_one(&ctx, &zone, "rand()")
        .await
        .expect_err("rand() must refuse");
    assert!(err.to_string().contains("NON_DETERMINISTIC"), "got: {err}");
}
