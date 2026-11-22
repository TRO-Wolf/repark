use super::*;

const PLANS: [&str; 4] = [
    "SELECT * FROM c.ns.t",
    "SELECT * FROM c.ns.t AS a",
    "SELECT n, c FROM c.ns.t WHERE n > 0",
    "SELECT CAST(count(*) AS INT) AS k FROM c.ns.t GROUP BY c",
];

async fn cached_plan(bed: &Bed, name: &str, sql: &str, level: CacheLevel) -> DataFrame {
    let plan = bed
        .session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
    bed.session
        .register_lazy_cache_view(name, plan, (None, None), None, None, level)
        .await
        .unwrap_or_else(|error| panic!("cache: {error}"));
    bed.session
        .sql(&format!("SELECT * FROM {name}"))
        .await
        .unwrap_or_else(|error| panic!("cached read: {error}"))
}

async fn filled(bed: &Bed, name: &str, level: CacheLevel) -> (DataFrame, CacheRead) {
    let cached = cached(bed, name, None, level).await;
    let first = read_cache(&cached).await;
    assert_eq!(first.rows, vec![1, 2], "{level:?}");
    (cached, first)
}

async fn another_writer_then_a_fresh_read(
    bed: &Bed,
    cached: &DataFrame,
    first: &CacheRead,
) -> ReparkSession {
    let other = other_writer(bed).await;
    commit_from(&other, "INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let held = read_cache(cached).await;
    assert_eq!((held.rows, held.buffer), (first.rows.clone(), first.buffer));
    let before = epoch(bed);
    assert_eq!(fresh_read(bed).await, vec![1, 2, 9]);
    assert_ne!(epoch(bed), before);
    other
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_level_fill_holds_over_another_writer_and_a_metadata_commit_until_a_refresh() {
    let bed = bed().await;
    let (cached, first) = filled(&bed, "__repark_cache_lt", CacheLevel::Table).await;
    let _other = another_writer_then_a_fresh_read(&bed, &cached, &first).await;
    let held = read_cache(&cached).await;
    assert_eq!((held.rows, held.buffer), (vec![1, 2], first.buffer));
    bed.run("ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'v')")
        .await;
    assert_eq!(fresh_read(&bed).await, vec![1, 2, 9]);
    let held = read_cache(&cached).await;
    assert_eq!((held.rows, held.buffer), (vec![1, 2], first.buffer));
    bed.run("REFRESH TABLE c.ns.t").await;
    let refilled = read_cache(&cached).await;
    assert_eq!(refilled.rows, vec![1, 2, 9]);
    assert_ne!(refilled.buffer, first.buffer);
    assert_eq!(read_cache(&cached).await.buffer, refilled.buffer);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_level_fill_holds_over_each_metadata_only_commit_after_another_writer() {
    for sql in [
        "ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'v')",
        "ALTER TABLE c.ns.t ADD COLUMN d INT",
        "ALTER TABLE c.ns.t CREATE BRANCH b",
    ] {
        let bed = bed().await;
        let (cached, first) = filled(&bed, "__repark_cache_lm", CacheLevel::Table).await;
        let _other = another_writer_then_a_fresh_read(&bed, &cached, &first).await;
        bed.run(sql).await;
        let held = read_cache(&cached).await;
        assert_eq!(
            (held.rows, held.buffer),
            (vec![1, 2], first.buffer),
            "{sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_level_fill_answers_the_latest_read_of_its_table() {
    let bed = bed().await;
    let (cached, first) = filled(&bed, "__repark_cache_lq", CacheLevel::Query).await;
    let _other = another_writer_then_a_fresh_read(&bed, &cached, &first).await;
    let refilled = read_cache(&cached).await;
    assert_eq!(refilled.rows, vec![1, 2, 9]);
    assert_ne!(refilled.buffer, first.buffer);
    assert_eq!(fresh_read(&bed).await, vec![1, 2, 9]);
    assert_eq!(read_cache(&cached).await.buffer, refilled.buffer);
    bed.run("ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'v')")
        .await;
    assert_eq!(fresh_read(&bed).await, vec![1, 2, 9]);
    let kept = read_cache(&cached).await;
    assert_eq!((kept.rows, kept.buffer), (vec![1, 2, 9], refilled.buffer));
    bed.run("REFRESH TABLE c.ns.t").await;
    let again = read_cache(&cached).await;
    assert_eq!(again.rows, vec![1, 2, 9]);
    assert_ne!(again.buffer, refilled.buffer);
    bed.run("ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'w')")
        .await;
    assert_eq!(read_cache(&cached).await.buffer, again.buffer);
}

#[tokio::test(flavor = "multi_thread")]
async fn both_levels_refill_after_a_same_session_data_commit_over_another_writer() {
    for level in LEVELS {
        let bed = bed().await;
        let (cached, first) = filled(&bed, "__repark_cache_ld", level).await;
        let _other = another_writer_then_a_fresh_read(&bed, &cached, &first).await;
        bed.run("INSERT INTO c.ns.t VALUES (8, 'q')").await;
        let refilled = read_cache(&cached).await;
        assert_eq!(refilled.rows, vec![1, 2, 8, 9], "{level:?}");
        assert_ne!(refilled.buffer, first.buffer, "{level:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_level_and_not_the_plan_decides_after_another_writer() {
    for sql in PLANS {
        for level in LEVELS {
            let bed = bed().await;
            let cached = cached_plan(&bed, "__repark_cache_lp", sql, level).await;
            let first = read_cache(&cached).await;
            let _other = another_writer_then_a_fresh_read(&bed, &cached, &first).await;
            let held = read_cache(&cached).await;
            let kept = (held.rows == first.rows, held.buffer == first.buffer);
            let want = level == CacheLevel::Table;
            assert_eq!(kept, (want, want), "{level:?} {sql}");
            assert_eq!(held.rows.len(), first.rows.len() + usize::from(!want));
        }
    }
}
