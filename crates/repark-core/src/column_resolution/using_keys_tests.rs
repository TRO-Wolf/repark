use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;

use super::sql_with_column_repair;

async fn tables() -> SessionContext {
    let ctx = SessionContext::new();
    for ddl in [
        "CREATE TABLE tl AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, s)",
        "CREATE TABLE tr AS SELECT * FROM (VALUES (2, 'x'), (3, 'y'), (4, 'z')) AS v(id, t)",
        "CREATE TABLE tq AS SELECT * FROM (VALUES (2, 'p'), (3, 'q'), (4, 'r')) AS v(id, u)",
    ] {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }
    ctx
}

async fn answer(ctx: &SessionContext, sql: &str, sorted: bool) -> (Vec<String>, Vec<String>) {
    let frame = sql_with_column_repair(ctx, sql, true).await.unwrap();
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        "-".to_string()
                    } else {
                        array_value_to_string(column, row).unwrap()
                    }
                })
                .collect::<Vec<_>>();
            rows.push(cells.join(","));
        }
    }
    if sorted {
        rows.sort();
    }
    (names, rows)
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    answer(ctx, sql, true).await.1
}

async fn ordered(ctx: &SessionContext, sql: &str) -> Vec<String> {
    answer(ctx, sql, false).await.1
}

const SIX: [&str; 6] = ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"];

#[tokio::test]
async fn star_shows_the_merged_key_on_right_and_full() {
    let ctx = tables().await;
    let (names, full) = answer(&ctx, "SELECT * FROM tl l FULL JOIN tr r USING (id)", true).await;
    assert_eq!(names, ["id", "s", "t"]);
    assert_eq!(full, ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]);
    assert_eq!(
        rows(&ctx, "SELECT * FROM tl l RIGHT JOIN tr r USING (id)").await,
        ["2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(&ctx, "SELECT * FROM tl FULL JOIN tr USING (id)").await,
        ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(&ctx, "SELECT * FROM tl l LEFT JOIN tr r USING (id)").await,
        ["1,a,-", "2,b,x", "3,c,y"]
    );
    assert_eq!(
        rows(&ctx, "SELECT * FROM tl l JOIN tr r USING (id)").await,
        ["2,b,x", "3,c,y"]
    );
}

#[tokio::test]
async fn unqualified_key_reads_the_merged_key_in_every_clause() {
    let ctx = tables().await;
    let full = "tl l FULL JOIN tr r USING (id)";
    let (names, keys) = answer(&ctx, &format!("SELECT id FROM {full}"), true).await;
    assert_eq!(names, ["id"]);
    assert_eq!(keys, ["1", "2", "3", "4"]);
    assert_eq!(
        rows(&ctx, &format!("SELECT id + 1 FROM {full}")).await,
        ["2", "3", "4", "5"]
    );
    assert_eq!(
        rows(&ctx, &format!("SELECT * FROM {full} WHERE id > 2")).await,
        ["3,c,y", "4,-,z"]
    );
    assert_eq!(
        ordered(&ctx, &format!("SELECT * FROM {full} ORDER BY id")).await,
        ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        ordered(&ctx, &format!("SELECT s FROM {full} ORDER BY id DESC")).await,
        ["-", "c", "b", "a"]
    );
    assert_eq!(
        rows(
            &ctx,
            &format!("SELECT id, count(*) FROM {full} GROUP BY id")
        )
        .await,
        ["1,1", "2,1", "3,1", "4,1"]
    );
    assert_eq!(
        rows(
            &ctx,
            &format!("SELECT id, count(*) FROM {full} GROUP BY id HAVING id > 3")
        )
        .await,
        ["4,1"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT ID FROM tl l RIGHT JOIN tr r USING (id) WHERE Id > 2"
        )
        .await,
        ["3", "4"]
    );
}

#[tokio::test]
async fn unqualified_key_in_where_answers_on_six_join_types() {
    let ctx = tables().await;
    let expected = [
        vec!["3"],
        vec!["3"],
        vec!["3", "4"],
        vec!["3", "4"],
        vec!["3"],
        vec![],
    ];
    for (how, wanted) in SIX.iter().zip(expected) {
        for source in [
            format!("tl l {how} JOIN tr r USING (id)"),
            format!("tl {how} JOIN tr USING (id)"),
        ] {
            assert_eq!(
                rows(&ctx, &format!("SELECT id FROM {source} WHERE id > 2")).await,
                wanted,
                "{source}"
            );
        }
    }
}

#[tokio::test]
async fn per_side_keys_keep_their_own_values() {
    let ctx = tables().await;
    let full = "tl l FULL JOIN tr r USING (id)";
    assert_eq!(
        rows(&ctx, &format!("SELECT l.id, r.id FROM {full}")).await,
        ["-,4", "1,-", "2,2", "3,3"]
    );
    assert_eq!(
        rows(&ctx, &format!("SELECT * FROM {full} WHERE r.id > 2")).await,
        ["3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(&ctx, &format!("SELECT * FROM {full} WHERE l.id > 2")).await,
        ["3,c,y"]
    );
    let (names, by_left) = answer(
        &ctx,
        &format!("SELECT * FROM {full} ORDER BY l.id NULLS FIRST"),
        false,
    )
    .await;
    assert_eq!(names, ["id", "s", "t"]);
    assert_eq!(by_left, ["4,-,z", "1,a,-", "2,b,x", "3,c,y"]);
    assert_eq!(
        ordered(
            &ctx,
            &format!("SELECT * FROM {full} ORDER BY r.id NULLS FIRST")
        )
        .await,
        ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        ordered(
            &ctx,
            &format!("SELECT * FROM {full} ORDER BY r.id DESC NULLS LAST LIMIT 2")
        )
        .await,
        ["4,-,z", "3,c,y"]
    );
    assert_eq!(
        ordered(
            &ctx,
            "SELECT * FROM tl l RIGHT JOIN tr r USING (id) ORDER BY l.id NULLS FIRST"
        )
        .await,
        ["4,-,z", "2,b,x", "3,c,y"]
    );
}

#[tokio::test]
async fn chained_using_joins_match_on_the_merged_key() {
    let ctx = tables().await;
    let (names, chained) = answer(
        &ctx,
        "SELECT * FROM tl l FULL JOIN tr r USING (id) FULL JOIN tq q USING (id)",
        true,
    )
    .await;
    assert_eq!(names, ["id", "s", "t", "u"]);
    assert_eq!(chained, ["1,a,-,-", "2,b,x,p", "3,c,y,q", "4,-,z,r"]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM tl l RIGHT JOIN tr r USING (id) JOIN tq q USING (id)"
        )
        .await,
        ["2,b,x,p", "3,c,y,q", "4,-,z,r"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM tl l FULL JOIN tr r USING (id) FULL JOIN tq q USING (id) WHERE id > 3"
        )
        .await,
        ["4"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM tl l JOIN tr r USING (id) JOIN tq q USING (id)"
        )
        .await,
        ["2,b,x,p", "3,c,y,q"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM tl l NATURAL FULL JOIN tr r NATURAL FULL JOIN tq q"
        )
        .await
        .len(),
        4
    );
}

#[tokio::test]
async fn scopes_and_shadows_are_respected() {
    let ctx = tables().await;
    assert_eq!(
        ordered(
            &ctx,
            "SELECT s AS id FROM tl l FULL JOIN tr r USING (id) ORDER BY id"
        )
        .await,
        ["a", "b", "c", "-"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT x.id FROM (SELECT * FROM tl l FULL JOIN tr r USING (id)) x"
        )
        .await,
        ["1", "2", "3", "4"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM tl l FULL JOIN tr r USING (id) \
             WHERE EXISTS (SELECT 1 FROM tq WHERE id = 4)"
        )
        .await,
        ["1", "2", "3", "4"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM tl l FULL JOIN tr r USING (id) UNION ALL SELECT id FROM tq"
        )
        .await,
        ["1", "2", "2", "3", "3", "4", "4"]
    );
    assert_eq!(
        rows(&ctx, "SELECT *, r.id FROM tl l FULL JOIN tr r USING (id)").await,
        ["-,-,z,4", "1,a,-,-", "2,b,x,2", "3,c,y,3"]
    );
}

#[tokio::test]
async fn joins_without_using_are_unchanged() {
    let ctx = tables().await;
    let (names, on_join) = answer(
        &ctx,
        "SELECT * FROM tl l FULL JOIN tr r ON l.id = r.id",
        true,
    )
    .await;
    assert_eq!(names, ["id", "s", "id", "t"]);
    assert_eq!(on_join, ["-,-,4,z", "1,a,-,-", "2,b,2,x", "3,c,3,y"]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT l.id FROM tl l FULL JOIN tr r ON l.id = r.id WHERE r.id > 2"
        )
        .await,
        ["-", "3"]
    );
    assert_eq!(rows(&ctx, "SELECT id FROM tl WHERE id > 2").await, ["3"]);
}

#[tokio::test]
async fn case_sensitive_sessions_coalesce_too() {
    let ctx = tables().await;
    let frame = sql_with_column_repair(&ctx, "SELECT * FROM tl l FULL JOIN tr r USING (id)", false)
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let mut keys = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            assert!(!batch.column(0).is_null(row));
            keys.push(array_value_to_string(batch.column(0), row).unwrap());
        }
    }
    keys.sort();
    assert_eq!(keys, ["1", "2", "3", "4"]);
}
