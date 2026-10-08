use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;

use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;

use super::sql_with_column_repair;
use super::using_marks::qualify_keys;

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
        ["1,a,-,-", "2,b,x,2", "3,c,y,3", "4,-,z,4"]
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

async fn more_tables() -> SessionContext {
    let ctx = tables().await;
    for ddl in [
        "CREATE TABLE tp AS SELECT * FROM (VALUES (3, 'm'), (4, 'n'), (5, 'o')) AS v(id, w)",
        "CREATE TABLE mv1 AS SELECT * FROM (VALUES (1, 10), (2, 20)) AS v(id, v)",
        "CREATE TABLE mv2 AS SELECT * FROM (VALUES (1, 100), (3, 300)) AS v(id, v)",
        "CREATE TABLE tx AS SELECT * FROM (VALUES (1, 1), (2, 2)) AS v(k1, k2)",
        "CREATE TABLE ty AS SELECT * FROM (VALUES (2, 'y2'), (3, 'y3')) AS v(k1, yv)",
        "CREATE TABLE tz AS SELECT * FROM (VALUES (2, 'z2')) AS v(k2, zv)",
        "CREATE TABLE th AS SELECT * FROM (VALUES (1, 'h')) AS v(id, __repark_using_k0)",
        "CREATE TABLE td AS SELECT CAST(id AS DECIMAL(10, 2)) AS id, dv \
         FROM (VALUES (2.00, 'd2'), (3.50, 'd3')) AS v(id, dv)",
    ] {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }
    ctx
}

async fn refusal(ctx: &SessionContext, sql: &str) -> String {
    match sql_with_column_repair(ctx, sql, true).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => String::from("answered"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn distinct_and_order_by_keep_planning_on_the_merged_key() {
    let ctx = more_tables().await;
    for source in [
        "tl FULL JOIN tr USING (id)",
        "tl l FULL JOIN tr r USING (id)",
    ] {
        assert_eq!(
            ordered(
                &ctx,
                &format!("SELECT DISTINCT id FROM {source} ORDER BY id NULLS FIRST")
            )
            .await,
            ["1", "2", "3", "4"],
            "{source}"
        );
        assert_eq!(
            ordered(
                &ctx,
                &format!("SELECT DISTINCT id, s FROM {source} ORDER BY id DESC NULLS LAST")
            )
            .await,
            ["4,-", "3,c", "2,b", "1,a"],
            "{source}"
        );
    }
}

#[tokio::test]
async fn order_by_a_side_key_with_a_column_outside_the_select_list() {
    let ctx = more_tables().await;
    assert_eq!(
        ordered(
            &ctx,
            "SELECT id FROM tl l RIGHT JOIN tr r USING (id) ORDER BY l.id NULLS FIRST, t"
        )
        .await,
        ["4", "2", "3"]
    );
    assert_eq!(
        ordered(
            &ctx,
            "SELECT id FROM tl l FULL JOIN tr r USING (id) ORDER BY l.id NULLS FIRST, upper(s)"
        )
        .await,
        ["4", "1", "2", "3"]
    );
    let (names, by_right) = answer(
        &ctx,
        "SELECT id, s FROM tl l FULL JOIN tr r USING (id) ORDER BY r.id NULLS FIRST, s",
        false,
    )
    .await;
    assert_eq!(names, ["id", "s"]);
    assert_eq!(by_right, ["1,a", "2,b", "3,c", "4,-"]);
}

#[tokio::test]
async fn shared_non_key_names_keep_their_output_names() {
    let ctx = more_tables().await;
    for order in ["r.id", "l.id"] {
        let (names, shared) = answer(
            &ctx,
            &format!("SELECT * FROM mv1 l FULL JOIN mv2 r USING (id) ORDER BY {order} NULLS FIRST"),
            true,
        )
        .await;
        assert_eq!(names, ["id", "v", "v"], "{order}");
        assert_eq!(shared, ["1,10,100", "2,20,-", "3,-,300"], "{order}");
    }
    let (names, picked) = answer(
        &ctx,
        "SELECT id, l.v, r.v FROM mv1 l RIGHT JOIN mv2 r USING (id) ORDER BY l.id NULLS FIRST",
        false,
    )
    .await;
    assert_eq!(names, ["id", "v", "v"]);
    assert_eq!(picked, ["3,-,300", "1,10,100"]);
}

#[tokio::test]
async fn a_key_spelled_in_another_case_follows_the_session_rule() {
    let ctx = more_tables().await;
    let full = "tl l FULL JOIN tr r USING (ID)";
    assert_eq!(
        rows(&ctx, &format!("SELECT * FROM {full}")).await,
        ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(&ctx, &format!("SELECT * FROM {full} WHERE Id > 3")).await,
        ["4,-,z"]
    );
    assert_eq!(
        rows(&ctx, &format!("SELECT id FROM {full}")).await,
        ["1", "2", "3", "4"]
    );
    assert_eq!(
        ordered(
            &ctx,
            &format!("SELECT Id FROM {full} ORDER BY iD DESC NULLS LAST")
        )
        .await,
        ["4", "3", "2", "1"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT ID, S FROM tl l RIGHT JOIN tr r USING (id) WHERE ID > 3"
        )
        .await,
        ["4,-"]
    );
}

#[tokio::test]
async fn explain_shows_the_plan_that_runs() {
    let ctx = more_tables().await;
    let shown = rows(&ctx, "EXPLAIN SELECT * FROM tl l FULL JOIN tr r USING (id)").await;
    assert!(
        shown.iter().any(|row| row.contains("ELSE r.id")),
        "{shown:?}"
    );
    let filtered = rows(
        &ctx,
        "EXPLAIN SELECT id FROM tl l FULL JOIN tr r USING (id) WHERE id > 2",
    )
    .await;
    assert!(
        filtered.iter().any(|row| row.contains("ELSE r.id")),
        "{filtered:?}"
    );
    let inner = rows(&ctx, "EXPLAIN SELECT * FROM tl l JOIN tr r ON l.id = r.id").await;
    assert!(inner.iter().all(|row| !row.contains("ELSE r.id")));
}

#[tokio::test]
async fn shapes_beyond_one_plain_chain() {
    let ctx = more_tables().await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM tl l FULL JOIN tr r USING (id) JOIN tq q ON r.id = q.id"
        )
        .await,
        ["2,b,x,2,p", "3,c,y,3,q", "4,-,z,4,r"]
    );
    assert_eq!(
        rows(&ctx, "SELECT * FROM tl l NATURAL FULL JOIN tr r").await,
        ["1,a,-", "2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM tl RIGHT JOIN (SELECT * FROM tr) USING (id)"
        )
        .await,
        ["2,b,x", "3,c,y", "4,-,z"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM tl l FULL JOIN tr r USING (id) FULL JOIN tq q USING (id)"
        )
        .await,
        ["1", "2", "3", "4"]
    );
    let chained = rows(
        &ctx,
        "SELECT * FROM tx FULL JOIN ty USING (k1) LEFT JOIN tz USING (k2)",
    )
    .await;
    assert!(
        chained.iter().all(|row| !row.starts_with('-')),
        "{chained:?}"
    );
    assert_eq!(chained.len(), 3);
    assert_eq!(
        rows(&ctx, "SELECT id FROM tl l FULL JOIN td d USING (id)").await,
        ["1.00", "2.00", "3.00", "3.50"]
    );
}

#[tokio::test]
async fn helper_names_do_not_collide_with_user_columns() {
    let ctx = more_tables().await;
    let (names, held) = answer(
        &ctx,
        "SELECT * FROM th l FULL JOIN tr r USING (id) ORDER BY l.id NULLS FIRST",
        false,
    )
    .await;
    assert_eq!(names, ["id", "__repark_using_k0", "t"]);
    assert_eq!(held, ["2,-,x", "3,-,y", "4,-,z", "1,h,-"]);
}

#[tokio::test]
async fn refusals_main_gave_stay_the_same_refusals() {
    let ctx = more_tables().await;
    let duplicate = refusal(
        &ctx,
        "SELECT id, l.id, r.id FROM tl l FULL JOIN tr r USING (id)",
    )
    .await;
    assert!(duplicate.contains("unique expression names"), "{duplicate}");
    let ambiguous = refusal(
        &ctx,
        "SELECT * FROM tl l FULL JOIN tr r USING (id) JOIN tq q ON r.id = q.id WHERE id > 1",
    )
    .await;
    assert!(ambiguous.contains("AMBIGUOUS_REFERENCE"), "{ambiguous}");
    let missing = refusal(&ctx, "SELECT nope FROM tl l FULL JOIN tr r USING (id)").await;
    assert!(missing.contains("nope"), "{missing}");
}

fn qualified(sql: &str) -> (bool, String) {
    let mut statements = DFParser::parse_sql_with_dialect(sql, &DatabricksDialect {}).unwrap();
    let mut statement = statements.pop_front().unwrap();
    let changed = qualify_keys(&mut statement, true);
    (changed, statement.to_string())
}

#[test]
fn lambda_parameters_shadow_the_key() {
    let (changed, text) = qualified(
        "SELECT id, transform(array(10, 20), id -> id + 1) AS x FROM tl JOIN tr USING (id) \
         WHERE exists(array(1, 2), id -> id = 2) AND id > 1",
    );
    assert!(changed);
    assert_eq!(
        text,
        "SELECT tl.id, transform(array(10, 20), id -> id + 1) AS x FROM tl JOIN tr USING(id) \
         WHERE exists(array(1, 2), id -> id = 2) AND tl.id > 1"
    );
    let (_, nested) = qualified(
        "SELECT filter(array(1, 2), id -> exists(array(3), x -> x > id)) FROM tl JOIN tr USING (id) \
         WHERE aggregate(array(1, 2), 0, (acc, id) -> acc + id) > id",
    );
    assert_eq!(
        nested,
        "SELECT filter(array(1, 2), id -> exists(array(3), x -> x > id)) FROM tl JOIN tr USING(id) \
         WHERE aggregate(array(1, 2), 0, (acc, id) -> acc + id) > tl.id"
    );
    let (_, outer) = qualified(
        "SELECT transform(array(1), x -> x + id), transform(array(1), l -> l + 1) \
         FROM tl l JOIN tr r USING (id) WHERE l.id > 0",
    );
    assert_eq!(
        outer,
        "SELECT transform(array(1), x -> x + l.id), transform(array(1), l -> l + 1) \
         FROM tl l JOIN tr r USING(id) WHERE l.id > 0"
    );
}

#[test]
fn qualification_keeps_to_plain_using_chains() {
    for sql in [
        "SELECT id FROM tl l JOIN tr r ON l.id = r.id WHERE id > 1",
        "SELECT id FROM tl l JOIN tr r USING (id) JOIN tq q ON r.id = q.id WHERE id > 1",
        "SELECT id FROM tl NATURAL JOIN tr WHERE id > 1",
        "SELECT id FROM tl, tr WHERE id > 1",
        "SELECT s FROM tl l JOIN tr r USING (id) WHERE s > 'a'",
        "SELECT x FROM tl l LEFT SEMI JOIN tr r USING (id) WHERE id > 1",
    ] {
        let (changed, text) = qualified(sql);
        assert!(!changed, "{text}");
    }
    let (changed, text) = qualified(
        "SELECT s AS id FROM tl l JOIN tr r USING (ID) WHERE Id > 1 AND `Id` > 1 ORDER BY id",
    );
    assert!(changed);
    assert_eq!(
        text,
        "SELECT s AS id FROM tl l JOIN tr r USING(ID) WHERE l.Id > 1 AND `Id` > 1 ORDER BY id"
    );
    let (_, scoped) = qualified(
        "SELECT id FROM tl l JOIN tr r USING (id) WHERE id IN (SELECT id FROM tq) ORDER BY id",
    );
    assert_eq!(
        scoped,
        "SELECT l.id FROM tl l JOIN tr r USING(id) WHERE l.id IN (SELECT id FROM tq) ORDER BY l.id"
    );
}
