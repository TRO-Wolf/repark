mod setop_reorder_common;

use setop_reorder_common::{
    Door, attempt, check_refusal_text, open_doors, render_rows, session_of,
};

fn large_list_view(order: &str, first: i64, second: i64) -> String {
    let (head, tail) = if order == "AB" {
        ("a", "b")
    } else {
        ("b", "a")
    };
    format!(
        "arrow_cast(make_array(named_struct('{head}', {first}, '{tail}', {second})), \
         'LargeListView(Struct(\"{head}\": Int64, \"{tail}\": Int64))')"
    )
}

fn recursive(order: &str) -> String {
    let anchor = large_list_view("AB", 1, 2);
    let step = large_list_view(order, 3, 4);
    format!(
        "WITH RECURSIVE r(s) AS (SELECT {anchor} AS s UNION ALL SELECT {step} AS s FROM r \
         WHERE FALSE) SELECT s FROM r"
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_list_view_recursive_term_refuses_reordered_and_answers_same_names() {
    let doors = open_doors().await;
    let session = session_of(&doors, Door::NativeDataFusion);
    let mut failures = Vec::new();
    match attempt(session, Door::NativeDataFusion, &recursive("BA")).await {
        Ok(batches) => failures.push(format!("reordered: answered {}", render_rows(&batches))),
        Err(error) => check_refusal_text(
            "f4-rcte-largelistview-reordered",
            &mut failures,
            &error,
            "UNION ALL",
            "s[]",
            "b, a",
            "a, b",
        ),
    }
    match attempt(session, Door::NativeDataFusion, &recursive("AB")).await {
        Ok(batches) => {
            let rows = render_rows(&batches);
            if rows.is_empty() {
                failures.push("same names: no anchor row".to_string());
            }
        }
        Err(error) => failures.push(format!("same names: refused {error}")),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
