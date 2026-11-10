mod setop_reorder_common;

use datafusion::arrow::array::RecordBatch;
use setop_reorder_common::{
    Door, Doors, Form, State, attempt, check_refusal_text, create_shaped_table, observe,
    open_doors, render_rows, session_of, shape_by_id,
};

const TF1_PAIRS: [(&str, &str); 6] = [
    ("list", "list"),
    ("large", "list"),
    ("list", "fsl1"),
    ("fsl1", "fsl2"),
    ("largeview", "largeview"),
    ("view", "list"),
];

const TF1_DICT_ENCODINGS: [(&str, &str); 3] =
    [("dict", "dict"), ("dict", "plain"), ("plain", "dict")];

#[derive(Clone, Copy)]
enum Tf1Expect {
    Ours,
    Other,
}

fn tf1_struct_expr(order: &str, first: i64, second: i64) -> String {
    if order == "AB" {
        format!("named_struct('a', {first}, 'b', {second})")
    } else {
        format!("named_struct('b', {first}, 'a', {second})")
    }
}

fn tf1_struct_type(order: &str) -> String {
    if order == "AB" {
        "Struct(\"a\": Int64, \"b\": Int64)".to_string()
    } else {
        "Struct(\"b\": Int64, \"a\": Int64)".to_string()
    }
}

fn tf1_kind_type(kind: &str, element: &str) -> String {
    match kind {
        "list" => format!("List({element})"),
        "large" => format!("LargeList({element})"),
        "fsl1" => format!("FixedSizeList(1, {element})"),
        "fsl2" => format!("FixedSizeList(2, {element})"),
        "view" => format!("ListView({element})"),
        _ => format!("LargeListView({element})"),
    }
}

fn tf1_native_select(kind: &str, encoding: &str, order: &str, first: i64, second: i64) -> String {
    let inner = tf1_struct_expr(order, first, second);
    let row = if kind == "fsl2" {
        format!("make_array({inner}, {inner})")
    } else {
        format!("make_array({inner})")
    };
    let list_type = tf1_kind_type(kind, &tf1_struct_type(order));
    let target = if encoding == "dict" {
        format!("Dictionary(Int32, {list_type})")
    } else {
        list_type
    };
    format!("SELECT arrow_cast({row}, '{target}') AS s")
}

fn tf1_check_read(
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    expect: Tf1Expect,
) {
    match outcome {
        Ok(batches) => {
            failures.push(format!("{label}: answered {}", render_rows(&batches)));
        }
        Err(error) => match expect {
            Tf1Expect::Ours => {
                check_refusal_text(label, failures, &error, "UNION", "s[]", "b, a", "a, b");
            }
            Tf1Expect::Other => {
                if error.contains("[INCOMPATIBLE_COLUMN_TYPE]") {
                    failures.push(format!("{label}: fired the walker {error}"));
                }
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn tf1_check_insert(
    doors: &Doors,
    table: &str,
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    expect: Tf1Expect,
    before: &State,
) {
    let after = observe(doors, Door::NativeDataFusion, table).await;
    match outcome {
        Ok(_) => failures.push(format!("{label}: answered {}", after.rows)),
        Err(error) => {
            if after.rows != before.rows {
                failures.push(format!("{label}: rows changed {}", after.rows));
            }
            if after.snapshot != before.snapshot {
                failures.push(format!("{label}: snapshot changed"));
            }
            match expect {
                Tf1Expect::Ours => {
                    check_refusal_text(label, failures, &error, "UNION", "s[]", "b, a", "a, b");
                }
                Tf1Expect::Other => {
                    if error.contains("[INCOMPATIBLE_COLUMN_TYPE]") {
                        failures.push(format!("{label}: fired the walker {error}"));
                    }
                }
            }
        }
    }
}

fn tf1_pair_id(left: &str, right: &str, form: Form) -> String {
    let form_token = match form {
        Form::Read => "read",
        Form::Insert => "insert",
    };
    format!("tf1-native-{left}-{right}-fwd-union_all-{form_token}")
}

#[tokio::test(flavor = "multi_thread")]
async fn type_family_arrow_cast_pairs_refuse_on_native() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    let session = session_of(&doors, Door::NativeDataFusion);
    for (left_kind, right_kind) in TF1_PAIRS {
        let left = tf1_native_select(left_kind, "plain", "AB", 1, 2);
        let right = tf1_native_select(right_kind, "plain", "BA", 3, 4);
        let select = format!("{left} UNION ALL {right}");
        let expect = if left_kind == "view" || right_kind == "view" {
            Tf1Expect::Other
        } else {
            Tf1Expect::Ours
        };
        let label = tf1_pair_id(left_kind, right_kind, Form::Read);
        let outcome = attempt(session, Door::NativeDataFusion, &select).await;
        tf1_check_read(&label, &mut failures, outcome, expect);
        let table = format!("tf1_{left_kind}_{right_kind}_insert");
        create_shaped_table(
            &doors,
            Door::NativeDataFusion,
            &table,
            shape_by_id("array-reordered"),
        )
        .await;
        let before = observe(&doors, Door::NativeDataFusion, &table).await;
        if !before.rows.is_empty() {
            failures.push(format!("{label}: table not empty {}", before.rows));
            continue;
        }
        let insert = format!("INSERT INTO ice.ns.{table} {select}");
        let outcome = attempt(session, Door::NativeDataFusion, &insert).await;
        let label = tf1_pair_id(left_kind, right_kind, Form::Insert);
        tf1_check_insert(
            &doors,
            &table,
            &label,
            &mut failures,
            outcome,
            expect,
            &before,
        )
        .await;
    }
    for (left_encoding, right_encoding) in TF1_DICT_ENCODINGS {
        let left = tf1_native_select("list", left_encoding, "AB", 1, 2);
        let right = tf1_native_select("list", right_encoding, "BA", 3, 4);
        let select = format!("{left} UNION ALL {right}");
        let label = format!("tf1-native-dict-{left_encoding}-{right_encoding}-read");
        let outcome = attempt(session, Door::NativeDataFusion, &select).await;
        tf1_check_read(&label, &mut failures, outcome, Tf1Expect::Ours);
    }
    for (side, order, first, second) in [("left", "AB", 1, 2), ("right", "BA", 3, 4)] {
        let select = tf1_native_select("list", "dict", order, first, second);
        let label = format!("tf1-native-dict-ingest-{side}-read");
        let outcome = attempt(session, Door::NativeDataFusion, &select).await;
        tf1_check_read(&label, &mut failures, outcome, Tf1Expect::Other);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn type_family_recursive_row_refuses_on_native() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    let session = session_of(&doors, Door::NativeDataFusion);
    let anchor = tf1_native_select("list", "plain", "AB", 1, 2);
    let other = tf1_native_select("list", "plain", "BA", 3, 4);
    let select = format!(
        "WITH RECURSIVE r(s) AS ({anchor} UNION ALL SELECT s FROM r WHERE FALSE) \
         SELECT s FROM r UNION ALL {other}"
    );
    let outcome = attempt(session, Door::NativeDataFusion, &select).await;
    tf1_check_read(
        "tf1-native-rcte-union_all-read",
        &mut failures,
        outcome,
        Tf1Expect::Ours,
    );
    let table = "tf1_rcte_insert";
    create_shaped_table(
        &doors,
        Door::NativeDataFusion,
        table,
        shape_by_id("array-reordered"),
    )
    .await;
    let before = observe(&doors, Door::NativeDataFusion, table).await;
    if before.rows.is_empty() {
        let insert = format!("INSERT INTO ice.ns.{table} {select}");
        let outcome = attempt(session, Door::NativeDataFusion, &insert).await;
        tf1_check_insert(
            &doors,
            table,
            "tf1-native-rcte-union_all-insert",
            &mut failures,
            outcome,
            Tf1Expect::Ours,
            &before,
        )
        .await;
    } else {
        failures.push(format!("read: table not empty {}", before.rows));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
