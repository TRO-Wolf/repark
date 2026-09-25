use crate::write::writer_partitioning::WriterLayout;
use crate::write::writer_plan::{
    WriterAction, WriterPlan, WriterRefusal, WriterRequest, WriterStatement,
    missing_column_message, missing_column_name, plan_writer,
};

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(ToString::to_string).collect()
}

fn bucketed(column: &str) -> WriterLayout {
    WriterLayout {
        num_buckets: Some(4),
        bucket_columns: strings(&[column]),
        ..WriterLayout::default()
    }
}

fn plan(
    action: WriterAction,
    exists: bool,
    mode: &str,
    layout: &WriterLayout,
) -> Result<WriterPlan, WriterRefusal> {
    let relation = strings(&["u7", "t"]);
    let columns = strings(&["id", "data", "cat"]);
    plan_writer(&WriterRequest {
        action,
        target: "sc.u7.t",
        relation_parts: &relation,
        exists,
        mode,
        explicit_format: true,
        layout,
        frame_columns: &columns,
        case_sensitive: false,
    })
}

fn statement(
    action: WriterAction,
    exists: bool,
    mode: &str,
    layout: &WriterLayout,
) -> (&'static str, bool) {
    let planned = plan(action, exists, mode, layout)
        .unwrap_or_else(|refusal| panic!("{action:?} {exists} {mode} must plan, got {refusal:?}"));
    (planned.statement.as_str(), planned.check_layout)
}

fn missing(action: WriterAction, exists: bool, mode: &str, layout: &WriterLayout) -> String {
    match plan(action, exists, mode, layout) {
        Err(WriterRefusal::MissingBucketColumn(column)) => column,
        other => panic!("{action:?} {exists} {mode} must refuse a missing column, got {other:?}"),
    }
}

#[test]
fn save_as_table_statements_follow_spark_save_modes() {
    let plain = WriterLayout::default();
    let table = WriterAction::SaveAsTable;
    assert_eq!(statement(table, true, "append", &plain), ("append", true));
    assert_eq!(
        statement(table, true, "overwrite", &plain),
        ("overwrite", false)
    );
    assert_eq!(statement(table, true, "ignore", &plain), ("skip", false));
    for mode in ["append", "overwrite", "error", "errorifexists", "ignore"] {
        assert_eq!(
            statement(table, false, mode, &plain),
            ("ctas", false),
            "{mode}"
        );
    }
    let buckets = bucketed("id");
    assert_eq!(
        statement(table, true, "overwrite", &buckets),
        ("rtas", false)
    );
    assert_eq!(
        statement(table, false, "overwrite", &buckets),
        ("rtas", false)
    );
    assert_eq!(statement(table, false, "append", &buckets), ("ctas", false));
    assert_eq!(statement(table, true, "append", &buckets), ("append", true));
    assert_eq!(WriterStatement::Skip.as_str(), "skip");
}

#[test]
fn save_as_table_error_mode_on_an_existing_table_answers_spark_text() {
    for mode in ["error", "errorifexists"] {
        let refusal = plan(WriterAction::SaveAsTable, true, mode, &bucketed("id"));
        let Err(WriterRefusal::Spark(error)) = refusal else {
            panic!("{mode} must refuse, got {refusal:?}");
        };
        assert_eq!(
            error.to_string(),
            "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `u7`.`t` because it \
             already exists.\nChoose a different name, drop or replace the existing object, or \
             add the IF NOT EXISTS clause to tolerate pre-existing objects. SQLSTATE: 42P07"
        );
    }
}

#[test]
fn a_missing_bucket_column_refuses_on_every_create_or_replace_arm() {
    let nope = bucketed("nope");
    let table = WriterAction::SaveAsTable;
    for mode in ["append", "overwrite", "error", "ignore"] {
        assert_eq!(missing(table, false, mode, &nope), "nope", "{mode}");
    }
    for mode in ["overwrite", "error", "ignore"] {
        assert_eq!(missing(table, true, mode, &nope), "nope", "{mode}");
    }
    assert_eq!(statement(table, true, "append", &nope), ("append", true));
    assert_eq!(
        statement(table, false, "error", &bucketed("ID")),
        ("ctas", false)
    );
}

fn sorted(bucket: &str, sorts: &[&str]) -> WriterLayout {
    WriterLayout {
        sort_columns: strings(sorts),
        ..bucketed(bucket)
    }
}

#[test]
fn a_missing_sort_column_refuses_after_the_bucket_columns() {
    let table = WriterAction::SaveAsTable;
    for mode in ["append", "overwrite", "error", "ignore"] {
        assert_eq!(
            missing(table, false, mode, &sorted("id", &["zz"])),
            "zz",
            "{mode}"
        );
    }
    assert_eq!(
        missing(table, false, "error", &sorted("id", &["a.b"])),
        "a.b"
    );
    assert_eq!(
        missing(table, false, "error", &sorted("id", &["data", "zz"])),
        "zz"
    );
    assert_eq!(
        missing(table, false, "error", &sorted("zz", &["nope"])),
        "zz"
    );
    assert_eq!(
        statement(table, true, "append", &sorted("id", &["zz"])),
        ("append", true)
    );
    assert_eq!(
        statement(table, false, "error", &sorted("id", &["DATA"])),
        ("ctas", false)
    );
}

#[test]
fn a_case_sensitive_session_keeps_the_bucket_column_case() {
    let relation = strings(&["u7", "t"]);
    let columns = strings(&["id"]);
    let layout = bucketed("ID");
    let refusal = plan_writer(&WriterRequest {
        action: WriterAction::SaveAsTable,
        target: "sc.u7.t",
        relation_parts: &relation,
        exists: false,
        mode: "error",
        explicit_format: true,
        layout: &layout,
        frame_columns: &columns,
        case_sensitive: true,
    });
    assert!(matches!(refusal, Err(WriterRefusal::MissingBucketColumn(column)) if column == "ID"));
    let layout = sorted("id", &["DATA"]);
    let refusal = plan_writer(&WriterRequest {
        action: WriterAction::SaveAsTable,
        target: "sc.u7.t",
        relation_parts: &relation,
        exists: false,
        mode: "error",
        explicit_format: true,
        layout: &layout,
        frame_columns: &strings(&["id", "data"]),
        case_sensitive: true,
    });
    assert!(matches!(refusal, Err(WriterRefusal::MissingBucketColumn(column)) if column == "DATA"));
}

#[test]
fn the_missing_column_message_is_spark_legacy_3060_text() {
    let tree = "root\n |-- id: long (nullable = true)\n";
    assert_eq!(
        missing_column_message("nope", tree),
        "Couldn't find column nope in:\nroot\n |-- id: long (nullable = true)\n"
    );
}

#[test]
fn the_missing_column_message_backticks_only_a_dotted_name() {
    let nested = "root\n |-- id: long (nullable = true)\n |-- s: struct (nullable = true)\n |    \
                  |-- a: integer (nullable = true)\n |    |-- b: string (nullable = true)\n";
    assert_eq!(
        missing_column_message("s.a", nested),
        "Couldn't find column `s.a` in:\nroot\n |-- id: long (nullable = true)\n |-- s: struct \
         (nullable = true)\n |    |-- a: integer (nullable = true)\n |    |-- b: string \
         (nullable = true)\n"
    );
    assert_eq!(
        missing_column_message("s.zz", nested),
        "Couldn't find column `s.zz` in:\nroot\n |-- id: long (nullable = true)\n |-- s: struct \
         (nullable = true)\n |    |-- a: integer (nullable = true)\n |    |-- b: string \
         (nullable = true)\n"
    );
    for (column, name) in [
        ("nope", "nope"),
        ("a.b", "`a.b`"),
        ("a.`b", "`a.`b`"),
        (".a", "`.a`"),
        ("a.", "`a.`"),
        ("my col", "my col"),
        ("select", "select"),
        ("1a", "1a"),
        ("12", "12"),
        ("a`b", "a`b"),
        ("", ""),
    ] {
        assert_eq!(missing_column_name(column), name, "{column}");
    }
}

#[test]
fn save_statements_check_the_layout_on_existing_tables() {
    let plain = WriterLayout::default();
    let save = WriterAction::Save;
    assert_eq!(statement(save, true, "append", &plain), ("append", true));
    assert_eq!(
        statement(save, true, "overwrite", &plain),
        ("overwrite", true)
    );
    assert_eq!(statement(save, true, "ignore", &plain), ("skip", false));
    assert_eq!(statement(save, false, "error", &plain), ("ctas", false));
    let refusal = plan(save, false, "append", &plain);
    assert!(matches!(refusal, Err(WriterRefusal::Spark(error))
        if error.to_string().starts_with("[TABLE_OR_VIEW_NOT_FOUND] The table or view u7.t ")));
}
