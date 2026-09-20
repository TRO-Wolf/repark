use super::*;

fn is_live(name: &str) -> bool {
    name == "sc" || name == "hc"
}

fn rewritten(sql: &str) -> Option<String> {
    rewrite_system_function_calls(sql, is_live)
}

#[test]
fn rewrites_single_bucket_call() {
    assert_eq!(
        rewritten("SELECT sc.system.bucket(16, id) FROM t").as_deref(),
        Some("SELECT __iceberg_system_bucket(16, id) FROM t")
    );
}

#[test]
fn rewrites_all_seven_names() {
    for (public, internal) in [
        ("bucket", "__iceberg_system_bucket"),
        ("truncate", "__iceberg_system_truncate"),
        ("years", "__iceberg_system_years"),
        ("months", "__iceberg_system_months"),
        ("days", "__iceberg_system_days"),
        ("hours", "__iceberg_system_hours"),
        ("iceberg_version", "__iceberg_system_iceberg_version"),
    ] {
        let sql = format!("SELECT sc.system.{public}(x) FROM t");
        let expected = format!("SELECT {internal}(x) FROM t");
        assert_eq!(
            rewritten(&sql).as_deref(),
            Some(expected.as_str()),
            "{public}"
        );
    }
}

#[test]
fn rewrites_multiple_calls_in_one_statement() {
    assert_eq!(
        rewritten("SELECT sc.system.bucket(8, d), sc.system.bucket(8, ts) FROM t").as_deref(),
        Some("SELECT __iceberg_system_bucket(8, d), __iceberg_system_bucket(8, ts) FROM t")
    );
}

#[test]
fn leaves_unknown_catalog_untouched() {
    assert_eq!(
        rewritten("SELECT nosuch.system.bucket(16, id) FROM t"),
        None
    );
}

#[test]
fn leaves_two_part_system_call_untouched() {
    assert_eq!(rewritten("SELECT system.bucket(16, id) FROM t"), None);
}

#[test]
fn leaves_call_statement_untouched() {
    assert_eq!(rewritten("CALL sc.system.expire_snapshots('t')"), None);
}

#[test]
fn leaves_string_literal_untouched() {
    assert_eq!(
        rewritten("SELECT 'sc.system.bucket(16, id)' AS v FROM t"),
        None
    );
}

#[test]
fn leaves_quoted_identifiers_untouched() {
    assert_eq!(
        rewritten("SELECT \"sc\".system.bucket(16, id) FROM t"),
        None
    );
    assert_eq!(rewritten("SELECT `sc`.system.bucket(16, id) FROM t"), None);
}

#[test]
fn matches_system_and_function_case_insensitively() {
    assert_eq!(
        rewritten("SELECT sc.SYSTEM.Bucket(16, id) FROM t").as_deref(),
        Some("SELECT __iceberg_system_bucket(16, id) FROM t")
    );
}

#[test]
fn catalog_match_stays_exact() {
    assert_eq!(rewritten("SELECT SC.system.bucket(16, id) FROM t"), None);
}

#[test]
fn leaves_missing_paren_untouched() {
    assert_eq!(rewritten("SELECT sc.system.bucket FROM t"), None);
}

#[test]
fn tolerates_whitespace_around_dots() {
    assert_eq!(
        rewritten("SELECT sc . system . bucket (16, id) FROM t").as_deref(),
        Some("SELECT __iceberg_system_bucket (16, id) FROM t")
    );
}

#[test]
fn internal_names_match_registered_udfs() {
    let mut registered: Vec<String> = repark_functions::iceberg_system::functions()
        .iter()
        .map(|udf| udf.name().to_owned())
        .collect();
    registered.sort_unstable();
    let mut mapped: Vec<&str> = repark_functions::iceberg_system::SYSTEM_FUNCTION_NAMES
        .iter()
        .map(|public| {
            repark_functions::iceberg_system::internal_name(public).expect("every roster name maps")
        })
        .collect();
    mapped.sort_unstable();
    assert_eq!(mapped, registered);
}

#[test]
fn parses_show_user_functions_in_system() {
    let parsed =
        try_parse_show_system_functions("SHOW USER FUNCTIONS IN sc.system").expect("matches");
    assert_eq!(parsed.catalog, "sc");
}

#[test]
fn parses_show_functions_in_system_without_user() {
    let parsed = try_parse_show_system_functions("SHOW FUNCTIONS IN sc.system").expect("matches");
    assert_eq!(parsed.catalog, "sc");
}

#[test]
fn ignores_show_forms_without_in() {
    for sql in [
        "SHOW FUNCTIONS",
        "SHOW USER FUNCTIONS",
        "SHOW SYSTEM FUNCTIONS",
        "SHOW NAMESPACES IN sc",
    ] {
        assert!(try_parse_show_system_functions(sql).is_none(), "{sql}");
    }
}

#[test]
fn ignores_non_system_show_scope() {
    for sql in [
        "SHOW USER FUNCTIONS IN sc.sales",
        "SHOW USER FUNCTIONS IN sc",
        "SHOW USER FUNCTIONS IN sc.system EXTRA",
    ] {
        assert!(try_parse_show_system_functions(sql).is_none(), "{sql}");
    }
}

#[test]
fn show_batch_carries_function_column() {
    let batch =
        show_system_functions_batch(vec!["sc.system.bucket".to_owned()]).expect("batch builds");
    assert_eq!(batch.schema().field(0).name(), "function");
    assert_eq!(
        batch.schema().field(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Utf8
    );
    assert_eq!(batch.num_rows(), 1);
}
