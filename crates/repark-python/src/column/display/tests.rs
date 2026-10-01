use super::*;

#[test]
fn binary_paren_spaces_match_python() {
    assert_eq!(wrap_binary("x", "+", "1"), "(x + 1)");
    assert_eq!(wrap_binary("x", "AND", "flag"), "(x AND flag)");
}

#[test]
fn not_equal_uses_not_equals_form() {
    assert_eq!(wrap_not_equal("x", "1"), "(NOT (x = 1))");
}

#[test]
fn unary_neg_display_and_sql_diverge() {
    assert_eq!(wrap_negative_display("x"), "negative(x)");
    assert_eq!(wrap_negative_sql("x"), "(-(x))");
}

#[test]
fn null_safe_sql_is_distinct_from() {
    assert_eq!(wrap_null_safe_display("x", "NULL"), "(x <=> NULL)");
    assert_eq!(
        wrap_null_safe_sql("x", "NULL"),
        "(x IS NOT DISTINCT FROM NULL)"
    );
}

#[test]
fn case_open_and_closed_match_python() {
    let arms = vec![
        ("(x > 0)".to_string(), "1".to_string()),
        ("(x < 0)".to_string(), "-1".to_string()),
    ];
    assert_eq!(
        format_case_body(arms.clone(), None),
        "CASE WHEN (x > 0) THEN 1 WHEN (x < 0) THEN -1 END"
    );
    assert_eq!(
        format_case_body(arms[..1].to_vec(), Some("0")),
        "CASE WHEN (x > 0) THEN 1 ELSE 0 END"
    );
}

#[test]
fn cast_and_try_cast_keywords() {
    assert_eq!(wrap_cast("CAST", "x", "DOUBLE"), "CAST(x AS DOUBLE)");
    assert_eq!(wrap_cast("TRY_CAST", "x", "INT"), "TRY_CAST(x AS INT)");
}

#[test]
fn call_renders_name_comma_parts() {
    assert_eq!(wrap_call("sqrt", &["x"]), "sqrt(x)");
    assert_eq!(wrap_call("lpad", &["s", "10", "'x'"]), "lpad(s, 10, 'x')");
    assert_eq!(wrap_call("rand", &[] as &[&str]), "rand()");
}

#[test]
fn getitem_index_field_and_key_shapes() {
    assert_eq!(wrap_index_display("arr", "0"), "arr[0]");
    assert_eq!(wrap_index_sql("arr", "0"), "(arr)[0]");
    assert_eq!(wrap_field_sql("st", "\"a\""), "(st).\"a\"");
    assert_eq!(wrap_index_display("m", "'k'"), "m['k']");
    assert_eq!(wrap_alias("x", "z"), "x AS z");
    assert_eq!(wrap_invert("flag"), "(NOT flag)");
    assert_eq!(wrap_is_null("x"), "(x IS NULL)");
    assert_eq!(
        wrap_string_predicate_display("s", "startswith", "a"),
        "s.startswith(a)"
    );
    assert_eq!(
        wrap_string_predicate_sql("starts_with", "s", "'a'"),
        "starts_with(s, 'a')"
    );
    assert_eq!(wrap_substr("s", "1", "2"), "substr(s, 1, 2)");
}

fn assert_pure(column: &PyColumn, label: &str) {
    let (expr, plan) = column.grown_read(crate::deep_stack::survey_expression);
    assert_eq!(
        column.expression_depth(),
        expr,
        "{label}: cached expr depth"
    );
    assert_eq!(
        column.df_depth(),
        expr,
        "{label}: pure shapes count every level"
    );
    assert_eq!(column.plan_depth(), plan, "{label}: cached plan depth");
}

#[test]
fn parts_levels_match_a_fresh_survey() {
    let left = PyColumn::column("a").expect("a column builds");
    let right = PyColumn::column("b").expect("a column builds");
    let trio = ("a", "a", "a");
    for op in [
        "add", "sub", "mul", "div", "modulo", "eq", "lt", "gt", "le", "ge", "and_", "or_",
    ] {
        let (built, _, _, _) =
            PyColumnParts::binary(&left, &right, op, "+", trio, trio).expect("binary builds");
        assert_pure(&built, op);
    }
    let (built, _, _, _) = PyColumnParts::not_equal(&left, &right, trio, trio).expect("ne builds");
    assert_pure(&built, "not_equal");
    let (built, _, _, _) = PyColumnParts::unary_neg(&left, "a", "a").expect("unary neg builds");
    assert_pure(&built, "unary_neg");
    let (built, _, _, _) =
        PyColumnParts::eq_null_safe(&left, &right, trio, trio).expect("null-safe eq builds");
    assert_pure(&built, "eq_null_safe");
    let (built, _, _, _) = PyColumnParts::invert(&left, "a", "a", "a").expect("invert builds");
    assert_pure(&built, "invert");
    let (built, _, _, _) = PyColumnParts::is_null(&left, "a", "a", "a").expect("is_null builds");
    assert_pure(&built, "is_null");
    let (built, _, _, _) =
        PyColumnParts::is_not_null(&left, "a", "a", "a").expect("is_not_null builds");
    assert_pure(&built, "is_not_null");
    let (built, _, _, _) = PyColumnParts::case_when(
        vec![(left.clone(), right.clone())],
        Some(right.clone()),
        vec![("c".to_string(), "v".to_string())],
        vec![("c".to_string(), "v".to_string())],
        vec![("c".to_string(), "v".to_string())],
        Some(("e".to_string(), "e".to_string(), "e".to_string())),
    )
    .expect("case_when builds");
    assert_pure(&built, "case_when");
    let (built, _) = PyColumnParts::alias(&left, "a", "x").expect("alias builds");
    assert_pure(&built, "alias");
    let (built, _, _, _) = PyColumnParts::in_list(
        &left,
        vec![right.clone()],
        trio,
        vec![("b".to_string(), "b".to_string(), "b".to_string())],
    )
    .expect("in_list builds");
    assert_pure(&built, "in_list");
    let (built, _, _, _) =
        PyColumnParts::substr(&left, &right, &right, ("a", "b", "c"), ("a", "b", "c"))
            .expect("substr builds");
    assert_pure(&built, "substr");
    let (built, _, _, _) = PyColumnParts::string_predicate(
        &left,
        &right,
        "contains",
        "contains",
        ("a", "b"),
        ("a", "b"),
    )
    .expect("string predicate builds");
    assert_pure(&built, "string_predicate");
    let (built, _, _, _) =
        PyColumnParts::bitwise(&left, &right, "bitwise_and", "&", ("a", "b"), ("a", "b"))
            .expect("bitwise builds");
    assert_pure(&built, "bitwise");
    for kind in ["index", "field", "key"] {
        let (built, _, _, _) = PyColumnParts::getitem(&left, &right, kind, ("a", "b"), ("a", "b"))
            .expect("getitem builds");
        assert_pure(&built, kind);
    }
    let (built, _, _, _) = PyColumnParts::update_fields(
        &left,
        vec!["with".to_string()],
        vec!["a.b".to_string()],
        vec![right.clone()],
        trio,
        vec![("v".to_string(), "v".to_string(), "v".to_string())],
    )
    .expect("update_fields builds");
    assert_pure(&built, "update_fields");
    let (built, _, _, _) =
        PyColumnParts::repark_isnan(&left, ("a", "a", "a")).expect("isnan builds");
    assert_pure(&built, "repark_isnan");
    let built =
        PyColumnParts::time_window(&left, "1 hour", None, None).expect("time_window builds");
    assert_pure(&built, "time_window");
    let built = PyColumnParts::session_window(&left, &right).expect("session_window builds");
    assert_pure(&built, "session_window");
    let (built, _, _) = super::construct::lit_array_cast(&left, "a", "INT", "BIGINT")
        .expect("a literal array cast builds");
    assert_pure(&built, "lit_array_cast");
}
