use super::*;

fn engine_of(pattern: &str) -> Engine {
    compile_spark_regex(pattern, "rlike")
        .expect("compiles")
        .engine()
}

fn is_match(pattern: &str, text: &str) -> bool {
    compile_spark_regex(pattern, "rlike")
        .expect("compiles")
        .is_match(text)
        .expect("runs")
}

#[test]
fn plain_patterns_stay_plain() {
    for pattern in [
        "ab",
        "[0-9]+",
        "^[0-9]+$",
        "(a)-(b)",
        "(?<name>b)",
        "(?i)ab",
        "(?m)^cd$",
        "\\p{Lower}b",
        "\\Qa.b\\E",
        "(ab)",
        "(?:ab)",
        "(a)+",
        "(ab)*",
        "(a){2,3}",
        "(a){2,}",
        "(a|b){2,3}",
    ] {
        assert_eq!(
            engine_of(pattern),
            Engine::Plain,
            "pins: java-regex-features-1/C-001, C-007: {pattern}"
        );
    }
}

#[test]
fn invalid_plain_patterns_raise_the_class() {
    for pattern in ["a{2,1}", "(", "a{"] {
        let error = compile_spark_regex(pattern, "rlike").expect_err("rejects");
        assert!(
            error
                .to_string()
                .contains("INVALID_PARAMETER_VALUE.PATTERN"),
            "{error}"
        );
    }
}

#[test]
fn fancy_features_route_fancy() {
    for pattern in [
        "a(?=b)",
        "a(?!b)",
        "(a)\\1",
        "(?<x>a)\\k<x>",
        "^a*+a$",
        "\\d++",
        "a*+",
        "^(?>a*)a$",
        "(a|b)\\1",
    ] {
        assert!(matches!(engine_of(pattern), Engine::Fancy), "{pattern}");
    }
}

#[test]
fn lookbehind_patterns_route_lookbehind() {
    for pattern in [
        "(?<=a)b",
        "(?<!a)b",
        "(?<=a*)b",
        "(?<=ab+)c",
        "(?<=ab{2,4})c",
        "(?<=a.+)c",
        "(?<=a{2,})b",
        "(?<=(a){1,2})(b)(c)",
        "(?<=a+b)",
        "(?<=a*b)",
        "(?<=.*x)a",
        "(?<=a)(?<!b)b",
    ] {
        assert_eq!(engine_of(pattern), Engine::Lookbehind, "{pattern}");
    }
}

#[test]
fn catastrophic_shapes_route_by_feature_not_by_loop() {
    for pattern in ["(a+)+b", "(a|b)*c", "(a*)*", "(a|b){2,}"] {
        assert_eq!(
            engine_of(pattern),
            Engine::Plain,
            "no lookaround, backref, possessive or atomic: {pattern}"
        );
    }
    for pattern in ["(a+)+b(?=c)", "(a|b)*+c", "(?>(a|b)*)c"] {
        assert_eq!(
            engine_of(pattern),
            Engine::Fancy,
            "a fancy feature still routes fancy: {pattern}"
        );
    }
}

#[test]
fn p_group_is_invalid_java() {
    let error = compile_spark_regex("(?P<x>a)", "rlike").expect_err("rejects");
    assert!(
        error
            .to_string()
            .contains("INVALID_PARAMETER_VALUE.PATTERN"),
        "{error}"
    );
}

#[test]
fn k_quote_backref_is_invalid_java() {
    let error = compile_spark_regex("(a)\\k'x'", "rlike").expect_err("rejects");
    assert!(
        error
            .to_string()
            .contains("INVALID_PARAMETER_VALUE.PATTERN"),
        "{error}"
    );
}

#[test]
fn dangling_backref_reads_as_octal() {
    let compiled = compile_spark_regex("\\1", "rlike").expect("compiles");
    assert_eq!(compiled.engine(), Engine::Plain);
    assert!(!compiled.is_match("x").expect("runs"));
    assert!(compiled.is_match("\u{1}").expect("runs"));
}

#[test]
fn real_backref_stays_a_backref() {
    assert!(is_match("(a)\\1", "aa"));
    assert!(!is_match("(a)\\1", "ab"));
    assert!(is_match("(?<x>a)\\k<x>", "aa"));
}

#[test]
fn quoted_and_class_backslashes_survive_octal_rewrite() {
    assert!(is_match("\\\\1", "\\1"));
    assert!(!is_match("\\\\1", "\u{1}"));
}

#[test]
fn non_ascii_patterns_never_panic() {
    assert!(is_match("\u{e9}+", "\u{e9}e"));
    assert!(is_match("(?i)\u{e9}", "\u{c9}"));
    assert!(is_match("(?<=\u{e9}+)x", "\u{e9}x"));
    assert!(!is_match("(?<=\u{e9}+)x", "ax"));
}

#[test]
fn quoted_group_syntax_stays_literal() {
    assert!(is_match("\\Q(?<=a+)\\E", "(?<=a+)"));
    assert!(!is_match("\\Q(?<=a+)\\E", "(?<=a)"));
    assert!(is_match("\\Q(\\E(a)\\1", "(aa"));
    assert!(!is_match("\\Q(\\E(a)\\1", "(a)a"));
}

#[test]
fn dollar_braces_drop_before_expansion() {
    assert_eq!(strip_dollar_braces("[${name}]"), "[]");
    assert_eq!(strip_dollar_braces("${x}"), "");
    assert_eq!(strip_dollar_braces("$1${x}"), "$1");
    assert_eq!(strip_dollar_braces("a${x"), "a${x");
    assert_eq!(strip_dollar_braces("$$1"), "$$1");
}

#[test]
fn invalid_pattern_names_the_function() {
    let error = compile_spark_regex("(?<=a", "rlike").expect_err("rejects");
    assert_eq!(
        error.to_string(),
        "Execution error: [INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` \
         in `rlike` is invalid: '(?<=a'. SQLSTATE: 22023"
    );
    let error = compile_spark_regex("(?<=a", "regexp_extract").expect_err("rejects");
    assert!(error.to_string().contains("in `regexp_extract`"), "{error}");
    let error = compile_spark_regex("(?<=a", "split").expect_err("rejects");
    assert!(
        error
            .to_string()
            .starts_with("Execution error: invalid regular expression '(?<=a': "),
        "{error}"
    );
}

#[test]
fn nested_shape_without_fancy_feature_stays_plain() {
    let compiled = compile_spark_regex("(a+)+b", "rlike").expect("compiles");
    assert_eq!(compiled.engine(), Engine::Plain);
    assert!(!compiled.is_match(&"a".repeat(25)).expect("runs"));
    assert!(compiled.is_match("aab").expect("runs"));
}

#[test]
fn looping_pattern_on_huge_haystack_overruns() {
    let compiled = compile_spark_regex("(a|b)*c", "rlike").expect("compiles");
    let text = "ab".repeat(20_000);
    let error = compiled.is_match(&text).expect_err("overruns");
    let message = error.to_string();
    assert!(message.contains("(a|b)*c"), "{message}");
    assert!(message.contains("10000"), "{message}");
    assert!(message.contains("overrun"), "{message}");
    assert!(compiled.is_match("abc").expect("runs"));
    assert!(!compiled.is_match("abx").expect("runs"));
}

#[test]
fn non_looping_fancy_pattern_ignores_haystack_size() {
    let compiled = compile_spark_regex("a(?=b)", "rlike").expect("compiles");
    let text = "ab".repeat(20_000);
    assert!(compiled.is_match(&text).expect("runs"));
}

#[test]
fn backtrack_budget_trips_on_hard_exponential() {
    let compiled = compile_spark_regex("(a+)+b(?=c)", "rlike").expect("compiles");
    let text = "a".repeat(25);
    let started = std::time::Instant::now();
    let error = compiled.is_match(&text).expect_err("overruns");
    let message = error.to_string();
    assert!(message.contains("overrun"), "{message}");
    assert!(message.contains("100000000"), "{message}");
    assert!(started.elapsed().as_secs() < 120, "budget trips closed");
}

#[test]
fn fancy_replace_expands_groups() {
    let compiled = compile_spark_regex("(a)(?=b)", "regexp_replace").expect("compiles");
    assert_eq!(
        compiled.replace_all("abcabc", "$1$1").expect("runs"),
        "aabcaabc"
    );
    let compiled = compile_spark_regex("(?<name>b)", "regexp_replace").expect("compiles");
    assert_eq!(
        compiled
            .replace_all("abc", &strip_dollar_braces("[${name}]"))
            .expect("runs"),
        "a[]c"
    );
}

#[test]
fn fancy_collect_counts_empty_matches() {
    let compiled = compile_spark_regex("(?=X)", "regexp_count").expect("compiles");
    assert_eq!(compiled.count_non_overlapping("aXbXc").expect("runs"), 2);
    let compiled = compile_spark_regex("(?<=a)a", "regexp_count").expect("compiles");
    assert_eq!(compiled.count_non_overlapping("aaaa").expect("runs"), 3);
}

#[test]
fn unbounded_plus_lookbehind_collapses_to_single() {
    assert!(is_match("(?<=a+)b", "ab"));
    assert!(is_match("(?<=a+)b", "aaab"));
    assert!(!is_match("(?<=a+)b", "xb"));
    assert!(!is_match("(?<=a+)b", "b"));
}

#[test]
fn bounded_range_lookbehind_expands_to_alternation() {
    assert!(is_match("(?<=a{2,4})b", "aab"));
    assert!(is_match("(?<=a{2,4})b", "aaaab"));
    assert!(!is_match("(?<=a{2,4})b", "ab"));
    assert!(!is_match("(?<=a{2,4})b", "b"));
    assert!(is_match("(?<=a{1,3})b", "aaaab"));
    assert!(is_match("(?<=x{2})a", "xxa"));
}

#[test]
fn nullable_lookbehind_is_constant() {
    assert!(is_match("(?<=a*)b", "ab"));
    assert!(is_match("(?<=a*)b", "xb"));
    assert!(!is_match("(?<!a*)b", "ab"));
    assert!(!is_match("(?<!a*)b", "xb"));
}

#[test]
fn lookbehind_normalization_keeps_group_numbers() {
    assert!(is_match("(?<=(x))(a)\\2", "xaa"));
    assert!(is_match("(?<=a*)(x)\\1", "xx"));
}

#[test]
fn fancy_captures_answer_groups() {
    let compiled = compile_spark_regex("(a)\\1", "regexp_extract").expect("compiles");
    assert_eq!(compiled.captures_len(), 2);
    let found = compiled.find_first("xaay").expect("runs").expect("found");
    assert_eq!(
        compiled.capture_at("xaay", found.0, 0).expect("runs"),
        Some("aa".to_owned())
    );
}

#[test]
fn semantic_lookbehind_multi_char_atoms() {
    assert!(is_match("(?<=ab+)c", "abbbc"));
    assert!(!is_match("(?<=ab+)c", "ac"));
    assert!(!is_match("(?<!ab+)c", "abbbc"));
    assert!(is_match("(?<=ab{2,4})c", "xabbc"));
    assert!(!is_match("(?<=ab{2,4})c", "xabc"));
    assert!(is_match("(?<=a.+)c", "axyzc"));
    assert!(!is_match("(?<=a.+)c", "ac"));
}

#[test]
fn semantic_lookbehind_open_minimum() {
    assert!(is_match("(?<=a{2,})b", "aab"));
    assert!(!is_match("(?<=a{2,})b", "ab"));
    assert!(!is_match("(?<=a{2,})b", "b"));
}

#[test]
fn semantic_lookbehind_keeps_group_numbers() {
    let compiled = compile_spark_regex("(?<=(a){1,2})(b)(c)", "regexp_extract").expect("compiles");
    assert_eq!(compiled.captures_len(), 4);
    let found = compiled.find_first("aabc").expect("runs").expect("found");
    assert_eq!(
        compiled.capture_at("aabc", found.0, 2).expect("runs"),
        Some("b".to_owned())
    );
    assert_eq!(
        compiled.capture_at("aabc", found.0, 3).expect("runs"),
        Some("c".to_owned())
    );
}

#[test]
fn semantic_lookbehind_concatenated_bodies() {
    assert!(is_match("(?<=a+b)", "aab"));
    assert!(is_match("(?<=a+b)", "ab"));
    assert!(!is_match("(?<=a+b)", "aa"));
    assert!(is_match("(?<=a*b)", "aab"));
    assert!(is_match("(?<=a*b)", "ab"));
    assert!(is_match("(?<=.*x)a", "xab"));
    assert!(!is_match("(?<=.*x)a", "ab"));
    assert!(is_match("(?<=a)(?<!b)b", "ab"));
    assert!(!is_match("(?<=a)(?<!b)b", "bb"));
}

#[test]
fn numbered_backref_to_named_group() {
    assert!(is_match("(?<x>a)\\1", "aa"));
    assert!(!is_match("(?<x>a)\\1", "ab"));
    assert!(is_match("(?<x>a)\\k<x>(b)\\2", "aabb"));
    assert!(!is_match("(?<x>a)\\k<x>(b)\\2", "aaab"));
}

#[test]
fn backref_into_lookbehind_body_refuses_loud() {
    let error = compile_spark_regex("(?<=(a))b\\1", "rlike").expect_err("rejects");
    assert!(
        error
            .to_string()
            .contains("INVALID_PARAMETER_VALUE.PATTERN"),
        "{error}"
    );
}

#[test]
fn lookbehind_search_budget_trips_loud() {
    let compiled = compile_spark_regex("(?<=a{1,100}b)a", "rlike").expect("compiles");
    let error = compiled
        .is_match(&"a".repeat(20_000))
        .expect_err("overruns");
    let message = error.to_string();
    assert!(message.contains("overrun"), "{message}");
    assert!(message.contains("lookbehind search budget"), "{message}");
}

#[test]
fn zero_width_matches_step_supplementary_chars() {
    let compiled = compile_spark_regex("(?=)", "regexp_count").expect("compiles");
    assert_eq!(compiled.count_non_overlapping("😀").expect("runs"), 3);
    assert_eq!(
        compiled
            .collect_matches("😀", usize::MAX)
            .expect("runs")
            .len(),
        3
    );
    assert_eq!(compiled.count_non_overlapping("😀x").expect("runs"), 4);
    assert_eq!(
        compiled
            .collect_matches("😀x", usize::MAX)
            .expect("runs")
            .len(),
        4
    );
}
