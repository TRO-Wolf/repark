use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

const SEARCH_PATH: &str = "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]";
const INVALID_FUNCTION_MARKER: &str = "Invalid function '";
const TABLE_FUNCTION_MARKER: &str = "table function '";

#[must_use]
pub fn map_unknown_routine_message(sql: &str, message: &str) -> Option<String> {
    if let Some(dotted) = capture_quoted(message, INVALID_FUNCTION_MARKER) {
        routine_message(sql, &dotted)
    } else if let Some(name) = capture_quoted(message, TABLE_FUNCTION_MARKER) {
        table_valued_function_message(sql, &name)
    } else {
        None
    }
}

fn capture_quoted(message: &str, marker: &str) -> Option<String> {
    let rest = message.split(marker).nth(1)?;
    let name = rest.split('\'').next()?;
    (!name.is_empty()).then(|| name.to_string())
}

fn routine_message(sql: &str, dotted: &str) -> Option<String> {
    let key = flatten_name(dotted);
    if key.split('.').any(str::is_empty) {
        return None;
    }
    match locate_call_site(sql, &key) {
        Some(site) => {
            if let [first, second, _] = site.parts.as_slice()
                && first.eq_ignore_ascii_case("system")
                && second.eq_ignore_ascii_case("builtin")
            {
                Some(requires_message(first, second))
            } else {
                Some(unresolved_message(
                    &render_parts(&site.parts),
                    Some(site.start),
                ))
            }
        }
        None => Some(unresolved_message(&render_key_parts(&key), None)),
    }
}

fn table_valued_function_message(sql: &str, dotted: &str) -> Option<String> {
    let key = flatten_name(dotted);
    if key.split('.').any(str::is_empty) {
        return None;
    }
    let (rendered, start) = match locate_call_site(sql, &key) {
        Some(site) => (render_parts(&site.parts), Some(site.start)),
        None => (render_key_parts(&key), None),
    };
    Some(format!(
        "[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve {rendered} to a table-valued \
         function.\nPlease make sure that {rendered} is defined as a table-valued function and \
         that all required parameters are provided correctly.\nIf {rendered} is not defined, \
         please create the table-valued function before using it.\nFor more information about \
         defining table-valued functions, please refer to the Apache Spark documentation. \
         SQLSTATE: 42883{}",
        start.map_or(String::new(), position_suffix),
    ))
}

fn requires_message(first: &str, second: &str) -> String {
    format!(
        "[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, \
         but got `{first}`.`{second}`. SQLSTATE: 42K05"
    )
}

fn unresolved_message(rendered: &str, start: Option<Location>) -> String {
    format!(
        "[UNRESOLVED_ROUTINE] Cannot resolve routine {rendered} on search path {SEARCH_PATH}. \
         SQLSTATE: 42883{}",
        start.map_or(String::new(), position_suffix)
    )
}

fn flatten_name(dotted: &str) -> String {
    dotted
        .chars()
        .filter(|next| *next != '`' && *next != '"')
        .collect::<String>()
        .to_lowercase()
}

fn render_parts(spelled: &[String]) -> String {
    spelled
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

fn render_key_parts(key: &str) -> String {
    key.split('.')
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

struct CallSite {
    parts: Vec<String>,
    start: Location,
}

impl CallSite {
    fn matches(&self, key: &str) -> bool {
        self.parts.join(".").to_lowercase() == key
    }
}

fn locate_call_site(sql: &str, key: &str) -> Option<CallSite> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let mut index = 0;
    while index < tokens.len() {
        if let Some(site) = match_call_at(&tokens, index)
            && site.matches(key)
        {
            return Some(site);
        }
        index += 1;
    }
    None
}

fn match_call_at(tokens: &[TokenWithSpan], index: usize) -> Option<CallSite> {
    let mut parts = vec![word_value(&tokens.get(index)?.token)?];
    let mut cursor = skip_trivia(tokens, index + 1);
    while tokens
        .get(cursor)
        .is_some_and(|next| next.token == Token::Period)
    {
        cursor = skip_trivia(tokens, cursor + 1);
        parts.push(word_value(&tokens.get(cursor)?.token)?);
        cursor = skip_trivia(tokens, cursor + 1);
    }
    if tokens
        .get(cursor)
        .is_some_and(|next| next.token == Token::LParen)
    {
        Some(CallSite {
            parts,
            start: tokens[index].span.start,
        })
    } else {
        None
    }
}

fn skip_trivia(tokens: &[TokenWithSpan], mut cursor: usize) -> usize {
    while tokens
        .get(cursor)
        .is_some_and(|next| matches!(next.token, Token::Whitespace(_)))
    {
        cursor += 1;
    }
    cursor
}

fn word_value(token: &Token) -> Option<String> {
    if let Token::Word(word) = token {
        Some(word.value.clone())
    } else {
        None
    }
}

fn position_suffix(start: Location) -> String {
    format!(
        "; line {} pos {}",
        start.line,
        start.column.saturating_sub(1)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid(name: &str) -> String {
        format!("Error during planning: Invalid function '{name}'.\nDid you mean 'cosh'?")
    }

    #[test]
    fn arbitrary_unknown_name_maps_blanket() {
        let mapped =
            map_unknown_routine_message("SELECT zz_quux_9(1)", &invalid("zz_quux_9")).unwrap();
        assert_eq!(
            mapped,
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `zz_quux_9` on search path \
             [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; \
             line 1 pos 7"
        );
    }

    #[test]
    fn suggestion_tail_does_not_leak_into_name() {
        let mapped = map_unknown_routine_message(
            "SELECT nosuchfn(1)",
            "Error during planning: Invalid function 'nosuchfn'.\nDid you mean 'count'?",
        )
        .unwrap();
        assert!(mapped.contains("`nosuchfn`") && !mapped.contains("count"));
    }

    #[test]
    fn user_case_survives_lowercase_fold() {
        let mapped =
            map_unknown_routine_message("SELECT NoSuchFn(1)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.contains("`NoSuchFn`"), "{mapped}");
    }

    #[test]
    fn string_literal_case_does_not_leak_into_name() {
        let mapped =
            map_unknown_routine_message("SELECT 'NoSuchFn', nosuchfn(1)", &invalid("nosuchfn"))
                .unwrap();
        assert!(mapped.contains("`nosuchfn`"), "{mapped}");
        assert!(!mapped.contains("NoSuchFn"), "{mapped}");
        assert!(mapped.ends_with("; line 1 pos 19"), "{mapped}");
    }

    #[test]
    fn string_literal_call_decoy_does_not_win_position() {
        let mapped =
            map_unknown_routine_message("SELECT 'nosuchfn(', nosuchfn(1)", &invalid("nosuchfn"))
                .unwrap();
        assert!(mapped.ends_with("; line 1 pos 20"), "{mapped}");
    }

    #[test]
    fn block_comment_call_decoy_does_not_win_position() {
        let mapped =
            map_unknown_routine_message("SELECT /* nosuchfn( */ nosuchfn(1)", &invalid("nosuchfn"))
                .unwrap();
        assert!(mapped.ends_with("; line 1 pos 23"), "{mapped}");
    }

    #[test]
    fn line_comment_call_decoy_does_not_win_position() {
        let mapped =
            map_unknown_routine_message("SELECT -- nosuchfn(\n nosuchfn(1)", &invalid("nosuchfn"))
                .unwrap();
        assert!(mapped.ends_with("; line 2 pos 1"), "{mapped}");
    }

    #[test]
    fn quoted_name_renders_without_doubled_backticks() {
        let mapped =
            map_unknown_routine_message("SELECT `nosuchfn`(1)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.contains("`nosuchfn` on search path"), "{mapped}");
    }

    #[test]
    fn quoted_call_positions_at_opening_backtick() {
        let mapped =
            map_unknown_routine_message("SELECT `nosuchfn`(1)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn quoted_three_part_name_quotes_each_part_once() {
        let mapped = map_unknown_routine_message(
            "SELECT `spark_catalog`.`default`.`nosuchfn`(1)",
            &invalid("`spark_catalog`.`default`.`nosuchfn`"),
        )
        .unwrap();
        assert!(
            mapped.contains("`spark_catalog`.`default`.`nosuchfn`"),
            "{mapped}"
        );
        assert!(!mapped.contains("``"), "{mapped}");
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn quoted_two_part_name_quotes_each_part_once() {
        let mapped =
            map_unknown_routine_message("SELECT `nosuch`.`fn`(1)", &invalid("`nosuch`.`fn`"))
                .unwrap();
        assert!(mapped.contains("`nosuch`.`fn`"), "{mapped}");
        assert!(!mapped.contains("``"), "{mapped}");
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn single_dotted_quoted_ident_stays_one_part() {
        let mapped =
            map_unknown_routine_message("SELECT `nosuch.fn`(1)", &invalid("`nosuch.fn`")).unwrap();
        assert!(mapped.contains("`nosuch.fn` on search path"), "{mapped}");
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn three_part_name_quotes_each_part() {
        let mapped = map_unknown_routine_message(
            "SELECT spark_catalog.default.nosuchfn(1)",
            &invalid("spark_catalog.default.nosuchfn"),
        )
        .unwrap();
        assert!(
            mapped.contains("`spark_catalog`.`default`.`nosuchfn`"),
            "{mapped}"
        );
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn two_part_name_quotes_each_part() {
        let mapped =
            map_unknown_routine_message("SELECT nosuch.fn(1)", &invalid("nosuch.fn")).unwrap();
        assert!(mapped.contains("`nosuch`.`fn`"), "{mapped}");
    }

    #[test]
    fn backticked_system_builtin_qualifier_needs_single_part_namespace() {
        let mapped = map_unknown_routine_message(
            "SELECT `system`.`builtin`.`nosuchfn`(1)",
            &invalid("`system`.`builtin`.`nosuchfn`"),
        )
        .unwrap();
        assert_eq!(
            mapped,
            "[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, \
             but got `system`.`builtin`. SQLSTATE: 42K05"
        );
    }

    #[test]
    fn system_builtin_qualifier_needs_single_part_namespace() {
        let mapped = map_unknown_routine_message(
            "SELECT system.builtin.nosuchfn(1)",
            &invalid("system.builtin.nosuchfn"),
        )
        .unwrap();
        assert_eq!(
            mapped,
            "[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, \
             but got `system`.`builtin`. SQLSTATE: 42K05"
        );
    }

    #[test]
    fn nested_call_positions_at_outer_name() {
        let mapped = map_unknown_routine_message(
            "SELECT abs(1), NOSUCHFN(nosuchfn(1))",
            &invalid("nosuchfn"),
        )
        .unwrap();
        assert!(mapped.contains("`NOSUCHFN`"), "{mapped}");
        assert!(mapped.ends_with("; line 1 pos 15"), "{mapped}");
    }

    #[test]
    fn repeated_call_positions_at_first_name() {
        let mapped =
            map_unknown_routine_message("SELECT nosuchfn(1), nosuchfn(2)", &invalid("nosuchfn"))
                .unwrap();
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn unicode_literal_keeps_code_point_columns() {
        let mapped =
            map_unknown_routine_message("SELECT 'é', nosuchfn(1)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 1 pos 12"), "{mapped}");
        let mapped =
            map_unknown_routine_message("SELECT '😀', nosuchfn(1)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 1 pos 12"), "{mapped}");
    }

    #[test]
    fn multiline_sql_positions_at_name_line() {
        let mapped =
            map_unknown_routine_message("SELECT 1,\n nosuchfn(2)", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 2 pos 1"), "{mapped}");
    }

    #[test]
    fn zero_arg_call_positions_at_name() {
        let mapped =
            map_unknown_routine_message("SELECT nosuchfn()", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 1 pos 7"), "{mapped}");
    }

    #[test]
    fn unknown_table_valued_function_names_spark_class() {
        let mapped = map_unknown_routine_message(
            "SELECT * FROM nosuchtvf(1)",
            "Error during planning: table function 'nosuchtvf' not found",
        )
        .unwrap();
        assert_eq!(
            mapped,
            "[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve `nosuchtvf` to a \
             table-valued function.\nPlease make sure that `nosuchtvf` is defined as a \
             table-valued function and that all required parameters are provided \
             correctly.\nIf `nosuchtvf` is not defined, please create the table-valued function \
             before using it.\nFor more information about defining table-valued functions, please \
             refer to the Apache Spark documentation. SQLSTATE: 42883; line 1 pos 14"
        );
    }

    #[test]
    fn tokenize_failure_falls_back_without_position() {
        let mapped =
            map_unknown_routine_message("SELECT 'unterminated", &invalid("nosuchfn")).unwrap();
        assert!(mapped.contains("`nosuchfn` on search path"), "{mapped}");
        assert!(!mapped.contains("; line"), "{mapped}");
    }

    #[test]
    fn unmatched_name_falls_back_without_position() {
        let mapped = map_unknown_routine_message("SELECT 1", &invalid("nosuchfn")).unwrap();
        assert!(mapped.contains("`nosuchfn` on search path"), "{mapped}");
        assert!(!mapped.contains("; line"), "{mapped}");
    }

    #[test]
    fn unrelated_errors_pass_through() {
        assert_eq!(
            map_unknown_routine_message("SELECT a FROM t", "Schema error: No field named a."),
            None
        );
        assert_eq!(
            map_unknown_routine_message(
                "SELECT a FROM t",
                "This feature is not implemented: LATERAL VIEWS"
            ),
            None
        );
    }

    #[test]
    fn degenerate_names_pass_through() {
        assert_eq!(
            map_unknown_routine_message("SELECT 1", "Invalid function ''"),
            None
        );
        assert_eq!(
            map_unknown_routine_message("SELECT 1", "Invalid function '.'"),
            None
        );
    }
}
