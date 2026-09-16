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
    let parts: Vec<&str> = dotted.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }
    let qualifier = &parts[..parts.len().saturating_sub(1)];
    if qualifier.len() == 2
        && qualifier[0].eq_ignore_ascii_case("system")
        && qualifier[1].eq_ignore_ascii_case("builtin")
    {
        let first = recover_spelling(sql, qualifier[0]);
        let second = recover_spelling(sql, qualifier[1]);
        return Some(format!(
            "[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, \
             but got `{first}`.`{second}`. SQLSTATE: 42K05"
        ));
    }
    let rendered = render_parts(&spell_parts(sql, &parts));
    let position = locate_call(sql, &parts)
        .map(|offset| position_suffix(sql, offset))
        .unwrap_or_default();
    Some(format!(
        "[UNRESOLVED_ROUTINE] Cannot resolve routine {rendered} on search path {SEARCH_PATH}. \
         SQLSTATE: 42883{position}"
    ))
}

fn table_valued_function_message(sql: &str, dotted: &str) -> Option<String> {
    let parts: Vec<&str> = dotted.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }
    let rendered = render_parts(&spell_parts(sql, &parts));
    let position = locate_call(sql, &parts)
        .map(|offset| position_suffix(sql, offset))
        .unwrap_or_default();
    Some(format!(
        "[UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve {rendered} to a table-valued \
         function.\nPlease make sure that {rendered} is defined as a table-valued function and \
         that all required parameters are provided correctly.\nIf {rendered} is not defined, \
         please create the table-valued function before using it.\nFor more information about \
         defining table-valued functions, please refer to the Apache Spark documentation. \
         SQLSTATE: 42883{position}"
    ))
}

fn spell_parts(sql: &str, parts: &[&str]) -> Vec<String> {
    parts
        .iter()
        .map(|part| recover_spelling(sql, part))
        .collect()
}

fn recover_spelling(sql: &str, part: &str) -> String {
    find_ident_offsets(sql.as_bytes(), part)
        .into_iter()
        .next()
        .and_then(|offset| sql.get(offset..offset + part.len()).map(str::to_string))
        .unwrap_or_else(|| part.to_string())
}

fn render_parts(spelled: &[String]) -> String {
    spelled
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

fn locate_call(sql: &str, parts: &[&str]) -> Option<usize> {
    match_dotted_call(sql, parts)
        .or_else(|| parts.last().and_then(|last| match_single_call(sql, last)))
        .map(|offset| quote_adjust(sql, offset))
}

fn quote_adjust(sql: &str, offset: usize) -> usize {
    if offset > 0 && sql.as_bytes().get(offset - 1) == Some(&b'`') {
        offset - 1
    } else {
        offset
    }
}

fn match_dotted_call(sql: &str, parts: &[&str]) -> Option<usize> {
    let bytes = sql.as_bytes();
    let first = *parts.first()?;
    for start in find_ident_offsets(bytes, first) {
        let mut cursor = start + first.len();
        let mut matched = true;
        for part in parts.iter().skip(1) {
            cursor = skip_gap(bytes, cursor);
            if bytes.get(cursor) != Some(&b'.') {
                matched = false;
                break;
            }
            cursor = skip_gap(bytes, cursor + 1);
            if !matches_part_at(bytes, cursor, part) {
                matched = false;
                break;
            }
            cursor += part.len();
        }
        if matched && is_call_at(bytes, cursor) {
            return Some(start);
        }
    }
    None
}

fn match_single_call(sql: &str, part: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    find_ident_offsets(bytes, part)
        .into_iter()
        .find(|start| is_call_at(bytes, start + part.len()))
}

fn find_ident_offsets(haystack: &[u8], needle: &str) -> Vec<usize> {
    let pattern = needle.as_bytes();
    if pattern.is_empty() {
        return Vec::new();
    }
    let mut offsets = Vec::new();
    let mut start = 0;
    while start + pattern.len() <= haystack.len() {
        let preceded = start
            .checked_sub(1)
            .and_then(|index| haystack.get(index))
            .copied();
        if ascii_eq_ignore_case(&haystack[start..start + pattern.len()], pattern)
            && !is_ident_byte(preceded)
        {
            offsets.push(start);
        }
        start += 1;
    }
    offsets
}

fn matches_part_at(haystack: &[u8], cursor: usize, part: &str) -> bool {
    let pattern = part.as_bytes();
    haystack
        .get(cursor..cursor + pattern.len())
        .is_some_and(|window| ascii_eq_ignore_case(window, pattern))
}

fn is_call_at(bytes: &[u8], cursor: usize) -> bool {
    bytes.get(skip_gap(bytes, cursor)) == Some(&b'(')
}

fn skip_gap(bytes: &[u8], mut cursor: usize) -> usize {
    while matches!(bytes.get(cursor), Some(b' ' | b'\t' | b'\n' | b'\r' | b'`')) {
        cursor += 1;
    }
    cursor
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn is_ident_byte(byte: Option<u8>) -> bool {
    matches!(byte, Some(b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'_'))
}

fn position_suffix(sql: &str, offset: usize) -> String {
    let (line, column) = line_col(sql, offset);
    format!("; line {line} pos {column}")
}

fn line_col(sql: &str, offset: usize) -> (usize, usize) {
    let head = sql.get(..offset).unwrap_or("");
    let line = head.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = head
        .rsplit('\n')
        .next()
        .map_or(0, |last| last.chars().count());
    (line, column)
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
    fn nested_call_positions_at_inner_name() {
        let mapped =
            map_unknown_routine_message("SELECT abs(nosuchfn(1))", &invalid("nosuchfn")).unwrap();
        assert!(mapped.ends_with("; line 1 pos 11"), "{mapped}");
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
    }
}
