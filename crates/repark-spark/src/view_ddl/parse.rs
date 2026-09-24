use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::catalog_ops::{name_parts, sqlparser_err};
use crate::describe_show::tokenize_with_spans;
use crate::namespace_ddl::{
    consume_word, parse_namespace_property_list, parse_namespace_property_string,
};

pub(crate) const ALTER_VIEW_AS_REFUSAL: &str =
    "ALTER VIEW <viewName> AS is not supported. Use CREATE OR REPLACE VIEW instead";

pub(crate) struct CreateViewStatement {
    pub(crate) or_replace: bool,
    pub(crate) if_not_exists: bool,
    pub(crate) name: Vec<String>,
    pub(crate) aliases: Vec<(String, Option<String>)>,
    pub(crate) comment: Option<String>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) body_sql: String,
}

pub(crate) struct ShowViewsStatement {
    pub(crate) namespace: Vec<String>,
    pub(crate) like: Option<String>,
}

pub(crate) struct ShowTblpropertiesStatement {
    pub(crate) name: Vec<String>,
    pub(crate) key: Option<String>,
}

pub(crate) struct AlterViewStatement {
    pub(crate) name: Vec<String>,
    pub(crate) action: AlterViewAction,
}

pub(crate) enum AlterViewAction {
    SetProperties(HashMap<String, String>),
    UnsetProperties { keys: Vec<String>, if_exists: bool },
    RenameTo(Vec<String>),
}

pub(crate) fn try_parse_create_view(sql: &str) -> Option<Result<CreateViewStatement>> {
    if !is_create_view_statement(sql) {
        return None;
    }
    Some(parse_create_view_after_head(sql))
}

pub(crate) fn is_create_view_statement(sql: &str) -> bool {
    is_durable_create_view_head(sql)
}

fn is_durable_create_view_head(sql: &str) -> bool {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize()
        .unwrap_or_default();
    let words = tokens
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) if word.quote_style.is_none() => Some(word.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut position = 0;
    if !consume_head_word(&words, &mut position, "CREATE") {
        return false;
    }
    if consume_head_word(&words, &mut position, "OR")
        && !consume_head_word(&words, &mut position, "REPLACE")
    {
        return false;
    }
    if position < words.len()
        && (words[position].eq_ignore_ascii_case("TEMPORARY")
            || words[position].eq_ignore_ascii_case("TEMP"))
    {
        return false;
    }
    position < words.len() && words[position].eq_ignore_ascii_case("VIEW")
}

fn consume_head_word(words: &[&str], position: &mut usize, expected: &str) -> bool {
    if *position < words.len() && words[*position].eq_ignore_ascii_case(expected) {
        *position += 1;
        true
    } else {
        false
    }
}

fn parse_create_view_after_head(sql: &str) -> Result<CreateViewStatement> {
    let (tokens, _, ends) = tokenize_with_spans(sql)
        .ok_or_else(|| DataFusionError::Plan("could not tokenize CREATE VIEW".to_string()))?;
    let as_index = first_unquoted_word(&tokens, "AS").ok_or_else(|| {
        DataFusionError::Plan(format!(
            "could not parse `CREATE VIEW`: expected AS with the view query, got `{sql}`"
        ))
    })?;
    let as_end = ends.get(as_index).copied().unwrap_or(sql.len());
    let body_sql = body_after_as(sql, as_end)?;
    let header = tokens.get(..as_index).unwrap_or(&[]).to_vec();
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(header);
    parse_create_view_header(&mut parser, body_sql)
}

fn first_unquoted_word(tokens: &[Token], expected: &str) -> Option<usize> {
    tokens.iter().position(|token| match token {
        Token::Word(word) => {
            word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)
        }
        _ => false,
    })
}

fn body_after_as(sql: &str, as_end: usize) -> Result<String> {
    let rest = sql.get(as_end..).ok_or_else(|| {
        DataFusionError::Plan(
            "could not parse `CREATE VIEW`: the view query is missing".to_string(),
        )
    })?;
    let trimmed = rest.trim();
    let without_semicolon = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    if without_semicolon.is_empty() {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE VIEW`: the view query after AS is empty".to_string(),
        ));
    }
    Ok(without_semicolon.to_string())
}

fn parse_create_view_header(parser: &mut Parser, body_sql: String) -> Result<CreateViewStatement> {
    if !parser.parse_keyword(Keyword::CREATE) {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE VIEW`: expected CREATE".to_string(),
        ));
    }
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    if !consume_word(parser, "VIEW") {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE VIEW`: expected VIEW".to_string(),
        ));
    }
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let object_name = parser.parse_object_name(false).map_err(sqlparser_err)?;
    let name = name_parts(&object_name);
    if name.len() > 3
        && name
            .last()
            .is_some_and(|suffix| crate::is_metadata_table_name(suffix))
    {
        return Err(DataFusionError::Plan(format!(
            "Iceberg metadata table `{}` is read-only — INSERT/UPDATE/DELETE/MERGE/\
             CTAS/TRUNCATE/CREATE VIEW/DROP/ALTER targeting a metadata table is not supported",
            name.join(".")
        )));
    }
    if name.is_empty() || name.len() > 3 {
        return Err(DataFusionError::Plan(format!(
            "could not parse `CREATE VIEW`: expected a [catalog.[namespace.]]view name, got `{}`",
            name.join(".")
        )));
    }
    let aliases = parse_view_aliases(parser)?;
    let mut comment = None;
    if parser.parse_keyword(Keyword::COMMENT) {
        comment = Some(parser.parse_literal_string().map_err(sqlparser_err)?);
    }
    let mut properties = HashMap::new();
    if consume_word(parser, "TBLPROPERTIES") {
        parse_namespace_property_list(parser, &mut properties, "CREATE VIEW")?;
    }
    expect_end(parser, "CREATE VIEW")?;
    Ok(CreateViewStatement {
        or_replace,
        if_not_exists,
        name,
        aliases,
        comment,
        properties,
        body_sql,
    })
}

fn parse_view_aliases(parser: &mut Parser) -> Result<Vec<(String, Option<String>)>> {
    if !parser.consume_token(&Token::LParen) {
        return Ok(Vec::new());
    }
    let mut aliases = Vec::new();
    loop {
        let alias = parser.parse_identifier().map_err(sqlparser_err)?.value;
        let mut comment = None;
        if parser.parse_keyword(Keyword::COMMENT) {
            comment = Some(parser.parse_literal_string().map_err(sqlparser_err)?);
        }
        aliases.push((alias, comment));
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen).map_err(sqlparser_err)?;
    Ok(aliases)
}

fn expect_end(parser: &mut Parser, statement: &str) -> Result<()> {
    if matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "could not parse `{statement}` at `{}`",
        parser.peek_token()
    )))
}

pub(crate) fn try_parse_alter_view_as(sql: &str) -> Option<DataFusionError> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::ALTER) {
        return None;
    }
    if !consume_word(&mut parser, "VIEW") {
        return None;
    }
    parser.parse_object_name(false).ok()?;
    if !consume_word(&mut parser, "AS") {
        return None;
    }
    Some(DataFusionError::Plan(ALTER_VIEW_AS_REFUSAL.to_string()))
}

pub(crate) fn try_parse_alter_view(sql: &str) -> Option<Result<AlterViewStatement>> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::ALTER) {
        return None;
    }
    if !consume_word(&mut parser, "VIEW") {
        return None;
    }
    let object_name = parser.parse_object_name(false).ok()?;
    let name = name_parts(&object_name);
    let verb = match parser.peek_token().token {
        Token::Word(word) if word.quote_style.is_none() => word.value,
        _ => return None,
    };
    if !verb.eq_ignore_ascii_case("SET")
        && !verb.eq_ignore_ascii_case("UNSET")
        && !verb.eq_ignore_ascii_case("RENAME")
    {
        return None;
    }
    Some(parse_alter_view_tail(&mut parser, name))
}

fn parse_alter_view_tail(parser: &mut Parser, name: Vec<String>) -> Result<AlterViewStatement> {
    let action = if consume_word(parser, "SET") {
        if !consume_word(parser, "TBLPROPERTIES") {
            return Err(DataFusionError::Plan(
                "could not parse `ALTER VIEW`: expected TBLPROPERTIES after SET".to_string(),
            ));
        }
        let mut properties = HashMap::new();
        parse_namespace_property_list(parser, &mut properties, "ALTER VIEW")?;
        AlterViewAction::SetProperties(properties)
    } else if consume_word(parser, "UNSET") {
        if !consume_word(parser, "TBLPROPERTIES") {
            return Err(DataFusionError::Plan(
                "could not parse `ALTER VIEW`: expected TBLPROPERTIES after UNSET".to_string(),
            ));
        }
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let keys = parse_alter_view_unset_keys(parser)?;
        AlterViewAction::UnsetProperties { keys, if_exists }
    } else if consume_word(parser, "RENAME") {
        if !consume_word(parser, "TO") {
            return Err(DataFusionError::Plan(
                "could not parse `ALTER VIEW`: expected TO after RENAME".to_string(),
            ));
        }
        let target = parser.parse_object_name(false).map_err(sqlparser_err)?;
        AlterViewAction::RenameTo(name_parts(&target))
    } else {
        return Err(DataFusionError::Plan(
            "could not parse `ALTER VIEW`: expected SET, UNSET or RENAME".to_string(),
        ));
    };
    expect_end(parser, "ALTER VIEW")?;
    Ok(AlterViewStatement { name, action })
}

fn parse_alter_view_unset_keys(parser: &mut Parser) -> Result<Vec<String>> {
    parser.expect_token(&Token::LParen).map_err(sqlparser_err)?;
    let mut keys = Vec::new();
    if parser.consume_token(&Token::RParen) {
        return Ok(keys);
    }
    loop {
        keys.push(parse_namespace_property_string(parser, "ALTER VIEW")?);
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen).map_err(sqlparser_err)?;
    Ok(keys)
}

pub(crate) fn try_parse_show_views(sql: &str) -> Option<Result<ShowViewsStatement>> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::SHOW) {
        return None;
    }
    if !consume_word(&mut parser, "VIEWS") {
        return None;
    }
    Some(parse_show_views_after_head(&mut parser))
}

fn parse_show_views_after_head(parser: &mut Parser) -> Result<ShowViewsStatement> {
    let mut namespace = Vec::new();
    if parser.parse_keyword(Keyword::IN) {
        let object_name = parser.parse_object_name(false).map_err(sqlparser_err)?;
        namespace = name_parts(&object_name);
        if namespace.is_empty() || namespace.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "expected a two-part `IN <catalog.namespace>` name, got `{}`",
                namespace.join(".")
            )));
        }
    }
    let had_like = parser.parse_keyword(Keyword::LIKE);
    let like = match &parser.peek_token().token {
        Token::SingleQuotedString(pattern) | Token::DoubleQuotedString(pattern) => {
            let pattern = pattern.clone();
            parser.next_token();
            Some(pattern)
        }
        _ => None,
    };
    let like = match (had_like, like) {
        (true, None) => {
            return Err(DataFusionError::Plan(
                "SHOW VIEWS … LIKE needs a quoted pattern (e.g. SHOW VIEWS IN cat.ns LIKE \
                 'v*')"
                    .to_string(),
            ));
        }
        (_, pattern) => pattern,
    };
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Err(DataFusionError::Plan(format!(
            "could not parse `SHOW VIEWS` at `{}` — the supported form is SHOW VIEWS IN \
             <catalog.namespace> [LIKE] ['pattern']",
            parser.peek_token()
        )));
    }
    if namespace.is_empty() {
        return Err(DataFusionError::Plan(
            "SHOW VIEWS requires an explicit namespace — `SHOW VIEWS IN <catalog.namespace>` \
             (RePark has no current-catalog concept, so there is no default to resolve against)"
                .to_string(),
        ));
    }
    Ok(ShowViewsStatement { namespace, like })
}

pub(crate) fn try_parse_show_tblproperties(
    sql: &str,
) -> Option<Result<ShowTblpropertiesStatement>> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::SHOW) {
        return None;
    }
    if !consume_word(&mut parser, "TBLPROPERTIES") {
        return None;
    }
    Some(parse_show_tblproperties_after_head(&mut parser))
}

fn parse_show_tblproperties_after_head(parser: &mut Parser) -> Result<ShowTblpropertiesStatement> {
    let object_name = parser.parse_object_name(false).map_err(sqlparser_err)?;
    let name = name_parts(&object_name);
    let mut key = None;
    if parser.consume_token(&Token::LParen) {
        key = Some(parse_show_tblproperties_key(parser)?);
        parser.expect_token(&Token::RParen).map_err(sqlparser_err)?;
    }
    expect_end(parser, "SHOW TBLPROPERTIES")?;
    Ok(ShowTblpropertiesStatement { name, key })
}

fn parse_show_tblproperties_key(parser: &mut Parser) -> Result<String> {
    if let Token::SingleQuotedString(value) = parser.peek_token().token.clone() {
        parser.next_token();
        return Ok(value);
    }
    let object_name = parser.parse_object_name(false).map_err(sqlparser_err)?;
    Ok(name_parts(&object_name).join("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn created(sql: &str) -> CreateViewStatement {
        try_parse_create_view(sql)
            .unwrap_or_else(|| panic!("must match: {sql}"))
            .unwrap_or_else(|error| panic!("must parse: {sql}: {error}"))
    }

    #[test]
    fn plain_create_view_parses_with_verbatim_body() {
        let parsed = created("CREATE VIEW sc.ns.vw AS SELECT id, data FROM t WHERE id > 0");
        assert!(!parsed.or_replace);
        assert!(!parsed.if_not_exists);
        assert_eq!(parsed.name, vec!["sc", "ns", "vw"]);
        assert!(parsed.aliases.is_empty());
        assert_eq!(parsed.comment, None);
        assert!(parsed.properties.is_empty());
        assert_eq!(parsed.body_sql, "SELECT id, data FROM t WHERE id > 0");
    }

    #[test]
    fn or_replace_if_not_exists_and_aliases_parse() {
        let parsed = created(
            "CREATE OR REPLACE VIEW IF NOT EXISTS sc.ns.vw (i, d) AS SELECT id, data FROM t",
        );
        assert!(parsed.or_replace);
        assert!(parsed.if_not_exists);
        assert_eq!(
            parsed.aliases,
            vec![("i".to_string(), None), ("d".to_string(), None)]
        );
        assert_eq!(parsed.body_sql, "SELECT id, data FROM t");
    }

    #[test]
    fn alias_list_with_comment_parses() {
        let parsed = created(
            "CREATE VIEW sc.ns.vp (i COMMENT 'the id', d) COMMENT 'v' AS SELECT id, data FROM t",
        );
        assert_eq!(
            parsed.aliases,
            vec![
                ("i".to_string(), Some("the id".to_string())),
                ("d".to_string(), None)
            ]
        );
        assert_eq!(parsed.comment, Some("v".to_string()));
        assert_eq!(parsed.body_sql, "SELECT id, data FROM t");
    }

    #[test]
    fn tblproperties_and_trailing_semicolon_parse() {
        let parsed =
            created("CREATE VIEW sc.ns.vp TBLPROPERTIES ('k'='v', 'n'='1') AS SELECT id FROM t;");
        assert_eq!(parsed.properties.get("k"), Some(&"v".to_string()));
        assert_eq!(parsed.properties.get("n"), Some(&"1".to_string()));
        assert_eq!(parsed.body_sql, "SELECT id FROM t");
    }

    #[test]
    fn body_keeps_inner_as_and_version_clause_verbatim() {
        let parsed =
            created("CREATE VIEW sc.ns.vt AS SELECT id FROM t VERSION AS OF 'main' ORDER BY id");
        assert_eq!(
            parsed.body_sql,
            "SELECT id FROM t VERSION AS OF 'main' ORDER BY id"
        );
    }

    #[test]
    fn unqualified_and_two_part_names_parse() {
        let parsed = created("CREATE VIEW vw AS SELECT 1 AS id");
        assert_eq!(parsed.name, vec!["vw"]);
        let parsed = created("CREATE VIEW ns.vw AS SELECT 1 AS id");
        assert_eq!(parsed.name, vec!["ns", "vw"]);
    }

    #[test]
    fn quoted_names_and_aliases_parse() {
        let parsed = created("CREATE VIEW `sc`.`ns`.`v w` (`i d`) AS SELECT 1 AS `i d`");
        assert_eq!(parsed.name, vec!["sc", "ns", "v w"]);
        assert_eq!(parsed.aliases, vec![("i d".to_string(), None)]);
    }

    #[test]
    fn temporary_forms_do_not_match() {
        assert!(try_parse_create_view("CREATE TEMPORARY VIEW v AS SELECT 1").is_none());
        assert!(try_parse_create_view("CREATE TEMP VIEW v AS SELECT 1").is_none());
        assert!(try_parse_create_view("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1").is_none());
    }

    #[test]
    fn non_view_statements_do_not_match() {
        assert!(try_parse_create_view("CREATE TABLE t (id INT)").is_none());
        assert!(try_parse_create_view("CREATE OR REPLACE TABLE t AS SELECT 1").is_none());
        assert!(try_parse_create_view("SELECT 1").is_none());
        assert!(try_parse_create_view("DROP VIEW v").is_none());
    }

    #[test]
    fn create_view_statement_sniff_matches_durable_only() {
        assert!(is_create_view_statement(
            "CREATE VIEW sc.ns.v AS SELECT 1 AS id"
        ));
        assert!(is_create_view_statement(
            "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1 AS id"
        ));
        assert!(!is_create_view_statement(
            "CREATE TEMPORARY VIEW v AS SELECT 1 AS id"
        ));
        assert!(!is_create_view_statement("CREATE TABLE t (id INT)"));
        assert!(!is_create_view_statement("SELECT 1"));
    }

    #[test]
    fn malformed_create_view_fails_loud() {
        assert!(try_parse_create_view("CREATE VIEW v AS").is_some_and(|parsed| parsed.is_err()));
        assert!(
            try_parse_create_view("CREATE VIEW v SELECT 1").is_some_and(|parsed| parsed.is_err())
        );
        assert!(
            try_parse_create_view("CREATE VIEW a.b.c.d AS SELECT 1")
                .is_some_and(|parsed| parsed.is_err())
        );
    }

    #[test]
    fn alter_view_as_matches_only_the_as_shape() {
        let error = try_parse_alter_view_as("ALTER VIEW sc.ns.va AS SELECT 1 AS id")
            .unwrap_or_else(|| panic!("must match"));
        assert_eq!(
            error.to_string(),
            format!("Error during planning: {ALTER_VIEW_AS_REFUSAL}")
        );
        assert!(try_parse_alter_view_as("ALTER TABLE t ADD COLUMNS (id INT)").is_none());
        assert!(try_parse_alter_view_as("ALTER VIEW v SET TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view_as("ALTER VIEW v UNSET TBLPROPERTIES ('k')").is_none());
        assert!(try_parse_alter_view_as("ALTER VIEW v RENAME TO w").is_none());
    }

    fn altered(sql: &str) -> AlterViewStatement {
        try_parse_alter_view(sql)
            .unwrap_or_else(|| panic!("must match: {sql}"))
            .unwrap_or_else(|error| panic!("must parse: {sql}: {error}"))
    }

    #[test]
    fn alter_view_set_tblproperties_parses() {
        let parsed = altered("ALTER VIEW sc.ns.v SET TBLPROPERTIES ('k'='v', 'n'='1')");
        assert_eq!(parsed.name, vec!["sc", "ns", "v"]);
        let AlterViewAction::SetProperties(properties) = parsed.action else {
            panic!("must be a SET action");
        };
        assert_eq!(properties.get("k"), Some(&"v".to_string()));
        assert_eq!(properties.get("n"), Some(&"1".to_string()));
    }

    #[test]
    fn alter_view_unset_tblproperties_parses_with_and_without_if_exists() {
        let parsed = altered("ALTER VIEW sc.ns.v UNSET TBLPROPERTIES ('k', 'n')");
        let AlterViewAction::UnsetProperties { keys, if_exists } = parsed.action else {
            panic!("must be an UNSET action");
        };
        assert_eq!(keys, vec!["k".to_string(), "n".to_string()]);
        assert!(!if_exists);
        let parsed = altered("ALTER VIEW sc.ns.v UNSET TBLPROPERTIES IF EXISTS ('k')");
        let AlterViewAction::UnsetProperties { keys, if_exists } = parsed.action else {
            panic!("must be an UNSET action");
        };
        assert_eq!(keys, vec!["k".to_string()]);
        assert!(if_exists);
    }

    #[test]
    fn alter_view_rename_to_parses() {
        let parsed = altered("ALTER VIEW sc.ns.v RENAME TO sc.ns.w");
        assert_eq!(parsed.name, vec!["sc", "ns", "v"]);
        let AlterViewAction::RenameTo(target) = parsed.action else {
            panic!("must be a RENAME action");
        };
        assert_eq!(target, vec!["sc", "ns", "w"]);
        let parsed = altered("ALTER VIEW v RENAME TO w");
        let AlterViewAction::RenameTo(target) = parsed.action else {
            panic!("must be a RENAME action");
        };
        assert_eq!(target, vec!["w"]);
    }

    #[test]
    fn alter_view_malformed_tails_fail_loud() {
        for (sql, expected) in [
            (
                "ALTER VIEW sc.ns.v SET TBLPROPERTIES",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: (, found: EOF",
            ),
            (
                "ALTER VIEW sc.ns.v RENAME TO",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: EOF",
            ),
            (
                "ALTER VIEW sc.ns.v UNSET TBLPROPERTIES 'k'",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: (, found: 'k'",
            ),
            (
                "ALTER VIEW sc.ns.v SET TBLPROPERTIES ('k'='v') extra",
                "Error during planning: could not parse `ALTER VIEW` at `extra`",
            ),
            (
                "ALTER VIEW v SET ('k'='v')",
                "Error during planning: could not parse `ALTER VIEW`: expected TBLPROPERTIES after SET",
            ),
            (
                "ALTER VIEW v UNSET ('k')",
                "Error during planning: could not parse `ALTER VIEW`: expected TBLPROPERTIES after UNSET",
            ),
            (
                "ALTER VIEW v RENAME v2",
                "Error during planning: could not parse `ALTER VIEW`: expected TO after RENAME",
            ),
            (
                "ALTER VIEW v RENAME TO .",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: identifier, found: .",
            ),
            (
                "ALTER VIEW v UNSET TBLPROPERTIES ('k'",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: ), found: EOF",
            ),
        ] {
            let error = try_parse_alter_view(sql)
                .expect("ALTER VIEW must match")
                .err()
                .expect("malformed tail must refuse");
            assert!(matches!(error, DataFusionError::Plan(_)), "{sql}");
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn alter_view_unset_accepts_empty_and_comma_separated_key_lists() {
        let parsed = altered("ALTER VIEW v UNSET TBLPROPERTIES ()");
        let AlterViewAction::UnsetProperties { keys, if_exists } = parsed.action else {
            panic!("must be an UNSET action");
        };
        assert!(keys.is_empty());
        assert!(!if_exists);
        let parsed = altered("ALTER VIEW v UNSET TBLPROPERTIES ('first', 'second')");
        let AlterViewAction::UnsetProperties { keys, .. } = parsed.action else {
            panic!("must be an UNSET action");
        };
        assert_eq!(keys, vec!["first", "second"]);
    }

    #[test]
    fn alter_view_tail_rejects_an_unrecognized_verb() {
        let tokens = Tokenizer::new(&DatabricksDialect {}, "OTHER")
            .tokenize()
            .expect("tokens");
        let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(tokens);
        let error = parse_alter_view_tail(&mut parser, vec!["v".to_string()])
            .err()
            .expect("unrecognized verb must refuse");
        assert!(matches!(error, DataFusionError::Plan(_)));
        assert_eq!(
            error.to_string(),
            "Error during planning: could not parse `ALTER VIEW`: expected SET, UNSET or RENAME"
        );
    }

    #[test]
    fn alter_view_tail_reports_the_missing_keyword_for_each_action() {
        for (sql, expected) in [
            (
                "ALTER VIEW v SET ('k'='v')",
                "Error during planning: could not parse `ALTER VIEW`: expected TBLPROPERTIES after SET",
            ),
            (
                "ALTER VIEW v UNSET ('k')",
                "Error during planning: could not parse `ALTER VIEW`: expected TBLPROPERTIES after UNSET",
            ),
            (
                "ALTER VIEW v RENAME v2",
                "Error during planning: could not parse `ALTER VIEW`: expected TO after RENAME",
            ),
        ] {
            let error = try_parse_alter_view(sql)
                .expect("ALTER VIEW must match")
                .err()
                .expect("malformed action must refuse");
            assert!(matches!(error, DataFusionError::Plan(_)));
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn alter_view_unset_requires_the_opening_parenthesis() {
        let error = try_parse_alter_view("ALTER VIEW v UNSET TBLPROPERTIES 'k'")
            .expect("ALTER VIEW must match")
            .err()
            .expect("missing opening parenthesis must refuse");
        assert!(matches!(error, DataFusionError::Plan(_)));
        assert_eq!(
            error.to_string(),
            "Error during planning: could not parse CREATE NAMESPACE: sql parser error: Expected: (, found: 'k'"
        );
    }

    #[test]
    fn alter_view_unsupported_shapes_do_not_match() {
        assert!(try_parse_alter_view("ALTER VIEW sc.ns.v AS SELECT 1").is_none());
        assert!(try_parse_alter_view("ALTER VIEW sc.ns.v").is_none());
        assert!(try_parse_alter_view("ALTER VIEW sc.ns.v ADD COLUMN id INT").is_none());
        assert!(try_parse_alter_view("ALTER VIEWS sc.ns.v SET TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view("ALTER VIEWX sc.ns.v SET TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view("ALTER TABLE sc.ns.t SET TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view("SELECT 1").is_none());
        assert!(try_parse_alter_view("ALTER VIEW v \"SET\" TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view("ALTER VIEW . SET TBLPROPERTIES ('k'='v')").is_none());
        assert!(try_parse_alter_view("ALTER VIEW 'unterminated").is_none());
    }

    #[test]
    fn show_views_parses_namespace_and_like() {
        let parsed = try_parse_show_views("SHOW VIEWS IN sc.ns LIKE 'vs_*'")
            .unwrap_or_else(|| panic!("must match"))
            .unwrap_or_else(|error| panic!("must parse: {error}"));
        assert_eq!(parsed.namespace, vec!["sc", "ns"]);
        assert_eq!(parsed.like, Some("vs_*".to_string()));
        let parsed = try_parse_show_views("SHOW VIEWS IN sc.ns")
            .unwrap_or_else(|| panic!("must match"))
            .unwrap_or_else(|error| panic!("must parse: {error}"));
        assert_eq!(parsed.like, None);
    }

    #[test]
    fn show_views_without_namespace_fails_loud() {
        let parsed = try_parse_show_views("SHOW VIEWS").unwrap_or_else(|| panic!("must match"));
        let Err(error) = parsed else {
            panic!("session scope needs an explicit namespace")
        };
        assert!(error.to_string().contains("requires an explicit namespace"));
        assert!(
            try_parse_show_views("SHOW VIEWS LIKE 'v*'")
                .unwrap_or_else(|| panic!("must match"))
                .is_err()
        );
        assert!(
            try_parse_show_views("SHOW VIEWS IN sc.ns LIKE vs_*")
                .unwrap_or_else(|| panic!("must match"))
                .is_err()
        );
    }

    #[test]
    fn show_tables_and_other_show_forms_do_not_match() {
        assert!(try_parse_show_views("SHOW TABLES IN sc.ns").is_none());
        assert!(try_parse_show_views("SHOW NAMESPACES IN sc").is_none());
        assert!(try_parse_show_views("SHOW CREATE TABLE sc.ns.t").is_none());
    }

    fn shown_tblproperties(sql: &str) -> ShowTblpropertiesStatement {
        try_parse_show_tblproperties(sql)
            .unwrap_or_else(|| panic!("must match: {sql}"))
            .unwrap_or_else(|error| panic!("must parse: {sql}: {error}"))
    }

    #[test]
    fn show_tblproperties_parses_name_and_quoted_key() {
        let parsed = shown_tblproperties("SHOW TBLPROPERTIES sc.ns.v ('k')");
        assert_eq!(parsed.name, vec!["sc", "ns", "v"]);
        assert_eq!(parsed.key, Some("k".to_string()));
        let parsed = shown_tblproperties("SHOW TBLPROPERTIES v ('a.b');");
        assert_eq!(parsed.name, vec!["v"]);
        assert_eq!(parsed.key, Some("a.b".to_string()));
    }

    #[test]
    fn show_tblproperties_parses_unquoted_and_dotted_keys() {
        let parsed = shown_tblproperties("SHOW TBLPROPERTIES sc.ns.v (k)");
        assert_eq!(parsed.key, Some("k".to_string()));
        let parsed = shown_tblproperties("SHOW TBLPROPERTIES sc.ns.v (a.b)");
        assert_eq!(parsed.key, Some("a.b".to_string()));
    }

    #[test]
    fn show_tblproperties_parses_without_key() {
        let parsed = shown_tblproperties("SHOW TBLPROPERTIES sc.ns.v");
        assert_eq!(parsed.name, vec!["sc", "ns", "v"]);
        assert_eq!(parsed.key, None);
        let parsed = shown_tblproperties("show tblproperties ns.v");
        assert_eq!(parsed.name, vec!["ns", "v"]);
    }

    fn show_tblproperties_refusal(sql: &str) -> DataFusionError {
        match try_parse_show_tblproperties(sql) {
            Some(Err(error)) => error,
            Some(Ok(_)) => panic!("must refuse: {sql}"),
            None => panic!("must match: {sql}"),
        }
    }

    #[test]
    fn show_tblproperties_malformed_tokenized_tails_refuse_with_parser_text() {
        for (sql, expected) in [
            (
                "SHOW TBLPROPERTIES",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: \
                 Expected: identifier, found: EOF",
            ),
            (
                "SHOW TBLPROPERTIES v (",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: \
                 Expected: identifier, found: EOF",
            ),
            (
                "SHOW TBLPROPERTIES v (,)",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: \
                 Expected: identifier, found: ,",
            ),
            (
                "SHOW TBLPROPERTIES v ('k'",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: \
                 Expected: ), found: EOF",
            ),
            (
                "SHOW TBLPROPERTIES v ('k'.x)",
                "Error during planning: could not parse CREATE NAMESPACE: sql parser error: \
                 Expected: ), found: .",
            ),
            (
                "SHOW TBLPROPERTIES v ('k') extra",
                "Error during planning: could not parse `SHOW TBLPROPERTIES` at `extra`",
            ),
            (
                "SHOW TBLPROPERTIES v 'k'",
                "Error during planning: could not parse `SHOW TBLPROPERTIES` at `'k'`",
            ),
        ] {
            let error = show_tblproperties_refusal(sql);
            assert!(
                matches!(error, DataFusionError::Plan(_)),
                "{sql}: {error:?}"
            );
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn show_tblproperties_tokenizer_failure_falls_through() {
        for sql in ["SHOW TBLPROPERTIES v ('k", "SHOW TBLPROPERTIES `v"] {
            assert!(
                Tokenizer::new(&DatabricksDialect {}, sql)
                    .tokenize()
                    .is_err(),
                "{sql}"
            );
            assert!(try_parse_show_tblproperties(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn show_tblproperties_other_show_forms_do_not_match() {
        assert!(try_parse_show_tblproperties("SHOW VIEWS IN sc.ns").is_none());
        assert!(try_parse_show_tblproperties("SHOW TABLES IN sc.ns").is_none());
        assert!(try_parse_show_tblproperties("SHOW TBLPROPERTIESX sc.ns.v").is_none());
        assert!(try_parse_show_tblproperties("SHOW COLUMNS IN sc.ns.t").is_none());
        assert!(try_parse_show_tblproperties("SELECT 1").is_none());
        assert!(try_parse_show_tblproperties("SHOW TBLPROPERTIE v").is_none());
        assert!(try_parse_show_tblproperties("TBLPROPERTIES sc.ns.v").is_none());
        assert!(try_parse_show_tblproperties("SHOW TABLE EXTENDED IN sc.ns LIKE 'v'").is_none());
    }
}
