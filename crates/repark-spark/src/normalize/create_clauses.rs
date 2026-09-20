use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CreateClauses {
    pub(crate) comment: Option<String>,
}

pub(crate) fn extract_create_clauses(tokens: &[Token]) -> (Vec<Token>, CreateClauses) {
    let boundary = super::ctas_as_boundary(tokens);
    let mut clauses = CreateClauses::default();
    let mut kept = Vec::with_capacity(tokens.len());
    let mut depth = 0usize;
    let mut index = 0usize;
    while index < tokens.len() {
        match &tokens[index] {
            Token::LParen => {
                depth += 1;
                kept.push(tokens[index].clone());
                index += 1;
            }
            Token::RParen => {
                depth = depth.saturating_sub(1);
                kept.push(tokens[index].clone());
                index += 1;
            }
            Token::Word(word)
                if index < boundary
                    && depth == 0
                    && word.quote_style.is_none()
                    && word.keyword == Keyword::COMMENT =>
            {
                if let Some((literal, end)) = match_comment_literal(tokens, index + 1) {
                    clauses.comment = Some(literal);
                    index = end;
                } else {
                    kept.push(tokens[index].clone());
                    index += 1;
                }
            }
            _ => {
                kept.push(tokens[index].clone());
                index += 1;
            }
        }
    }
    (kept, clauses)
}

fn match_comment_literal(tokens: &[Token], start: usize) -> Option<(String, usize)> {
    let mut cursor = skip_whitespace(tokens, start);
    if matches!(tokens.get(cursor), Some(Token::Eq)) {
        cursor = skip_whitespace(tokens, cursor + 1);
    }
    match tokens.get(cursor) {
        Some(Token::SingleQuotedString(literal)) => Some((literal.clone(), cursor + 1)),
        _ => None,
    }
}

fn skip_whitespace(tokens: &[Token], start: usize) -> usize {
    let mut cursor = start;
    while cursor < tokens.len() && matches!(tokens[cursor], Token::Whitespace(_)) {
        cursor += 1;
    }
    cursor
}

pub(crate) fn strip_create_table_using(tokens: &[Token]) -> Vec<Token> {
    let boundary = super::ctas_as_boundary(tokens);
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let is_using = matches!(&tokens[i], Token::Word(word) if word.keyword == Keyword::USING);
        if i < boundary && is_using {
            let mut j = i + 1;
            while j < tokens.len() && matches!(tokens[j], Token::Whitespace(_)) {
                j += 1;
            }
            if j < tokens.len() && matches!(tokens[j], Token::Word(_)) {
                i = j + 1;
                continue;
            }
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    fn extract(sql: &str) -> (String, CreateClauses) {
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let (kept, clauses) = extract_create_clauses(&tokens);
        let rendered = kept.iter().map(ToString::to_string).collect::<String>();
        (rendered, clauses)
    }

    #[test]
    fn strips_comment_before_tblproperties() {
        let (rendered, clauses) = extract(
            "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg COMMENT 'tbl' \
             TBLPROPERTIES ('k' = 'v')",
        );
        assert_eq!(clauses.comment.as_deref(), Some("tbl"));
        assert!(!rendered.contains("COMMENT"), "got: {rendered}");
        assert!(rendered.contains("TBLPROPERTIES"), "got: {rendered}");
    }

    #[test]
    fn strips_comment_after_tblproperties() {
        let (rendered, clauses) = extract(
            "CREATE TABLE ice.ns.t USING iceberg TBLPROPERTIES ('k' = 'v') COMMENT 'tbl' \
             AS SELECT 1 AS i",
        );
        assert_eq!(clauses.comment.as_deref(), Some("tbl"));
        assert!(!rendered.contains("COMMENT"), "got: {rendered}");
        assert!(rendered.contains("AS SELECT"), "got: {rendered}");
    }

    #[test]
    fn leaves_column_comments_bare_names_and_select_alone() {
        let (rendered, clauses) = extract(
            "CREATE TABLE ice.ns.comment (id BIGINT COMMENT 'col', data STRING) USING iceberg \
             AS SELECT 'comment' AS data",
        );
        assert_eq!(clauses.comment, None);
        assert!(rendered.contains("COMMENT 'col'"), "got: {rendered}");
        assert!(rendered.contains("ice.ns.comment"), "got: {rendered}");
        assert!(rendered.contains("AS SELECT"), "got: {rendered}");
    }

    #[test]
    fn last_comment_wins() {
        let (_, clauses) = extract(
            "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg COMMENT 'first' COMMENT 'second'",
        );
        assert_eq!(clauses.comment.as_deref(), Some("second"));
    }
}
