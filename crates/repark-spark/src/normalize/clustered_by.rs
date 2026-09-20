use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Whitespace, Word};

pub(crate) fn rewrite_clustered_by(tokens: &[Token]) -> Vec<Token> {
    let boundary = super::ctas_as_boundary(tokens);
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    for window in significant.windows(8) {
        if window[7] >= boundary || !is_clustered_run(tokens, window) {
            continue;
        }
        let mut out = Vec::with_capacity(tokens.len() + 8);
        out.extend_from_slice(&tokens[..window[0]]);
        out.push(keyword_token("PARTITIONED", Keyword::PARTITIONED));
        out.push(Token::Whitespace(Whitespace::Space));
        out.push(keyword_token("BY", Keyword::BY));
        out.push(Token::Whitespace(Whitespace::Space));
        out.push(Token::LParen);
        out.push(keyword_token("bucket", Keyword::NoKeyword));
        out.push(Token::LParen);
        out.push(tokens[window[6]].clone());
        out.push(Token::Comma);
        out.push(Token::Whitespace(Whitespace::Space));
        out.push(tokens[window[3]].clone());
        out.push(Token::RParen);
        out.push(Token::RParen);
        out.extend_from_slice(&tokens[window[7] + 1..]);
        return out;
    }
    tokens.to_vec()
}

fn is_clustered_run(tokens: &[Token], window: &[usize]) -> bool {
    let [
        clustered,
        by,
        open,
        column,
        close,
        into,
        buckets,
        buckets_word,
    ] = window
    else {
        return false;
    };
    word_is(&tokens[*clustered], "CLUSTERED")
        && word_is(&tokens[*by], "BY")
        && matches!(tokens[*open], Token::LParen)
        && matches!(
            tokens[*column],
            Token::Word(_) | Token::DoubleQuotedString(_)
        )
        && matches!(tokens[*close], Token::RParen)
        && word_is(&tokens[*into], "INTO")
        && matches!(tokens[*buckets], Token::Number(_, _))
        && word_is(&tokens[*buckets_word], "BUCKETS")
}

fn word_is(token: &Token, value: &str) -> bool {
    match token {
        Token::Word(word) => word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value),
        _ => false,
    }
}

fn keyword_token(value: &str, keyword: Keyword) -> Token {
    Token::Word(Word {
        value: value.to_string(),
        quote_style: None,
        keyword,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

    use super::rewrite_clustered_by;

    fn tokenize(sql: &str) -> Vec<Token> {
        Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap_or_else(|error| panic!("{sql:?} must tokenize: {error}"))
    }

    fn render(tokens: &[Token]) -> String {
        tokens.iter().map(ToString::to_string).collect::<String>()
    }

    #[test]
    fn clustered_by_rewrites_to_partitioned_by_bucket() {
        let sql = "CREATE TABLE ice.ns.t (id BIGINT, data STRING) CLUSTERED BY (id) \
         INTO 4 BUCKETS";
        assert_eq!(
            render(&rewrite_clustered_by(&tokenize(sql))),
            "CREATE TABLE ice.ns.t (id BIGINT, data STRING) PARTITIONED BY (bucket(4, id))"
        );
        let lower = "CREATE TABLE ice.ns.t (id BIGINT) clustered by (id) into 4 buckets";
        assert_eq!(
            render(&rewrite_clustered_by(&tokenize(lower))),
            "CREATE TABLE ice.ns.t (id BIGINT) PARTITIONED BY (bucket(4, id))"
        );
    }

    #[test]
    fn trailing_clauses_survive_the_splice() {
        let sql = "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) INTO 4 BUCKETS \
         TBLPROPERTIES ('k'='v')";
        assert_eq!(
            render(&rewrite_clustered_by(&tokenize(sql))),
            "CREATE TABLE ice.ns.t (id BIGINT) PARTITIONED BY (bucket(4, id)) TBLPROPERTIES \
             ('k'='v')"
        );
    }

    #[test]
    fn non_clustered_create_statements_stay_byte_identical() {
        for sql in [
            "CREATE TABLE ice.ns.t (id BIGINT, data STRING)",
            "CREATE TABLE ice.ns.t (id BIGINT) PARTITIONED BY (bucket(4, id))",
            "CREATE TABLE ice.ns.t USING iceberg AS SELECT id FROM src",
            "CREATE TABLE ice.ns.t (id BIGINT) TBLPROPERTIES ('k'='v')",
            "CREATE TABLE \"clustered\" (id BIGINT)",
            "ALTER TABLE ice.ns.t ADD COLUMN c1 INT",
        ] {
            let tokens = tokenize(sql);
            assert_eq!(rewrite_clustered_by(&tokens), tokens, "{sql:?}");
        }
    }

    #[test]
    fn unsupported_clustered_shapes_pass_through_untouched() {
        for sql in [
            "CREATE TABLE ice.ns.t (a INT, b INT) CLUSTERED BY (a, b) INTO 4 BUCKETS",
            "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) SORTED BY (id) INTO 4 BUCKETS",
            "SELECT clustered FROM t",
        ] {
            let tokens = tokenize(sql);
            assert_eq!(rewrite_clustered_by(&tokens), tokens, "{sql:?}");
        }
    }
}
