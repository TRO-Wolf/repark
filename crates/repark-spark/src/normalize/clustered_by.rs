use datafusion::error::Result;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Whitespace, Word};

struct BucketRun {
    first: usize,
    last: usize,
    count: Token,
    columns: Vec<Token>,
    sorts: Vec<Token>,
}

pub(crate) fn rewrite_clustered_by(tokens: &[Token]) -> Result<Vec<Token>> {
    let boundary = super::ctas_as_boundary(tokens);
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(index, token)| *index < boundary && !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    let Some(run) = significant
        .iter()
        .enumerate()
        .filter(|(_, index)| word_is(&tokens[**index], "CLUSTERED"))
        .find_map(|(start, _)| parse_bucket_run(tokens, &significant[start..]))
    else {
        return Ok(tokens.to_vec());
    };
    if run.columns.len() > 1 || !run.sorts.is_empty() {
        return Err(repark_iceberg::write::illegal_argument_error(format!(
            "Cannot convert transform with more than one column reference: {}",
            describe_run(&run)
        )));
    }
    let mut out = Vec::with_capacity(tokens.len() + 8);
    out.extend_from_slice(&tokens[..run.first]);
    let splice_at = out.len();
    out.extend_from_slice(&tokens[run.last + 1..]);
    let bucket = bucket_tokens(&run);
    if let Some(close) = partitioned_by_close(&out) {
        let mut element = vec![Token::Comma, Token::Whitespace(Whitespace::Space)];
        element.extend(bucket);
        out.splice(close..close, element);
        return Ok(out);
    }
    let mut clause = vec![
        keyword_token("PARTITIONED", Keyword::PARTITIONED),
        Token::Whitespace(Whitespace::Space),
        keyword_token("BY", Keyword::BY),
        Token::Whitespace(Whitespace::Space),
        Token::LParen,
    ];
    clause.extend(bucket);
    clause.push(Token::RParen);
    out.splice(splice_at..splice_at, clause);
    Ok(out)
}

fn parse_bucket_run(tokens: &[Token], significant: &[usize]) -> Option<BucketRun> {
    let at = |position: usize| significant.get(position).map(|index| &tokens[*index]);
    if !at(1).is_some_and(|token| word_is(token, "BY")) {
        return None;
    }
    let (columns, mut position) = parse_column_list(tokens, significant, 2, false)?;
    let mut sorts = Vec::new();
    if at(position).is_some_and(|token| word_is(token, "SORTED")) {
        if !at(position + 1).is_some_and(|token| word_is(token, "BY")) {
            return None;
        }
        (sorts, position) = parse_column_list(tokens, significant, position + 2, true)?;
    }
    if !at(position).is_some_and(|token| word_is(token, "INTO")) {
        return None;
    }
    let count = at(position + 1)?.clone();
    if !matches!(count, Token::Number(_, _))
        || !at(position + 2).is_some_and(|token| word_is(token, "BUCKETS"))
    {
        return None;
    }
    Some(BucketRun {
        first: significant[0],
        last: significant[position + 2],
        count,
        columns,
        sorts,
    })
}

fn parse_column_list(
    tokens: &[Token],
    significant: &[usize],
    open: usize,
    allow_ascending: bool,
) -> Option<(Vec<Token>, usize)> {
    if !matches!(tokens[*significant.get(open)?], Token::LParen) {
        return None;
    }
    let mut columns = Vec::new();
    let mut position = open + 1;
    loop {
        let column = &tokens[*significant.get(position)?];
        if !matches!(column, Token::Word(_) | Token::DoubleQuotedString(_)) {
            return None;
        }
        columns.push(column.clone());
        position += 1;
        if allow_ascending && word_is(&tokens[*significant.get(position)?], "ASC") {
            position += 1;
        }
        match tokens[*significant.get(position)?] {
            Token::Comma => position += 1,
            Token::RParen => return Some((columns, position + 1)),
            _ => return None,
        }
    }
}

fn partitioned_by_close(tokens: &[Token]) -> Option<usize> {
    let boundary = super::ctas_as_boundary(tokens);
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(index, token)| *index < boundary && !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    let start = significant.windows(3).find(|window| {
        word_is(&tokens[window[0]], "PARTITIONED")
            && word_is(&tokens[window[1]], "BY")
            && matches!(tokens[window[2]], Token::LParen)
    })?[2];
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn bucket_tokens(run: &BucketRun) -> Vec<Token> {
    let mut out = vec![keyword_token("bucket", Keyword::NoKeyword), Token::LParen];
    out.push(run.count.clone());
    out.push(Token::Comma);
    out.push(Token::Whitespace(Whitespace::Space));
    out.extend(run.columns.iter().cloned());
    out.push(Token::RParen);
    out
}

fn column_name(token: &Token) -> String {
    let raw = match token {
        Token::Word(word) => word.value.clone(),
        Token::DoubleQuotedString(value) => value.clone(),
        other => other.to_string(),
    };
    let plain = !raw.is_empty()
        && raw
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    if plain && !raw.chars().all(|character| character.is_ascii_digit()) {
        raw
    } else {
        format!("`{}`", raw.replace('`', "``"))
    }
}

fn describe_run(run: &BucketRun) -> String {
    let join = |columns: &[Token]| {
        columns
            .iter()
            .map(column_name)
            .collect::<Vec<_>>()
            .join(", ")
    };
    if run.sorts.is_empty() {
        format!("bucket({}, {})", run.count, join(&run.columns))
    } else {
        format!(
            "sorted_bucket({}, {}, {})",
            join(&run.columns),
            run.count,
            join(&run.sorts)
        )
    }
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
    use datafusion::error::DataFusionError;
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

    fn rewritten(sql: &str) -> String {
        render(
            &rewrite_clustered_by(&tokenize(sql))
                .unwrap_or_else(|error| panic!("{sql:?} must rewrite: {error}")),
        )
    }

    fn refusal(sql: &str) -> String {
        let error = rewrite_clustered_by(&tokenize(sql)).expect_err("shape refuses");
        let DataFusionError::External(inner) = &error else {
            panic!("expected an External marker, got {error:?}");
        };
        inner
            .downcast_ref::<repark_iceberg::write::IllegalArgumentMarker>()
            .map_or_else(
                || panic!("expected an IllegalArgumentMarker, got {inner:?}"),
                |marker| marker.0.clone(),
            )
    }

    #[test]
    fn clustered_by_rewrites_to_partitioned_by_bucket() {
        assert_eq!(
            rewritten(
                "CREATE TABLE ice.ns.t (id BIGINT, data STRING) CLUSTERED BY (id) INTO 4 BUCKETS"
            ),
            "CREATE TABLE ice.ns.t (id BIGINT, data STRING) PARTITIONED BY (bucket(4, id))"
        );
        assert_eq!(
            rewritten("CREATE TABLE ice.ns.t (id BIGINT) clustered by (id) into 4 buckets"),
            "CREATE TABLE ice.ns.t (id BIGINT) PARTITIONED BY (bucket(4, id))"
        );
    }

    #[test]
    fn trailing_clauses_survive_the_splice() {
        assert_eq!(
            rewritten(
                "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) INTO 4 BUCKETS \
                 TBLPROPERTIES ('k'='v')"
            ),
            "CREATE TABLE ice.ns.t (id BIGINT) PARTITIONED BY (bucket(4, id)) TBLPROPERTIES \
             ('k'='v')"
        );
    }

    #[test]
    fn the_bucket_joins_an_existing_partitioned_by_list_last() {
        assert_eq!(
            rewritten(
                "CREATE TABLE ice.ns.t PARTITIONED BY (cat) CLUSTERED BY (id) INTO 4 BUCKETS \
                 AS SELECT * FROM v"
            ),
            "CREATE TABLE ice.ns.t PARTITIONED BY (cat, bucket(4, id))  AS SELECT * FROM v"
        );
        assert_eq!(
            rewritten(
                "CREATE TABLE ice.ns.t CLUSTERED BY (`id`) INTO 4 BUCKETS PARTITIONED BY \
                 (days(ts)) AS SELECT * FROM v"
            ),
            "CREATE TABLE ice.ns.t  PARTITIONED BY (days(ts), bucket(4, `id`)) AS SELECT * \
             FROM v"
        );
    }

    #[test]
    fn multi_column_and_sorted_buckets_refuse_with_spark_text() {
        assert_eq!(
            refusal(
                "CREATE TABLE ice.ns.t USING iceberg CLUSTERED BY (id, data) INTO 4 BUCKETS \
                 AS SELECT * FROM v"
            ),
            "Cannot convert transform with more than one column reference: bucket(4, id, data)"
        );
        assert_eq!(
            refusal(
                "CREATE TABLE ice.ns.t (id BIGINT, data STRING) CLUSTERED BY (id) SORTED BY \
                 (data) INTO 4 BUCKETS"
            ),
            "Cannot convert transform with more than one column reference: \
             sorted_bucket(id, 4, data)"
        );
        assert_eq!(
            refusal(
                "CREATE TABLE ice.ns.t CLUSTERED BY (`a b`, c) SORTED BY (d, e) INTO 2 BUCKETS \
                 AS SELECT * FROM v"
            ),
            "Cannot convert transform with more than one column reference: \
             sorted_bucket(`a b`, c, 2, d, e)"
        );
    }

    #[test]
    fn a_column_named_clustered_does_not_hide_the_bucket_clause() {
        assert_eq!(
            rewritten(
                "CREATE TABLE ice.ns.t (clustered BIGINT, data STRING) USING iceberg CLUSTERED \
                 BY (clustered) INTO 4 BUCKETS"
            ),
            "CREATE TABLE ice.ns.t (clustered BIGINT, data STRING) USING iceberg PARTITIONED BY \
             (bucket(4, clustered))"
        );
    }

    #[test]
    fn an_ascending_sort_refuses_like_an_unqualified_one() {
        assert_eq!(
            refusal(
                "CREATE TABLE ice.ns.t (id BIGINT, x STRING) USING iceberg CLUSTERED BY (id) \
                 SORTED BY (x ASC) INTO 4 BUCKETS"
            ),
            "Cannot convert transform with more than one column reference: \
             sorted_bucket(id, 4, x)"
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
            "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) SORTED BY (id DESC) INTO 4 BUCKETS",
            "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) SORTED BY (id ASC, x DESC) INTO 4 \
             BUCKETS",
            "CREATE TABLE ice.ns.t (id BIGINT) CLUSTERED BY (id) INTO x BUCKETS",
            "SELECT clustered FROM t",
        ] {
            let tokens = tokenize(sql);
            assert_eq!(
                rewrite_clustered_by(&tokens).expect("pass-through"),
                tokens,
                "{sql:?}"
            );
        }
    }
}
