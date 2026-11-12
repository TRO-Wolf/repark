use datafusion::config::Dialect as ParserDialect;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect, dialect_from_str};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

pub(crate) const SETOP_ALL_CARD: &str = "SQL-SETOP-ALL-MULTISET-1";

const OPERATOR_WORDS: [&[u8]; 3] = [b"intersect", b"except", b"minus"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultisetSetOperation {
    Intersect,
    Except,
    Minus,
}

impl MultisetSetOperation {
    fn from_word(word: &str) -> Option<Self> {
        if word.eq_ignore_ascii_case("INTERSECT") {
            Some(Self::Intersect)
        } else if word.eq_ignore_ascii_case("EXCEPT") {
            Some(Self::Except)
        } else if word.eq_ignore_ascii_case("MINUS") {
            Some(Self::Minus)
        } else {
            None
        }
    }

    fn spelled(self) -> &'static str {
        match self {
            Self::Intersect => "INTERSECT ALL",
            Self::Except => "EXCEPT ALL",
            Self::Minus => "MINUS ALL",
        }
    }

    fn distinct_form(self) -> &'static str {
        match self {
            Self::Intersect => "INTERSECT",
            Self::Except | Self::Minus => "EXCEPT",
        }
    }

    pub(crate) fn refusal_message(self) -> String {
        let distinct = self.distinct_form();
        format!(
            "SQL {} multiset semantics are not Spark-correct on this engine yet; use {distinct} \
             for distinct bags, or number each side's duplicates with row_number() and \
             {distinct} the numbered rows ({SETOP_ALL_CARD})",
            self.spelled()
        )
    }
}

fn may_name_operation(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let has = |needle: &[u8]| {
        bytes
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
    };
    has(b"all") && OPERATOR_WORDS.iter().any(|word| has(word))
}

pub(crate) fn multiset_set_operation(
    dialect: &dyn Dialect,
    sql: &str,
) -> Option<MultisetSetOperation> {
    if !may_name_operation(sql) {
        return None;
    }
    let tokens = Tokenizer::new(dialect, sql).tokenize().ok()?;
    let mut pending = None;
    for token in &tokens {
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        let word = match token {
            Token::Word(word) if word.quote_style.is_none() => Some(word.value.as_str()),
            _ => None,
        };
        if let (Some(operation), Some(value)) = (pending, word)
            && value.eq_ignore_ascii_case("ALL")
        {
            return Some(operation);
        }
        pending = word.and_then(MultisetSetOperation::from_word);
    }
    None
}

fn operation_in(parser: ParserDialect, sql: &str) -> Option<MultisetSetOperation> {
    if !may_name_operation(sql) {
        return None;
    }
    let dialect = dialect_from_str(parser).unwrap_or_else(|| Box::new(GenericDialect {}));
    multiset_set_operation(dialect.as_ref(), sql)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_multiset_set_operation(
    parser: ParserDialect,
    sql: &str,
) -> datafusion::error::Result<()> {
    match operation_in(parser, sql) {
        Some(operation) => Err(repark_iceberg::write::unsupported_error(
            operation.refusal_message(),
        )),
        None => Ok(()),
    }
}

pub(crate) fn refuse_in_session_sql(ctx: &SessionContext, sql: &str) -> repark_common::Result<()> {
    if !may_name_operation(sql) {
        return Ok(());
    }
    let parser = ctx.state_ref().read().config_options().sql_parser.dialect;
    match operation_in(parser, sql) {
        Some(operation) => Err(repark_common::Error::NotImplemented(
            operation.refusal_message(),
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
