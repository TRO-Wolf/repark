use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, TimeUnit};
use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlDataType};
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, Tokenizer};

use super::{CAST_MAP_NAME, EMPTY_MAP_CAST_NAME, contains_map, spark_sql_name};

const MAX_CAST_NESTING: usize = 64;
const MAX_TYPE_DEPTH: usize = 32;

#[must_use]
pub fn map_cast_target(type_text: &str) -> Option<ArrowDataType> {
    let dialect = SparkSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(type_text).ok()?;
    let parsed = parser.parse_data_type().ok()?;
    if parser.peek_token_ref().token != Token::EOF {
        return None;
    }
    let arrow = arrow_type(&parsed, 0)?;
    contains_map(&arrow).then_some(arrow)
}

#[must_use]
pub fn map_cast_token(type_text: &str) -> Option<String> {
    map_cast_target(type_text).map(|target| spark_sql_name(&target))
}

fn arrow_type(sql_type: &SqlDataType, depth: usize) -> Option<ArrowDataType> {
    if depth > MAX_TYPE_DEPTH {
        return None;
    }
    match sql_type {
        SqlDataType::Map(key, value) => Some(ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", arrow_type(key, depth + 1)?, false),
                    Field::new("value", arrow_type(value, depth + 1)?, true),
                ])),
                false,
            )),
            false,
        )),
        SqlDataType::Array(ArrayElemTypeDef::AngleBracket(element)) => Some(ArrowDataType::List(
            Arc::new(Field::new("item", arrow_type(element, depth + 1)?, true)),
        )),
        SqlDataType::Struct(fields, _) => {
            let mut arrow_fields = Vec::with_capacity(fields.len());
            for field in fields {
                let name = field.field_name.as_ref()?.value.clone();
                arrow_fields.push(Field::new(
                    name,
                    arrow_type(&field.field_type, depth + 1)?,
                    true,
                ));
            }
            Some(ArrowDataType::Struct(Fields::from(arrow_fields)))
        }
        leaf => atomic_type(&leaf.to_string()),
    }
}

fn atomic_type(spelling: &str) -> Option<ArrowDataType> {
    let upper = spelling.to_ascii_uppercase();
    let (name, parameters) = match upper.split_once('(') {
        Some((name, rest)) => (name.trim(), Some(rest.strip_suffix(')')?)),
        None => (upper.trim(), None),
    };
    let atomic = match (name, parameters) {
        ("STRING", None) | ("CHAR" | "VARCHAR", Some(_)) => ArrowDataType::Utf8,
        ("BINARY", None) => ArrowDataType::Binary,
        ("BOOLEAN" | "BOOL", None) => ArrowDataType::Boolean,
        ("TINYINT" | "BYTE", None) => ArrowDataType::Int8,
        ("SMALLINT" | "SHORT", None) => ArrowDataType::Int16,
        ("INT" | "INTEGER", None) => ArrowDataType::Int32,
        ("BIGINT" | "LONG", None) => ArrowDataType::Int64,
        ("FLOAT" | "REAL", None) => ArrowDataType::Float32,
        ("DOUBLE", None) => ArrowDataType::Float64,
        ("DATE", None) => ArrowDataType::Date32,
        ("TIMESTAMP" | "TIMESTAMP_LTZ", None) => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
        ("TIMESTAMP_NTZ", None) => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        ("VOID", None) => ArrowDataType::Null,
        ("DECIMAL" | "DEC" | "NUMERIC", None) => ArrowDataType::Decimal128(10, 0),
        ("DECIMAL" | "DEC" | "NUMERIC", Some(parameters)) => decimal_type(parameters)?,
        _ => return None,
    };
    Some(atomic)
}

fn decimal_type(parameters: &str) -> Option<ArrowDataType> {
    let (precision, scale) = match parameters.split_once(',') {
        Some((precision, scale)) => (precision.trim(), scale.trim()),
        None => (parameters.trim(), "0"),
    };
    let precision: u8 = precision.parse().ok()?;
    let scale: i8 = scale.parse().ok()?;
    ((1..=38).contains(&precision) && (0..=i16::from(precision)).contains(&i16::from(scale)))
        .then_some(ArrowDataType::Decimal128(precision, scale))
}

#[must_use]
pub fn rewrite_map_casts(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("map") || !lower.contains("cast") {
        return None;
    }
    let lexed = Lexed::new(sql)?;
    let mut out = String::with_capacity(sql.len() + 64);
    lexed
        .rewrite(0, lexed.tokens.len(), 0, sql.len(), 0, &mut out)
        .then_some(out)
}

struct Lexed<'a> {
    sql: &'a str,
    tokens: Vec<Token>,
    starts: Vec<usize>,
    ends: Vec<usize>,
}

struct CastSite {
    close: usize,
    operand: (usize, usize),
    target: ArrowDataType,
    try_cast: bool,
}

fn byte_offset(line_starts: &[usize], char_bytes: &[usize], at: Location) -> Option<usize> {
    let line = usize::try_from(at.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(at.column).ok()?.checked_sub(1)?;
    char_bytes
        .get(line_starts.get(line)?.checked_add(column)?)
        .copied()
}

impl<'a> Lexed<'a> {
    fn new(sql: &'a str) -> Option<Self> {
        let spanned = Tokenizer::new(&SparkSqlDialect {}, sql)
            .with_unescape(false)
            .tokenize_with_location()
            .ok()?;
        let mut char_bytes: Vec<usize> = Vec::with_capacity(sql.len() + 1);
        let mut line_starts: Vec<usize> = vec![0];
        for (index, (byte, character)) in sql.char_indices().enumerate() {
            char_bytes.push(byte);
            if character == '\n' {
                line_starts.push(index + 1);
            }
        }
        char_bytes.push(sql.len());
        let mut lexed = Lexed {
            sql,
            tokens: Vec::new(),
            starts: Vec::new(),
            ends: Vec::new(),
        };
        for spanned in spanned {
            if matches!(spanned.token, Token::Whitespace(_) | Token::EOF) {
                continue;
            }
            let start = byte_offset(&line_starts, &char_bytes, spanned.span.start)?;
            let end = byte_offset(&line_starts, &char_bytes, spanned.span.end)?;
            if start > end || !sql.is_char_boundary(start) || !sql.is_char_boundary(end) {
                return None;
            }
            lexed.tokens.push(spanned.token);
            lexed.starts.push(start);
            lexed.ends.push(end);
        }
        Some(lexed)
    }

    fn keyword_at(&self, index: usize, keyword: Keyword) -> bool {
        matches!(self.tokens.get(index), Some(Token::Word(word)) if word.keyword == keyword && word.quote_style.is_none())
    }

    fn matching_close(&self, open: usize, limit: usize) -> Option<usize> {
        let mut depth = 0_usize;
        for index in open..limit {
            match self.tokens[index] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn top_level_as(&self, from: usize, to: usize) -> Option<usize> {
        let mut depth = 0_usize;
        for index in from..to {
            match self.tokens[index] {
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                _ if depth == 0 && self.keyword_at(index, Keyword::AS) => return Some(index),
                _ => {}
            }
        }
        None
    }

    fn names_a_map(&self, from: usize, to: usize) -> bool {
        (from..to).any(|index| {
            self.keyword_at(index, Keyword::MAP)
                && matches!(self.tokens.get(index + 1), Some(Token::Lt))
        })
    }

    fn type_text(&self, from: usize, to: usize) -> String {
        let mut text = String::new();
        let mut previous_word = false;
        for token in &self.tokens[from..to] {
            let word = matches!(token, Token::Word(_) | Token::Number(_, _));
            if word && previous_word {
                text.push(' ');
            }
            text.push_str(&token.to_string());
            previous_word = word;
        }
        text
    }

    fn is_empty_map_call(&self, from: usize, to: usize) -> bool {
        to == from + 3
            && self.keyword_at(from, Keyword::MAP)
            && self.tokens[from + 1] == Token::LParen
            && self.tokens[from + 2] == Token::RParen
    }

    fn cast_site(&self, index: usize, limit: usize) -> Option<CastSite> {
        let try_cast = if self.keyword_at(index, Keyword::TRY_CAST) {
            true
        } else if self.keyword_at(index, Keyword::CAST) {
            false
        } else {
            return None;
        };
        if self.tokens.get(index + 1) != Some(&Token::LParen) {
            return None;
        }
        let close = self.matching_close(index + 1, limit)?;
        let as_index = self.top_level_as(index + 2, close)?;
        if as_index == index + 2 || !self.names_a_map(as_index + 1, close) {
            return None;
        }
        let target = map_cast_target(&self.type_text(as_index + 1, close))?;
        Some(CastSite {
            close,
            operand: (index + 2, as_index),
            target,
            try_cast,
        })
    }

    fn rewrite(
        &self,
        from: usize,
        to: usize,
        byte_from: usize,
        byte_to: usize,
        depth: usize,
        out: &mut String,
    ) -> bool {
        let mut cursor = byte_from;
        let mut rewritten = false;
        let mut index = from;
        while index < to {
            let Some(site) = self.cast_site(index, to) else {
                index += 1;
                continue;
            };
            out.push_str(&self.sql[cursor..self.starts[index]]);
            let (operand_from, operand_to) = site.operand;
            if self.is_empty_map_call(operand_from, operand_to) {
                out.push_str(EMPTY_MAP_CAST_NAME);
                out.push_str("(NULL");
            } else {
                out.push_str(CAST_MAP_NAME);
                out.push('(');
                let operand_bytes = (self.starts[operand_from], self.ends[operand_to - 1]);
                if depth < MAX_CAST_NESTING {
                    self.rewrite(
                        operand_from,
                        operand_to,
                        operand_bytes.0,
                        operand_bytes.1,
                        depth + 1,
                        out,
                    );
                } else {
                    out.push_str(&self.sql[operand_bytes.0..operand_bytes.1]);
                }
            }
            out.push_str(", '");
            out.push_str(&site.target.to_string().replace('\'', "''"));
            out.push_str(if site.try_cast {
                "', true)"
            } else {
                "', false)"
            });
            cursor = self.ends[site.close];
            index = site.close + 1;
            rewritten = true;
        }
        out.push_str(&self.sql[cursor..byte_to]);
        rewritten
    }
}
