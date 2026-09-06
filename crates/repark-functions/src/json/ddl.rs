use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::common::{Result, exec_err};

pub(crate) const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

pub(crate) fn parse_schema(spec: &str) -> Result<DataType> {
    let tokens = tokenize(spec)?;
    let mut cursor = Cursor {
        tokens: &tokens,
        position: 0,
    };
    if let Ok(found) = cursor.read_type()
        && cursor.finished()
    {
        return Ok(found);
    }
    let mut fields_cursor = Cursor {
        tokens: &tokens,
        position: 0,
    };
    let fields = fields_cursor.read_field_list(false)?;
    if !fields_cursor.finished() {
        return exec_err!("[PARSE_SYNTAX_ERROR] cannot parse the schema {spec:?}");
    }
    Ok(DataType::Struct(fields))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Quoted(String),
    Number(String),
    Symbol(char),
}

fn tokenize(spec: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let bytes: Vec<char> = spec.chars().collect();
    let mut index = 0;
    while index < bytes.len() {
        let found = bytes[index];
        if found.is_whitespace() {
            index += 1;
        } else if found == '`' {
            let mut name = String::new();
            index += 1;
            while index < bytes.len() && bytes[index] != '`' {
                name.push(bytes[index]);
                index += 1;
            }
            if index >= bytes.len() {
                return exec_err!("[PARSE_SYNTAX_ERROR] unterminated identifier in {spec:?}");
            }
            index += 1;
            tokens.push(Token::Quoted(name));
        } else if found.is_ascii_digit() {
            let mut digits = String::new();
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                digits.push(bytes[index]);
                index += 1;
            }
            tokens.push(Token::Number(digits));
        } else if found.is_alphanumeric() || found == '_' {
            let mut word = String::new();
            while index < bytes.len() && (bytes[index].is_alphanumeric() || bytes[index] == '_') {
                word.push(bytes[index]);
                index += 1;
            }
            tokens.push(Token::Word(word));
        } else if matches!(found, '<' | '>' | ',' | ':' | '(' | ')') {
            tokens.push(Token::Symbol(found));
            index += 1;
        } else {
            return exec_err!("[PARSE_SYNTAX_ERROR] unexpected character {found:?} in {spec:?}");
        }
    }
    Ok(tokens)
}

struct Cursor<'a> {
    tokens: &'a [Token],
    position: usize,
}

impl Cursor<'_> {
    fn finished(&self) -> bool {
        self.position >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn take_symbol(&mut self, symbol: char) -> Result<()> {
        match self.peek() {
            Some(Token::Symbol(found)) if *found == symbol => {
                self.position += 1;
                Ok(())
            }
            other => exec_err!("[PARSE_SYNTAX_ERROR] expected {symbol:?}, got {other:?}"),
        }
    }

    fn take_name(&mut self) -> Result<String> {
        match self.peek() {
            Some(Token::Word(word) | Token::Quoted(word)) => {
                let name = word.clone();
                self.position += 1;
                Ok(name)
            }
            other => exec_err!("[PARSE_SYNTAX_ERROR] expected a field name, got {other:?}"),
        }
    }

    fn take_number(&mut self) -> Result<u8> {
        match self.peek() {
            Some(Token::Number(digits)) => {
                let parsed = digits.parse::<u8>().map_err(|_| bad_number(digits))?;
                self.position += 1;
                Ok(parsed)
            }
            other => exec_err!("[PARSE_SYNTAX_ERROR] expected a number, got {other:?}"),
        }
    }

    fn read_field_list(&mut self, bracketed: bool) -> Result<Fields> {
        let mut fields: Vec<Arc<Field>> = Vec::new();
        loop {
            if bracketed && matches!(self.peek(), Some(Token::Symbol('>'))) {
                break;
            }
            let name = self.take_name()?;
            if matches!(self.peek(), Some(Token::Symbol(':'))) {
                self.position += 1;
            }
            let data_type = self.read_type()?;
            fields.push(Arc::new(Field::new(name, data_type, true)));
            if matches!(self.peek(), Some(Token::Symbol(','))) {
                self.position += 1;
                continue;
            }
            break;
        }
        Ok(Fields::from(fields))
    }

    fn read_type(&mut self) -> Result<DataType> {
        let Some(Token::Word(word)) = self.peek() else {
            return exec_err!(
                "[PARSE_SYNTAX_ERROR] expected a data type, got {:?}",
                self.peek()
            );
        };
        let name = word.to_ascii_uppercase();
        self.position += 1;
        match name.as_str() {
            "ARRAY" => {
                self.take_symbol('<')?;
                let element = self.read_type()?;
                self.take_symbol('>')?;
                Ok(DataType::List(Arc::new(Field::new(
                    "element", element, true,
                ))))
            }
            "MAP" => {
                self.take_symbol('<')?;
                let key = self.read_type()?;
                self.take_symbol(',')?;
                let value = self.read_type()?;
                self.take_symbol('>')?;
                Ok(map_type(key, value))
            }
            "STRUCT" => {
                self.take_symbol('<')?;
                if matches!(self.peek(), Some(Token::Symbol('>'))) {
                    self.position += 1;
                    return Ok(DataType::Struct(Fields::empty()));
                }
                let fields = self.read_field_list(true)?;
                self.take_symbol('>')?;
                Ok(DataType::Struct(fields))
            }
            "DECIMAL" | "DEC" | "NUMERIC" => self.read_decimal(),
            other => primitive_type(other),
        }
    }

    fn read_decimal(&mut self) -> Result<DataType> {
        if !matches!(self.peek(), Some(Token::Symbol('('))) {
            return Ok(DataType::Decimal128(10, 0));
        }
        self.take_symbol('(')?;
        let precision = self.take_number()?;
        let scale = if matches!(self.peek(), Some(Token::Symbol(','))) {
            self.position += 1;
            i8::try_from(self.take_number()?).map_err(|_| bad_number("scale"))?
        } else {
            0
        };
        self.take_symbol(')')?;
        Ok(DataType::Decimal128(precision, scale))
    }
}

fn bad_number(text: &str) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[PARSE_SYNTAX_ERROR] {text:?} is not a valid decimal parameter"
    ))
}

pub(crate) fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key, false),
                Field::new("values", value, true),
            ])),
            false,
        )),
        false,
    )
}

fn primitive_type(name: &str) -> Result<DataType> {
    match name {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "BYTE" | "TINYINT" => Ok(DataType::Int8),
        "SHORT" | "SMALLINT" => Ok(DataType::Int16),
        "INT" | "INTEGER" => Ok(DataType::Int32),
        "LONG" | "BIGINT" => Ok(DataType::Int64),
        "FLOAT" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "STRING" => Ok(DataType::Utf8),
        "BINARY" => Ok(DataType::Binary),
        "DATE" => Ok(DataType::Date32),
        "TIMESTAMP" | "TIMESTAMP_LTZ" => {
            Ok(DataType::Timestamp(TIMESTAMP_UNIT, Some(Arc::from("UTC"))))
        }
        "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(TIMESTAMP_UNIT, None)),
        other => exec_err!("[PARSE_SYNTAX_ERROR] unsupported data type {other:?}"),
    }
}
