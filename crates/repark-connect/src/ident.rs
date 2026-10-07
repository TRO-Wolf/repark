use std::fmt;

use crate::error::{ConnectError, Result};

pub const MAX_IDENT_BYTES: usize = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentRefusal {
    Empty,
    Nul,
    TooLong { bytes: usize },
    Qualification,
}

impl fmt::Display for IdentRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdentRefusal::Empty => f.write_str("is empty"),
            IdentRefusal::Nul => f.write_str("contains a NUL byte"),
            IdentRefusal::TooLong { bytes } => write!(
                f,
                "is {bytes} bytes, past the {MAX_IDENT_BYTES}-byte limit beyond which Postgres \
                 truncates names"
            ),
            IdentRefusal::Qualification => f.write_str(
                "is not `table` or `schema.table`, each part bare or double-quoted with `\"\"` \
                 for a quote",
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PgIdent(String);

impl PgIdent {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let refusal = if name.is_empty() {
            Some(IdentRefusal::Empty)
        } else if name.contains('\0') {
            Some(IdentRefusal::Nul)
        } else if name.len() > MAX_IDENT_BYTES {
            Some(IdentRefusal::TooLong { bytes: name.len() })
        } else {
            None
        };
        match refusal {
            Some(reason) => Err(ConnectError::InvalidIdentifier { reason }),
            None => Ok(Self(name)),
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PgIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"")?;
        for (index, part) in self.0.split('"').enumerate() {
            if index > 0 {
                f.write_str("\"\"")?;
            }
            f.write_str(part)?;
        }
        f.write_str("\"")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QualifiedRelation {
    pub schema: PgIdent,
    pub table: PgIdent,
}

impl QualifiedRelation {
    #[must_use]
    pub fn new(schema: PgIdent, table: PgIdent) -> Self {
        Self { schema, table }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn parse(dbtable: &str) -> Result<Self> {
        let malformed = || ConnectError::InvalidIdentifier {
            reason: IdentRefusal::Qualification,
        };
        let mut parts = Vec::new();
        let mut rest = dbtable;
        loop {
            let (part, after) = split_part(rest).ok_or_else(malformed)?;
            parts.push(PgIdent::new(part)?);
            match after.strip_prefix('.') {
                Some(next) => rest = next,
                None if after.is_empty() => break,
                None => return Err(malformed()),
            }
        }
        let mut parts = parts.into_iter();
        match (parts.next(), parts.next(), parts.next()) {
            (Some(table), None, None) => Ok(Self::new(PgIdent::new(DEFAULT_SCHEMA)?, table)),
            (Some(schema), Some(table), None) => Ok(Self::new(schema, table)),
            _ => Err(malformed()),
        }
    }
}

pub const DEFAULT_SCHEMA: &str = "public";

fn split_part(text: &str) -> Option<(String, &str)> {
    let Some(mut quoted) = text.strip_prefix('"') else {
        let end = text.find(['.', '"']).unwrap_or(text.len());
        return Some((text.get(..end)?.to_string(), text.get(end..)?));
    };
    let mut part = String::new();
    loop {
        let close = quoted.find('"')?;
        part.push_str(quoted.get(..close)?);
        let after = quoted.get(close + 1..)?;
        match after.strip_prefix('"') {
            Some(next) => {
                part.push('"');
                quoted = next;
            }
            None => return Some((part, after)),
        }
    }
}

impl fmt::Display for QualifiedRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}
