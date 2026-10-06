use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Postgres,
    SqlServer,
    Trino,
}

impl SourceKind {
    #[must_use]
    pub fn from_spelling(spelling: &str) -> Option<SourceKind> {
        match spelling {
            "postgres" => Some(SourceKind::Postgres),
            "sqlserver" => Some(SourceKind::SqlServer),
            "trino" => Some(SourceKind::Trino),
            _ => None,
        }
    }

    #[must_use]
    pub fn spelling(self) -> &'static str {
        match self {
            SourceKind::Postgres => "postgres",
            SourceKind::SqlServer => "sqlserver",
            SourceKind::Trino => "trino",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Generation(NonZeroU64);

impl Generation {
    #[must_use]
    pub fn new(value: u64) -> Option<Generation> {
        NonZeroU64::new(value).map(Generation)
    }

    #[must_use]
    pub fn get(self) -> NonZeroU64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIdentity {
    pub name: String,
    pub kind: SourceKind,
    pub generation: Option<Generation>,
}

impl SourceIdentity {
    #[must_use]
    pub fn unassigned(name: String, kind: SourceKind) -> Self {
        Self {
            name,
            kind,
            generation: None,
        }
    }
}
