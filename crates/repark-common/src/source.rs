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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIdentity {
    pub name: String,
    pub kind: SourceKind,
    pub generation: u64,
}

impl SourceIdentity {
    pub const UNASSIGNED_GENERATION: u64 = 0;

    #[must_use]
    pub fn unassigned(name: String, kind: SourceKind) -> Self {
        Self {
            name,
            kind,
            generation: Self::UNASSIGNED_GENERATION,
        }
    }
}
