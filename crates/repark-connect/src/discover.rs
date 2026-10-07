use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::types::Kind;
use tokio_postgres::{Client, Row};

use crate::error::{ConnectError, ProtocolViolation, Result};
use crate::ident::{PgIdent, QualifiedRelation};
use crate::pool::PostgresPool;
use crate::read::postgres::request;
use crate::types::postgres::{PgTypeKind, PlannedColumn, PostgresMapping, TypeMod, postgres_type};

pub const MIN_SERVER_VERSION_NUM: i32 = 140_000;
pub const SERVER_VERSION_ROW: &str = "CONNECT-DECL-pg-server-version";
pub const QUERY_ALIAS: &str = "repark_q";

const CATALOG: &str = "pg_catalog";

pub const BEGIN_DISCOVERY: &str = "BEGIN READ ONLY; SET LOCAL statement_timeout = 30000";

pub const QUERY_SEARCH_PATH: &str = "\
SELECT pg_catalog.set_config('search_path', COALESCE((
  SELECT pg_catalog.substr(c.entry, pg_catalog.strpos(c.entry, '=') + 1)
  FROM pg_catalog.pg_db_role_setting s
  CROSS JOIN pg_catalog.unnest(s.setconfig) AS c(entry)
  WHERE s.setrole IN (0, (SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = session_user))
    AND s.setdatabase IN (0, (SELECT d.oid FROM pg_catalog.pg_database d
                              WHERE d.datname = pg_catalog.current_database()))
    AND pg_catalog.split_part(c.entry, '=', 1) = 'search_path'
  ORDER BY s.setrole = 0, s.setdatabase = 0
  LIMIT 1), '\"$user\", public'), true)";

const SERVER_FACTS: &str = "SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
     pg_catalog.current_setting('server_encoding')";

const RELATION_COLUMNS: &str = "\
WITH RECURSIVE rel AS (
  SELECT r.oid,
         pg_catalog.has_schema_privilege(n.oid, 'USAGE') AS schema_usage,
         pg_catalog.has_any_column_privilege(r.oid, 'SELECT') AS can_select
  FROM pg_catalog.pg_class r
  JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
  WHERE n.nspname = $1 AND r.relname = $2 AND r.relkind IN ('r', 'v', 'm', 'f', 'p')
), col AS (
  SELECT a.attnum, a.attname, a.attnotnull, a.atttypid, a.atttypmod, a.attcollation
  FROM pg_catalog.pg_attribute a
  JOIN rel ON a.attrelid = rel.oid
  WHERE a.attnum > 0 AND NOT a.attisdropped
), base AS (
  SELECT col.attnum, col.atttypid AS typid, col.atttypmod AS typmod FROM col
  UNION ALL
  SELECT base.attnum, t.typbasetype,
         CASE WHEN base.typmod = -1 THEN t.typtypmod ELSE base.typmod END
  FROM base JOIN pg_catalog.pg_type t ON t.oid = base.typid
  WHERE t.typtype = 'd'
), resolved AS (
  SELECT base.attnum, base.typmod, t.typname, t.typtype, tn.nspname AS typnspname
  FROM base
  JOIN pg_catalog.pg_type t ON t.oid = base.typid
  JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
  WHERE t.typtype <> 'd'
)
SELECT rel.schema_usage, rel.can_select, col.attname::pg_catalog.text, col.attnotnull,
       resolved.typname::pg_catalog.text, resolved.typtype::pg_catalog.text,
       resolved.typnspname::pg_catalog.text, resolved.typmod,
       c.collname::pg_catalog.text, c.collisdeterministic
FROM rel
LEFT JOIN (col
  JOIN resolved ON resolved.attnum = col.attnum
  LEFT JOIN pg_catalog.pg_collation c ON c.oid = col.attcollation) ON true
ORDER BY col.attnum";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    Usage,
    Select,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Privilege::Usage => "USAGE",
            Privilege::Select => "SELECT",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanSource {
    Relation(QualifiedRelation),
    Query(Arc<str>),
}

impl ScanSource {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_dbtable(dbtable: &str) -> Result<ScanSource> {
        if dbtable.trim_start().starts_with('(') {
            return Ok(ScanSource::Query(format!("SELECT * FROM {dbtable}").into()));
        }
        QualifiedRelation::parse(dbtable).map(ScanSource::Relation)
    }

    #[must_use]
    pub fn query(sql: &str) -> ScanSource {
        ScanSource::Query(sql.into())
    }

    #[must_use]
    pub fn relation(&self) -> Option<&QualifiedRelation> {
        match self {
            ScanSource::Relation(relation) => Some(relation),
            ScanSource::Query(_) => None,
        }
    }

    #[must_use]
    pub fn search_path(&self) -> Option<&'static str> {
        match self {
            ScanSource::Relation(_) => None,
            ScanSource::Query(_) => Some(QUERY_SEARCH_PATH),
        }
    }

    pub(crate) fn sql_source(&self) -> String {
        match self {
            ScanSource::Relation(relation) => relation.to_string(),
            ScanSource::Query(sql) => format!("({sql}) AS {QUERY_ALIAS}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastType {
    Catalog(&'static str),
    Numeric { precision: i32, scale: i32 },
    Text,
}

impl CastType {
    fn of(
        planned: &PlannedColumn,
        row_name: &'static str,
        kind: PgTypeKind,
        typmod: TypeMod,
    ) -> Self {
        match (kind, planned.mapping()) {
            (PgTypeKind::Enum, _) | (_, PostgresMapping::ServerText) => CastType::Text,
            (_, PostgresMapping::Numeric(_)) if typmod.get() >= 4 => {
                let packed = typmod.get() - 4;
                CastType::Numeric {
                    precision: (packed >> 16) & 0xffff,
                    scale: ((packed & 0x7ff) ^ 0x400) - 0x400,
                }
            }
            _ => CastType::Catalog(row_name),
        }
    }

    #[must_use]
    pub fn unconstrained(self) -> CastType {
        match self {
            CastType::Numeric { .. } => CastType::Catalog("numeric"),
            other => other,
        }
    }
}

impl fmt::Display for CastType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CastType::Catalog(name) => write!(f, "{CATALOG}.{name}"),
            CastType::Numeric { precision, scale } => {
                write!(f, "{CATALOG}.numeric({precision},{scale})")
            }
            CastType::Text => write!(f, "{CATALOG}.text"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCollation {
    pub name: PgIdent,
    pub deterministic: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanColumn {
    pub name: PgIdent,
    pub cast: CastType,
    pub planned: PlannedColumn,
    pub collation: Option<ColumnCollation>,
}

impl ScanColumn {
    #[allow(clippy::missing_errors_doc)]
    pub fn resolve(
        name: PgIdent,
        typname: &str,
        kind: PgTypeKind,
        typmod: TypeMod,
        nullable: bool,
    ) -> Result<ScanColumn> {
        let column: Arc<str> = name.as_str().into();
        let planned =
            PlannedColumn::resolve(column, typname, kind, typmod)?.with_nullable(nullable);
        let row_name = postgres_type(typname).map_or("text", |row| row.postgres_name);
        let cast = CastType::of(&planned, row_name, kind, typmod);
        Ok(ScanColumn {
            name,
            cast,
            planned,
            collation: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSource {
    pub source: ScanSource,
    pub columns: Vec<ScanColumn>,
    pub server_version_num: i32,
    pub server_encoding: Arc<str>,
}

#[allow(clippy::missing_errors_doc)]
pub fn check_server_version(server_version_num: i32) -> Result<()> {
    if server_version_num < MIN_SERVER_VERSION_NUM {
        return Err(ConnectError::DeclaredServerVersion { server_version_num });
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub async fn discover(
    pool: &Arc<PostgresPool>,
    source: &ScanSource,
    read_timeout: Duration,
) -> Result<ResolvedSource> {
    let pooled = pool.checkout().await?;
    let client = pooled.client();
    let relation = source.relation();
    let begin = match source.search_path() {
        Some(search_path) => format!("{BEGIN_DISCOVERY}; {search_path}"),
        None => BEGIN_DISCOVERY.to_string(),
    };
    request(read_timeout, relation, client.batch_execute(&begin)).await?;
    let facts = request(read_timeout, relation, client.query_one(SERVER_FACTS, &[])).await?;
    let server_version_num: i32 = cell(&facts, 0)?;
    check_server_version(server_version_num)?;
    let server_encoding: String = cell(&facts, 1)?;
    let columns = match source {
        ScanSource::Relation(relation) => relation_columns(client, relation, read_timeout).await?,
        ScanSource::Query(_) => query_columns(client, source, read_timeout).await?,
    };
    request(read_timeout, relation, client.batch_execute("COMMIT")).await?;
    pooled.release_clean().await;
    Ok(ResolvedSource {
        source: source.clone(),
        columns,
        server_version_num,
        server_encoding: server_encoding.into(),
    })
}

async fn relation_columns(
    client: &Client,
    relation: &QualifiedRelation,
    read_timeout: Duration,
) -> Result<Vec<ScanColumn>> {
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] =
        [&relation.schema.as_str(), &relation.table.as_str()];
    let rows = request(
        read_timeout,
        Some(relation),
        client.query(RELATION_COLUMNS, &params),
    )
    .await?;
    let Some(first) = rows.first() else {
        return Err(ConnectError::RelationNotFound {
            relation: relation.clone(),
        });
    };
    let denied = |privilege| ConnectError::PermissionDenied {
        relation: relation.clone(),
        privilege,
    };
    if !cell::<bool>(first, 0)? {
        return Err(denied(Privilege::Usage));
    }
    if !cell::<bool>(first, 1)? {
        return Err(denied(Privilege::Select));
    }
    let mut columns = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(name) = cell::<Option<String>>(row, 2)? else {
            continue;
        };
        let typtype: String = cell(row, 5)?;
        let namespace: String = cell(row, 6)?;
        let kind = type_kind(&typtype, &namespace);
        let typname: String = cell(row, 4)?;
        let typmod = TypeMod::new(cell(row, 7)?);
        let nullable = !cell::<bool>(row, 3)?;
        let mut column =
            ScanColumn::resolve(PgIdent::new(name)?, &typname, kind, typmod, nullable)?;
        if let Some(collation) = cell::<Option<String>>(row, 8)? {
            column.collation = Some(ColumnCollation {
                name: PgIdent::new(collation)?,
                deterministic: cell(row, 9)?,
            });
        }
        columns.push(column);
    }
    Ok(columns)
}

async fn query_columns(
    client: &Client,
    source: &ScanSource,
    read_timeout: Duration,
) -> Result<Vec<ScanColumn>> {
    let probe = format!("SELECT * FROM {}", source.sql_source());
    let statement = request(read_timeout, None, client.prepare(&probe)).await?;
    statement
        .columns()
        .iter()
        .map(|column| {
            let type_ = column.type_();
            let kind = match type_.kind() {
                Kind::Simple if type_.schema() == CATALOG => PgTypeKind::Base,
                Kind::Enum(_) => PgTypeKind::Enum,
                _ => PgTypeKind::Other,
            };
            let typmod = TypeMod::new(column.type_modifier());
            ScanColumn::resolve(
                PgIdent::new(column.name())?,
                type_.name(),
                kind,
                typmod,
                true,
            )
        })
        .collect()
}

fn type_kind(typtype: &str, namespace: &str) -> PgTypeKind {
    match (typtype, namespace) {
        ("b", CATALOG) => PgTypeKind::Base,
        ("e", _) => PgTypeKind::Enum,
        _ => PgTypeKind::Other,
    }
}

fn cell<'a, T: tokio_postgres::types::FromSql<'a>>(row: &'a Row, index: usize) -> Result<T> {
    row.try_get(index).map_err(|_| ConnectError::Protocol {
        violation: ProtocolViolation::UnexpectedResponse,
    })
}
