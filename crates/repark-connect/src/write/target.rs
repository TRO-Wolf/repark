use std::fmt;
use std::time::Duration;

use tokio_postgres::types::FromSql;
use tokio_postgres::{Client, Row};

use super::{WritePath, WriteRequest, asked};
use crate::copy_binary::ProtocolViolation;
use crate::discover::Privilege;
use crate::error::{ConnectError, Result, WriteRefusal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFallback {
    View,
    ForeignTable,
    InsertRule,
    RowSecurity,
    StatementTrigger,
    ColumnType,
}

impl fmt::Display for RowFallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RowFallback::View => "the target is a view, which COPY cannot write",
            RowFallback::ForeignTable => "the target is, or routes rows to, a foreign table",
            RowFallback::InsertRule => "the target has an INSERT rule, which COPY does not fire",
            RowFallback::RowSecurity => {
                "row-level security applies to the role, and COPY FROM refuses under it"
            }
            RowFallback::StatementTrigger => {
                "the target has a statement-level INSERT trigger, which COPY fires once"
            }
            RowFallback::ColumnType => "a written column's type has no COPY BINARY form here",
        })
    }
}

pub const TARGET_FACTS: &str = "\
SELECT c.relkind::pg_catalog.text,
       pg_catalog.row_security_active(c.oid),
       EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite r
               WHERE r.ev_class = c.oid AND r.ev_type = '3' AND r.ev_enabled <> 'D'),
       EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
               WHERE t.tgrelid = c.oid AND NOT t.tgisinternal AND t.tgenabled <> 'D'
                 AND (t.tgtype & 5) = 4),
       c.relkind = 'p' AND EXISTS (SELECT 1 FROM pg_catalog.pg_partition_tree(c.oid) p
               JOIN pg_catalog.pg_class leaf ON leaf.oid = p.relid WHERE leaf.relkind = 'f'),
       COALESCE((SELECT pg_catalog.bool_and(
                   pg_catalog.has_column_privilege(c.oid, a.attnum, 'INSERT'))
                 FROM pg_catalog.pg_attribute a
                 WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                   AND a.attname = ANY($3)), false),
       (SELECT a.attname::pg_catalog.text FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
          AND a.attname = ANY($3) AND a.attidentity = 'a' ORDER BY a.attnum LIMIT 1),
       (SELECT a.attname::pg_catalog.text FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
          AND a.attname = ANY($3) AND a.attgenerated <> '' ORDER BY a.attnum LIMIT 1)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2";

fn cell<'a, T: FromSql<'a>>(row: &'a Row, index: usize) -> Result<T> {
    row.try_get(index).map_err(|_| ConnectError::Protocol {
        violation: ProtocolViolation::UnexpectedResponse,
    })
}

fn relation_fallback(facts: &Row) -> Result<Option<RowFallback>> {
    let kind: String = cell(facts, 0)?;
    let reasons = [
        (matches!(kind.as_str(), "v" | "m"), RowFallback::View),
        (kind == "f" || cell(facts, 4)?, RowFallback::ForeignTable),
        (cell(facts, 2)?, RowFallback::InsertRule),
        (cell(facts, 1)?, RowFallback::RowSecurity),
        (cell(facts, 3)?, RowFallback::StatementTrigger),
    ];
    Ok(reasons
        .into_iter()
        .find_map(|(applies, reason)| applies.then_some(reason)))
}

pub(crate) async fn route(
    client: &Client,
    request: &WriteRequest,
    requested: WritePath,
    read_timeout: Duration,
) -> Result<(WritePath, Option<RowFallback>)> {
    let relation = request.relation();
    let (schema, table) = (relation.schema.as_str(), relation.table.as_str());
    let names = request.column_names();
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 3] = [&schema, &table, &names];
    let asking = client.query_opt(TARGET_FACTS, &params);
    let facts = asked(read_timeout, relation, asking)
        .await?
        .ok_or_else(|| ConnectError::RelationNotFound {
            relation: relation.clone(),
        })?;
    if !cell::<bool>(&facts, 5)? {
        return Err(ConnectError::PermissionDenied {
            relation: relation.clone(),
            privilege: Privilege::Insert,
        });
    }
    let refused = |refusal| Err(ConnectError::WriteRefused { refusal });
    if let Some(column) = cell::<Option<String>>(&facts, 6)? {
        return refused(WriteRefusal::IdentityAlways { column });
    }
    if let Some(column) = cell::<Option<String>>(&facts, 7)? {
        return refused(WriteRefusal::GeneratedColumn { column });
    }
    let reason = match relation_fallback(&facts)? {
        Some(reason) => Some(reason),
        None => request.column_fallback(),
    };
    Ok(match (requested, reason) {
        (WritePath::Bulk, None) => (WritePath::Bulk, None),
        (WritePath::Bulk, reason) => (WritePath::Row, reason),
        (WritePath::Row, _) => (WritePath::Row, None),
    })
}
