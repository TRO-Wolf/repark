pub(crate) mod postgres_copy;
pub(crate) mod row;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;

use self::postgres_copy::CopyLane;
use self::row::RowLane;
use crate::discover::{Privilege, ResolvedSource};
use crate::error::{ConnectError, Result, WriteRefusal};
use crate::ident::{PgIdent, QualifiedRelation};
use crate::pool::{PooledClient, PostgresConnector, PostgresPool, TimeoutSetting};
use crate::read::postgres::request;
use crate::settings::PostgresSettings;
use crate::types::postgres::{ColumnEncoder, PlannedColumn, WriteCarriage};

pub const BEGIN_WRITE: &str = "BEGIN READ WRITE";
pub const DEFAULT_COPY_CHUNK_BYTES: usize = 1 << 20;
pub const DEFAULT_ROWS_PER_INSERT: usize = 256;
pub const MAX_INSERT_PARAMS: usize = 65_535;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePath {
    Bulk,
    Row,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    pub read_timeout: Duration,
    pub copy_chunk_bytes: usize,
    pub rows_per_insert: usize,
}

impl WriteOptions {
    #[must_use]
    pub fn from_settings(settings: &PostgresSettings) -> WriteOptions {
        WriteOptions {
            read_timeout: settings.read_timeout,
            copy_chunk_bytes: DEFAULT_COPY_CHUNK_BYTES,
            rows_per_insert: DEFAULT_ROWS_PER_INSERT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteReport {
    pub path: WritePath,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteColumn {
    name: PgIdent,
    planned: PlannedColumn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRequest {
    relation: QualifiedRelation,
    columns: Vec<WriteColumn>,
}

impl WriteRequest {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(resolved: &ResolvedSource) -> Result<WriteRequest> {
        let relation = resolved
            .source
            .relation()
            .ok_or(ConnectError::WriteRefused {
                refusal: WriteRefusal::QueryTarget,
            })?;
        let columns = resolved
            .columns
            .iter()
            .map(|column| WriteColumn {
                name: column.name.clone(),
                planned: column.planned.clone(),
            })
            .collect();
        Ok(WriteRequest {
            relation: relation.clone(),
            columns,
        })
    }

    #[must_use]
    pub fn columns(mut self, indexes: &[usize]) -> Option<WriteRequest> {
        self.columns = indexes
            .iter()
            .map(|&index| self.columns.get(index).cloned())
            .collect::<Option<_>>()?;
        Some(self)
    }

    #[must_use]
    pub fn relation(&self) -> &QualifiedRelation {
        &self.relation
    }

    #[must_use]
    pub fn carriages(&self) -> Vec<WriteCarriage> {
        self.columns
            .iter()
            .map(|column| column.planned.carriage())
            .collect()
    }

    #[must_use]
    pub fn path(&self, requested: WritePath) -> WritePath {
        let bulk = self
            .columns
            .iter()
            .all(|column| column.planned.carriage() == WriteCarriage::CopyBinary);
        match requested {
            WritePath::Bulk if bulk => WritePath::Bulk,
            WritePath::Bulk | WritePath::Row => WritePath::Row,
        }
    }

    fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    #[must_use]
    pub fn copy_statement(&self) -> String {
        format!(
            "COPY {} ({}) FROM STDIN (FORMAT BINARY)",
            self.relation,
            self.column_list()
        )
    }

    #[must_use]
    pub fn insert_statement(&self, rows: usize) -> String {
        let width = self.columns.len();
        let tuples = (0..rows)
            .map(|row| {
                let slots = self
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(column, written)| {
                        let slot = row * width + column + 1;
                        match written.planned.carriage() {
                            WriteCarriage::CopyBinary => format!("${slot}"),
                            WriteCarriage::RowText => format!(
                                "${slot}::pg_catalog.text::pg_catalog.{}",
                                written.planned.postgres_type()
                            ),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({slots})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {} ({}) VALUES {tuples}",
            self.relation,
            self.column_list()
        )
    }

    fn planned(&self) -> Vec<PlannedColumn> {
        self.columns
            .iter()
            .map(|column| column.planned.clone())
            .collect()
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn open(
        self,
        pool: &Arc<PostgresPool>,
        path: WritePath,
        options: WriteOptions,
    ) -> Result<PostgresWriter> {
        if self.columns.is_empty() {
            return Err(ConnectError::WriteRefused {
                refusal: WriteRefusal::NoColumns,
            });
        }
        let path = self.path(path);
        let pooled = pool.checkout().await?;
        let timeout = options.read_timeout;
        let relation = &self.relation;
        let begin = pooled.client().batch_execute(BEGIN_WRITE);
        asked(timeout, relation, begin).await?;
        let lane = match path {
            WritePath::Bulk => Lane::Copy(CopyLane::open(pooled.client(), &self, options).await?),
            WritePath::Row => Lane::Row(RowLane::open(pooled.client(), &self, options).await?),
        };
        Ok(PostgresWriter {
            pooled,
            lane,
            relation: self.relation,
            read_timeout: timeout,
            failed: None,
        })
    }
}

pub(crate) async fn asked<T>(
    read_timeout: Duration,
    relation: &QualifiedRelation,
    work: impl Future<Output = std::result::Result<T, tokio_postgres::Error>>,
) -> Result<T> {
    request(read_timeout, Some(relation), work)
        .await
        .map_err(|error| match error {
            ConnectError::PermissionDenied { relation, .. } => ConnectError::PermissionDenied {
                relation,
                privilege: Privilege::Insert,
            },
            other => other,
        })
}

pub(crate) fn encoders<'a>(
    columns: &[PlannedColumn],
    batch: &'a RecordBatch,
) -> Result<Vec<ColumnEncoder<'a>>> {
    if batch.num_columns() != columns.len() {
        return Err(ConnectError::WriteRefused {
            refusal: WriteRefusal::ColumnCount {
                expected: columns.len(),
                actual: batch.num_columns(),
            },
        });
    }
    columns
        .iter()
        .zip(batch.columns())
        .map(|(column, array)| ColumnEncoder::new(column, array.as_ref()))
        .collect()
}

enum Lane {
    Copy(CopyLane),
    Row(RowLane),
}

pub struct PostgresWriter {
    pooled: PooledClient<PostgresConnector>,
    lane: Lane,
    relation: QualifiedRelation,
    read_timeout: Duration,
    failed: Option<ConnectError>,
}

impl PostgresWriter {
    #[must_use]
    pub fn path(&self) -> WritePath {
        match self.lane {
            Lane::Copy(_) => WritePath::Bulk,
            Lane::Row(_) => WritePath::Row,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(failed) = &self.failed {
            return Err(failed.clone());
        }
        let written = match &mut self.lane {
            Lane::Copy(lane) => lane.write(batch).await,
            Lane::Row(lane) => lane.write(self.pooled.client(), batch).await,
        };
        if let Err(error) = &written {
            self.failed = Some(error.clone());
        }
        written
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn commit(self) -> Result<WriteReport> {
        let PostgresWriter {
            pooled,
            lane,
            relation,
            read_timeout,
            failed,
        } = self;
        if let Some(failed) = failed {
            return Err(failed);
        }
        let (path, rows) = match lane {
            Lane::Copy(lane) => (WritePath::Bulk, lane.finish().await?),
            Lane::Row(lane) => (WritePath::Row, lane.finish(pooled.client()).await?),
        };
        let commit = pooled.client().batch_execute("COMMIT");
        asked(read_timeout, &relation, commit)
            .await
            .map_err(|error| match error {
                ConnectError::Disconnected
                | ConnectError::Timeout {
                    which: TimeoutSetting::Read,
                } => ConnectError::CommitUnknown { relation },
                definite => definite,
            })?;
        pooled.release_clean().await;
        Ok(WriteReport { path, rows })
    }
}
