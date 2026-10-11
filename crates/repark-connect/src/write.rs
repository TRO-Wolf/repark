pub(crate) mod postgres_copy;
pub(crate) mod row;
pub(crate) mod target;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use repark_common::redaction::REDACTED;

use self::postgres_copy::CopyLane;
use self::row::RowLane;
use self::target::RowFallback;
use crate::discover::ResolvedSource;
use crate::error::{ConnectError, Result, WriteRefusal};
use crate::ident::{PgIdent, QualifiedRelation};
use crate::pool::{PooledClient, PostgresConnector, PostgresPool, TimeoutSetting, within};
use crate::read::postgres::request_error;
use crate::settings::PostgresSettings;
use crate::types::postgres::{ColumnEncoder, PlannedColumn, WriteCarriage};

pub const BEGIN_WRITE: &str = "BEGIN READ WRITE";
pub const DEFAULT_COPY_CHUNK_BYTES: usize = 1 << 20;
pub const DEFAULT_ROWS_PER_INSERT: usize = 256;
pub const MAX_INSERT_PARAMS: usize = 65_535;

const INSUFFICIENT_PRIVILEGE: &str = "42501";
const IDLE_IN_TRANSACTION: &str = "25P03";
const DATA_EXCEPTION: &str = "22";
const CLOSING_WAIT: Duration = Duration::from_secs(1);

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

    pub(crate) fn column_fallback(&self) -> Option<RowFallback> {
        self.columns
            .iter()
            .any(|column| column.planned.carriage() == WriteCarriage::RowText)
            .then_some(RowFallback::ColumnType)
    }

    pub(crate) fn column_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect()
    }

    #[must_use]
    pub fn path(&self, requested: WritePath) -> WritePath {
        match (requested, self.column_fallback()) {
            (WritePath::Bulk, None) => WritePath::Bulk,
            (WritePath::Bulk, Some(_)) | (WritePath::Row, _) => WritePath::Row,
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
        let pooled = pool.checkout().await?;
        let timeout = options.read_timeout;
        let relation = &self.relation;
        let begin = pooled.client().batch_execute(BEGIN_WRITE);
        asked(timeout, relation, begin).await?;
        let (path, fallback) = target::route(pooled.client(), &self, path, timeout).await?;
        let lane = match path {
            WritePath::Bulk => Lane::Copy(CopyLane::open(pooled.client(), &self, options).await?),
            WritePath::Row => Lane::Row(RowLane::open(pooled.client(), &self, options).await?),
        };
        Ok(PostgresWriter {
            pooled,
            lane,
            relation: self.relation,
            read_timeout: timeout,
            fallback,
            failed: None,
        })
    }
}

pub(crate) async fn asked<T>(
    read_timeout: Duration,
    relation: &QualifiedRelation,
    work: impl Future<Output = std::result::Result<T, tokio_postgres::Error>>,
) -> Result<T> {
    let classified = async { work.await.map_err(|error| write_error(&error, relation)) };
    within(TimeoutSetting::Read, read_timeout, classified).await
}

fn write_error(error: &tokio_postgres::Error, relation: &QualifiedRelation) -> ConnectError {
    match error.as_db_error() {
        Some(refused) if refused.code().code() == INSUFFICIENT_PRIVILEGE => ConnectError::Server {
            sqlstate: INSUFFICIENT_PRIVILEGE.to_string(),
            message: refused.message().to_string(),
        },
        Some(refused) if refused.code().code().starts_with(DATA_EXCEPTION) => {
            ConnectError::Server {
                sqlstate: refused.code().code().to_string(),
                message: without_value(refused.message()),
            }
        }
        _ => request_error(error, Some(relation)),
    }
}

fn without_value(message: &str) -> String {
    match message.find([':', '"']) {
        Some(cut) => format!("{} {REDACTED}", message[..cut].trim_end()),
        None => message.to_string(),
    }
}

pub(crate) fn encoders(
    columns: &[PlannedColumn],
    batch: &RecordBatch,
) -> Result<Vec<ColumnEncoder>> {
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

async fn settled(pooled: &PooledClient<PostgresConnector>, error: ConnectError) -> ConnectError {
    if error != ConnectError::Disconnected {
        return error;
    }
    match pooled.closing_sqlstate(CLOSING_WAIT).await.as_deref() {
        Some(IDLE_IN_TRANSACTION) => ConnectError::Timeout {
            which: TimeoutSetting::Read,
        },
        _ => error,
    }
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
    fallback: Option<RowFallback>,
    failed: Option<ConnectError>,
}

impl PostgresWriter {
    #[must_use]
    pub fn fallback(&self) -> Option<RowFallback> {
        self.fallback
    }

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
        self.failed = Some(ConnectError::WriteRefused {
            refusal: WriteRefusal::Interrupted,
        });
        let written = match &mut self.lane {
            Lane::Copy(lane) => lane.write(batch).await,
            Lane::Row(lane) => lane.write(self.pooled.client(), batch).await,
        };
        let written = match written {
            Err(error) => Err(settled(&self.pooled, error).await),
            written => written,
        };
        self.failed = written.as_ref().err().cloned();
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
            ..
        } = self;
        if let Some(failed) = failed {
            return Err(failed);
        }
        let (path, finished) = match lane {
            Lane::Copy(lane) => (WritePath::Bulk, lane.finish().await),
            Lane::Row(lane) => (WritePath::Row, lane.finish(pooled.client()).await),
        };
        let rows = match finished {
            Ok(rows) => rows,
            Err(error) => return Err(settled(&pooled, error).await),
        };
        let commit = pooled.client().batch_execute("COMMIT");
        if let Err(error) = asked(read_timeout, &relation, commit).await {
            let unanswered = ConnectError::CommitUnknown { relation };
            return Err(match error {
                ConnectError::Timeout {
                    which: TimeoutSetting::Read,
                } => unanswered,
                ConnectError::Disconnected => match settled(&pooled, error).await {
                    ConnectError::Disconnected => unanswered,
                    idle => idle,
                },
                definite => definite,
            });
        }
        pooled.release_clean().await;
        Ok(WriteReport { path, rows })
    }
}
