use std::error::Error as _;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use bytes::{Buf, Bytes};
use datafusion::physical_plan::metrics::{Count, Time};
use futures::{Stream, StreamExt, stream};
use tokio_postgres::CopyOutStream;
use tokio_postgres::types::ToSql;

use crate::copy_binary::{BatchLimits, CopyBinaryDecoder, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS};
use crate::discover::{CastType, Privilege, ResolvedSource, ScanColumn};
use crate::error::{ConnectError, ProtocolViolation, Result};
use crate::ident::QualifiedRelation;
use crate::pool::{PooledClient, PostgresConnector, PostgresPool, TimeoutSetting, within};
use crate::settings::PostgresSettings;

pub const MAX_PARAM_SLOTS: u16 = 1024;
pub const BEGIN_SCAN: &str = "BEGIN READ ONLY";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParamSlot(u16);

impl ParamSlot {
    #[must_use]
    pub fn new(index: u16) -> Option<ParamSlot> {
        (index < MAX_PARAM_SLOTS).then_some(ParamSlot(index))
    }

    #[must_use]
    pub fn setting_name(self) -> String {
        format!("repark.p{}", self.0)
    }
}

impl fmt::Display for ParamSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pg_catalog.current_setting('repark.p{}')", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CompareOp {
    fn sql(self) -> &'static str {
        match self {
            CompareOp::Eq => "OPERATOR(pg_catalog.=)",
            CompareOp::NotEq => "OPERATOR(pg_catalog.<>)",
            CompareOp::Lt => "OPERATOR(pg_catalog.<)",
            CompareOp::LtEq => "OPERATOR(pg_catalog.<=)",
            CompareOp::Gt => "OPERATOR(pg_catalog.>)",
            CompareOp::GtEq => "OPERATOR(pg_catalog.>=)",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Comparison {
    column: usize,
    op: CompareOp,
    slot: ParamSlot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRequest {
    resolved: Arc<ResolvedSource>,
    projection: Vec<usize>,
    comparisons: Vec<Comparison>,
    filters: Vec<String>,
    values: Vec<String>,
    limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanStatement {
    pub copy: String,
    pub settings: Vec<(String, String)>,
}

impl ScanRequest {
    #[must_use]
    pub fn new(resolved: Arc<ResolvedSource>) -> ScanRequest {
        let projection = (0..resolved.columns.len()).collect();
        ScanRequest {
            resolved,
            projection,
            comparisons: Vec::new(),
            filters: Vec::new(),
            values: Vec::new(),
            limit: None,
        }
    }

    #[must_use]
    pub fn project(mut self, projection: &[usize]) -> Option<ScanRequest> {
        let columns = self.resolved.columns.len();
        if projection.iter().any(|&index| index >= columns) {
            return None;
        }
        self.projection = projection.to_vec();
        Some(self)
    }

    #[must_use]
    pub fn compare(mut self, column: usize, op: CompareOp, value: String) -> Option<ScanRequest> {
        if column >= self.resolved.columns.len() {
            return None;
        }
        let slot = ParamSlot::new(u16::try_from(self.values.len()).ok()?)?;
        self.comparisons.push(Comparison { column, op, slot });
        self.values.push(value);
        Some(self)
    }

    pub(crate) fn filter(mut self, sql: String, values: Vec<String>) -> Option<ScanRequest> {
        if self.values.len() + values.len() > usize::from(MAX_PARAM_SLOTS) {
            return None;
        }
        self.filters.push(sql);
        self.values.extend(values);
        Some(self)
    }

    #[must_use]
    pub fn bound_values(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    pub fn limit(mut self, limit: u64) -> ScanRequest {
        self.limit = Some(limit);
        self
    }

    #[must_use]
    pub fn relation(&self) -> Option<&QualifiedRelation> {
        self.resolved.source.relation()
    }

    #[must_use]
    pub fn statement(&self) -> ScanStatement {
        let select = self
            .projection
            .iter()
            .filter_map(|&index| self.resolved.columns.get(index))
            .map(|column| format!("{}::{}", column.name, column.cast))
            .collect::<Vec<_>>()
            .join(", ");
        let gap = if select.is_empty() { "" } else { " " };
        let mut copy = format!(
            "COPY (SELECT {select}{gap}FROM {}",
            self.resolved.source.sql_source()
        );
        let predicates = self
            .comparisons
            .iter()
            .filter_map(|comparison| {
                let column = self.resolved.columns.get(comparison.column)?;
                Some(format!(
                    "{} {} {}::{}",
                    operand(column),
                    comparison.op.sql(),
                    comparison.slot,
                    column.cast.unconstrained()
                ))
            })
            .chain(self.filters.iter().cloned())
            .collect::<Vec<_>>();
        if !predicates.is_empty() {
            copy.push_str(" WHERE ");
            copy.push_str(&predicates.join(" AND "));
        }
        if let Some(limit) = self.limit {
            copy.push_str(" LIMIT ");
            copy.push_str(&limit.to_string());
        }
        copy.push_str(") TO STDOUT (FORMAT BINARY)");
        let settings = (0..MAX_PARAM_SLOTS)
            .filter_map(ParamSlot::new)
            .zip(&self.values)
            .map(|(slot, value)| (slot.setting_name(), value.clone()))
            .collect();
        ScanStatement { copy, settings }
    }

    fn decoder(&self, limits: BatchLimits) -> Result<CopyBinaryDecoder> {
        let planned = self
            .projection
            .iter()
            .filter_map(|&index| self.resolved.columns.get(index))
            .map(|column| column.planned.clone())
            .collect();
        CopyBinaryDecoder::new(planned, limits)
    }
}

fn operand(column: &ScanColumn) -> String {
    match column.cast {
        CastType::Text => format!("{}::{}", column.name, CastType::Text),
        CastType::Catalog(_) | CastType::Numeric { .. } => column.name.to_string(),
    }
}

impl ScanStatement {
    #[must_use]
    pub fn set_config_sql(&self) -> Option<String> {
        if self.settings.is_empty() {
            return None;
        }
        let calls = (0..self.settings.len())
            .map(|pair| {
                format!(
                    "pg_catalog.set_config(${}, ${}, true)",
                    pair * 2 + 1,
                    pair * 2 + 2
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!("SELECT {calls}"))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScanMeter {
    pub bytes_received: Count,
    pub time_to_first_byte: Time,
    pub decode: Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanOptions {
    pub read_timeout: Duration,
    pub batch: BatchLimits,
}

impl ScanOptions {
    #[must_use]
    pub fn from_settings(settings: &PostgresSettings) -> ScanOptions {
        let rows = settings.batch_rows.unwrap_or(DEFAULT_BATCH_ROWS);
        ScanOptions {
            read_timeout: settings.read_timeout,
            batch: BatchLimits::new(rows, DEFAULT_BATCH_BYTES),
        }
    }
}

pub(crate) fn request_error(
    error: &tokio_postgres::Error,
    relation: Option<&QualifiedRelation>,
) -> ConnectError {
    if let Some(db) = error.as_db_error() {
        let which = |which| ConnectError::Timeout { which };
        let code = db.code().code();
        return match (code, relation) {
            ("42501", Some(relation)) => ConnectError::PermissionDenied {
                relation: relation.clone(),
                privilege: Privilege::Select,
            },
            ("55P03", _) => which(TimeoutSetting::Lock),
            ("57014", _) => which(TimeoutSetting::Query),
            ("25P03", _) => which(TimeoutSetting::Read),
            ("42P01", Some(relation)) => ConnectError::RelationNotFound {
                relation: relation.clone(),
            },
            _ if code.starts_with("57P") => ConnectError::Disconnected,
            _ => ConnectError::Server {
                sqlstate: code.to_string(),
                message: db.message().to_string(),
            },
        };
    }
    let mut cause = error.source();
    while let Some(current) = cause {
        if current.is::<io::Error>() {
            return ConnectError::Disconnected;
        }
        cause = current.source();
    }
    if error.is_closed() {
        return ConnectError::Disconnected;
    }
    ConnectError::Protocol {
        violation: ProtocolViolation::UnexpectedResponse,
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn request<T>(
    read_timeout: Duration,
    relation: Option<&QualifiedRelation>,
    work: impl Future<Output = std::result::Result<T, tokio_postgres::Error>>,
) -> Result<T> {
    let classified = async { work.await.map_err(|error| request_error(&error, relation)) };
    within(TimeoutSetting::Read, read_timeout, classified).await
}

struct Copying {
    pooled: PooledClient<PostgresConnector>,
    copy: Pin<Box<CopyOutStream>>,
    decoder: CopyBinaryDecoder,
    pending: Bytes,
    in_transaction: bool,
    read_timeout: Duration,
    relation: Option<QualifiedRelation>,
    meter: ScanMeter,
    opened: Option<Instant>,
}

impl Copying {
    async fn open(
        pool: &Arc<PostgresPool>,
        scan: &ScanRequest,
        options: ScanOptions,
        meter: ScanMeter,
    ) -> Result<Self> {
        let opened = Some(Instant::now());
        let statement = scan.statement();
        let decoder = scan.decoder(options.batch)?;
        let relation = scan.relation().cloned();
        let timeout = options.read_timeout;
        let pooled = pool.checkout().await?;
        let client = pooled.client();
        let set_config = statement.set_config_sql();
        let search_path = scan.resolved.source.search_path();
        let in_transaction = set_config.is_some() || search_path.is_some();
        if in_transaction {
            let begin = match search_path {
                Some(search_path) => format!("{BEGIN_SCAN}; {search_path}"),
                None => BEGIN_SCAN.to_string(),
            };
            request(timeout, relation.as_ref(), client.batch_execute(&begin)).await?;
        }
        if let Some(set_config) = &set_config {
            let params: Vec<&(dyn ToSql + Sync)> = statement
                .settings
                .iter()
                .flat_map(|(name, value)| [name as &(dyn ToSql + Sync), value])
                .collect();
            let carried = client.execute(set_config.as_str(), &params);
            request(timeout, relation.as_ref(), carried).await?;
        }
        let copy = client.copy_out(statement.copy.as_str());
        let copy = request(timeout, relation.as_ref(), copy).await?;
        Ok(Copying {
            pooled,
            copy: Box::pin(copy),
            decoder,
            pending: Bytes::new(),
            in_transaction,
            read_timeout: timeout,
            relation,
            meter,
            opened,
        })
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if !self.pending.is_empty() {
                let mut rest: &[u8] = &self.pending;
                let timer = self.meter.decode.timer();
                let decoded = self.decoder.decode(&mut rest);
                timer.done();
                let consumed = self.pending.len() - rest.len();
                self.pending.advance(consumed);
                if let Some(batch) = decoded? {
                    return Ok(Some(batch));
                }
                continue;
            }
            let relation = self.relation.as_ref();
            let next = async { self.copy.next().await.transpose() };
            let Some(chunk) = request(self.read_timeout, relation, next).await? else {
                self.decoder.finish()?;
                return Ok(None);
            };
            if let Some(opened) = self.opened.take() {
                self.meter.time_to_first_byte.add_elapsed(opened);
            }
            self.meter.bytes_received.add(chunk.len());
            self.pending = chunk;
        }
    }

    async fn finish(self) -> Result<()> {
        let Copying {
            pooled,
            copy,
            in_transaction,
            read_timeout,
            relation,
            ..
        } = self;
        drop(copy);
        if in_transaction {
            let commit = pooled.client().batch_execute("COMMIT");
            request(read_timeout, relation.as_ref(), commit).await?;
        }
        pooled.release_clean().await;
        Ok(())
    }
}

enum Scan {
    Pending {
        pool: Arc<PostgresPool>,
        request: ScanRequest,
        options: ScanOptions,
        meter: ScanMeter,
    },
    Copying(Box<Copying>),
}

impl Scan {
    async fn step(self) -> Result<Option<(RecordBatch, Scan)>> {
        let mut copying = match self {
            Scan::Pending {
                pool,
                request,
                options,
                meter,
            } => Box::new(Copying::open(&pool, &request, options, meter).await?),
            Scan::Copying(copying) => copying,
        };
        let Some(batch) = copying.next_batch().await? else {
            copying.finish().await?;
            return Ok(None);
        };
        Ok(Some((batch, Scan::Copying(copying))))
    }
}

pub fn scan(
    pool: Arc<PostgresPool>,
    request: ScanRequest,
    options: ScanOptions,
) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
    scan_metered(pool, request, options, ScanMeter::default())
}

pub fn scan_metered(
    pool: Arc<PostgresPool>,
    request: ScanRequest,
    options: ScanOptions,
    meter: ScanMeter,
) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
    let start = Scan::Pending {
        pool,
        request,
        options,
        meter,
    };
    stream::try_unfold(start, Scan::step)
}
