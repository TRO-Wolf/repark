use std::error::Error;
use std::ops::Range;
use std::time::Duration;

use arrow::array::RecordBatch;
use bytes::BytesMut;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Statement};

use super::{MAX_INSERT_PARAMS, WriteOptions, WriteRequest, asked, encoders};
use crate::error::Result;
use crate::ident::QualifiedRelation;
use crate::types::postgres::PlannedColumn;

#[derive(Debug, Clone, Copy)]
struct WireParam<'a>(Option<&'a [u8]>);

impl ToSql for WireParam<'_> {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        Ok(match self.0 {
            Some(wire) => {
                out.extend_from_slice(wire);
                IsNull::No
            }
            None => IsNull::Yes,
        })
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

#[derive(Default)]
struct Pending {
    wire: Vec<u8>,
    fields: Vec<Option<Range<usize>>>,
}

impl Pending {
    fn params(&self, fields: Range<usize>) -> impl ExactSizeIterator<Item = WireParam<'_>> {
        self.fields[fields]
            .iter()
            .map(|field| WireParam(field.clone().map(|range| &self.wire[range])))
    }

    fn clear(&mut self) {
        self.wire.clear();
        self.fields.clear();
    }
}

pub(crate) struct RowLane {
    columns: Vec<PlannedColumn>,
    relation: QualifiedRelation,
    request: WriteRequest,
    read_timeout: Duration,
    single: Statement,
    many: Option<Statement>,
    many_sql: String,
    rows_per_insert: usize,
    buffer_bytes: usize,
    pending: Pending,
    sent: usize,
    stored: u64,
}

impl RowLane {
    pub(crate) async fn open(
        client: &Client,
        request: &WriteRequest,
        options: WriteOptions,
    ) -> Result<RowLane> {
        let columns = request.planned();
        let relation = request.relation().clone();
        let widest = MAX_INSERT_PARAMS / columns.len().max(1);
        let rows_per_insert = options.rows_per_insert.clamp(1, widest.max(1));
        let single_sql = request.insert_statement(1);
        let single = client.prepare(single_sql.as_str());
        let single = asked(options.read_timeout, &relation, single).await?;
        Ok(RowLane {
            columns,
            relation,
            request: request.clone(),
            read_timeout: options.read_timeout,
            single,
            many: None,
            many_sql: request.insert_statement(rows_per_insert),
            rows_per_insert,
            buffer_bytes: options.copy_chunk_bytes,
            pending: Pending::default(),
            sent: 0,
            stored: 0,
        })
    }

    fn pending_rows(&self) -> usize {
        self.pending.fields.len() / self.columns.len().max(1)
    }

    async fn run(
        &mut self,
        client: &Client,
        statement: &Statement,
        rows: Range<usize>,
    ) -> Result<()> {
        let width = self.columns.len();
        let params = self.pending.params(rows.start * width..rows.end * width);
        let running = client.execute_raw(statement, params);
        self.stored += asked(self.read_timeout, &self.relation, running).await?;
        Ok(())
    }

    async fn flush_many(&mut self, client: &Client) -> Result<()> {
        let many = match self.many.take() {
            Some(many) => many,
            None if self.rows_per_insert == 1 => self.single.clone(),
            None => {
                let preparing = client.prepare(self.many_sql.as_str());
                asked(self.read_timeout, &self.relation, preparing).await?
            }
        };
        self.run(client, &many, 0..self.rows_per_insert).await?;
        self.many = Some(many);
        self.pending.clear();
        Ok(())
    }

    async fn flush_remainder(&mut self, client: &Client) -> Result<()> {
        let rows = self.pending_rows();
        if rows == 0 {
            self.pending.clear();
            return Ok(());
        }
        let sql = self.request.insert_statement(rows);
        let preparing = client.prepare(sql.as_str());
        let statement = asked(self.read_timeout, &self.relation, preparing).await?;
        self.run(client, &statement, 0..rows).await?;
        self.pending.clear();
        Ok(())
    }

    pub(crate) async fn write(&mut self, client: &Client, batch: &RecordBatch) -> Result<()> {
        let fields = encoders(&self.columns, batch)?;
        for row in 0..batch.num_rows() {
            for (field, column) in fields.iter().zip(&self.columns) {
                if field.is_null(row) {
                    self.pending.fields.push(None);
                    continue;
                }
                let start = self.pending.wire.len();
                field
                    .append(row, &mut self.pending.wire)
                    .map_err(|reason| column.unwritable(self.sent, reason))?;
                self.pending
                    .fields
                    .push(Some(start..self.pending.wire.len()));
            }
            self.sent += 1;
            if self.pending_rows() == self.rows_per_insert {
                self.flush_many(client).await?;
            } else if self.pending.wire.len() >= self.buffer_bytes {
                self.flush_remainder(client).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn finish(mut self, client: &Client) -> Result<u64> {
        self.flush_remainder(client).await?;
        Ok(self.stored)
    }
}
