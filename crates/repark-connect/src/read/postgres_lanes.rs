use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::try_join_all;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tokio_postgres::SimpleQueryMessage;

use super::postgres::{Copying, Prepared, ScanMeter, ScanOptions, ScanRequest, request};
use crate::error::{ConnectError, ProtocolViolation, Result};
use crate::ident::QualifiedRelation;
use crate::pool::{PooledClient, PostgresConnector, PostgresPool};

pub const BEGIN_SNAPSHOT_SCAN: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
pub const EXPORT_SNAPSHOT: &str = "SELECT pg_catalog.pg_export_snapshot()";

pub type LaneStream = BoxStream<'static, Result<RecordBatch>>;

type Client = PooledClient<PostgresConnector>;

#[derive(Clone)]
struct LaneSettings {
    pooled_at_rest: bool,
    read_timeout: Duration,
    relation: Option<QualifiedRelation>,
    meter: ScanMeter,
}

enum Lane {
    Idle {
        pooled: Box<Client>,
        queue: VecDeque<Prepared>,
        opened: Option<Instant>,
        settings: LaneSettings,
    },
    Copying {
        copying: Box<Copying>,
        queue: VecDeque<Prepared>,
        settings: LaneSettings,
    },
}

impl Lane {
    async fn step(self) -> Result<Option<(RecordBatch, Lane)>> {
        let mut lane = self;
        loop {
            lane = match lane {
                Lane::Idle {
                    pooled,
                    mut queue,
                    opened,
                    settings,
                } => {
                    let Some(prepared) = queue.pop_front() else {
                        let commit = pooled.client().batch_execute("COMMIT");
                        request(settings.read_timeout, settings.relation.as_ref(), commit).await?;
                        if settings.pooled_at_rest {
                            pooled.release_clean().await;
                        } else {
                            pooled.retire();
                        }
                        return Ok(None);
                    };
                    let meter = settings.meter.clone();
                    let timeout = settings.read_timeout;
                    let copying = Copying::start(*pooled, prepared, timeout, meter, opened).await?;
                    Lane::Copying {
                        copying: Box::new(copying),
                        queue,
                        settings,
                    }
                }
                Lane::Copying {
                    mut copying,
                    queue,
                    settings,
                } => match copying.next_batch().await? {
                    Some(batch) => {
                        let lane = Lane::Copying {
                            copying,
                            queue,
                            settings,
                        };
                        return Ok(Some((batch, lane)));
                    }
                    None => Lane::Idle {
                        pooled: Box::new(copying.into_client()),
                        queue,
                        opened: None,
                        settings,
                    },
                },
            };
        }
    }
}

fn snapshot_id(shown: &[SimpleQueryMessage]) -> Result<String> {
    let id = shown.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.try_get(0).ok().flatten(),
        _ => None,
    });
    id.filter(|id| {
        !id.is_empty()
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    })
    .map(str::to_string)
    .ok_or(ConnectError::Protocol {
        violation: ProtocolViolation::UnexpectedResponse,
    })
}

async fn share_snapshot(
    clients: &[Client],
    search_path: Option<&'static str>,
    read_timeout: Duration,
    relation: Option<&QualifiedRelation>,
) -> Result<()> {
    let Some((first, rest)) = clients.split_first() else {
        return Ok(());
    };
    let path = search_path.map_or_else(String::new, |path| format!("; {path}"));
    let begin = format!("{BEGIN_SNAPSHOT_SCAN}{path}");
    request(read_timeout, relation, first.client().batch_execute(&begin)).await?;
    if rest.is_empty() {
        return Ok(());
    }
    let exported = first.client().simple_query(EXPORT_SNAPSHOT);
    let id = snapshot_id(&request(read_timeout, relation, exported).await?)?;
    let import = format!("{BEGIN_SNAPSHOT_SCAN}; SET TRANSACTION SNAPSHOT '{id}'{path}");
    let imports = rest.iter().map(|client| {
        request(
            read_timeout,
            relation,
            client.client().batch_execute(&import),
        )
    });
    try_join_all(imports).await.map(drop)
}

#[allow(clippy::missing_errors_doc)]
pub async fn scan_lanes(
    pool: Arc<PostgresPool>,
    strides: Vec<ScanRequest>,
    max_lanes: usize,
    options: ScanOptions,
    meter: ScanMeter,
) -> Result<Vec<LaneStream>> {
    let Some(first) = strides.first() else {
        return Ok(Vec::new());
    };
    let relation = first.relation().cloned();
    let search_path = first.search_path();
    let prepared = strides
        .iter()
        .map(|stride| stride.prepared(options))
        .collect::<Result<Vec<_>>>()?;
    let wanted = max_lanes.clamp(1, prepared.len());
    let clients = pool.checkout_up_to(wanted).await?;
    let read_timeout = options.read_timeout;
    share_snapshot(&clients, search_path, read_timeout, relation.as_ref()).await?;
    let lanes = clients.len();
    let mut queues: Vec<VecDeque<Prepared>> = (0..lanes).map(|_| VecDeque::new()).collect();
    for (index, stride) in prepared.into_iter().enumerate() {
        if let Some(queue) = queues.get_mut(index % lanes) {
            queue.push_back(stride);
        }
    }
    let opened = Instant::now();
    let streams = clients
        .into_iter()
        .zip(queues)
        .enumerate()
        .map(|(index, (pooled, queue))| {
            let lane = Lane::Idle {
                pooled: Box::new(pooled),
                queue,
                opened: (index == 0).then_some(opened),
                settings: LaneSettings {
                    pooled_at_rest: index == 0,
                    read_timeout,
                    relation: relation.clone(),
                    meter: meter.clone(),
                },
            };
            stream::try_unfold(lane, Lane::step).into_stream().boxed()
        })
        .collect();
    Ok(streams)
}
