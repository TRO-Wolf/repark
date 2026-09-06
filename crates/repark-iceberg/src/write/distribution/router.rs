use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::take_record_batch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use futures::channel::mpsc;
use futures::future::Either;
use futures::{SinkExt, Stream, StreamExt};
use iceberg::arrow::PartitionValueCalculator;
use iceberg::table::Table;

use super::distribution_is_none;
use crate::write::merge::iceberg_err;

pub(crate) struct PartitionRouter {
    calculator: PartitionValueCalculator,
    slots: usize,
}

impl PartitionRouter {
    pub(crate) fn try_new(table: &Table, slots: usize) -> Result<Self> {
        let calculator = PartitionValueCalculator::try_new(
            table.metadata().default_partition_spec(),
            table.metadata().current_schema(),
        )
        .map_err(iceberg_err)?;
        Ok(Self {
            calculator,
            slots: slots.max(1),
        })
    }

    pub(crate) fn route(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        let values = self.calculator.calculate(batch).map_err(iceberg_err)?;
        let mut hashes = vec![0u64; batch.num_rows()];
        create_hashes(
            &[values],
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )?;
        let slots = u64::try_from(self.slots).map_err(|_| slot_count_error(self.slots))?;
        let mut rows_per_slot: Vec<Vec<u32>> = vec![Vec::new(); self.slots];
        for (row, hash) in hashes.iter().enumerate() {
            let slot = usize::try_from(hash % slots).map_err(|_| slot_count_error(self.slots))?;
            let row = u32::try_from(row).map_err(|_| {
                DataFusionError::Execution(format!(
                    "a batch of {} rows is too tall to route by partition value",
                    batch.num_rows()
                ))
            })?;
            rows_per_slot[slot].push(row);
        }
        let mut routed = Vec::with_capacity(self.slots);
        for (slot, rows) in rows_per_slot.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            routed.push((slot, take_record_batch(batch, &UInt32Array::from(rows))?));
        }
        Ok(routed)
    }
}

fn slot_count_error(slots: usize) -> DataFusionError {
    DataFusionError::Internal(format!("{slots} write workers cannot be addressed as u64"))
}

pub(crate) fn route_partitioned_stream<S>(
    table: &Table,
    slots: usize,
    stream: S,
) -> Result<impl Stream<Item = Result<Vec<(usize, RecordBatch)>>> + Unpin>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    if distribution_is_none(table)? {
        let slots = slots.max(1);
        return Ok(Either::Right(stream.enumerate().map(
            move |(index, item)| item.map(|batch| vec![(index % slots, batch)]),
        )));
    }
    let router = PartitionRouter::try_new(table, slots)?;
    Ok(Either::Left(stream.map(move |item| {
        item.and_then(|batch| router.route(&batch))
    })))
}

pub(crate) async fn send_routed(
    senders: &mut [mpsc::Sender<RecordBatch>],
    parts: Vec<(usize, RecordBatch)>,
) -> bool {
    for (slot, part) in parts {
        if senders[slot].send(part).await.is_err() {
            return false;
        }
    }
    true
}
