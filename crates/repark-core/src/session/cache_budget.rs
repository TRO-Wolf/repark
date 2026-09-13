use std::collections::HashSet;

use arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use repark_common::Result;

use super::ReparkSession;
use crate::engine_err;

pub(crate) const CACHE_VIEW_PREFIX: &str = "__repark_cache_";

pub(crate) fn distinct_buffer_bytes(batches: &[RecordBatch], seen: &mut HashSet<usize>) -> u64 {
    let mut total = 0_u64;
    for batch in batches {
        for column in batch.columns() {
            walk_array_data(&column.to_data(), seen, &mut total);
        }
    }
    total
}

fn walk_array_data(data: &arrow::array::ArrayData, seen: &mut HashSet<usize>, total: &mut u64) {
    let mut record = |buffer: &arrow::buffer::Buffer| {
        if seen.insert(buffer.data_ptr().as_ptr() as usize) {
            *total = total.saturating_add(u64::try_from(buffer.capacity()).unwrap_or(u64::MAX));
        }
    };
    for buffer in data.buffers() {
        record(buffer);
    }
    if let Some(nulls) = data.nulls() {
        record(nulls.buffer());
    }
    for child in data.child_data() {
        walk_array_data(child, seen, total);
    }
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn retained_cache_bytes(&self) -> Result<u64> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        let Some(schema) = self.temp_view_home.provider.as_ref() else {
            return Ok(0);
        };
        let mut seen = HashSet::new();
        let mut total = 0_u64;
        for name in schema.table_names() {
            if !name.starts_with(CACHE_VIEW_PREFIX) {
                continue;
            }
            let Some(provider) = schema.table(&name).await.map_err(engine_err)? else {
                continue;
            };
            let provider_any: &dyn std::any::Any = provider.as_ref();
            let Some(table) = provider_any.downcast_ref::<MemTable>() else {
                continue;
            };
            for partition in &table.batches {
                let batches = partition.read().await.clone();
                total = total.saturating_add(distinct_buffer_bytes(&batches, &mut seen));
            }
        }
        Ok(total)
    }
}
