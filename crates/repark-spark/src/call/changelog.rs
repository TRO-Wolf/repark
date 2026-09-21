use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_CHANGE_ORDINAL, RESERVED_COL_NAME_CHANGE_TYPE,
    RESERVED_COL_NAME_COMMIT_SNAPSHOT_ID,
};

pub const INSERT: &str = "INSERT";
pub const DELETE: &str = "DELETE";
pub const UPDATE_BEFORE: &str = "UPDATE_BEFORE";
pub const UPDATE_AFTER: &str = "UPDATE_AFTER";

const MULTIPLE_ROWS_PER_IDENTIFIER: &str = "Cannot compute updates because there are multiple rows with the same identifier fields([{}]). Please make sure the rows are unique.";

#[derive(Debug, Clone, Copy)]
struct Emitted {
    row: usize,
    change_type: Option<&'static str>,
}

struct ChangelogPlan {
    change_types: Vec<String>,
    order: Vec<usize>,
    identity: Rows,
}

impl ChangelogPlan {
    fn new(
        batch: &RecordBatch,
        sort_columns: &[usize],
        identity_columns: &[usize],
    ) -> Result<Self> {
        let change_types = change_type_values(batch)?;
        let order = sorted_order(batch, sort_columns)?;
        let identity = key_rows(batch, identity_columns)?;
        Ok(Self {
            change_types,
            order,
            identity,
        })
    }

    fn same(&self, left: usize, right: usize) -> bool {
        self.identity.row(left) == self.identity.row(right)
    }

    fn is(&self, row: usize, change_type: &str) -> bool {
        self.change_types[row] == change_type
    }

    fn opposite(&self, left: usize, right: usize) -> bool {
        (self.is(right, INSERT) && self.is(left, DELETE))
            || (self.is(right, DELETE) && self.is(left, INSERT))
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn remove_carryovers(batch: &RecordBatch, net_changes: bool) -> Result<RecordBatch> {
    let metadata = metadata_indices(batch)?;
    let mut identity: Vec<usize> = (0..batch.num_columns()).collect();
    identity.retain(|index| {
        *index != metadata.change_type
            && !(net_changes && (*index == metadata.ordinal || *index == metadata.commit))
    });
    let mut sort_columns = identity.clone();
    if net_changes {
        sort_columns.push(metadata.ordinal);
    }
    sort_columns.push(metadata.change_type);
    let plan = ChangelogPlan::new(batch, &sort_columns, &identity)?;
    let emitted = if net_changes {
        walk_net_carryovers(&plan)
    } else {
        walk_carryovers(&plan)
    };
    materialize(batch, &emitted, metadata.change_type)
}

#[allow(clippy::missing_errors_doc)]
pub fn compute_updates(batch: &RecordBatch, identifier_columns: &[String]) -> Result<RecordBatch> {
    let metadata = metadata_indices(batch)?;
    let identifiers = identifier_indices(batch, identifier_columns)?;
    let mut sort_columns = identifiers.clone();
    sort_columns.push(metadata.ordinal);
    sort_columns.push(metadata.change_type);
    let mut whole_row: Vec<usize> = (0..batch.num_columns()).collect();
    whole_row.retain(|index| *index != metadata.change_type);
    let carryover = ChangelogPlan::new(batch, &sort_columns, &whole_row)?;
    let surviving = walk_carryovers(&carryover);
    let paired = walk_updates(
        &carryover.change_types,
        &surviving,
        &key_rows(batch, &identifiers)?,
        identifier_columns,
    )?;
    materialize(batch, &paired, metadata.change_type)
}

fn walk_carryovers(plan: &ChangelogPlan) -> Vec<Emitted> {
    let order = &plan.order;
    let mut emitted = Vec::with_capacity(order.len());
    let mut index = 0;
    while index < order.len() {
        let row = order[index];
        if !plan.is(row, DELETE) || index + 1 == order.len() {
            emitted.push(Emitted {
                row,
                change_type: None,
            });
            index += 1;
            continue;
        }
        let mut deleted = 1_i64;
        let mut cursor = index + 1;
        while cursor < order.len() && plan.same(row, order[cursor]) && deleted > 0 {
            if plan.is(order[cursor], INSERT) {
                deleted -= 1;
            } else {
                deleted += 1;
            }
            cursor += 1;
        }
        for _ in 0..deleted.max(0) {
            emitted.push(Emitted {
                row,
                change_type: None,
            });
        }
        index = cursor;
    }
    emitted
}

fn walk_net_carryovers(plan: &ChangelogPlan) -> Vec<Emitted> {
    let order = &plan.order;
    let mut emitted = Vec::with_capacity(order.len());
    let mut index = 0;
    while index < order.len() {
        let row = order[index];
        if index + 1 == order.len() {
            emitted.push(Emitted {
                row,
                change_type: None,
            });
            break;
        }
        let mut net = 1_i64;
        let mut cursor = index + 1;
        let resume = loop {
            if !plan.same(row, order[cursor]) {
                break cursor;
            }
            if plan.opposite(row, order[cursor]) {
                net -= 1;
            } else {
                net += 1;
            }
            if net > 0 && cursor + 1 < order.len() {
                cursor += 1;
            } else {
                break cursor + 1;
            }
        };
        for _ in 0..net.max(0) {
            emitted.push(Emitted {
                row,
                change_type: None,
            });
        }
        index = resume;
    }
    emitted
}

fn walk_updates(
    change_types: &[String],
    surviving: &[Emitted],
    identity: &Rows,
    identifier_columns: &[String],
) -> Result<Vec<Emitted>> {
    let mut emitted = Vec::with_capacity(surviving.len());
    let mut index = 0;
    while index < surviving.len() {
        let current = surviving[index];
        if change_types[current.row] != DELETE || index + 1 == surviving.len() {
            emitted.push(current);
            index += 1;
            continue;
        }
        let next = surviving[index + 1];
        if identity.row(current.row) != identity.row(next.row) {
            emitted.push(current);
            index += 1;
            continue;
        }
        if change_types[next.row] != INSERT {
            return Err(repark_core::illegal_argument_error(
                MULTIPLE_ROWS_PER_IDENTIFIER.replace("{}", &identifier_columns.join(",")),
            ));
        }
        emitted.push(Emitted {
            row: current.row,
            change_type: Some(UPDATE_BEFORE),
        });
        emitted.push(Emitted {
            row: next.row,
            change_type: Some(UPDATE_AFTER),
        });
        index += 2;
    }
    Ok(emitted)
}

struct MetadataIndices {
    change_type: usize,
    ordinal: usize,
    commit: usize,
}

fn metadata_indices(batch: &RecordBatch) -> Result<MetadataIndices> {
    Ok(MetadataIndices {
        change_type: column_index(batch.schema_ref(), RESERVED_COL_NAME_CHANGE_TYPE)?,
        ordinal: column_index(batch.schema_ref(), RESERVED_COL_NAME_CHANGE_ORDINAL)?,
        commit: column_index(batch.schema_ref(), RESERVED_COL_NAME_COMMIT_SNAPSHOT_ID)?,
    })
}

fn column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
    schema.index_of(name).map_err(|_| {
        DataFusionError::Plan(format!("changelog rows are missing the `{name}` column"))
    })
}

fn identifier_indices(batch: &RecordBatch, identifier_columns: &[String]) -> Result<Vec<usize>> {
    identifier_columns
        .iter()
        .map(|name| {
            batch.schema_ref().index_of(name).map_err(|_| {
                repark_core::illegal_argument_error(format!(
                    "Identifier column `{name}` is not in the changelog schema"
                ))
            })
        })
        .collect()
}

fn change_type_values(batch: &RecordBatch) -> Result<Vec<String>> {
    let index = column_index(batch.schema_ref(), RESERVED_COL_NAME_CHANGE_TYPE)?;
    let column = batch.column(index);
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "changelog `_change_type` must be a string column, got {}",
                column.data_type()
            ))
        })?;
    Ok((0..values.len())
        .map(|row| {
            if values.is_null(row) {
                String::new()
            } else {
                values.value(row).to_string()
            }
        })
        .collect())
}

fn key_rows(batch: &RecordBatch, columns: &[usize]) -> Result<Rows> {
    let fields: Vec<SortField> = columns
        .iter()
        .map(|index| SortField::new(batch.column(*index).data_type().clone()))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|index| Arc::clone(batch.column(*index)))
        .collect();
    let converter = RowConverter::new(fields)?;
    converter.convert_columns(&arrays).map_err(Into::into)
}

fn sorted_order(batch: &RecordBatch, sort_columns: &[usize]) -> Result<Vec<usize>> {
    let rows = key_rows(batch, sort_columns)?;
    let mut order: Vec<usize> = (0..batch.num_rows()).collect();
    order.sort_by(|left, right| rows.row(*left).cmp(&rows.row(*right)));
    Ok(order)
}

fn materialize(
    batch: &RecordBatch,
    emitted: &[Emitted],
    change_type_index: usize,
) -> Result<RecordBatch> {
    let indices = UInt32Array::from(
        emitted
            .iter()
            .map(|entry| u32::try_from(entry.row).unwrap_or(u32::MAX))
            .collect::<Vec<u32>>(),
    );
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (index, column) in batch.columns().iter().enumerate() {
        if index == change_type_index {
            columns.push(rebuilt_change_types(batch, emitted, change_type_index)?);
            continue;
        }
        columns.push(take(column.as_ref(), &indices, None)?);
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn rebuilt_change_types(
    batch: &RecordBatch,
    emitted: &[Emitted],
    change_type_index: usize,
) -> Result<ArrayRef> {
    let source = batch
        .column(change_type_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("changelog `_change_type` must be a string column".to_string())
        })?;
    let values: Vec<Option<String>> = emitted
        .iter()
        .map(|entry| match entry.change_type {
            Some(overridden) => Some(overridden.to_string()),
            None if source.is_null(entry.row) => None,
            None => Some(source.value(entry.row).to_string()),
        })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

#[derive(Debug, Clone)]
pub(crate) enum ChangelogTransform {
    Carryovers { net_changes: bool },
    UpdateImages { identifier_columns: Vec<String> },
}

impl ChangelogTransform {
    pub(crate) fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        match self {
            Self::Carryovers { net_changes } => remove_carryovers(batch, *net_changes),
            Self::UpdateImages { identifier_columns } => compute_updates(batch, identifier_columns),
        }
    }
}

#[cfg(test)]
mod tests;
