//! Declared-sorted temp views (SE-1): verify a claimed row ordering, then let the
//! `MemTable` advertise it so DataFusion's `EnforceSorting` elides redundant `SortExec`s.
//!
//! Trust model: **declare + always-verify, refuse loud.** A wrong sortedness claim would
//! silently corrupt every window result, so the O(n) adjacent-pair verification pass has
//! no skip switch — it runs once per declaration and costs milliseconds against the
//! O(n log n) sort it removes from every subsequent query. Ordering spelling is
//! ASC NULLS LAST per key, matching DataFusion's `ORDER BY` defaults so a window over
//! the same keys is satisfied exactly.

use arrow::array::ArrayRef;
use arrow::compute::concat;
use arrow::compute::kernels::sort::{LexicographicalComparator, SortColumn, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, SortExpr};

use crate::error_map::engine_err;
use repark_common::{Error, Result};

/// ===========================================================================================
/// The declared sort order handed to `MemTable::with_sort_order` (one partition).
///
/// Keys are ENGINE field names, referenced via `Column::from_name` — never through the
/// ident-parsing `col()` constructor, which folds unquoted mixed-case names to lowercase
/// (the U-DF-1 defect class).
/// ===========================================================================================
pub(crate) fn declared_sort_order(keys: &[String]) -> Vec<Vec<SortExpr>> {
    let order = keys
        .iter()
        .map(|key| SortExpr::new(Expr::Column(Column::from_name(key.clone())), true, false))
        .collect();
    vec![order]
}

/// ===========================================================================================
/// Verify `batches` are lexicographically sorted by `keys` (ASC NULLS LAST), including
/// across batch boundaries.
///
/// # Errors
/// [`Error::Analysis`] naming the unknown key, or the first out-of-order row pair (global
/// row indices), so the caller can find the offending data. [`Error::DataFusion`] only for
/// engine-level comparator failures (an unsupported key type surfaces here loudly).
/// ===========================================================================================
pub(crate) fn verify_batches_sorted(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    keys: &[String],
) -> Result<()> {
    let indices: Vec<usize> = keys
        .iter()
        .map(|key| {
            schema.index_of(key).map_err(|_| {
                Error::Analysis(format!(
                    "declared-sorted view: key column '{key}' is not in the frame schema"
                ))
            })
        })
        .collect::<Result<Vec<usize>>>()?;
    let options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let mut global_row: usize = 0;
    let mut previous_tail: Option<Vec<ArrayRef>> = None;
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let key_columns: Vec<ArrayRef> = indices
            .iter()
            .map(|&index| ArrayRef::clone(batch.column(index)))
            .collect();

        // Boundary pair: previous batch's last row vs this batch's first row, compared
        // through the same lexicographic machinery over a stitched two-row column set.
        if let Some(tail) = previous_tail.take() {
            let stitched: Vec<SortColumn> = tail
                .iter()
                .zip(&key_columns)
                .map(|(last, column)| {
                    Ok(SortColumn {
                        values: concat(&[last.as_ref(), column.slice(0, 1).as_ref()])
                            .map_err(|error| engine_err(error.into()))?,
                        options: Some(options),
                    })
                })
                .collect::<Result<Vec<SortColumn>>>()?;
            let comparator = LexicographicalComparator::try_new(&stitched)
                .map_err(|error| engine_err(error.into()))?;
            if comparator.compare(0, 1) == std::cmp::Ordering::Greater {
                return Err(out_of_order(global_row - 1, global_row, keys));
            }
        }

        if batch.num_rows() >= 2 {
            let sort_columns: Vec<SortColumn> = key_columns
                .iter()
                .map(|column| SortColumn {
                    values: ArrayRef::clone(column),
                    options: Some(options),
                })
                .collect();
            let comparator = LexicographicalComparator::try_new(&sort_columns)
                .map_err(|error| engine_err(error.into()))?;
            for row in 0..batch.num_rows() - 1 {
                if comparator.compare(row, row + 1) == std::cmp::Ordering::Greater {
                    return Err(out_of_order(global_row + row, global_row + row + 1, keys));
                }
            }
        }

        previous_tail = Some(
            key_columns
                .iter()
                .map(|column| column.slice(batch.num_rows() - 1, 1))
                .collect(),
        );
        global_row += batch.num_rows();
    }
    Ok(())
}

fn out_of_order(first: usize, second: usize, keys: &[String]) -> Error {
    Error::Analysis(format!(
        "declared-sorted view: rows {first} and {second} are out of order for keys \
         [{}] (ASC NULLS LAST) — the data is not sorted as declared",
        keys.join(", ")
    ))
}
