//! Declared-sorted temp views (SE-1): verify a claimed row ordering, then let the
//! `MemTable` advertise it so DataFusion's `EnforceSorting` elides redundant `SortExec`s.
//!
//! Trust model: **declare + always-verify, refuse loud.** A wrong sortedness claim would
//! silently corrupt every window result, so the O(n) adjacent-pair verification pass has
//! no skip switch — it runs once per declaration and costs milliseconds against the
//! O(n log n) sort it removes from every subsequent query. Ordering spelling is
//! ASC NULLS LAST per key, matching DataFusion's `ORDER BY` defaults so a window over
//! the same keys is satisfied exactly.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::concat;
use arrow::compute::kernels::sort::{LexicographicalComparator, SortColumn, SortOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, SortExpr};

use crate::error_map::engine_err;
use repark_common::{Error, Result};

/// Arrow field metadata key written onto a key that `tightenNulls` flipped from nullable to
/// non-nullable. D2 (and the D1 Iceberg-CREATE refuse) read exactly this key; fields that were
/// already non-nullable are never tagged.
pub const TIGHTEN_NULLS_METADATA_KEY: &str = "repark.tighten_nulls";

/// Value stored under [`TIGHTEN_NULLS_METADATA_KEY`].
pub const TIGHTEN_NULLS_METADATA_VALUE: &str = "1";

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

/// ===========================================================================================
/// Apply this declare's nullability mode to the verified batches.
///
/// Always restores any previous tighten first (so a later hint-mode call is a full reset),
/// then — when `tighten_nulls` is set — refuses a NULL in any declared key and flips only
/// the keys that are still nullable, tagging exactly those with
/// [`TIGHTEN_NULLS_METADATA_KEY`].
/// ===========================================================================================
///
/// # Errors
/// [`Error::Analysis`] naming a key that is missing from the schema or that contains a NULL
/// under tighten (drop `tightenNulls` or clean the data). [`Error::DataFusion`] if a rebuilt
/// batch cannot be constructed.
pub(crate) fn apply_declare_nullability(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    keys: &[String],
    tighten_nulls: bool,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let (schema, batches) = restore_tighten_metadata(schema, batches)?;
    if tighten_nulls {
        apply_tighten(schema, batches, keys)
    } else {
        Ok((schema, batches))
    }
}

/// ===========================================================================================
/// Names of fields this declare (or a prior one) flipped, in schema order.
/// ===========================================================================================
#[must_use]
pub fn tightened_field_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|field| is_tighten_tagged(field))
        .map(|field| field.name().clone())
        .collect()
}

/// ===========================================================================================
/// D1 Iceberg-CREATE refuse: a tightened frame must not derive a table schema until D2
/// relaxes exactly the tagged fields. INSERT into an existing table is not this path.
/// ===========================================================================================
///
/// # Errors
/// [`Error::Analysis`] naming `tightenNulls` and the tagged fields when any field carries
/// the tighten metadata.
pub fn refuse_iceberg_create_of_tightened_schema(schema: &Schema) -> Result<()> {
    let names = tightened_field_names(schema);
    if names.is_empty() {
        return Ok(());
    }
    Err(Error::Analysis(format!(
        "Iceberg CREATE of a frame declared with tightenNulls=True is refused until PR-D2 \
         (the write-boundary relax). Fields tightened: [{}]. Drop tightenNulls or wait for \
         the create-path relax.",
        names.join(", ")
    )))
}

fn is_tighten_tagged(field: &Field) -> bool {
    field
        .metadata()
        .get(TIGHTEN_NULLS_METADATA_KEY)
        .is_some_and(|value| value == TIGHTEN_NULLS_METADATA_VALUE)
}

fn restore_tighten_metadata(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    if !schema.fields().iter().any(|field| is_tighten_tagged(field)) {
        return Ok((schema, batches));
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if !is_tighten_tagged(field) {
                return field.as_ref().clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.remove(TIGHTEN_NULLS_METADATA_KEY);
            field
                .as_ref()
                .clone()
                .with_nullable(true)
                .with_metadata(metadata)
        })
        .collect();
    rebuild_batches(Arc::new(Schema::new(fields)), batches)
}

fn apply_tighten(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    keys: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut flipped_any = false;
    for key in keys {
        let index = schema.index_of(key).map_err(|_| {
            Error::Analysis(format!(
                "declared-sorted view: key column '{key}' is not in the frame schema"
            ))
        })?;
        for batch in &batches {
            if batch.column(index).null_count() > 0 {
                return Err(Error::Analysis(format!(
                    "declared-sorted view: key column '{key}' contains nulls — \
                     drop tightenNulls or clean the data"
                )));
            }
        }
        let field = &fields[index];
        if !field.is_nullable() {
            continue;
        }
        let mut metadata = field.metadata().clone();
        metadata.insert(
            TIGHTEN_NULLS_METADATA_KEY.to_string(),
            TIGHTEN_NULLS_METADATA_VALUE.to_string(),
        );
        fields[index] = field.clone().with_nullable(false).with_metadata(metadata);
        flipped_any = true;
    }
    if !flipped_any {
        return Ok((schema, batches));
    }
    rebuild_batches(Arc::new(Schema::new(fields)), batches)
}

fn rebuild_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let rebuilt = batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                .map_err(|error| engine_err(error.into()))
        })
        .collect::<Result<Vec<RecordBatch>>>()?;
    Ok((schema, rebuilt))
}
