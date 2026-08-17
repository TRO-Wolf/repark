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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};

use crate::error_map::engine_err;
use repark_common::{Error, Result};

/// Arrow field metadata key written onto a key that `tightenNulls` flipped from nullable to
/// non-nullable, and onto reminted non-nullable computed columns (R-A). D1 CREATE refuse
/// is source-walk based (not "exactly this key"); D2 relaxes via the same walk. Already
/// non-nullable keys are not field-tagged at declare time; the schema-level stamp still
/// marks the provider as tighten-derived.
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
/// True when this registered provider is tighten-derived: a field tag and/or the
/// schema-level provenance stamp written by cache/persist/checkpoint materialize.
/// ===========================================================================================
#[must_use]
pub fn schema_is_tighten_derived(schema: &Schema) -> bool {
    schema_has_tighten_provenance(schema)
        || schema.fields().iter().any(|field| is_tighten_tagged(field))
}

/// ===========================================================================================
/// D1 Iceberg-CREATE refuse: a tightened frame must not derive a table schema until D2
/// relaxes via the same source walk. INSERT into an existing table is not this path.
///
/// R-D: refuse only when the schema is tighten-derived AND at least one output field is
/// non-nullable (a CREATE that would persist no required column is allowed).
/// ===========================================================================================
///
/// # Errors
/// [`Error::Analysis`] naming `tightenNulls` when a tighten-derived schema would persist
/// a non-nullable column.
pub fn refuse_iceberg_create_of_tightened_schema(schema: &Schema) -> Result<()> {
    if !schema_is_tighten_derived(schema) {
        return Ok(());
    }
    if schema
        .fields()
        .iter()
        .all(|field| !field_or_child_is_non_nullable(field))
    {
        return Ok(());
    }
    refuse_tightened_create(&tightened_field_names(schema))
}

/// ===========================================================================================
/// Source-based Iceberg-CREATE refuse (SQM F1 / R-B / R-D): walk every `TableScan`
/// **including expression subqueries** and refuse if a scanned provider is tighten-derived
/// AND the plan output has at least one non-nullable field. Output-schema tags are not
/// enough — DataFusion drops field metadata on computed expressions while propagating
/// non-nullability. `TreeNode::apply` misses subquery-expression sources; this walk uses
/// [`LogicalPlan::apply_with_subqueries`] and also follows `TableSource::get_logical_plan`
/// so a lazy `into_view` / `createOrReplaceTempView` hop cannot hide the `MemTable`.
/// ===========================================================================================
///
/// # Errors
/// [`Error::Analysis`] when a tightened source would persist a required column;
/// [`Error::DataFusion`] if the plan walk fails.
pub fn refuse_iceberg_create_of_tightened_plan(plan: &LogicalPlan) -> Result<()> {
    let (has_tightened_source, tagged) = collect_tighten_sources(plan)?;
    if !has_tightened_source || !plan_has_non_nullable_output(plan) {
        return Ok(());
    }
    refuse_tightened_create(&tagged)
}

/// ===========================================================================================
/// R-A: after cache/persist/checkpoint collect, re-stamp tighten provenance onto the new
/// `MemTable` when any plan source (including subqueries) is tighten-derived. DataFusion
/// keeps propagated `nullable: false` on computed columns but drops field metadata, so
/// the reminted provider would otherwise be untagged and both doors would go blind.
/// ===========================================================================================
///
/// # Errors
/// [`Error::DataFusion`] if the plan walk or a batch rebuild fails.
pub(crate) fn apply_tighten_provenance_on_materialize(
    plan: &LogicalPlan,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    if !plan_has_tightened_source(plan)? {
        return Ok((schema, batches));
    }
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        TIGHTEN_NULLS_METADATA_KEY.to_string(),
        TIGHTEN_NULLS_METADATA_VALUE.to_string(),
    );
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if is_tighten_tagged(field) || field.is_nullable() {
                return field.as_ref().clone();
            }
            let mut field_metadata = field.metadata().clone();
            field_metadata.insert(
                TIGHTEN_NULLS_METADATA_KEY.to_string(),
                TIGHTEN_NULLS_METADATA_VALUE.to_string(),
            );
            field.as_ref().clone().with_metadata(field_metadata)
        })
        .collect();
    rebuild_batches(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        batches,
    )
}

fn plan_has_tightened_source(plan: &LogicalPlan) -> Result<bool> {
    collect_tighten_sources(plan).map(|(has, _)| has)
}

/// Walk `TableScan`s, expression subqueries, and lazy view/`into_view` inner plans
/// (`TableSource::get_logical_plan`). A `ViewTable` schema is the *output* of the
/// stored plan, so computed columns drop field tags unless this recurse happens.
fn collect_tighten_sources(plan: &LogicalPlan) -> Result<(bool, Vec<String>)> {
    let mut tagged: Vec<String> = Vec::new();
    let mut has_tightened_source = false;
    let mut nested: Vec<LogicalPlan> = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let source_schema = scan.source.schema();
            if schema_is_tighten_derived(source_schema.as_ref()) {
                has_tightened_source = true;
                for name in tightened_field_names(source_schema.as_ref()) {
                    if !tagged.iter().any(|existing| existing == &name) {
                        tagged.push(name);
                    }
                }
            }
            if let Some(inner) = scan.source.get_logical_plan() {
                nested.push(inner.into_owned());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(engine_err)?;
    for inner in nested {
        let (inner_has, inner_tagged) = collect_tighten_sources(&inner)?;
        if inner_has {
            has_tightened_source = true;
            for name in inner_tagged {
                if !tagged.iter().any(|existing| existing == &name) {
                    tagged.push(name);
                }
            }
        }
    }
    Ok((has_tightened_source, tagged))
}

/// ===========================================================================================
/// Drop internal `repark.tighten_nulls` tags from a user-visible export schema.
/// Nullability is kept — only the provenance key is removed (field + schema metadata).
/// ===========================================================================================
#[must_use]
pub fn strip_tighten_export_metadata(schema: SchemaRef) -> SchemaRef {
    let has_schema_tag = schema_has_tighten_provenance(schema.as_ref());
    let has_field_tag = schema.fields().iter().any(|field| is_tighten_tagged(field));
    if !has_schema_tag && !has_field_tag {
        return schema;
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
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect();
    let mut metadata = schema.metadata().clone();
    metadata.remove(TIGHTEN_NULLS_METADATA_KEY);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

fn plan_has_non_nullable_output(plan: &LogicalPlan) -> bool {
    plan.schema()
        .as_arrow()
        .fields()
        .iter()
        .any(|field| field_or_child_is_non_nullable(field))
}

fn field_or_child_is_non_nullable(field: &Field) -> bool {
    if !field.is_nullable() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .any(|child| field_or_child_is_non_nullable(child)),
        DataType::List(inner) | DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
            field_or_child_is_non_nullable(inner)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            // Arrow map entries are a non-null struct; Iceberg requiredness of the
            // map column is the map field itself. Only a required *value* persists
            // a nested required Iceberg field (keys are spec-required).
            DataType::Struct(fields) if fields.len() >= 2 => {
                field_or_child_is_non_nullable(fields[1].as_ref())
            }
            _ => false,
        },
        _ => false,
    }
}

fn refuse_tightened_create(names: &[String]) -> Result<()> {
    let detail = if names.is_empty() {
        "the SELECT would persist a non-nullable column from a tighten-derived source".to_string()
    } else {
        format!("Fields tightened: [{}]", names.join(", "))
    };
    Err(Error::Analysis(format!(
        "Iceberg CREATE of a frame declared with tightenNulls=True is refused until PR-D2 \
         (the write-boundary relax). {detail}. Drop tightenNulls or wait for \
         the create-path relax."
    )))
}

fn is_tighten_tagged(field: &Field) -> bool {
    field
        .metadata()
        .get(TIGHTEN_NULLS_METADATA_KEY)
        .is_some_and(|value| value == TIGHTEN_NULLS_METADATA_VALUE)
}

fn schema_has_tighten_provenance(schema: &Schema) -> bool {
    schema
        .metadata()
        .get(TIGHTEN_NULLS_METADATA_KEY)
        .is_some_and(|value| value == TIGHTEN_NULLS_METADATA_VALUE)
}

fn restore_tighten_metadata(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let has_field_tags = schema.fields().iter().any(|field| is_tighten_tagged(field));
    let has_schema_tag = schema_has_tighten_provenance(schema.as_ref());
    if !has_field_tags && !has_schema_tag {
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
    let mut metadata = schema.metadata().clone();
    metadata.remove(TIGHTEN_NULLS_METADATA_KEY);
    rebuild_batches(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        batches,
    )
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
        if schema_has_tighten_provenance(schema.as_ref()) {
            return Ok((schema, batches));
        }
        let mut metadata = schema.metadata().clone();
        metadata.insert(
            TIGHTEN_NULLS_METADATA_KEY.to_string(),
            TIGHTEN_NULLS_METADATA_VALUE.to_string(),
        );
        return rebuild_batches(
            Arc::new(Schema::new_with_metadata(fields, metadata)),
            batches,
        );
    }
    rebuild_batches(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batches,
    )
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
