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
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan, SortExpr};
use datafusion::prelude::SessionContext;

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
        || schema
            .fields()
            .iter()
            .any(|field| field_or_descendant_is_tagged(field, 0))
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
/// Y-3 / Y-4 (round 4): the **DDL sink** door. `CREATE VIEW cat.ns.v AS SELECT …` and
/// `SELECT … INTO cat.ns.t FROM …` never reach either door's CTAS derivation — both routers
/// drop them into their catch-all passthrough/delegate arm. DataFusion plans them as
/// `DdlStatement::CreateView` / `CreateMemoryTable` and registers the result through the
/// target schema provider's `register_table`, which for an Iceberg catalog **persists a real
/// format-v2 table** (measured — see `task/se1-declared-sorted-ledger.md` round 4). Applying
/// the same R-D predicate to the DDL body closes the tighten leak on both statements.
///
/// Scope is deliberately narrow and matches the CTAS refuse exactly:
/// - only when the DDL target **resolves** to a registered Iceberg catalog (a session-scoped
///   `CREATE VIEW v AS …` / `SELECT … INTO t` persists nothing and stays allowed — the
///   existing lazy-view pins depend on that);
/// - only under the R-D predicate (tightened source AND ≥1 non-nullable output field).
///
/// This is NOT the fix for "CREATE VIEW persists a table at all" — that behaviour predates
/// this branch (measured on BASE with an untightened source) and is recorded as a separate
/// payload finding.
/// ===========================================================================================
///
/// **Round 5 (Z-1): gate on the RESOLVED catalog, never on spelling.** A `Bare` / `Partial`
/// name is not "session-scoped" — DataFusion resolves it against
/// `datafusion.catalog.default_catalog` / `default_schema`, so after
/// `SET datafusion.catalog.default_catalog = ice` a one-part `CREATE VIEW v AS …` registers
/// into the Iceberg catalog and persists exactly the same required columns as the three-part
/// spelling (MEASURED — round-5 ledger). The resolution below is DataFusion's own
/// `TableReference::resolve` against this session's config, so the gate cannot drift from
/// where the sink actually writes.
///
/// # Errors
/// [`Error::Analysis`] when the DDL body would persist a required column from a tightened
/// source; [`Error::DataFusion`] if the plan walk fails.
pub fn refuse_iceberg_create_of_tightened_ddl(
    plan: &LogicalPlan,
    ctx: &SessionContext,
    catalogs: &crate::CatalogRegistry,
) -> Result<()> {
    let (name, input) = match plan {
        LogicalPlan::Ddl(DdlStatement::CreateView(view)) => (&view.name, view.input.as_ref()),
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(table)) => {
            (&table.name, table.input.as_ref())
        }
        _ => return Ok(()),
    };
    let config = ctx.copied_config();
    let catalog_options = &config.options().catalog;
    let resolved = name.clone().resolve(
        &catalog_options.default_catalog,
        &catalog_options.default_schema,
    );
    if catalogs.get(resolved.catalog.as_ref()).is_none() {
        return Ok(());
    }
    refuse_iceberg_create_of_tightened_plan(input)
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
    let walked = walk_tighten_sources(plan)?;
    if !walked.has_tightened_source {
        return Ok((schema, batches));
    }
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        TIGHTEN_NULLS_METADATA_KEY.to_string(),
        TIGHTEN_NULLS_METADATA_VALUE.to_string(),
    );
    // Tag every reminted required field (top-level and nested). Do not skip
    // by source-column name: `ts + 1 AS symbol` would then stay required and
    // untagged after hint restore, and CREATE would persist it (C2-Q-001).
    // Originally-required columns may widen to nullable on remint+hint — that
    // is conservative for the Iceberg "writes stay nullable" contract.
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| remint_annotate_field(field.as_ref()))
        .collect();
    rebuild_batches(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        batches,
    )
}

struct TightenSourceWalk {
    has_tightened_source: bool,
    tagged: Vec<String>,
}

fn collect_tighten_sources(plan: &LogicalPlan) -> Result<(bool, Vec<String>)> {
    walk_tighten_sources(plan).map(|walked| (walked.has_tightened_source, walked.tagged))
}

/// Walk `TableScan`s, expression subqueries, and lazy view/`into_view` inner plans
/// (`TableSource::get_logical_plan`). A `ViewTable` schema is the *output* of the
/// stored plan, so computed columns drop field tags unless this recurse happens.
///
/// Iterative (no extra Rust stack per hop). A visit budget bounds cyclic
/// `createOrReplaceTempView` graphs. The budget counts inner-plan *visits*
/// (width + depth), not nesting depth. Overflow is a generic walk error — never
/// a `tightenNulls` CREATE refusal — so a wide non-tighten UNION/CTAS/cache
/// cannot be mis-blamed on `declareSorted` (C1-Q-001).
fn walk_tighten_sources(plan: &LogicalPlan) -> Result<TightenSourceWalk> {
    const MAX_VIEW_VISITS: usize = 4096;
    let mut walked = TightenSourceWalk {
        has_tightened_source: false,
        tagged: Vec::new(),
    };
    let mut pending: Vec<LogicalPlan> = Vec::new();
    visit_tighten_sources(plan, &mut walked, &mut pending)?;
    let mut visits = 0_usize;
    while let Some(inner) = pending.pop() {
        visits += 1;
        if visits > MAX_VIEW_VISITS {
            return Err(Error::Analysis(
                "tighten-source walk exceeded view-visit budget \
                 (cyclic or extremely wide temp-view graph)"
                    .to_string(),
            ));
        }
        visit_tighten_sources(&inner, &mut walked, &mut pending)?;
    }
    Ok(walked)
}

fn visit_tighten_sources(
    plan: &LogicalPlan,
    walked: &mut TightenSourceWalk,
    pending: &mut Vec<LogicalPlan>,
) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let source_schema = scan.source.schema();
            if schema_is_tighten_derived(source_schema.as_ref()) {
                walked.has_tightened_source = true;
                for name in tightened_field_names(source_schema.as_ref()) {
                    if !walked.tagged.iter().any(|existing| existing == &name) {
                        walked.tagged.push(name);
                    }
                }
            }
            if let Some(inner) = scan.source.get_logical_plan() {
                pending.push(inner.into_owned());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(engine_err)
    .map(|_| ())
}

/// ===========================================================================================
/// Drop internal `repark.tighten_nulls` tags from a user-visible export schema.
/// Nullability is kept — only the provenance key is removed (field + schema metadata).
/// ===========================================================================================
#[must_use]
pub fn strip_tighten_export_metadata(schema: SchemaRef) -> SchemaRef {
    let has_schema_tag = schema_has_tighten_provenance(schema.as_ref());
    let has_field_tag = schema
        .fields()
        .iter()
        .any(|field| field_or_descendant_is_tagged(field, 0));
    if !has_schema_tag && !has_field_tag {
        return schema;
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| strip_field_export(field.as_ref()))
        .collect();
    let metadata = unstamp_tighten_metadata(schema.metadata().clone());
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
    field_or_child_is_non_nullable_at(field, 0)
}

const MAX_NESTED_TYPE_DEPTH: usize = 32;

fn field_or_child_is_non_nullable_at(field: &Field, depth: usize) -> bool {
    if depth > MAX_NESTED_TYPE_DEPTH {
        // Fail closed: treat as required so CREATE refuses rather than
        // stack-overflow on a hostile nested type (C1-CRATE-001).
        return true;
    }
    if !field.is_nullable() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .any(|child| field_or_child_is_non_nullable_at(child, depth + 1)),
        DataType::List(inner) | DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
            field_or_child_is_non_nullable_at(inner, depth + 1)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            // Arrow map entries are a non-null struct; Iceberg requiredness of the
            // map column is the map field itself. Only a required *value* persists
            // a nested required Iceberg field (keys are spec-required).
            DataType::Struct(fields) if fields.len() >= 2 => {
                field_or_child_is_non_nullable_at(fields[1].as_ref(), depth + 1)
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

fn stamp_tighten_metadata(
    mut metadata: std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    metadata.insert(
        TIGHTEN_NULLS_METADATA_KEY.to_string(),
        TIGHTEN_NULLS_METADATA_VALUE.to_string(),
    );
    metadata
}

fn unstamp_tighten_metadata(
    mut metadata: std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    metadata.remove(TIGHTEN_NULLS_METADATA_KEY);
    metadata
}

fn field_or_descendant_is_tagged(field: &Field, depth: usize) -> bool {
    if is_tighten_tagged(field) {
        return true;
    }
    if depth > MAX_NESTED_TYPE_DEPTH {
        return false;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .any(|child| field_or_descendant_is_tagged(child, depth + 1)),
        DataType::List(inner) | DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
            field_or_descendant_is_tagged(inner, depth + 1)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() >= 2 => {
                field_or_descendant_is_tagged(fields[1].as_ref(), depth + 1)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Tag reminted required fields (including nested struct children / list items /
/// map values) so hint restore can unflip them. Depth-capped: a hostile nest
/// must not stack-overflow remint/restore/export (C2-SAF-001).
fn remint_annotate_field(field: &Field) -> Field {
    remint_annotate_field_at(field, 0)
}

fn remint_annotate_field_at(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone();
    }
    let data_type = remint_annotate_data_type(field.data_type(), depth);
    let mut out = field.clone().with_data_type(data_type);
    if !is_tighten_tagged(field) && !field.is_nullable() {
        out = out.with_metadata(stamp_tighten_metadata(field.metadata().clone()));
    }
    out
}

fn remint_annotate_data_type(data_type: &DataType, depth: usize) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|child| remint_annotate_field_at(child.as_ref(), depth + 1))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(inner) => {
            DataType::List(Arc::new(remint_annotate_field_at(inner, depth + 1)))
        }
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(remint_annotate_field_at(inner, depth + 1)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(remint_annotate_field_at(inner, depth + 1)), *size)
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() >= 2 => {
                let key = fields[0].as_ref().clone();
                let value = remint_annotate_field_at(fields[1].as_ref(), depth + 1);
                let rebuilt = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
                DataType::Map(Arc::new(rebuilt), *sorted)
            }
            _ => data_type.clone(),
        },
        other => other.clone(),
    }
}

fn restore_field(field: &Field) -> Field {
    restore_field_at(field, 0)
}

fn restore_field_at(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone();
    }
    let data_type = restore_data_type(field.data_type(), depth);
    let mut out = field.clone().with_data_type(data_type);
    if is_tighten_tagged(field) {
        out = out
            .with_nullable(true)
            .with_metadata(unstamp_tighten_metadata(field.metadata().clone()));
    }
    out
}

fn restore_data_type(data_type: &DataType, depth: usize) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|child| restore_field_at(child.as_ref(), depth + 1))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(inner) => DataType::List(Arc::new(restore_field_at(inner, depth + 1))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(restore_field_at(inner, depth + 1)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(restore_field_at(inner, depth + 1)), *size)
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() >= 2 => {
                let key = fields[0].as_ref().clone();
                let value = restore_field_at(fields[1].as_ref(), depth + 1);
                let rebuilt = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
                DataType::Map(Arc::new(rebuilt), *sorted)
            }
            _ => data_type.clone(),
        },
        other => other.clone(),
    }
}

fn strip_field_export(field: &Field) -> Field {
    strip_field_export_at(field, 0)
}

fn strip_field_export_at(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone();
    }
    let data_type = strip_data_type_export(field.data_type(), depth);
    let mut out = field.clone().with_data_type(data_type);
    if is_tighten_tagged(field) {
        out = out.with_metadata(unstamp_tighten_metadata(field.metadata().clone()));
    }
    out
}

fn strip_data_type_export(data_type: &DataType, depth: usize) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|child| strip_field_export_at(child.as_ref(), depth + 1))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(inner) => DataType::List(Arc::new(strip_field_export_at(inner, depth + 1))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(strip_field_export_at(inner, depth + 1)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(strip_field_export_at(inner, depth + 1)), *size)
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() >= 2 => {
                let key = fields[0].as_ref().clone();
                let value = strip_field_export_at(fields[1].as_ref(), depth + 1);
                let rebuilt = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
                DataType::Map(Arc::new(rebuilt), *sorted)
            }
            _ => data_type.clone(),
        },
        other => other.clone(),
    }
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
    let has_field_tags = schema
        .fields()
        .iter()
        .any(|field| field_or_descendant_is_tagged(field, 0));
    let has_schema_tag = schema_has_tighten_provenance(schema.as_ref());
    if !has_field_tags && !has_schema_tag {
        return Ok((schema, batches));
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| restore_field(field.as_ref()))
        .collect();
    let metadata = unstamp_tighten_metadata(schema.metadata().clone());
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
