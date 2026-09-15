//! Temp views — the session-local registration family (`createOrReplaceTempView` and friends).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;
use repark_common::{Error, Result};

use super::ReparkSession;
use crate::engine_err;

impl ReparkSession {
    /// Register `batches` as a replaceable in-memory view named `name`.
    /// # Errors
    /// Returns [`Error::DataFusion`] on empty batches or register fail; Analysis if qualified.
    pub fn create_or_replace_temp_view(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let schema = batches
            .first()
            .ok_or_else(|| {
                Error::DataFusion(format!(
                    "cannot register temp view '{name}': no batches to infer a schema from"
                ))
            })?
            .schema();
        let table = MemTable::try_new(schema, vec![batches]).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Register a planned [`DataFrame`] as a replaceable temp view named `name`.
    /// # Errors
    /// Returns [`Error::DataFusion`] if register fails; [`Error::Analysis`] if `name` is qualified.
    pub fn create_or_replace_temp_view_from(&self, name: &str, frame: &DataFrame) -> Result<()> {
        self.replace_view(name, frame.clone().into_view())
    }

    /// Collect `frame` once and register a [`MemTable`] temp view.
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if collect or registration fails.
    pub async fn materialize_dataframe_as_temp_view(
        &self,
        name: &str,
        frame: DataFrame,
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, None, None)
            .await
    }

    // === cache-honesty ===
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if collect or registration fails.
    pub async fn materialize_dataframe_as_cache_view(
        &self,
        name: &str,
        frame: DataFrame,
        budgets: (Option<u64>, Option<u64>),
    ) -> Result<()> {
        self.register_collected_memtable(name, frame, budgets.0, budgets.1)
            .await
    }

    /// Register pre-built Arrow [`RecordBatch`]es as a [`MemTable`] temp view.
    /// # Errors
    /// # Errors Returns [`Error::DataFusion`] if `MemTable` construction or registration fails.
    pub fn register_record_batches_as_temp_view(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Declare a temp view sorted by `keys` after verifying ASC NULLS LAST ordering.
    /// # Errors
    /// Returns `Error::Analysis` for a qualified name, unknown view, or non-in-memory provider.
    pub async fn declare_temp_view_sorted(
        &self,
        name: &str,
        keys: &[String],
        tighten_nulls: bool,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(Error::Analysis(
                "declared-sorted view: at least one key column is required".to_string(),
            ));
        }
        // R6-1: resolve through the temp-view choke point so declare reads the same registration.
        let reference = self.temp_view_ref(name)?;
        let provider = self
            .context()
            .table_provider(reference.clone())
            .await
            .map_err(|_| {
                Error::Analysis(format!("declared-sorted view: no temp view named '{name}'"))
            })?;
        let provider_any: &dyn std::any::Any = provider.as_ref();
        if provider_any.downcast_ref::<MemTable>().is_none() {
            return Err(Error::Analysis(format!(
                "declared-sorted view: '{name}' is not an in-memory frame — sortedness \
                 declarations support createDataFrame/cache views only"
            )));
        }
        let schema = provider.schema();
        let batches = self
            .context()
            .table(reference)
            .await
            .map_err(engine_err)?
            .collect()
            .await
            .map_err(engine_err)?;
        crate::sorted_view::verify_batches_sorted(&schema, &batches, keys)?;
        let (schema, batches) =
            crate::sorted_view::apply_declare_nullability(schema, batches, keys, tighten_nulls)?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions)
            .map_err(engine_err)?
            .with_sort_order(crate::sorted_view::declared_sort_order(keys));
        self.replace_view(name, Arc::new(table))
    }

    async fn register_collected_memtable(
        &self,
        name: &str,
        frame: DataFrame,
        max_bytes: Option<u64>,
        max_total_bytes: Option<u64>,
    ) -> Result<()> {
        let plan = frame.logical_plan().clone();
        let state = self.context().state();
        let analyzed = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .map_err(engine_err)?;
        let schema = Arc::new(analyzed.schema().as_arrow().clone());
        let optimized = state
            .optimizer()
            .optimize(analyzed.clone(), &state, |_, _| {})
            .map_err(engine_err)?;
        let physical = state
            .query_planner()
            .create_physical_plan(&optimized, &state)
            .await
            .map_err(engine_err)?;
        let (mut seen, retained) = if max_total_bytes.is_some() {
            self.live_cache_buffer_set().await?
        } else {
            (std::collections::HashSet::new(), 0)
        };
        let task_ctx = Arc::new(datafusion::execution::TaskContext::from(&state));
        let mut stream =
            datafusion::physical_plan::execute_stream(physical, task_ctx).map_err(engine_err)?;
        let mut batches = Vec::new();
        let mut admitted = 0_u64;
        let mut total = 0_u64;
        let mut over_limit = false;
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            let batch = item.map_err(engine_err)?;
            if max_total_bytes.is_some() {
                admitted = admitted.saturating_add(super::cache_budget::distinct_buffer_bytes(
                    std::slice::from_ref(&batch),
                    &mut seen,
                ));
            }
            if let Some(limit) = max_bytes {
                total = total.saturating_add(
                    u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                );
                over_limit = over_limit || total > limit;
            }
            if let Some(budget) = max_total_bytes
                && retained.saturating_add(admitted) > budget
            {
                return Err(Error::Config(format!(
                    "[REPARK_CACHE_BUDGET_EXCEEDED] cache materialize refused: \
                     repark.cache.max_total_bytes={budget}, retained {retained} bytes, \
                     admitted {admitted} bytes before refusal; release cached/eager frames \
                     with unpersist() or spark.catalog.clearCache(), or raise \
                     repark.cache.max_total_bytes"
                )));
            }
            if !over_limit {
                batches.push(batch);
            }
        }
        if let Some(limit) = max_bytes
            && over_limit
        {
            return Err(Error::Config(format!(
                "cache materialize size {total} bytes exceeds \
                 repark.cache.max_bytes={limit}; raise the conf or avoid \
                 cache()/persist() on this plan (single-node MemTable pin; no disk spill)"
            )));
        }
        drop(stream);
        let (schema, batches) = crate::sorted_view::apply_tighten_provenance_on_materialize(
            &analyzed, schema, batches,
        )?;
        let batches = conform_batches_to_schema(&schema, &batches)?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Resolve names through the build-time home and re-check its provider identity before use.
    pub(super) fn temp_view_ref(&self, name: &str) -> Result<TableReference> {
        let reference = crate::temp_view::temp_view_ref(&self.temp_view_home, name)?;
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(reference)
    }

    /// Build a home reference from an already-parsed segment after checking provider identity.
    pub(super) fn temp_view_ref_from_segment(
        &self,
        segment: &str,
        quoted: bool,
    ) -> Result<TableReference> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(crate::temp_view::temp_view_ref_from_segment(
            &self.temp_view_home,
            segment,
            quoted,
        ))
    }

    /// The session's temp-view home as `[catalog, schema]`.
    /// # Errors
    /// [`Error::Analysis`] when this session has no session-local temp-view home left.
    pub fn temp_view_home(&self) -> Result<Vec<String>> {
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        Ok(vec![
            self.temp_view_home.catalog.clone(),
            self.temp_view_home.schema.clone(),
        ])
    }

    /// Resolve a one-part name to the home-qualified `[catalog, schema, table]` reference.
    /// # Errors
    /// Fails with [`Error::Analysis`] when a catalog has replaced this session's temp-view home.
    pub fn resolve_temp_view_home_ref(&self, name: &str) -> Result<Option<Vec<String>>> {
        let Ok(parts) = crate::parse_table_identifier_segments(name) else {
            return Ok(None);
        };
        let [view] = parts.as_slice() else {
            return Ok(None);
        };
        let quoted = name.trim().starts_with(['"', '`']);
        let reference = self.temp_view_ref_from_segment(view, quoted)?;
        if self
            .context()
            .table_exist(reference.clone())
            .map_err(engine_err)?
        {
            Ok(Some(vec![
                reference.catalog().unwrap_or_default().to_string(),
                reference.schema().unwrap_or_default().to_string(),
                reference.table().to_string(),
            ]))
        } else {
            Ok(None)
        }
    }

    /// Register or replace a view through the single temp-view name seam.
    fn replace_view(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        let reference = self.temp_view_ref(name)?;
        self.context()
            .deregister_table(reference.clone())
            .map_err(engine_err)?;
        self.context()
            .register_table(reference, provider)
            .map_err(engine_err)?;
        Ok(())
    }

    /// Drop a temp view (PySpark `spark.catalog.dropTempView`).
    /// # Errors
    /// Returns `Error::DataFusion` if the name cannot be resolved as a table reference.
    pub fn drop_temp_view(&self, name: &str) -> Result<bool> {
        let reference = self.temp_view_ref(name)?;
        Ok(self
            .context()
            .deregister_table(reference)
            .map_err(engine_err)?
            .is_some())
    }
}

fn conform_batches_to_schema(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| conform_batch_to_schema(schema, batch))
        .collect()
}

fn conform_batch_to_schema(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.schema().as_ref() == schema.as_ref() {
        return Ok(batch.clone());
    }
    let batch_schema = batch.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let positions: Vec<usize> = batch_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, batch_field)| (batch_field.name() == name).then_some(index))
            .collect();
        let [position] = positions.as_slice() else {
            let plan_type = cache_column_type(field.data_type());
            if positions.is_empty() {
                return Err(Error::Analysis(format!(
                    "cache materialize: column '{name}' is missing in the executed batches but {plan_type} in the plan schema"
                )));
            }
            let seen = positions.len();
            return Err(Error::Analysis(format!(
                "cache materialize: column '{name}' appears {seen} times in the executed batches but {plan_type} in the plan schema"
            )));
        };
        let column = batch.column(*position);
        if column.data_type() == field.data_type() {
            columns.push(Arc::clone(column));
            continue;
        }
        let cast = cast_with_options(
            column,
            field.data_type(),
            &CastOptions {
                safe: false,
                ..CastOptions::default()
            },
        )
        .map_err(|error| {
            let batch_type = cache_column_type(column.data_type());
            let plan_type = cache_column_type(field.data_type());
            Error::Analysis(format!(
                "cache materialize: column '{name}' is {batch_type} in the executed batches but {plan_type} in the plan schema: {error}"
            ))
        })?;
        columns.push(cast);
    }
    let null_violator = schema
        .fields()
        .iter()
        .zip(columns.iter())
        .find_map(|(field, column)| {
            if field.is_nullable() || column.null_count() == 0 {
                None
            } else {
                Some(field.name().clone())
            }
        });
    if let Some(name) = null_violator {
        return Err(Error::Analysis(format!(
            "cache materialize: column '{name}' is nullable in the executed batches but non-nullable in the plan schema"
        )));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        Error::Analysis(format!(
            "cache materialize: conformed batch does not match the plan schema: {error}"
        ))
    })
}

fn cache_column_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        _ => data_type.to_string(),
    }
}

#[cfg(test)]
mod cache_conform_tests {
    use super::*;

    use std::collections::HashMap;

    use arrow::array::{Decimal128Array, StringArray};
    use arrow::datatypes::Field;

    fn decimal_batch(
        name: &str,
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    ) -> RecordBatch {
        let array = Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("decimal fixture keeps its precision and scale");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            name,
            DataType::Decimal128(precision, scale),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("decimal fixture batch")
    }

    fn logical_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("n", DataType::Decimal128(38, 8), false)
                .with_metadata(HashMap::from([("repark".to_string(), "cache".to_string())])),
        ]))
    }

    #[test]
    fn drifted_batch_conforms_to_the_logical_field() {
        let batch = decimal_batch("n", 38, 6, vec![Some(882_800_000)]);
        let schema = logical_schema();
        let conformed = conform_batch_to_schema(&schema, &batch).expect("drifted batch conforms");
        assert_eq!(conformed.schema(), schema);
        assert!(!conformed.schema().field(0).is_nullable());
        assert_eq!(
            conformed.schema().field(0).metadata(),
            schema.field(0).metadata()
        );
        let array = conformed
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128 output");
        assert_eq!(array.value(0), 88_280_000_000);
    }

    #[test]
    fn same_type_batch_passes_through_untouched() {
        let batch = decimal_batch("n", 38, 8, vec![Some(88_280_000_000)]);
        let schema = logical_schema();
        let conformed = conform_batch_to_schema(&schema, &batch).expect("same type passes");
        assert_eq!(conformed.schema(), schema);
        assert!(Arc::ptr_eq(conformed.column(0), batch.column(0)));
    }

    #[test]
    fn uncastable_batch_refuses_naming_both_fields() {
        let schema = logical_schema();
        let batch_schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "n",
            DataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(batch_schema, vec![Arc::new(StringArray::from(vec!["abc"]))])
                .expect("utf8 fixture batch");
        let error = conform_batch_to_schema(&schema, &batch).expect_err("uncastable batch refuses");
        assert!(matches!(error, Error::Analysis(_)));
        assert_eq!(
            error.to_string(),
            "cache materialize: column 'n' is Utf8 in the executed batches but decimal(38,8) in the plan schema: Cast error: Cannot cast string 'abc' to value of Decimal128(38, 10) type"
        );
    }

    fn two_column_batch(first: &str, second: &str) -> RecordBatch {
        let array = |value: i128| {
            Arc::new(
                Decimal128Array::from(vec![Some(value)])
                    .with_precision_and_scale(38, 8)
                    .expect("decimal fixture keeps its precision and scale"),
            ) as Arc<dyn arrow::array::Array>
        };
        let batch_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new(first, DataType::Decimal128(38, 8), true),
            Field::new(second, DataType::Decimal128(38, 8), true),
        ]));
        RecordBatch::try_new(batch_schema, vec![array(100_000_000), array(200_000_000)])
            .expect("two-column fixture batch")
    }

    fn two_column_schema(first: &str, second: &str) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new(first, DataType::Decimal128(38, 8), true),
            Field::new(second, DataType::Decimal128(38, 8), true),
        ]))
    }

    fn decimal_value(batch: &RecordBatch, index: usize) -> i128 {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128 output")
            .value(0)
    }

    #[test]
    fn reordered_equal_arity_batch_conforms_by_name() {
        let schema = two_column_schema("m", "n");
        let batch = two_column_batch("n", "m");
        let conformed =
            conform_batch_to_schema(&schema, &batch).expect("reordered batch conforms by name");
        assert_eq!(conformed.schema(), schema);
        assert_eq!(decimal_value(&conformed, 0), 200_000_000);
        assert_eq!(decimal_value(&conformed, 1), 100_000_000);
    }

    #[test]
    fn missing_name_batch_refuses() {
        let schema = two_column_schema("n", "m");
        let batch = decimal_batch("n", 38, 8, vec![Some(100_000_000)]);
        let error = conform_batch_to_schema(&schema, &batch).expect_err("missing name refuses");
        assert!(matches!(error, Error::Analysis(_)));
        assert_eq!(
            error.to_string(),
            "cache materialize: column 'm' is missing in the executed batches but decimal(38,8) in the plan schema"
        );
    }

    #[test]
    fn duplicate_name_batch_refuses() {
        let schema = logical_schema();
        let batch_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("n", DataType::Decimal128(38, 8), true),
            Field::new("n", DataType::Decimal128(38, 8), true),
        ]));
        let array = || {
            Arc::new(
                Decimal128Array::from(vec![Some(100_000_000_i128)])
                    .with_precision_and_scale(38, 8)
                    .expect("decimal fixture keeps its precision and scale"),
            ) as Arc<dyn arrow::array::Array>
        };
        let batch =
            RecordBatch::try_new(batch_schema, vec![array(), array()]).expect("duplicate fixture");
        let error = conform_batch_to_schema(&schema, &batch).expect_err("duplicate name refuses");
        assert!(matches!(error, Error::Analysis(_)));
        assert_eq!(
            error.to_string(),
            "cache materialize: column 'n' appears 2 times in the executed batches but decimal(38,8) in the plan schema"
        );
    }

    #[test]
    fn extra_batch_column_projects_away() {
        let schema = logical_schema();
        let batch = two_column_batch("n", "m");
        let conformed =
            conform_batch_to_schema(&schema, &batch).expect("extra batch column projects away");
        assert_eq!(conformed.num_columns(), 1);
        assert_eq!(decimal_value(&conformed, 0), 100_000_000);
    }

    #[test]
    fn overflowing_cast_refuses_with_cast_error() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "n",
            DataType::Decimal128(2, 1),
            true,
        )]));
        let batch = decimal_batch("n", 10, 2, vec![Some(17_656)]);
        let error = conform_batch_to_schema(&schema, &batch).expect_err("overflow refuses");
        assert!(matches!(error, Error::Analysis(_)));
        assert_eq!(
            error.to_string(),
            "cache materialize: column 'n' is decimal(10,2) in the executed batches but decimal(2,1) in the plan schema: Invalid argument error: 176.6 is too large to store in a Decimal128 of precision 2. Max is 9.9"
        );
    }

    #[test]
    fn null_in_non_nullable_plan_field_refuses() {
        let schema = logical_schema();
        let batch = decimal_batch("n", 38, 8, vec![Some(100_000_000), None]);
        let error =
            conform_batch_to_schema(&schema, &batch).expect_err("null over non-nullable refuses");
        assert!(matches!(error, Error::Analysis(_)));
        assert_eq!(
            error.to_string(),
            "cache materialize: column 'n' is nullable in the executed batches but non-nullable in the plan schema"
        );
    }
}
