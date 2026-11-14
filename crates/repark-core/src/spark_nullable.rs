use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, GetExt, ScalarValue, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory, FileMeta};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};
use object_store::{ObjectMeta, ObjectStore};

const MAX_NESTED_TYPE_DEPTH: usize = 32;

#[must_use]
pub fn relax_schema_to_nullable(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| relax_field(field))
        .collect::<Vec<Field>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub(crate) async fn read_parquet_nullable(
    context: &SessionContext,
    path: &str,
) -> DataFusionResult<DataFrame> {
    let resolved = crate::object_store_s3::resolve_s3_prefix_for_read(context, path)
        .await
        .map_err(|error| datafusion::error::DataFusionError::External(error.to_string().into()))?;
    let inferred = context
        .read_parquet(&resolved, ParquetReadOptions::default())
        .await?;
    let relaxed = relax_schema_to_nullable(inferred.schema().as_arrow());
    let sparked = promote_parquet_null_types(&relaxed);
    read_parquet_float_bounds_inexact(context, &resolved, &sparked)
}

fn read_parquet_float_bounds_inexact(
    context: &SessionContext,
    path: &str,
    schema: &Schema,
) -> DataFusionResult<DataFrame> {
    let mut options = ParquetReadOptions::default()
        .schema(schema)
        .to_listing_options(&context.copied_config(), context.copied_table_options());
    options.format = Arc::new(FloatBoundsInexactFormat::new(options.format));
    let config = ListingTableConfig::new(ListingTableUrl::parse(path)?)
        .with_listing_options(options)
        .with_schema(Arc::new(schema.clone()));
    let provider = ListingTable::try_new(config)?.with_cache(
        context
            .runtime_env()
            .cache_manager
            .get_file_statistic_cache(),
    );
    context.read_table(Arc::new(provider))
}

pub(crate) fn register_float_bounds_inexact_parquet(
    context: &SessionContext,
) -> DataFusionResult<()> {
    context
        .state_ref()
        .write()
        .register_file_format(Arc::new(FloatBoundsInexactFactory::new()), true)
}

#[derive(Debug)]
pub(crate) struct FloatBoundsInexactFactory {
    inner: ParquetFormatFactory,
}

impl FloatBoundsInexactFactory {
    pub(crate) fn new() -> Self {
        Self {
            inner: ParquetFormatFactory::new(),
        }
    }
}

impl GetExt for FloatBoundsInexactFactory {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }
}

impl FileFormatFactory for FloatBoundsInexactFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> DataFusionResult<Arc<dyn FileFormat>> {
        let inner = self.inner.create(state, format_options)?;
        Ok(Arc::new(FloatBoundsInexactFormat::new(inner)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(FloatBoundsInexactFormat::new(self.inner.default()))
    }
}

#[derive(Debug)]
pub(crate) struct FloatBoundsInexactFormat {
    inner: Arc<dyn FileFormat>,
}

impl FloatBoundsInexactFormat {
    pub(crate) fn new(inner: Arc<dyn FileFormat>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl FileFormat for FloatBoundsInexactFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DataFusionResult<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DataFusionResult<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DataFusionResult<Statistics> {
        let statistics = self
            .inner
            .infer_stats(state, store, table_schema, object)
            .await?;
        Ok(constant_float_bounds_inexact(statistics))
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DataFusionResult<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DataFusionResult<FileMeta> {
        let meta = self
            .inner
            .infer_stats_and_ordering(state, store, table_schema, object)
            .await?;
        Ok(
            FileMeta::new(constant_float_bounds_inexact(meta.statistics))
                .with_ordering(meta.ordering),
        )
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_physical_plan(state, scan_float_bounds_inexact(conf))
            .await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

fn scan_float_bounds_inexact(mut conf: FileScanConfig) -> FileScanConfig {
    for group in &mut conf.file_groups {
        if let Some(statistics) = group.statistics_mut() {
            statistics
                .column_statistics
                .iter_mut()
                .for_each(float_column_bounds_inexact);
        }
    }
    if conf.file_source().filter().is_some() {
        return conf;
    }
    let statistics = float_bounds_inexact(conf.statistics());
    FileScanConfigBuilder::from(conf)
        .with_statistics(statistics)
        .build()
}

pub(crate) fn float_bounds_inexact(mut statistics: Statistics) -> Statistics {
    statistics
        .column_statistics
        .iter_mut()
        .for_each(float_column_bounds_inexact);
    statistics
}

pub(crate) fn constant_float_bounds_inexact(mut statistics: Statistics) -> Statistics {
    for column in &mut statistics.column_statistics {
        let constant = matches!(
            (&column.min_value, &column.max_value),
            (Precision::Exact(min), Precision::Exact(max)) if min == max
        );
        if constant {
            float_column_bounds_inexact(column);
        }
    }
    statistics
}

fn float_column_bounds_inexact(column: &mut ColumnStatistics) {
    if is_float_bound(&column.min_value) || is_float_bound(&column.max_value) {
        column.min_value = column.min_value.clone().to_inexact();
        column.max_value = column.max_value.clone().to_inexact();
    }
}

fn is_float_bound(bound: &Precision<ScalarValue>) -> bool {
    matches!(
        bound.get_value(),
        Some(ScalarValue::Float16(_) | ScalarValue::Float32(_) | ScalarValue::Float64(_))
    )
}

fn promote_parquet_null_types(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| promote_field(field, 0))
        .collect::<Vec<Field>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn promote_field(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field
            .clone()
            .with_data_type(promote_leaf(field.data_type()));
    }
    field
        .clone()
        .with_data_type(promote_data_type(field.data_type(), depth))
}

fn promote_leaf(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Null => DataType::Int32,
        other => other.clone(),
    }
}

fn promote_data_type(data_type: &DataType, depth: usize) -> DataType {
    let child = depth + 1;
    match data_type {
        DataType::Null => DataType::Int32,
        DataType::Struct(fields) => DataType::Struct(promote_fields(fields, child)),
        DataType::List(inner) => DataType::List(Arc::new(promote_field(inner, child))),
        DataType::LargeList(inner) => DataType::LargeList(Arc::new(promote_field(inner, child))),
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(promote_field(inner, child)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(promote_field(inner, child))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(promote_field(inner, child)))
        }
        DataType::Map(entries, sorted) => promote_map(entries, child).map_or_else(
            || data_type.clone(),
            |rebuilt| DataType::Map(rebuilt, *sorted),
        ),
        other => other.clone(),
    }
}

fn promote_fields(fields: &Fields, depth: usize) -> Fields {
    Fields::from(
        fields
            .iter()
            .map(|field| promote_field(field, depth))
            .collect::<Vec<Field>>(),
    )
}

fn promote_map(entries: &Field, depth: usize) -> Option<Arc<Field>> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() >= 2 => {
            let key = fields[0].as_ref().clone();
            let value = promote_field(fields[1].as_ref(), depth);
            Some(Arc::new(entries.clone().with_data_type(DataType::Struct(
                Fields::from(vec![key, value]),
            ))))
        }
        _ => None,
    }
}

enum RelaxJob {
    Field(Field),
    MapEntries(Box<RelaxMapEntries>),
}

struct RelaxMapEntries {
    entries: Field,
    key: Field,
    value: Field,
}

struct RelaxRecord {
    job: RelaxJob,
    child_count: usize,
}

fn relax_field(field: &Field) -> Field {
    let mut order: Vec<RelaxRecord> = Vec::new();
    let mut stack: Vec<RelaxJob> = vec![RelaxJob::Field(field.clone())];
    while let Some(job) = stack.pop() {
        match job {
            RelaxJob::Field(node) => {
                let children = relax_children(node.data_type());
                order.push(RelaxRecord {
                    job: RelaxJob::Field(node),
                    child_count: children.len(),
                });
                stack.extend(children);
            }
            RelaxJob::MapEntries(job) => {
                stack.push(RelaxJob::Field(job.value.clone()));
                order.push(RelaxRecord {
                    job: RelaxJob::MapEntries(job),
                    child_count: 1,
                });
            }
        }
    }
    let mut values: Vec<Field> = Vec::new();
    let mut finished = field.clone();
    for record in order.into_iter().rev() {
        let start = values
            .len()
            .checked_sub(record.child_count)
            .unwrap_or(values.len());
        let kids: Vec<Field> = values.drain(start..).collect();
        finished = match record.job {
            RelaxJob::Field(template) => rebuild_field(&template, kids),
            RelaxJob::MapEntries(job) => {
                rebuild_map_entries(&job.entries, &job.key, &job.value, kids)
            }
        };
        values.push(finished.clone());
    }
    finished
}

fn relax_children(data_type: &DataType) -> Vec<RelaxJob> {
    match data_type {
        DataType::Struct(fields) => fields
            .iter()
            .map(|child| RelaxJob::Field(child.as_ref().clone()))
            .collect(),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => vec![RelaxJob::Field(inner.as_ref().clone())],
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() >= 2 => {
                vec![RelaxJob::MapEntries(Box::new(RelaxMapEntries {
                    entries: entries.as_ref().clone(),
                    key: pair[0].as_ref().clone(),
                    value: pair[1].as_ref().clone(),
                }))]
            }
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn rebuild_field(template: &Field, kids: Vec<Field>) -> Field {
    template
        .clone()
        .with_data_type(rebuild_data_type(template.data_type(), kids))
        .with_nullable(true)
}

fn rebuild_data_type(data_type: &DataType, kids: Vec<Field>) -> DataType {
    let mut kids = kids.into_iter();
    match data_type {
        DataType::Struct(_) => DataType::Struct(Fields::from(kids.collect::<Vec<Field>>())),
        DataType::List(_) => DataType::List(Arc::new(relax_child_or(data_type, kids.next()))),
        DataType::LargeList(_) => {
            DataType::LargeList(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::ListView(_) => {
            DataType::ListView(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::LargeListView(_) => {
            DataType::LargeListView(Arc::new(relax_child_or(data_type, kids.next())))
        }
        DataType::FixedSizeList(_, size) => {
            DataType::FixedSizeList(Arc::new(relax_child_or(data_type, kids.next())), *size)
        }
        DataType::Map(_, sorted) => match kids.next() {
            Some(entries) => DataType::Map(Arc::new(entries), *sorted),
            None => data_type.clone(),
        },
        _ => data_type.clone(),
    }
}

fn relax_child_or(data_type: &DataType, kid: Option<Field>) -> Field {
    kid.unwrap_or_else(|| relax_child_placeholder(data_type))
}

fn relax_child_placeholder(data_type: &DataType) -> Field {
    match data_type {
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => inner.as_ref().clone(),
        _ => Field::new("element", DataType::Null, true),
    }
}

fn rebuild_map_entries(entries: &Field, key: &Field, value: &Field, kids: Vec<Field>) -> Field {
    let mut kids = kids.into_iter();
    let relaxed = kids.next().unwrap_or_else(|| value.clone());
    entries
        .clone()
        .with_data_type(DataType::Struct(Fields::from(vec![key.clone(), relaxed])))
}

#[cfg(test)]
mod float_bounds_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn required(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    #[test]
    fn flat_required_fields_become_nullable() {
        let schema = Schema::new(vec![
            required("id", DataType::Utf8),
            required("part", DataType::Int32),
        ]);
        let relaxed = relax_schema_to_nullable(&schema);
        assert!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("part")
                .expect("part present")
                .is_nullable()
        );
        assert_eq!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn nested_struct_list_and_map_values_become_nullable() {
        let schema = Schema::new(vec![
            required(
                "top",
                DataType::Struct(Fields::from(vec![required("inner", DataType::Utf8)])),
            ),
            required(
                "vals",
                DataType::List(Arc::new(required("element", DataType::Int32))),
            ),
            required(
                "props",
                DataType::Map(
                    Arc::new(required(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            required("key", DataType::Utf8),
                            required("value", DataType::Utf8),
                        ])),
                    )),
                    false,
                ),
            ),
        ]);
        let relaxed = relax_schema_to_nullable(&schema);
        assert!(
            relaxed
                .field_with_name("top")
                .expect("top present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("vals")
                .expect("vals present")
                .is_nullable()
        );
        assert!(
            relaxed
                .field_with_name("props")
                .expect("props present")
                .is_nullable()
        );
        let DataType::Struct(top) = relaxed
            .field_with_name("top")
            .expect("top present")
            .data_type()
        else {
            panic!("top stays a struct");
        };
        assert!(top[0].is_nullable());
        let DataType::List(element) = relaxed
            .field_with_name("vals")
            .expect("vals present")
            .data_type()
        else {
            panic!("vals stays a list");
        };
        assert!(element.is_nullable());
        let DataType::Map(entries, _) = relaxed
            .field_with_name("props")
            .expect("props present")
            .data_type()
        else {
            panic!("props stays a map");
        };
        let DataType::Struct(pair) = entries.data_type() else {
            panic!("map entries stay a struct");
        };
        assert!(!pair[0].is_nullable());
        assert!(pair[1].is_nullable());
    }

    #[test]
    fn already_nullable_schema_is_unchanged() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
        assert_eq!(relax_schema_to_nullable(&schema), schema);
    }

    #[test]
    fn schema_and_field_metadata_survive() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("k".to_string(), "v".to_string());
        let field = required("id", DataType::Utf8).with_metadata(metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], metadata.clone());
        let relaxed = relax_schema_to_nullable(&schema);
        assert_eq!(relaxed.metadata(), &metadata);
        assert_eq!(
            relaxed
                .field_with_name("id")
                .expect("id present")
                .metadata(),
            &metadata
        );
    }

    fn nested_schema(depth: usize) -> Schema {
        let mut data_type = DataType::Int32;
        for level in 0..depth {
            data_type = DataType::Struct(Fields::from(vec![Field::new(
                format!("n{level}"),
                data_type,
                false,
            )]));
        }
        Schema::new(vec![Field::new("root", data_type, false)])
    }

    fn relax_flags(depth: usize) -> Vec<bool> {
        let relaxed = relax_schema_to_nullable(&nested_schema(depth));
        let mut flags = vec![
            relaxed
                .field_with_name("root")
                .expect("root present")
                .is_nullable(),
        ];
        let mut current = relaxed
            .field_with_name("root")
            .expect("root present")
            .data_type();
        for _ in 0..depth {
            let DataType::Struct(fields) = current else {
                panic!("nest stays structs");
            };
            flags.push(fields[0].is_nullable());
            current = fields[0].data_type();
        }
        flags
    }

    #[test]
    fn relax_covers_depth_40() {
        assert_eq!(relax_flags(40), vec![true; 41]);
    }

    #[test]
    fn relax_covers_depth_200() {
        assert_eq!(relax_flags(200), vec![true; 201]);
    }

    #[test]
    fn deep_nesting_completes_with_nullable_flags() {
        assert_eq!(relax_flags(600), vec![true; 601]);
    }

    #[test]
    fn parquet_null_list_becomes_int32_list() {
        let schema = Schema::new(vec![Field::new(
            "user_properties",
            DataType::List(Arc::new(Field::new("element", DataType::Null, true))),
            true,
        )]);
        let promoted = promote_parquet_null_types(&schema);
        let DataType::List(element) = promoted
            .field_with_name("user_properties")
            .expect("user_properties present")
            .data_type()
        else {
            panic!("user_properties stays a list");
        };
        assert_eq!(element.data_type(), &DataType::Int32);
        assert!(element.is_nullable());
    }

    #[test]
    fn parquet_scalar_null_becomes_int32() {
        let schema = Schema::new(vec![Field::new("bare", DataType::Null, true)]);
        let promoted = promote_parquet_null_types(&schema);
        assert_eq!(
            promoted
                .field_with_name("bare")
                .expect("bare present")
                .data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn parquet_null_inside_struct_and_map_value_becomes_int32() {
        let schema = Schema::new(vec![
            Field::new(
                "wrap",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::Null,
                    true,
                )])),
                true,
            ),
            Field::new(
                "props",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Null, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);
        let promoted = promote_parquet_null_types(&schema);
        let DataType::Struct(wrap) = promoted
            .field_with_name("wrap")
            .expect("wrap present")
            .data_type()
        else {
            panic!("wrap stays a struct");
        };
        assert_eq!(wrap[0].data_type(), &DataType::Int32);
        let DataType::Map(entries, _) = promoted
            .field_with_name("props")
            .expect("props present")
            .data_type()
        else {
            panic!("props stays a map");
        };
        let DataType::Struct(pair) = entries.data_type() else {
            panic!("map entries stay a struct");
        };
        assert_eq!(pair[0].data_type(), &DataType::Utf8);
        assert_eq!(pair[1].data_type(), &DataType::Int32);
    }

    #[test]
    fn relax_leaves_null_list_element_as_null() {
        let schema = Schema::new(vec![Field::new(
            "user_properties",
            DataType::List(Arc::new(Field::new("element", DataType::Null, true))),
            true,
        )]);
        let relaxed = relax_schema_to_nullable(&schema);
        let DataType::List(element) = relaxed
            .field_with_name("user_properties")
            .expect("user_properties present")
            .data_type()
        else {
            panic!("user_properties stays a list");
        };
        assert_eq!(element.data_type(), &DataType::Null);
    }

    #[test]
    fn promote_preserves_int32_lists() {
        let schema = Schema::new(vec![Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        )]);
        assert_eq!(promote_parquet_null_types(&schema), schema);
    }
}
