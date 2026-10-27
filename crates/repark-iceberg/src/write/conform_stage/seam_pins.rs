use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Int32Array, ListArray, MapArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, Fields, Int64Type, Schema as ArrowSchema, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    FormatVersion, ListType, NestedField, PrimitiveType, Schema, StructType, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type as ParquetType;
use repark_common::udf_names::ZONE_SEAM_NAME;
use tempfile::TempDir;

use super::{StageRequest, StageSite, conform_stage};
use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::session_staging::write_new_data_files_from_stream_with;
use crate::write::struct_pairing::{StructPairing, store_pair_udf};
use crate::write::write_options::WriterStagingOverrides;

fn wall() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn structure(fields: Vec<(&str, DataType)>) -> DataType {
    DataType::Struct(Fields::from(
        fields
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect::<Vec<_>>(),
    ))
}

fn list(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("element", element, true)))
}

const NEW_YORK_MIDNIGHT: i64 = 1_767_330_000_000_000;
const KOLKATA_MIDNIGHT: i64 = 1_767_292_200_000_000;
const UTC_MIDNIGHT: i64 = 1_767_312_000_000_000;
const NEW_YORK_WALL: i64 = 1_767_341_045_123_456;
const KOLKATA_WALL: i64 = 1_767_303_245_123_456;
const UTC_WALL: i64 = 1_767_323_045_123_456;
const NEW_YORK_SHIFT: i64 = -18_000_000_000;
const KOLKATA_SHIFT: i64 = 19_800_000_000;
const WALL_MICROS: i64 = 1_767_323_045_123_456;
const DATE_DAYS: i32 = 20455;
const ZONES: [(i64, i64, i64); 2] = [
    (NEW_YORK_SHIFT, NEW_YORK_MIDNIGHT, NEW_YORK_WALL),
    (KOLKATA_SHIFT, KOLKATA_MIDNIGHT, KOLKATA_WALL),
];

fn utc() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn field_id(field: Field, id: &str) -> Field {
    field.with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        id.to_string(),
    )]))
}

fn instants(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
}

#[derive(Debug)]
struct MirrorSeam {
    shift: i64,
    signature: Signature,
}

impl MirrorSeam {
    fn udf(shift: i64) -> ScalarUDF {
        ScalarUDF::from(Self {
            shift,
            signature: Signature::any(2, Volatility::Volatile),
        })
    }
}

impl PartialEq for MirrorSeam {
    fn eq(&self, other: &Self) -> bool {
        self.shift == other.shift
    }
}

impl Eq for MirrorSeam {}

impl Hash for MirrorSeam {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.shift.hash(state);
    }
}

fn mirror_leaf(array: &ArrayRef, target: &DataType, shift: i64) -> Result<ArrayRef> {
    match (array.data_type(), target) {
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, Some(tz))) => {
            let dates = array.as_primitive::<Date32Type>();
            let instants: TimestampMicrosecondArray = dates
                .iter()
                .map(|days| days.map(|days| i64::from(days) * 86_400_000_000 - shift))
                .collect();
            Ok(Arc::new(instants.with_timezone(Arc::clone(tz))))
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
        ) => {
            let walls = array.as_primitive::<TimestampMicrosecondType>();
            let instants: TimestampMicrosecondArray = walls
                .iter()
                .map(|wall| wall.map(|wall| wall - shift))
                .collect();
            Ok(Arc::new(instants.with_timezone(Arc::clone(tz))))
        }
        (source, target) => Err(DataFusionError::Internal(format!(
            "the test seam converts no {source} leaf into {target}"
        ))),
    }
}

fn mirror_convert(array: &ArrayRef, target: &DataType, shift: i64) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| mirror_convert(column, field.data_type(), shift))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                structs.nulls().cloned(),
            )?))
        }
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_list::<i32>();
            let values = mirror_convert(list.values(), field.data_type(), shift)?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(entries, sorted)) => {
            let map = array.as_map();
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Internal(format!(
                    "the test seam needs map entries, got {entries}"
                )));
            };
            let keys = mirror_convert(map.keys(), fields[0].data_type(), shift)?;
            let values = mirror_convert(map.values(), fields[1].data_type(), shift)?;
            let pairs = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                map.offsets().clone(),
                pairs,
                map.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => mirror_leaf(array, target, shift),
    }
}

impl ScalarUDFImpl for MirrorSeam {
    fn name(&self) -> &str {
        ZONE_SEAM_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.get(1).cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("'{}' expects a value and a target", self.name()))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [value, target] = args.arg_fields else {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects a value and a target",
                self.name()
            )));
        };
        Ok(Arc::new(Field::new(
            self.name(),
            target.data_type().clone(),
            value.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target = args.return_field.data_type().clone();
        let Some(value) = args.args.first() else {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects a value and a target",
                self.name()
            )));
        };
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(mirror_convert(
                array, &target, self.shift,
            )?)),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let converted = mirror_convert(&array, &target, self.shift)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &converted, 0,
                )?))
            }
        }
    }
}

fn seam_ctx(shift: i64) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_udf(MirrorSeam::udf(shift));
    ctx.register_udf(store_pair_udf().as_ref().clone());
    ctx
}

async fn analyzed(ctx: &SessionContext, sql: &str) -> LogicalPlan {
    let state = ctx.state();
    let plan = state.create_logical_plan(sql).await.expect("plan");
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
        .expect("analyzed")
}

fn analyzed_expr(ctx: &SessionContext, expr: Expr) -> LogicalPlan {
    let state = ctx.state();
    let empty = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(DFSchema::empty()),
    });
    let plan = LogicalPlanBuilder::from(empty)
        .project(vec![expr.alias("v")])
        .expect("project")
        .build()
        .expect("plan");
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
        .expect("analyzed")
}

fn staged(
    ctx: &SessionContext,
    plan: LogicalPlan,
    targets: &[(String, DataType)],
    site: StageSite,
) -> LogicalPlan {
    let request = StageRequest {
        site,
        targets,
        by_name: true,
        fold: false,
        gate_op: None,
        pairing: StructPairing::by_name(false),
        table: "``",
    };
    conform_stage(ctx, plan, &request).expect("staged")
}

async fn staged_batches(ctx: &SessionContext, plan: LogicalPlan) -> Vec<RecordBatch> {
    ctx.execute_logical_plan(plan)
        .await
        .expect("frame")
        .collect()
        .await
        .expect("collect")
}

fn struct_field_micros(batch: &RecordBatch, field: usize) -> Vec<Option<i64>> {
    let structs = batch.column(0).as_struct();
    let ticks = cast(structs.column(field), &DataType::Int64).expect("ticks");
    let ticks = ticks.as_primitive::<Int64Type>();
    (0..structs.len())
        .map(|row| (!structs.is_null(row) && !ticks.is_null(row)).then(|| ticks.value(row)))
        .collect()
}

fn list_first_micros(batch: &RecordBatch) -> Vec<Option<i64>> {
    let list = batch.column(0).as_list::<i32>();
    (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                return None;
            }
            let values = list.value(row);
            if values.is_empty() {
                return None;
            }
            let ticks = cast(&values, &DataType::Int64).expect("ticks");
            let ticks = ticks.as_primitive::<Int64Type>();
            (!ticks.is_null(0)).then(|| ticks.value(0))
        })
        .collect()
}

fn array_struct_first_micros(batch: &RecordBatch) -> Vec<Option<i64>> {
    let list = batch.column(0).as_list::<i32>();
    (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                return None;
            }
            let elements = list.value(row);
            if elements.is_empty() {
                return None;
            }
            let structs = elements.as_struct();
            let ticks = cast(structs.column(0), &DataType::Int64).expect("ticks");
            let ticks = ticks.as_primitive::<Int64Type>();
            (!structs.is_null(0) && !ticks.is_null(0)).then(|| ticks.value(0))
        })
        .collect()
}

fn map_key_micros(batch: &RecordBatch) -> Vec<Option<i64>> {
    let map = batch.column(0).as_map();
    let ticks = cast(map.keys(), &DataType::Int64).expect("ticks");
    let ticks = ticks.as_primitive::<Int64Type>();
    let keys: Vec<Option<i64>> = (0..ticks.len())
        .map(|row| (!ticks.is_null(row)).then(|| ticks.value(row)))
        .collect();
    assert_eq!(keys.len(), map.len());
    keys
}

fn struct_target() -> DataType {
    structure(vec![("a", utc())])
}

fn array_target() -> DataType {
    list(utc())
}

fn map_target(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key, false),
                Field::new("values", value, true),
            ])),
            false,
        )),
        false,
    )
}

fn array_struct_target() -> DataType {
    list(structure(vec![("a", utc())]))
}

fn case_sql(value: &str, target: &DataType) -> String {
    let text = target.to_string().replace('\'', "''");
    format!(
        "SELECT id, CASE WHEN id = 1 THEN arrow_cast(({value}), '{text}') ELSE v END AS v FROM t"
    )
}

fn case_scan(ctx: &SessionContext, target: &DataType) {
    let ids = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let column: ArrayRef = match target {
        DataType::Struct(fields) => Arc::new(
            StructArray::try_new(
                fields.clone(),
                vec![instants(vec![Some(UTC_MIDNIGHT)])],
                None,
            )
            .expect("scan struct"),
        ),
        DataType::List(field) => {
            let values: ArrayRef = match field.data_type() {
                DataType::Struct(fields) => Arc::new(
                    StructArray::try_new(
                        fields.clone(),
                        vec![instants(vec![Some(UTC_MIDNIGHT)])],
                        None,
                    )
                    .expect("scan elements"),
                ),
                _ => instants(vec![Some(UTC_MIDNIGHT)]),
            };
            Arc::new(
                ListArray::try_new(
                    Arc::clone(field),
                    OffsetBuffer::from_lengths([1]),
                    values,
                    None,
                )
                .expect("scan list"),
            )
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                panic!("scan map needs entries");
            };
            let pairs = StructArray::try_new(
                fields.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
                    instants(vec![Some(UTC_MIDNIGHT)]),
                ],
                None,
            )
            .expect("scan pairs");
            Arc::new(
                MapArray::try_new(
                    Arc::clone(entries),
                    OffsetBuffer::from_lengths([1]),
                    pairs,
                    None,
                    *sorted,
                )
                .expect("scan map"),
            )
        }
        _ => panic!("scan shape needs a container target"),
    };
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", target.clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, column]).expect("scan batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("scan table");
    ctx.register_table("t", Arc::new(table)).expect("register");
}

fn scan_target(shape: &str) -> DataType {
    match shape {
        "struct" => struct_target(),
        "array" => DataType::List(Arc::new(field_id(Field::new("element", utc(), true), "4"))),
        "map" => DataType::Map(
            Arc::new(field_id(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        field_id(Field::new("keys", DataType::Utf8, false), "6"),
                        field_id(Field::new("values", utc(), true), "7"),
                    ])),
                    false,
                ),
                "5",
            )),
            false,
        ),
        "array_struct" => DataType::List(Arc::new(field_id(
            Field::new("element", structure(vec![("a", utc())]), true),
            "4",
        ))),
        _ => panic!("scan shape names a container"),
    }
}

fn case_shapes() -> Vec<(&'static str, String, DataType)> {
    vec![
        (
            "struct",
            "named_struct('a', DATE '2026-01-02')".to_string(),
            struct_target(),
        ),
        (
            "array",
            "make_array(DATE '2026-01-02')".to_string(),
            array_target(),
        ),
        (
            "map",
            "map('k', DATE '2026-01-02')".to_string(),
            map_target(DataType::Utf8, utc()),
        ),
        (
            "array_struct",
            "make_array(named_struct('a', DATE '2026-01-02'))".to_string(),
            array_struct_target(),
        ),
    ]
}

fn case_micros(shape: &str, batch: &RecordBatch) -> Vec<Option<i64>> {
    let values = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            batch.schema().field(1).data_type().clone(),
            true,
        )])),
        vec![batch.column(1).clone()],
    )
    .expect("value column");
    match shape {
        "struct" => struct_field_micros(&values, 0),
        "array" => list_first_micros(&values),
        "map" => {
            let map = values.column(0).as_map();
            let ticks = cast(map.values(), &DataType::Int64).expect("ticks");
            let ticks = ticks.as_primitive::<Int64Type>();
            let found: Vec<Option<i64>> = (0..ticks.len())
                .map(|row| (!ticks.is_null(row)).then(|| ticks.value(row)))
                .collect();
            assert_eq!(found.len(), map.len());
            found
        }
        "array_struct" => array_struct_first_micros(&values),
        _ => panic!("value shape names a container"),
    }
}

#[tokio::test]
async fn case_literals_under_the_row_merge_case_store_the_session_midnight() {
    for (shift, expected, _) in ZONES {
        for (shape, value, target) in case_shapes() {
            let ctx = seam_ctx(shift);
            case_scan(&ctx, &scan_target(shape));
            let plan = analyzed(&ctx, &case_sql(&value, &target)).await;
            let targets = vec![
                ("id".to_string(), DataType::Int32),
                ("v".to_string(), target),
            ];
            let staged = staged(&ctx, plan, &targets, StageSite::MergeUpdate);
            let batches = staged_batches(&ctx, staged).await;
            assert_eq!(
                case_micros(shape, &batches[0]),
                vec![Some(expected)],
                "{shape}"
            );
        }
    }
}

#[tokio::test]
async fn a_constant_struct_stores_the_session_midnight() {
    for (shift, expected, _) in ZONES {
        let ctx = seam_ctx(shift);
        let target = struct_target();
        let text = target.to_string().replace('\'', "''");
        let plan = analyzed(
            &ctx,
            &format!("SELECT arrow_cast((named_struct('a', DATE '2026-01-02')), '{text}') AS v"),
        )
        .await;
        let targets = vec![("v".to_string(), target)];
        let staged = staged(&ctx, plan, &targets, StageSite::PredicateUpdate);
        let batches = staged_batches(&ctx, staged).await;
        assert_eq!(struct_field_micros(&batches[0], 0), vec![Some(expected)]);
    }
}

#[tokio::test]
async fn map_keys_store_the_session_midnight_from_a_literal_and_a_column() {
    for (shift, expected, _) in ZONES {
        let ctx = seam_ctx(shift);
        let keyed = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Date32, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        let pairs = StructArray::try_new(
            Fields::from(vec![
                Field::new("key", DataType::Date32, false),
                Field::new("value", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(Date32Array::from(vec![DATE_DAYS])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
            None,
        )
        .expect("source pairs");
        let maps = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", pairs.data_type().clone(), false)),
                OffsetBuffer::from_lengths([1]),
                pairs,
                None,
                false,
            )
            .expect("source maps"),
        ) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("c_map", keyed, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![maps]).expect("source batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("source table");
        ctx.register_table("src", Arc::new(table))
            .expect("register");
        let target = map_target(utc(), DataType::Utf8);
        for (value, from) in [
            ("map(DATE '2026-01-02', 'x')".to_string(), ""),
            ("c_map".to_string(), " FROM src"),
        ] {
            let plan = analyzed(&ctx, &format!("SELECT {value} AS v, 0 AS k, 1 AS id{from}")).await;
            let targets = vec![
                ("id".to_string(), DataType::Int32),
                ("v".to_string(), target.clone()),
                ("k".to_string(), DataType::Int32),
            ];
            let staged = staged(&ctx, plan, &targets, StageSite::InsertByName);
            let batches = staged_batches(&ctx, staged).await;
            assert_eq!(map_key_micros(&batches[0]), vec![Some(expected)], "{value}");
        }
    }
}

fn try_cast_to_wall(inner: Expr) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(inner), wall()))
}

fn constructor(ctx: &SessionContext, name: &str, args: Vec<Expr>) -> Expr {
    let udf = ctx.udf(name).expect("constructor");
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
}

fn arrow_cast_to(ctx: &SessionContext, value: Expr, target: &DataType) -> Expr {
    let udf = ctx.udf("arrow_cast").expect("arrow cast");
    Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![
            value,
            Expr::Literal(ScalarValue::Utf8(Some(target.to_string())), None),
        ],
    ))
}

fn text_lit(text: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(Some(text.to_string())), None)
}

#[tokio::test]
async fn try_cast_leaves_store_spark_values_and_an_instant_leaf_keeps_utc() {
    for (shift, midnight, walled) in ZONES {
        let ctx = seam_ctx(shift);
        let date = Expr::Literal(ScalarValue::Date32(Some(DATE_DAYS)), None);
        let wall_lit = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(WALL_MICROS), None),
            None,
        );
        let instant_lit = Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(UTC_WALL * 1000), Some(Arc::from("UTC"))),
            None,
        );
        let one = Expr::Literal(ScalarValue::Int32(Some(1)), None);
        let shapes: Vec<(Expr, DataType, Vec<Option<i64>>)> = vec![
            (
                constructor(
                    &ctx,
                    "named_struct",
                    vec![
                        text_lit("v"),
                        try_cast_to_wall(date.clone()),
                        text_lit("n"),
                        one.clone(),
                    ],
                ),
                structure(vec![("v", utc()), ("n", DataType::Int32)]),
                vec![Some(midnight)],
            ),
            (
                constructor(
                    &ctx,
                    "named_struct",
                    vec![
                        text_lit("v"),
                        try_cast_to_wall(wall_lit.clone()),
                        text_lit("n"),
                        one.clone(),
                    ],
                ),
                structure(vec![("v", utc()), ("n", DataType::Int32)]),
                vec![Some(walled)],
            ),
            (
                constructor(
                    &ctx,
                    "named_struct",
                    vec![
                        text_lit("d"),
                        date.clone(),
                        text_lit("v"),
                        try_cast_to_wall(instant_lit.clone()),
                        text_lit("n"),
                        one.clone(),
                    ],
                ),
                structure(vec![("d", utc()), ("v", utc()), ("n", DataType::Int32)]),
                vec![Some(midnight), Some(UTC_WALL)],
            ),
        ];
        for (value, target, expected) in shapes {
            let plan = analyzed_expr(&ctx, arrow_cast_to(&ctx, value, &target));
            let targets = vec![("v".to_string(), target.clone())];
            let staged = staged(&ctx, plan, &targets, StageSite::PredicateUpdate);
            let batches = staged_batches(&ctx, staged).await;
            let found: Vec<Option<i64>> = (0..expected.len())
                .map(|field| struct_field_micros(&batches[0], field)[0])
                .collect();
            assert_eq!(found, expected);
        }
    }
}

#[tokio::test]
async fn a_try_cast_instant_leaf_keeps_its_source_type_in_the_seam_carrier() {
    let ctx = seam_ctx(NEW_YORK_SHIFT);
    let date = Expr::Literal(ScalarValue::Date32(Some(DATE_DAYS)), None);
    let instant_lit = Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(UTC_WALL), Some(Arc::from("UTC"))),
        None,
    );
    let one = Expr::Literal(ScalarValue::Int32(Some(1)), None);
    let value = constructor(
        &ctx,
        "named_struct",
        vec![
            text_lit("d"),
            date,
            text_lit("v"),
            try_cast_to_wall(instant_lit),
            text_lit("n"),
            one,
        ],
    );
    let target = structure(vec![("d", utc()), ("v", utc()), ("n", DataType::Int32)]);
    let plan = analyzed_expr(&ctx, arrow_cast_to(&ctx, value, &target));
    let targets = vec![("v".to_string(), target)];
    let staged = staged(&ctx, plan, &targets, StageSite::PredicateUpdate);
    let LogicalPlan::Projection(projection) = &staged else {
        panic!("the stage keeps a projection");
    };
    let mut carriers = Vec::new();
    for expr in &projection.expr {
        expr.exists(|node| {
            if let Expr::ScalarFunction(function) = node
                && function.func.name() == ZONE_SEAM_NAME
                && let Some(Expr::Literal(value, _)) = function.args.get(1)
            {
                carriers.push(value.data_type());
            }
            Ok(false)
        })
        .expect("walk");
    }
    assert_eq!(
        carriers,
        vec![structure(vec![
            ("d", utc()),
            ("v", wall()),
            ("n", DataType::Int32),
        ])]
    );
}

async fn pin_table(warehouse: &TempDir, name: &str, schema: Schema, partitioned: bool) -> Table {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
        )
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new("ns".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .format_version(FormatVersion::V2);
    let creation = if partitioned {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(3, "k", Transform::Identity)
            .expect("identity partition field")
            .build();
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    catalog
        .load_table(&TableIdent::new(namespace, name.into()))
        .await
        .expect("load")
}

fn list_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(
                2,
                "v",
                Type::List(ListType::new(Arc::new(NestedField::optional(
                    3,
                    "element",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )))),
            )
            .into(),
        ])
        .build()
        .expect("schema")
}

fn struct_schema() -> Schema {
    let pair = StructType::new(vec![
        NestedField::optional(4, "a", Type::Primitive(PrimitiveType::Timestamptz)).into(),
    ]);
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Struct(pair)).into(),
            NestedField::required(3, "k", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema")
}

async fn sink_batches(table: &Table, batches: Vec<RecordBatch>) {
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("arrow schema"),
    );
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    write_new_data_files_from_stream_with(
        table,
        &write_schema,
        stream,
        WriteConcurrency::default(),
        &WriterStagingOverrides::none(),
    )
    .await
    .expect("sink");
}

fn parquet_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            parquet_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
}

fn child_named<'a>(group: &'a ParquetType, name: &str) -> Option<&'a ParquetType> {
    group
        .get_fields()
        .iter()
        .find(|field| field.get_basic_info().name() == name)
        .map(AsRef::as_ref)
}

fn parquet_batches(path: &Path) -> Vec<RecordBatch> {
    let file = std::fs::File::open(path).expect("parquet");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("builder")
        .build()
        .expect("reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("read")
}

fn value_column(batch: &RecordBatch) -> RecordBatch {
    let index = batch.schema().index_of("v").expect("v");
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![batch.schema().field(index).clone()])),
        vec![batch.column(index).clone()],
    )
    .expect("value column")
}

#[tokio::test]
async fn a_stripped_array_case_writes_field_ids_through_the_sink() {
    for (shift, expected, _) in ZONES {
        let ctx = seam_ctx(shift);
        case_scan(&ctx, &scan_target("array"));
        let target = array_target();
        let plan = analyzed(&ctx, &case_sql("make_array(DATE '2026-01-02')", &target)).await;
        let targets = vec![
            ("id".to_string(), DataType::Int32),
            ("v".to_string(), target),
        ];
        let staged = staged(&ctx, plan, &targets, StageSite::MergeUpdate);
        let batches = staged_batches(&ctx, staged).await;
        assert!(matches!(
            batches[0].schema().field(1).data_type(),
            DataType::List(element) if element.metadata().is_empty()
        ));
        let warehouse = TempDir::new().expect("warehouse");
        let table = pin_table(&warehouse, "fid", list_schema(), false).await;
        sink_batches(&table, batches).await;
        let mut files = Vec::new();
        parquet_files(warehouse.path(), &mut files);
        assert_eq!(files.len(), 1);
        let file = std::fs::File::open(&files[0]).expect("parquet");
        let reader = SerializedFileReader::new(file).expect("reader");
        let schema = reader.metadata().file_metadata().schema();
        let element = child_named(
            child_named(child_named(schema, "v").expect("v"), "list").expect("list"),
            "element",
        )
        .expect("element");
        assert!(element.get_basic_info().has_id());
        assert_eq!(element.get_basic_info().id(), 3);
        let stored = parquet_batches(&files[0]);
        assert_eq!(
            list_first_micros(&value_column(&stored[0])),
            vec![Some(expected)]
        );
    }
}

#[tokio::test]
async fn a_constant_struct_on_a_partitioned_target_stores_the_session_midnight() {
    for (shift, expected, _) in ZONES {
        let ctx = seam_ctx(shift);
        let target = struct_target();
        let text = target.to_string().replace('\'', "''");
        let plan = analyzed(
            &ctx,
            &format!(
                "SELECT 1 AS id, arrow_cast((named_struct('a', DATE '2026-01-02')), '{text}') AS v, 0 AS k"
            ),
        )
        .await;
        let targets = vec![
            ("id".to_string(), DataType::Int32),
            ("v".to_string(), target),
            ("k".to_string(), DataType::Int32),
        ];
        let staged = staged(&ctx, plan, &targets, StageSite::PredicateUpdate);
        let batches = staged_batches(&ctx, staged).await;
        let warehouse = TempDir::new().expect("warehouse");
        let table = pin_table(&warehouse, "parted", struct_schema(), true).await;
        sink_batches(&table, batches).await;
        let mut files = Vec::new();
        parquet_files(warehouse.path(), &mut files);
        assert_eq!(files.len(), 1);
        let stored = parquet_batches(&files[0]);
        assert_eq!(
            struct_field_micros(&value_column(&stored[0]), 0),
            vec![Some(expected)]
        );
    }
}
