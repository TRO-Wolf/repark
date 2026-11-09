#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StringViewArray,
    StructArray,
};
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::{
    FormatVersion, ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{DataFusionDialect, ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Door {
    Ansi,
    NativeDataFusion,
    Spark,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    UnionAll,
    Union,
    UnionDistinct,
    Intersect,
    Except,
    IntersectAll,
    ExceptAll,
    DfUnion,
    DfUnionByName,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Form {
    Read,
    Insert,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Struct2,
    Struct2Mixed,
    Struct3,
    Nested,
    ListStruct,
    MapStruct,
}

#[derive(Clone, Copy, Debug)]
enum Expect {
    Answer((&'static str, &'static str, &'static str)),
    Refuse((&'static str, &'static str, &'static str)),
    MapAnswer((&'static str, &'static str, &'static str)),
    MapRefuse((&'static str, &'static str, &'static str)),
}

pub struct Shape {
    id: &'static str,
    left: &'static str,
    right_union: &'static str,
    right_set: &'static str,
    family: Family,
    expect: Expect,
}

const A1B2: &str = "named_struct('a', 1, 'b', 2)";
const A3B4: &str = "named_struct('a', 3, 'b', 4)";
const B3A4: &str = "named_struct('b', 3, 'a', 4)";
const B2A1: &str = "named_struct('b', 2, 'a', 1)";
const X1B2: &str = "named_struct('x', named_struct('a', 1, 'b', 2))";
const X3B4: &str = "named_struct('x', named_struct('a', 3, 'b', 4))";
const ARR_AB: &str = "array(named_struct('a', 1, 'b', 2))";
const ARR_AB34: &str = "array(named_struct('a', 3, 'b', 4))";
const ARR_BA34: &str = "array(named_struct('b', 3, 'a', 4))";
const ARR_BA21: &str = "array(named_struct('b', 2, 'a', 1))";
const AX: &str = "named_struct('a', 1, 'b', 'x')";
const BYA2: &str = "named_struct('b', 'y', 'a', 2)";
const BXA1: &str = "named_struct('b', 'x', 'a', 1)";
const A3BY: &str = "named_struct('a', 3, 'b', 'y')";
const CD34: &str = "named_struct('c', 3, 'd', 4)";
const CD12: &str = "named_struct('c', 1, 'd', 2)";
const AC34: &str = "named_struct('a', 3, 'c', 4)";
const AC12: &str = "named_struct('a', 1, 'c', 2)";
const BA34: &str = "named_struct('B', 3, 'A', 4)";
const BA21: &str = "named_struct('B', 2, 'A', 1)";
const XBA34: &str = "named_struct('x', named_struct('b', 3, 'a', 4))";
const XBA21: &str = "named_struct('x', named_struct('b', 2, 'a', 1))";
const T123: &str = "named_struct('a', 1, 'b', 2, 'c', 3)";
const TCAB456: &str = "named_struct('c', 4, 'a', 5, 'b', 6)";
const TCAB312: &str = "named_struct('c', 3, 'a', 1, 'b', 2)";
const NARR_AB: &str = "ARRAY[named_struct('a', 1, 'b', 2)]";
const NARR_AB34: &str = "ARRAY[named_struct('a', 3, 'b', 4)]";
const NARR_BA34: &str = "ARRAY[named_struct('b', 3, 'a', 4)]";
const NARR_BA21: &str = "ARRAY[named_struct('b', 2, 'a', 1)]";
const NMAP_AB: &str = "MAP(ARRAY['k'], ARRAY[named_struct('a', 1, 'b', 2)])";
const NMAP_AB34: &str = "MAP(ARRAY['k'], ARRAY[named_struct('a', 3, 'b', 4)])";
const NMAP_BA34: &str = "MAP(ARRAY['k'], ARRAY[named_struct('b', 3, 'a', 4)])";
const NMAP_BA21: &str = "MAP(ARRAY['k'], ARRAY[named_struct('b', 2, 'a', 1)])";
const T345: &str = "named_struct('a', 3, 'b', 4, 'c', 5)";
const UNION_12_34: (&str, &str, &str) = ("[1,2]|[3,4]", "", "[1,2]");

const fn answer_shape(
    id: &'static str,
    operands: (&'static str, &'static str, &'static str),
    family: Family,
    rows: (&'static str, &'static str, &'static str),
) -> Shape {
    Shape {
        id,
        left: operands.0,
        right_union: operands.1,
        right_set: operands.2,
        family,
        expect: Expect::Answer(rows),
    }
}

const fn refuse_shape(
    id: &'static str,
    operands: (&'static str, &'static str, &'static str),
    family: Family,
    finding: (&'static str, &'static str, &'static str),
) -> Shape {
    Shape {
        id,
        left: operands.0,
        right_union: operands.1,
        right_set: operands.2,
        family,
        expect: Expect::Refuse(finding),
    }
}

pub const SHAPES: [Shape; 20] = [
    answer_shape(
        "same-order",
        (A1B2, A3B4, A1B2),
        Family::Struct2,
        ("[1,2]|[3,4]", "[1,2]", ""),
    ),
    refuse_shape(
        "reordered",
        (A1B2, B3A4, B2A1),
        Family::Struct2,
        ("s", "b, a", "a, b"),
    ),
    refuse_shape(
        "reordered-typed",
        (AX, BYA2, BXA1),
        Family::Struct2Mixed,
        ("s", "b, a", "a, b"),
    ),
    refuse_shape(
        "disjoint-names",
        (A1B2, CD34, CD12),
        Family::Struct2,
        ("s", "c, d", "a, b"),
    ),
    refuse_shape(
        "partial-overlap",
        (A1B2, AC34, AC12),
        Family::Struct2,
        ("s", "a, c", "a, b"),
    ),
    refuse_shape(
        "case-reordered",
        (A1B2, BA34, BA21),
        Family::Struct2,
        ("s", "B, A", "a, b"),
    ),
    refuse_shape(
        "nested-reordered",
        (X1B2, XBA34, XBA21),
        Family::Nested,
        ("s.x", "b, a", "a, b"),
    ),
    refuse_shape(
        "array-reordered",
        (ARR_AB, ARR_BA34, ARR_BA21),
        Family::ListStruct,
        ("s[]", "b, a", "a, b"),
    ),
    Shape {
        id: "map-value-reordered",
        left: "map('k', named_struct('a', 1, 'b', 2))",
        right_union: "map('k', named_struct('b', 3, 'a', 4))",
        right_set: "map('k', named_struct('b', 2, 'a', 1))",
        family: Family::MapStruct,
        expect: Expect::MapRefuse(("s{value}", "b, a", "a, b")),
    },
    refuse_shape(
        "three-way",
        (T123, TCAB456, TCAB312),
        Family::Struct3,
        ("s", "c, a, b", "a, b, c"),
    ),
    answer_shape(
        "ctl-same-order",
        (A1B2, A3B4, A3B4),
        Family::Struct2,
        UNION_12_34,
    ),
    answer_shape(
        "ctl-reordered",
        (A1B2, A3B4, A3B4),
        Family::Struct2,
        UNION_12_34,
    ),
    answer_shape(
        "ctl-reordered-typed",
        (AX, A3BY, A3BY),
        Family::Struct2Mixed,
        ("[1,\"x\"]|[3,\"y\"]", "", "[1,\"x\"]"),
    ),
    answer_shape(
        "ctl-disjoint-names",
        (A1B2, A3B4, A3B4),
        Family::Struct2,
        UNION_12_34,
    ),
    answer_shape(
        "ctl-partial-overlap",
        (A1B2, A3B4, A3B4),
        Family::Struct2,
        UNION_12_34,
    ),
    answer_shape(
        "ctl-case-reordered",
        (A1B2, A3B4, A3B4),
        Family::Struct2,
        UNION_12_34,
    ),
    answer_shape(
        "ctl-nested-reordered",
        (X1B2, X3B4, X3B4),
        Family::Nested,
        ("[[1,2]]|[[3,4]]", "", "[[1,2]]"),
    ),
    answer_shape(
        "ctl-array-reordered",
        (ARR_AB, ARR_AB34, ARR_AB34),
        Family::ListStruct,
        ("[[1,2]]|[[3,4]]", "", "[[1,2]]"),
    ),
    Shape {
        id: "ctl-map-value-reordered",
        left: "map('k', named_struct('a', 1, 'b', 2))",
        right_union: "map('k', named_struct('a', 3, 'b', 4))",
        right_set: "map('k', named_struct('a', 3, 'b', 4))",
        family: Family::MapStruct,
        expect: Expect::MapAnswer(("[[\"k\",[1,2]]]|[[\"k\",[3,4]]]", "", "[[\"k\",[1,2]]]")),
    },
    answer_shape(
        "ctl-three-way",
        (T123, T345, T345),
        Family::Struct3,
        ("[1,2,3]|[3,4,5]", "", "[1,2,3]"),
    ),
];

const OVERREFUSAL: [&str; 5] = [
    "reordered",
    "reordered-typed",
    "nested-reordered",
    "array-reordered",
    "three-way",
];

fn native_spelling(id: &str) -> Option<(&'static str, &'static str, &'static str)> {
    match id {
        "array-reordered" => Some((NARR_AB, NARR_BA34, NARR_BA21)),
        "ctl-array-reordered" => Some((NARR_AB, NARR_AB34, NARR_AB34)),
        "map-value-reordered" => Some((NMAP_AB, NMAP_BA34, NMAP_BA21)),
        "ctl-map-value-reordered" => Some((NMAP_AB, NMAP_AB34, NMAP_AB34)),
        _ => None,
    }
}

fn operands(door: Door, shape: &Shape) -> (&'static str, &'static str, &'static str) {
    if door == Door::Spark {
        return (shape.left, shape.right_union, shape.right_set);
    }
    native_spelling(shape.id).unwrap_or((shape.left, shape.right_union, shape.right_set))
}

fn op_token(op: Op) -> &'static str {
    match op {
        Op::UnionAll => "UNION ALL",
        Op::Union | Op::DfUnion | Op::DfUnionByName => "UNION",
        Op::UnionDistinct => "UNION DISTINCT",
        Op::Intersect => "INTERSECT",
        Op::Except => "EXCEPT",
        Op::IntersectAll => "INTERSECT ALL",
        Op::ExceptAll => "EXCEPT ALL",
    }
}

fn op_word(op: Op) -> &'static str {
    match op {
        Op::UnionAll | Op::Union | Op::UnionDistinct | Op::DfUnion | Op::DfUnionByName => "UNION",
        Op::Intersect => "INTERSECT",
        Op::IntersectAll => "INTERSECT ALL",
        Op::Except => "EXCEPT",
        Op::ExceptAll => "EXCEPT ALL",
    }
}

fn op_slug(op: Op) -> &'static str {
    match op {
        Op::UnionAll => "union-all",
        Op::Union => "union",
        Op::UnionDistinct => "union-distinct",
        Op::Intersect => "intersect",
        Op::Except => "except",
        Op::IntersectAll => "intersect-all",
        Op::ExceptAll => "except-all",
        Op::DfUnion => "df-union",
        Op::DfUnionByName => "df-union-by-name",
    }
}

fn row_name(door: Door, op: Op, shape: &Shape, form: Form) -> String {
    let door_token = match door {
        Door::Ansi => "ansi",
        Door::NativeDataFusion => "native",
        Door::Spark => "spark",
    };
    let form_token = match form {
        Form::Read => "read",
        Form::Insert => "insert",
    };
    let mut name = format!("{door_token}-{}-{}-{form_token}", op_slug(op), shape.id);
    if op == Op::DfUnionByName && OVERREFUSAL.contains(&shape.id) {
        name.push_str("-STRUCT-SETOP-UNIONBYNAME-OVERREFUSAL-1");
    }
    if op == Op::IntersectAll || op == Op::ExceptAll {
        name.push_str("-unmeasured-fail-closed");
    }
    name
}

pub struct Doors {
    spark: ReparkSession,
    spark_root: String,
    native: ReparkSession,
    native_root: String,
    _spark_warehouse: TempDir,
    _native_warehouse: TempDir,
}

fn dialect(door: Door) -> Arc<dyn SqlDialect> {
    match door {
        Door::Ansi => Arc::new(AnsiDialect),
        Door::NativeDataFusion => Arc::new(DataFusionDialect),
        Door::Spark => Arc::new(SparkDialect),
    }
}

pub fn session_of(doors: &Doors, door: Door) -> &ReparkSession {
    if door == Door::Spark {
        &doors.spark
    } else {
        &doors.native
    }
}

fn root_of(doors: &Doors, door: Door) -> &str {
    if door == Door::Spark {
        &doors.spark_root
    } else {
        &doors.native_root
    }
}

pub async fn attempt(
    session: &ReparkSession,
    door: Door,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let frame = session
        .sql_with(&dialect(door), sql)
        .await
        .map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, door: Door, sql: &str) -> Vec<RecordBatch> {
    attempt(session, door, sql)
        .await
        .unwrap_or_else(|error| panic!("{door:?} `{sql}`: {error}"))
}

pub async fn open_doors() -> Doors {
    let spark_warehouse = TempDir::new().expect("spark warehouse");
    let native_warehouse = TempDir::new().expect("native warehouse");
    let spark = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .build()
        .expect("spark session");
    let native = ReparkSession::builder()
        .with_sql_dialect(Arc::new(AnsiDialect))
        .build()
        .expect("native session");
    for (session, warehouse, door) in [
        (&spark, &spark_warehouse, Door::Spark),
        (&native, &native_warehouse, Door::Ansi),
    ] {
        session
            .register_memory_catalog("ice", warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        run(session, door, "CREATE SCHEMA ice.ns").await;
    }
    Doors {
        spark_root: spark_warehouse.path().to_str().expect("utf8").to_string(),
        native_root: native_warehouse.path().to_str().expect("utf8").to_string(),
        spark,
        native,
        _spark_warehouse: spark_warehouse,
        _native_warehouse: native_warehouse,
    }
}

fn int_width(door: Door) -> PrimitiveType {
    if door == Door::Spark {
        PrimitiveType::Int
    } else {
        PrimitiveType::Long
    }
}

fn struct_ab(int: &PrimitiveType, first: i32) -> Type {
    Type::Struct(StructType::new(vec![
        NestedField::optional(first, "a", Type::Primitive(int.clone())).into(),
        NestedField::optional(first + 1, "b", Type::Primitive(int.clone())).into(),
    ]))
}

fn table_schema(family: Family, door: Door) -> Schema {
    let int = int_width(door);
    let column = match family {
        Family::Struct2 => struct_ab(&int, 2),
        Family::Struct2Mixed => Type::Struct(StructType::new(vec![
            NestedField::optional(2, "a", Type::Primitive(int.clone())).into(),
            NestedField::optional(3, "b", Type::Primitive(PrimitiveType::String)).into(),
        ])),
        Family::Struct3 => Type::Struct(StructType::new(vec![
            NestedField::optional(2, "a", Type::Primitive(int.clone())).into(),
            NestedField::optional(3, "b", Type::Primitive(int.clone())).into(),
            NestedField::optional(4, "c", Type::Primitive(int.clone())).into(),
        ])),
        Family::Nested => Type::Struct(StructType::new(vec![
            NestedField::optional(2, "x", struct_ab(&int, 3)).into(),
        ])),
        Family::ListStruct => Type::List(ListType::new(
            NestedField::list_element(2, struct_ab(&int, 3), false).into(),
        )),
        Family::MapStruct => Type::Map(MapType::new(
            Arc::new(NestedField::map_key_element(
                2,
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::map_value_element(3, struct_ab(&int, 4), false)),
        )),
    };
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![NestedField::optional(1, "s", column).into()])
        .build()
        .expect("schema")
}

pub async fn create_shaped_table(doors: &Doors, door: Door, table: &str, shape: &Shape) {
    let session = session_of(doors, door);
    let creation = TableCreation::builder()
        .name(table.to_string())
        .location(format!("{}/ns/{table}", root_of(doors, door)))
        .schema(table_schema(shape.family, door))
        .format_version(FormatVersion::V2)
        .properties(HashMap::from([
            ("write.delete.mode".to_string(), "copy-on-write".to_string()),
            ("write.update.mode".to_string(), "copy-on-write".to_string()),
        ]))
        .build();
    let catalog: Arc<dyn Catalog> = session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone();
    catalog
        .create_table(&NamespaceIdent::new("ns".to_string()), creation)
        .await
        .expect("create through the catalog");
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("refresh the provider");
}

fn as_typed<T: 'static>(array: &dyn Array) -> Option<&T> {
    array.as_any().downcast_ref::<T>()
}

fn render_scalar(data_type: &DataType, array: &dyn Array, index: usize) -> Option<String> {
    match data_type {
        DataType::Int32 => as_typed::<Int32Array>(array).map(|v| v.value(index).to_string()),
        DataType::Int64 => as_typed::<Int64Array>(array).map(|v| v.value(index).to_string()),
        DataType::Utf8 => as_typed::<StringArray>(array).map(|v| format!("{:?}", v.value(index))),
        DataType::Utf8View => {
            as_typed::<StringViewArray>(array).map(|v| format!("{:?}", v.value(index)))
        }
        _ => None,
    }
}

fn render_list(
    values: &dyn Array,
    element: &DataType,
    start: usize,
    length: usize,
) -> Option<String> {
    let mut items = Vec::with_capacity(length);
    for offset in start..start + length {
        items.push(render_value(element, values, offset)?);
    }
    Some(format!("[{}]", items.join(",")))
}

fn list_bounds(offsets: &[i64], row: usize) -> Option<(usize, usize)> {
    let start = usize::try_from(*offsets.get(row)?).ok()?;
    let end = usize::try_from(*offsets.get(row + 1)?).ok()?;
    Some((start, end.saturating_sub(start)))
}

fn render_value(data_type: &DataType, array: &dyn Array, index: usize) -> Option<String> {
    if array.is_null(index) {
        return Some("null".to_string());
    }
    if let Some(scalar) = render_scalar(data_type, array, index) {
        return Some(scalar);
    }
    match data_type {
        DataType::Struct(fields) => {
            let structure = array.as_any().downcast_ref::<StructArray>()?;
            let mut items = Vec::with_capacity(fields.len());
            for (position, field) in fields.iter().enumerate() {
                let column = structure.column(position);
                items.push(render_value(field.data_type(), column.as_ref(), index)?);
            }
            Some(format!("[{}]", items.join(",")))
        }
        DataType::List(field) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            let offsets: Vec<i64> = list
                .value_offsets()
                .iter()
                .map(|offset| i64::from(*offset))
                .collect();
            let (start, length) = list_bounds(&offsets, index)?;
            render_list(list.values().as_ref(), field.data_type(), start, length)
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>()?;
            let offsets: Vec<i64> = map
                .value_offsets()
                .iter()
                .map(|offset| i64::from(*offset))
                .collect();
            let (start, length) = list_bounds(&offsets, index)?;
            let keys = map.keys();
            let values = map.values();
            let mut entries = Vec::with_capacity(length);
            for offset in start..start + length {
                let key = render_value(keys.data_type(), keys.as_ref(), offset)?;
                let value = render_value(values.data_type(), values.as_ref(), offset)?;
                entries.push(format!("[{key},{value}]"));
            }
            Some(format!("[{}]", entries.join(",")))
        }
        _ => None,
    }
}

pub fn render_rows(batches: &[RecordBatch]) -> String {
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        for index in 0..batch.num_rows() {
            rows.push(
                render_value(column.data_type(), column.as_ref(), index)
                    .unwrap_or_else(|| format!("unrendered {:?}", column.data_type())),
            );
        }
    }
    rows.sort();
    rows.join("|")
}

pub struct State {
    pub rows: String,
    pub snapshot: (usize, Option<i64>),
}

pub async fn observe(doors: &Doors, door: Door, table: &str) -> State {
    let session = session_of(doors, door);
    let read_door = if door == Door::Spark {
        Door::Spark
    } else {
        Door::NativeDataFusion
    };
    let batches = run(session, read_door, &format!("SELECT s FROM ice.ns.{table}")).await;
    let rows = render_rows(&batches);
    let catalog: Arc<dyn Catalog> = session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone();
    let loaded = catalog
        .load_table(&TableIdent::from_strs(["ns", table]).expect("ident"))
        .await
        .expect("load");
    let metadata = loaded.metadata();
    State {
        rows,
        snapshot: (metadata.snapshots().count(), metadata.current_snapshot_id()),
    }
}

fn refusal_parts(error: &str) -> Option<(String, String)> {
    let marker = "Struct field names differ at ";
    let start = error.find(marker)? + marker.len();
    let rest = error[start..]
        .find(';')
        .map(|end| &error[start..start + end])?;
    let (path, lists) = rest.split_once(": ")?;
    Some((path.to_string(), lists.to_string()))
}

pub fn check_refusal_text(
    label: &str,
    failures: &mut Vec<String>,
    error: &str,
    word: &str,
    path: &str,
    right: &str,
    left: &str,
) {
    if !error.contains("[INCOMPATIBLE_COLUMN_TYPE]") {
        failures.push(format!("{label}: class missing in {error}"));
    }
    if !error.contains(&format!("{word} can only be performed")) {
        failures.push(format!("{label}: operation {word} missing in {error}"));
    }
    match refusal_parts(error) {
        Some((found_path, found_lists)) => {
            if found_path != path {
                failures.push(format!("{label}: path {found_path} != {path}"));
            }
            let expected = format!("[{right}] versus [{left}]");
            if found_lists != expected {
                failures.push(format!("{label}: lists {found_lists} != {expected}"));
            }
        }
        None => failures.push(format!("{label}: refusal shape missing in {error}")),
    }
}

fn expected_rows(shape: &Shape, op: Op) -> &'static str {
    let rows = match shape.expect {
        Expect::Answer(rows) | Expect::MapAnswer(rows) => rows,
        Expect::Refuse(_) | Expect::MapRefuse(_) => panic!("{} answers nothing", shape.id),
    };
    match op {
        Op::UnionAll | Op::Union | Op::UnionDistinct | Op::DfUnion | Op::DfUnionByName => rows.0,
        Op::Intersect | Op::IntersectAll => rows.1,
        Op::Except | Op::ExceptAll => rows.2,
    }
}

fn is_answer_shape(shape: &Shape, op: Op) -> bool {
    match shape.expect {
        Expect::Answer(_) => true,
        Expect::MapAnswer(_) => matches!(
            op,
            Op::UnionAll | Op::DfUnion | Op::DfUnionByName | Op::IntersectAll | Op::ExceptAll
        ),
        Expect::Refuse(_) | Expect::MapRefuse(_) => false,
    }
}

fn refusal_expectation(shape: &Shape) -> (&'static str, &'static str, &'static str) {
    match shape.expect {
        Expect::Refuse(found) | Expect::MapRefuse(found) => found,
        Expect::Answer(_) | Expect::MapAnswer(_) => panic!("{} refuses nothing", shape.id),
    }
}

fn table_name(shape: &Shape, op: Op) -> String {
    let shape = shape.id.replace('-', "_");
    let op = op_slug(op).replace('-', "_");
    format!("t_{shape}_{op}_insert")
}

fn select_statement(shape: &Shape, door: Door, op: Op) -> String {
    let (left, right_union, right_set) = operands(door, shape);
    let set_op = matches!(
        op,
        Op::Intersect | Op::Except | Op::IntersectAll | Op::ExceptAll
    );
    let right = if set_op { right_set } else { right_union };
    format!("SELECT {left} AS s {} SELECT {right} AS s", op_token(op))
}

async fn check_sql_cell(
    doors: &Doors,
    door: Door,
    op: Op,
    shape: &Shape,
    form: Form,
    failures: &mut Vec<String>,
) {
    let label = row_name(door, op, shape, form);
    if form == Form::Read {
        let statement = select_statement(shape, door, op);
        let outcome = attempt(session_of(doors, door), door, &statement).await;
        check_read_outcome(&label, failures, outcome, op, shape);
        return;
    }
    let table = table_name(shape, op);
    create_shaped_table(doors, door, &table, shape).await;
    let before = observe(doors, door, &table).await;
    if !before.rows.is_empty() {
        failures.push(format!("{label}: table not empty {}", before.rows));
        return;
    }
    let statement = format!(
        "INSERT INTO ice.ns.{table} {}",
        select_statement(shape, door, op)
    );
    let outcome = attempt(session_of(doors, door), door, &statement).await;
    check_insert_outcome(
        doors, door, &table, &label, failures, outcome, op, shape, &before,
    )
    .await;
}

fn check_read_outcome(
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    op: Op,
    shape: &Shape,
) {
    let answer_shape = is_answer_shape(shape, op);
    match outcome {
        Ok(batches) => {
            if !answer_shape {
                failures.push(format!("{label}: answered {}", render_rows(&batches)));
                return;
            }
            let expected = expected_rows(shape, op);
            let rows = render_rows(&batches);
            if rows != expected {
                failures.push(format!("{label}: rows {rows} != {expected}"));
            }
        }
        Err(error) => {
            if answer_shape {
                failures.push(format!("{label}: refused {error}"));
                return;
            }
            check_refusal_cell(label, failures, &error, op, shape);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn check_insert_outcome(
    doors: &Doors,
    door: Door,
    table: &str,
    label: &str,
    failures: &mut Vec<String>,
    outcome: Result<Vec<RecordBatch>, String>,
    op: Op,
    shape: &Shape,
    before: &State,
) {
    let answer_shape = is_answer_shape(shape, op);
    match outcome {
        Ok(_) => {
            if !answer_shape {
                let after = observe(doors, door, table).await;
                failures.push(format!("{label}: answered {}", after.rows));
                return;
            }
            let expected = expected_rows(shape, op);
            let after = observe(doors, door, table).await;
            if after.rows != expected {
                failures.push(format!("{label}: rows {} != {expected}", after.rows));
            }
            if after.snapshot == before.snapshot {
                failures.push(format!("{label}: snapshot unchanged"));
            }
        }
        Err(error) => {
            let after = observe(doors, door, table).await;
            if after.rows != before.rows {
                failures.push(format!("{label}: rows changed {}", after.rows));
            }
            if after.snapshot != before.snapshot {
                failures.push(format!("{label}: snapshot changed"));
            }
            if answer_shape {
                failures.push(format!("{label}: refused {error}"));
                return;
            }
            check_refusal_cell(label, failures, &error, op, shape);
        }
    }
}

fn check_refusal_cell(label: &str, failures: &mut Vec<String>, error: &str, op: Op, shape: &Shape) {
    if matches!(shape.expect, Expect::MapAnswer(_)) {
        if error.contains("Struct field names differ") {
            failures.push(format!("{label}: fired on identical names {error}"));
        }
    } else if matches!(shape.expect, Expect::Refuse(_))
        || matches!(op, Op::UnionAll | Op::DfUnion | Op::DfUnionByName)
    {
        let (path, right, left) = refusal_expectation(shape);
        check_refusal_text(label, failures, error, op_word(op), path, right, left);
    }
}

const SQL_OPS: [Op; 7] = [
    Op::UnionAll,
    Op::Union,
    Op::UnionDistinct,
    Op::Intersect,
    Op::Except,
    Op::IntersectAll,
    Op::ExceptAll,
];

pub async fn check_sql_door(door: Door) {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    for op in SQL_OPS {
        for shape in &SHAPES {
            for form in [Form::Read, Form::Insert] {
                check_sql_cell(&doors, door, op, shape, form, &mut failures).await;
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn operand_frame(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
    expr: &str,
) -> Result<datafusion::prelude::DataFrame, String> {
    session
        .sql_with(dialect, &format!("SELECT {expr} AS s"))
        .await
        .map_err(|error| error.to_string())
}

async fn build_df_union(
    doors: &Doors,
    op: Op,
    shape: &Shape,
) -> Result<datafusion::prelude::DataFrame, String> {
    let session = &doors.spark;
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let (left_sql, right_sql, _) = operands(Door::Spark, shape);
    let left = operand_frame(session, &dialect, left_sql).await?;
    let right = operand_frame(session, &dialect, right_sql).await?;
    if op == Op::DfUnion {
        left.union(right).map_err(|error| error.to_string())
    } else {
        left.union_by_name(right).map_err(|error| error.to_string())
    }
}

async fn df_insert_outcome(
    doors: &Doors,
    op: Op,
    shape: &Shape,
    table: &str,
) -> Result<Vec<RecordBatch>, String> {
    let combined = build_df_union(doors, op, shape).await?;
    doors
        .spark
        .create_or_replace_temp_view_from("df_union_view", &combined)
        .map_err(|error| error.to_string())?;
    attempt(
        &doors.spark,
        Door::Spark,
        &format!("INSERT INTO ice.ns.{table} SELECT * FROM df_union_view"),
    )
    .await
}

pub async fn check_df_cell(
    doors: &Doors,
    op: Op,
    shape: &Shape,
    form: Form,
    failures: &mut Vec<String>,
) {
    let label = row_name(Door::Spark, op, shape, form);
    if form == Form::Read {
        let outcome = match build_df_union(doors, op, shape).await {
            Ok(combined) => combined.collect().await.map_err(|error| error.to_string()),
            Err(error) => Err(error),
        };
        check_read_outcome(&label, failures, outcome, op, shape);
        return;
    }
    let table = table_name(shape, op);
    create_shaped_table(doors, Door::Spark, &table, shape).await;
    let before = observe(doors, Door::Spark, &table).await;
    if !before.rows.is_empty() {
        failures.push(format!("{label}: table not empty {}", before.rows));
        return;
    }
    let outcome = df_insert_outcome(doors, op, shape, &table).await;
    check_insert_outcome(
        doors,
        Door::Spark,
        &table,
        &label,
        failures,
        outcome,
        op,
        shape,
        &before,
    )
    .await;
}

pub fn shape_by_id(id: &str) -> &'static Shape {
    SHAPES
        .iter()
        .find(|shape| shape.id == id)
        .expect("delta shape")
}
