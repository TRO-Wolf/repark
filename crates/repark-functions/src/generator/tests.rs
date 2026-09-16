use super::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use datafusion::arrow::datatypes::{Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

fn columns() -> (ListArray, ListArray, ListArray) {
    let ints = Int32Array::from(vec![10, 20]);
    let arr_i = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::from_lengths([2_usize, 0, 0]),
        Arc::new(ints),
        Some(datafusion::arrow::buffer::NullBuffer::from(vec![
            true, false, true,
        ])),
    );
    let pair = Fields::from(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    let structs = StructArray::new(
        pair.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["p", "q"])),
        ],
        None,
    );
    let arr_s = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(pair), true)),
        OffsetBuffer::from_lengths([2_usize, 0, 0]),
        Arc::new(structs),
        Some(datafusion::arrow::buffer::NullBuffer::from(vec![
            true, false, true,
        ])),
    );
    let nn_pair = Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]);
    let nn_structs = StructArray::new(
        nn_pair.clone(),
        vec![
            Arc::new(Int32Array::from(vec![7, 0])),
            Arc::new(StringArray::from(vec!["s", ""])),
        ],
        Some(datafusion::arrow::buffer::NullBuffer::from(vec![
            true, false,
        ])),
    );
    let arr_sn = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(nn_pair), true)),
        OffsetBuffer::from_lengths([2_usize, 0, 0]),
        Arc::new(nn_structs),
        Some(datafusion::arrow::buffer::NullBuffer::from(vec![
            true, false, true,
        ])),
    );
    (arr_i, arr_s, arr_sn)
}

fn frame() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    ctx.register_udf(crate::collection::create_map_udf().as_ref().clone());
    ctx.add_analyzer_rule(Arc::new(GeneratorRewrite));
    let (arr_i, arr_s, arr_sn) = columns();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "arr_i",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "arr_s",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
        Field::new(
            "arr_sn",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Utf8, false),
                ])),
                true,
            ))),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arr_i),
            Arc::new(arr_s),
            Arc::new(arr_sn),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

fn run(sql: &str) -> Result<(Vec<String>, Vec<String>)> {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.sql(sql).await?;
        let batches = frame.collect().await?;
        let names: Vec<String> = batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut rendered = Vec::new();
                for column in batch.columns() {
                    let value = ScalarValue::try_from_array(column.as_ref(), row)?;
                    rendered.push(format!("{value:?}"));
                }
                rows.push(rendered.join("|"));
            }
        }
        Ok((names, rows))
    })
}

#[test]
fn posexplode_array_answers_pos_and_col() {
    let (names, rows) = run("SELECT id, posexplode(arr_i) FROM t").unwrap();
    assert_eq!(names, ["id", "pos", "col"]);
    assert_eq!(
        rows,
        ["Int32(1)|Int32(0)|Int32(10)", "Int32(1)|Int32(1)|Int32(20)"]
    );
}

#[test]
fn posexplode_keeps_select_list_position() {
    let (names, rows) = run("SELECT posexplode(arr_i), id FROM t").unwrap();
    assert_eq!(names, ["pos", "col", "id"]);
    assert_eq!(
        rows,
        ["Int32(0)|Int32(10)|Int32(1)", "Int32(1)|Int32(20)|Int32(1)"]
    );
}

#[test]
fn posexplode_outer_keeps_null_and_empty_rows() {
    let (names, rows) = run("SELECT id, posexplode_outer(arr_i) FROM t").unwrap();
    assert_eq!(names, ["id", "pos", "col"]);
    assert_eq!(
        rows,
        [
            "Int32(1)|Int32(0)|Int32(10)",
            "Int32(1)|Int32(1)|Int32(20)",
            "Int32(2)|Int32(NULL)|Int32(NULL)",
            "Int32(3)|Int32(NULL)|Int32(NULL)"
        ]
    );
}

#[test]
fn posexplode_map_answers_pos_key_value() {
    let (names, rows) = run("SELECT id, posexplode(create_map('a', id)) FROM t").unwrap();
    assert_eq!(names, ["id", "pos", "key", "value"]);
    assert_eq!(
        rows,
        [
            "Int32(1)|Int32(0)|Utf8(\"a\")|Int32(1)",
            "Int32(2)|Int32(0)|Utf8(\"a\")|Int32(2)",
            "Int32(3)|Int32(0)|Utf8(\"a\")|Int32(3)"
        ]
    );
}

#[test]
fn inline_projects_struct_fields() {
    let (names, rows) = run("SELECT id, inline(arr_s) FROM t").unwrap();
    assert_eq!(names, ["id", "x", "y"]);
    assert_eq!(
        rows,
        [
            "Int32(1)|Int32(1)|Utf8(\"p\")",
            "Int32(1)|Int32(2)|Utf8(\"q\")"
        ]
    );
}

#[test]
fn inline_keeps_null_struct_element_as_null_fields() {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.sql("SELECT inline(arr_sn) FROM t").await.unwrap();
        let batches = frame.collect().await.unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        let names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, ["x", "y"]);
        assert!(schema.fields().iter().all(|field| field.is_nullable()));
        let mut rows = Vec::new();
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::new();
            for column in batch.columns() {
                let value = ScalarValue::try_from_array(column.as_ref(), row).unwrap();
                rendered.push(format!("{value:?}"));
            }
            rows.push(rendered.join("|"));
        }
        assert_eq!(rows, ["Int32(7)|Utf8(\"s\")", "Int32(NULL)|Utf8(NULL)"]);
    });
}

#[test]
fn inline_outer_keeps_null_and_empty_rows() {
    let (names, rows) = run("SELECT id, inline_outer(arr_s) FROM t").unwrap();
    assert_eq!(names, ["id", "x", "y"]);
    assert_eq!(
        rows,
        [
            "Int32(1)|Int32(1)|Utf8(\"p\")",
            "Int32(1)|Int32(2)|Utf8(\"q\")",
            "Int32(2)|Int32(NULL)|Utf8(NULL)",
            "Int32(3)|Int32(NULL)|Utf8(NULL)"
        ]
    );
}

#[test]
fn sql_as_alias_on_generator_refuses() {
    let error = run("SELECT posexplode(arr_i) AS foo FROM t").unwrap_err();
    assert!(
        format!("{error}").contains("COLUMN_ALIASES_MISMATCH"),
        "{error}"
    );
}

#[test]
fn name_restored_alias_is_not_a_user_alias() {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.table("t").await.unwrap();
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf("posexplode"),
            vec![Expr::Column(Column::new_unqualified("arr_i"))],
        ));
        let frame = frame.select(vec![call.alias("posexplode(arr_i)")]).unwrap();
        let batches = frame.collect().await.unwrap();
        let names: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, ["pos", "col"]);
    });
}

#[test]
fn generator_alias_marker_renames_outputs() {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.table("t").await.unwrap();
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf("posexplode"),
            vec![Expr::Column(Column::new_unqualified("arr_i"))],
        ));
        let marked = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf(GENERATOR_ALIAS_UDF),
            vec![call, lit("p"), lit("v")],
        ));
        let frame = frame.select(vec![marked]).unwrap();
        let batches = frame.collect().await.unwrap();
        let names: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(names, ["p", "v"]);
    });
}

#[test]
fn nested_generator_is_refused() {
    let error = run("SELECT posexplode(arr_i) IS NULL FROM t").unwrap_err();
    assert!(
        format!("{error}").contains("UNSUPPORTED_GENERATOR"),
        "{error}"
    );
}

#[test]
fn two_generators_are_refused() {
    let error = run("SELECT posexplode(arr_i), inline(arr_s) FROM t").unwrap_err();
    assert!(format!("{error}").contains("Only one generator"), "{error}");
}

#[test]
fn generator_over_aggregate_is_refused() {
    let error = run("SELECT posexplode(collect_list(arr_i)) FROM t").unwrap_err();
    assert!(format!("{error}").contains("MISSING_GROUP_BY"), "{error}");
}

#[test]
fn stack_sibling_is_refused() {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.table("t").await.unwrap();
        let stack_call = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf("stack"),
            vec![lit(2_i64), lit("a"), lit("b")],
        ));
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf("inline"),
            vec![Expr::Column(Column::new_unqualified("arr_s"))],
        ));
        let frame = frame.select(vec![stack_call, call]).unwrap();
        let error = frame.collect().await.unwrap_err();
        assert!(format!("{error}").contains("Only one generator"), "{error}");
    });
}

#[test]
fn explode_temp_sibling_is_refused() {
    let ctx = frame();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let frame = ctx.table("t").await.unwrap();
        let explode_temp = Expr::Column(Column::new_unqualified("arr_i"))
            .alias("__repark_arr_0123456789abcdef0123456789abcdef");
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            generator_udf("posexplode"),
            vec![Expr::Column(Column::new_unqualified("arr_i"))],
        ));
        let frame = frame.select(vec![explode_temp, call]).unwrap();
        let error = frame.collect().await.unwrap_err();
        assert!(format!("{error}").contains("Only one generator"), "{error}");
    });
}

#[test]
fn posexplode_of_a_scalar_is_refused() {
    let error = run("SELECT posexplode(id) FROM t").unwrap_err();
    assert!(
        format!("{error}").contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        "{error}"
    );
}

#[test]
fn ordinality_packs_positions_for_a_sliced_list() {
    use datafusion::arrow::array::{Int64Builder, ListBuilder};
    let mut builder = ListBuilder::new(Int64Builder::new());
    for row in [vec![10_i64, 11, 12], vec![20, 21], vec![30, 31, 32, 33]] {
        for value in row {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    let full = builder.finish();
    let sliced = full.slice(1, 2);
    let positions = super::ordinality(&sliced);
    assert_eq!(positions.len(), 2);
    assert_eq!(positions.offsets().first().copied(), Some(0));
    let first: Vec<i32> = positions
        .value(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(first, vec![0, 1]);
    let second: Vec<i32> = positions
        .value(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(second, vec![0, 1, 2, 3]);
}
