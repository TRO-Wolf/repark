mod setop_reorder_common;

use setop_reorder_common::{Door, Form, Op, SHAPES, check_df_cell, check_sql_door, open_doors};

#[tokio::test(flavor = "multi_thread")]
async fn the_ansi_door_refuses_reordered_struct_set_operations() {
    check_sql_door(Door::Ansi).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_native_door_refuses_reordered_struct_set_operations() {
    check_sql_door(Door::NativeDataFusion).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_spark_door_refuses_reordered_struct_set_operations() {
    check_sql_door(Door::Spark).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dataframe_union_and_union_by_name_refuse_reordered_structs() {
    let doors = open_doors().await;
    let mut failures = Vec::new();
    for op in [Op::DfUnion, Op::DfUnionByName] {
        for shape in &SHAPES {
            for form in [Form::Read, Form::Insert] {
                check_df_cell(&doors, op, shape, form, &mut failures).await;
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
