use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, JoinType};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col, lit};

use crate::ReparkSession;
use crate::frame_names::{
    FrameNode, HIDDEN_PREFIX, NameRule::IgnoreCase, attribute_ids, expose_hidden_keys,
    hidden_names_in, hidden_names_in_text, join_on_named_keys, join_output_sources, output_columns,
    rebind_key_name, shown_columns, spark_key_type, stamp, using_hidden_keys,
};

async fn side(session: &ReparkSession, alias: &str, rows: &str, second: &str) -> DataFrame {
    let frame = session
        .sql(&format!(
            "SELECT * FROM (VALUES {rows}) AS {alias}(id, {second})"
        ))
        .await
        .unwrap();
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
}

async fn sides(session: &ReparkSession) -> (DataFrame, DataFrame) {
    let left = side(session, "jl", "(1, 'a'), (2, 'b'), (3, 'c')", "s").await;
    let right = side(session, "jr", "(2, 'x'), (3, 'y'), (4, 'z')", "t").await;
    (left, right)
}

fn joined(left: DataFrame, right: DataFrame, how: JoinType) -> DataFrame {
    let left_node = FrameNode::root(left.schema()).unwrap();
    let right_node = FrameNode::root(right.schema()).unwrap();
    join_on_named_keys(
        left,
        right,
        &["id".to_string()],
        how,
        IgnoreCase,
        left_node,
        right_node,
    )
    .unwrap()
    .0
}

async fn first_column(frame: DataFrame) -> Vec<Option<i64>> {
    let mut values = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        for row in 0..column.len() {
            values.push(column.is_valid(row).then(|| column.value(row)));
        }
    }
    values.sort_unstable();
    values
}

fn hidden_alias(frame: &DataFrame, right: bool) -> String {
    using_hidden_keys(frame.logical_plan())
        .into_iter()
        .find(|key| key.right == right)
        .unwrap()
        .alias
}

async fn side_keys(frame: &DataFrame, right: bool) -> Vec<Option<i64>> {
    let alias = hidden_alias(frame, right);
    let wide = expose_hidden_keys(frame.logical_plan(), std::slice::from_ref(&alias))
        .unwrap()
        .unwrap();
    let (state, _) = frame.clone().into_parts();
    let picked = DataFrame::new(state, wide)
        .select(vec![col(Column::new_unqualified(alias))])
        .unwrap();
    first_column(picked).await
}

fn names(frame: &DataFrame) -> Vec<String> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

#[tokio::test]
async fn shown_key_is_left_on_left_right_on_right_and_coalesced_on_full() {
    let session = ReparkSession::new().unwrap();
    for (how, expected) in [
        (JoinType::Inner, vec![Some(2), Some(3)]),
        (JoinType::Left, vec![Some(1), Some(2), Some(3)]),
        (JoinType::Right, vec![Some(2), Some(3), Some(4)]),
        (JoinType::Full, vec![Some(1), Some(2), Some(3), Some(4)]),
    ] {
        let (left, right) = sides(&session).await;
        let frame = joined(left, right, how);
        assert_eq!(names(&frame), ["id", "s", "t"], "{how:?}");
        assert_eq!(first_column(frame).await, expected, "{how:?}");
    }
}

#[tokio::test]
async fn shown_key_attribute_id_follows_the_join_type() {
    let session = ReparkSession::new().unwrap();
    for how in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
    ] {
        let (left, right) = sides(&session).await;
        let left_id = attribute_ids(left.schema())[0].clone();
        let right_id = attribute_ids(right.schema())[0].clone();
        let frame = joined(left, right, how);
        let shown = attribute_ids(frame.schema())[0].clone();
        assert!(shown.is_some());
        match how {
            JoinType::Right => assert_eq!(shown, right_id),
            JoinType::Full => assert!(shown != left_id && shown != right_id),
            _ => assert_eq!(shown, left_id),
        }
    }
}

#[tokio::test]
async fn hidden_keys_are_join_columns_never_output_fields() {
    let session = ReparkSession::new().unwrap();
    for (how, hidden_right, hidden_left) in [
        (JoinType::Inner, true, false),
        (JoinType::Left, true, false),
        (JoinType::Right, false, true),
        (JoinType::Full, true, true),
        (JoinType::LeftSemi, false, false),
        (JoinType::LeftAnti, false, false),
    ] {
        let (left, right) = sides(&session).await;
        let frame = joined(left, right, how);
        let hidden = using_hidden_keys(frame.logical_plan());
        assert_eq!(hidden.iter().any(|key| key.right), hidden_right, "{how:?}");
        assert_eq!(hidden.iter().any(|key| !key.right), hidden_left, "{how:?}");
        assert!(
            hidden
                .iter()
                .all(|key| key.alias.starts_with(HIDDEN_PREFIX))
        );
        assert!(
            hidden
                .iter()
                .all(|key| key.display == "id" && key.side_position == 0)
        );
        assert!(
            names(&frame)
                .iter()
                .all(|name| !name.starts_with(HIDDEN_PREFIX)),
            "{how:?}"
        );
    }
}

#[tokio::test]
async fn exposed_side_keys_answer_per_side_values() {
    let session = ReparkSession::new().unwrap();
    let (left, right) = sides(&session).await;
    let full = joined(left, right, JoinType::Full);
    assert_eq!(
        side_keys(&full, true).await,
        vec![None, Some(2), Some(3), Some(4)]
    );
    assert_eq!(
        side_keys(&full, false).await,
        vec![None, Some(1), Some(2), Some(3)]
    );
    let (left, right) = sides(&session).await;
    let left_join = joined(left, right, JoinType::Left);
    assert_eq!(
        side_keys(&left_join, true).await,
        vec![None, Some(2), Some(3)]
    );
    let (left, right) = sides(&session).await;
    let right_join = joined(left, right, JoinType::Right);
    assert_eq!(
        side_keys(&right_join, false).await,
        vec![None, Some(2), Some(3)]
    );
}

#[tokio::test]
async fn exposure_passes_filter_sort_and_limit_and_stops_at_a_narrowing_select() {
    let session = ReparkSession::new().unwrap();
    let (left, right) = sides(&session).await;
    let frame = joined(left, right, JoinType::Full);
    let alias = hidden_alias(&frame, true);
    let wanted = std::slice::from_ref(&alias);
    let shaped = frame
        .clone()
        .filter(col("s").is_not_null())
        .unwrap()
        .sort(vec![col("t").sort(true, true)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap();
    let wide = expose_hidden_keys(shaped.logical_plan(), wanted)
        .unwrap()
        .unwrap();
    assert_eq!(wide.schema().fields().len(), 4);
    assert_eq!(wide.schema().field(3).name(), &alias);
    let narrowed = frame.clone().select(vec![col("s")]).unwrap();
    assert!(
        expose_hidden_keys(narrowed.logical_plan(), wanted)
            .unwrap()
            .is_none()
    );
    let renamed = frame.clone().alias("x").unwrap();
    assert!(
        expose_hidden_keys(renamed.logical_plan(), wanted)
            .unwrap()
            .is_none()
    );
    assert!(
        expose_hidden_keys(
            frame.logical_plan(),
            &["__repark_using__none__id".to_string()]
        )
        .unwrap()
        .is_none()
    );
    assert!(
        expose_hidden_keys(frame.logical_plan(), &[])
            .unwrap()
            .is_none()
    );
    assert_eq!(output_columns(frame.schema()).len(), 3);
}

#[tokio::test]
async fn chained_full_join_matches_on_the_coalesced_key() {
    let session = ReparkSession::new().unwrap();
    let (left, right) = sides(&session).await;
    let first = joined(left, right, JoinType::Full);
    let (state, plan) = first.into_parts();
    let first = DataFrame::new(state, stamp(plan).unwrap());
    let other = side(&session, "jq", "(2, 'p'), (3, 'q'), (4, 'r')", "u").await;
    let chained = joined(first, other, JoinType::Full);
    assert_eq!(names(&chained), ["id", "s", "t", "u"]);
    assert_eq!(
        first_column(chained).await,
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
}

#[tokio::test]
async fn mixed_type_keys_show_the_right_key_on_right_and_the_common_type_on_full() {
    let session = ReparkSession::new().unwrap();
    for (how, shown) in [
        (JoinType::Right, DataType::Utf8),
        (JoinType::Full, DataType::Int64),
        (JoinType::Left, DataType::Int64),
    ] {
        let left = side(&session, "jl", "(1, 'a'), (2, 'b')", "s").await;
        let right = side(&session, "jr", "('2', 'x'), ('3', 'y')", "t").await;
        let frame = joined(left, right, how);
        assert_eq!(frame.schema().field(0).data_type(), &shown, "{how:?}");
    }
    let left = side(&session, "jl", "(1, 'a'), (2, 'b')", "s").await;
    let right = side(&session, "jr", "(2.5, 'x'), (3.5, 'y')", "t").await;
    let frame = joined(left, right, JoinType::Full);
    assert_eq!(frame.schema().field(0).data_type(), &DataType::Float64);
    let mut keys = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .clone();
        keys.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    keys.sort();
    assert_eq!(keys, ["1", "2", "2.5", "3.5"]);
}

#[test]
fn spark_key_type_follows_the_measured_pairs() {
    let decimal = |precision, scale| DataType::Decimal128(precision, scale);
    let stamp = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
    for (left, right, wide) in [
        (DataType::Int32, DataType::Int64, Some(DataType::Int64)),
        (DataType::Int32, DataType::Int16, Some(DataType::Int32)),
        (DataType::Int32, decimal(10, 2), Some(decimal(12, 2))),
        (DataType::Int64, decimal(10, 2), Some(decimal(22, 2))),
        (DataType::Int32, DataType::Float64, Some(DataType::Float64)),
        (DataType::Int32, DataType::Float32, Some(DataType::Float64)),
        (
            DataType::Float32,
            DataType::Float64,
            Some(DataType::Float64),
        ),
        (decimal(10, 2), DataType::Float64, Some(DataType::Float64)),
        (DataType::Int32, DataType::Utf8, Some(DataType::Int64)),
        (DataType::Float64, DataType::Utf8, Some(DataType::Float64)),
        (DataType::Date32, stamp.clone(), Some(stamp.clone())),
        (DataType::Date32, DataType::Utf8, None),
        (stamp.clone(), DataType::Utf8, None),
        (DataType::Boolean, DataType::Int32, None),
        (decimal(38, 0), decimal(38, 10), None),
    ] {
        assert_eq!(spark_key_type(&left, &right), wide, "{left:?} {right:?}");
        assert_eq!(spark_key_type(&right, &left), wide, "{right:?} {left:?}");
    }
}

#[tokio::test]
async fn a_second_side_key_is_reachable_after_a_narrowed_filter() {
    let session = ReparkSession::new().unwrap();
    let (left, right) = sides(&session).await;
    let frame = joined(left, right, JoinType::Full);
    let right_alias = hidden_alias(&frame, true);
    let left_alias = hidden_alias(&frame, false);
    let wide = expose_hidden_keys(frame.logical_plan(), std::slice::from_ref(&right_alias))
        .unwrap()
        .unwrap();
    let (state, _) = frame.clone().into_parts();
    let filtered = DataFrame::new(state, wide)
        .filter(col(Column::new_unqualified(right_alias.clone())).gt(lit(2)))
        .unwrap();
    let narrowed = filtered
        .clone()
        .select(shown_columns(filtered.schema()))
        .unwrap();
    assert_eq!(names(&narrowed), ["id", "s", "t"]);
    for alias in [&right_alias, &left_alias] {
        let again = expose_hidden_keys(narrowed.logical_plan(), std::slice::from_ref(alias))
            .unwrap()
            .unwrap();
        assert_eq!(again.schema().fields().len(), 4);
        assert_eq!(again.schema().field(3).name(), alias);
    }
    let both = expose_hidden_keys(
        narrowed.logical_plan(),
        &[left_alias.clone(), right_alias.clone()],
    )
    .unwrap()
    .unwrap();
    let (state, _) = frame.into_parts();
    let picked = DataFrame::new(state, both)
        .select(vec![col(Column::new_unqualified(left_alias))])
        .unwrap();
    assert_eq!(first_column(picked).await, vec![None, Some(3)]);
}

#[tokio::test]
async fn a_user_column_named_like_the_alias_keeps_the_key_unexposed() {
    let session = ReparkSession::new().unwrap();
    let left = side(
        &session,
        "jl",
        "(1, 'a'), (2, 'b')",
        "__repark_using__jr__id",
    )
    .await;
    let right = side(&session, "jr", "(2, 'x'), (3, 'y')", "t").await;
    let frame = joined(left, right, JoinType::Full);
    assert_eq!(names(&frame), ["id", "__repark_using__jr__id", "t"]);
    let hidden = using_hidden_keys(frame.logical_plan());
    assert!(hidden.is_empty());
    assert!(
        expose_hidden_keys(
            frame.logical_plan(),
            &["__repark_using__jr__id".to_string()]
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(first_column(frame).await, vec![None, Some(1), Some(2)]);
}

#[tokio::test]
async fn only_an_inner_join_pairs_the_shown_key_with_both_sides() {
    let session = ReparkSession::new().unwrap();
    for (how, expected) in [
        (JoinType::Inner, vec![(false, 0), (true, 0)]),
        (JoinType::Left, vec![(false, 0)]),
        (JoinType::Right, vec![(true, 0)]),
        (JoinType::Full, Vec::new()),
    ] {
        let (left, right) = sides(&session).await;
        let frame = joined(left, right, how);
        let sources = join_output_sources(frame.logical_plan());
        assert_eq!(sources[0], expected, "{how:?}");
        assert_eq!(sources[1], vec![(false, 1)]);
        assert_eq!(sources[2], vec![(true, 1)]);
    }
}

#[tokio::test]
async fn hidden_names_are_read_from_expressions_and_text() {
    let session = ReparkSession::new().unwrap();
    let (left, right) = sides(&session).await;
    let frame = joined(left, right, JoinType::Full);
    let alias = hidden_alias(&frame, true);
    let reference = col(Column::new_unqualified(alias.clone())).gt(lit(1));
    assert_eq!(
        hidden_names_in([&reference, &col("s")]),
        vec![alias.clone()]
    );
    assert!(hidden_names_in([&col("id")]).is_empty());
    assert_eq!(
        hidden_names_in_text(&format!("`{alias}` > 1 AND {alias} < 9")),
        vec![alias.clone()]
    );
    assert!(hidden_names_in_text("id > 1").is_empty());
    assert_eq!(
        hidden_names_in_text("x.__repark_using__other > 1"),
        ["__repark_using__other"]
    );
}

#[test]
fn rebind_swaps_only_the_named_key() {
    let bare = col("id").gt(lit(1)).and(col("s").is_not_null());
    let rebound = rebind_key_name(bare.clone(), None, "ID", "hidden", IgnoreCase, false).unwrap();
    assert_eq!(
        rebound,
        col("hidden").gt(lit(1)).and(col("s").is_not_null())
    );
    let qualified = Expr::Column(Column::new(Some("r"), "id")) + lit(1);
    let named = rebind_key_name(
        qualified.clone(),
        Some("R"),
        "id",
        "hidden",
        IgnoreCase,
        true,
    )
    .unwrap();
    assert_eq!(named, (col("hidden") + lit(1)).alias("r.id + Int32(1)"));
    let other = rebind_key_name(
        qualified.clone(),
        Some("l"),
        "id",
        "hidden",
        IgnoreCase,
        true,
    )
    .unwrap();
    assert_eq!(other, qualified);
    let dotted = col(Column::new_unqualified("r.id"));
    assert_eq!(
        rebind_key_name(dotted, Some("r"), "id", "hidden", IgnoreCase, false).unwrap(),
        col("hidden")
    );
    let kept = rebind_key_name(bare.clone(), None, "t", "hidden", IgnoreCase, true).unwrap();
    assert_eq!(kept, bare);
}
