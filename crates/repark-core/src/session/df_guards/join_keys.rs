use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::common::{Column, DFSchema, Result, plan_err};
use datafusion::logical_expr::utils::can_hash;
use datafusion::logical_expr::{Expr, cast};

pub const UNHASHABLE_JOIN_KEY: &str = "REPARK_UNHASHABLE_JOIN_KEY";

pub type JoinKeyExprs = (Vec<Expr>, Vec<Expr>);

#[allow(clippy::missing_errors_doc)]
pub fn hashable_join_keys(
    left: &DFSchema,
    right: &DFSchema,
    left_keys: &[Column],
    right_keys: &[Column],
) -> Result<Option<JoinKeyExprs>> {
    let (Some(left_types), Some(right_types)) =
        (key_types(left, left_keys), key_types(right, right_keys))
    else {
        return Ok(None);
    };
    if left_types.iter().chain(&right_types).all(can_hash) {
        return Ok(None);
    }
    let left_exprs = side_exprs(left_keys, &left_types)?;
    let right_exprs = side_exprs(right_keys, &right_types)?;
    Ok(Some((left_exprs, right_exprs)))
}

fn key_types(schema: &DFSchema, keys: &[Column]) -> Option<Vec<DataType>> {
    keys.iter()
        .map(|key| {
            schema
                .qualified_field_from_column(key)
                .ok()
                .map(|(_, field)| field.data_type().clone())
        })
        .collect()
}

fn side_exprs(keys: &[Column], types: &[DataType]) -> Result<Vec<Expr>> {
    keys.iter()
        .zip(types)
        .map(|(key, data_type)| {
            let column = Expr::Column(key.clone());
            if can_hash(data_type) {
                return Ok(column);
            }
            match hashable_kind(data_type) {
                Some(hashable) => Ok(cast(column, hashable)),
                None => plan_err!(
                    "[{UNHASHABLE_JOIN_KEY}] The join key {} has type {data_type}, which has no \
                     hashable form, so a key-name join cannot use it.",
                    key.name
                ),
            }
        })
        .collect()
}

fn hashable_kind(data_type: &DataType) -> Option<DataType> {
    if can_hash(data_type) {
        return Some(data_type.clone());
    }
    Some(match data_type {
        DataType::List(field) | DataType::ListView(field) => DataType::List(hashable_field(field)?),
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            DataType::LargeList(hashable_field(field)?)
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(hashable_field(field)?, *size)
        }
        DataType::Map(field, sorted) => DataType::Map(hashable_field(field)?, *sorted),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(hashable_field)
                .collect::<Option<Fields>>()?,
        ),
        DataType::RunEndEncoded(_, values) => hashable_kind(values.data_type())?,
        DataType::Dictionary(_, value) => hashable_kind(value)?,
        _ => return None,
    })
}

fn hashable_field(field: &FieldRef) -> Option<FieldRef> {
    if can_hash(field.data_type()) {
        return Some(Arc::clone(field));
    }
    let hashable = hashable_kind(field.data_type())?;
    Some(Arc::new(field.as_ref().clone().with_data_type(hashable)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::compute::can_cast_types;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{Column, DFSchema, JoinType};
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::utils::can_hash;
    use datafusion::logical_expr::{Expr, Join, LogicalPlan, LogicalPlanBuilder};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    use super::{UNHASHABLE_JOIN_KEY, hashable_join_keys, hashable_kind};
    use crate::frame_names::{FrameNode, NameRule, join_on_named_keys, stamp};

    const LEFT: &[(i64, &str)] = &[(1, "1, 2"), (2, "2, 1"), (3, "1, 2")];
    const RIGHT: &[(i64, &str)] = &[(10, "1, 2"), (11, "3")];
    const ROWS_BY_TYPE: [(JoinType, usize); 6] = [
        (JoinType::Inner, 2),
        (JoinType::Left, 3),
        (JoinType::Right, 3),
        (JoinType::Full, 4),
        (JoinType::LeftSemi, 2),
        (JoinType::LeftAnti, 1),
    ];

    fn item(data_type: DataType) -> Arc<Field> {
        Arc::new(Field::new("item", data_type, true))
    }

    fn list_view(data_type: DataType) -> DataType {
        DataType::ListView(item(data_type))
    }

    fn large_list_view(data_type: DataType) -> DataType {
        DataType::LargeListView(item(data_type))
    }

    fn list(data_type: DataType) -> DataType {
        DataType::List(item(data_type))
    }

    fn large_list(data_type: DataType) -> DataType {
        DataType::LargeList(item(data_type))
    }

    fn fixed_size_list(data_type: DataType) -> DataType {
        DataType::FixedSizeList(item(data_type), 2)
    }

    fn run_end_encoded(data_type: DataType) -> DataType {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", data_type, true)),
        )
    }

    fn dictionary(data_type: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type))
    }

    fn struct_of(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn map_of(value: DataType) -> DataType {
        let entries = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", value, true),
        ]);
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            false,
        )
    }

    fn union_of_int64() -> DataType {
        let fields = UnionFields::try_new(vec![0], vec![Field::new("a", DataType::Int64, true)])
            .expect("a one-child union");
        DataType::Union(fields, UnionMode::Sparse)
    }

    fn element() -> DataType {
        struct_of(&[("x", DataType::Int64), ("y", DataType::Utf8)])
    }

    fn unhashable_kinds() -> Vec<(DataType, DataType)> {
        vec![
            (list_view(DataType::Int64), list(DataType::Int64)),
            (
                large_list_view(DataType::Int64),
                large_list(DataType::Int64),
            ),
            (list_view(DataType::Utf8), list(DataType::Utf8)),
            (large_list_view(DataType::Utf8), large_list(DataType::Utf8)),
            (list_view(element()), list(element())),
            (large_list_view(element()), large_list(element())),
            (
                struct_of(&[("a", list_view(DataType::Int64))]),
                struct_of(&[("a", list(DataType::Int64))]),
            ),
            (run_end_encoded(DataType::Int64), DataType::Int64),
            (run_end_encoded(DataType::Utf8), DataType::Utf8),
            (
                dictionary(list_view(DataType::Int64)),
                list(DataType::Int64),
            ),
            (
                dictionary(run_end_encoded(DataType::Int64)),
                DataType::Int64,
            ),
            (
                list(list_view(DataType::Int64)),
                list(list(DataType::Int64)),
            ),
            (
                large_list(list_view(DataType::Int64)),
                large_list(list(DataType::Int64)),
            ),
            (
                fixed_size_list(list_view(DataType::Int64)),
                fixed_size_list(list(DataType::Int64)),
            ),
            (
                map_of(list_view(DataType::Int64)),
                map_of(list(DataType::Int64)),
            ),
            (
                struct_of(&[("a", run_end_encoded(DataType::Int64))]),
                struct_of(&[("a", DataType::Int64)]),
            ),
            (
                list(run_end_encoded(DataType::Int64)),
                list(DataType::Int64),
            ),
            (
                struct_of(&[("h", DataType::Int64), ("a", list_view(DataType::Utf8))]),
                struct_of(&[("h", DataType::Int64), ("a", list(DataType::Utf8))]),
            ),
        ]
    }

    fn hashable_kinds() -> Vec<DataType> {
        vec![
            DataType::Int64,
            DataType::Utf8,
            list(DataType::Int64),
            large_list(DataType::Utf8),
            fixed_size_list(DataType::Int64),
            list(element()),
            struct_of(&[("a", list(DataType::Int64))]),
            dictionary(DataType::Utf8),
            map_of(list(DataType::Int64)),
        ]
    }

    fn schema_of(qualifier: &str, types: &[(&str, DataType)]) -> DFSchema {
        let fields = types
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>();
        DFSchema::try_from_qualified_schema(qualifier, &Schema::new(fields)).expect("a schema")
    }

    fn keys(names: &[&str]) -> Vec<Column> {
        names.iter().map(|name| Column::from_name(*name)).collect()
    }

    #[test]
    fn every_unhashable_kind_maps_to_a_castable_hashable_kind() {
        for (kind, expected) in unhashable_kinds() {
            assert!(!can_hash(&kind), "{kind}");
            let hashable = hashable_kind(&kind).expect("a hashable kind");
            assert_eq!(hashable, expected, "{kind}");
            assert!(can_hash(&hashable), "{hashable}");
            assert!(can_cast_types(&kind, &hashable), "{kind} -> {hashable}");
        }
    }

    #[test]
    fn hashable_kinds_stay_as_they_are() {
        for kind in hashable_kinds() {
            assert_eq!(hashable_kind(&kind), Some(kind.clone()));
            let left = schema_of("l", &[("k", kind.clone())]);
            let right = schema_of("r", &[("k", kind)]);
            let joined = hashable_join_keys(&left, &right, &keys(&["k"]), &keys(&["k"]));
            assert_eq!(joined.expect("no refusal"), None);
        }
    }

    #[test]
    fn a_union_at_any_depth_is_refused_with_its_name() {
        let kinds = [
            union_of_int64(),
            list(union_of_int64()),
            struct_of(&[("a", union_of_int64())]),
            map_of(union_of_int64()),
            run_end_encoded(union_of_int64()),
        ];
        for kind in kinds {
            assert_eq!(hashable_kind(&kind), None, "{kind}");
            let left = schema_of("l", &[("u", kind.clone()), ("j", DataType::Int64)]);
            let right = schema_of("r", &[("u", kind.clone()), ("j", DataType::Int64)]);
            let message = hashable_join_keys(&left, &right, &keys(&["j", "u"]), &keys(&["j", "u"]))
                .expect_err("a union key refuses")
                .to_string();
            assert!(
                message.contains(&format!("[{UNHASHABLE_JOIN_KEY}]")),
                "{message}"
            );
            assert!(message.contains("The join key u has type"), "{message}");
        }
    }

    #[test]
    fn only_the_unhashable_keys_are_cast_on_each_side() {
        let left = schema_of("l", &[("k", list_view(element())), ("j", DataType::Int64)]);
        let right = schema_of("r", &[("k", list(element())), ("j", DataType::Int64)]);
        let names = keys(&["k", "j"]);
        let (left_keys, right_keys) = hashable_join_keys(&left, &right, &names, &names)
            .expect("no refusal")
            .expect("the keys are rebuilt");
        assert_eq!(
            left_keys,
            vec![
                datafusion::logical_expr::cast(Expr::Column(names[0].clone()), list(element())),
                Expr::Column(names[1].clone()),
            ]
        );
        assert_eq!(
            right_keys,
            vec![
                Expr::Column(names[0].clone()),
                Expr::Column(names[1].clone())
            ]
        );
    }

    #[test]
    fn an_unresolved_key_leaves_the_join_to_report_it() {
        let left = schema_of("l", &[("k", list_view(DataType::Int64))]);
        let right = schema_of("r", &[("k", list_view(DataType::Int64))]);
        let joined = hashable_join_keys(&left, &right, &keys(&["k", "absent"]), &keys(&["k", "k"]));
        assert_eq!(joined.expect("no refusal"), None);
    }

    fn side_sql(id: &str, kind: &str, rows: &[(i64, &str)]) -> String {
        rows.iter()
            .map(|(value, items)| {
                format!(
                    "SELECT CAST({value} AS BIGINT) AS {id}, \
                     arrow_cast(make_array({items}), '{kind}') AS k"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }

    async fn side(id: &str, kind: &str, rows: &[(i64, &str)], alias: &str) -> DataFrame {
        let frame = SessionContext::new()
            .sql(&side_sql(id, kind, rows))
            .await
            .expect("a side builds")
            .alias(alias)
            .expect("an alias");
        let (state, plan) = frame.into_parts();
        DataFrame::new(state, stamp(plan).expect("stamped"))
    }

    async fn sides(left_kind: &str, right_kind: &str) -> (DataFrame, DataFrame) {
        (
            side("lid", left_kind, LEFT, "l").await,
            side("rid", right_kind, RIGHT, "r").await,
        )
    }

    fn named_join(left: DataFrame, right: DataFrame, join_type: JoinType) -> DataFrame {
        let left_node = FrameNode::root(left.schema()).expect("a left node");
        let right_node = FrameNode::root(right.schema()).expect("a right node");
        let keys = ["k".to_string()];
        join_on_named_keys(
            left,
            right,
            &keys,
            join_type,
            NameRule::Exact,
            left_node,
            right_node,
        )
        .expect("the key-name join builds")
        .0
    }

    fn join_node(plan: &LogicalPlan) -> Join {
        let mut found = None;
        plan.apply(|node| {
            if let LogicalPlan::Join(join) = node {
                found = Some(join.clone());
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("a plan walk");
        found.expect("a join node")
    }

    async fn physical_text(frame: DataFrame) -> String {
        let plan = frame.create_physical_plan().await.expect("a physical plan");
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn key_type(frame: &DataFrame) -> DataType {
        frame
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == "k")
            .expect("the key column")
            .data_type()
            .clone()
    }

    #[tokio::test]
    async fn a_hashable_key_join_builds_mains_plan() {
        for (join_type, rows) in ROWS_BY_TYPE {
            let (left, right) = sides("List(Int64)", "List(Int64)").await;
            let expected = LogicalPlanBuilder::from(left.clone().into_unoptimized_plan())
                .join(
                    right.clone().into_unoptimized_plan(),
                    join_type,
                    (keys(&["k"]), keys(&["k"])),
                    None,
                )
                .expect("main's join builds")
                .build()
                .expect("main's plan");
            let joined = named_join(left, right, join_type);
            assert_eq!(
                LogicalPlan::Join(join_node(joined.logical_plan())),
                expected
            );
            assert_eq!(joined.count().await.expect("a count"), rows, "{join_type}");
        }
    }

    #[tokio::test]
    async fn a_listview_key_join_keeps_its_hash_join_on_the_cast_key() {
        let kinds = [
            (
                "ListView(Int64)",
                list_view(DataType::Int64),
                list(DataType::Int64),
            ),
            (
                "LargeListView(Int64)",
                large_list_view(DataType::Int64),
                large_list(DataType::Int64),
            ),
        ];
        for (kind, original, cast_to) in kinds {
            for (join_type, rows) in ROWS_BY_TYPE {
                let (left, right) = sides(kind, kind).await;
                let joined = named_join(left, right, join_type);
                let join = join_node(joined.logical_plan());
                assert_eq!(join.on.len(), 1);
                for side in [&join.on[0].0, &join.on[0].1] {
                    let Expr::Cast(cast) = side else {
                        panic!("{join_type}: the key is not cast: {side}");
                    };
                    assert_eq!(cast.field.data_type(), &cast_to);
                }
                assert_eq!(key_type(&joined), original, "{join_type}");
                let physical = physical_text(joined.clone()).await;
                assert!(!physical.contains("CrossJoinExec"), "{physical}");
                assert!(!physical.contains("NestedLoopJoinExec"), "{physical}");
                let hash_join = physical
                    .lines()
                    .find(|line| line.contains("HashJoinExec"))
                    .unwrap_or_else(|| panic!("{join_type}: no hash join: {physical}"));
                assert!(hash_join.contains("CAST("), "{hash_join}");
                assert_eq!(joined.count().await.expect("a count"), rows, "{join_type}");
            }
        }
    }

    #[tokio::test]
    async fn a_mixed_listview_and_list_key_join_answers_on_the_cast_key() {
        for (join_type, rows) in ROWS_BY_TYPE {
            let (left, right) = sides("ListView(Int64)", "List(Int64)").await;
            let joined = named_join(left, right, join_type);
            let join = join_node(joined.logical_plan());
            assert!(matches!(join.on[0].0, Expr::Cast(_)), "{}", join.on[0].0);
            assert!(matches!(join.on[0].1, Expr::Column(_)), "{}", join.on[0].1);
            assert_eq!(joined.count().await.expect("a count"), rows, "{join_type}");
        }
    }
}
