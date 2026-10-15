use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, FixedSizeListArray, GenericListArray,
    GenericListViewArray, Int32Array, MapArray, RunArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int32Type};

use super::{
    STORE_PAIR_KERNEL_NAME, StoreEntry, StructPairing, pair_store_value, refuse_unpaired_struct,
    spark_names_equal_ignoring_case, store_pair_sql,
};

const SHAPES: [(&str, &[(&str, i32)]); 8] = [
    ("same", &[("a", 1), ("b", 2)]),
    ("reordered", &[("b", 2), ("a", 1)]),
    ("renamed", &[("a", 1), ("q", 2)]),
    ("renamed_first", &[("q", 1), ("b", 2)]),
    ("extra", &[("a", 1), ("b", 2), ("c", 3)]),
    ("missing", &[("a", 1)]),
    ("case", &[("A", 1), ("B", 2)]),
    ("case_reordered", &[("B", 2), ("A", 1)]),
];

const MODES: [StructPairing; 3] = [
    StructPairing::ByPosition,
    StructPairing::ByName {
        case_sensitive: false,
    },
    StructPairing::ByName {
        case_sensitive: true,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Struct,
    List,
    LargeList,
    ListView,
    LargeListView,
    FixedSizeList,
    MapKey,
    MapValue,
    Dictionary,
    RunEndEncoded,
}

const LAYOUTS: [Layout; 10] = [
    Layout::Struct,
    Layout::List,
    Layout::LargeList,
    Layout::ListView,
    Layout::LargeListView,
    Layout::FixedSizeList,
    Layout::MapKey,
    Layout::MapValue,
    Layout::Dictionary,
    Layout::RunEndEncoded,
];

#[derive(Debug, PartialEq, Eq)]
enum Expected {
    Stored(i32, i32),
    Refused(&'static str, String),
}

fn rule(mode: StructPairing, shape: &[(&str, i32)]) -> Expected {
    let target = ["a", "b"];
    let StructPairing::ByName { case_sensitive } = mode else {
        if shape.len() > target.len() {
            return Expected::Refused("EXTRA_STRUCT_FIELDS", String::new());
        }
        if shape.len() < target.len() {
            return Expected::Refused("STRUCT_MISSING_FIELDS", String::new());
        }
        return Expected::Stored(shape[0].1, shape[1].1);
    };
    let alike = |source: &str, target: &str| {
        if case_sensitive {
            source == target
        } else {
            source.to_lowercase() == target.to_lowercase()
        }
    };
    let mut values = Vec::new();
    let mut used = 0;
    for name in target {
        let found: Vec<i32> = shape
            .iter()
            .filter(|(source, _)| alike(source, name))
            .map(|(_, value)| *value)
            .collect();
        match found.as_slice() {
            [] => return Expected::Refused("CANNOT_FIND_DATA", format!(".`{name}`")),
            [value] => values.push(*value),
            _ => return Expected::Refused("AMBIGUOUS_COLUMN_NAME", format!(".`{name}`")),
        }
        used += 1;
    }
    if shape.len() > used {
        return Expected::Refused("EXTRA_STRUCT_FIELDS", String::new());
    }
    Expected::Stored(values[0], values[1])
}

fn shape_fields(shape: &[(&str, i32)]) -> Fields {
    shape
        .iter()
        .map(|(name, _)| Field::new(*name, DataType::Int32, true))
        .collect()
}

fn shape_struct(shape: &[(&str, i32)], rows: usize, nulls: Option<Vec<bool>>) -> StructArray {
    let columns: Vec<ArrayRef> = shape
        .iter()
        .map(|(_, value)| Arc::new(Int32Array::from(vec![*value; rows])) as ArrayRef)
        .collect();
    StructArray::try_new(shape_fields(shape), columns, nulls.map(NullBuffer::from))
        .expect("shape struct")
}

fn target_struct() -> DataType {
    DataType::Struct(shape_fields(&[("a", 0), ("b", 0)]))
}

fn element(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("element", data_type, true))
}

fn map_entries(key: DataType, value: DataType) -> Arc<Field> {
    Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ])),
        false,
    ))
}

fn source_value(layout: Layout, shape: &[(&str, i32)]) -> ArrayRef {
    let pair = || shape_struct(shape, 2, Some(vec![true, false]));
    let lists = || Some(NullBuffer::from(vec![true, false]));
    match layout {
        Layout::Struct => Arc::new(pair()),
        Layout::List => Arc::new(GenericListArray::<i32>::new(
            element(pair().data_type().clone()),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2])),
            Arc::new(pair()),
            lists(),
        )),
        Layout::LargeList => Arc::new(GenericListArray::<i64>::new(
            element(pair().data_type().clone()),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 2])),
            Arc::new(pair()),
            lists(),
        )),
        Layout::ListView => Arc::new(GenericListViewArray::<i32>::new(
            element(pair().data_type().clone()),
            ScalarBuffer::from(vec![0, 2]),
            ScalarBuffer::from(vec![2, 0]),
            Arc::new(pair()),
            lists(),
        )),
        Layout::LargeListView => Arc::new(GenericListViewArray::<i64>::new(
            element(pair().data_type().clone()),
            ScalarBuffer::from(vec![0_i64, 2]),
            ScalarBuffer::from(vec![2_i64, 0]),
            Arc::new(pair()),
            lists(),
        )),
        Layout::FixedSizeList => Arc::new(FixedSizeListArray::new(
            element(pair().data_type().clone()),
            1,
            Arc::new(pair()),
            lists(),
        )),
        Layout::MapKey => {
            let keys = shape_struct(shape, 2, None);
            let entries = StructArray::try_new(
                Fields::from(vec![
                    Field::new("key", keys.data_type().clone(), false),
                    Field::new("value", DataType::Int32, true),
                ]),
                vec![Arc::new(keys), Arc::new(Int32Array::from(vec![7, 8]))],
                None,
            )
            .expect("map entries");
            Arc::new(
                MapArray::try_new(
                    map_entries(
                        shape_struct(shape, 0, None).data_type().clone(),
                        DataType::Int32,
                    ),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2])),
                    entries,
                    lists(),
                    false,
                )
                .expect("map"),
            )
        }
        Layout::MapValue => {
            let entries = StructArray::try_new(
                Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", pair().data_type().clone(), true),
                ]),
                vec![
                    Arc::new(StringArray::from(vec!["k", "j"])),
                    Arc::new(pair()),
                ],
                None,
            )
            .expect("map entries");
            Arc::new(
                MapArray::try_new(
                    map_entries(DataType::Utf8, pair().data_type().clone()),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2])),
                    entries,
                    lists(),
                    false,
                )
                .expect("map"),
            )
        }
        Layout::Dictionary => Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(1), None]),
                Arc::new(pair()),
            )
            .expect("dictionary"),
        ),
        Layout::RunEndEncoded => Arc::new(
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3]), &pair())
                .expect("run array"),
        ),
    }
}

fn target_type(layout: Layout) -> DataType {
    match layout {
        Layout::Struct | Layout::Dictionary | Layout::RunEndEncoded => target_struct(),
        Layout::List
        | Layout::LargeList
        | Layout::ListView
        | Layout::LargeListView
        | Layout::FixedSizeList => DataType::List(element(target_struct())),
        Layout::MapKey => DataType::Map(map_entries(target_struct(), DataType::Int32), false),
        Layout::MapValue => DataType::Map(map_entries(DataType::Utf8, target_struct()), false),
    }
}

fn path_of(layout: Layout) -> &'static str {
    match layout {
        Layout::Struct | Layout::Dictionary | Layout::RunEndEncoded => "`st`",
        Layout::MapKey => "`st`.`key`",
        Layout::MapValue => "`st`.`value`",
        _ => "`st`.`element`",
    }
}

fn inner_struct(layout: Layout, value: &ArrayRef) -> StructArray {
    let inner: ArrayRef = match layout {
        Layout::Struct => Arc::clone(value),
        Layout::List => Arc::clone(value.as_list::<i32>().values()),
        Layout::LargeList => Arc::clone(value.as_list::<i64>().values()),
        Layout::ListView => Arc::clone(value.as_list_view::<i32>().values()),
        Layout::LargeListView => Arc::clone(value.as_list_view::<i64>().values()),
        Layout::FixedSizeList => Arc::clone(value.as_fixed_size_list().values()),
        Layout::MapKey => Arc::clone(value.as_map().keys()),
        Layout::MapValue => Arc::clone(value.as_map().values()),
        Layout::Dictionary => Arc::clone(value.as_any_dictionary().values()),
        Layout::RunEndEncoded => Arc::clone(
            value
                .as_any()
                .downcast_ref::<RunArray<Int32Type>>()
                .expect("run array")
                .values(),
        ),
    };
    inner.as_struct().clone()
}

fn field_value(inner: &StructArray, name: &str) -> i32 {
    inner
        .column_by_name(name)
        .expect("paired field")
        .as_primitive::<Int32Type>()
        .value(0)
}

#[test]
fn the_rule_pairs_every_mode_shape_and_layout_and_keeps_every_null() {
    let mut cells = 0;
    for mode in MODES {
        for (shape_name, shape) in SHAPES {
            for layout in LAYOUTS {
                cells += 1;
                let label = format!("{mode:?} {shape_name} {layout:?}");
                let value = source_value(layout, shape);
                let target = target_type(layout);
                let outcome = pair_store_value(mode, "`t`", "st", value.data_type(), &target);
                match (rule(mode, shape), outcome) {
                    (Expected::Refused(class, suffix), Err(error)) => {
                        let text = error.to_string();
                        assert!(
                            text.contains(&format!("[INCOMPATIBLE_DATA_FOR_TABLE.{class}]")),
                            "{label}: {text}"
                        );
                        assert!(text.contains("for the table `t`:"), "{label}: {text}");
                        let path = format!("{}{suffix}", path_of(layout));
                        assert!(text.contains(&path), "{label}: {text}");
                    }
                    (Expected::Stored(a, b), Ok(pairs)) => {
                        let paired = pairs.apply(&value).expect("apply");
                        assert_eq!(paired.data_type(), &pairs.paired_type(), "{label}");
                        assert_eq!(paired.len(), value.len(), "{label}");
                        assert_eq!(paired.logical_nulls(), value.logical_nulls(), "{label}");
                        let inner = inner_struct(layout, &paired);
                        let names: Vec<&str> =
                            inner.fields().iter().map(|f| f.name().as_str()).collect();
                        assert_eq!(names, ["a", "b"], "{label}");
                        assert_eq!(
                            (field_value(&inner, "a"), field_value(&inner, "b")),
                            (a, b),
                            "{label}"
                        );
                        if layout != Layout::MapKey {
                            assert!(inner.is_null(1), "{label}: a NULL struct stays NULL");
                        }
                        assert_eq!(
                            pairs.is_identity(),
                            shape_name == "same",
                            "{label}: identity"
                        );
                        for again in MODES {
                            let twice =
                                pair_store_value(again, "`t`", "st", &pairs.paired_type(), &target)
                                    .expect("a paired value pairs again");
                            assert!(twice.is_identity(), "{label} then {again:?}");
                            let same = twice.apply(&paired).expect("identity apply");
                            assert!(Arc::ptr_eq(&same, &paired), "{label}");
                        }
                    }
                    (expected, outcome) => {
                        panic!(
                            "{label}: rule {expected:?}, seam {:?}",
                            outcome.map(|p| p.paired_type())
                        )
                    }
                }
            }
        }
    }
    assert_eq!(cells, 3 * 8 * 10);
}

#[test]
fn the_leaf_pairs_name_their_paths_and_are_never_converted() {
    let source = DataType::List(element(DataType::Struct(Fields::from(vec![
        Field::new("b", DataType::Utf8, true),
        Field::new("a", DataType::Int64, true),
    ]))));
    let target = DataType::List(element(target_struct()));
    let pairs = pair_store_value(StructPairing::by_name(false), "``", "st", &source, &target)
        .expect("pairs");
    let leaves: Vec<(&str, &DataType, &DataType)> = pairs
        .leaves()
        .iter()
        .map(|leaf| (leaf.path.as_str(), &leaf.source, &leaf.target))
        .collect();
    assert_eq!(
        leaves,
        [
            ("`st`.`element`.`a`", &DataType::Int64, &DataType::Int32),
            ("`st`.`element`.`b`", &DataType::Utf8, &DataType::Int32),
        ]
    );
    let DataType::List(paired) = pairs.paired_type() else {
        panic!("a list stays a list");
    };
    assert_eq!(
        paired.data_type(),
        &DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    );
}

#[test]
fn a_struct_reaching_a_batch_cast_unpaired_takes_hint_h1() {
    let reordered = DataType::Struct(shape_fields(&[("b", 0), ("a", 0)]));
    let text = refuse_unpaired_struct("``", StoreEntry::Batch, "st", &reordered, &target_struct())
        .expect_err("unpaired")
        .to_string();
    assert!(
        text.contains(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table ``: the struct `st` has the fields `b`, `a` and the table has `a`, `b`. \
             This entry does not pair struct fields"
        ),
        "{text}"
    );
    let partition = refuse_unpaired_struct(
        "``",
        StoreEntry::StaticPartition,
        "st",
        &DataType::List(element(reordered)),
        &DataType::List(element(target_struct())),
    )
    .expect_err("unpaired")
    .to_string();
    assert!(
        partition.contains("the struct `st`.`element`"),
        "{partition}"
    );
    assert!(partition.contains("static PARTITION value"), "{partition}");
    for paired in [target_struct(), DataType::Int32] {
        assert!(refuse_unpaired_struct("``", StoreEntry::Batch, "st", &paired, &paired).is_ok());
    }
}

#[test]
fn a_union_holding_a_struct_takes_hint_h3() {
    let union = DataType::Union(
        datafusion::arrow::datatypes::UnionFields::try_new(
            vec![0],
            vec![Field::new("s", target_struct(), true)],
        )
        .expect("union fields"),
        datafusion::arrow::datatypes::UnionMode::Dense,
    );
    for text in [
        pair_store_value(
            StructPairing::ByPosition,
            "``",
            "st",
            &union,
            &target_struct(),
        )
        .expect_err("union")
        .to_string(),
        refuse_unpaired_struct("``", StoreEntry::Batch, "st", &union, &target_struct())
            .expect_err("union")
            .to_string(),
    ] {
        assert!(
            text.contains("is not written into a struct column `st`"),
            "{text}"
        );
    }
}

#[test]
fn the_kernel_text_names_the_target_and_the_mode_only_for_a_struct_target() {
    assert_eq!(
        store_pair_sql(
            "s.v",
            &DataType::Int32,
            StructPairing::ByPosition,
            "``",
            "v"
        ),
        None
    );
    assert_eq!(
        store_pair_sql(
            "s.st",
            &target_struct(),
            StructPairing::by_name(true),
            "``",
            "`it's`"
        )
        .as_deref(),
        Some(
            format!(
                "{STORE_PAIR_KERNEL_NAME}((s.st), 'Struct(\"a\": Int32, \"b\": Int32)', \
                 'by_name_case_sensitive', '``', '`it''s`')"
            )
            .as_str()
        )
    );
}

#[tokio::test]
async fn the_kernel_refuses_at_plan_time_and_pairs_at_run_time() {
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_udf(super::store_pair_udf().as_ref().clone());
    let target = "Struct(\"a\": Int64, \"b\": Int64)";
    let paired = ctx
        .sql(&format!(
            "SELECT {STORE_PAIR_KERNEL_NAME}(named_struct('b', 2, 'a', 1), '{target}', \
             'by_name', '``', '`st`') AS st"
        ))
        .await
        .expect("plans")
        .collect()
        .await
        .expect("runs");
    let column = paired[0].column(0).as_struct();
    assert_eq!(column.column_names(), ["a", "b"]);
    assert_eq!(
        column
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0),
        1
    );
    let refused = ctx
        .sql(&format!(
            "SELECT {STORE_PAIR_KERNEL_NAME}(named_struct('a', 1, 'q', 2), '{target}', \
             'by_name', '``', '`st`') AS st"
        ))
        .await
        .expect_err("plan-time refusal")
        .to_string();
    assert!(
        refused.contains("Cannot find data for the output column `st`.`b`"),
        "{refused}"
    );
}

fn unpaired_batch() -> (
    datafusion::arrow::datatypes::SchemaRef,
    datafusion::arrow::record_batch::RecordBatch,
) {
    use datafusion::arrow::datatypes::Schema;
    let reordered = shape_struct(&[("b", 2), ("a", 1)], 1, None);
    let source = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("st", reordered.data_type().clone(), true),
    ]));
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        source,
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(reordered) as ArrayRef,
        ],
    )
    .expect("batch");
    let table = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("st", target_struct(), true),
    ]));
    (table, batch)
}

const HINT_H1_BATCH: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
     incompatible data for the table ``: the struct `st` has the fields `b`, `a` and the table \
     has `a`, `b`. This entry does not pair struct fields; give the value the table's field \
     names in the table's order.";

#[test]
fn the_append_batch_cast_s1_refuses_an_unpaired_struct_with_hint_h1() {
    let (table, batch) = unpaired_batch();
    let text =
        crate::write::conform::conform_batch(&table, &std::collections::HashSet::new(), &batch)
            .expect_err("S1 refuses")
            .to_string();
    assert!(text.contains(HINT_H1_BATCH), "{text}");
}

#[test]
fn the_positional_overwrite_cast_s2_refuses_an_unpaired_struct_with_hint_h1() {
    let (table, batch) = unpaired_batch();
    let text = crate::write::overwrite::positional_map_overwrite_batch(&batch, &table, &[])
        .expect_err("S2 refuses")
        .to_string();
    assert!(text.contains(HINT_H1_BATCH), "{text}");
    let listed = ["id".to_string(), "st".to_string()];
    let text = crate::write::overwrite::positional_map_overwrite_batch(&batch, &table, &listed)
        .expect_err("S2 with a column list refuses")
        .to_string();
    assert!(text.contains(HINT_H1_BATCH), "{text}");
}

fn named(fields: &[(&str, DataType)]) -> DataType {
    DataType::Struct(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>()
            .into(),
    )
}

#[test]
fn every_paired_leaf_is_judged_as_a_flat_column_is() {
    let typed = named(&[("a", DataType::Int64), ("b", DataType::Utf8)]);
    let in_order = named(&[("a", DataType::Utf8), ("b", DataType::Utf8)]);
    let text = refuse_unpaired_struct("`t`", StoreEntry::Batch, "st", &in_order, &typed)
        .expect_err("a STRING leaf into BIGINT")
        .to_string();
    assert!(
        text.contains(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `t`: Cannot safely cast `st`.`a` \"STRING\" to \"BIGINT\". SQLSTATE: KD000"
        ),
        "{text}"
    );
    let two_deep = named(&[(
        "o",
        named(&[
            (
                "p",
                named(&[("a", DataType::Int32), ("b", DataType::Int32)]),
            ),
            ("n", DataType::Int32),
        ]),
    )]);
    let swapped = named(&[(
        "o",
        named(&[
            ("n", DataType::Int32),
            (
                "p",
                named(&[("b", DataType::Int32), ("a", DataType::Int32)]),
            ),
        ]),
    )]);
    for entry in [StoreEntry::Batch, StoreEntry::StaticPartition] {
        let text = refuse_unpaired_struct("`t`", entry, "st", &swapped, &two_deep)
            .expect_err("an INT leaf into a STRUCT")
            .to_string();
        assert!(
            text.contains("Cannot safely cast `st`.`o`.`p` \"INT\" to \"STRUCT<A:INT,B:INT>\""),
            "{text}"
        );
    }
    let pairs = pair_store_value(
        StructPairing::ByPosition,
        "`t`",
        "st",
        &DataType::List(element(swapped.clone())),
        &DataType::List(element(two_deep.clone())),
    )
    .expect("paired by position");
    assert_eq!(
        crate::write::update_cast::unassignable_leaf_message("`t`", &pairs).as_deref(),
        Some(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `t`: Cannot safely cast `st`.`element`.`o`.`p` \"INT\" to \
             \"STRUCT<A:INT,B:INT>\". SQLSTATE: KD000"
        )
    );
    let widened = named(&[("a", DataType::Int32), ("b", DataType::Utf8View)]);
    let pairs = pair_store_value(StructPairing::by_name(false), "`t`", "st", &widened, &typed)
        .expect("paired by name");
    assert_eq!(
        crate::write::update_cast::unassignable_leaf_message("`t`", &pairs),
        None
    );
    let extra = named(&[
        ("a", DataType::Int64),
        ("b", DataType::Utf8),
        ("c", DataType::Int32),
    ]);
    let text = refuse_unpaired_struct("`t`", StoreEntry::StaticPartition, "st", &extra, &typed)
        .expect_err("a surplus field by position")
        .to_string();
    assert!(
        text.contains("[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS]"),
        "{text}"
    );
}

#[test]
fn case_insensitive_names_match_as_java_equals_ignore_case() {
    for (source, target) in [
        ("É", "é"),
        ("ẞ", "ß"),
        ("ß", "ẞ"),
        ("A", "a"),
        ("İ", "i"),
        ("ᾀ", "ᾈ"),
        ("ſ", "s"),
    ] {
        assert!(
            spark_names_equal_ignoring_case(source, target),
            "{source} {target}"
        );
    }
    for (source, target) in [
        ("SS", "ß"),
        ("ß", "SS"),
        ("é", "e"),
        ("ab", "a"),
        ("ſ", "ss"),
    ] {
        assert!(
            !spark_names_equal_ignoring_case(source, target),
            "{source} {target}"
        );
    }
    let target = named(&[("é", DataType::Int32), ("b", DataType::Int32)]);
    let source = named(&[("b", DataType::Int32), ("É", DataType::Int32)]);
    let pairs = pair_store_value(StructPairing::by_name(false), "`t`", "st", &source, &target)
        .expect("É pairs with é");
    assert!(!pairs.is_identity());
    let text = pair_store_value(StructPairing::by_name(true), "`t`", "st", &source, &target)
        .expect_err("case-sensitive")
        .to_string();
    assert!(
        text.contains("Cannot find data for the output column `st`.`é`"),
        "{text}"
    );
    let sharp = named(&[("ß", DataType::Int32), ("b", DataType::Int32)]);
    let doubled = named(&[("SS", DataType::Int32), ("b", DataType::Int32)]);
    let text = pair_store_value(StructPairing::by_name(false), "`t`", "st", &doubled, &sharp)
        .expect_err("SS is not ß")
        .to_string();
    assert!(
        text.contains("Cannot find data for the output column `st`.`ß`"),
        "{text}"
    );
}
