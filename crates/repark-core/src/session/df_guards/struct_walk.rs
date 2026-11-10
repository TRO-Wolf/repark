use datafusion::arrow::datatypes::{DataType, FieldRef};

#[derive(Debug, Clone, PartialEq)]
pub enum StructNameMismatch {
    Names {
        path: String,
        left_names: Vec<String>,
        right_names: Vec<String>,
    },
    Unpaired {
        path: String,
        left: DataType,
        right: DataType,
    },
}

#[must_use]
pub fn peel_encoding(data_type: &DataType) -> &DataType {
    let mut current = data_type;
    loop {
        match current {
            DataType::Dictionary(_, value) => current = value,
            DataType::RunEndEncoded(_, values) => current = values.data_type(),
            _ => return current,
        }
    }
}

#[must_use]
pub fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

#[must_use]
pub fn holds_struct(data_type: &DataType) -> bool {
    match peel_encoding(data_type) {
        DataType::Struct(_) => true,
        DataType::Map(entries, _) => match map_key_and_value(entries.data_type()) {
            Some((key, value)) => holds_struct(key) || holds_struct(value),
            None => holds_struct(entries.data_type()),
        },
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| holds_struct(field.data_type())),
        peeled => list_element(peeled).is_some_and(|element| holds_struct(element.data_type())),
    }
}

#[must_use]
pub fn first_struct_name_mismatch(left: &DataType, right: &DataType) -> Option<StructNameMismatch> {
    mismatch_at(left, right, String::new())
}

fn mismatch_at(left: &DataType, right: &DataType, path: String) -> Option<StructNameMismatch> {
    let left = peel_encoding(left);
    let right = peel_encoding(right);
    if left == right || matches!(left, DataType::Null) || matches!(right, DataType::Null) {
        return None;
    }
    if let (DataType::Struct(left_fields), DataType::Struct(right_fields)) = (left, right) {
        let left_names: Vec<&str> = left_fields
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let right_names: Vec<&str> = right_fields
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        if left_names != right_names {
            return Some(StructNameMismatch::Names {
                path,
                left_names: left_names.iter().map(ToString::to_string).collect(),
                right_names: right_names.iter().map(ToString::to_string).collect(),
            });
        }
        for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
            let mut child = path.clone();
            child.push('.');
            child.push_str(left_field.name());
            if let Some(found) = mismatch_at(left_field.data_type(), right_field.data_type(), child)
            {
                return Some(found);
            }
        }
        return None;
    }
    if let (Some(left_element), Some(right_element)) = (list_element(left), list_element(right)) {
        return mismatch_at(
            left_element.data_type(),
            right_element.data_type(),
            format!("{path}[]"),
        );
    }
    if let (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) = (left, right)
        && let (Some((left_key, left_value)), Some((right_key, right_value))) = (
            map_key_and_value(left_entries.data_type()),
            map_key_and_value(right_entries.data_type()),
        )
    {
        return mismatch_at(left_key, right_key, format!("{path}{{key}}"))
            .or_else(|| mismatch_at(left_value, right_value, format!("{path}{{value}}")));
    }
    if holds_struct(left) && holds_struct(right) {
        return Some(StructNameMismatch::Unpaired {
            path,
            left: left.clone(),
            right: right.clone(),
        });
    }
    None
}

fn map_key_and_value(entries: &DataType) -> Option<(&DataType, &DataType)> {
    if let DataType::Struct(fields) = entries
        && let [key, value] = &fields[..]
    {
        return Some((key.data_type(), value.data_type()));
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};

    use super::*;

    const KINDS: [&str; 6] = ["list", "large", "fsl1", "fsl2", "view", "largeview"];

    fn struct_ab() -> DataType {
        DataType::Struct(
            vec![
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(Field::new("b", DataType::Int64, true)),
            ]
            .into(),
        )
    }

    fn struct_ba() -> DataType {
        DataType::Struct(
            vec![
                Arc::new(Field::new("b", DataType::Int64, true)),
                Arc::new(Field::new("a", DataType::Int64, true)),
            ]
            .into(),
        )
    }

    fn list_of(kind: &str, element: DataType) -> DataType {
        let field = Arc::new(Field::new("item", element, true));
        match kind {
            "list" => DataType::List(field),
            "large" => DataType::LargeList(field),
            "fsl1" => DataType::FixedSizeList(field, 1),
            "fsl2" => DataType::FixedSizeList(field, 2),
            "view" => DataType::ListView(field),
            _ => DataType::LargeListView(field),
        }
    }

    fn dictionary_of(inner: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner))
    }

    fn run_end_encoded(inner: DataType) -> DataType {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", inner, true)),
        )
    }

    fn map_of(entry_names: (&str, &str), value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new(entry_names.0, DataType::Utf8, false)),
                        Arc::new(Field::new(entry_names.1, value, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    fn union_with(first: DataType, second: DataType) -> DataType {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Arc::new(Field::new("first", first, true)),
                Arc::new(Field::new("second", second, true)),
            ],
        )
        .expect("two distinct union type ids");
        DataType::Union(fields, UnionMode::Dense)
    }

    fn names(path: &str, left: &[&str], right: &[&str]) -> StructNameMismatch {
        StructNameMismatch::Names {
            path: path.to_string(),
            left_names: left.iter().map(ToString::to_string).collect(),
            right_names: right.iter().map(ToString::to_string).collect(),
        }
    }

    #[test]
    fn ordered_list_kind_pairs_recurse_into_elements() {
        for left_kind in KINDS {
            for right_kind in KINDS {
                let left = list_of(left_kind, struct_ab());
                let right = list_of(right_kind, struct_ba());
                assert_eq!(
                    first_struct_name_mismatch(&left, &right),
                    Some(names("[]", &["a", "b"], &["b", "a"])),
                    "{left_kind} against {right_kind}"
                );
                let same = list_of(right_kind, struct_ab());
                assert_eq!(
                    first_struct_name_mismatch(&left, &same),
                    None,
                    "{left_kind} against {right_kind} same order"
                );
            }
        }
    }

    #[test]
    fn fixed_size_list_sizes_mix_freely() {
        for (left_kind, right_kind) in [
            ("fsl1", "fsl1"),
            ("fsl1", "fsl2"),
            ("fsl2", "fsl1"),
            ("fsl2", "fsl2"),
        ] {
            let left = list_of(left_kind, struct_ab());
            let right = list_of(right_kind, struct_ba());
            assert_eq!(
                first_struct_name_mismatch(&left, &right),
                Some(names("[]", &["a", "b"], &["b", "a"])),
                "{left_kind} against {right_kind}"
            );
        }
    }

    #[test]
    fn dictionary_and_run_end_encoded_peel_on_either_side() {
        let plain_ab = list_of("list", struct_ab());
        let plain_ba = list_of("large", struct_ba());
        let reordered = [
            (dictionary_of(plain_ab.clone()), plain_ba.clone()),
            (plain_ab.clone(), dictionary_of(plain_ba.clone())),
            (
                dictionary_of(plain_ab.clone()),
                dictionary_of(plain_ba.clone()),
            ),
            (run_end_encoded(plain_ab.clone()), plain_ba.clone()),
            (plain_ab.clone(), run_end_encoded(plain_ba.clone())),
            (
                run_end_encoded(plain_ab.clone()),
                run_end_encoded(plain_ba.clone()),
            ),
            (
                dictionary_of(run_end_encoded(plain_ab.clone())),
                plain_ba.clone(),
            ),
        ];
        for (left, right) in reordered {
            assert_eq!(
                first_struct_name_mismatch(&left, &right),
                Some(names("[]", &["a", "b"], &["b", "a"]))
            );
        }
        let same_ba = list_of("large", struct_ab());
        assert_eq!(
            first_struct_name_mismatch(&dictionary_of(plain_ab.clone()), &same_ba),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(&run_end_encoded(plain_ab.clone()), &same_ba),
            None
        );
    }

    #[test]
    fn map_with_renamed_entry_fields_checks_the_value_struct() {
        let left = map_of(("key", "value"), list_of("list", struct_ab()));
        let same = map_of(("k", "v"), list_of("large", struct_ab()));
        assert_eq!(first_struct_name_mismatch(&left, &same), None);
        let reordered = map_of(("k", "v"), list_of("large", struct_ba()));
        assert_eq!(
            first_struct_name_mismatch(&left, &reordered),
            Some(names("{value}[]", &["a", "b"], &["b", "a"]))
        );
    }

    #[test]
    fn union_holding_structs_on_both_sides_is_unpaired() {
        let left = union_with(struct_ab(), DataType::Int32);
        let right = union_with(DataType::Int32, struct_ba());
        assert_eq!(
            first_struct_name_mismatch(&left, &right),
            Some(StructNameMismatch::Unpaired {
                path: String::new(),
                left: left.clone(),
                right: right.clone(),
            })
        );
    }

    #[test]
    fn identical_types_never_mismatch_even_through_a_union() {
        let union = union_with(struct_ab(), struct_ba());
        assert_eq!(first_struct_name_mismatch(&union, &union.clone()), None);
        let listed = list_of("largeview", union_with(struct_ba(), DataType::Int32));
        assert_eq!(first_struct_name_mismatch(&listed, &listed.clone()), None);
        let encoded = dictionary_of(map_of(("key", "value"), union.clone()));
        assert_eq!(first_struct_name_mismatch(&encoded, &encoded.clone()), None);
        let other = union_with(struct_ab(), struct_ab());
        assert!(matches!(
            first_struct_name_mismatch(&union, &other),
            Some(StructNameMismatch::Unpaired { .. })
        ));
    }

    #[test]
    fn pairs_without_structs_on_both_sides_answer() {
        assert_eq!(
            first_struct_name_mismatch(&DataType::Int32, &DataType::Int64),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(
                &list_of("list", DataType::Int32),
                &list_of("large", DataType::Int64)
            ),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(
                &union_with(DataType::Int32, DataType::Utf8),
                &union_with(DataType::Int64, DataType::Utf8)
            ),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(&list_of("view", struct_ab()), &DataType::Utf8),
            None
        );
    }

    #[test]
    fn null_on_either_side_skips_before_every_step() {
        let struct_list = list_of("large", struct_ba());
        assert_eq!(
            first_struct_name_mismatch(&DataType::Null, &struct_list),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(&struct_list, &DataType::Null),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(&DataType::Null, &DataType::Null),
            None
        );
        assert_eq!(
            first_struct_name_mismatch(&DataType::Null, &union_with(struct_ab(), struct_ba())),
            None
        );
    }

    #[test]
    fn three_deep_nesting_reports_the_leaf_path() {
        let left = list_of("large", list_of("view", list_of("fsl2", struct_ab())));
        let right = list_of("list", list_of("fsl1", list_of("largeview", struct_ba())));
        assert_eq!(
            first_struct_name_mismatch(&left, &right),
            Some(names("[][][]", &["a", "b"], &["b", "a"]))
        );
        let map_left = map_of(("key", "value"), list_of("fsl1", struct_ab()));
        let map_right = map_of(("key", "value"), list_of("fsl2", struct_ba()));
        assert_eq!(
            first_struct_name_mismatch(&map_left, &map_right),
            Some(names("{value}[]", &["a", "b"], &["b", "a"]))
        );
    }
}
