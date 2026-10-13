use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use iceberg::spec::Schema;

use super::*;

const NAIVE_TEXT: &str = "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would \
     create column `c` as timestamp_ns, a type Spark cannot read, and the statement does not ask \
     for it. Ask with CAST(<expression> AS timestamp_ns) on the column (SQL or selectExpr), or \
     turn the setting on. For microseconds write CAST(<expression> AS TIMESTAMP_NTZ).";

fn nanos(zoned: bool) -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, zoned.then(|| Arc::from("UTC")))
}

fn micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn list(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("element", element, true)))
}

fn structure(fields: &[(&str, DataType)]) -> DataType {
    DataType::Struct(Fields::from(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn map(value: DataType) -> DataType {
    let entries = DataType::Struct(Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", value, true),
    ]));
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

fn arrow(fields: &[(&str, DataType)]) -> ArrowSchema {
    ArrowSchema::new(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    )
}

fn stored(fields: &[(&str, DataType)]) -> Schema {
    incoming_schema(&arrow(fields)).unwrap()
}

fn none(columns: usize) -> NanosecondAsk {
    NanosecondAsk::columns(vec![false; columns], TEST_REMEDIES)
}

fn request<'a>(
    arrow: &'a ArrowSchema,
    ask: &'a NanosecondAsk,
    existing: Option<&'a Schema>,
) -> NewColumns<'a> {
    NewColumns {
        arrow,
        ask,
        existing,
        form: "CTAS",
        case_sensitive: false,
        target: Target::FormatV3,
    }
}

fn refusal(request: &NewColumns<'_>) -> String {
    let created = new_table_schema(request).unwrap_err().to_string();
    let added = added_columns_schema(request).unwrap_err().to_string();
    assert_eq!(created, added);
    created
}

fn passes(request: &NewColumns<'_>) {
    assert_eq!(
        new_table_schema(request).unwrap(),
        arrow_schema_to_iceberg_with_unknown(request.arrow).unwrap()
    );
    assert_eq!(
        added_columns_schema(request).unwrap(),
        incoming_schema(request.arrow).unwrap()
    );
}

#[test]
fn a_new_unasked_nanosecond_column_is_refused_and_the_text_names_both_opt_ins() {
    let naive = arrow(&[("id", DataType::Int32), ("c", nanos(false))]);
    assert_eq!(refusal(&request(&naive, &none(2), None)), NAIVE_TEXT);
    let zoned = arrow(&[("id", DataType::Int32), ("cz", nanos(true))]);
    let ask = none(2);
    let evolving = NewColumns {
        form: "MERGE WITH SCHEMA EVOLUTION",
        ..request(&zoned, &ask, None)
    };
    assert_eq!(
        refusal(&evolving),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] MERGE WITH SCHEMA EVOLUTION would \
         create column `cz` as timestamptz_ns, a type Spark cannot read, and the statement does \
         not ask for it. Ask with CAST(<expression> AS timestamptz_ns) on the column (SQL or \
         selectExpr), or turn the setting on. For microseconds write \
         CAST(<expression> AS TIMESTAMP)."
    );
}

#[test]
fn the_text_names_only_the_remedies_the_seat_can_write() {
    let seat = Remedies {
        nanosecond_cast: false,
        naive_microseconds: Some("CAST(<expression> AS TIMESTAMP(6))"),
        zoned_microseconds: None,
        ..TEST_REMEDIES
    };
    let ask = NanosecondAsk::columns(vec![false], seat);
    let naive = arrow(&[("c", nanos(false))]);
    assert_eq!(
        refusal(&request(&naive, &ask, None)),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would create column `c` as \
         timestamp_ns, a type Spark cannot read, and the statement does not ask for it. To ask, \
         turn the setting on. For microseconds write CAST(<expression> AS TIMESTAMP(6))."
    );
    let zoned = arrow(&[("cz", nanos(true))]);
    assert_eq!(
        refusal(&request(&zoned, &ask, None)),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would create column `cz` as \
         timestamptz_ns, a type Spark cannot read, and the statement does not ask for it. To \
         ask, turn the setting on."
    );
    passes(&request(&naive, &NanosecondAsk::every(seat), None));
}

#[test]
fn below_format_v3_the_text_names_the_seat_s_own_way_to_a_v3_table() {
    let schema = arrow(&[("c", nanos(false))]);
    let asked = NanosecondAsk::every(TEST_REMEDIES);
    let unasked = none(1);
    for (target, remedy) in [
        (Target::CreatedBelowV3, "Create it as v3."),
        (Target::ReplacedBelowV3, "Replace it as v3."),
        (Target::EvolvedBelowV3, "Upgrade it to v3."),
    ] {
        let asked_below = NewColumns {
            target,
            ..request(&schema, &asked, None)
        };
        assert_eq!(
            refusal(&asked_below),
            format!(
                "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for c: \
                 timestamp_ns is not supported until v3. {remedy}"
            )
        );
        let unasked_below = NewColumns {
            target,
            ..request(&schema, &unasked, None)
        };
        assert_eq!(
            refusal(&unasked_below),
            NAIVE_TEXT.replace(
                " For microseconds",
                &format!(
                    " A nanosecond column also needs a format version 3 table. {remedy} For \
                     microseconds"
                )
            )
        );
    }
    let plain = arrow(&[("t", micros())]);
    let below = NewColumns {
        target: Target::CreatedBelowV3,
        ..request(&plain, &unasked, None)
    };
    passes(&below);
    assert_eq!(Target::of(true, Target::CreatedBelowV3), Target::FormatV3);
    assert_eq!(
        Target::of(false, Target::EvolvedBelowV3),
        Target::EvolvedBelowV3
    );
}

#[test]
fn an_asked_column_passes() {
    let schema = arrow(&[("c", nanos(false)), ("d", nanos(true))]);
    let both = NanosecondAsk::columns(vec![true, true], TEST_REMEDIES);
    passes(&request(&schema, &both, None));
    let second_only = NanosecondAsk::columns(vec![false, true], TEST_REMEDIES);
    assert!(refusal(&request(&schema, &second_only, None)).contains("column `c` as timestamp_ns"));
    let first_only = NanosecondAsk::columns(vec![true], TEST_REMEDIES);
    assert!(refusal(&request(&schema, &first_only, None)).contains("column `d` as timestamptz_ns"));
}

#[test]
fn every_passes_all() {
    let schema = arrow(&[
        ("c", nanos(false)),
        ("d", nanos(true)),
        ("e", list(nanos(false))),
    ]);
    passes(&request(
        &schema,
        &NanosecondAsk::every(TEST_REMEDIES),
        None,
    ));
}

#[test]
fn a_kept_column_passes_without_an_ask() {
    let ask = none(2);
    let schema = arrow(&[("id", DataType::Int32), ("C", nanos(false))]);
    let held = stored(&[("other", DataType::Utf8), ("c", nanos(false))]);
    passes(&request(&schema, &ask, Some(&held)));
    let exact = NewColumns {
        case_sensitive: true,
        ..request(&schema, &ask, Some(&held))
    };
    assert!(refusal(&exact).contains("column `C` as timestamp_ns"));
    assert!(refusal(&request(&schema, &ask, None)).contains("column `C` as timestamp_ns"));
}

#[test]
fn a_kept_name_with_another_type_is_new_to_a_replaced_table_and_not_added_by_an_evolution() {
    let ask = none(1);
    let schema = arrow(&[("c", nanos(false))]);
    for held in [stored(&[("c", nanos(true))]), stored(&[("c", micros())])] {
        let request = request(&schema, &ask, Some(&held));
        let replaced = new_table_schema(&request).unwrap_err().to_string();
        assert!(replaced.contains("`c` as timestamp_ns"), "{replaced}");
        assert_eq!(
            added_columns_schema(&request).unwrap(),
            incoming_schema(&schema).unwrap()
        );
    }
    let other_name = stored(&[("d", nanos(false))]);
    assert!(refusal(&request(&schema, &ask, Some(&other_name))).contains("`c` as timestamp_ns"));
    let exact = NewColumns {
        case_sensitive: true,
        ..request(&schema, &ask, Some(&other_name))
    };
    let upper = stored(&[("C", micros())]);
    let cased = NewColumns {
        existing: Some(&upper),
        ..exact
    };
    assert!(added_columns_schema(&cased).is_err());
}

#[test]
fn a_nested_nanosecond_field_is_checked() {
    let ask = none(2);
    let nested = structure(&[
        ("label", DataType::Utf8),
        ("at", nanos(true)),
        ("wall", nanos(false)),
    ]);
    let listed = arrow(&[("id", DataType::Int32), ("a", list(nanos(false)))]);
    assert!(refusal(&request(&listed, &ask, None)).contains("column `a` as timestamp_ns,"));
    let structured = arrow(&[("id", DataType::Int32), ("s", nested.clone())]);
    let text = refusal(&request(&structured, &ask, None));
    assert!(text.contains("column `s` as timestamptz_ns,"), "{text}");
    assert!(text.ends_with("CAST(<expression> AS TIMESTAMP)."), "{text}");
    let held = stored(&[
        ("pad", DataType::Int64),
        ("a", list(nanos(false))),
        ("s", nested),
    ]);
    passes(&request(&listed, &ask, Some(&held)));
    passes(&request(&structured, &ask, Some(&held)));
    let reshaped = stored(&[("s", structure(&[("at", nanos(true))]))]);
    let replaced = new_table_schema(&request(&structured, &ask, Some(&reshaped)));
    assert!(replaced.unwrap_err().to_string().contains("column `s`"));
}

#[test]
fn a_nanosecond_field_two_and_three_levels_down_is_checked() {
    let ask = none(2);
    let leaf = structure(&[("n", DataType::Int32), ("at", nanos(false))]);
    for (name, deep) in [
        ("array of struct", list(leaf.clone())),
        ("struct of struct", structure(&[("inner", leaf.clone())])),
        (
            "struct of array of struct",
            structure(&[("items", list(leaf.clone()))]),
        ),
        ("map value struct", map(leaf.clone())),
        ("map of map", map(map(nanos(false)))),
        ("array of array", list(list(nanos(false)))),
    ] {
        let schema = arrow(&[("id", DataType::Int32), ("deep", deep.clone())]);
        let text = refusal(&request(&schema, &ask, None));
        assert!(
            text.contains("column `deep` as timestamp_ns,"),
            "{name}: {text}"
        );
        let held = stored(&[("deep", deep)]);
        passes(&request(&schema, &ask, Some(&held)));
        let other = stored(&[("deep", list(structure(&[("at", nanos(true))])))]);
        let replaced = new_table_schema(&request(&schema, &ask, Some(&other)));
        assert!(replaced.is_err(), "{name}");
    }
    let zoned_deep = arrow(&[(
        "deep",
        structure(&[("items", list(structure(&[("at", nanos(true))])))]),
    )]);
    let text = refusal(&request(&zoned_deep, &none(1), None));
    assert!(text.contains("column `deep` as timestamptz_ns,"), "{text}");
}

#[test]
fn a_schema_with_no_nanosecond_equals_the_old_conversion() {
    let plain = arrow(&[
        ("id", DataType::Int32),
        ("t", micros()),
        ("void", DataType::Null),
        ("l", list(micros())),
    ]);
    assert_eq!(
        new_table_schema(&request(&plain, &none(0), None)).unwrap(),
        arrow_schema_to_iceberg_with_unknown(&plain).unwrap()
    );
    let unsupported = arrow(&[("c", nanos(false)), ("u", DataType::UInt64)]);
    for ask in [none(0), NanosecondAsk::every(TEST_REMEDIES)] {
        assert_eq!(
            new_table_schema(&request(&unsupported, &ask, None))
                .unwrap_err()
                .to_string(),
            iceberg_err(arrow_schema_to_iceberg_with_unknown(&unsupported).unwrap_err())
                .to_string()
        );
        assert_eq!(
            added_columns_schema(&request(&unsupported, &ask, None))
                .unwrap_err()
                .to_string(),
            incoming_schema(&unsupported).unwrap_err().to_string()
        );
    }
    let typed = arrow(&[("id", DataType::Int32), ("t", micros())]);
    passes(&request(&typed, &none(0), None));
}

#[test]
fn the_first_refused_column_in_schema_order_is_named() {
    let schema = arrow(&[
        ("kept", nanos(false)),
        ("asked", nanos(true)),
        ("first", nanos(true)),
        ("second", nanos(false)),
    ]);
    let held = stored(&[("kept", nanos(false))]);
    let ask = NanosecondAsk::columns(vec![false, true, false, false], TEST_REMEDIES);
    let text = refusal(&request(&schema, &ask, Some(&held)));
    assert!(text.contains("column `first` as timestamptz_ns"), "{text}");
    assert!(!text.contains("`second`"), "{text}");
}
