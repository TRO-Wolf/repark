use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use iceberg::spec::Schema;

use super::*;

fn nanos(zoned: bool) -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, zoned.then(|| Arc::from("UTC")))
}

fn micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
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
    let none = NanosecondAsk::columns(vec![false, false], TEST_REMEDIES);
    let naive = arrow(&[("id", DataType::Int32), ("c", nanos(false))]);
    assert_eq!(
        refusal(&request(&naive, &none, None)),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would create column `c` as \
         timestamp_ns, a type Spark cannot read, and the statement does not ask for it. Ask with \
         CAST(<expression> AS timestamp_ns) on the column (SQL or selectExpr), or set \
         `repark.sql.allowImplicitTimestampNanos` = true on the session. For microseconds write \
         CAST(<expression> AS TIMESTAMP_NTZ)."
    );
    let zoned = arrow(&[("id", DataType::Int32), ("cz", nanos(true))]);
    let evolving = NewColumns {
        form: "MERGE WITH SCHEMA EVOLUTION",
        ..request(&zoned, &none, None)
    };
    assert_eq!(
        refusal(&evolving),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] MERGE WITH SCHEMA EVOLUTION would \
         create column `cz` as timestamptz_ns, a type Spark cannot read, and the statement does \
         not ask for it. Ask with CAST(<expression> AS timestamptz_ns) on the column (SQL or \
         selectExpr), or set `repark.sql.allowImplicitTimestampNanos` = true on the session. For \
         microseconds write CAST(<expression> AS TIMESTAMP)."
    );
}

#[test]
fn an_asked_column_passes() {
    let schema = arrow(&[("c", nanos(false)), ("d", nanos(true))]);
    passes(&request(
        &schema,
        &NanosecondAsk::columns(vec![true, true], TEST_REMEDIES),
        None,
    ));
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
        (
            "e",
            DataType::List(Arc::new(Field::new("element", nanos(false), true))),
        ),
    ]);
    passes(&request(
        &schema,
        &NanosecondAsk::every(TEST_REMEDIES),
        None,
    ));
}

#[test]
fn a_kept_column_passes_without_an_ask() {
    let none = NanosecondAsk::columns(vec![false, false], TEST_REMEDIES);
    let schema = arrow(&[("id", DataType::Int32), ("C", nanos(false))]);
    let held = stored(&[("other", DataType::Utf8), ("c", nanos(false))]);
    passes(&request(&schema, &none, Some(&held)));
    let exact = NewColumns {
        case_sensitive: true,
        ..request(&schema, &none, Some(&held))
    };
    assert!(refusal(&exact).contains("column `C` as timestamp_ns"));
    assert!(refusal(&request(&schema, &none, None)).contains("column `C` as timestamp_ns"));
}

#[test]
fn a_kept_name_with_another_type_is_new() {
    let none = NanosecondAsk::columns(vec![false], TEST_REMEDIES);
    let schema = arrow(&[("c", nanos(false))]);
    for held in [
        stored(&[("c", nanos(true))]),
        stored(&[("c", micros())]),
        stored(&[("d", nanos(false))]),
    ] {
        assert!(refusal(&request(&schema, &none, Some(&held))).contains("`c` as timestamp_ns"));
    }
}

#[test]
fn a_nested_nanosecond_field_is_checked() {
    let none = NanosecondAsk::columns(vec![false, false], TEST_REMEDIES);
    let list = DataType::List(Arc::new(Field::new("element", nanos(false), true)));
    let nested = DataType::Struct(Fields::from(vec![
        Field::new("label", DataType::Utf8, true),
        Field::new("at", nanos(true), true),
        Field::new("wall", nanos(false), true),
    ]));
    let listed = arrow(&[("id", DataType::Int32), ("a", list.clone())]);
    assert!(refusal(&request(&listed, &none, None)).contains("column `a` as timestamp_ns,"));
    let structured = arrow(&[("id", DataType::Int32), ("s", nested.clone())]);
    let text = refusal(&request(&structured, &none, None));
    assert!(text.contains("column `s` as timestamptz_ns,"), "{text}");
    assert!(text.contains("CAST(<expression> AS TIMESTAMP)."), "{text}");
    let held = stored(&[("pad", DataType::Int64), ("a", list), ("s", nested)]);
    passes(&request(&listed, &none, Some(&held)));
    passes(&request(&structured, &none, Some(&held)));
    let other = DataType::Struct(Fields::from(vec![Field::new("at", nanos(true), true)]));
    let reshaped = stored(&[("s", other)]);
    assert!(refusal(&request(&structured, &none, Some(&reshaped))).contains("column `s`"));
}

#[test]
fn a_schema_with_no_nanosecond_equals_the_old_conversion() {
    let none = NanosecondAsk::columns(Vec::new(), TEST_REMEDIES);
    let plain = arrow(&[
        ("id", DataType::Int32),
        ("t", micros()),
        ("void", DataType::Null),
        (
            "l",
            DataType::List(Arc::new(Field::new("element", micros(), true))),
        ),
    ]);
    assert_eq!(
        new_table_schema(&request(&plain, &none, None)).unwrap(),
        arrow_schema_to_iceberg_with_unknown(&plain).unwrap()
    );
    let unsupported = arrow(&[("c", nanos(false)), ("u", DataType::UInt64)]);
    for ask in [none, NanosecondAsk::every(TEST_REMEDIES)] {
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
    passes(&request(
        &typed,
        &NanosecondAsk::columns(Vec::new(), TEST_REMEDIES),
        None,
    ));
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

#[test]
fn the_text_names_only_the_remedies_the_seat_can_write() {
    let seat = Remedies {
        nanosecond_cast: false,
        naive_microseconds: "TIMESTAMP(6)",
        zoned_microseconds: "TIMESTAMP(6) WITH TIME ZONE",
    };
    let none = NanosecondAsk::columns(vec![false], seat);
    let naive = arrow(&[("c", nanos(false))]);
    assert_eq!(
        refusal(&request(&naive, &none, None)),
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would create column `c` as \
         timestamp_ns, a type Spark cannot read, and the statement does not ask for it. Set \
         `repark.sql.allowImplicitTimestampNanos` = true on the session. For microseconds write \
         CAST(<expression> AS TIMESTAMP(6))."
    );
    let zoned = arrow(&[("cz", nanos(true))]);
    let text = refusal(&request(&zoned, &none, None));
    assert!(
        text.ends_with("CAST(<expression> AS TIMESTAMP(6) WITH TIME ZONE)."),
        "{text}"
    );
    assert!(!text.contains("Ask with"), "{text}");
    passes(&request(&naive, &NanosecondAsk::every(seat), None));
}
