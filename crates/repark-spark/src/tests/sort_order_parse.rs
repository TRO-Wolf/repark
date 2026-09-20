use crate::sort_order_parse::{
    OrderParseError, ZOrderScan, parse_identity_sort_order, parse_zorder_columns,
};
use iceberg::spec::{NullOrder, SortDirection};

fn parsed(text: &str) -> Vec<(String, SortDirection, NullOrder)> {
    parse_identity_sort_order(text)
        .expect("must parse")
        .into_iter()
        .map(|field| (field.name, field.direction, field.null_order))
        .collect()
}

fn refusal(text: &str) -> OrderParseError {
    parse_identity_sort_order(text).expect_err("must refuse")
}

#[test]
fn bare_direction_picks_javas_tied_null_order() {
    assert_eq!(
        parsed("id"),
        vec![("id".into(), SortDirection::Ascending, NullOrder::First)]
    );
    assert_eq!(
        parsed("id ASC"),
        vec![("id".into(), SortDirection::Ascending, NullOrder::First)]
    );
    assert_eq!(
        parsed("id DESC"),
        vec![("id".into(), SortDirection::Descending, NullOrder::Last)]
    );
}

#[test]
fn explicit_null_order_overrides_the_tied_default() {
    assert_eq!(
        parsed("id ASC NULLS LAST"),
        vec![("id".into(), SortDirection::Ascending, NullOrder::Last)]
    );
    assert_eq!(
        parsed("id DESC NULLS FIRST"),
        vec![("id".into(), SortDirection::Descending, NullOrder::First)]
    );
    assert_eq!(
        parsed("id desc nulls last"),
        vec![("id".into(), SortDirection::Descending, NullOrder::Last)]
    );
}

#[test]
fn a_list_keeps_its_order_and_dotted_names() {
    assert_eq!(
        parsed("st.a DESC, id, b ASC NULLS LAST"),
        vec![
            ("st.a".into(), SortDirection::Descending, NullOrder::Last),
            ("id".into(), SortDirection::Ascending, NullOrder::First),
            ("b".into(), SortDirection::Ascending, NullOrder::Last),
        ]
    );
}

#[test]
fn malformed_terms_are_classified_for_the_door_to_render() {
    assert_eq!(refusal(""), OrderParseError::Empty);
    assert_eq!(refusal("'id'"), OrderParseError::QuotedName);
    assert_eq!(refusal("(id)"), OrderParseError::MissingName);
    assert_eq!(
        refusal("bucket(4, id)"),
        OrderParseError::Transform {
            name: "bucket".into()
        }
    );
    assert_eq!(
        refusal("id NULLS SIDEWAYS"),
        OrderParseError::BadNulls {
            name: "id".into(),
            got: "SIDEWAYS".into()
        }
    );
    assert_eq!(
        refusal("id DESC junk"),
        OrderParseError::Trailing {
            name: "id".into(),
            got: "junk".into()
        }
    );
}

#[test]
fn zorder_is_recognised_in_every_spelling() {
    for text in [
        "zorder(id, data)",
        "ZORDER(id, data)",
        "  ZOrder ( id , data )  ",
    ] {
        assert_eq!(
            parse_zorder_columns(text),
            ZOrderScan::Columns(vec!["id".into(), "data".into()]),
            "{text}"
        );
    }
    assert_eq!(
        parse_zorder_columns("zorder()"),
        ZOrderScan::Columns(vec![])
    );
}

#[test]
fn zorder_scan_separates_absent_from_mixed() {
    assert_eq!(parse_zorder_columns("id DESC"), ZOrderScan::Absent);
    assert_eq!(parse_zorder_columns("zorder"), ZOrderScan::Absent);
    assert_eq!(parse_zorder_columns("bucket(4, id)"), ZOrderScan::Absent);
    assert_eq!(parse_zorder_columns("id, zorder(data)"), ZOrderScan::Mixed);
    assert_eq!(parse_zorder_columns("zorder(data), id"), ZOrderScan::Mixed);
    assert_eq!(
        parse_zorder_columns("zorder(a), zorder(b)"),
        ZOrderScan::Columns(vec!["a".into(), "b".into()])
    );
}
