use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit, UnionFields, UnionMode};
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::{
    Feed, InsertSupply, NaiveLeaf, Supply, assigned, holds_zoned_timestamp, insert_supply,
    lands_on_naive_leaf, merged, nested_naive_leaf, reaches_leaf,
};

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

fn record(name: &str, data_type: DataType) -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("n", DataType::Int32, true),
        Field::new(name, data_type, true),
    ]))
}

fn map(key: DataType, value: DataType) -> DataType {
    let entries = DataType::Struct(Fields::from(vec![
        Field::new("key", key, false),
        Field::new("value", value, true),
    ]));
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

#[test]
fn every_container_kind_is_searched_for_a_naive_nanosecond_leaf() {
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let found: [(DataType, &[&str]); 12] = [
        (record("v", nanos.clone()), &["v"]),
        (record("i", record("v", nanos.clone())), &["i", "v"]),
        (DataType::List(item(nanos.clone())), &["element"]),
        (DataType::LargeList(item(nanos.clone())), &["element"]),
        (DataType::ListView(item(nanos.clone())), &["element"]),
        (DataType::LargeListView(item(nanos.clone())), &["element"]),
        (
            DataType::FixedSizeList(item(nanos.clone()), 2),
            &["element"],
        ),
        (
            DataType::List(item(record("v", nanos.clone()))),
            &["element", "v"],
        ),
        (
            record("a", DataType::List(item(nanos.clone()))),
            &["a", "element"],
        ),
        (map(DataType::Utf8, nanos.clone()), &["value"]),
        (map(nanos.clone(), DataType::Int32), &["key"]),
        (
            map(DataType::Utf8, record("v", nanos.clone())),
            &["value", "v"],
        ),
    ];
    for (data_type, path) in found {
        let leaf = nested_naive_leaf(&data_type, NaiveLeaf::Nanos);
        let expected: Vec<String> = path.iter().map(ToString::to_string).collect();
        assert_eq!(leaf, Some(expected), "{data_type}");
    }
    let zoned = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    for data_type in [
        nanos,
        record("v", zoned.clone()),
        record("v", micros.clone()),
        DataType::List(item(zoned.clone())),
        DataType::List(item(micros.clone())),
        map(zoned, micros),
        DataType::Int64,
    ] {
        assert_eq!(
            nested_naive_leaf(&data_type, NaiveLeaf::Nanos),
            None,
            "{data_type}"
        );
    }
}

fn statement(sql: &str) -> Statement {
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).expect(sql);
    parsed.remove(0)
}

const FIELDS: [&str; 3] = ["id", "st", "k"];

fn insert(sql: &str, by_name: bool) -> Supply {
    let Statement::Insert(insert) = statement(sql) else {
        panic!("not an insert: {sql}");
    };
    let listed: Vec<String> = insert.columns.iter().map(ToString::to_string).collect();
    let supply = InsertSupply {
        listed: &listed,
        source: insert.source.as_deref(),
        partition: &[],
        positional: !by_name,
        by_name,
    };
    insert_supply(&supply, Some(&[]), &FIELDS, "st").supply
}

#[test]
fn an_insert_supplies_the_column_unless_it_omits_it_or_gives_a_bare_null() {
    for (sql, by_name, expected) in [
        ("INSERT INTO t VALUES (1, x, 0)", false, Supply::Value),
        ("INSERT INTO t VALUES (1, NULL, 0)", false, Supply::Null),
        (
            "INSERT INTO t VALUES (1, NULL, 0), (2, x, 0)",
            false,
            Supply::Value,
        ),
        (
            "INSERT INTO t VALUES (1, (NULL), 0), (2, NULL, 0)",
            false,
            Supply::Null,
        ),
        (
            "INSERT INTO t VALUES (1, CAST(NULL AS INT), 0)",
            false,
            Supply::Value,
        ),
        ("INSERT INTO t (id, k) VALUES (1, 0)", false, Supply::Absent),
        ("INSERT INTO t (id, ST) VALUES (1, x)", false, Supply::Value),
        (
            "INSERT INTO t (k, st) SELECT 1, NULL FROM s",
            false,
            Supply::Null,
        ),
        ("INSERT INTO t SELECT id, v, 0 FROM s", false, Supply::Value),
        (
            "INSERT INTO t SELECT id, NULL, 0 FROM s",
            false,
            Supply::Null,
        ),
        ("INSERT INTO t SELECT * FROM s", false, Supply::Value),
        ("INSERT INTO t SELECT id FROM s", false, Supply::Value),
        (
            "INSERT INTO t SELECT a, b, c FROM s UNION ALL SELECT a, b, c FROM s",
            false,
            Supply::Value,
        ),
        ("INSERT INTO t TABLE s", false, Supply::Value),
        (
            "INSERT INTO t SELECT 0 AS k, id FROM s",
            true,
            Supply::Absent,
        ),
        (
            "INSERT INTO t SELECT 0 AS k, NULL AS st, id FROM s",
            true,
            Supply::Null,
        ),
        (
            "INSERT INTO t SELECT 0 AS k, v AS St, id FROM s",
            true,
            Supply::Value,
        ),
        (
            "INSERT INTO t SELECT 0 AS k, s.st, id FROM s",
            true,
            Supply::Value,
        ),
        (
            "INSERT INTO t SELECT 0 AS k, f(v), id FROM s",
            true,
            Supply::Value,
        ),
        ("INSERT INTO t SELECT * FROM s", true, Supply::Value),
    ] {
        assert_eq!(insert(sql, by_name), expected, "{sql}");
    }
}

#[test]
fn an_unresolved_partition_clause_or_a_frame_written_by_name_is_read_as_a_value() {
    let Statement::Insert(parsed) = statement("INSERT INTO t SELECT id, NULL, 0 FROM s") else {
        panic!("not an insert");
    };
    let Statement::Insert(fixed) = statement("INSERT INTO t SELECT NULL, 0 FROM s") else {
        panic!("not an insert");
    };
    let supply = |statics: Option<&[String]>, positional, by_name| {
        let supply = InsertSupply {
            listed: &[],
            source: parsed.source.as_deref(),
            partition: &[],
            positional,
            by_name,
        };
        insert_supply(&supply, statics, &FIELDS, "st").supply
    };
    assert_eq!(supply(Some(&[]), true, false), Supply::Null);
    assert_eq!(supply(None, true, false), Supply::Value);
    assert_eq!(supply(Some(&[]), true, true), Supply::Value);
    let statics = ["ID".to_string()];
    let partitioned = InsertSupply {
        listed: &[],
        source: fixed.source.as_deref(),
        partition: &[],
        positional: true,
        by_name: false,
    };
    assert_eq!(
        insert_supply(&partitioned, Some(&statics), &FIELDS, "st").supply,
        Supply::Null
    );
    assert_eq!(
        insert_supply(&partitioned, Some(&[]), &FIELDS, "st").supply,
        Supply::Value
    );
    let absent = InsertSupply {
        listed: &[],
        source: None,
        partition: &[],
        positional: true,
        by_name: false,
    };
    assert_eq!(
        insert_supply(&absent, Some(&[]), &FIELDS, "st").supply,
        Supply::Value
    );
}

#[test]
fn an_assignment_supplies_the_column_when_any_part_of_its_path_names_it() {
    for (sql, expected) in [
        ("UPDATE t SET k = 1", Supply::Absent),
        ("UPDATE t SET st = x", Supply::Value),
        ("UPDATE t SET k = 1, ST = x", Supply::Value),
        ("UPDATE t SET st = NULL", Supply::Null),
        ("UPDATE t SET t.st = NULL", Supply::Null),
        ("UPDATE t SET st.v = NULL", Supply::Value),
        ("UPDATE t SET st.v = x", Supply::Value),
        ("UPDATE t SET t.st.v = x", Supply::Value),
        ("UPDATE t SET (id, st) = (1, x)", Supply::Value),
        (
            "UPDATE t SET __repark_merge_star_sentinel__ = 1",
            Supply::Value,
        ),
    ] {
        let Statement::Update(update) = statement(sql) else {
            panic!("not an update: {sql}");
        };
        assert_eq!(
            assigned(&update.assignments, "st").supply,
            expected,
            "{sql}"
        );
    }
    assert_eq!(assigned(&[], "st").supply, Supply::Value);
}

#[test]
fn a_merge_supplies_the_column_through_any_one_clause() {
    let on = "MERGE INTO t USING s ON t.id = s.id";
    for (clauses, expected) in [
        ("WHEN MATCHED THEN DELETE", Supply::Absent),
        ("WHEN MATCHED THEN UPDATE SET k = 1", Supply::Absent),
        ("WHEN MATCHED THEN UPDATE SET st = s.v", Supply::Value),
        ("WHEN MATCHED THEN UPDATE SET t.st.v = s.v", Supply::Value),
        ("WHEN MATCHED THEN UPDATE SET st = NULL", Supply::Null),
        (
            "WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, 0)",
            Supply::Absent,
        ),
        (
            "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
            Supply::Value,
        ),
        (
            "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, NULL)",
            Supply::Null,
        ),
        (
            "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, 0)",
            Supply::Value,
        ),
        (
            "WHEN NOT MATCHED THEN INSERT VALUES (s.id, NULL, 0)",
            Supply::Null,
        ),
        (
            "WHEN NOT MATCHED THEN INSERT (__repark_merge_star_sentinel__) VALUES (1)",
            Supply::Value,
        ),
        (
            "WHEN MATCHED THEN UPDATE SET k = 1 \
             WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
            Supply::Value,
        ),
        (
            "WHEN MATCHED THEN UPDATE SET st = NULL \
             WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, 0)",
            Supply::Null,
        ),
    ] {
        let sql = format!("{on} {clauses}");
        let Statement::Merge(merge) = statement(&sql) else {
            panic!("not a merge: {sql}");
        };
        assert_eq!(
            merged(&merge.clauses, &FIELDS, "st").supply,
            expected,
            "{sql}"
        );
    }
}

#[test]
fn the_microsecond_walk_finds_only_a_naive_microsecond_leaf_below_the_top() {
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
    let found: [(DataType, &[&str]); 6] = [
        (record("v", micros.clone()), &["v"]),
        (record("i", record("v", micros.clone())), &["i", "v"]),
        (DataType::LargeList(item(micros.clone())), &["element"]),
        (
            DataType::FixedSizeList(item(record("v", micros.clone())), 2),
            &["element", "v"],
        ),
        (map(DataType::Utf8, micros.clone()), &["value"]),
        (map(micros.clone(), DataType::Int32), &["key"]),
    ];
    for (data_type, path) in found {
        let expected: Vec<String> = path.iter().map(ToString::to_string).collect();
        assert_eq!(
            nested_naive_leaf(&data_type, NaiveLeaf::Micros),
            Some(expected),
            "{data_type}"
        );
        assert_eq!(
            nested_naive_leaf(&data_type, NaiveLeaf::Nanos),
            None,
            "{data_type}"
        );
    }
    for data_type in [
        micros,
        record("v", nanos.clone()),
        record("v", zoned.clone()),
        DataType::List(item(zoned.clone())),
        map(zoned, nanos),
        DataType::Utf8,
    ] {
        assert_eq!(
            nested_naive_leaf(&data_type, NaiveLeaf::Micros),
            None,
            "{data_type}"
        );
    }
}

#[test]
fn a_zoned_timestamp_is_found_at_any_depth_and_in_any_layout() {
    let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let union = |data_type: DataType| {
        DataType::Union(
            UnionFields::try_new(vec![0], vec![Field::new("u", data_type, true)]).expect("union"),
            UnionMode::Sparse,
        )
    };
    let run_ends = |data_type: DataType| {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", data_type, true)),
        )
    };
    for (data_type, expected) in [
        (zoned.clone(), true),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+05:30"))),
            true,
        ),
        (record("v", zoned.clone()), true),
        (record("i", record("v", zoned.clone())), true),
        (DataType::ListView(item(zoned.clone())), true),
        (map(DataType::Utf8, zoned.clone()), true),
        (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(zoned.clone())),
            true,
        ),
        (run_ends(zoned.clone()), true),
        (union(zoned), true),
        (naive.clone(), false),
        (record("v", naive.clone()), false),
        (DataType::List(item(naive.clone())), false),
        (run_ends(naive.clone()), false),
        (union(naive), false),
        (DataType::Utf8, false),
        (DataType::Date32, false),
    ] {
        assert_eq!(holds_zoned_timestamp(&data_type), expected, "{data_type}");
    }
}

#[test]
fn a_field_path_reaches_the_leaf_only_through_a_field_that_holds_it() {
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    let column = DataType::Struct(Fields::from(vec![
        Field::new("v", micros.clone(), true),
        Field::new("n", DataType::Int32, true),
        Field::new("i", record("w", micros), true),
    ]));
    let path = |steps: &[&str]| steps.iter().map(ToString::to_string).collect::<Vec<_>>();
    for (steps, expected) in [
        (&[][..], true),
        (&["v"][..], true),
        (&["V"][..], true),
        (&["i"][..], true),
        (&["i", "w"][..], true),
        (&["n"][..], false),
        (&["i", "n"][..], false),
        (&["missing"][..], true),
        (&["v", "deeper"][..], true),
    ] {
        assert_eq!(
            reaches_leaf(&column, &path(steps), NaiveLeaf::Micros),
            expected,
            "{steps:?}"
        );
    }
    for (steps, expected) in [
        (&["v"][..], true),
        (&["I", "W"][..], true),
        (&[][..], false),
        (&["i"][..], false),
        (&["n"][..], false),
        (&["missing"][..], false),
    ] {
        assert_eq!(
            lands_on_naive_leaf(&column, &path(steps), NaiveLeaf::Micros),
            expected,
            "{steps:?}"
        );
    }
}

fn shown(feeds: &[Feed<'_>]) -> Vec<String> {
    feeds
        .iter()
        .map(|feed| match feed {
            Feed::Expr { value, path } => format!("expr {value} {path:?}"),
            Feed::SourceColumn { position, name } => format!("column {position:?} {name:?}"),
            Feed::SourceStar => "star".to_string(),
            Feed::Opaque => "opaque".to_string(),
        })
        .collect()
}

#[test]
fn an_update_or_merge_names_each_value_that_reaches_the_column() {
    for (sql, expected) in [
        ("UPDATE t SET k = 1", vec![]),
        ("UPDATE t SET st = NULL", vec![]),
        ("UPDATE t SET st.v = x", vec!["expr x [\"v\"]"]),
        ("UPDATE t SET t.st.v = x", vec!["expr x [\"v\"]"]),
        ("UPDATE t SET st = f(x)", vec!["expr f(x) []"]),
        (
            "UPDATE t SET st.n = 1, st.v = x",
            vec!["expr 1 [\"n\"]", "expr x [\"v\"]"],
        ),
        ("UPDATE t SET (id, st) = (1, x)", vec!["opaque"]),
        (
            "UPDATE t SET __repark_merge_star_sentinel__ = 1",
            vec!["star"],
        ),
    ] {
        let Statement::Update(update) = statement(sql) else {
            panic!("not an update: {sql}");
        };
        assert_eq!(
            shown(&assigned(&update.assignments, "st").feeds),
            expected,
            "{sql}"
        );
    }
    assert_eq!(shown(&assigned(&[], "st").feeds), vec!["opaque"]);
    let on = "MERGE INTO t USING s ON t.id = s.id";
    for (clauses, expected) in [
        ("WHEN MATCHED THEN DELETE", vec![]),
        (
            "WHEN MATCHED THEN UPDATE SET t.st = s.v",
            vec!["expr s.v []"],
        ),
        (
            "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
            vec!["expr s.v []"],
        ),
        (
            "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, 0)",
            vec!["expr s.v []"],
        ),
        (
            "WHEN NOT MATCHED THEN INSERT (__repark_merge_star_sentinel__) VALUES (1)",
            vec!["star"],
        ),
        (
            "WHEN MATCHED THEN UPDATE SET st.v = s.x \
             WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
            vec!["expr s.x [\"v\"]", "expr s.v []"],
        ),
        (
            "WHEN MATCHED THEN UPDATE SET st = NULL \
             WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
            vec!["expr s.v []"],
        ),
    ] {
        let sql = format!("{on} {clauses}");
        let Statement::Merge(merge) = statement(&sql) else {
            panic!("not a merge: {sql}");
        };
        assert_eq!(
            shown(&merged(&merge.clauses, &FIELDS, "st").feeds),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn an_insert_names_the_source_column_by_position_by_name_or_not_at_all() {
    let Statement::Insert(parsed) = statement("INSERT INTO t SELECT id, v, 0 FROM s") else {
        panic!("not an insert");
    };
    let statics = ["Id".to_string()];
    let feeds = |statics: Option<&[String]>, positional, by_name| {
        let supply = InsertSupply {
            listed: &[],
            source: parsed.source.as_deref(),
            partition: &[],
            positional,
            by_name,
        };
        shown(&insert_supply(&supply, statics, &FIELDS, "st").feeds)
    };
    assert_eq!(feeds(Some(&[]), true, false), vec!["column Some(1) None"]);
    assert_eq!(
        feeds(Some(&statics), true, false),
        vec!["column Some(0) None"]
    );
    assert_eq!(feeds(None, true, false), vec!["column None None"]);
    assert_eq!(
        feeds(Some(&[]), true, true),
        vec!["column Some(1) None", "column None Some(\"st\")"]
    );
    let Statement::Insert(named) = statement("INSERT INTO t SELECT 0 AS k, v AS st FROM s") else {
        panic!("not an insert");
    };
    let supply = InsertSupply {
        listed: &[],
        source: named.source.as_deref(),
        partition: &[],
        positional: false,
        by_name: true,
    };
    assert_eq!(
        shown(&insert_supply(&supply, Some(&[]), &FIELDS, "st").feeds),
        vec!["column None Some(\"st\")"]
    );
}
