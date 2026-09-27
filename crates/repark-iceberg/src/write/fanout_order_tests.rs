use std::collections::HashMap;

use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct};

fn single(value: Option<Literal>) -> Struct {
    Struct::from_iter([value])
}

fn int_keys(values: &[i32]) -> Vec<Struct> {
    values
        .iter()
        .map(|value| single(Some(Literal::int(*value))))
        .collect()
}

fn long_keys(values: &[i64]) -> Vec<Struct> {
    values
        .iter()
        .map(|value| single(Some(Literal::long(*value))))
        .collect()
}

fn string_keys(words: &[&str]) -> Vec<Struct> {
    words
        .iter()
        .map(|word| single(Some(Literal::string(*word))))
        .collect()
}

fn date_keys(days: &[i32]) -> Vec<Struct> {
    days.iter()
        .map(|days| single(Some(Literal::date(*days))))
        .collect()
}

fn files_for(keys: Vec<Struct>) -> Vec<DataFile> {
    keys.into_iter()
        .enumerate()
        .map(|(position, partition)| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("file-{position}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .partition(partition)
                .record_count(1)
                .file_size_in_bytes(1)
                .partition_spec_id(0)
                .build()
                .unwrap()
        })
        .collect()
}

fn positions(files: &[DataFile]) -> Vec<usize> {
    files
        .iter()
        .map(|file| {
            file.file_path()
                .strip_prefix("file-")
                .unwrap()
                .strip_suffix(".parquet")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                + 1
        })
        .collect()
}

fn order_of(keys: Vec<Struct>, shuffle_partitions: u32) -> Vec<usize> {
    positions(&super::spark_fanout_commit_order(
        files_for(keys),
        shuffle_partitions,
    ))
}

#[test]
fn java_struct_hash_matches_java_goldens() {
    let _: &str = "pins: row-lineage-order-1/C-001";
    let strings = [
        ("x", 168_705),
        ("y", 168_706),
        ("", 163_275),
        ("é", 168_818),
        ("😀", 2_106_094),
        ("a", 168_682),
        ("b", 168_683),
        ("c", 168_684),
    ];
    for (value, expected) in strings {
        assert_eq!(
            super::java_struct_hash(&single(Some(Literal::string(value)))),
            Some(expected),
            "{value}"
        );
    }
    assert_eq!(super::java_struct_hash(&single(None)), Some(163_098));
    let ints = [
        (0, 163_098),
        (1, 163_099),
        (6, 163_104),
        (-1, 163_097),
        (2_147_483_647, -2_147_320_551),
    ];
    for (value, expected) in ints {
        assert_eq!(
            super::java_struct_hash(&single(Some(Literal::int(value)))),
            Some(expected),
            "{value}"
        );
    }
    let longs: [(i64, i32); 5] = [
        (0, 163_098),
        (1, 163_099),
        (6, 163_104),
        (-1, 163_098),
        (4_294_967_296, 163_099),
    ];
    for (value, expected) in longs {
        assert_eq!(
            super::java_struct_hash(&single(Some(Literal::long(value)))),
            Some(expected),
            "{value}"
        );
    }
    assert_eq!(
        super::java_struct_hash(&single(Some(Literal::date(0)))),
        Some(163_098)
    );
    assert_eq!(
        super::java_struct_hash(&single(Some(Literal::date(20_722)))),
        Some(183_820)
    );
    assert_eq!(
        super::java_struct_hash(&single(Some(Literal::bool(true)))),
        Some(164_329)
    );
    assert_eq!(
        super::java_struct_hash(&single(Some(Literal::bool(false)))),
        Some(164_335)
    );
    let pairs: [(Vec<Option<Literal>>, i32); 4] = [
        (
            vec![Some(Literal::string("x")), Some(Literal::int(1))],
            6_918_587,
        ),
        (vec![Some(Literal::string("y")), None], 6_918_627),
        (vec![None, Some(Literal::int(6))], 6_688_705),
        (vec![None, None], 6_688_699),
    ];
    for (fields, expected) in pairs {
        assert_eq!(
            super::java_struct_hash(&Struct::from_iter(fields)),
            Some(expected)
        );
    }
}

#[test]
fn spark_murmur3_matches_spark_hash() {
    let _: &str = "pins: row-lineage-order-1/C-002";
    let cases = [
        ("é", 2_119_106_806),
        ("中", -1_211_605_188),
        ("bü", 115_660_468),
        ("c中", -221_272_255),
        ("ab中", -470_445_353),
        ("abcd", -396_302_900),
    ];
    for (value, expected) in cases {
        assert_eq!(
            super::spark_murmur3(&single(Some(Literal::string(value)))),
            Some(expected),
            "{value}"
        );
    }
}

#[test]
fn order_matches_recorded_spark_at_shuffle_4() {
    let _: &str = "pins: row-lineage-order-1/C-003";
    let ints = [
        100, 3, 47, 16, 64, 1, 33, 250, 17, 90, 5, 48, 1000, 31, 2, 80, 65, 7, 129, 18, 40, 11,
        300, 99, 22, 63, 8, 32, 500, 15,
    ];
    let first: [(usize, Vec<usize>); 4] = [
        (12, vec![8, 10, 4, 5, 12, 9, 6, 7, 3, 1, 11, 2]),
        (13, vec![13, 4, 12, 9, 3, 8, 10, 5, 6, 7, 1, 11, 2]),
        (
            24,
            vec![
                13, 21, 18, 23, 22, 4, 12, 16, 9, 3, 20, 8, 10, 5, 17, 6, 7, 19, 14, 1, 11, 15, 24,
                2,
            ],
        ),
        (
            25,
            vec![
                13, 21, 23, 12, 3, 8, 5, 17, 6, 19, 11, 15, 2, 18, 22, 4, 16, 9, 20, 25, 10, 7, 14,
                1, 24,
            ],
        ),
    ];
    for (count, expected) in first {
        assert_eq!(
            order_of(int_keys(&ints[..count]), 4),
            expected,
            "first {count}"
        );
    }
    let longs = [
        8_589_934_597,
        -1,
        4_294_967_296,
        7,
        -4_294_967_297,
        123_456_789_012,
        0,
        42,
    ];
    assert_eq!(order_of(long_keys(&longs), 4), vec![1, 4, 8, 2, 7, 3, 5, 6]);
    assert_eq!(
        order_of(date_keys(&[19_723, 0, 22_080, 10_956, 19_782]), 4),
        vec![5, 4, 1, 3, 2]
    );
    let bools = vec![
        single(Some(Literal::bool(true))),
        single(None),
        single(Some(Literal::bool(false))),
    ];
    assert_eq!(order_of(bools, 4), vec![2, 1, 3]);
    let null_string = vec![
        single(Some(Literal::string("x"))),
        single(None),
        single(Some(Literal::string("y"))),
    ];
    assert_eq!(order_of(null_string, 4), vec![3, 1, 2]);
    assert_eq!(
        order_of(string_keys(&["😀", "a", "é", "zz", "中"]), 4),
        vec![3, 5, 2, 4, 1]
    );
    let pair_keys = [("x", 1), ("y", 1), ("x", 2), ("a1", 7), ("y", 0)]
        .iter()
        .map(|(name, value)| {
            Struct::from_iter([Some(Literal::string(*name)), Some(Literal::int(*value))])
        })
        .collect();
    assert_eq!(order_of(pair_keys, 4), vec![1, 3, 4, 5, 2]);
    assert_eq!(
        order_of(int_keys(&[19_727, 1, 19_723, 22_341]), 4),
        vec![3, 2, 1, 4]
    );
    assert_eq!(order_of(string_keys(&["é", "c中", "bü"]), 4), vec![3, 2, 1]);
}

#[test]
fn ties_follow_the_shuffle_reducer() {
    let _: &str = "pins: row-lineage-order-1/C-004";
    let words = ["br", "aq", "bb", "aa"];
    let cases = [
        (4, vec![2, 3, 1, 4]),
        (7, vec![3, 4, 1, 2]),
        (200, vec![1, 3, 2, 4]),
    ];
    for (shuffle_partitions, expected) in cases {
        assert_eq!(
            order_of(string_keys(&words), shuffle_partitions),
            expected,
            "shuffle {shuffle_partitions}"
        );
    }
}

#[test]
fn shuffle_1_matches_the_registry_rows() {
    let _: &str = "pins: row-lineage-order-1/C-005";
    assert_eq!(order_of(int_keys(&[0, 1, 2, 3, 4]), 1), vec![1, 2, 5, 3, 4]);
    assert_eq!(
        order_of(int_keys(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 1),
        vec![9, 10, 7, 8, 1, 2, 5, 6, 3, 4]
    );
    assert_eq!(
        order_of(string_keys(&["a", "b", "c", "d", "e"]), 1),
        vec![1, 2, 5, 3, 4]
    );
    let grid = [(0, 1), (0, 0), (1, 1), (1, 0), (2, 0)]
        .iter()
        .map(|(left, right)| {
            Struct::from_iter([Some(Literal::int(*left)), Some(Literal::int(*right))])
        })
        .collect();
    assert_eq!(order_of(grid, 1), vec![4, 3, 1, 5, 2]);
    let arrivals: [(Vec<Option<Literal>>, Vec<usize>); 3] = [
        (
            vec![Some(Literal::int(0)), None, Some(Literal::int(1))],
            vec![1, 2, 3],
        ),
        (
            vec![None, Some(Literal::int(0)), Some(Literal::int(1))],
            vec![1, 2, 3],
        ),
        (
            vec![Some(Literal::int(1)), None, Some(Literal::int(0))],
            vec![2, 3, 1],
        ),
    ];
    for (fields, expected) in arrivals {
        let keys = fields.into_iter().map(single).collect();
        assert_eq!(order_of(keys, 1), expected);
    }
}

#[test]
fn falls_back_to_input_order() {
    let _: &str = "pins: row-lineage-order-1/C-006";
    let floats = [2.0, 1.0]
        .iter()
        .map(|value| single(Some(Literal::double(*value))))
        .collect();
    assert_eq!(order_of(floats, 4), vec![1, 2]);
    assert_eq!(
        order_of(int_keys(&[0, 16, 32, 48, 64, 80, 96, 112]), 4),
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    );
    let mut sorted = files_for(string_keys(&["c", "a", "b"]));
    sorted[1] = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("file-1.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(single(Some(Literal::string("a"))))
        .record_count(1)
        .file_size_in_bytes(1)
        .partition_spec_id(0)
        .sort_order_id(1)
        .build()
        .unwrap();
    assert_eq!(
        positions(&super::spark_fanout_commit_order(sorted, 4)),
        vec![1, 2, 3]
    );
    let bare = files_for(vec![Struct::empty(), Struct::empty(), Struct::empty()]);
    assert_eq!(
        positions(&super::spark_fanout_commit_order(bare, 4)),
        vec![1, 2, 3]
    );
    assert_eq!(order_of(string_keys(&["x", "x", "y"]), 4), vec![3, 1, 2]);
}

#[test]
fn shuffle_partitions_conf() {
    let _: &str = "pins: row-lineage-order-1/C-007";
    let absent = HashMap::new();
    assert_eq!(
        super::shuffle_partitions_from_config_map(&absent).unwrap(),
        200
    );
    let set = HashMap::from([("spark.sql.shuffle.partitions".to_string(), "4".to_string())]);
    assert_eq!(super::shuffle_partitions_from_config_map(&set).unwrap(), 4);
    for raw in ["0", "abc"] {
        let bad = HashMap::from([("spark.sql.shuffle.partitions".to_string(), raw.to_string())]);
        let error = super::shuffle_partitions_from_config_map(&bad).unwrap_err();
        assert!(
            error.to_string().contains("spark.sql.shuffle.partitions"),
            "{raw}: {error}"
        );
    }
}
