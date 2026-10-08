use std::collections::BTreeMap;

use repark_common::{Error, ErrorClass};
use repark_connect::{
    ConnectError, MAX_STRIDES, PARTITIONED_READ_ROW, PartitionOptions, PartitionRefusal,
    PartitionSpec, Stride, stride_cuts, strides,
};

const SPARK_GRID: &str =
    include_str!("../../../../python/repark-parity/tests/live_spark/c3_stride_grid.txt");

fn refusal(error: ConnectError) -> PartitionRefusal {
    match error {
        ConnectError::PartitionedRead { refusal } => refusal,
        other => panic!("expected a partition refusal, got {other:?}"),
    }
}

fn numbers(words: &[&str]) -> Vec<i64> {
    words
        .iter()
        .map(|word| word.parse().expect("an i64 in the grid"))
        .collect()
}

#[test]
fn strides_equal_sparks_recorded_grid() {
    let mut triples = 0;
    let mut refused = 0;
    for line in SPARK_GRID.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        let head = numbers(&words[..3]);
        let (lower, upper, count) = (head[0], head[1], head[2]);
        let ours = stride_cuts(lower, upper, count);
        match words[3] {
            "refused" => {
                assert_eq!(
                    refusal(ours.expect_err("Spark refused this triple")),
                    PartitionRefusal::Reversed { lower, upper },
                    "{line}"
                );
                refused += 1;
            }
            "cuts" => assert_eq!(
                ours.expect("Spark planned this triple"),
                numbers(&words[4..])
            ),
            other => panic!("unknown grid verdict {other}"),
        }
        triples += 1;
    }
    assert_eq!(triples, 740);
    assert_eq!(refused, 1);
}

#[test]
fn strides_at_the_i64_bounds_neither_wrap_nor_repeat() {
    let (min, max) = (i64::MIN, i64::MAX);
    assert_eq!(stride_cuts(min, max, 2).expect("planned"), [-1]);
    assert_eq!(
        stride_cuts(min, max, 4).expect("planned"),
        [-4_611_686_018_427_387_903, 0, 4_611_686_018_427_387_903]
    );
    assert_eq!(
        stride_cuts(max - 10, max, 4).expect("planned"),
        [max - 7, max - 5, max - 3]
    );
    assert_eq!(
        stride_cuts(min, min + 10, 4).expect("planned"),
        [min + 3, min + 5, min + 7]
    );
    assert_eq!(stride_cuts(max - 1, max, 2).expect("planned"), [0i64; 0]);
    for count in [2, 3, 5, 7, 16, 64] {
        let cuts = stride_cuts(min, max, count).expect("planned");
        assert_eq!(cuts.len(), usize::try_from(count - 1).expect("count"));
        assert!(cuts.windows(2).all(|pair| pair[0] < pair[1]), "{cuts:?}");
    }
}

#[test]
fn one_stride_means_an_unpartitioned_read() {
    assert!(stride_cuts(5, 5, 4).expect("equal bounds").is_empty());
    assert!(stride_cuts(0, 200, 1).expect("one partition").is_empty());
    assert!(stride_cuts(0, 200, 0).expect("zero partitions").is_empty());
    assert!(
        stride_cuts(0, 200, -1)
            .expect("negative partitions")
            .is_empty()
    );
    assert!(stride_cuts(200, 0, 1).expect("checked first").is_empty());
    assert!(stride_cuts(0, 1, 31).expect("a span of one").is_empty());
    assert!(strides(&[]).is_empty());
}

#[test]
fn more_partitions_than_values_shrink_to_the_span() {
    assert_eq!(stride_cuts(0, 3, 10).expect("planned"), [1, 2]);
    assert_eq!(stride_cuts(0, 2, 2).expect("planned"), [1]);
    assert_eq!(
        stride_cuts(0, 63, 64).expect("planned"),
        (1..=62).collect::<Vec<i64>>()
    );
    assert_eq!(
        stride_cuts(0, 65, 64).expect("planned"),
        (2..=64).collect::<Vec<i64>>()
    );
}

#[test]
fn negative_and_straddling_ranges_follow_spark() {
    assert_eq!(stride_cuts(-100, -10, 3).expect("planned"), [-70, -40]);
    assert_eq!(stride_cuts(-100, 100, 4).expect("planned"), [-50, 0, 50]);
    assert_eq!(stride_cuts(-7, 5, 5).expect("planned"), [-4, -2, 0, 2]);
    assert_eq!(stride_cuts(0, 200, 4).expect("planned"), [50, 100, 150]);
    assert_eq!(stride_cuts(1, 10, 3).expect("planned"), [4, 7]);
}

#[test]
fn reversed_bounds_refuse_in_sparks_words() {
    let error = stride_cuts(200, 0, 4).expect_err("reversed");
    assert_eq!(
        error.to_string(),
        "Operation not allowed: the lower bound of partitioning column is larger than the upper \
         bound. Lower bound: 200; Upper bound: 0"
    );
    assert_eq!(
        Error::from(error).exception_class(),
        ErrorClass::IllegalArgument
    );
}

#[test]
fn strides_hold_every_value_exactly_once() {
    let cuts = stride_cuts(0, 200, 4).expect("planned");
    let planned = strides(&cuts);
    assert_eq!(
        planned,
        [
            Stride {
                lower: None,
                upper: Some(50)
            },
            Stride {
                lower: Some(50),
                upper: Some(100)
            },
            Stride {
                lower: Some(100),
                upper: Some(150)
            },
            Stride {
                lower: Some(150),
                upper: None
            },
        ]
    );
    let holds = |stride: &Stride, value: i64| {
        stride.lower.is_none_or(|lower| value >= lower)
            && stride.upper.is_none_or(|upper| value < upper)
    };
    for value in [
        i64::MIN,
        -500,
        -1,
        0,
        49,
        50,
        51,
        99,
        100,
        149,
        150,
        200,
        5000,
        i64::MAX,
    ] {
        let homes = planned.iter().filter(|stride| holds(stride, value)).count();
        assert_eq!(homes, 1, "{value}");
    }
    for (lower, upper, count) in [(i64::MIN, i64::MAX, 7), (-3, 10, 64), (0, 1_000_000, 64)] {
        let planned = strides(&stride_cuts(lower, upper, count).expect("planned"));
        assert_eq!(planned.first().expect("first").lower, None);
        assert_eq!(planned.last().expect("last").upper, None);
        assert!(
            planned
                .windows(2)
                .all(|pair| pair[0].upper.is_some() && pair[0].upper == pair[1].lower)
        );
    }
}

fn options(
    column: Option<&str>,
    lower_bound: Option<i64>,
    upper_bound: Option<i64>,
    num_partitions: Option<i64>,
) -> PartitionOptions {
    PartitionOptions::of(
        column.map(str::to_string),
        lower_bound,
        upper_bound,
        num_partitions,
    )
    .expect("a count inside Spark's Int")
}

#[test]
fn the_four_options_are_all_or_none_as_spark_rules() {
    assert_eq!(options(None, None, None, None).spec(), Ok(None));
    assert_eq!(options(None, None, None, Some(4)).spec(), Ok(None));
    assert_eq!(
        options(Some("n"), Some(0), Some(200), Some(4)).spec(),
        Ok(Some(PartitionSpec {
            column: "n".to_string(),
            lower_bound: "0".to_string(),
            upper_bound: "200".to_string(),
            num_partitions: 4,
        }))
    );
    let incomplete = [
        options(None, Some(0), Some(200), Some(4)),
        options(Some("n"), None, Some(200), Some(4)),
        options(Some("n"), Some(0), None, Some(4)),
        options(Some("n"), Some(0), Some(200), None),
        options(Some("n"), None, None, None),
        options(None, Some(0), Some(200), None),
    ];
    for given in incomplete {
        let error = given.clone().spec().expect_err("incomplete");
        assert_eq!(
            error.to_string(),
            "When reading JDBC data sources, users need to specify all or none for the \
             following options: 'partitionColumn', 'lowerBound', 'upperBound', and \
             'numPartitions'",
            "{given:?}"
        );
        assert_eq!(
            Error::from(error).exception_class(),
            ErrorClass::IllegalArgument
        );
    }
}

fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn the_four_spellings_lift_out_of_the_properties() {
    let mut given = props(&[
        ("PARTITIONCOLUMN", "n"),
        ("lowerbound", "-5"),
        ("upperBound", "+200"),
        ("numPartitions", "4"),
        ("user", "u"),
    ]);
    let lifted = PartitionOptions::default()
        .with_props(&mut given)
        .expect("lifted");
    assert_eq!(lifted.column.as_deref(), Some("n"));
    assert_eq!(lifted.lower_bound.as_deref(), Some("-5"));
    assert_eq!(
        lifted.upper_bound.as_deref(),
        Some("+200"),
        "the text is kept"
    );
    assert_eq!(lifted.num_partitions, Some(4));
    let spec = lifted.spec().expect("complete").expect("a spec");
    assert_eq!(spec.bounds(), Ok((-5, 200)));
    assert_eq!(given, props(&[("user", "u")]));

    let mut twice = props(&[("lowerBound", "0"), ("lowerbound", "1")]);
    let conflict = PartitionOptions::default().with_props(&mut twice);
    assert!(matches!(
        conflict,
        Err(ConnectError::InvalidSpecification { .. })
    ));

    let mut both = props(&[("numPartitions", "4")]);
    let conflict = options(None, None, None, Some(8)).with_props(&mut both);
    assert!(matches!(
        conflict,
        Err(ConnectError::InvalidSpecification { .. })
    ));
}

#[test]
fn a_bound_that_is_not_an_i64_refuses_naming_the_option_never_the_value() {
    for (key, value, named) in [
        ("lowerBound", "abc-secret", "lowerBound"),
        ("lowerBound", "1.5", "lowerBound"),
        ("upperBound", "9223372036854775808", "upperBound"),
        ("numPartitions", "x-secret", "numPartitions"),
        ("lowerBound", " 5", "lowerBound"),
    ] {
        let mut given = props(&[
            ("partitionColumn", "n"),
            ("lowerBound", "0"),
            ("upperBound", "9"),
            ("numPartitions", "4"),
        ]);
        given.insert(key.to_string(), value.to_string());
        let parsed = PartitionOptions::default()
            .with_props(&mut given)
            .and_then(PartitionOptions::spec)
            .and_then(|spec| spec.expect("a spec").bounds());
        let error = parsed.expect_err("not an integer");
        assert_eq!(
            refusal(error.clone()),
            PartitionRefusal::NotInteger { option: named }
        );
        assert!(!error.to_string().contains(value), "{error}");
        assert_eq!(
            Error::from(error).exception_class(),
            ErrorClass::NumberFormat
        );
    }
}

#[test]
fn predicates_stay_declared_naming_the_row() {
    let mut given = props(&[("Predicates", "a > 1")]);
    let error = PartitionOptions::default()
        .with_props(&mut given)
        .expect_err("declared");
    let message = error.to_string();
    assert!(message.contains(PARTITIONED_READ_ROW), "{message}");
    assert!(message.contains("`Predicates`"), "{message}");
    assert!(!message.contains("a > 1"), "{message}");
    assert_eq!(
        Error::from(error).exception_class(),
        ErrorClass::Unsupported
    );
}

#[test]
fn a_declared_column_type_is_unsupported_and_names_the_row() {
    let declared = ConnectError::PartitionedRead {
        refusal: PartitionRefusal::DeclaredColumnType {
            column: "d".to_string(),
            postgres_type: "date",
        },
    };
    let message = declared.to_string();
    assert!(message.contains(PARTITIONED_READ_ROW), "{message}");
    assert!(message.contains("`int2`, `int4` and `int8`"), "{message}");
    assert_eq!(
        Error::from(declared).exception_class(),
        ErrorClass::Unsupported
    );
    let wrong = ConnectError::PartitionedRead {
        refusal: PartitionRefusal::ColumnType { found: "string" },
    };
    assert_eq!(
        wrong.to_string(),
        "Partition column type should be numeric, date, or timestamp, but string found."
    );
    assert_eq!(Error::from(wrong).exception_class(), ErrorClass::Analysis);
    let missing = ConnectError::PartitionedRead {
        refusal: PartitionRefusal::ColumnNotFound {
            column: "nope".to_string(),
            columns: vec!["id".to_string(), "n".to_string()],
        },
    };
    assert_eq!(
        missing.to_string(),
        "User-defined partition column nope not found in the JDBC relation: id, n"
    );
    assert_eq!(Error::from(missing).exception_class(), ErrorClass::Analysis);
}

#[cfg(feature = "postgres")]
mod rendering {
    use std::sync::Arc;

    use repark_connect::postgres::{PgTypeKind, TypeMod};
    use repark_connect::{
        CompareOp, PgIdent, QualifiedRelation, ResolvedSource, ScanColumn, ScanRequest, ScanSource,
        Stride, stride_cuts, strides,
    };

    fn ident(name: &str) -> PgIdent {
        PgIdent::new(name).expect("identifier")
    }

    fn column(name: &str, typname: &str) -> ScanColumn {
        ScanColumn::resolve(ident(name), typname, PgTypeKind::Base, TypeMod::NONE, true)
            .expect("column resolves")
    }

    fn request() -> ScanRequest {
        ScanRequest::new(Arc::new(ResolvedSource {
            source: ScanSource::Relation(QualifiedRelation::new(ident("s"), ident("t"))),
            columns: vec![
                column("id", "int8"),
                column("N\"o", "int4"),
                column("s", "text"),
            ],
            server_version_num: 160_004,
            server_encoding: "UTF8".into(),
        }))
    }

    const SELECT: &str = "COPY (SELECT \"id\"::pg_catalog.int8, \"N\"\"o\"::pg_catalog.int4, \
                          \"s\"::pg_catalog.text FROM \"s\".\"t\" WHERE ";
    const END: &str = ") TO STDOUT (FORMAT BINARY)";

    fn slot(index: usize) -> String {
        format!("pg_catalog.current_setting('repark.p{index}')::pg_catalog.int8")
    }

    #[test]
    fn the_first_stride_is_open_below_and_takes_the_nulls() {
        let first = Stride {
            lower: None,
            upper: Some(50),
        };
        let statement = request().stride(1, first).expect("stride").statement();
        assert_eq!(
            statement.copy,
            format!(
                "{SELECT}(\"N\"\"o\" OPERATOR(pg_catalog.<) {} OR \"N\"\"o\" IS NULL){END}",
                slot(0)
            )
        );
        assert_eq!(
            statement.settings,
            [("repark.p0".to_string(), "50".to_string())]
        );
    }

    #[test]
    fn a_middle_stride_is_closed_below_and_open_above() {
        let middle = Stride {
            lower: Some(50),
            upper: Some(100),
        };
        let statement = request().stride(1, middle).expect("stride").statement();
        assert_eq!(
            statement.copy,
            format!(
                "{SELECT}(\"N\"\"o\" OPERATOR(pg_catalog.>=) {} AND \"N\"\"o\" \
                 OPERATOR(pg_catalog.<) {}){END}",
                slot(0),
                slot(1)
            )
        );
        assert_eq!(
            statement.settings,
            [
                ("repark.p0".to_string(), "50".to_string()),
                ("repark.p1".to_string(), "100".to_string()),
            ]
        );
        assert!(!statement.copy.contains("IS NULL"));
    }

    #[test]
    fn the_last_stride_is_open_above() {
        let last = Stride {
            lower: Some(i64::MAX),
            upper: None,
        };
        let statement = request().stride(0, last).expect("stride").statement();
        assert_eq!(
            statement.copy,
            format!("{SELECT}(\"id\" OPERATOR(pg_catalog.>=) {}){END}", slot(0))
        );
        assert_eq!(
            statement.settings,
            [("repark.p0".to_string(), i64::MAX.to_string())]
        );
    }

    #[test]
    fn a_stride_follows_the_pushed_conjuncts_and_keeps_projection_and_limit() {
        let stride = Stride {
            lower: Some(-70),
            upper: Some(-40),
        };
        let statement = request()
            .project(&[2])
            .expect("projection")
            .compare(0, CompareOp::Gt, "7".to_string())
            .expect("compare")
            .limit(3)
            .stride(1, stride)
            .expect("stride")
            .statement();
        assert_eq!(
            statement.copy,
            format!(
                "COPY (SELECT \"s\"::pg_catalog.text FROM \"s\".\"t\" WHERE \"id\" \
                 OPERATOR(pg_catalog.>) pg_catalog.current_setting('repark.p0')::pg_catalog.int8 \
                 AND (\"N\"\"o\" OPERATOR(pg_catalog.>=) {} AND \"N\"\"o\" \
                 OPERATOR(pg_catalog.<) {}) LIMIT 3{END}",
                slot(1),
                slot(2)
            )
        );
        let values: Vec<&str> = statement
            .settings
            .iter()
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(values, ["7", "-70", "-40"]);
    }

    #[test]
    fn the_strides_of_one_plan_bind_every_cut_once_per_side() {
        let planned = strides(&stride_cuts(0, 200, 4).expect("planned"));
        let statements: Vec<_> = planned
            .iter()
            .map(|stride| request().stride(1, *stride).expect("stride").statement())
            .collect();
        let nulls = statements
            .iter()
            .filter(|statement| statement.copy.contains("IS NULL"))
            .count();
        assert_eq!(nulls, 1);
        let bound: Vec<Vec<&str>> = statements
            .iter()
            .map(|statement| {
                statement
                    .settings
                    .iter()
                    .map(|(_, value)| value.as_str())
                    .collect()
            })
            .collect();
        assert_eq!(
            bound,
            [
                vec!["50"],
                vec!["50", "100"],
                vec!["100", "150"],
                vec!["150"]
            ]
        );
        assert!(
            statements
                .iter()
                .all(|statement| !statement.copy.contains("<=")
                    && !statement.copy.contains("pg_catalog.>)"))
        );
    }

    #[test]
    fn a_stride_refuses_past_the_slot_bound_or_the_columns() {
        let whole = Stride {
            lower: None,
            upper: None,
        };
        assert_eq!(request().stride(1, whole), Some(request()));
        let stride = Stride {
            lower: Some(1),
            upper: Some(2),
        };
        assert_eq!(request().stride(9, stride), None);
        let mut full = request();
        for _ in 0..1023 {
            full = full
                .compare(0, CompareOp::Gt, "1".to_string())
                .expect("slot");
        }
        assert_eq!(full.stride(1, stride), None);
    }
}

#[test]
fn bounds_stay_text_until_the_column_is_known() {
    let mut given = props(&[
        ("partitionColumn", "day"),
        ("lowerBound", "2024-01-01"),
        ("upperBound", "2024-02-01 00:00:00"),
        ("numPartitions", "4"),
    ]);
    let spec = PartitionOptions::default()
        .with_props(&mut given)
        .and_then(PartitionOptions::spec)
        .expect("Spark's date spelling is not refused before the column's type is known")
        .expect("a spec");
    assert_eq!(spec.lower_bound, "2024-01-01");
    assert_eq!(spec.upper_bound, "2024-02-01 00:00:00");
    assert_eq!(
        refusal(spec.bounds().expect_err("not integers")),
        PartitionRefusal::NotInteger {
            option: "lowerBound"
        }
    );
}

#[test]
fn num_partitions_is_sparks_32_bit_int() {
    let count = |text: &str| {
        let mut given = props(&[("numPartitions", text)]);
        PartitionOptions::default()
            .with_props(&mut given)
            .map(|lifted| lifted.num_partitions)
    };
    assert_eq!(count("2147483647"), Ok(Some(i32::MAX)));
    assert_eq!(count("-2147483648"), Ok(Some(i32::MIN)));
    assert_eq!(count("0"), Ok(Some(0)));
    for refused in [
        "3000000000",
        "2147483648",
        "-2147483649",
        " 4",
        "4 ",
        "4.0",
        "four",
    ] {
        let error = count(refused).expect_err(refused);
        assert_eq!(
            refusal(error.clone()),
            PartitionRefusal::NotInteger {
                option: "numPartitions"
            },
            "{refused}"
        );
        assert_eq!(
            error.to_string(),
            "`numPartitions` must be a 32-bit integer"
        );
        assert_eq!(
            Error::from(error).exception_class(),
            ErrorClass::NumberFormat
        );
    }
    let from_arguments = PartitionOptions::of(None, None, None, Some(3_000_000_000));
    assert_eq!(
        refusal(from_arguments.expect_err("past Spark's Int")),
        PartitionRefusal::NotInteger {
            option: "numPartitions"
        }
    );
    let bound = PartitionSpec {
        column: "n".to_string(),
        lower_bound: "9223372036854775808".to_string(),
        upper_bound: "9".to_string(),
        num_partitions: 4,
    };
    assert_eq!(
        bound.bounds().expect_err("past i64").to_string(),
        "`lowerBound` must be a 64-bit integer"
    );
}

#[test]
fn strides_above_the_ceiling_refuse_after_sparks_shrink() {
    assert_eq!(MAX_STRIDES, 10_000);
    let at_the_ceiling = stride_cuts(0, 10_000_000, MAX_STRIDES).expect("planned");
    assert_eq!(at_the_ceiling.len(), 9_999);
    let above = stride_cuts(0, 10_000_000, MAX_STRIDES + 1).expect_err("above the ceiling");
    assert_eq!(
        refusal(above.clone()),
        PartitionRefusal::TooManyStrides { strides: 10_001 }
    );
    let message = above.to_string();
    assert!(message.contains(PARTITIONED_READ_ROW), "{message}");
    assert!(message.contains("at most 10000 strides"), "{message}");
    assert_eq!(
        Error::from(above).exception_class(),
        ErrorClass::Unsupported
    );

    let int_max = i64::from(i32::MAX);
    assert_eq!(stride_cuts(0, 3, int_max).expect("shrunk to 3"), [1, 2]);
    let shrunk_to_the_ceiling = stride_cuts(0, MAX_STRIDES, int_max).expect("shrunk to 10000");
    assert_eq!(shrunk_to_the_ceiling.len(), 9_999);
    let shrunk_above = stride_cuts(0, MAX_STRIDES + 1, int_max).expect_err("shrunk to 10001");
    assert_eq!(
        refusal(shrunk_above),
        PartitionRefusal::TooManyStrides { strides: 10_001 }
    );
    let wrapped = stride_cuts(i64::MIN, i64::MAX, MAX_STRIDES + 1).expect_err("an overflowed span");
    assert_eq!(
        refusal(wrapped),
        PartitionRefusal::TooManyStrides { strides: 10_001 }
    );
    assert!(stride_cuts(0, 200, 1).expect("one stride").is_empty());
}
