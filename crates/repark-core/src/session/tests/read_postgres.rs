use std::collections::BTreeMap;

use repark_common::ErrorClass;

use crate::{PostgresRead, PostgresTarget, ReparkSession};

fn read(properties: &[(&str, &str)]) -> PostgresRead {
    PostgresRead {
        url: "postgresql://203.0.113.1:5432/db".to_string(),
        target: PostgresTarget::Relation("public.t".to_string()),
        properties: properties
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>(),
        partition_column: None,
        lower_bound: None,
        upper_bound: None,
        num_partitions: None,
        predicates: false,
    }
}

#[tokio::test]
async fn a_dbtable_property_is_the_target_never_a_setting() {
    let session = ReparkSession::builder().build().expect("session");
    for key in ["dbtable", "DBTABLE"] {
        let error = session
            .read_postgres(read(&[(key, "public.t")]))
            .await
            .expect_err("a read without `user` refuses before any connection");
        let message = error.to_string();
        assert!(
            message.contains("database source `jdbc`"),
            "{key}: {message}"
        );
        assert!(message.contains("`user` is required"), "{key}: {message}");
        assert!(
            !message.to_lowercase().contains("dbtable"),
            "{key}: {message}"
        );
    }
}

type Ranged = (Option<&'static str>, Option<i64>, Option<i64>, Option<i64>);

fn partitioned(mut read: PostgresRead, ranged: Ranged) -> PostgresRead {
    let (column, lower_bound, upper_bound, num_partitions) = ranged;
    read.partition_column = column.map(str::to_string);
    read.lower_bound = lower_bound;
    read.upper_bound = upper_bound;
    read.num_partitions = num_partitions;
    read
}

async fn refusal(session: &ReparkSession, read: PostgresRead) -> (ErrorClass, String) {
    let error = session
        .read_postgres(read)
        .await
        .expect_err("refuses before any connection");
    (error.exception_class(), error.to_string())
}

#[tokio::test]
async fn partition_options_refuse_as_spark_does_before_any_connection() {
    let session = ReparkSession::builder().build().expect("session");
    let all_or_none = "When reading JDBC data sources, users need to specify all or none for \
                       the following options: 'partitionColumn', 'lowerBound', 'upperBound', \
                       and 'numPartitions'";
    let incomplete: [Ranged; 4] = [
        (Some("id"), None, None, None),
        (Some("id"), Some(0), Some(9), None),
        (None, Some(0), Some(9), Some(4)),
        (None, Some(0), None, None),
    ];
    for ranged in incomplete {
        let given = partitioned(read(&[("user", "u")]), ranged);
        let (class, message) = refusal(&session, given).await;
        assert_eq!(class, ErrorClass::IllegalArgument, "{message}");
        assert!(message.contains(all_or_none), "{message}");
        assert!(message.contains("database source `jdbc`"), "{message}");
    }

    let from_props = read(&[
        ("user", "u"),
        ("partitionColumn", "id"),
        ("lowerBound", "0"),
    ]);
    let (class, message) = refusal(&session, from_props).await;
    assert_eq!(class, ErrorClass::IllegalArgument, "{message}");
    assert!(message.contains(all_or_none), "{message}");

    let not_a_number = read(&[
        ("user", "u"),
        ("PartitionColumn", "id"),
        ("lowerbound", "2024-01-01"),
        ("upperBound", "9"),
        ("numpartitions", "sentinel-count"),
    ]);
    let (class, message) = refusal(&session, not_a_number).await;
    assert_eq!(class, ErrorClass::NumberFormat, "{message}");
    assert!(message.contains("`numPartitions`"), "{message}");
    assert!(!message.contains("sentinel-count"), "{message}");

    let all_four = (Some("id"), Some(0), Some(9), Some(4));
    let mut with_query = partitioned(read(&[("user", "u")]), all_four);
    with_query.target = PostgresTarget::Query("SELECT 1 AS id".to_string());
    let (class, message) = refusal(&session, with_query).await;
    assert_eq!(class, ErrorClass::IllegalArgument, "{message}");
    assert!(
        message.contains("Options 'query' and 'partitionColumn' can not be specified together"),
        "{message}"
    );

    let mut predicates = read(&[("user", "u")]);
    predicates.predicates = true;
    let (class, message) = refusal(&session, predicates).await;
    assert_eq!(class, ErrorClass::Unsupported, "{message}");
    assert!(
        message.contains("CONNECT-DECL-pg-partitioned-read") && message.contains("`predicates`"),
        "{message}"
    );

    let in_props = read(&[("user", "u"), ("predicates", "id > 1")]);
    let (class, message) = refusal(&session, in_props).await;
    assert_eq!(class, ErrorClass::Unsupported, "{message}");
    assert!(!message.contains("id > 1"), "{message}");

    let twice = read(&[("user", "u"), ("numPartitions", "4")]);
    let twice = partitioned(twice, (None, None, None, Some(8)));
    let (class, message) = refusal(&session, twice).await;
    assert_eq!(class, ErrorClass::IllegalArgument, "{message}");
    assert!(message.contains("give it once"), "{message}");
}

#[tokio::test]
async fn num_partitions_alone_is_no_partitioning_and_never_a_setting() {
    let session = ReparkSession::builder().build().expect("session");
    let alone = partitioned(read(&[]), (None, None, None, Some(4)));
    let (_, message) = refusal(&session, alone).await;
    assert!(message.contains("`user` is required"), "{message}");
    let (_, message) = refusal(&session, read(&[("NumPartitions", "4")])).await;
    assert!(message.contains("`user` is required"), "{message}");
}
