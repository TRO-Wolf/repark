use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use repark_common::{Error, ErrorClass};
use repark_connect::{
    BEGIN_SNAPSHOT_SCAN, ConnectError, EXPORT_SNAPSHOT, PARTITIONED_READ_ROW, PartitionRefusal,
    PartitionSpec, PostgresTable, ScanColumn, Stride,
};

use crate::pushdown::{column, find_scan, orders, physical, resolved, source};

fn spec(column: &str, lower: i64, upper: i64, count: i64) -> PartitionSpec {
    PartitionSpec {
        column: column.to_string(),
        lower_bound: lower,
        upper_bound: upper,
        num_partitions: count,
    }
}

fn table(columns: Vec<ScanColumn>, extra: &[(&str, &str)]) -> PostgresTable {
    source(extra)
        .table(resolved(columns, "UTF8"))
        .expect("an injected resolution")
}

fn refusal(outcome: repark_connect::Result<PostgresTable>) -> (PartitionRefusal, ErrorClass) {
    match outcome {
        Err(error @ ConnectError::PartitionedRead { .. }) => {
            let class = Error::from(error.clone()).exception_class();
            match error {
                ConnectError::PartitionedRead { refusal } => (refusal, class),
                other => panic!("{other:?}"),
            }
        }
        other => panic!("expected a partition refusal, got {other:?}"),
    }
}

fn registered(table: PostgresTable) -> SessionContext {
    let context = SessionContext::new();
    context
        .register_table("orders", Arc::new(table))
        .expect("register");
    context
}

async fn explain(context: &SessionContext, statement: &str) -> String {
    let batches: Vec<RecordBatch> = context
        .sql(statement)
        .await
        .expect(statement)
        .collect()
        .await
        .expect(statement);
    batches
        .iter()
        .flat_map(|batch| {
            let plans = batch.column(1).as_string::<i32>();
            plans
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn scan_line(text: &str) -> &str {
    text.lines()
        .map(str::trim)
        .find(|line| line.starts_with("PostgresScanExec:"))
        .expect("a PostgresScanExec line")
}

#[test]
fn the_snapshot_statements_are_repeatable_read_and_read_only() {
    assert_eq!(
        BEGIN_SNAPSHOT_SCAN,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
    );
    assert_eq!(EXPORT_SNAPSHOT, "SELECT pg_catalog.pg_export_snapshot()");
}

#[test]
fn the_partition_column_resolves_as_spark_resolves_it() {
    let planned = |name: &str| {
        table(orders(), &[])
            .partitioned(&spec(name, 0, 200, 4))
            .expect(name)
            .strides()
            .to_vec()
    };
    let expected = planned("qty");
    assert_eq!(expected.len(), 4);
    assert_eq!(planned("QTY"), expected);
    assert_eq!(planned("\"qty\""), expected);
    assert_eq!(planned("id"), expected);
    assert_eq!(planned("small"), expected);

    let (missing, class) = refusal(table(orders(), &[]).partitioned(&spec("nope", 0, 200, 4)));
    assert_eq!(class, ErrorClass::Analysis);
    let message = ConnectError::PartitionedRead { refusal: missing }.to_string();
    assert!(
        message.starts_with(
            "User-defined partition column nope not found in the JDBC relation: id, small, qty, "
        ),
        "{message}"
    );

    let (quoted, _) = refusal(table(orders(), &[]).partitioned(&spec("\"QTY\"", 0, 200, 4)));
    assert!(matches!(quoted, PartitionRefusal::ColumnNotFound { .. }));

    let twins = || {
        vec![
            column("Key", "int4", -1, true),
            column("KEY", "int4", -1, true),
            column("other", "int4", -1, true),
        ]
    };
    let (ambiguous, class) = refusal(table(twins(), &[]).partitioned(&spec("key", 0, 200, 4)));
    assert_eq!(
        ambiguous,
        PartitionRefusal::AmbiguousColumn {
            column: "key".to_string(),
            matches: vec!["Key".to_string(), "KEY".to_string()],
        }
    );
    assert_eq!(class, ErrorClass::Analysis);
    let exact = table(twins(), &[])
        .partitioned(&spec("KEY", 0, 200, 4))
        .expect("the exact name wins");
    assert_eq!(exact.strides().len(), 4);
}

#[test]
fn only_integer_columns_partition_and_the_rest_refuse_by_kind() {
    for (name, found) in [
        ("note", "string"),
        ("code", "string"),
        ("token", "string"),
        ("doc", "string"),
        ("span", "string"),
        ("mood", "string"),
        ("paid", "boolean"),
        ("blob", "binary"),
    ] {
        let (refused, class) = refusal(table(orders(), &[]).partitioned(&spec(name, 0, 9, 3)));
        assert_eq!(refused, PartitionRefusal::ColumnType { found }, "{name}");
        assert_eq!(class, ErrorClass::Analysis, "{name}");
    }
    for (name, postgres_type) in [
        ("amount", "numeric"),
        ("ratio", "numeric"),
        ("day", "date"),
        ("at", "timestamptz"),
        ("wall", "timestamp"),
        ("score", "float8"),
    ] {
        let (refused, class) = refusal(table(orders(), &[]).partitioned(&spec(name, 0, 9, 3)));
        assert_eq!(
            refused,
            PartitionRefusal::DeclaredColumnType {
                column: name.to_string(),
                postgres_type,
            },
            "{name}"
        );
        assert_eq!(class, ErrorClass::Unsupported, "{name}");
        let message = ConnectError::PartitionedRead { refusal: refused }.to_string();
        assert!(message.contains(PARTITIONED_READ_ROW), "{message}");
    }
}

#[test]
fn the_column_is_verified_before_the_one_stride_cases() {
    for (lower, upper, count) in [(0, 200, 1), (0, 200, 0), (0, 200, -1), (5, 5, 4), (9, 0, 1)] {
        let plain = table(orders(), &[])
            .partitioned(&spec("qty", lower, upper, count))
            .expect("one unpartitioned read");
        assert!(plain.strides().is_empty(), "{lower} {upper} {count}");
        let (missing, _) =
            refusal(table(orders(), &[]).partitioned(&spec("nope", lower, upper, count)));
        assert!(matches!(missing, PartitionRefusal::ColumnNotFound { .. }));
    }
    let (reversed, class) = refusal(table(orders(), &[]).partitioned(&spec("qty", 200, 0, 4)));
    assert_eq!(
        reversed,
        PartitionRefusal::Reversed {
            lower: 200,
            upper: 0
        }
    );
    assert_eq!(class, ErrorClass::IllegalArgument);
}

fn scan_of(plan: &Arc<dyn ExecutionPlan>) -> &repark_connect::PostgresScanExec {
    find_scan(plan).expect("one Postgres scan")
}

#[tokio::test]
async fn a_partitioned_scan_is_one_partition_over_bounded_connections() {
    let partitioned = table(orders(), &[])
        .partitioned(&spec("qty", 0, 200, 4))
        .expect("partitioned");
    assert_eq!(
        partitioned.strides().first(),
        Some(&Stride {
            lower: None,
            upper: Some(50)
        })
    );
    let context = registered(partitioned);
    let plan = physical(&context, "SELECT id FROM orders").await;
    let scan = scan_of(&plan);
    assert_eq!(scan.properties().partitioning.partition_count(), 1);
    assert_eq!(scan.partition_column(), Some("qty"));
    assert_eq!(scan.strides().len(), 4);
    assert_eq!(scan.max_connections(), 4);

    let narrow = table(orders(), &[("pool_max_size", "2")])
        .partitioned(&spec("qty", 0, 1000, 16))
        .expect("partitioned");
    let context = registered(narrow);
    let plan = physical(&context, "SELECT id FROM orders").await;
    let scan = scan_of(&plan);
    assert_eq!(scan.strides().len(), 16);
    assert_eq!(scan.max_connections(), 2, "never past pool_max_size");
    assert_eq!(scan.properties().partitioning.partition_count(), 1);

    let wide = table(orders(), &[("pool_max_size", "64")])
        .partitioned(&spec("qty", 0, 3, 10))
        .expect("partitioned");
    let context = registered(wide);
    let plan = physical(&context, "SELECT id FROM orders").await;
    let scan = scan_of(&plan);
    assert_eq!(scan.strides().len(), 3, "the count shrank to the span");
    assert_eq!(scan.max_connections(), 3, "never more than the strides");

    assert_eq!(context_plain().await, (None, 0, 1));
}

async fn context_plain() -> (Option<String>, usize, usize) {
    let context = registered(table(orders(), &[]));
    let plan = physical(&context, "SELECT id FROM orders").await;
    let scan = scan_of(&plan);
    (
        scan.partition_column().map(str::to_string),
        scan.strides().len(),
        scan.max_connections(),
    )
}

#[tokio::test]
async fn every_stride_carries_the_pushed_filter_the_projection_and_the_limit() {
    let partitioned = table(orders(), &[])
        .partitioned(&spec("qty", 0, 200, 4))
        .expect("partitioned");
    let context = registered(partitioned);
    let plan = physical(&context, "SELECT note FROM orders WHERE small = 7 LIMIT 3").await;
    let scan = scan_of(&plan);
    assert_eq!(scan.pushed_limit(), Some(3));
    let statements: Vec<String> = scan
        .strides()
        .iter()
        .map(|stride| stride.statement().copy)
        .collect();
    let head = "COPY (SELECT \"note\"::pg_catalog.text FROM \"public\".\"orders\" WHERE \
                (\"small\" OPERATOR(pg_catalog.=) \
                pg_catalog.current_setting('repark.p0')::pg_catalog.int2) AND (\"qty\" ";
    for statement in &statements {
        assert!(statement.starts_with(head), "{statement}");
        assert!(
            statement.ends_with(" LIMIT 3) TO STDOUT (FORMAT BINARY)"),
            "{statement}"
        );
    }
    assert_eq!(
        statements
            .iter()
            .filter(|statement| statement.contains("\"qty\" IS NULL"))
            .count(),
        1
    );
    let bound: Vec<usize> = scan
        .strides()
        .iter()
        .map(repark_connect::ScanRequest::bound_values)
        .collect();
    assert_eq!(bound, [2, 3, 3, 2]);
    assert_eq!(scan.request().bound_values(), 1, "the base request");

    let residual = physical(
        &context,
        "SELECT note FROM orders WHERE lower(note) = 'x' LIMIT 3",
    )
    .await;
    let scan = scan_of(&residual);
    assert_eq!(
        scan.pushed_limit(),
        None,
        "a residual keeps the limit above"
    );
    assert!(
        scan.strides()
            .iter()
            .all(|stride| !stride.statement().copy.contains("LIMIT"))
    );
}

#[tokio::test]
async fn explain_names_the_column_the_strides_and_the_connection_bound() {
    let partitioned = table(orders(), &[("pool_max_size", "2")])
        .partitioned(&spec("QTY", 0, 200, 4))
        .expect("partitioned");
    let context = registered(partitioned);
    let text = explain(&context, "EXPLAIN SELECT id FROM orders WHERE small = 7").await;
    assert_eq!(
        scan_line(&text),
        "PostgresScanExec: source=company_db, relation=\"public\".\"orders\", projection=[id], \
         pushed_filters=[small = Int16(7)], residual_filters=[], pushed_limit=None, \
         partition_column=qty, strides=4, max_connections=2"
    );
    let verbose = explain(
        &context,
        "EXPLAIN VERBOSE SELECT id FROM orders WHERE small = 7",
    )
    .await;
    let line = scan_line(&verbose);
    assert!(
        line.ends_with(
            "AND (\"qty\" OPERATOR(pg_catalog.<) \
             pg_catalog.current_setting('repark.p1')::pg_catalog.int8 OR \"qty\" IS NULL)) TO \
             STDOUT (FORMAT BINARY), bound_values=2"
        ),
        "{line}"
    );
    let tree = explain(&context, "EXPLAIN FORMAT TREE SELECT id FROM orders").await;
    for key in ["partition_column: qty", "strides: 4", "max_connections: 2"] {
        assert!(tree.contains(key), "{key}: {tree}");
    }
    for secret in ["db.internal.example", "6543", "S3cretPw", "reader"] {
        assert!(!text.contains(secret) && !verbose.contains(secret) && !tree.contains(secret));
    }
}
