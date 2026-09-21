use std::sync::Arc;

use datafusion::arrow::array::{Decimal128Array, Float32Array, Float64Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

fn sweep_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
        Field::new("dec", DataType::Decimal128(6, 2), true),
    ]));
    let decimals = Decimal128Array::from(vec![
        Some(1050),
        Some(-325),
        Some(0),
        Some(1),
        None,
        Some(99999),
        Some(-9999),
        Some(700),
        Some(100),
        Some(4242),
    ])
    .with_precision_and_scale(6, 2)
    .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(-0.0),
                Some(0.0),
                Some(1.5),
                None,
                Some(f64::INFINITY),
                Some(-1e300),
                Some(2.0),
                Some(0.1),
                Some(3.0),
            ])),
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(-0.0),
                Some(0.0),
                Some(1.5),
                None,
                Some(f32::INFINITY),
                Some(-1e30),
                Some(2.0),
                Some(0.1),
                Some(0.1),
            ])),
            Arc::new(decimals),
        ],
    )
    .unwrap()
}

async fn ids_where(session: &ReparkSession, predicate: &str) -> Vec<i32> {
    let batches = session
        .sql(&format!(
            "SELECT id FROM sweep_temp WHERE {predicate} ORDER BY id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..column.len() {
            ids.push(column.value(row));
        }
    }
    ids
}

#[tokio::test]
async fn decimal_literal_against_float_matches_spark() {
    let session = spark_session();
    session
        .create_or_replace_temp_view("sweep_temp", vec![sweep_batch()])
        .unwrap();
    let cases: Vec<(&str, Vec<i32>)> = vec![
        ("d > 1.0", vec![1, 4, 6, 8, 10]),
        ("d > 0.0", vec![1, 4, 6, 8, 9, 10]),
        ("d <= 0.0", vec![2, 3, 7]),
        ("d <= 0.1", vec![2, 3, 7, 9]),
        ("d >= 1.0", vec![1, 4, 6, 8, 10]),
        ("d <> 0.1", vec![1, 2, 3, 4, 6, 7, 8, 10]),
        ("d BETWEEN 1.0 AND 3.0", vec![4, 8, 10]),
        ("d NOT BETWEEN 1.0 AND 3.0", vec![1, 2, 3, 6, 7, 9]),
        ("d = -0.5", vec![]),
        ("d > -1.0", vec![1, 2, 3, 4, 6, 8, 9, 10]),
        ("f > 1.0", vec![1, 4, 6, 8]),
        ("f > 0.0", vec![1, 4, 6, 8, 9, 10]),
        ("f >= 1.0", vec![1, 4, 6, 8]),
        ("f <= 0.0", vec![2, 3, 7]),
        ("f < 0.1", vec![2, 3, 7]),
        ("f = 0.1", vec![]),
        ("f <> 0.1", vec![1, 2, 3, 4, 6, 7, 8, 9, 10]),
        ("f BETWEEN 1.0 AND 2.0", vec![4, 8]),
        ("f NOT BETWEEN 1.0 AND 2.0", vec![1, 2, 3, 6, 7, 9, 10]),
        ("0.1 = f", vec![]),
        ("f = 1.5", vec![4]),
        ("dec = 0", vec![3]),
        ("dec = 0.0", vec![3]),
        ("dec > 10", vec![1, 6, 10]),
        ("dec < 0", vec![2, 7]),
        ("dec IN (0.0, 1.00)", vec![3, 9]),
    ];
    for (predicate, expected) in cases {
        assert_eq!(
            ids_where(&session, predicate).await,
            expected,
            "{predicate}"
        );
    }
}

#[tokio::test]
async fn signed_zero_eq_documents_kernel_divergence() {
    let session = spark_session();
    session
        .create_or_replace_temp_view("sweep_temp", vec![sweep_batch()])
        .unwrap();
    let cases: Vec<(&str, Vec<i32>, Vec<i32>)> = vec![
        ("d = 0.0", vec![2, 3], vec![3]),
        ("0.0 = d", vec![2, 3], vec![3]),
        (
            "d <> 0.0",
            vec![1, 4, 6, 7, 8, 9, 10],
            vec![1, 2, 4, 6, 7, 8, 9, 10],
        ),
        ("d < 0.0", vec![7], vec![2, 7]),
        (
            "d >= 0.0",
            vec![1, 2, 3, 4, 6, 8, 9, 10],
            vec![1, 3, 4, 6, 8, 9, 10],
        ),
        (
            "d BETWEEN 0.0 AND 2.0",
            vec![2, 3, 4, 8, 9],
            vec![3, 4, 8, 9],
        ),
        (
            "d NOT BETWEEN 0.0 AND 2.0",
            vec![1, 6, 7, 10],
            vec![1, 2, 6, 7, 10],
        ),
        ("d IN (0.0, 1.5)", vec![2, 3, 4], vec![3, 4]),
        (
            "d NOT IN (0.0, 1.5)",
            vec![1, 6, 7, 8, 9, 10],
            vec![1, 2, 6, 7, 8, 9, 10],
        ),
        ("f = 0.0", vec![2, 3], vec![3]),
        (
            "f <> 0.0",
            vec![1, 4, 6, 7, 8, 9, 10],
            vec![1, 2, 4, 6, 7, 8, 9, 10],
        ),
        ("f < 0.0", vec![7], vec![2, 7]),
        (
            "f >= 0.0",
            vec![1, 2, 3, 4, 6, 8, 9, 10],
            vec![1, 3, 4, 6, 8, 9, 10],
        ),
        (
            "f BETWEEN 0.0 AND 1.5",
            vec![2, 3, 4, 9, 10],
            vec![3, 4, 9, 10],
        ),
        (
            "f NOT BETWEEN 0.0 AND 1.5",
            vec![1, 6, 7, 8],
            vec![1, 2, 6, 7, 8],
        ),
        ("f IN (0.0, 0.1)", vec![2, 3], vec![3]),
        (
            "f NOT IN (0.0, 0.1)",
            vec![1, 4, 6, 7, 8, 9, 10],
            vec![1, 2, 4, 6, 7, 8, 9, 10],
        ),
    ];
    for (predicate, spark, repark) in cases {
        assert_eq!(
            ids_where(&session, predicate).await,
            repark,
            "{predicate}: spark answers {spark:?}; the float eq kernel keeps -0.0 distinct from 0.0"
        );
    }
}

#[tokio::test]
async fn decimal_literal_against_float_casts_the_literal() {
    let session = spark_session();
    session
        .create_or_replace_temp_view("sweep_temp", vec![sweep_batch()])
        .unwrap();
    let batches = session
        .sql("EXPLAIN SELECT id FROM sweep_temp WHERE d = 0.0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let logical = plan.value(0);
    assert!(
        !logical.contains("Decimal128"),
        "no decimal cast may remain, got {logical}"
    );
    assert!(
        logical.contains("Float64"),
        "the literal must widen to double, got {logical}"
    );
}
