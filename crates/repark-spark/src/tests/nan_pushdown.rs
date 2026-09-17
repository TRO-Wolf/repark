use super::super::*;
use super::common::*;

use datafusion::arrow::array::Int32Array;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::expr::{Predicate, PredicateOperator};
use iceberg_datafusion::IcebergTableScan;

async fn nan_ids(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i32> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            ids.push(column.value(index));
        }
    }
    ids.sort_unstable();
    ids
}

async fn nan_seed(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id INT, d DOUBLE, f FLOAT) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), (2, CAST(1.0 AS DOUBLE), CAST(1.0 AS FLOAT)), (3, NULL, NULL)",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (4, CAST(0.5 AS DOUBLE), CAST(0.5 AS FLOAT)), (5, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)), (6, CAST(-2.0 AS DOUBLE), CAST(-2.0 AS FLOAT))",
    )
    .await;
}

#[tokio::test]
async fn nan_equality_answers_the_nan_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d = CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE CAST('NaN' AS DOUBLE) = d ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d <=> CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE f = CAST('NaN' AS FLOAT) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
}

#[tokio::test]
async fn nan_in_and_inequality_answer_the_oracle_sets() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d IN (CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d IN (CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![1, 2, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d != CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d NOT IN (CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d < CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d >= CAST('NaN' AS DOUBLE) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE NOT (d = CAST('NaN' AS DOUBLE)) ORDER BY id"
        )
        .await,
        vec![2, 4, 6]
    );
    assert_eq!(
        nan_ids(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE isnan(d) ORDER BY id"
        )
        .await,
        vec![1, 5]
    );
}

fn scan_predicates(plan: &Arc<dyn ExecutionPlan>, hits: &mut Vec<Option<Predicate>>) {
    if let Some(scan) = plan.as_ref().downcast_ref::<IcebergTableScan>() {
        hits.push(scan.predicates().cloned());
    }
    for child in plan.children() {
        scan_predicates(child, hits);
    }
}

async fn planned_predicates(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<Option<Predicate>> {
    let physical = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut hits = Vec::new();
    scan_predicates(&physical, &mut hits);
    hits
}

fn assert_unary_nan(pushed: &[Option<Predicate>], sql: &str, expected: PredicateOperator) {
    assert_eq!(pushed.len(), 1, "one scan must plan {sql}");
    match &pushed[0] {
        Some(Predicate::Unary(unary)) => {
            assert_eq!(unary.op(), expected, "pushed operator for {sql}");
            assert_eq!(unary.term().name(), "d", "pushed term for {sql}");
        }
        other => panic!("{sql} must push a unary NaN predicate, got {other:?}"),
    }
}

#[tokio::test]
async fn nan_equality_pushes_is_nan() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    for sql in [
        "SELECT id FROM ice.sales.t WHERE d = CAST('NaN' AS DOUBLE)",
        "SELECT id FROM ice.sales.t WHERE CAST('NaN' AS DOUBLE) = d",
    ] {
        assert_unary_nan(
            &planned_predicates(&ctx, &catalogs, sql).await,
            sql,
            PredicateOperator::IsNan,
        );
    }
}

#[tokio::test]
async fn nan_nullsafe_leaves_pushdown_empty() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    let sql = "SELECT id FROM ice.sales.t WHERE d <=> CAST('NaN' AS DOUBLE)";
    let pushed = planned_predicates(&ctx, &catalogs, sql).await;
    assert_eq!(pushed.len(), 1, "one scan must plan {sql}");
    assert!(pushed[0].is_none(), "{sql} must push no predicate");
}

#[tokio::test]
async fn nan_inequality_pushes_not_nan() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_unary_nan(
        &planned_predicates(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d != CAST('NaN' AS DOUBLE)",
        )
        .await,
        "d != NaN",
        PredicateOperator::NotNan,
    );
}

#[tokio::test]
async fn nan_in_pushes_is_nan_or_in() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    let sql = "SELECT id FROM ice.sales.t WHERE d IN (CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE))";
    let pushed = planned_predicates(&ctx, &catalogs, sql).await;
    assert_eq!(pushed.len(), 1, "one scan must plan {sql}");
    match &pushed[0] {
        Some(Predicate::Or(or)) => {
            let [left, right] = or.inputs();
            match left {
                Predicate::Unary(unary) => {
                    assert_eq!(unary.op(), PredicateOperator::IsNan);
                    assert_eq!(unary.term().name(), "d");
                }
                other => panic!("IN left arm must be Unary IsNan(d), got {other:?}"),
            }
            match right {
                Predicate::Binary(binary) => {
                    assert_eq!(binary.op(), PredicateOperator::Eq);
                    assert_eq!(binary.term().name(), "d");
                    assert!(!binary.literal().is_nan());
                }
                other => panic!("IN right arm must be a NaN-free Eq, got {other:?}"),
            }
        }
        other => panic!("{sql} must push IsNan OR In, got {other:?}"),
    }
}

#[tokio::test]
async fn nan_range_stays_unpushed() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    let sql = "SELECT id FROM ice.sales.t WHERE d < CAST('NaN' AS DOUBLE)";
    let pushed = planned_predicates(&ctx, &catalogs, sql).await;
    assert_eq!(pushed.len(), 1, "one scan must plan {sql}");
    match &pushed[0] {
        None => {}
        other => panic!("{sql} must push no predicate, got {other:?}"),
    }
}

#[tokio::test]
async fn nan_single_not_in_pushes_not_nan() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    nan_seed(&ctx, &catalogs).await;
    assert_unary_nan(
        &planned_predicates(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.t WHERE d NOT IN (CAST('NaN' AS DOUBLE))",
        )
        .await,
        "d NOT IN (NaN)",
        PredicateOperator::NotNan,
    );
}
