use super::super::*;
use super::common::*;

async fn session_ctx() -> (SessionContext, CatalogRegistry, TempDir) {
    let holder = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&holder).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs, holder)
}

async fn utf8_values(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..column.len() {
            assert!(column.is_valid(row));
            values.push(column.value(row).to_string());
        }
    }
    values
}

async fn first_field_nullable(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> bool {
    let frame = execute(ctx, catalogs, sql).await.unwrap();
    frame.schema().field(0).is_nullable()
}

fn register_shadow(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, true),
        Field::new("current_user", DataType::Utf8, true),
        Field::new("session_user", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["colval"])),
            Arc::new(StringArray::from(vec!["cval"])),
            Arc::new(StringArray::from(vec!["sval"])),
            Arc::new(StringArray::from(vec!["vval"])),
        ],
    )
    .unwrap();
    ctx.register_batch("shadow", batch).unwrap();
}

#[tokio::test]
async fn spark_door_user_call_answers_facade_identity() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT user()").await,
        vec!["repark".to_string()]
    );
    assert!(!first_field_nullable(&ctx, &catalogs, "SELECT user()").await);
}

#[tokio::test]
async fn spark_door_current_user_call_answers_facade_identity() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT current_user()").await,
        vec!["repark".to_string()]
    );
    assert!(!first_field_nullable(&ctx, &catalogs, "SELECT current_user()").await);
}

#[tokio::test]
async fn spark_door_session_user_call_equals_current_user_call() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT session_user()").await,
        utf8_values(&ctx, &catalogs, "SELECT current_user()").await,
    );
    assert!(!first_field_nullable(&ctx, &catalogs, "SELECT session_user()").await);
}

#[tokio::test]
async fn spark_door_version_call_answers_repark_version() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    let values = utf8_values(&ctx, &catalogs, "SELECT version()").await;
    assert_eq!(values.len(), 1);
    assert!(!values[0].contains("DataFusion"), "{}", values[0]);
    assert_eq!(
        values,
        vec![format!("repark-{}", env!("CARGO_PKG_VERSION"))]
    );
    assert!(!first_field_nullable(&ctx, &catalogs, "SELECT version()").await);
}

#[tokio::test]
async fn spark_door_user_call_inside_concat() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT concat(user(), 'x')").await,
        vec!["reparkx".to_string()]
    );
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT concat(version(), 'x')").await,
        vec![format!("repark-{}x", env!("CARGO_PKG_VERSION"))]
    );
}

#[tokio::test]
async fn spark_door_current_user_call_in_where_and_from() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT current_user() AS c, id FROM src ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut pairs = Vec::new();
    for batch in &batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            pairs.push((names.value(row).to_string(), ids.value(row)));
        }
    }
    assert_eq!(
        pairs,
        vec![
            ("repark".to_string(), 1),
            ("repark".to_string(), 2),
            ("repark".to_string(), 3),
        ]
    );
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id FROM src WHERE current_user() = 'repark' ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let kept: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(kept, 3);
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id FROM src WHERE current_user() = 'nosuchuser_xyz'",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let kept: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(kept, 0);
}

#[tokio::test]
async fn spark_door_bare_user_keeps_column_or_error() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    register_shadow(&ctx);
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT user FROM shadow").await,
        vec!["colval".to_string()]
    );
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT current_user FROM shadow").await,
        vec!["cval".to_string()]
    );
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT session_user FROM shadow").await,
        vec!["sval".to_string()]
    );
    let error = execute(&ctx, &catalogs, "SELECT user")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("No field named user"), "{error}");
}

#[tokio::test]
async fn spark_door_bare_version_keeps_column() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    register_shadow(&ctx);
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT version FROM shadow").await,
        vec!["vval".to_string()]
    );
}

#[tokio::test]
async fn spark_door_broken_user_call_keeps_original_error() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    let error = execute(&ctx, &catalogs, "SELECT user() FROM")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Column: 12"), "{error}");
}

#[tokio::test]
async fn spark_door_user_call_case_insensitive_spelling() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT USER()").await,
        vec!["repark".to_string()]
    );
    assert_eq!(
        utf8_values(&ctx, &catalogs, "SELECT Current_User()").await,
        vec!["repark".to_string()]
    );
}

#[tokio::test]
async fn spark_door_user_call_with_argument_refuses_loud() {
    let (ctx, catalogs, _holder) = session_ctx().await;
    let error = execute(&ctx, &catalogs, "SELECT user(1)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("user"), "{error}");
}
