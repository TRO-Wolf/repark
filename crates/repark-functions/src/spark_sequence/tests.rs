use super::*;

use datafusion::arrow::array::AsArray;
use datafusion::prelude::SessionContext;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

async fn values_of(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
    let lists = batches[0].column(0).as_list::<i32>();
    assert_eq!(lists.len(), 1);
    let inner = lists.value(0);
    if let Some(numbers) = inner.as_any().downcast_ref::<Int64Array>() {
        return numbers.values().to_vec();
    }
    inner
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .expect("int values")
        .values()
        .iter()
        .map(|value| i64::from(*value))
        .collect()
}

#[tokio::test]
async fn sequence_int_widths_and_steps() {
    let ctx = ctx();
    assert_eq!(
        values_of(&ctx, "SELECT sequence(1, 3)").await,
        vec![1, 2, 3]
    );
    assert_eq!(
        values_of(&ctx, "SELECT sequence(3, 1)").await,
        vec![3, 2, 1]
    );
    assert_eq!(
        values_of(&ctx, "SELECT sequence(1, 10, 3)").await,
        vec![1, 4, 7, 10]
    );
    let batches = ctx
        .sql("SELECT sequence(CAST(1 AS TINYINT), CAST(3 AS TINYINT))")
        .await
        .expect("plan tinyint")
        .collect()
        .await
        .expect("execute tinyint");
    assert_eq!(
        batches[0].column(0).data_type(),
        &DataType::List(Arc::new(Field::new("element", DataType::Int8, false)))
    );
}

#[tokio::test]
async fn sequence_bigint_keeps_width() {
    let ctx = ctx();
    let batches = ctx
        .sql("SELECT sequence(CAST(1 AS BIGINT), CAST(3 AS BIGINT))")
        .await
        .expect("plan bigint")
        .collect()
        .await
        .expect("execute bigint");
    assert_eq!(
        batches[0].column(0).data_type(),
        &DataType::List(Arc::new(Field::new("element", DataType::Int64, false)))
    );
}

#[tokio::test]
async fn sequence_step_errors_carry_spark_text() {
    let ctx = ctx();
    for (sql, text) in [
        (
            "SELECT sequence(1, 3, 0)",
            "requirement failed: Illegal sequence boundaries: 1 to 3 by 0",
        ),
        (
            "SELECT sequence(1, 3, -1)",
            "requirement failed: Illegal sequence boundaries: 1 to 3 by -1",
        ),
    ] {
        let error = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(text), "{sql}: {error}");
    }
}

#[tokio::test]
async fn sequence_decimal_bound_refuses_wrong_input_types() {
    let ctx = ctx();
    let error = ctx
        .sql("SELECT sequence(1.5, 3)")
        .await
        .err()
        .unwrap_or_else(|| panic!("should refuse"))
        .to_string();
    assert!(
        error.contains("DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES"),
        "{error}"
    );
}

#[tokio::test]
async fn sequence_null_bound_answers_null_with_empty_contains() {
    let ctx = ctx();
    let batches = ctx
        .sql("SELECT sequence(1, CAST(NULL AS INT))")
        .await
        .expect("plan null bound")
        .collect()
        .await
        .expect("execute null bound");
    assert_eq!(
        batches[0].column(0).data_type(),
        &DataType::List(Arc::new(Field::new("element", DataType::Int32, false)))
    );
    assert!(batches[0].column(0).is_null(0));
}

#[tokio::test]
async fn sequence_string_bound_casts_to_bigint() {
    let ctx = ctx();
    let values = values_of(&ctx, "SELECT sequence(CAST(1 AS INT), '101')").await;
    assert_eq!(values.len(), 101);
    assert_eq!((values[0], values[100]), (1, 101));
    for start in ["CAST(1 AS INT)", "CAST(1 AS BIGINT)", "CAST(1 AS SMALLINT)"] {
        let batches = ctx
            .sql(&format!("SELECT sequence({start}, '3')"))
            .await
            .unwrap_or_else(|error| panic!("plan {start}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {start}: {error}"));
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int64, false))),
            "{start}"
        );
    }
    assert_eq!(
        values_of(&ctx, "SELECT sequence(1, ' 5 ')").await,
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        values_of(&ctx, "SELECT sequence(2147483647, '2147483648')").await,
        vec![2_147_483_647, 2_147_483_648]
    );
    assert_eq!(
        values_of(&ctx, "SELECT sequence('1', 5)").await,
        vec![1, 2, 3, 4, 5]
    );
    let error = ctx
        .sql("SELECT sequence(1, 'abc')")
        .await
        .expect("plan garbage string bound")
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("CAST_INVALID_INPUT"), "{error}");
}

#[tokio::test]
async fn sequence_string_bound_refuses_without_ansi() {
    use datafusion::prelude::SessionConfig;

    use crate::ansi::with_spark_ansi_config;
    let ctx = SessionContext::new_with_config(with_spark_ansi_config(SessionConfig::new(), false));
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    for sql in [
        "SELECT sequence(1, '5')",
        "SELECT sequence('1', 5)",
        "SELECT sequence(1, 5, '2')",
    ] {
        let error = crate::analyze_eagerly(
            &ctx.state(),
            ctx.sql(sql)
                .await
                .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
                .logical_plan()
                .clone(),
        )
        .expect_err("string bound refuses without ansi")
        .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn sequence_string_date_bound_casts_to_date() {
    let ctx = ctx();
    for sql in [
        "SELECT sequence(DATE'2024-01-01', '2024-01-03')",
        "SELECT sequence('2024-01-01', DATE'2024-01-03')",
        "SELECT sequence(DATE'2024-01-01', ' 2024-01-03 ')",
        "SELECT sequence(DATE'2024-01-01', '2024-01-05', INTERVAL 2 DAY)",
    ] {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Date32, false))),
            "{sql}"
        );
    }
    let batches = ctx
        .sql("SELECT sequence(DATE'2024-01-01', '2024-01-03')")
        .await
        .expect("plan date string bounds")
        .collect()
        .await
        .expect("execute date string bounds");
    let lists = batches[0].column(0).as_list::<i32>();
    let row = lists.value(0);
    let days = row
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date values");
    assert_eq!(days.values(), &[19723, 19724, 19725]);
}

#[tokio::test]
async fn sequence_dates_and_month_step() {
    let ctx = ctx();
    let batches = ctx
        .sql("SELECT sequence(DATE'2024-01-01', DATE'2024-01-03')")
        .await
        .expect("plan dates")
        .collect()
        .await
        .expect("execute dates");
    let lists = batches[0].column(0).as_list::<i32>();
    let row = lists.value(0);
    let days = row
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date values");
    assert_eq!(days.values(), &[19723, 19724, 19725]);
    let batches = ctx
        .sql("SELECT sequence(DATE'2024-01-01', DATE'2024-03-01', INTERVAL 1 MONTH)")
        .await
        .expect("plan month step")
        .collect()
        .await
        .expect("execute month step");
    let lists = batches[0].column(0).as_list::<i32>();
    let row = lists.value(0);
    let days = row
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date values");
    assert_eq!(days.values(), &[19723, 19754, 19783]);
    let batches = ctx
        .sql("SELECT sequence(DATE'2024-01-31', DATE'2024-03-31', INTERVAL 1 MONTH)")
        .await
        .expect("plan clamping month step")
        .collect()
        .await
        .expect("execute clamping month step");
    let lists = batches[0].column(0).as_list::<i32>();
    let row = lists.value(0);
    let days = row
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date values");
    let rendered: Vec<String> = days
        .iter()
        .map(|day| rows::format_days(day.expect("date value")).expect("format date"))
        .collect();
    assert_eq!(rendered, vec!["2024-01-31", "2024-02-29", "2024-03-31"]);
}
