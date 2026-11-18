use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, EmitTo, Expr, GroupsAccumulator, Operator, col, lit,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::sum_decimal::{
    DecimalSum, DecimalSumDistinct, DecimalSumGroups, OVERFLOWED, Place, SumCheck,
};
use super::{SparkAvgAnsiMode, sum_udaf, try_sum_udaf};

const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
const NINES: &str = "99999999999999999999999999999999999999";

fn decimal(values: &[Option<i128>], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .expect("decimal fixture"),
    )
}

fn context(ansi: bool, config: SessionConfig) -> SessionContext {
    let ctx = SessionContext::new_with_config(crate::ansi::with_spark_ansi_config(config, ansi));
    crate::register_all(&ctx);
    ctx.add_analyzer_rule(Arc::new(SparkAvgAnsiMode));
    ctx
}

fn source() -> String {
    let rows = [
        (1, 1, NINES.to_string()),
        (1, 2, NINES.to_string()),
        (1, 3, format!("-{NINES}")),
        (1, 4, format!("-{NINES}")),
        (1, 5, "5".to_string()),
        (2, 6, NINES.to_string()),
        (2, 7, "1".to_string()),
    ];
    let values: Vec<String> = rows
        .iter()
        .map(|(g, id, x)| format!("({g}, {id}, '{x}')"))
        .collect();
    format!(
        "(SELECT g, id, CAST(x AS DECIMAL(38,0)) AS v FROM (VALUES {}) t(g, id, x))",
        values.join(", ")
    )
}

async fn texts(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
    let frame = ctx.sql(sql).await.map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            out.push(
                arrow::util::display::array_value_to_string(batch.column(0), row).expect("display"),
            );
        }
    }
    Ok(out)
}

fn condition(result: Result<Vec<String>, String>) -> String {
    match result {
        Ok(rows) => format!("rows {rows:?}"),
        Err(error)
            if error
                .contains("[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. Use 'try_sum'") =>
        {
            "ARITHMETIC_OVERFLOW".to_string()
        }
        Err(error) if error.contains("[NUMERIC_VALUE_OUT_OF_RANGE]") => {
            "NUMERIC_VALUE_OUT_OF_RANGE".to_string()
        }
        Err(error) => error,
    }
}

#[tokio::test]
async fn sum_checks_where_spark_checks_in_both_ansi_modes() {
    let src = source();
    let max = NINES.to_string();
    let cases = [
        (
            format!("SELECT sum(v) FROM {src} s WHERE g = 2"),
            "ARITHMETIC_OVERFLOW",
            vec![String::new()],
        ),
        (
            format!("SELECT sum(v) FROM {src} s WHERE g = 1"),
            "rows [\"5\"]",
            vec!["5".to_string()],
        ),
        (
            format!("SELECT sum(v) FROM {src} s WHERE g = 1 GROUP BY g"),
            "ARITHMETIC_OVERFLOW",
            vec![String::new()],
        ),
        (
            format!(
                "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {src} s WHERE g = 1 ORDER BY id"
            ),
            "NUMERIC_VALUE_OUT_OF_RANGE",
            vec![
                max.clone(),
                String::new(),
                "0".to_string(),
                String::new(),
                format!("-{}", "9".repeat(37) + "4"),
            ],
        ),
        (
            format!("SELECT sum(v) OVER (ORDER BY id) FROM {src} s WHERE g = 1 ORDER BY id"),
            "NUMERIC_VALUE_OUT_OF_RANGE",
            vec![
                max.clone(),
                String::new(),
                max,
                "0".to_string(),
                "5".to_string(),
            ],
        ),
    ];
    for (sql, on, off) in cases {
        assert_eq!(
            condition(texts(&context(true, SessionConfig::new()), &sql).await),
            on,
            "{sql}"
        );
        assert_eq!(
            texts(&context(false, SessionConfig::new()), &sql).await,
            Ok(off),
            "{sql}"
        );
    }
    for ansi in [true, false] {
        let ctx = context(ansi, SessionConfig::new());
        for (sql, want) in [
            (format!("SELECT try_sum(v) FROM {src} s WHERE g = 1"), "5"),
            (
                format!("SELECT try_sum(v) FROM {src} s WHERE g = 1 GROUP BY g"),
                "",
            ),
            (format!("SELECT try_sum(v) FROM {src} s WHERE g = 2"), ""),
        ] {
            assert_eq!(texts(&ctx, &sql).await, Ok(vec![want.to_string()]), "{sql}");
        }
    }
}

fn shipped(accumulator: &mut dyn Accumulator) -> Vec<ArrayRef> {
    accumulator
        .state()
        .expect("state")
        .into_iter()
        .map(|value| value.to_array().expect("state array"))
        .collect()
}

#[test]
fn an_ungrouped_total_raises_sparks_condition_by_where_it_overflowed() {
    let ansi = SumCheck::new(38, 0, true);
    let mut merged = DecimalSum::new(38, ansi, Place::Ungrouped);
    for value in [MAX38, 1] {
        let mut partial = DecimalSum::new(38, ansi, Place::Ungrouped);
        partial
            .update_batch(&[decimal(&[Some(value)], 38, 0)])
            .expect("update");
        merged.merge_batch(&shipped(&mut partial)).expect("merge");
    }
    let error = merged.evaluate().expect_err("merged total past 38 digits");
    assert!(
        error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
        "{error}"
    );
    let mut single = DecimalSum::new(38, ansi, Place::Ungrouped);
    single
        .update_batch(&[decimal(&[Some(MAX38), Some(1)], 38, 0)])
        .expect("update");
    let error = single.evaluate().expect_err("single-phase total");
    assert!(
        error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
        "{error}"
    );
    let error = single.state().expect_err("overflowed partial");
    assert!(error.to_string().contains("'try_sum'"), "{error}");
    let legacy = SumCheck::new(38, 0, false);
    let mut partial = DecimalSum::new(38, legacy, Place::Ungrouped);
    partial
        .update_batch(&[decimal(&[Some(MAX38), Some(MAX38), Some(-MAX38)], 38, 0)])
        .expect("update");
    assert_eq!(
        partial.evaluate().expect("exact"),
        ScalarValue::Decimal128(Some(MAX38), 38, 0)
    );
    partial
        .update_batch(&[decimal(&[Some(1)], 38, 0)])
        .expect("update");
    let state = shipped(&mut partial);
    assert_eq!(
        state[0].as_primitive::<Decimal128Type>().value(0),
        OVERFLOWED
    );
    let mut merged = DecimalSum::new(38, legacy, Place::Ungrouped);
    merged.merge_batch(&state).expect("merge");
    merged
        .merge_batch(&[decimal(&[Some(-MAX38)], 38, 0)])
        .expect("merge");
    assert!(merged.evaluate().expect("null").is_null());
    let mut window = DecimalSum::new(38, ansi, Place::Window);
    window
        .update_batch(&[decimal(&[Some(MAX38), Some(MAX38)], 38, 0)])
        .expect("update");
    let error = window.evaluate().expect_err("window row");
    assert!(
        error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
        "{error}"
    );
    window
        .retract_batch(&[decimal(&[Some(MAX38)], 38, 0)])
        .expect("retract");
    assert_eq!(
        window.evaluate().expect("back inside"),
        ScalarValue::Decimal128(Some(MAX38), 38, 0)
    );
}

#[test]
fn a_distinct_sum_is_exact_and_raises_by_place() {
    let ansi = SumCheck::new(38, 0, true);
    let rows = decimal(&[Some(MAX38), Some(MAX38), Some(1), None], 38, 0);
    let mut merged = DecimalSumDistinct::new(ansi, Place::Ungrouped);
    for part in [&[Some(MAX38), Some(MAX38)][..], &[Some(1)][..]] {
        let mut partial = DecimalSumDistinct::new(ansi, Place::Ungrouped);
        partial
            .update_batch(&[decimal(part, 38, 0)])
            .expect("update");
        merged.merge_batch(&shipped(&mut partial)).expect("merge");
    }
    let error = merged.evaluate().expect_err("merged");
    assert!(
        error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
        "{error}"
    );
    for place in [Place::Grouped, Place::Ungrouped] {
        let mut single = DecimalSumDistinct::new(ansi, place);
        single.update_batch(&[Arc::clone(&rows)]).expect("update");
        let error = single.evaluate().expect_err("single");
        assert!(
            error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
            "{error}"
        );
    }
    let mut legacy = DecimalSumDistinct::new(SumCheck::new(38, 0, false), Place::Grouped);
    legacy.update_batch(&[rows]).expect("update");
    assert!(legacy.evaluate().expect("null").is_null());
    let mut fits = DecimalSumDistinct::new(ansi, Place::Grouped);
    fits.update_batch(&[decimal(&[Some(MAX38), Some(MAX38)], 38, 0)])
        .expect("update");
    assert_eq!(
        fits.evaluate().expect("one value"),
        ScalarValue::Decimal128(Some(MAX38), 38, 0)
    );
}

#[test]
fn a_grouped_sum_is_checked_after_every_row_and_sticks() {
    let legacy = SumCheck::new(38, 0, false);
    let mut groups = DecimalSumGroups::new(legacy);
    groups
        .update_batch(
            &[decimal(
                &[
                    Some(MAX38),
                    Some(5),
                    Some(MAX38),
                    Some(-MAX38),
                    Some(-MAX38),
                ],
                38,
                0,
            )],
            &[0, 1, 0, 0, 0],
            None,
            2,
        )
        .expect("update");
    let state = groups.state(EmitTo::All).expect("state");
    let partials = state[0].as_primitive::<Decimal128Type>();
    assert_eq!((partials.value(0), partials.value(1)), (OVERFLOWED, 5));
    assert_eq!(partials.data_type(), &DataType::Decimal128(38, 0));
    let mut merged = DecimalSumGroups::new(legacy);
    merged.merge_batch(&state, &[0, 1], None, 2).expect("merge");
    merged
        .merge_batch(&[decimal(&[Some(-1), None], 38, 0)], &[0, 1], None, 2)
        .expect("merge");
    let result = merged.evaluate(EmitTo::All).expect("evaluate");
    let result = result.as_primitive::<Decimal128Type>();
    assert!(
        result.is_null(0),
        "a group that passed 38 digits stays NULL"
    );
    assert_eq!(result.value(1), 5);
    let mut ansi = DecimalSumGroups::new(SumCheck::new(38, 0, true));
    let error = ansi
        .update_batch(&[decimal(&[Some(MAX38), Some(1)], 38, 0)], &[0, 0], None, 1)
        .expect_err("the first overflowing row raises");
    assert!(error.to_string().contains("'try_sum'"), "{error}");
    let converted = DecimalSumGroups::new(SumCheck::new(38, 2, false))
        .convert_to_state(&[decimal(&[Some(7), None], 28, 2)], None)
        .expect("convert");
    assert_eq!(converted[0].data_type(), &DataType::Decimal128(38, 2));
    assert!(converted[0].is_null(1));
}

#[test]
fn the_overflow_marker_survives_an_arrow_ipc_round_trip() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Decimal128(38, 0),
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![decimal(&[Some(OVERFLOWED), Some(5), None], 38, 0)],
    )
    .expect("state batch");
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("writer");
    writer.write(&batch).expect("write");
    writer.finish().expect("finish");
    let read: Vec<RecordBatch> = StreamReader::try_new(bytes.as_slice(), None)
        .expect("reader")
        .collect::<Result<_, _>>()
        .expect("read");
    let column = read[0].column(0).as_primitive::<Decimal128Type>();
    assert_eq!(column.value(0), OVERFLOWED);
    let mut merged = DecimalSumGroups::new(SumCheck::new(38, 0, false));
    merged
        .merge_batch(&[Arc::clone(read[0].column(0))], &[0, 1, 2], None, 3)
        .expect("merge");
    let result = merged.evaluate(EmitTo::All).expect("evaluate");
    let result = result.as_primitive::<Decimal128Type>();
    assert!(result.is_null(0) && result.is_null(2));
    assert_eq!(result.value(1), 5);
}

fn grouped_table(groups: i64, partitions: usize) -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(38, 0), true),
    ]));
    let mut batches = Vec::new();
    for part in 0..partitions {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        if part == 0 {
            keys.extend([7, 7]);
            values.extend([Some(MAX38), Some(MAX38)]);
        }
        for key in
            (0..groups).filter(|key| usize::try_from(*key).expect("key") % partitions == part)
        {
            keys.push(key);
            values.push(Some(1));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(keys)), decimal(&values, 38, 0)],
        )
        .expect("batch");
        batches.push(vec![batch]);
    }
    MemTable::try_new(schema, batches).expect("table")
}

fn spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = plan
        .metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0);
    own + plan.children().into_iter().map(spills).sum::<usize>()
}

async fn grouped_answer(
    ctx: &SessionContext,
    function: &str,
) -> Result<(Vec<(i64, Option<i128>)>, usize), String> {
    let frame = ctx
        .sql(&format!("SELECT g, {function}(v) FROM t GROUP BY g"))
        .await
        .map_err(|error| error.to_string())?;
    let plan = frame
        .create_physical_plan()
        .await
        .map_err(|error| error.to_string())?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>();
        let sums = batch.column(1).as_primitive::<Decimal128Type>();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), sums.is_valid(row).then(|| sums.value(row))));
        }
    }
    rows.sort_unstable();
    Ok((rows, spills(&plan)))
}

fn assert_marked(rows: &[(i64, Option<i128>)], groups: i64) {
    assert_eq!(rows.len(), usize::try_from(groups).expect("groups"));
    for (key, sum) in rows {
        if *key == 7 {
            assert_eq!(*sum, None, "the overflowed group is NULL");
        } else {
            assert_eq!(*sum, Some(1), "group {key}");
        }
    }
}

#[tokio::test]
async fn the_overflow_marker_survives_spill_repartition_and_skipped_partials() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(8 * 1024 * 1024, 1.0)
        .build_arc()
        .expect("runtime");
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(4096)
        .set_usize(
            "datafusion.execution.sort_spill_reservation_bytes",
            1024 * 1024,
        );
    let spilling = SessionContext::new_with_config_rt(
        crate::ansi::with_spark_ansi_config(config, false),
        runtime,
    );
    crate::register_all(&spilling);
    spilling.add_analyzer_rule(Arc::new(SparkAvgAnsiMode));
    spilling
        .register_table("t", Arc::new(grouped_table(300_000, 1)))
        .expect("register");
    for function in ["sum", "try_sum"] {
        let (rows, spilled) = grouped_answer(&spilling, function)
            .await
            .expect("spilled answer");
        assert!(spilled > 0, "{function}: the aggregate must spill");
        assert_marked(&rows, 300_000);
    }
    for skip in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1024);
        if skip {
            config = config
                .set_usize(
                    "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                    1,
                )
                .set_str(
                    "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                    "0.0",
                );
        }
        for ansi in [false, true] {
            let ctx = context(ansi, config.clone());
            ctx.register_table("t", Arc::new(grouped_table(20_000, 4)))
                .expect("register");
            for function in ["sum", "try_sum"] {
                match grouped_answer(&ctx, function).await {
                    Ok((rows, _)) => {
                        assert!(!ansi || function == "try_sum", "ANSI sum must raise");
                        assert_marked(&rows, 20_000);
                    }
                    Err(error) => {
                        assert!(ansi && function == "sum", "{error}");
                        assert!(error.contains("[ARITHMETIC_OVERFLOW]"), "{error}");
                    }
                }
            }
        }
    }
}

fn sliding_retracts(udaf: &AggregateUDF, data_type: &DataType) -> bool {
    let field: FieldRef = Arc::new(Field::new("v", data_type.clone(), true));
    let fields = vec![Arc::clone(&field)];
    let schema = Schema::new(vec![field.as_ref().clone()]);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("v", 0))];
    let coerced = udaf
        .coerce_types(std::slice::from_ref(data_type))
        .unwrap_or_else(|_| vec![data_type.clone()]);
    assert_eq!(coerced[0], *data_type, "{}: {data_type}", udaf.name());
    let return_field = udaf.return_field(&fields).expect("return field");
    udaf.create_sliding_accumulator(AccumulatorArgs {
        return_field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: udaf.name(),
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &fields,
    })
    .expect("sliding accumulator")
    .supports_retract_batch()
}

#[test]
fn every_sum_arm_and_decimal_try_sum_retract_a_sliding_frame() {
    let sum = sum_udaf();
    for data_type in [
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 0),
        DataType::Int64,
        DataType::Float64,
    ] {
        assert!(sliding_retracts(&sum, &data_type), "sum {data_type}");
    }
    let try_sum = try_sum_udaf();
    for data_type in [DataType::Decimal128(10, 2), DataType::Decimal128(38, 0)] {
        assert!(
            sliding_retracts(&try_sum, &data_type),
            "try_sum {data_type}"
        );
    }
}

#[tokio::test]
async fn non_decimal_sums_keep_datafusions_answers_and_the_linear_rewrite() {
    let ctx = context(true, SessionConfig::new());
    let wrapped = texts(&ctx, "SELECT sum(b) FROM (VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT))) t(b)").await;
    assert_eq!(wrapped, Ok(vec!["-9223372036854775808".to_string()]));
    let doubles = texts(&ctx, "SELECT sum(x) FROM (VALUES (1.5), (2.25)) t(x)").await;
    assert_eq!(doubles, Ok(vec!["3.75".to_string()]));
    let sum = sum_udaf();
    let call = |argument: Expr| match sum.call(vec![argument]) {
        Expr::AggregateFunction(function) => function,
        other => panic!("not an aggregate: {other}"),
    };
    let integer = call(col("i") + lit(1_i64));
    let rewritten = sum
        .inner()
        .simplify_expr_op_literal(&integer, &col("i"), Operator::Plus, &lit(1_i64), true)
        .expect("integer hook");
    let rewritten = rewritten
        .expect("the integer linear rewrite survives")
        .to_string();
    assert!(rewritten.contains("count(i)"), "{rewritten}");
    let tenth = lit(ScalarValue::Decimal128(Some(1), 10, 1));
    let decimal = call(col("d") + tenth.clone());
    let kept = sum
        .inner()
        .simplify_expr_op_literal(&decimal, &col("d"), Operator::Plus, &tenth, true)
        .expect("decimal hook");
    assert!(
        kept.is_none(),
        "a decimal sum is never split into sum + count"
    );
}
