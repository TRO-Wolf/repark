use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, EmitTo, Expr, GroupsAccumulator, Operator, col, lit,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::sum_decimal::{
    DecimalSum, DecimalSumDistinct, DecimalSumGroups, NARROW_PRECISION, Place, SumCheck,
};
use super::{SparkAvgAnsiMode, sum_udaf, try_sum_udaf};
use crate::avg_decimal::WideDecimal;

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
async fn sum_answers_the_exact_total_and_checks_it_once_in_both_ansi_modes() {
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
            "rows [\"5\"]",
            vec!["5".to_string()],
        ),
        (
            format!("SELECT sum(v) FROM {src} s WHERE g = 2 GROUP BY g"),
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
                "5",
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

fn carried(values: &[i128]) -> (i128, i64) {
    let mut wraps = 0;
    let low = values
        .iter()
        .fold(0_i128, |low, value| low.add_carry(*value, &mut wraps));
    (low, wraps)
}

fn wide_state(partials: &[(i128, i64)]) -> Vec<ArrayRef> {
    let lows: Vec<Option<i128>> = partials.iter().map(|(low, _)| Some(*low)).collect();
    let wraps: Vec<i64> = partials.iter().map(|(_, wraps)| *wraps).collect();
    vec![decimal(&lows, 38, 0), Arc::new(Int64Array::from(wraps))]
}

fn low_of(state: &[ArrayRef]) -> i128 {
    state[0].as_primitive::<Decimal128Type>().value(0)
}

#[test]
fn an_ungrouped_total_is_checked_once_at_evaluate_and_a_window_at_every_row() {
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
        error.to_string().contains("[ARITHMETIC_OVERFLOW]")
            && error.to_string().contains("'try_sum'"),
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
    assert_eq!(
        carried(&[MAX38, 1]),
        (low_of(&shipped(&mut single)), 0),
        "a partial past 38 digits ships its exact total"
    );
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
        .update_batch(&[decimal(&[Some(MAX38)], 38, 0)])
        .expect("update");
    let state = shipped(&mut partial);
    let wraps = state[1]
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!((low_of(&state), wraps), carried(&[MAX38, MAX38]));
    let mut merged = DecimalSum::new(38, legacy, Place::Ungrouped);
    merged.merge_batch(&state).expect("merge");
    merged
        .merge_batch(&wide_state(&[(-MAX38, 0), (-5, 0)]))
        .expect("merge");
    assert_eq!(
        merged.evaluate().expect("back inside"),
        ScalarValue::Decimal128(Some(MAX38 - 5), 38, 0)
    );
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
fn a_grouped_sum_adds_every_row_exactly_and_checks_at_evaluate() {
    let legacy = SumCheck::new(38, 0, false);
    let mut groups = DecimalSumGroups::new(38, legacy);
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
    assert_eq!((partials.value(0), partials.value(1)), (0, 5));
    assert_eq!(partials.data_type(), &DataType::Decimal128(38, 0));
    let mut merged = DecimalSumGroups::new(38, legacy);
    merged.merge_batch(&state, &[0, 1], None, 2).expect("merge");
    merged
        .merge_batch(&wide_state(&[(-1, 0), (MAX38, 0)]), &[0, 1], None, 2)
        .expect("merge");
    let result = merged.evaluate(EmitTo::All).expect("evaluate");
    let result = result.as_primitive::<Decimal128Type>();
    assert_eq!(result.value(0), -1);
    assert!(result.is_null(1), "a group past 38 digits is NULL");
    let mut ansi = DecimalSumGroups::new(38, SumCheck::new(38, 0, true));
    ansi.update_batch(&[decimal(&[Some(MAX38), Some(1)], 38, 0)], &[0, 0], None, 1)
        .expect("no row check");
    let error = ansi.evaluate(EmitTo::All).expect_err("checked at evaluate");
    assert!(
        error.to_string().contains("[ARITHMETIC_OVERFLOW]")
            && error.to_string().contains("'try_sum'"),
        "{error}"
    );
    let converted = DecimalSumGroups::new(28, SumCheck::new(38, 2, false))
        .convert_to_state(&[decimal(&[Some(7), None], 28, 2)], None)
        .expect("convert");
    assert_eq!(converted.len(), 2);
    assert_eq!(converted[0].data_type(), &DataType::Decimal128(38, 2));
    assert!(converted[0].is_null(1));
    assert_eq!(
        converted[1]
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        0
    );
}

fn merged_in_order(check: SumCheck, partials: &[(i128, i64)]) -> Result<ArrayRef, String> {
    let mut merged = DecimalSumGroups::new(38, check);
    for partial in partials {
        merged
            .merge_batch(&wide_state(&[*partial]), &[0], None, 1)
            .map_err(|error| error.to_string())?;
    }
    merged
        .evaluate(EmitTo::All)
        .map_err(|error| error.to_string())
}

fn orders<T: Copy>([a, b, c]: [T; 3]) -> [[T; 3]; 6] {
    [
        [a, b, c],
        [a, c, b],
        [b, a, c],
        [b, c, a],
        [c, a, b],
        [c, b, a],
    ]
}

#[test]
fn a_grouped_total_is_exact_in_any_partial_order_and_checked_once() {
    let wrapped_up = carried(&[MAX38, MAX38]);
    let wrapped_down = carried(&[-MAX38, -MAX38]);
    assert_eq!((wrapped_up.1, wrapped_down.1), (1, -1));
    for raise in [false, true] {
        let check = SumCheck::new(38, 0, raise);
        for (partials, want) in [
            ([(MAX38, 0), (1, 0), (-1, 0)], MAX38),
            ([wrapped_up, wrapped_down, (5, 0)], 5),
        ] {
            for order in orders(partials) {
                let result = merged_in_order(check, &order).expect("the total fits");
                assert_eq!(
                    result.as_primitive::<Decimal128Type>().value(0),
                    want,
                    "{order:?}"
                );
            }
        }
        for order in [
            [(MAX38, 0), (1, 0)],
            [(1, 0), (MAX38, 0)],
            [wrapped_up, (-MAX38 + 1, 0)],
        ] {
            match merged_in_order(check, &order) {
                Ok(result) => {
                    assert!(!raise, "{order:?}");
                    assert!(result.is_null(0), "{order:?}");
                }
                Err(error) => {
                    assert!(raise, "{error}");
                    assert!(
                        error.contains(
                            "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals. Use 'try_sum'"
                        ),
                        "{error}"
                    );
                }
            }
        }
    }
    let legacy = SumCheck::new(38, 0, false);
    for (partials, want) in [
        (vec![wrapped_up, (-MAX38, 0)], Some(MAX38 - 5)),
        (vec![wrapped_up], None),
    ] {
        let mut spilled = DecimalSumGroups::new(38, legacy);
        for partial in &partials {
            spilled
                .merge_batch(&wide_state(&[*partial]), &[0], None, 1)
                .expect("merge");
        }
        let state = spilled.state(EmitTo::All).expect("a final step's spill");
        let mut merged = DecimalSumGroups::new(38, legacy);
        merged.merge_batch(&state, &[0], None, 1).expect("re-merge");
        merged
            .merge_batch(&wide_state(&[(-5, 0)]), &[0], None, 1)
            .expect("merge");
        let result = merged.evaluate(EmitTo::All).expect("evaluate");
        let result = result.as_primitive::<Decimal128Type>();
        assert_eq!(
            result.is_valid(0).then(|| result.value(0)),
            want,
            "{partials:?}"
        );
    }
}

#[test]
fn a_wide_total_wraps_128_bits_and_comes_back_from_precision_20() {
    let near = i128::MAX - 4;
    for precision in [20_u8, 38] {
        let result_precision = precision.saturating_add(10).min(38);
        let check = SumCheck::new(result_precision, 0, true);
        let mut groups = DecimalSumGroups::new(precision, check);
        groups
            .merge_batch(&wide_state(&[(near, 0), (near, 0)]), &[0, 0], None, 1)
            .expect("two partials past 128 bits");
        let state = groups.state(EmitTo::All).expect("state");
        let wraps = state[1]
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0);
        assert_eq!((low_of(&state), wraps), carried(&[near, near]));
        let mut merged = DecimalSumGroups::new(precision, check);
        merged.merge_batch(&state, &[0], None, 1).expect("re-merge");
        merged
            .merge_batch(
                &wide_state(&[(-near, 0), (-near, 0), (9, 0)]),
                &[0, 0, 0],
                None,
                1,
            )
            .expect("back");
        let result = merged.evaluate(EmitTo::All).expect("exact");
        assert_eq!(result.as_primitive::<Decimal128Type>().value(0), 9);
        let mut ungrouped = DecimalSum::new(precision, check, Place::Ungrouped);
        for partial in [(near, 0), (near, 0), (-near, 0), (-near, 0), (9, 0)] {
            ungrouped
                .merge_batch(&wide_state(&[partial]))
                .expect("merge");
        }
        assert_eq!(
            ungrouped.evaluate().expect("exact"),
            ScalarValue::Decimal128(Some(9), result_precision, 0)
        );
    }
}

#[test]
fn a_total_at_the_38_digit_limit_fits_and_one_past_it_does_not() {
    for (values, fits) in [
        (vec![MAX38 - 1, 1], true),
        (vec![-MAX38 + 1, -1], true),
        (vec![MAX38, 1], false),
        (vec![-MAX38, -1], false),
    ] {
        for raise in [false, true] {
            let check = SumCheck::new(38, 0, raise);
            let rows = decimal(&values.iter().copied().map(Some).collect::<Vec<_>>(), 38, 0);
            let mut ungrouped = DecimalSum::new(38, check, Place::Ungrouped);
            ungrouped
                .update_batch(&[Arc::clone(&rows)])
                .expect("update");
            let mut groups = DecimalSumGroups::new(38, check);
            groups
                .update_batch(&[rows], &[0, 0], None, 1)
                .expect("update");
            let total = values.iter().sum::<i128>();
            match (ungrouped.evaluate(), groups.evaluate(EmitTo::All)) {
                (Ok(single), Ok(grouped)) => {
                    let grouped = grouped.as_primitive::<Decimal128Type>();
                    let grouped = grouped.is_valid(0).then(|| grouped.value(0));
                    let want = fits.then_some(total);
                    assert!(fits || !raise, "{values:?}");
                    assert_eq!(single, ScalarValue::Decimal128(want, 38, 0), "{values:?}");
                    assert_eq!(grouped, want, "{values:?}");
                }
                (Err(single), Err(grouped)) => {
                    assert!(raise && !fits, "{values:?}");
                    for error in [single, grouped] {
                        assert!(
                            error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
                            "{error}"
                        );
                    }
                }
                other => panic!("{values:?}: {other:?}"),
            }
        }
    }
}

fn state_columns(udaf: &AggregateUDF, data_type: &DataType) -> Vec<DataType> {
    let field: FieldRef = Arc::new(Field::new("v", data_type.clone(), true));
    let return_field = udaf
        .return_field(std::slice::from_ref(&field))
        .expect("return field");
    udaf.state_fields(StateFieldsArgs {
        name: udaf.name(),
        input_fields: &[field],
        return_field,
        ordering_fields: &[],
        is_distinct: false,
    })
    .expect("state fields")
    .iter()
    .map(|field| field.data_type().clone())
    .collect()
}

#[test]
fn the_state_is_one_column_to_precision_19_and_two_from_precision_20() {
    let wide =
        |precision: u8, scale: i8| vec![DataType::Decimal128(precision, scale), DataType::Int64];
    for udaf in [sum_udaf(), try_sum_udaf()] {
        for (input, want) in [
            ((10, 2), vec![DataType::Decimal128(20, 2)]),
            ((18, 0), vec![DataType::Decimal128(28, 0)]),
            ((19, 0), vec![DataType::Decimal128(29, 0)]),
            ((19, 19), vec![DataType::Decimal128(29, 19)]),
            ((20, 0), wide(30, 0)),
            ((28, 2), wide(38, 2)),
            ((38, 0), wide(38, 0)),
        ] {
            let data_type = DataType::Decimal128(input.0, input.1);
            assert_eq!(
                state_columns(&udaf, &data_type),
                want,
                "{} {data_type}",
                udaf.name()
            );
        }
    }
    let check = SumCheck::new(29, 0, false);
    for (precision, columns) in [(NARROW_PRECISION, 1), (NARROW_PRECISION + 1, 2)] {
        let rows = decimal(&[Some(7), None], precision, 0);
        let mut ungrouped = DecimalSum::new(precision, check, Place::Ungrouped);
        ungrouped
            .update_batch(&[Arc::clone(&rows)])
            .expect("update");
        assert_eq!(shipped(&mut ungrouped).len(), columns, "p {precision}");
        let mut groups = DecimalSumGroups::new(precision, check);
        groups
            .update_batch(&[Arc::clone(&rows)], &[0, 0], None, 1)
            .expect("update");
        assert_eq!(groups.state(EmitTo::All).expect("state").len(), columns);
        let converted = groups.convert_to_state(&[rows], None).expect("convert");
        assert_eq!(converted.len(), columns, "p {precision}");
    }
}

#[test]
fn a_narrow_state_ships_the_raw_total_and_never_a_marker() {
    let part = -9 * 10_i128.pow(36);
    for raise in [false, true] {
        let check = SumCheck::new(29, 0, raise);
        let mut partial = DecimalSum::new(19, check, Place::Ungrouped);
        let largest = 10_i128.pow(19) - 1;
        partial
            .update_batch(&[decimal(&[Some(-largest), Some(-largest)], 19, 0)])
            .expect("update");
        let state = shipped(&mut partial);
        assert_eq!((state.len(), low_of(&state)), (1, -2 * largest));
        let mut ungrouped = DecimalSum::new(19, check, Place::Ungrouped);
        let mut groups = DecimalSumGroups::new(19, check);
        for _ in 0..10 {
            ungrouped
                .merge_batch(&[decimal(&[Some(part)], 29, 0)])
                .expect("merge");
        }
        groups
            .merge_batch(&[decimal(&[Some(part); 10], 29, 0)], &[0; 10], None, 1)
            .expect("merge");
        let ungrouped_state = shipped(&mut ungrouped);
        let grouped_state = groups.state(EmitTo::All).expect("a final step's spill");
        for state in [&ungrouped_state, &grouped_state] {
            assert_eq!((state.len(), low_of(state)), (1, 10 * part));
            assert_ne!(low_of(state), i128::MIN);
        }
        groups
            .merge_batch(&grouped_state, &[0], None, 1)
            .expect("re-merge");
        match (ungrouped.evaluate(), groups.evaluate(EmitTo::All)) {
            (Ok(single), Ok(grouped)) => {
                assert!(!raise);
                assert!(single.is_null() && grouped.is_null(0));
            }
            (Err(single), Err(grouped)) => {
                assert!(raise);
                for error in [single, grouped] {
                    assert!(
                        error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
                        "{error}"
                    );
                }
            }
            other => panic!("{other:?}"),
        }
    }
    let legacy = SumCheck::new(29, 0, false);
    let mut corrupt = DecimalSumGroups::new(19, legacy);
    corrupt
        .update_batch(
            &[decimal(&[Some(i128::MAX), Some(1)], 19, 0)],
            &[0, 0],
            None,
            1,
        )
        .expect("update");
    let error = corrupt.state(EmitTo::All).expect_err("no marker, an error");
    assert!(
        error.to_string().contains("past its declared precision"),
        "{error}"
    );
    let mut corrupt = DecimalSum::new(19, legacy, Place::Ungrouped);
    corrupt
        .merge_batch(&[decimal(&[Some(i128::MAX), Some(1)], 29, 0)])
        .expect("merge");
    let error = corrupt.state().expect_err("no marker, an error");
    assert!(
        error.to_string().contains("past its declared precision"),
        "{error}"
    );
}

#[test]
fn the_wide_carrier_survives_an_arrow_ipc_round_trip() {
    let (low, wraps) = carried(&[MAX38, MAX38]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Decimal128(38, 0), true),
        Field::new("w", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            decimal(&[Some(low), Some(5), None], 38, 0),
            Arc::new(Int64Array::from(vec![wraps, 0, 0])),
        ],
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
    let read_wraps = read[0]
        .column(1)
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(
        (low_of(read[0].columns()), read_wraps.value(0)),
        (low, wraps)
    );
    let mut merged = DecimalSumGroups::new(38, SumCheck::new(38, 0, false));
    merged
        .merge_batch(read[0].columns(), &[0, 1, 2], None, 3)
        .expect("merge");
    merged
        .merge_batch(&wide_state(&[(-MAX38, 0)]), &[0], None, 3)
        .expect("merge");
    let result = merged.evaluate(EmitTo::All).expect("evaluate");
    let result = result.as_primitive::<Decimal128Type>();
    assert_eq!((result.value(0), result.value(1)), (MAX38, 5));
    assert!(result.is_null(2));
}

fn key_batch(schema: &Arc<Schema>, keys: Vec<i64>, values: &[Option<i128>]) -> RecordBatch {
    let precision = match schema.field(1).data_type() {
        DataType::Decimal128(precision, _) => *precision,
        _ => 38,
    };
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys)),
            decimal(values, precision, 0),
        ],
    )
    .expect("batch")
}

fn grouped_table(groups: i64, partitions: usize, precision: u8) -> MemTable {
    let largest = 10_i128.pow(u32::from(precision)) - 1;
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(precision, 0), true),
    ]));
    let mut batches = Vec::new();
    for part in 0..partitions {
        let (mut keys, mut values) = (Vec::new(), Vec::new());
        let (mut late_keys, mut late_values) = (Vec::new(), Vec::new());
        if part == 0 {
            late_keys.extend([7, 7, groups + 1, groups + 1, groups + 1, groups + 1]);
            late_values.extend([Some(largest); 4]);
            late_values.extend([Some(-largest), Some(-largest)]);
        }
        for (index, value) in [largest, 1, -1].into_iter().enumerate() {
            if index % partitions == part {
                late_keys.push(groups + 2);
                late_values.push(Some(value));
            }
        }
        for key in
            (0..groups).filter(|key| usize::try_from(*key).expect("key") % partitions == part)
        {
            keys.push(key);
            values.push(Some(1));
        }
        batches.push(vec![
            key_batch(&schema, keys, &values),
            key_batch(&schema, late_keys, &late_values),
        ]);
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

fn assert_exact(rows: &[(i64, Option<i128>)], groups: i64, precision: u8) {
    let largest = 10_i128.pow(u32::from(precision)) - 1;
    let limit = 10_i128.pow(u32::from(precision.saturating_add(10).min(38)));
    let group_seven = largest
        .checked_mul(2)
        .and_then(|total| total.checked_add(1))
        .filter(|total| total.abs() < limit);
    assert_eq!(rows.len(), usize::try_from(groups + 2).expect("groups"));
    for (key, sum) in rows {
        if *key == 7 {
            assert_eq!(
                *sum, group_seven,
                "p {precision} group {key}: the exact total, checked once"
            );
        } else if *key == groups + 1 {
            assert_eq!(
                *sum,
                Some(0),
                "p {precision} group {key}: past 128 bits and back"
            );
        } else if *key == groups + 2 {
            assert_eq!(*sum, Some(largest), "p {precision} group {key}");
        } else {
            assert_eq!(*sum, Some(1), "p {precision} group {key}");
        }
    }
}

#[tokio::test]
async fn the_exact_carrier_survives_spill_repartition_and_skipped_partials() {
    for precision in [NARROW_PRECISION, NARROW_PRECISION + 1, 38] {
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
            .register_table("t", Arc::new(grouped_table(300_000, 1, precision)))
            .expect("register");
        for function in ["sum", "try_sum"] {
            let (rows, spilled) = grouped_answer(&spilling, function)
                .await
                .expect("spilled answer");
            assert!(spilled > 0, "{function}: the aggregate must spill");
            assert_exact(&rows, 300_000, precision);
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
                ctx.register_table("t", Arc::new(grouped_table(20_000, 4, precision)))
                    .expect("register");
                for function in ["sum", "try_sum"] {
                    let raises = ansi && function == "sum" && precision == 38;
                    match grouped_answer(&ctx, function).await {
                        Ok((rows, _)) => {
                            assert!(!raises, "p {precision}: ANSI sum must raise");
                            assert_exact(&rows, 20_000, precision);
                        }
                        Err(error) => {
                            assert!(raises, "{error}");
                            assert!(error.contains("[ARITHMETIC_OVERFLOW]"), "{error}");
                        }
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
