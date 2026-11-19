use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Accumulator;

use super::sum_decimal::{DecimalSum, NARROW_PRECISION, Place, SumCheck};

const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

fn decimal(values: &[Option<i128>], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .expect("decimal fixture"),
    )
}

type Case = (Vec<Option<i128>>, (i128, i64));

fn past_precision() -> [Case; 4] {
    let rest = 5_i128.wrapping_sub(MAX38.wrapping_mul(3));
    let wrap5 = vec![Some(MAX38), Some(MAX38), Some(MAX38), Some(rest)];
    let neg = wrap5.iter().map(|value| value.map(|v| -v)).collect();
    let nulls = vec![
        Some(MAX38),
        None,
        Some(MAX38),
        Some(MAX38),
        None,
        Some(rest),
    ];
    let fit = vec![Some(MAX38), Some(MAX38), Some(-MAX38), Some(7)];
    [
        (wrap5, (5, 1)),
        (neg, (-5, -1)),
        (nulls, (5, 1)),
        (fit, (MAX38 + 7, 0)),
    ]
}

#[test]
fn an_ungrouped_batch_past_its_declared_precision_counts_every_wrap() {
    for precision in [NARROW_PRECISION, NARROW_PRECISION + 1] {
        for (rows, (low, wraps)) in past_precision() {
            let values = decimal(&rows, precision, 0);
            for raise in [false, true] {
                let check = SumCheck::new(precision + 10, 0, raise);
                let mut single = DecimalSum::new(precision, check, Place::Ungrouped);
                single.update_batch(&[Arc::clone(&values)]).expect("update");
                match single.evaluate() {
                    Ok(value) => assert!(!raise && value.is_null(), "p{precision} {rows:?}"),
                    Err(error) => assert!(
                        raise && error.to_string().contains("[ARITHMETIC_OVERFLOW]"),
                        "p{precision} {rows:?}: {error}"
                    ),
                }
                match (single.state(), precision > NARROW_PRECISION) {
                    (Ok(state), true) => {
                        assert_eq!(
                            state[0],
                            ScalarValue::Decimal128(Some(low), precision + 10, 0)
                        );
                        assert_eq!(state[1], ScalarValue::Int64(Some(wraps)));
                    }
                    (Ok(state), false) => {
                        assert_eq!(wraps, 0, "p{precision} {rows:?}");
                        assert_eq!(state, vec![ScalarValue::Decimal128(Some(low), 29, 0)]);
                    }
                    (Err(error), wide) => assert!(
                        !wide
                            && wraps != 0
                            && error.to_string().contains("past its declared precision"),
                        "p{precision} {rows:?}: {error}"
                    ),
                }
            }
        }
        let check = SumCheck::new(precision + 10, 0, true);
        let mut window = DecimalSum::new(precision, check, Place::Window);
        let (rows, _) = past_precision()[0].clone();
        for row in rows.iter().chain(&[Some(3)]) {
            window
                .update_batch(&[decimal(&[*row], precision, 0)])
                .expect("update");
        }
        let error = window.evaluate().expect_err("a frame past 128 bits");
        assert!(
            error.to_string().contains("[NUMERIC_VALUE_OUT_OF_RANGE]"),
            "{error}"
        );
        window
            .retract_batch(&[decimal(&rows, precision, 0)])
            .expect("retract");
        assert_eq!(
            window.evaluate().expect("back inside"),
            ScalarValue::Decimal128(Some(3), precision + 10, 0)
        );
    }
}
