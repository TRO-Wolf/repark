use super::{POW10_U128, by_text, decimal_preimage_of_double, java_decimal_to_double};

fn digit_path(unscaled: i128, scale: i8) -> f64 {
    let magnitude = by_text(unscaled.unsigned_abs(), scale);
    if unscaled < 0 { -magnitude } else { magnitude }
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn wide(&mut self) -> u128 {
        (u128::from(self.next()) << 64) | u128::from(self.next())
    }
}

fn agrees(unscaled: i128, scale: i8) {
    let fast = java_decimal_to_double(unscaled, scale);
    let text = digit_path(unscaled, scale);
    assert_eq!(
        fast.to_bits(),
        text.to_bits(),
        "{unscaled} scale {scale}: {fast:?} against {text:?}"
    );
}

#[test]
fn spark_measured_cells_round_once() {
    let cases: [(i128, i8, f64); 6] = [
        (-22_129_547_339_983_247_000, 18, -22.129_547_339_983_247),
        (1_000_000_000_000_000_400, 18, 1.000_000_000_000_000_4),
        (2_230_000_000_000_000_400, 18, 2.230_000_000_000_000_4),
        (125, 2, 1.25),
        (-1, 18, -1e-18),
        (0, 18, 0.0),
    ];
    for (unscaled, scale, expected) in cases {
        assert_eq!(
            java_decimal_to_double(unscaled, scale).to_bits(),
            expected.to_bits(),
            "{unscaled} scale {scale}"
        );
    }
}

#[test]
fn zero_is_positive() {
    assert!(java_decimal_to_double(0, 18).is_sign_positive());
    assert!(java_decimal_to_double(0, 0).is_sign_positive());
}

#[test]
fn the_text_free_path_agrees_with_the_digit_path_on_random_values() {
    let mut rng = XorShift(0x9e37_79b9_7f4a_7c15);
    for _ in 0..200_000 {
        let scale = i8::try_from(rng.next() % 39).unwrap_or(0);
        let digits = rng.next() % 39;
        let limit = POW10_U128[usize::try_from(digits).unwrap_or(0)];
        let magnitude = rng.wide() % limit.max(1);
        let unscaled = i128::try_from(magnitude).unwrap_or(0);
        let signed = if rng.next() & 1 == 0 {
            unscaled
        } else {
            -unscaled
        };
        agrees(signed, scale);
    }
}

#[test]
fn the_fast_path_agrees_with_the_digit_path_below_two_to_the_53() {
    let mut rng = XorShift(0x2545_f491_4f6c_dd1d);
    for _ in 0..200_000 {
        let scale = i8::try_from(rng.next() % 23).unwrap_or(0);
        let unscaled = i128::from(rng.next() % (1 << 53));
        agrees(unscaled, scale);
        agrees(-unscaled, scale);
    }
}

#[test]
fn halfway_points_and_their_neighbours_round_half_even() {
    let mut rng = XorShift(0x0123_4567_89ab_cdef);
    let mut checked = 0;
    for _ in 0..50_000 {
        let exponent = -i32::try_from(rng.next() % 31).unwrap_or(0);
        let mantissa = (1u64 << 52) | (rng.next() & ((1u64 << 52) - 1));
        let sum = u128::from(mantissa) * 2 + 1;
        let scale = 1 - exponent;
        let Some(five) = 5u128.checked_pow(scale.unsigned_abs()) else {
            continue;
        };
        let Some(midpoint) = sum.checked_mul(five) else {
            continue;
        };
        if midpoint >= POW10_U128[38] {
            continue;
        }
        let scale = i8::try_from(scale).unwrap_or(0);
        let unscaled = i128::try_from(midpoint).unwrap_or(0);
        for delta in [-1, 0, 1] {
            agrees(unscaled + delta, scale);
            agrees(-(unscaled + delta), scale);
        }
        checked += 1;
    }
    assert!(checked > 10_000, "{checked}");
}

#[test]
fn extreme_magnitudes_agree() {
    for scale in [0, 1, 18, 37, 38] {
        for unscaled in [
            i128::MAX,
            i128::MIN + 1,
            i128::MIN,
            99_999_999_999_999_999_999_999_999_999_999_999_999,
            1,
            -1,
            (1 << 53) - 1,
            1 << 53,
            (1 << 53) + 1,
        ] {
            agrees(unscaled, scale);
        }
    }
    agrees(12_345, -3);
}

const OPERATORS: [&str; 8] = ["=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM", "<=>"];

fn per_row(value: f64, operator: &str, constant: f64) -> bool {
    let order = value.total_cmp(&constant);
    match operator {
        "=" | "<=>" => order.is_eq(),
        "<>" | "IS DISTINCT FROM" => order.is_ne(),
        "<" => order.is_lt(),
        "<=" => order.is_le(),
        ">" => order.is_gt(),
        _ => order.is_ge(),
    }
}

fn rewritten(unscaled: i128, operator: &str, lower: i128, upper: i128) -> bool {
    match operator {
        "=" | "<=>" => unscaled >= lower && unscaled < upper,
        "<>" | "IS DISTINCT FROM" => unscaled < lower || unscaled >= upper,
        "<" => unscaled < lower,
        "<=" => unscaled < upper,
        ">" => unscaled >= upper,
        _ => unscaled >= lower,
    }
}

fn swapped(operator: &str) -> &str {
    match operator {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        symmetric => symmetric,
    }
}

fn top_of(precision: u8) -> i128 {
    i128::try_from(POW10_U128[usize::from(precision)] - 1).unwrap_or(0)
}

fn check_at(unscaled: i128, scale: i8, constant: f64, lower: i128, upper: i128) {
    check_value(
        unscaled,
        java_decimal_to_double(unscaled, scale),
        constant,
        lower,
        upper,
    );
}

fn check_value(unscaled: i128, value: f64, constant: f64, lower: i128, upper: i128) {
    for operator in OPERATORS {
        let expected = per_row(value, operator, constant);
        assert_eq!(
            rewritten(unscaled, operator, lower, upper),
            expected,
            "{unscaled} ({value:?}) {operator} {constant:?}: [{lower}, {upper})"
        );
        let mirrored = per_row(constant, swapped(operator), value);
        assert_eq!(
            mirrored, expected,
            "{constant:?} {operator} {unscaled} ({value:?}), mirrored"
        );
    }
}

fn bounds_or_top(constant: f64, precision: u8, scale: i8) -> Option<(i128, i128)> {
    let found = decimal_preimage_of_double(constant, precision, scale);
    let top_value = java_decimal_to_double(top_of(precision), scale);
    if !constant.is_finite() || constant.total_cmp(&top_value).is_ge() {
        assert_eq!(found, None, "{constant:?} at ({precision}, {scale})");
    } else {
        assert!(found.is_some(), "{constant:?} at ({precision}, {scale})");
    }
    found
}

fn check_boundaries(constant: f64, precision: u8, scale: i8, lower: i128, upper: i128) {
    let top = top_of(precision);
    assert!(-top <= lower && lower <= upper && upper <= top);
    for unscaled in [
        lower - 2,
        lower - 1,
        lower,
        lower + 1,
        upper - 2,
        upper - 1,
        upper,
        upper + 1,
    ]
    .into_iter()
    .chain([-top, top])
    .filter(|unscaled| (-top..=top).contains(unscaled))
    {
        check_at(unscaled, scale, constant, lower, upper);
    }
}

fn constants_around(values: &[f64]) -> Vec<f64> {
    let mut constants = vec![0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
    for pair in values.windows(2) {
        constants.push(f64::midpoint(pair[0], pair[1]));
    }
    for &value in values {
        constants.extend([value, value.next_up(), value.next_down()]);
    }
    if let (Some(&first), Some(&last)) = (values.first(), values.last()) {
        constants.extend([
            first.next_down(),
            last.next_up(),
            first * 2.0 - 1.0,
            last * 2.0 + 1.0,
        ]);
        constants.extend([-1e300, 1e300, f64::MIN, f64::MAX]);
    }
    constants
}

#[test]
fn the_conversion_never_decreases_over_every_value_to_precision_four() {
    for precision in 1..=4u8 {
        let top = top_of(precision);
        for scale in 0..=i8::try_from(precision).unwrap_or(0) {
            let mut previous = java_decimal_to_double(-top, scale);
            for unscaled in -top + 1..=top {
                let value = java_decimal_to_double(unscaled, scale);
                assert!(
                    previous.total_cmp(&value).is_lt(),
                    "{unscaled} at ({precision}, {scale})"
                );
                previous = value;
            }
        }
    }
}

#[test]
fn the_preimage_equals_the_per_row_form_on_every_value_to_precision_three() {
    for precision in 1..=3u8 {
        let top = top_of(precision);
        for scale in 0..=i8::try_from(precision).unwrap_or(0) {
            let values: Vec<f64> = (-top..=top)
                .map(|unscaled| java_decimal_to_double(unscaled, scale))
                .collect();
            for constant in constants_around(&values) {
                let Some((lower, upper)) = bounds_or_top(constant, precision, scale) else {
                    continue;
                };
                for (unscaled, &value) in (-top..=top).zip(&values) {
                    check_value(unscaled, value, constant, lower, upper);
                }
            }
        }
    }
}

#[test]
fn the_preimage_sits_on_the_per_row_boundaries_at_precision_four() {
    let precision = 4;
    let top = top_of(precision);
    for scale in 0..=4 {
        let values: Vec<f64> = (-top..=top)
            .map(|unscaled| java_decimal_to_double(unscaled, scale))
            .collect();
        for constant in constants_around(&values) {
            if let Some((lower, upper)) = bounds_or_top(constant, precision, scale) {
                check_boundaries(constant, precision, scale, lower, upper);
            }
        }
    }
}

#[test]
fn the_preimage_equals_the_per_row_form_on_random_constants_at_wide_types() {
    let mut rng = XorShift(0x5851_f42d_4c95_7f2d);
    for (precision, scale) in [(38u8, 18i8), (38, 0), (20, 10), (10, 2)] {
        let top = top_of(precision);
        let span = top.unsigned_abs() * 2 + 1;
        let pick = |rng: &mut XorShift| {
            let offset = i128::try_from(rng.wide() % span).unwrap_or(0);
            offset - top
        };
        for round in 0..20_000 {
            let anchor = pick(&mut rng);
            let near = java_decimal_to_double(anchor, scale);
            let steps = rng.next() % 7;
            let mut constant = near;
            for _ in 0..steps {
                constant = if rng.next() & 1 == 0 {
                    constant.next_up()
                } else {
                    constant.next_down()
                };
            }
            if round % 5 == 0 {
                let exponent = i32::try_from(rng.next() % 80).unwrap_or(0) - 40;
                let sign = if rng.next() & 1 == 0 { 1.0 } else { -1.0 };
                let mantissa = f64::from_bits((rng.next() >> 12) | 0x3ff0_0000_0000_0000);
                constant = sign * mantissa * 10f64.powi(exponent);
            }
            let Some((lower, upper)) = bounds_or_top(constant, precision, scale) else {
                continue;
            };
            check_boundaries(constant, precision, scale, lower, upper);
            for _ in 0..4 {
                check_at(pick(&mut rng), scale, constant, lower, upper);
            }
        }
        for constant in [0.0, -0.0] {
            let (lower, upper) = bounds_or_top(constant, precision, scale).unwrap_or((0, 0));
            assert_eq!((lower, upper), (0, i128::from(constant.is_sign_positive())));
            check_boundaries(constant, precision, scale, lower, upper);
        }
    }
}

#[test]
fn the_preimage_refuses_what_it_cannot_bound() {
    for constant in [f64::NAN, -f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(decimal_preimage_of_double(constant, 10, 2), None);
    }
    let top = java_decimal_to_double(top_of(10), 2);
    assert_eq!(decimal_preimage_of_double(top, 10, 2), None);
    assert_eq!(decimal_preimage_of_double(top.next_up(), 10, 2), None);
    assert_eq!(
        decimal_preimage_of_double(top.next_down(), 10, 2),
        Some((top_of(10), top_of(10)))
    );
    assert_eq!(decimal_preimage_of_double(0.5, 0, 0), None);
    assert_eq!(decimal_preimage_of_double(0.5, 39, 0), None);
    let bottom = -top_of(10);
    assert_eq!(
        decimal_preimage_of_double(-1e300, 10, 2),
        Some((bottom, bottom))
    );
    assert_eq!(decimal_preimage_of_double(0.5, 10, 2), Some((50, 51)));
    assert_eq!(decimal_preimage_of_double(0.505, 10, 2), Some((51, 51)));
}
