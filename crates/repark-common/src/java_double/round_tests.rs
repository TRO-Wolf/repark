use super::{
    JavaRounding, Rounded, digits_round, dtoa_double_bits, java_double_round, java_float_round,
    with_decimal_text,
};

const UP: JavaRounding = JavaRounding::HalfUp;
const EVEN: JavaRounding = JavaRounding::HalfEven;

fn reference_f64(value: f64, scale: i32, mode: JavaRounding) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if value == 0.0 {
        return 0.0;
    }
    match digits_round(&dtoa_double_bits(value.to_bits()), scale, mode) {
        Rounded::Unchanged => value,
        Rounded::Kept(0) => 0.0,
        Rounded::Kept(kept) => {
            let magnitude =
                with_decimal_text(kept, scale, |text| text.parse::<f64>().ok()).unwrap_or(f64::NAN);
            if value.is_sign_negative() {
                -magnitude
            } else {
                magnitude
            }
        }
    }
}

fn reference_f32(value: f32, scale: i32, mode: JavaRounding) -> f32 {
    if !value.is_finite() {
        return value;
    }
    if value == 0.0 {
        return 0.0;
    }
    match digits_round(&dtoa_double_bits(f64::from(value).to_bits()), scale, mode) {
        Rounded::Unchanged => value,
        Rounded::Kept(0) => 0.0,
        Rounded::Kept(kept) => {
            let magnitude =
                with_decimal_text(kept, scale, |text| text.parse::<f32>().ok()).unwrap_or(f32::NAN);
            if value.is_sign_negative() {
                -magnitude
            } else {
                magnitude
            }
        }
    }
}

fn round64(value: f64, scale: i32, mode: JavaRounding) -> f64 {
    java_double_round(value, scale, mode).unwrap_or(f64::NAN)
}

fn round32(value: f32, scale: i32, mode: JavaRounding) -> f32 {
    java_float_round(value, scale, mode).unwrap_or(f32::NAN)
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn spark_measured_double_cells() {
    let cells: [(f64, i32, JavaRounding, f64); 22] = [
        (1.005, 2, UP, 1.01),
        (0.285, 2, UP, 0.29),
        (1.255, 2, UP, 1.26),
        (2.675, 2, UP, 2.68),
        (2.675, 2, EVEN, 2.68),
        (0.125, 2, EVEN, 0.12),
        (0.125, 2, UP, 0.13),
        (-1.005, 2, UP, -1.01),
        (-22.129_547_339_983_247, 18, UP, -22.129_547_339_983_247),
        (1.000_000_000_000_000_4, 18, UP, 1.000_000_000_000_000_4),
        (1e307, 2, UP, 1e307),
        (f64::MAX, 1, UP, f64::MAX),
        (1.949_133_209_895_265e16, -2, UP, 1.949_133_209_895_26e16),
        (1250.0, -2, UP, 1300.0),
        (1250.0, -2, EVEN, 1200.0),
        (123_456_789_012_345.67, 1, UP, 123_456_789_012_345.7),
        (1.005, 309, UP, 1.005),
        (1.005, 400, UP, 1.005),
        (1.5, -400, UP, 0.0),
        (2.5, 0, UP, 3.0),
        (2.5, 0, EVEN, 2.0),
        (-2.5, 0, UP, -3.0),
    ];
    for (value, scale, mode, want) in cells {
        assert_eq!(
            round64(value, scale, mode).to_bits(),
            want.to_bits(),
            "{value} at {scale} {mode:?}"
        );
    }
}

#[test]
fn zero_results_are_positive_zero() {
    for (value, scale) in [
        (-0.0f64, 2),
        (-0.4, 0),
        (-0.0004, 2),
        (-5e-324, 2),
        (-2.5e-18, 17),
        (-1e300, -400),
    ] {
        for mode in [UP, EVEN] {
            assert_eq!(
                round64(value, scale, mode).to_bits(),
                0,
                "{value} at {scale}"
            );
        }
    }
    for (value, scale) in [(-0.0f32, 2), (-0.4, 0), (-0.0004, 2)] {
        for mode in [UP, EVEN] {
            assert_eq!(
                round32(value, scale, mode).to_bits(),
                0,
                "{value} at {scale}"
            );
        }
    }
}

#[test]
fn non_finite_values_pass_through() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(round64(value, 2, UP).to_bits(), value.to_bits());
    }
    for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        assert_eq!(round32(value, 2, EVEN).to_bits(), value.to_bits());
    }
}

#[test]
fn float_rounds_the_widened_double_text_and_converts_once() {
    let cells: [(f32, i32, JavaRounding, f32); 6] = [
        (1.255, 2, UP, 1.25),
        (1.255, 2, EVEN, 1.25),
        (1.005, 2, UP, 1.0),
        (2.5, 0, EVEN, 2.0),
        (16_777_217.0, -1, UP, 16_777_220.0),
        (1e10, 2, UP, 1e10),
    ];
    for (value, scale, mode, want) in cells {
        assert_eq!(
            round32(value, scale, mode).to_bits(),
            want.to_bits(),
            "{value} at {scale} {mode:?}"
        );
    }
}

#[test]
fn every_fast_path_equals_the_digit_path() {
    let mut rng = XorShift(0x9e37_79b9_7f4a_7c15);
    for _ in 0..200_000 {
        let bits = rng.next();
        let exponent = 1023 - 70 + (bits >> 52) % 140;
        let value = f64::from_bits((exponent << 52) | (rng.next() & ((1 << 52) - 1)));
        let value = if bits & 1 == 1 { -value } else { value };
        let scale = i32::try_from(rng.next() % 50).unwrap_or(0) - 25;
        for mode in [UP, EVEN] {
            assert_eq!(
                round64(value, scale, mode).to_bits(),
                reference_f64(value, scale, mode).to_bits(),
                "{value:e} at {scale} {mode:?}"
            );
            let narrow = f32::from_bits(u32::try_from(rng.next() >> 33).unwrap_or(0));
            if narrow.is_finite() {
                assert_eq!(
                    round32(narrow, scale, mode).to_bits(),
                    reference_f32(narrow, scale, mode).to_bits(),
                    "{narrow:e} at {scale} {mode:?}"
                );
            }
        }
    }
}

#[test]
fn halves_and_their_neighbours_equal_the_digit_path() {
    let mut rng = XorShift(0x2545_f491_4f6c_dd1d);
    for scale in -22i32..=22 {
        for _ in 0..400 {
            let digits = rng.next() % 1_000_000_000_000;
            let text = format!("{digits}5e{}", -scale - 1);
            let Ok(nearest) = text.parse::<f64>() else {
                continue;
            };
            let Ok(narrow) = text.parse::<f32>() else {
                continue;
            };
            for step in -3i64..=3 {
                let wide = f64::from_bits(nearest.to_bits().saturating_add_signed(step));
                let thin = f32::from_bits(
                    narrow
                        .to_bits()
                        .saturating_add_signed(i32::try_from(step).unwrap_or(0)),
                );
                for mode in [UP, EVEN] {
                    for value in [wide, -wide] {
                        assert_eq!(
                            round64(value, scale, mode).to_bits(),
                            reference_f64(value, scale, mode).to_bits(),
                            "{value:e} at {scale} {mode:?}"
                        );
                    }
                    if thin.is_finite() {
                        assert_eq!(
                            round32(thin, scale, mode).to_bits(),
                            reference_f32(thin, scale, mode).to_bits(),
                            "{thin:e} at {scale} {mode:?}"
                        );
                    }
                }
            }
        }
    }
}
