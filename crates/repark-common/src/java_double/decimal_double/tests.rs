use super::{POW10_U128, by_text, java_decimal_to_double};

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
