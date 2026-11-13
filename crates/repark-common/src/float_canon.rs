#[must_use]
pub fn canonical_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value + 0.0
    }
}

#[must_use]
pub fn canonical_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else {
        value + 0.0
    }
}

#[must_use]
pub fn is_canonical_f64(value: f64) -> bool {
    value.to_bits() == canonical_f64(value).to_bits()
}

#[must_use]
pub fn is_canonical_f32(value: f32) -> bool {
    value.to_bits() == canonical_f32(value).to_bits()
}

#[cfg(test)]
mod tests {
    use super::{canonical_f32, canonical_f64, is_canonical_f32, is_canonical_f64};

    #[test]
    fn zeros_and_nans_take_one_value_and_everything_else_keeps_its_bits() {
        assert_eq!(canonical_f64(-0.0).to_bits(), 0.0f64.to_bits());
        assert_eq!(canonical_f32(-0.0).to_bits(), 0.0f32.to_bits());
        assert_eq!(canonical_f64(-f64::NAN).to_bits(), f64::NAN.to_bits());
        assert_eq!(
            canonical_f64(f64::from_bits(0x7ff8_0000_0000_0001)).to_bits(),
            f64::NAN.to_bits()
        );
        assert_eq!(canonical_f32(-f32::NAN).to_bits(), f32::NAN.to_bits());
        for value in [
            1.5,
            -2.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            -5e-324,
        ] {
            assert_eq!(canonical_f64(value).to_bits(), value.to_bits());
            assert!(is_canonical_f64(value));
        }
        assert!(!is_canonical_f64(-0.0));
        assert!(!is_canonical_f64(-f64::NAN));
        assert!(is_canonical_f64(f64::NAN));
        assert!(!is_canonical_f32(-0.0));
        assert!(is_canonical_f32(f32::NAN));
    }
}
