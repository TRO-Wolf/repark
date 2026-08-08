use super::*;

#[test]
fn epsilon_guards_match_c_macros() {
    assert!(is_zero(0.0));
    assert!(is_zero(0.000_000_009));
    assert!(is_zero(-0.000_000_009));
    assert!(!is_zero(0.000_000_01));
    assert!(is_zero_or_neg(-5.0));
    assert!(is_zero_or_neg(0.000_000_009));
    assert!(!is_zero_or_neg(0.000_000_01));
}

#[test]
fn true_range_picks_greatest_of_three() {
    assert!((true_range(10.0, 8.0, 9.0) - 2.0).abs() < f64::EPSILON);
    assert!((true_range(10.0, 8.0, 12.0) - 4.0).abs() < f64::EPSILON);
    assert!((true_range(10.0, 8.0, 5.0) - 5.0).abs() < f64::EPSILON);
}
