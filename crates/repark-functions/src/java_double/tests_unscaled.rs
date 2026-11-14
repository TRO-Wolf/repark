use repark_common::java_double::java_double_unscaled;

#[test]
fn the_kernel_rounds_the_jdk_17_text_half_up() {
    let widened = |value: f32| f64::from(value);
    for (value, scale, want) in [
        (1.255, 2, Some(126)),
        (1.005, 2, Some(101)),
        (2.675, 2, Some(268)),
        (9.995, 2, Some(1000)),
        (-1.255, 2, Some(-126)),
        (0.125, 2, Some(13)),
        (-0.0, 2, Some(0)),
        (1e-20, 18, Some(0)),
        (1.254_999_999_999_999_7, 2, Some(125)),
        (0.300_000_000_000_000_04, 18, Some(300_000_000_000_000_040)),
        (1.005, 18, Some(1_005_000_000_000_000_000)),
        (1e23, 0, Some(99_999_999_999_999_990_000_000)),
        (
            2.828_793_848_061_59e17,
            18,
            Some(282_879_384_806_159_008 * 10_i128.pow(18)),
        ),
        (widened(1.255), 2, Some(125)),
        (widened(1.255), 18, Some(1_254_999_995_231_628_400)),
        (1.255, 30, Some(1255 * 10_i128.pow(27))),
        (1e300, 2, None),
        (f64::NAN, 2, None),
        (f64::INFINITY, 0, None),
    ] {
        assert_eq!(
            java_double_unscaled(value, scale),
            want,
            "{value:?} at scale {scale}"
        );
    }
}

fn half_up_from_text(text: &str, scale: i8) -> i128 {
    let (negative, body) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (mantissa, exponent) = body
        .split_once('E')
        .map_or((body, 0), |(m, e)| (m, e.parse::<i32>().unwrap()));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = format!("{whole}{fraction}");
    let point = i32::try_from(whole.len()).unwrap() + exponent + i32::from(scale);
    let padded = if point > i32::try_from(digits.len()).unwrap() {
        format!(
            "{digits}{}",
            "0".repeat(usize::try_from(point).unwrap() - digits.len())
        )
    } else {
        digits
    };
    let (kept, next) = if point <= 0 {
        (
            String::from("0"),
            if point == 0 {
                padded.chars().next()
            } else {
                None
            },
        )
    } else {
        let cut = usize::try_from(point).unwrap();
        (padded[..cut].to_string(), padded.chars().nth(cut))
    };
    let mut value: i128 = kept.parse().unwrap();
    if next.is_some_and(|digit| digit >= '5') {
        value += 1;
    }
    if negative { -value } else { value }
}

#[test]
fn the_fast_path_agrees_with_the_text_on_a_dense_sweep() {
    let mut value = 0.000_1_f64;
    let mut checked = 0;
    while value < 2_000.0 {
        for scale in [0_i8, 1, 2, 3, 4, 6, 9] {
            for signed in [value, -value] {
                let text = super::java_double_text(signed);
                assert_eq!(
                    java_double_unscaled(signed, scale),
                    Some(half_up_from_text(&text, scale)),
                    "{text} at scale {scale}"
                );
                checked += 1;
            }
        }
        value = value * 1.000_37 + 0.000_013;
    }
    for thousandths in 0..300_000_u32 {
        let value = f64::from(thousandths) / 1000.0;
        for scale in [0_i8, 1, 2, 15] {
            let text = super::java_double_text(value);
            assert_eq!(
                java_double_unscaled(value, scale),
                Some(half_up_from_text(&text, scale)),
                "{text} at scale {scale}"
            );
            checked += 1;
        }
    }
    assert!(checked > 1_000_000, "{checked}");
}
