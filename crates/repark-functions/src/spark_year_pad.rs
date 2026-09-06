#[must_use]
pub(crate) fn format_year(year: i32, count: usize) -> String {
    if count == 2 {
        format!("{:02}", year.unsigned_abs() % 100)
    } else if year < 0 {
        format!("-{:0width$}", year.unsigned_abs(), width = count)
    } else {
        let rendered = format!("{year:0count$}");
        if count >= 4 && rendered.len() > count {
            format!("+{rendered}")
        } else {
            rendered
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pads_negative_year_digits_after_the_sign() {
        assert_eq!(format_year(-499, 4), "-0499");
        assert_eq!(format_year(-2, 4), "-0002");
        assert_eq!(format_year(-499, 5), "-00499");
        assert_eq!(format_year(-499, 3), "-499");
        assert_eq!(format_year(-499, 1), "-499");
        assert_eq!(format_year(-2, 1), "-2");
        assert_eq!(format_year(-499, 2), "99");
        assert_eq!(format_year(-501, 2), "01");
        assert_eq!(format_year(-2, 2), "02");
    }

    #[test]
    fn keeps_positive_year_spelling() {
        assert_eq!(format_year(1970, 4), "1970");
        assert_eq!(format_year(5, 4), "0005");
        assert_eq!(format_year(1970, 2), "70");
        assert_eq!(format_year(51190, 4), "+51190");
        assert_eq!(format_year(1234, 5), "01234");
    }
}
