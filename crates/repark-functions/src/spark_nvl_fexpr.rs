use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, ScalarUDF};

use crate::csv::default_timestamp_micros;
use crate::spark_nvl_udf::SparkNvl2;

#[must_use]
pub fn nvl2_fexpr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNvl2::new_fexpr()))
}

pub(crate) fn utc_fold_nullif_literals(first: &Expr, second: &Expr) -> Option<(Expr, Expr)> {
    let (text, stamp) = string_stamp_pair(first, second)?;
    let want = utc_micros(text)?;
    let (unit, have, zone) = stamp_micros(stamp)?;
    if want != have {
        return None;
    }
    let off = Expr::Literal(null_stamp(unit, &zone), None);
    let Expr::Literal(first_value, _) = first else {
        return None;
    };
    let on = Expr::Literal(null_scalar(first_value), None);
    Some((off, on))
}

fn string_stamp_pair<'a>(first: &'a Expr, second: &'a Expr) -> Option<(&'a str, &'a ScalarValue)> {
    let (Expr::Literal(first_value, _), Expr::Literal(second_value, _)) = (first, second) else {
        return None;
    };
    if let Some(text) = literal_text(first_value) {
        return Some((text, second_value));
    }
    if let Some(text) = literal_text(second_value) {
        return Some((text, first_value));
    }
    None
}

fn literal_text(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Some(text.as_str()),
        _ => None,
    }
}

fn stamp_micros(value: &ScalarValue) -> Option<(TimeUnit, i64, Arc<str>)> {
    let (unit, ticks, zone) = match value {
        ScalarValue::TimestampSecond(Some(ticks), zone) => (TimeUnit::Second, ticks, zone),
        ScalarValue::TimestampMillisecond(Some(ticks), zone) => {
            (TimeUnit::Millisecond, ticks, zone)
        }
        ScalarValue::TimestampMicrosecond(Some(ticks), zone) => {
            (TimeUnit::Microsecond, ticks, zone)
        }
        ScalarValue::TimestampNanosecond(Some(ticks), zone) => (TimeUnit::Nanosecond, ticks, zone),
        _ => return None,
    };
    if zone.as_deref() != Some("UTC") {
        return None;
    }
    let micros = match unit {
        TimeUnit::Second => ticks.checked_mul(1_000_000)?,
        TimeUnit::Millisecond => ticks.checked_mul(1_000)?,
        TimeUnit::Microsecond => *ticks,
        TimeUnit::Nanosecond => ticks / 1_000,
    };
    Some((
        unit,
        micros,
        zone.clone().unwrap_or_else(|| Arc::from("UTC")),
    ))
}

fn utc_micros(text: &str) -> Option<i64> {
    let trimmed = text.trim_ascii();
    if let Some(bare) = trimmed
        .strip_suffix('Z')
        .or_else(|| trimmed.strip_suffix('z'))
    {
        let naive = chrono::NaiveDateTime::parse_from_str(bare, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
        return Some(naive.and_utc().timestamp_micros());
    }
    let zone: Tz = "UTC".parse().ok()?;
    default_timestamp_micros(trimmed, Some(zone))
}

fn null_stamp(unit: TimeUnit, zone: &Arc<str>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(None, Some(Arc::clone(zone))),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(None, Some(Arc::clone(zone))),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(None, Some(Arc::clone(zone))),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(None, Some(Arc::clone(zone))),
    }
}

fn null_scalar(value: &ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Utf8(_) => ScalarValue::Utf8(None),
        ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(None),
        ScalarValue::Utf8View(_) => ScalarValue::Utf8View(None),
        ScalarValue::TimestampSecond(_, zone) => ScalarValue::TimestampSecond(None, zone.clone()),
        ScalarValue::TimestampMillisecond(_, zone) => {
            ScalarValue::TimestampMillisecond(None, zone.clone())
        }
        ScalarValue::TimestampMicrosecond(_, zone) => {
            ScalarValue::TimestampMicrosecond(None, zone.clone())
        }
        ScalarValue::TimestampNanosecond(_, zone) => {
            ScalarValue::TimestampNanosecond(None, zone.clone())
        }
        other => ScalarValue::try_from(other.data_type()).unwrap_or(ScalarValue::Null),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;

    use super::*;

    fn stamp(text: &str) -> Expr {
        let zone: Tz = "UTC".parse().expect("utc zone");
        let micros = default_timestamp_micros(text, Some(zone)).expect("parse stamp");
        Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(micros), Some(Arc::from("UTC"))),
            None,
        )
    }

    fn text(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_owned())), None)
    }

    #[test]
    fn equal_pair_folds_with_door_types() {
        let (off, on) =
            utc_fold_nullif_literals(&stamp("2024-01-02 03:04:05"), &text("2024-01-02 03:04:05"))
                .expect("equal pair folds");
        assert_eq!(
            off.get_type(&datafusion::common::DFSchema::empty()).ok(),
            Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC"))
            ))
        );
        assert_eq!(
            on.get_type(&datafusion::common::DFSchema::empty()).ok(),
            Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC"))
            ))
        );
    }

    #[test]
    fn reversed_pair_keeps_first_type_on() {
        let (off, on) =
            utc_fold_nullif_literals(&text("2024-01-02 03:04:05"), &stamp("2024-01-02 03:04:05"))
                .expect("reversed pair folds");
        assert!(matches!(
            off.get_type(&datafusion::common::DFSchema::empty()),
            Ok(DataType::Timestamp(TimeUnit::Microsecond, _))
        ));
        assert_eq!(
            on.get_type(&datafusion::common::DFSchema::empty()).ok(),
            Some(DataType::Utf8)
        );
    }

    #[test]
    fn zulu_text_folds() {
        let folded =
            utc_fold_nullif_literals(&stamp("2024-01-02 03:04:05"), &text("2024-01-02T03:04:05Z"));
        assert!(folded.is_some(), "zulu text parses in utc");
    }

    #[test]
    fn unequal_unparsable_and_other_zone_pairs_pass_through() {
        assert!(
            utc_fold_nullif_literals(&stamp("2024-01-02 03:04:05"), &text("2024-01-03 03:04:05"))
                .is_none()
        );
        assert!(
            utc_fold_nullif_literals(&stamp("2024-01-02 03:04:05"), &text("not a stamp")).is_none()
        );
        let ntz = Expr::Literal(ScalarValue::TimestampMicrosecond(Some(0), None), None);
        assert!(utc_fold_nullif_literals(&ntz, &text("1970-01-01 00:00:00")).is_none());
        assert!(utc_fold_nullif_literals(&text("a"), &text("a")).is_none());
    }
}
