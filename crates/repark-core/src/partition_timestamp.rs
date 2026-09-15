use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};

pub(crate) fn parse_wall_naive(raw: &str) -> Option<NaiveDateTime> {
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return date.and_hms_opt(0, 0, 0);
    }
    let (date_text, time_text) = raw.split_once(' ').or_else(|| raw.split_once('T'))?;
    let date = NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()?;
    let (clock, fraction) = match time_text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time_text, None),
    };
    let time = NaiveTime::parse_from_str(clock, "%H:%M:%S").ok()?;
    let nanos: i64 = match fraction {
        None => 0,
        Some(text) => {
            if text.is_empty() || text.len() > 9 || !text.bytes().all(|byte| byte.is_ascii_digit())
            {
                return None;
            }
            format!("{text:0<9}").parse().ok()?
        }
    };
    NaiveDateTime::new(date, time).checked_add_signed(TimeDelta::nanoseconds(nanos))
}

pub(crate) fn parse_timestamp_ntz_micros(raw: &str) -> Option<i64> {
    let wall = parse_wall_naive(raw)?;
    let utc = wall.and_utc();
    utc.timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(utc.timestamp_subsec_micros()))
}

pub(crate) fn looks_like_timestamp(text: &str) -> bool {
    text.len() == 19 && NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use arrow::datatypes::{DataType, TimeUnit};

    use crate::partition_discovery::{
        PartitionValue, cast_raw_partition_value, discover_partitions,
    };

    fn ny_zone() -> arrow::array::timezone::Tz {
        "America/New_York"
            .parse()
            .expect("NY parses as a session zone")
    }

    fn leaf(root: &std::path::Path, segments: &[&str], name: &str) -> PathBuf {
        let mut path = root.to_path_buf();
        for segment in segments {
            path.push(segment);
        }
        path.push(name);
        path
    }

    #[test]
    fn ntz_date_only_is_naive_midnight() {
        assert_eq!(
            parse_timestamp_ntz_micros("2024-01-02"),
            Some(1_704_153_600_000_000)
        );
    }

    #[test]
    fn ntz_wall_forms_share_one_instant() {
        let wall = Some(1_704_164_645_000_000);
        assert_eq!(parse_timestamp_ntz_micros("2024-01-02 03:04:05"), wall);
        assert_eq!(parse_timestamp_ntz_micros("2024-01-02T03:04:05"), wall);
        assert_eq!(
            parse_timestamp_ntz_micros("2024-01-02 03:04:05.5"),
            Some(1_704_164_645_500_000)
        );
    }

    #[test]
    fn ntz_refuses_non_walls() {
        assert_eq!(parse_timestamp_ntz_micros("7"), None);
        assert_eq!(parse_timestamp_ntz_micros(""), None);
        assert_eq!(parse_timestamp_ntz_micros("2024-01-02 03:04:05."), None);
        assert_eq!(
            parse_timestamp_ntz_micros("2024-01-02 03:04:05.1234567890"),
            None
        );
    }

    #[test]
    fn ntz_overlay_ignores_the_session_zone() {
        let ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            cast_raw_partition_value("2024-01-02", &ntz, ny_zone()),
            Some(PartitionValue::TimestampMicros(1_704_153_600_000_000))
        );
        assert_eq!(
            cast_raw_partition_value("2024-01-02 03:04:05", &ntz, ny_zone()),
            Some(PartitionValue::TimestampMicros(1_704_164_645_000_000))
        );
        assert_eq!(
            cast_raw_partition_value("2024-01-02T03:04:05", &ntz, ny_zone()),
            Some(PartitionValue::TimestampMicros(1_704_164_645_000_000))
        );
        assert_eq!(cast_raw_partition_value("7", &ntz, ny_zone()), None);
    }

    #[test]
    fn zoned_overlay_keeps_the_session_wall() {
        let ltz = DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()));
        assert_eq!(
            cast_raw_partition_value("2024-01-02", &ltz, ny_zone()),
            Some(PartitionValue::TimestampMicros(1_704_171_600_000_000))
        );
        assert_eq!(
            cast_raw_partition_value("2024-01-02 03:04:05", &ltz, ny_zone()),
            Some(PartitionValue::TimestampMicros(1_704_182_645_000_000))
        );
    }

    #[test]
    fn discovery_infers_space_wall_as_session_timestamp() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=2024-01-02 03:04:05"], "part-00000.txt"),
            leaf(&root, &["k=2024-01-02 03:04:05"], "part-00001.txt"),
        ];
        let discovered = discover_partitions(&root, &files, "America/New_York").unwrap();
        assert_eq!(
            discovered.fields[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
        );
        assert_eq!(
            discovered.values[&files[0]],
            vec![PartitionValue::TimestampMicros(1_704_182_645_000_000)]
        );
    }

    #[test]
    fn discovery_leaves_fractional_and_iso_walls_as_string() {
        let root = PathBuf::from("/root");
        let files = vec![
            leaf(&root, &["k=2024-01-02 08:04:05.12"], "part-00000.txt"),
            leaf(&root, &["k=2024-01-02 08:04:05.12"], "part-00001.txt"),
        ];
        let discovered = discover_partitions(&root, &files, "America/New_York").unwrap();
        assert_eq!(discovered.fields[0].data_type(), &DataType::Utf8);
        let root = PathBuf::from("/iso");
        let files = vec![leaf(&root, &["k=2024-01-02T03:04:05"], "part-00000.txt")];
        let discovered = discover_partitions(&root, &files, "America/New_York").unwrap();
        assert_eq!(discovered.fields[0].data_type(), &DataType::Utf8);
    }

    #[test]
    fn inference_accepts_only_the_space_wall() {
        assert!(looks_like_timestamp("2024-01-02 03:04:05"));
        assert!(!looks_like_timestamp("2024-01-02T03:04:05"));
        assert!(!looks_like_timestamp("2024-01-02 08:04:05.12"));
        assert!(!looks_like_timestamp("2024-01-02"));
        assert!(!looks_like_timestamp("true"));
        assert!(!looks_like_timestamp("1.50"));
    }
}
