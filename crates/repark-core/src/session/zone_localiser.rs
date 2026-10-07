use std::sync::{Arc, RwLock};

use super::ReparkSession;
use crate::session_time_zone::SessionTimeZone;

#[derive(Debug, Clone)]
pub struct SessionZoneLocaliser {
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "only the Postgres placement reads the zone")
    )]
    zone: Arc<RwLock<Arc<SessionTimeZone>>>,
}

impl ReparkSession {
    pub(crate) fn zone_localiser(&self) -> SessionZoneLocaliser {
        SessionZoneLocaliser {
            zone: Arc::clone(&self.runtime_zone),
        }
    }
}

#[cfg(feature = "postgres")]
mod placement {
    use std::str::FromStr;
    use std::sync::{Arc, PoisonError, RwLock};

    use arrow::array::TimestampMicrosecondArray;
    use arrow::array::timezone::Tz;
    use chrono::{DateTime, LocalResult, TimeZone};
    use repark_connect::{ConnectError, ValueRefusal, WallClockLocaliser};

    use super::{SessionTimeZone, SessionZoneLocaliser};

    impl SessionZoneLocaliser {
        fn current(&self) -> Arc<SessionTimeZone> {
            Arc::clone(&RwLock::read(&self.zone).unwrap_or_else(PoisonError::into_inner))
        }
    }

    fn refused(index: usize, reason: ValueRefusal) -> ConnectError {
        ConnectError::UnrepresentableValue {
            column: "".into(),
            postgres_type: "timestamp",
            index,
            reason,
        }
    }

    pub(super) fn place(micros: i64, zone: Tz, index: usize) -> repark_connect::Result<i64> {
        let wall = DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| refused(index, ValueRefusal::TimestampOutOfRange))?
            .naive_utc();
        match zone.from_local_datetime(&wall) {
            LocalResult::Single(instant) => Ok(instant.timestamp_micros()),
            LocalResult::Ambiguous(_, _) => Err(refused(index, ValueRefusal::WallClockOverlap)),
            LocalResult::None => Err(refused(index, ValueRefusal::WallClockGap)),
        }
    }

    impl WallClockLocaliser for SessionZoneLocaliser {
        fn localise(
            &self,
            wall: &TimestampMicrosecondArray,
        ) -> repark_connect::Result<TimestampMicrosecondArray> {
            let current = self.current();
            let zone = Tz::from_str(current.id()).map_err(|error| ConnectError::Arrow {
                message: error.to_string(),
            })?;
            wall.iter()
                .enumerate()
                .map(|(index, value)| value.map(|micros| place(micros, zone, index)).transpose())
                .collect()
        }

        fn zone_label(&self) -> Arc<str> {
            self.current().id().into()
        }
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use std::str::FromStr;

    use arrow::array::timezone::Tz;
    use repark_connect::{ConnectError, ValueRefusal};

    use super::placement::place;

    const MICROS_PER_HOUR: i64 = 3_600_000_000;

    fn wall(text: &str) -> i64 {
        chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
            .expect("a wall clock")
            .and_utc()
            .timestamp_micros()
    }

    fn refusal(result: &repark_connect::Result<i64>) -> Option<ValueRefusal> {
        match result {
            Err(ConnectError::UnrepresentableValue { reason, .. }) => Some(*reason),
            _ => None,
        }
    }

    #[test]
    fn a_wall_clock_is_placed_at_the_zone_offset_of_its_date() {
        let zone = Tz::from_str("America/New_York").expect("zone");
        let winter = wall("2024-01-15 12:00:00");
        let summer = wall("2024-07-15 12:00:00");
        assert_eq!(
            place(winter, zone, 0).ok(),
            Some(winter + 5 * MICROS_PER_HOUR)
        );
        assert_eq!(
            place(summer, zone, 0).ok(),
            Some(summer + 4 * MICROS_PER_HOUR)
        );
        let fixed = Tz::from_str("+05:30").expect("offset");
        assert_eq!(
            place(winter, fixed, 0).ok(),
            Some(winter - 11 * MICROS_PER_HOUR / 2)
        );
    }

    #[test]
    fn gap_and_overlap_wall_clocks_refuse_naming_the_zone_row() {
        let zone = Tz::from_str("America/New_York").expect("zone");
        let gap = refusal(&place(wall("2024-03-10 02:30:00"), zone, 3));
        let overlap = refusal(&place(wall("2024-11-03 01:30:00"), zone, 4));
        assert_eq!(gap, Some(ValueRefusal::WallClockGap));
        assert_eq!(overlap, Some(ValueRefusal::WallClockOverlap));
        assert_eq!(
            ValueRefusal::WallClockGap.registry_row(),
            "CONNECT-DIV-pg-timestamp-zone"
        );
        let utc = Tz::from_str("UTC").expect("utc");
        let edge = wall("2024-03-10 02:30:00");
        assert_eq!(place(edge, utc, 0).ok(), Some(edge));
    }
}
