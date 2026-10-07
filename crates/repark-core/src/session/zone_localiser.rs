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
    use chrono::{DateTime, Datelike, LocalResult, Offset, TimeZone};
    use repark_common::zone_horizon::proxy_year;
    use repark_connect::{ConnectError, ValueRefusal, WallClockLocaliser};

    use super::SessionZoneLocaliser;
    use crate::session_time_zone::canonical_session_zone_id;

    impl SessionZoneLocaliser {
        fn canonical_zone(&self) -> String {
            let current =
                Arc::clone(&RwLock::read(&self.zone).unwrap_or_else(PoisonError::into_inner));
            canonical_session_zone_id(current.id())
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
        let past = || refused(index, ValueRefusal::TimestampPastCalendar);
        let wall = DateTime::from_timestamp_micros(micros)
            .ok_or_else(past)?
            .naive_utc();
        let proxy = i32::try_from(proxy_year(i64::from(wall.year())))
            .ok()
            .and_then(|year| wall.with_year(year))
            .ok_or_else(past)?;
        let offset = match zone.offset_from_local_datetime(&proxy) {
            LocalResult::Single(offset) => offset.fix(),
            LocalResult::Ambiguous(_, _) => {
                return Err(refused(index, ValueRefusal::WallClockOverlap));
            }
            LocalResult::None => return Err(refused(index, ValueRefusal::WallClockGap)),
        };
        wall.checked_sub_offset(offset)
            .map(|instant| instant.and_utc().timestamp_micros())
            .ok_or_else(past)
    }

    impl WallClockLocaliser for SessionZoneLocaliser {
        fn localise(
            &self,
            wall: &TimestampMicrosecondArray,
        ) -> repark_connect::Result<TimestampMicrosecondArray> {
            let zone =
                Tz::from_str(&self.canonical_zone()).map_err(|error| ConnectError::Arrow {
                    message: error.to_string(),
                })?;
            wall.iter()
                .enumerate()
                .map(|(index, value)| value.map(|micros| place(micros, zone, index)).transpose())
                .collect()
        }

        fn zone_label(&self) -> Arc<str> {
            self.canonical_zone().into()
        }
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests;
