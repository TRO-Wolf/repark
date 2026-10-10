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
    use chrono::{DateTime, LocalResult};
    use repark_common::zone_horizon::{offsets_at_wall, wall_at_instant};
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

    fn zone_of(id: &str) -> repark_connect::Result<Tz> {
        Tz::from_str(&canonical_session_zone_id(id)).map_err(|error| ConnectError::Arrow {
            message: error.to_string(),
        })
    }

    fn convert(
        array: &TimestampMicrosecondArray,
        zone: Tz,
        convert: fn(i64, Tz, usize) -> repark_connect::Result<i64>,
    ) -> repark_connect::Result<TimestampMicrosecondArray> {
        array
            .iter()
            .enumerate()
            .map(|(index, value)| value.map(|micros| convert(micros, zone, index)).transpose())
            .collect()
    }

    pub(crate) fn localise_at(
        zone_id: &str,
        wall: &TimestampMicrosecondArray,
    ) -> repark_connect::Result<TimestampMicrosecondArray> {
        convert(wall, zone_of(zone_id)?, place)
    }

    pub(crate) fn unlocalise(
        zone_id: &str,
        zoned: &TimestampMicrosecondArray,
    ) -> repark_connect::Result<TimestampMicrosecondArray> {
        convert(zoned, zone_of(zone_id)?, unplace)
    }

    pub(super) fn unplace(micros: i64, zone: Tz, index: usize) -> repark_connect::Result<i64> {
        let past = || refused(index, ValueRefusal::TimestampPastCalendar);
        let utc = DateTime::from_timestamp_micros(micros)
            .ok_or_else(past)?
            .naive_utc();
        let wall = wall_at_instant(&zone, &utc).ok_or_else(past)?;
        Ok(wall.and_utc().timestamp_micros())
    }

    pub(super) fn place(micros: i64, zone: Tz, index: usize) -> repark_connect::Result<i64> {
        let past = || refused(index, ValueRefusal::TimestampPastCalendar);
        let wall = DateTime::from_timestamp_micros(micros)
            .ok_or_else(past)?
            .naive_utc();
        let offset = match offsets_at_wall(&zone, &wall) {
            LocalResult::Single(offset) => offset,
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
            localise_at(&self.canonical_zone(), wall)
        }

        fn zone_label(&self) -> Arc<str> {
            self.canonical_zone().into()
        }
    }
}

#[cfg(feature = "postgres")]
pub(crate) use placement::{localise_at, unlocalise};

#[cfg(all(test, feature = "postgres"))]
mod tests;
