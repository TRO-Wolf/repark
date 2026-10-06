use super::{CodecError, fixed};
use crate::error::ValueRefusal;

pub const POSTGRES_EPOCH_DAYS: i32 = 10_957;
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;
pub const UTC_ZONE_LABEL: &str = "+00:00";

pub(super) fn date(bytes: &[u8]) -> Result<i32, CodecError> {
    let days = i32::from_be_bytes(fixed::<4>(bytes)?);
    if days == i32::MAX || days == i32::MIN {
        return Err(CodecError::Refused(ValueRefusal::InfiniteDate));
    }
    days.checked_add(POSTGRES_EPOCH_DAYS)
        .ok_or(CodecError::Refused(ValueRefusal::DateOutOfRange))
}

pub(super) fn timestamp(bytes: &[u8]) -> Result<i64, CodecError> {
    let micros = i64::from_be_bytes(fixed::<8>(bytes)?);
    if micros == i64::MAX || micros == i64::MIN {
        return Err(CodecError::Refused(ValueRefusal::InfiniteTimestamp));
    }
    micros
        .checked_add(POSTGRES_EPOCH_MICROS)
        .ok_or(CodecError::Refused(ValueRefusal::TimestampOutOfRange))
}
