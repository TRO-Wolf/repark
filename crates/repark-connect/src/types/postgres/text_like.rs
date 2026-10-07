use super::{CodecError, fixed};
use crate::error::ProtocolViolation;

pub(super) const UUID_TEXT_BYTES: usize = 36;
const JSONB_VERSION: u8 = 1;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(super) fn text(bytes: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(bytes).map_err(|_| CodecError::InvalidUtf8)
}

pub(super) fn jsonb(bytes: &[u8]) -> Result<&str, CodecError> {
    match bytes.split_first() {
        Some((&JSONB_VERSION, body)) => text(body),
        Some((&version, _)) => Err(CodecError::Malformed(ProtocolViolation::JsonbVersion {
            found: Some(version),
        })),
        None => Err(CodecError::Malformed(ProtocolViolation::JsonbVersion {
            found: None,
        })),
    }
}

pub(super) fn uuid<'out>(
    bytes: &[u8],
    out: &'out mut [u8; UUID_TEXT_BYTES],
) -> Result<&'out str, CodecError> {
    let raw = fixed::<16>(bytes)?;
    let mut slots = out.iter_mut();
    for (index, byte) in raw.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10)
            && let Some(slot) = slots.next()
        {
            *slot = b'-';
        }
        for nibble in [byte >> 4, byte & 0x0f] {
            if let (Some(slot), Some(hex)) = (slots.next(), HEX_DIGITS.get(usize::from(nibble))) {
                *slot = *hex;
            }
        }
    }
    text(out)
}
