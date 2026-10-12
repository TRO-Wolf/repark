use super::{CodecError, fixed};
use crate::error::ProtocolViolation;

pub(super) const UUID_TEXT_BYTES: usize = 36;
pub(super) const JSONB_VERSION: u8 = 1;
const UUID_BYTES: usize = 16;
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

fn hex_value(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
}

pub(super) fn uuid_wire(text: &str) -> Option<[u8; UUID_BYTES]> {
    let (braced, mut rest) = match text.as_bytes() {
        [b'{', rest @ ..] => (true, rest),
        rest => (false, rest),
    };
    let mut raw = [0_u8; UUID_BYTES];
    for (index, slot) in raw.iter_mut().enumerate() {
        let [high, low, tail @ ..] = rest else {
            return None;
        };
        *slot = (hex_value(*high)? << 4) | hex_value(*low)?;
        rest = tail;
        if index % 2 == 1
            && index + 1 < UUID_BYTES
            && let [b'-', tail @ ..] = rest
        {
            rest = tail;
        }
    }
    if braced {
        let [b'}', tail @ ..] = rest else {
            return None;
        };
        rest = tail;
    }
    rest.is_empty().then_some(raw)
}
