use arrow::array::Decimal128Builder;

use super::CodecError;
use crate::error::{ConnectError, ProtocolViolation, Result, ValueRefusal};

const POSITIVE: u16 = 0x0000;
const NEGATIVE: u16 = 0x4000;
const NAN: u16 = 0xC000;
const POSITIVE_INFINITY: u16 = 0xD000;
const NEGATIVE_INFINITY: u16 = 0xF000;
const HEADER_BYTES: usize = 8;
const NBASE: i16 = 10_000;
const DEC_DIGITS: i64 = 4;
const TYPMOD_HEADER: i32 = 4;
const MAX_TYPMOD_PRECISION: i32 = 1000;
const MAX_DECIMAL128_PRECISION: i32 = 38;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalTarget {
    precision: u8,
    scale: i8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NumericModifier {
    Unconstrained,
    Constrained { precision: i32, scale: i32 },
}

impl DecimalTarget {
    pub const UNCONSTRAINED: DecimalTarget = DecimalTarget {
        precision: 38,
        scale: 18,
    };

    #[must_use]
    pub fn precision(self) -> u8 {
        self.precision
    }

    #[must_use]
    pub fn scale(self) -> i8 {
        self.scale
    }

    pub(super) fn from_modifier(modifier: NumericModifier) -> Option<DecimalTarget> {
        let NumericModifier::Constrained { precision, scale } = modifier else {
            return Some(DecimalTarget::UNCONSTRAINED);
        };
        if !(1..=MAX_TYPMOD_PRECISION).contains(&precision) {
            return None;
        }
        let jdbc_scale = scale & 0xffff;
        let effective = precision.max(jdbc_scale);
        let (target_precision, target_scale) = if effective <= MAX_DECIMAL128_PRECISION {
            (effective, jdbc_scale)
        } else {
            (
                MAX_DECIMAL128_PRECISION,
                0.max(jdbc_scale - (effective - MAX_DECIMAL128_PRECISION)),
            )
        };
        Some(DecimalTarget {
            precision: u8::try_from(target_precision).ok()?,
            scale: i8::try_from(target_scale).ok()?,
        })
    }
}

impl NumericModifier {
    pub(super) fn render(self) -> String {
        match self {
            NumericModifier::Unconstrained => String::new(),
            NumericModifier::Constrained { precision, scale } => format!("({precision},{scale})"),
        }
    }
}

pub(super) fn modifier(typmod: i32) -> NumericModifier {
    if typmod < TYPMOD_HEADER {
        return NumericModifier::Unconstrained;
    }
    let packed = typmod - TYPMOD_HEADER;
    NumericModifier::Constrained {
        precision: (packed >> 16) & 0xffff,
        scale: ((packed & 0x7ff) ^ 1024) - 1024,
    }
}

pub(super) fn builder(capacity: usize, target: DecimalTarget) -> Result<Decimal128Builder> {
    Decimal128Builder::with_capacity(capacity)
        .with_precision_and_scale(target.precision, target.scale)
        .map_err(|error| ConnectError::Arrow {
            message: error.to_string(),
        })
}

fn fixed_prefix<const N: usize>(bytes: &[u8]) -> std::result::Result<[u8; N], CodecError> {
    bytes
        .get(..N)
        .and_then(|prefix| <[u8; N]>::try_from(prefix).ok())
        .ok_or(CodecError::WireLength {
            expected: N,
            actual: bytes.len(),
        })
}

pub(super) fn decode(bytes: &[u8], target: DecimalTarget) -> std::result::Result<i128, CodecError> {
    let header: [u8; HEADER_BYTES] = fixed_prefix(bytes)?;
    let [n0, n1, w0, w1, s0, s1, _, _] = header;
    let ndigits = usize::from(u16::from_be_bytes([n0, n1]));
    let weight = i64::from(i16::from_be_bytes([w0, w1]));
    let negative = match u16::from_be_bytes([s0, s1]) {
        POSITIVE => false,
        NEGATIVE => true,
        NAN => return Err(CodecError::Refused(ValueRefusal::NumericNaN)),
        POSITIVE_INFINITY | NEGATIVE_INFINITY => {
            return Err(CodecError::Refused(ValueRefusal::NumericInfinity));
        }
        sign => {
            return Err(CodecError::Malformed(ProtocolViolation::NumericSign {
                sign,
            }));
        }
    };
    let expected = HEADER_BYTES + 2 * ndigits;
    if bytes.len() != expected {
        return Err(CodecError::WireLength {
            expected,
            actual: bytes.len(),
        });
    }
    let scale = i64::from(target.scale);
    let out_of_range = CodecError::Refused(ValueRefusal::NumericOutOfRange);
    let mut magnitude: i128 = 0;
    let mut round_up = false;
    for (position, pair) in (0_i64..).zip(bytes[HEADER_BYTES..].chunks_exact(2)) {
        let digit = i16::from_be_bytes([pair[0], pair[1]]);
        if !(0..NBASE).contains(&digit) {
            return Err(CodecError::Malformed(ProtocolViolation::NumericDigit {
                digit,
            }));
        }
        let exponent = DEC_DIGITS * (weight - position) + scale;
        if digit == 0 || exponent < -DEC_DIGITS {
            continue;
        }
        let digit = i128::from(digit);
        if exponent >= 0 {
            let term = u32::try_from(exponent)
                .ok()
                .and_then(|power| 10_i128.checked_pow(power))
                .and_then(|factor| digit.checked_mul(factor))
                .ok_or(out_of_range)?;
            magnitude = magnitude.checked_add(term).ok_or(out_of_range)?;
        } else {
            let dropped = u32::try_from(-exponent).map_err(|_| out_of_range)?;
            let divisor = 10_i128.pow(dropped);
            magnitude = magnitude.checked_add(digit / divisor).ok_or(out_of_range)?;
            round_up = (digit / (divisor / 10)) % 10 >= 5;
        }
    }
    if round_up {
        magnitude = magnitude.checked_add(1).ok_or(out_of_range)?;
    }
    let bound = 10_i128
        .checked_pow(u32::from(target.precision))
        .ok_or(out_of_range)?;
    if magnitude >= bound {
        return Err(out_of_range);
    }
    Ok(if negative { -magnitude } else { magnitude })
}
