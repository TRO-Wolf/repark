const POW10_EXACT: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];
const EXACT_LIMIT: u128 = 1 << 53;
const MAX_SCALE: usize = 38;
const REFINE_STEPS: usize = 4;

const fn pow10_u128() -> [u128; MAX_SCALE + 1] {
    let mut table = [1u128; MAX_SCALE + 1];
    let mut index = 1;
    while index <= MAX_SCALE {
        table[index] = table[index - 1] * 10;
        index += 1;
    }
    table
}

const POW10_U128: [u128; MAX_SCALE + 1] = pow10_u128();

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct U256 {
    hi: u128,
    lo: u128,
}

impl U256 {
    const fn from_u128(lo: u128) -> Self {
        Self { hi: 0, lo }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn product(left: u128, right: u64) -> Self {
        let low = u128::from(left as u64) * u128::from(right);
        let high = (left >> 64) * u128::from(right);
        let lo = low.wrapping_add(high << 64);
        let carry = u128::from(lo < low);
        Self {
            hi: (high >> 64) + carry,
            lo,
        }
    }

    fn leading_zeros(self) -> u32 {
        if self.hi == 0 {
            128 + self.lo.leading_zeros()
        } else {
            self.hi.leading_zeros()
        }
    }

    fn shifted(self, bits: u32) -> Option<Self> {
        if bits == 0 {
            return Some(self);
        }
        if self.leading_zeros() < bits {
            return None;
        }
        if bits >= 128 {
            return Some(Self {
                hi: self.lo << (bits - 128),
                lo: 0,
            });
        }
        Some(Self {
            hi: (self.hi << bits) | (self.lo >> (128 - bits)),
            lo: self.lo << bits,
        })
    }
}

#[must_use]
pub fn java_decimal_to_double(unscaled: i128, scale: i8) -> f64 {
    let magnitude = correctly_rounded(unscaled.unsigned_abs(), scale);
    if unscaled < 0 { -magnitude } else { magnitude }
}

#[must_use]
pub fn decimal_preimage_of_double(constant: f64, precision: u8, scale: i8) -> Option<(i128, i128)> {
    if !constant.is_finite() {
        return None;
    }
    let top = POW10_U128
        .get(usize::from(precision))
        .and_then(|power| power.checked_sub(1))
        .and_then(|top| i128::try_from(top).ok())
        .filter(|&top| top > 0)?;
    let lower = least_reaching(-top, top, |unscaled| {
        java_decimal_to_double(unscaled, scale)
            .total_cmp(&constant)
            .is_ge()
    })?;
    let upper = least_reaching(lower, top, |unscaled| {
        java_decimal_to_double(unscaled, scale)
            .total_cmp(&constant)
            .is_gt()
    })?;
    Some((lower, upper))
}

fn least_reaching(mut low: i128, mut high: i128, reaches: impl Fn(i128) -> bool) -> Option<i128> {
    if !reaches(high) {
        return None;
    }
    while low < high {
        let middle = low.checked_add_unsigned(low.abs_diff(high) / 2)?;
        if reaches(middle) {
            high = middle;
        } else {
            low = middle.checked_add(1)?;
        }
    }
    Some(low)
}

#[allow(clippy::cast_precision_loss)]
fn correctly_rounded(magnitude: u128, scale: i8) -> f64 {
    if magnitude == 0 {
        return 0.0;
    }
    if scale == 0 {
        return magnitude as f64;
    }
    let Some(digits) = usize::try_from(scale)
        .ok()
        .filter(|&digits| digits <= MAX_SCALE)
    else {
        return by_text(magnitude, scale);
    };
    if magnitude < EXACT_LIMIT
        && let Some(divisor) = POW10_EXACT.get(digits)
    {
        return magnitude as f64 / divisor;
    }
    let divisor = POW10_EXACT
        .get(digits)
        .copied()
        .unwrap_or_else(|| 10f64.powi(i32::from(scale)));
    refine(magnitude, digits, magnitude as f64 / divisor)
        .unwrap_or_else(|| by_text(magnitude, scale))
}

fn refine(magnitude: u128, digits: usize, start: f64) -> Option<f64> {
    let mut candidate = start;
    for _ in 0..REFINE_STEPS {
        if !candidate.is_finite() || candidate <= 0.0 {
            return None;
        }
        let below = candidate.next_down();
        match against_midpoint(magnitude, digits, below, candidate)? {
            std::cmp::Ordering::Less => {
                candidate = below;
                continue;
            }
            std::cmp::Ordering::Equal => return Some(even_of(below, candidate)),
            std::cmp::Ordering::Greater => {}
        }
        let above = candidate.next_up();
        match against_midpoint(magnitude, digits, candidate, above)? {
            std::cmp::Ordering::Greater => candidate = above,
            std::cmp::Ordering::Equal => return Some(even_of(candidate, above)),
            std::cmp::Ordering::Less => return Some(candidate),
        }
    }
    None
}

fn even_of(low: f64, high: f64) -> f64 {
    if low.to_bits() & 1 == 0 { low } else { high }
}

fn parts(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let fraction = bits & 0x000f_ffff_ffff_ffff;
    let field = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    if field == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), field - 1075)
    }
}

fn against_midpoint(
    magnitude: u128,
    digits: usize,
    low: f64,
    high: f64,
) -> Option<std::cmp::Ordering> {
    let (low_mantissa, low_exponent) = parts(low);
    let (high_mantissa, high_exponent) = parts(high);
    let exponent = low_exponent.min(high_exponent);
    let sum = (low_mantissa << u32::try_from(low_exponent - exponent).ok()?)
        + (high_mantissa << u32::try_from(high_exponent - exponent).ok()?);
    let power = *POW10_U128.get(digits)?;
    let twice = U256::from_u128(magnitude).shifted(1)?;
    let (left, right) = if exponent <= 0 {
        (
            twice.shifted(exponent.unsigned_abs())?,
            U256::product(power, sum),
        )
    } else {
        (
            twice,
            U256::product(power, sum).shifted(exponent.unsigned_abs())?,
        )
    };
    Some(left.cmp(&right))
}

fn by_text(magnitude: u128, scale: i8) -> f64 {
    format!("{magnitude}e{}", -i32::from(scale))
        .parse()
        .unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests;
