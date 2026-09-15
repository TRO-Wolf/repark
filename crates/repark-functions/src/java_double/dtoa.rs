use std::cmp::Ordering;

use datafusion::arrow::array::{Array, Float32Array, Float64Array, StringArray, StringBuilder};

use super::bigint::FdBig;

const EXP_SHIFT: i32 = 52;
const FRACT_HOB: u64 = 1 << EXP_SHIFT;
const EXP_ONE: u64 = 1023 << EXP_SHIFT;
const SIGN_BIT: u64 = 1 << 63;
const EXP_MASK: u64 = 0x7ff0_0000_0000_0000;
const SIGNIF_MASK: u64 = 0x000f_ffff_ffff_ffff;
const MAX_SMALL_BIN_EXP: i32 = 62;
const MIN_SMALL_BIN_EXP: i32 = -(63 / 3);
const SINGLE_EXP_SHIFT: i32 = 23;
const SINGLE_FRACT_HOB: u32 = 1 << SINGLE_EXP_SHIFT;
const SINGLE_EXP_BIAS: i32 = 127;
const DOUBLE_EXP_BIAS: i32 = 1023;

const N_5_BITS: [i32; 27] = [
    0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56,
    59, 61,
];

const fn small_5_pow() -> [i32; 14] {
    let mut table = [0i32; 14];
    let mut value = 1i32;
    let mut index = 0usize;
    while index < 14 {
        table[index] = value;
        value = value.wrapping_mul(5);
        index += 1;
    }
    table
}

const fn long_5_pow() -> [i64; 27] {
    let mut table = [0i64; 27];
    let mut value = 1i64;
    let mut index = 0usize;
    while index < 27 {
        table[index] = value;
        value = value.wrapping_mul(5);
        index += 1;
    }
    table
}

const SMALL_5_POW: [i32; 14] = small_5_pow();
const LONG_5_POW: [i64; 27] = long_5_pow();

const INSIGNIFICANT_DIGITS_NUMBER: [i32; 64] = [
    0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9,
    9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15, 16, 16,
    16, 17, 17, 17, 18, 18, 18, 19,
];

fn insignificant_digits_for_pow2(power: i32) -> i32 {
    if power > 1 && (power as usize) < INSIGNIFICANT_DIGITS_NUMBER.len() {
        INSIGNIFICANT_DIGITS_NUMBER[power as usize]
    } else {
        0
    }
}

fn estimate_dec_exp(fract_bits: u64, bin_exp: i32) -> i32 {
    let probe = f64::from_bits(EXP_ONE | (fract_bits & SIGNIF_MASK));
    let guess = (probe - 1.5) * 0.289529654 + 0.176091259 + f64::from(bin_exp) * 0.301029995663981;
    let guess_bits = guess.to_bits();
    let exponent = ((guess_bits & EXP_MASK) >> EXP_SHIFT) as i32 - DOUBLE_EXP_BIAS;
    let negative = guess_bits & SIGN_BIT != 0;
    if exponent >= 0 && exponent < EXP_SHIFT {
        let mask = SIGNIF_MASK >> exponent;
        let kept = (guess_bits & SIGNIF_MASK) | FRACT_HOB;
        let rounded = (kept >> (EXP_SHIFT - exponent)) as i32;
        if negative {
            if mask & guess_bits == 0 {
                -rounded
            } else {
                -rounded - 1
            }
        } else {
            rounded
        }
    } else if exponent < 0 {
        if guess_bits & !SIGN_BIT == 0 {
            0
        } else if negative {
            -1
        } else {
            0
        }
    } else {
        guess as i32
    }
}

struct Dtoa {
    negative: bool,
    digits: [u8; 20],
    first: usize,
    n_digits: usize,
    dec_exponent: i32,
}

impl Dtoa {
    fn develop_long_digits(&mut self, mut dec_exponent: i32, mut lvalue: i64, insignificant: i32) {
        if insignificant != 0 {
            let power = LONG_5_POW[insignificant as usize].wrapping_shl(insignificant as u32);
            let residue = lvalue.wrapping_rem(power);
            lvalue = lvalue.wrapping_div(power);
            dec_exponent += insignificant;
            if residue >= power.wrapping_shr(1) {
                lvalue += 1;
            }
        }
        let mut slot = self.digits.len() - 1;
        if lvalue <= i64::from(i32::MAX) {
            let mut ivalue = lvalue as i32;
            let mut digit = ivalue.wrapping_rem(10);
            ivalue = ivalue.wrapping_div(10);
            while digit == 0 {
                dec_exponent += 1;
                digit = ivalue.wrapping_rem(10);
                ivalue = ivalue.wrapping_div(10);
            }
            while ivalue != 0 {
                self.digits[slot] = b'0' + digit as u8;
                slot -= 1;
                dec_exponent += 1;
                digit = ivalue.wrapping_rem(10);
                ivalue = ivalue.wrapping_div(10);
            }
            self.digits[slot] = b'0' + digit as u8;
        } else {
            let mut digit = lvalue.wrapping_rem(10) as u8;
            lvalue = lvalue.wrapping_div(10);
            while digit == 0 {
                dec_exponent += 1;
                digit = lvalue.wrapping_rem(10) as u8;
                lvalue = lvalue.wrapping_div(10);
            }
            while lvalue != 0 {
                self.digits[slot] = b'0' + digit;
                slot -= 1;
                dec_exponent += 1;
                digit = lvalue.wrapping_rem(10) as u8;
                lvalue = lvalue.wrapping_div(10);
            }
            self.digits[slot] = b'0' + digit;
        }
        self.dec_exponent = dec_exponent + 1;
        self.first = slot;
        self.n_digits = self.digits.len() - slot;
    }

    fn roundup(&mut self) {
        let mut index = self.first + self.n_digits - 1;
        while self.digits[index] == b'9' && index > self.first {
            self.digits[index] = b'0';
            index -= 1;
        }
        if self.digits[index] == b'9' {
            self.dec_exponent += 1;
            self.digits[self.first] = b'1';
            return;
        }
        self.digits[index] += 1;
    }

    fn first_digit(
        &mut self,
        ndigit: &mut usize,
        dec_exp: &mut i32,
        quotient: i32,
        low: &mut bool,
        high: &mut bool,
    ) {
        if quotient == 0 && !*high {
            *dec_exp -= 1;
        } else {
            self.digits[*ndigit] = b'0'.wrapping_add(quotient as u8);
            *ndigit += 1;
        }
        if *dec_exp < -3 || *dec_exp >= 8 {
            *low = false;
            *high = false;
        }
    }

    fn dtoa(&mut self, bin_exp: i32, mut fract_bits: u64, n_significant_bits: i32) {
        let tail_zeros = fract_bits.trailing_zeros() as i32;
        let n_fract_bits = EXP_SHIFT + 1 - tail_zeros;
        let n_tiny_bits = (n_fract_bits - bin_exp - 1).max(0);
        if bin_exp <= MAX_SMALL_BIN_EXP
            && bin_exp >= MIN_SMALL_BIN_EXP
            && (n_tiny_bits as usize) < LONG_5_POW.len()
            && (n_fract_bits + N_5_BITS[n_tiny_bits as usize]) < 64
            && n_tiny_bits == 0
        {
            let insignificant = if bin_exp > n_significant_bits {
                insignificant_digits_for_pow2(bin_exp - n_significant_bits - 1)
            } else {
                0
            };
            if bin_exp >= EXP_SHIFT {
                fract_bits = fract_bits.wrapping_shl((bin_exp - EXP_SHIFT) as u32);
            } else {
                fract_bits = fract_bits.wrapping_shr((EXP_SHIFT - bin_exp) as u32);
            }
            self.develop_long_digits(0, fract_bits as i64, insignificant);
            return;
        }
        let mut dec_exp = estimate_dec_exp(fract_bits, bin_exp);
        let bound5 = 0.max(-dec_exp);
        let shift5 = 0.max(dec_exp);
        let mut bound2 = bound5 + n_tiny_bits + bin_exp;
        let mut shift2 = shift5 + n_tiny_bits;
        let mut measure2 = bound2 - n_significant_bits;
        fract_bits >>= tail_zeros;
        bound2 -= n_fract_bits - 1;
        let common = bound2.min(shift2);
        bound2 -= common;
        shift2 -= common;
        measure2 -= common;
        if n_fract_bits == 1 {
            measure2 -= 1;
        }
        if measure2 < 0 {
            bound2 -= measure2;
            shift2 -= measure2;
            measure2 = 0;
        }
        let bound5_usize = bound5 as usize;
        let shift5_usize = shift5 as usize;
        let bound_bits =
            n_fract_bits + bound2 + N_5_BITS.get(bound5_usize).copied().unwrap_or(bound5 * 3);
        let ten_shift_bits = shift2
            + 1
            + N_5_BITS
                .get(shift5_usize + 1)
                .copied()
                .unwrap_or((shift5 + 1) * 3);
        if bound_bits < 64 && ten_shift_bits < 64 {
            if bound_bits < 32 && ten_shift_bits < 32 {
                self.dtoa_int(
                    fract_bits,
                    bound5_usize,
                    shift5_usize,
                    bound2,
                    shift2,
                    measure2,
                    &mut dec_exp,
                );
            } else {
                self.dtoa_long(
                    fract_bits,
                    bound5_usize,
                    shift5_usize,
                    bound2,
                    shift2,
                    measure2,
                    &mut dec_exp,
                );
            }
            return;
        }
        self.dtoa_big(
            fract_bits,
            bound5,
            shift5,
            bound2,
            shift2,
            measure2,
            &mut dec_exp,
        );
    }

    fn dtoa_int(
        &mut self,
        fract_bits: u64,
        bound5: usize,
        shift5: usize,
        bound2: i32,
        shift2: i32,
        measure2: i32,
        dec_exp: &mut i32,
    ) {
        let mut value = (fract_bits as u32 as i32)
            .wrapping_mul(SMALL_5_POW[bound5])
            .wrapping_shl(bound2 as u32);
        let scale = SMALL_5_POW[shift5].wrapping_shl(shift2 as u32);
        let mut measure = SMALL_5_POW[bound5].wrapping_shl(measure2 as u32);
        let tens = scale.wrapping_mul(10);
        let mut ndigit = 0usize;
        let mut quotient = value.wrapping_div(scale);
        value = (value.wrapping_rem(scale)).wrapping_mul(10);
        measure = measure.wrapping_mul(10);
        let mut low = value < measure;
        let mut high = value.wrapping_add(measure) > tens;
        self.first_digit(&mut ndigit, dec_exp, quotient, &mut low, &mut high);
        while !low && !high && ndigit < self.digits.len() {
            quotient = value.wrapping_div(scale);
            value = (value.wrapping_rem(scale)).wrapping_mul(10);
            measure = measure.wrapping_mul(10);
            if measure > 0 {
                low = value < measure;
                high = value.wrapping_add(measure) > tens;
            } else {
                low = true;
                high = true;
            }
            self.digits[ndigit] = b'0'.wrapping_add(quotient as u8);
            ndigit += 1;
        }
        let low_digit_difference = (value.wrapping_shl(1).wrapping_sub(tens)) as i64;
        self.finish(dec_exp, ndigit, high, low, low_digit_difference);
    }

    fn dtoa_long(
        &mut self,
        fract_bits: u64,
        bound5: usize,
        shift5: usize,
        bound2: i32,
        shift2: i32,
        measure2: i32,
        dec_exp: &mut i32,
    ) {
        let mut value = (fract_bits as i64)
            .wrapping_mul(LONG_5_POW[bound5])
            .wrapping_shl(bound2 as u32);
        let scale = LONG_5_POW[shift5].wrapping_shl(shift2 as u32);
        let mut measure = LONG_5_POW[bound5].wrapping_shl(measure2 as u32);
        let tens = scale.wrapping_mul(10);
        let mut ndigit = 0usize;
        let mut quotient = value.wrapping_div(scale) as i32;
        value = (value.wrapping_rem(scale)).wrapping_mul(10);
        measure = measure.wrapping_mul(10);
        let mut low = value < measure;
        let mut high = value.wrapping_add(measure) > tens;
        self.first_digit(&mut ndigit, dec_exp, quotient, &mut low, &mut high);
        while !low && !high && ndigit < self.digits.len() {
            quotient = value.wrapping_div(scale) as i32;
            value = (value.wrapping_rem(scale)).wrapping_mul(10);
            measure = measure.wrapping_mul(10);
            if measure > 0 {
                low = value < measure;
                high = value.wrapping_add(measure) > tens;
            } else {
                low = true;
                high = true;
            }
            self.digits[ndigit] = b'0'.wrapping_add(quotient as u8);
            ndigit += 1;
        }
        let low_digit_difference = value.wrapping_shl(1).wrapping_sub(tens);
        self.finish(dec_exp, ndigit, high, low, low_digit_difference);
    }

    fn dtoa_big(
        &mut self,
        fract_bits: u64,
        bound5: i32,
        shift5: i32,
        bound2: i32,
        shift2: i32,
        measure2: i32,
        dec_exp: &mut i32,
    ) {
        let mut scale = FdBig::value_of_pow52(shift5 as usize, shift2 as usize);
        let bias = scale.normalization_bias();
        scale.left_shift(bias);
        let mut value =
            FdBig::value_of_mul_pow52(fract_bits, bound5 as usize, (bound2 + bias as i32) as usize);
        let mut measure =
            FdBig::value_of_pow52((bound5 + 1) as usize, (measure2 + bias as i32 + 1) as usize);
        let ten_scale =
            FdBig::value_of_pow52((shift5 + 1) as usize, (shift2 + bias as i32 + 1) as usize);
        let mut ndigit = 0usize;
        let quotient = value.quo_rem_iteration(&scale) as i32;
        let mut low = value.cmp(&measure) == Ordering::Less;
        let mut high = ten_scale.add_and_cmp(&value, &measure) != Ordering::Greater;
        self.first_digit(&mut ndigit, dec_exp, quotient, &mut low, &mut high);
        while !low && !high && ndigit < self.digits.len() {
            let digit = value.quo_rem_iteration(&scale);
            measure.mult_by_10();
            low = value.cmp(&measure) == Ordering::Less;
            high = ten_scale.add_and_cmp(&value, &measure) != Ordering::Greater;
            self.digits[ndigit] = b'0'.wrapping_add(digit as u8);
            ndigit += 1;
        }
        let low_digit_difference = if high && low {
            value.left_shift(1);
            match value.cmp(&ten_scale) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        } else {
            0
        };
        self.finish(dec_exp, ndigit, high, low, low_digit_difference);
    }

    fn finish(
        &mut self,
        dec_exp: &mut i32,
        ndigit: usize,
        high: bool,
        low: bool,
        low_digit_difference: i64,
    ) {
        self.dec_exponent = *dec_exp + 1;
        self.first = 0;
        self.n_digits = ndigit;
        if high {
            if low {
                if low_digit_difference == 0 {
                    if self.digits[self.first + ndigit - 1] & 1 != 0 {
                        self.roundup();
                    }
                } else if low_digit_difference > 0 {
                    self.roundup();
                }
            } else {
                self.roundup();
            }
        }
    }

    fn layout<'a>(&self, composed: &'a mut [u8; 32]) -> &'a str {
        let mut pos = 0usize;
        if self.negative {
            composed[0] = b'-';
            pos = 1;
        }
        if self.dec_exponent > 0 && self.dec_exponent < 8 {
            let shown = self.n_digits.min(self.dec_exponent as usize);
            composed[pos..pos + shown]
                .copy_from_slice(&self.digits[self.first..self.first + shown]);
            pos += shown;
            if shown < self.dec_exponent as usize {
                let rest = self.dec_exponent as usize - shown;
                composed[pos..pos + rest].fill(b'0');
                pos += rest;
                composed[pos] = b'.';
                composed[pos + 1] = b'0';
                pos += 2;
            } else {
                composed[pos] = b'.';
                pos += 1;
                if shown < self.n_digits {
                    let rest = self.n_digits - shown;
                    composed[pos..pos + rest].copy_from_slice(
                        &self.digits[self.first + shown..self.first + self.n_digits],
                    );
                    pos += rest;
                } else {
                    composed[pos] = b'0';
                    pos += 1;
                }
            }
        } else if self.dec_exponent <= 0 && self.dec_exponent > -3 {
            composed[pos] = b'0';
            composed[pos + 1] = b'.';
            pos += 2;
            if self.dec_exponent != 0 {
                let zeros = (-self.dec_exponent) as usize;
                composed[pos..pos + zeros].fill(b'0');
                pos += zeros;
            }
            composed[pos..pos + self.n_digits]
                .copy_from_slice(&self.digits[self.first..self.first + self.n_digits]);
            pos += self.n_digits;
        } else {
            composed[pos] = self.digits[self.first];
            composed[pos + 1] = b'.';
            pos += 2;
            if self.n_digits > 1 {
                composed[pos..pos + self.n_digits - 1]
                    .copy_from_slice(&self.digits[self.first + 1..self.first + self.n_digits]);
                pos += self.n_digits - 1;
            } else {
                composed[pos] = b'0';
                pos += 1;
            }
            composed[pos] = b'E';
            pos += 1;
            let exponent = if self.dec_exponent <= 0 {
                composed[pos] = b'-';
                pos += 1;
                -self.dec_exponent + 1
            } else {
                self.dec_exponent - 1
            };
            if exponent <= 9 {
                composed[pos] = b'0' + exponent as u8;
                pos += 1;
            } else if exponent <= 99 {
                composed[pos] = b'0' + (exponent / 10) as u8;
                composed[pos + 1] = b'0' + (exponent % 10) as u8;
                pos += 2;
            } else {
                composed[pos] = b'0' + (exponent / 100) as u8;
                let rest = exponent % 100;
                composed[pos + 1] = b'0' + (rest / 10) as u8;
                composed[pos + 2] = b'0' + (rest % 10) as u8;
                pos += 3;
            }
        }
        str::from_utf8(&composed[..pos]).unwrap_or("")
    }
}

fn dtoa_double_bits(bits: u64) -> Dtoa {
    let negative = bits & SIGN_BIT != 0;
    let mut fract_bits = bits & SIGNIF_MASK;
    let raw_exp = ((bits & EXP_MASK) >> EXP_SHIFT) as i32;
    let mut converter = Dtoa {
        negative,
        digits: [0u8; 20],
        first: 0,
        n_digits: 0,
        dec_exponent: 0,
    };
    let (bin_exp, n_significant_bits) = if raw_exp == 0 {
        let leading = fract_bits.leading_zeros() as i32 - (63 - EXP_SHIFT);
        fract_bits <<= leading;
        (1 - leading, 64 - leading - (63 - EXP_SHIFT))
    } else {
        fract_bits |= FRACT_HOB;
        (raw_exp, EXP_SHIFT + 1)
    };
    converter.dtoa(bin_exp - DOUBLE_EXP_BIAS, fract_bits, n_significant_bits);
    converter
}

fn dtoa_float_bits(bits: u32) -> Dtoa {
    let negative = bits & (1 << 31) != 0;
    let mut fract_bits = bits & 0x007f_ffff;
    let raw_exp = ((bits >> SINGLE_EXP_SHIFT) & 0xff) as i32;
    let mut converter = Dtoa {
        negative,
        digits: [0u8; 20],
        first: 0,
        n_digits: 0,
        dec_exponent: 0,
    };
    let (bin_exp, n_significant_bits) = if raw_exp == 0 {
        let leading = fract_bits.leading_zeros() as i32 - (31 - SINGLE_EXP_SHIFT);
        fract_bits <<= leading;
        (1 - leading, 32 - leading - (31 - SINGLE_EXP_SHIFT))
    } else {
        fract_bits |= SINGLE_FRACT_HOB;
        (raw_exp, SINGLE_EXP_SHIFT + 1)
    };
    converter.dtoa(
        bin_exp - SINGLE_EXP_BIAS,
        (fract_bits as u64) << (EXP_SHIFT - SINGLE_EXP_SHIFT),
        n_significant_bits,
    );
    converter
}

pub(crate) fn with_java_double_text<R>(value: f64, render: impl FnOnce(&str) -> R) -> R {
    if value.is_nan() {
        return render("NaN");
    }
    if value.is_infinite() {
        if value.is_sign_negative() {
            return render("-Infinity");
        }
        return render("Infinity");
    }
    let bits = value.to_bits();
    if bits & !SIGN_BIT == 0 {
        if value.is_sign_negative() {
            return render("-0.0");
        }
        return render("0.0");
    }
    let converter = dtoa_double_bits(bits);
    let mut composed = [0u8; 32];
    render(converter.layout(&mut composed))
}

pub fn with_java_float_text<R>(value: f32, render: impl FnOnce(&str) -> R) -> R {
    if value.is_nan() {
        return render("NaN");
    }
    if value.is_infinite() {
        if value.is_sign_negative() {
            return render("-Infinity");
        }
        return render("Infinity");
    }
    let bits = value.to_bits();
    if bits & 0x7fff_ffff == 0 {
        if value.is_sign_negative() {
            return render("-0.0");
        }
        return render("0.0");
    }
    let converter = dtoa_float_bits(bits);
    let mut composed = [0u8; 32];
    render(converter.layout(&mut composed))
}

pub fn java_double_text(value: f64) -> String {
    with_java_double_text(value, str::to_owned)
}

pub fn java_float_text(value: f32) -> String {
    with_java_float_text(value, str::to_owned)
}

pub fn java_double_text_len(value: f64) -> usize {
    with_java_double_text(value, str::len)
}

pub fn java_float_text_len(value: f32) -> usize {
    with_java_float_text(value, str::len)
}

pub(crate) fn java_double_strings(values: &Float64Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_double_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}

pub(crate) fn java_float_strings(values: &Float32Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            with_java_float_text(values.value(index), |text| builder.append_value(text));
        }
    }
    builder.finish()
}
