//! Spark-compatible seeded `rand` / `randn` using XORShiftRandom and MurmurHash3.
//!
//! RePark fixes the single-node partition index at zero and restarts each invoke batch. This is
//! deterministic for one batch and a documented residual for multi-batch execution.

// Intentional Java/Scala bit-width casts for XORShift + MurmurHash3 bit-exact parity.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// Scala `MurmurHash3.arraySeed` (spark `XORShiftRandom.hashSeed`).
const MURMUR3_ARRAY_SEED: u32 = 0x3c_07_4a_61;

/// ===========================================================================================
/// Spark `XORShiftRandom` (extends `java.util.Random`; only `next(bits)` overridden).
/// ===========================================================================================
#[derive(Clone, Debug)]
struct XorShiftRandom {
    seed: i64,
    /// Cached next Gaussian (`java.util.Random` polar method).
    next_gaussian: Option<f64>,
}

impl XorShiftRandom {
    fn new(init: i64) -> Self {
        Self {
            seed: hash_seed(init),
            next_gaussian: None,
        }
    }

    /// `XORShiftRandom.next(bits)` — Marsaglia xorshift on the hashed seed.
    fn next_bits(&mut self, bits: u32) -> i32 {
        let mut next_seed = self.seed as u64;
        next_seed ^= next_seed << 21;
        next_seed ^= next_seed >> 35;
        next_seed ^= next_seed << 4;
        self.seed = next_seed as i64;
        let mask = (1u64 << bits) - 1;
        (next_seed & mask) as i32
    }

    /// Generate a `java.util.Random.nextDouble()` value from XORShift output.
    fn next_double(&mut self) -> f64 {
        let high = i64::from(self.next_bits(26));
        let low = i64::from(self.next_bits(27));
        ((high << 27) + low) as f64 / ((1i64 << 53) as f64)
    }

    /// `java.util.Random.nextGaussian()` polar (Box–Muller) method.
    fn next_gaussian(&mut self) -> f64 {
        if let Some(cached) = self.next_gaussian.take() {
            return cached;
        }
        loop {
            let v1 = 2.0 * self.next_double() - 1.0;
            let v2 = 2.0 * self.next_double() - 1.0;
            let s = v1 * v1 + v2 * v2;
            if s >= 1.0 || s == 0.0 {
                continue;
            }
            let multiplier = (-2.0 * s.ln() / s).sqrt();
            self.next_gaussian = Some(v2 * multiplier);
            return v1 * multiplier;
        }
    }
}

/// ===========================================================================================
/// Spark `XORShiftRandom.hashSeed` — double `MurmurHash3` over big-endian long bytes.
/// ===========================================================================================
fn hash_seed(seed: i64) -> i64 {
    let bytes = seed.to_be_bytes();
    let low_bits = murmur3_x86_32(&bytes, MURMUR3_ARRAY_SEED);
    let high_bits = murmur3_x86_32(&bytes, low_bits);
    (i64::from(high_bits) << 32) | (i64::from(low_bits) & 0xffff_ffff)
}

/// `MurmurHash3` `x86_32` (Scala `MurmurHash3.bytesHash` / Guava / Spark).
fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let length = data.len();
    let mut h1 = seed;
    let rounded_end = length & !3;
    let mut index = 0;
    while index < rounded_end {
        let mut k1 = u32::from(data[index])
            | (u32::from(data[index + 1]) << 8)
            | (u32::from(data[index + 2]) << 16)
            | (u32::from(data[index + 3]) << 24);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe654_6b64);
        index += 4;
    }
    let mut k1 = 0u32;
    let tail = length & 3;
    if tail == 3 {
        k1 ^= u32::from(data[rounded_end + 2]) << 16;
    }
    if tail >= 2 {
        k1 ^= u32::from(data[rounded_end + 1]) << 8;
    }
    if tail >= 1 {
        k1 ^= u32::from(data[rounded_end]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }
    h1 ^= length as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1
}

/// ===========================================================================================
/// Registered Spark `rand` / `randn` UDF instances.
/// ===========================================================================================
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        spark_rand_udf(),
        spark_randn_udf(),
        spark_randstr_udf(),
        spark_uniform_udf(),
    ]
}

/// Spark `rand([seed])` — uniform [0, 1). Alias `random` overwrites DF's unseeded `random()`.
#[must_use]
pub fn spark_rand_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRand::new()).with_aliases(["random"]))
}

/// Spark `randn([seed])` — standard normal.
#[must_use]
pub fn spark_randn_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRandn::new()))
}

/// Spark `randstr(length[, seed])` — a random alphanumeric string.
#[must_use]
pub fn spark_randstr_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkRandstr::new()))
}

/// Spark `uniform(min, max[, seed])` — i.i.d. values in `[min, max)`.
#[must_use]
pub fn spark_uniform_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkUniform::new()))
}

/// Cap `randstr` length to keep per-row allocation failures catchable.
const MAX_RANDSTR_LENGTH: i64 = 1_000_000;

/// Spark `randstr`'s character pool, in Spark's own order: digits, then lower, then upper.
///
/// The order is load-bearing, not cosmetic — the index comes from the same `XORShift` stream
/// `rand` uses, so a different ordering yields a different string for the same seed.
const RANDSTR_POOL: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// ===========================================================================================
/// Shared seed extraction (null seed → 0 like Spark `UnresolvedSeed` fallback examples).
/// ===========================================================================================
fn extract_seed(args: &[ColumnarValue]) -> Result<i64> {
    if args.is_empty() {
        // Unseeded calls use deterministic seed zero; explicit seeds select other deterministic streams.
        return Ok(0);
    }
    match &args[0] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
            ScalarValue::Int64(None) | ScalarValue::Int32(None) | ScalarValue::Null => Ok(0),
            other => exec_err!("spark rand/randn seed must be int/long, got {other}"),
        },
        ColumnarValue::Array(_) => {
            exec_err!("spark rand/randn seed must be a foldable scalar, not a column")
        }
    }
}

fn fill_doubles(number_rows: usize, seed: i64, gaussian: bool) -> Float64Array {
    // Single-node: partitionIndex = 0 (Spark: seed + partitionIndex).
    let mut rng = XorShiftRandom::new(seed);
    let mut values = Vec::with_capacity(number_rows);
    for _ in 0..number_rows {
        values.push(if gaussian {
            rng.next_gaussian()
        } else {
            rng.next_double()
        });
    }
    Float64Array::from(values)
}

/// ===========================================================================================
/// `SparkRand` — name `rand` (also registered as `random` for DataFusion SQL parity).
/// ===========================================================================================
#[derive(Debug)]
struct SparkRand {
    signature: Signature,
}

impl SparkRand {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl PartialEq for SparkRand {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRand {}

impl Hash for SparkRand {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRand {
    crate::shim_udf_boilerplate!("rand");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let seed = extract_seed(&args.args)?;
        let array = fill_doubles(args.number_rows, seed, false);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// ===========================================================================================
/// `SparkRandn` — name `randn`.
/// ===========================================================================================
#[derive(Debug)]
struct SparkRandn {
    signature: Signature,
}

impl SparkRandn {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl PartialEq for SparkRandn {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRandn {}

impl Hash for SparkRandn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRandn {
    crate::shim_udf_boilerplate!("randn");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let seed = extract_seed(&args.args)?;
        let array = fill_doubles(args.number_rows, seed, true);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_seed_zero_matches_spark() {
        // Verified against Python/Scala MurmurHash3 + Spark XORShiftRandom.hashSeed(0).
        assert_eq!(hash_seed(0), -9_171_266_021_732_529_057);
    }

    #[test]
    fn rand_seed_zero_first_value_matches_spark_docs() {
        // Spark docs: SELECT rand(0) → 0.7604953758285915
        let mut rng = XorShiftRandom::new(0);
        let first = rng.next_double();
        assert!(
            (first - 0.760_495_375_828_591_5).abs() < 1e-15,
            "got {first}"
        );
    }

    #[test]
    fn sample_by_seed_zero_count_in_spark_band() {
        // Apache FunctionsTests.test_sampleby: 100 rows, b=i%3, fractions {0:0.5,1:0.5}, seed=0
        let mut rng = XorShiftRandom::new(0);
        let mut count = 0;
        for a in 0..100 {
            let b = a % 3;
            let r = rng.next_double();
            let fraction = match b {
                0 | 1 => 0.5,
                _ => 0.0,
            };
            if r < fraction {
                count += 1;
            }
        }
        assert!((35..=36).contains(&count), "count={count}");
    }
}

/// ===========================================================================================
/// `SparkRandstr` — `randstr(length[, seed])`.
/// ===========================================================================================
///
/// Spark requires `length` to be a **constant** SMALLINT/INT, so a column argument is refused
/// loudly rather than silently reading the first row.
#[derive(Debug)]
struct SparkRandstr {
    signature: Signature,
}

impl SparkRandstr {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkRandstr {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkRandstr {}

impl Hash for SparkRandstr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkRandstr {
    crate::shim_udf_boilerplate!("randstr");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let length = constant_i64(args.args.first(), "randstr length")?;
        if !(0..=MAX_RANDSTR_LENGTH).contains(&length) {
            return exec_err!(
                "randstr length must be between 0 and {MAX_RANDSTR_LENGTH}, got {length}"
            );
        }
        let length = usize::try_from(length).map_err(|_| {
            DataFusionError::Execution(format!("randstr length must not be negative, got {length}"))
        })?;
        if length.saturating_mul(args.number_rows) > i32::MAX as usize {
            return exec_err!(
                "randstr would build {length} characters x {} rows, past the {} byte limit of a \
                 string column; reduce the length or the batch",
                args.number_rows,
                i32::MAX
            );
        }
        let seed = extract_seed(args.args.get(1..).unwrap_or_default())?;
        let mut rng = XorShiftRandom::new(seed);
        let mut values = Vec::with_capacity(args.number_rows);
        for _ in 0..args.number_rows {
            let mut text = String::with_capacity(length);
            for _ in 0..length {
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "next_double is in [0, 1), so the product is in [0, pool_len)"
                )]
                let index = (rng.next_double() * RANDSTR_POOL.len() as f64) as usize;
                let byte = RANDSTR_POOL[index.min(RANDSTR_POOL.len() - 1)];
                text.push(char::from(byte));
            }
            values.push(Some(text));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

/// ===========================================================================================
/// `SparkUniform` — `uniform(min, max[, seed])`.
/// ===========================================================================================
///
/// **The return type follows the argument types**, which is Spark's documented rule: two integer
/// bounds give an integer result, anything else gives a double. Getting this wrong would be a
/// silent type change rather than an error, so the bounds are inspected at planning time.
#[derive(Debug)]
struct SparkUniform {
    signature: Signature,
}

impl SparkUniform {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkUniform {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkUniform {}

impl Hash for SparkUniform {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkUniform {
    crate::shim_udf_boilerplate!("uniform");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let integral = arg_types
            .iter()
            .take(2)
            .all(datafusion::arrow::datatypes::DataType::is_integer);
        Ok(if integral {
            DataType::Int64
        } else {
            DataType::Float64
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let low = constant_f64(args.args.first(), "uniform min")?;
        let high = constant_f64(args.args.get(1), "uniform max")?;
        match low.partial_cmp(&high) {
            Some(Ordering::Less | Ordering::Equal) => {}
            Some(Ordering::Greater) => {
                return exec_err!("uniform min must not exceed max, got {low} and {high}");
            }
            None => return exec_err!("uniform bounds must not be NaN, got {low} and {high}"),
        }
        let seed = extract_seed(args.args.get(2..).unwrap_or_default())?;
        let mut rng = XorShiftRandom::new(seed);
        let integral = matches!(args.return_field.data_type(), DataType::Int64);

        if integral {
            let mut values = Vec::with_capacity(args.number_rows);
            for _ in 0..args.number_rows {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "bounds are finite and the draw stays within them"
                )]
                let drawn = low.mul_add(1.0, (high - low) * rng.next_double()) as i64;
                values.push(Some(drawn));
            }
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))))
        } else {
            let mut values = Vec::with_capacity(args.number_rows);
            for _ in 0..args.number_rows {
                values.push(low + (high - low) * rng.next_double());
            }
            Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
        }
    }
}

/// A bound that Spark requires to be constant — a column argument is a loud refusal, never a
/// silent read of row zero.
fn constant_i64(value: Option<&ColumnarValue>, what: &str) -> Result<i64> {
    match value {
        Some(ColumnarValue::Scalar(scalar)) => match scalar {
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
            ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
            other => exec_err!("{what} must be a non-null integer constant, got {other}"),
        },
        Some(ColumnarValue::Array(_)) => {
            exec_err!("{what} must be a constant, not a column (Spark requires a literal)")
        }
        None => exec_err!("{what} is required"),
    }
}

fn constant_f64(value: Option<&ColumnarValue>, what: &str) -> Result<f64> {
    match value {
        Some(ColumnarValue::Scalar(scalar)) => scalar
            .cast_to(&DataType::Float64)
            .ok()
            .and_then(|cast| match cast {
                ScalarValue::Float64(Some(v)) => Some(v),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Execution(format!("{what} must be a non-null numeric constant"))
            }),
        Some(ColumnarValue::Array(_)) => {
            exec_err!("{what} must be a constant, not a column (Spark requires a literal)")
        }
        None => exec_err!("{what} is required"),
    }
}
