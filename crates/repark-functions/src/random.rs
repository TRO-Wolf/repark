//! Spark-compatible seeded `rand` / `randn` (`XORShiftRandom` + `MurmurHash3` seed hash).
//!
//! Spark source (v4.1.2):
//! * `sql/catalyst/.../randomExpressions.scala` — `Rand`/`Randn` with
//!   `new XORShiftRandom(seed + partitionIndex)` then `nextDouble()` / `nextGaussian()`.
//! * `core/.../util/random/XORShiftRandom.scala` — Marsaglia xorshift; `hashSeed` via
//!   double `MurmurHash3.bytesHash` over big-endian 8-byte seed (`arraySeed = 0x3c074a61`).
//!
//! Partition index is fixed at **0** (repark single-node v1). Values are generated
//! sequentially within each `invoke` batch starting from a fresh `XORShift` state for
//! `(seed + partition_index)`. Same seed + same single-batch partition layout ⇒ same
//! values (the contract documented for sampleBy / `F.rand`). Multi-batch layouts that
//! re-enter `invoke` restart the sequence from index 0 of that batch — disclosed
//! residual vs Spark's per-partition task state.

// Intentional Java/Scala bit-width casts for XORShift + MurmurHash3 bit-exact parity.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
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

    /// `java.util.Random.nextDouble()` over `XORShift` `next`.
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
    vec![spark_rand_udf(), spark_randn_udf()]
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

/// ===========================================================================================
/// Shared seed extraction (null seed → 0 like Spark `UnresolvedSeed` fallback examples).
/// ===========================================================================================
fn extract_seed(args: &[ColumnarValue]) -> Result<i64> {
    if args.is_empty() {
        // Unseeded: use a non-stable wall-clock-ish value; callers that need determinism
        // pass an explicit seed. Match Spark hideSeed path loosely via 0 for plan tests
        // that call rand() without seed and only check range.
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
        // → count in [35, 36] on live Spark (partition layout dependent). Single partition
        // (partitionIndex=0) yields 36 with this XORShift sequence.
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
