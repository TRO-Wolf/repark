use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

/// Spark `count_min_sketch` — byte-exact Count-Min sketch over the engine API.
#[must_use]
pub fn count_min_sketch_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SparkCountMinSketch::new()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SparkCountMinSketch {
    signature: Signature,
}

impl SparkCountMinSketch {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

const MURMUR_C1: u32 = 0xcc9e_2d51;
const MURMUR_C2: u32 = 0x1b87_3593;
const MURMUR_FMIX_A: u32 = 0x85eb_ca6b;
const MURMUR_FMIX_B: u32 = 0xc2b2_ae35;
const JAVA_RANDOM_MULT: u64 = 0x5DEE_CE66D;
const JAVA_RANDOM_ADD: u64 = 0xB;
const JAVA_RANDOM_MASK: u64 = (1 << 48) - 1;
const SKETCH_VERSION: u32 = 1;

fn murmur_mix_k(value: u32) -> u32 {
    value
        .wrapping_mul(MURMUR_C1)
        .rotate_left(15)
        .wrapping_mul(MURMUR_C2)
}

fn murmur_mix_h(hash: u32, mixed: u32) -> u32 {
    (hash ^ mixed)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64)
}

fn murmur_fmix(mut hash: u32, length: u32) -> u32 {
    hash ^= length;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(MURMUR_FMIX_A);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(MURMUR_FMIX_B);
    hash ^= hash >> 16;
    hash
}

#[allow(clippy::cast_possible_truncation)]
fn murmur_bytes(data: &[u8], seed: u32) -> u32 {
    let mut hash = seed;
    let end = data.len() - data.len() % 4;
    for word in data[..end].chunks_exact(4) {
        let word = u32::from_le_bytes([word[0], word[1], word[2], word[3]]);
        hash = murmur_mix_h(hash, murmur_mix_k(word));
    }
    for byte in &data[end..] {
        hash = murmur_mix_h(hash, murmur_mix_k(u32::from(*byte)));
    }
    murmur_fmix(hash, data.len() as u32)
}

struct JavaRandom {
    state: u64,
}

#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
impl JavaRandom {
    fn new(seed: i32) -> Self {
        Self {
            state: (i64::from(seed) as u64 ^ JAVA_RANDOM_MULT) & JAVA_RANDOM_MASK,
        }
    }

    fn next(&mut self, bits: u32) -> i32 {
        self.state = (self
            .state
            .wrapping_mul(JAVA_RANDOM_MULT)
            .wrapping_add(JAVA_RANDOM_ADD))
            & JAVA_RANDOM_MASK;
        (self.state >> (48 - bits)) as i32
    }

    fn next_int_bounded(&mut self, bound: i32) -> i32 {
        loop {
            let bits = self.next(31);
            let value = bits % bound;
            if bits - value + (bound - 1) >= 0 {
                return value;
            }
        }
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn sketch_dimensions(eps: f64, confidence: f64) -> (i32, i32) {
    let width = (2.0 / eps).ceil() as i32;
    let depth = (-(-confidence).ln_1p() / 2.0f64.ln()).ceil() as i32;
    (depth, width)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
fn long_bucket(value: i64, seed: i64, width: usize) -> usize {
    let hash = seed.wrapping_mul(value);
    let folded = hash.wrapping_add(((hash as u64) >> 32) as i64);
    ((folded & 0x7fff_ffff) % width as i64) as usize
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
fn binary_buckets(data: &[u8], depth: usize, width: usize) -> Vec<usize> {
    let first = murmur_bytes(data, 0) as i32;
    let second = murmur_bytes(data, first as u32) as i32;
    let width = width as i32;
    (0..depth)
        .map(|row| {
            first
                .wrapping_add((row as i32).wrapping_mul(second))
                .wrapping_rem(width)
                .wrapping_abs() as usize
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sketch {
    depth: usize,
    width: usize,
    seeds: Vec<i64>,
    table: Vec<i64>,
    total: i64,
}

fn invalid(message: String) -> DataFusionError {
    DataFusionError::Plan(message)
}

struct SketchCursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl SketchCursor<'_> {
    fn take<const COUNT: usize>(&mut self) -> Result<[u8; COUNT]> {
        let end = self
            .at
            .checked_add(COUNT)
            .ok_or_else(|| invalid("count_min_sketch state is truncated".to_string()))?;
        let slot = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| invalid("count_min_sketch state is truncated".to_string()))?;
        self.at = end;
        slot.try_into()
            .map_err(|_| invalid("count_min_sketch state is truncated".to_string()))
    }

    fn take_u32(&mut self) -> Result<u32> {
        self.take().map(u32::from_be_bytes)
    }

    fn take_i32(&mut self) -> Result<i32> {
        self.take().map(i32::from_be_bytes)
    }

    fn take_i64(&mut self) -> Result<i64> {
        self.take().map(i64::from_be_bytes)
    }
}

impl Sketch {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn create(eps: f64, confidence: f64, seed: i32) -> Result<Self> {
        if eps <= 0.0 {
            return Err(invalid("Relative error must be positive".to_string()));
        }
        if confidence <= 0.0 || confidence >= 1.0 {
            return Err(invalid(
                "Confidence must be within range (0.0, 1.0)".to_string(),
            ));
        }
        let (depth, width) = sketch_dimensions(eps, confidence);
        let mut random = JavaRandom::new(seed);
        let seeds = (0..depth)
            .map(|_| i64::from(random.next_int_bounded(i32::MAX)))
            .collect::<Vec<_>>();
        let cells = (depth as usize)
            .checked_mul(width as usize)
            .ok_or_else(|| invalid("count_min_sketch depth and width overflow".to_string()))?;
        let mut table = Vec::new();
        table
            .try_reserve_exact(cells)
            .map_err(|_| invalid("count_min_sketch table allocation failed".to_string()))?;
        table.resize(cells, 0);
        Ok(Self {
            depth: depth as usize,
            width: width as usize,
            seeds,
            table,
            total: 0,
        })
    }

    fn add_long(&mut self, value: i64) -> Result<()> {
        if self.width == 0 {
            return Err(invalid(
                "count_min_sketch width is zero, cannot add".to_string(),
            ));
        }
        for row in 0..self.depth {
            let bucket = long_bucket(value, self.seeds[row], self.width);
            let slot = row * self.width + bucket;
            self.table[slot] = self.table[slot].wrapping_add(1);
        }
        self.total = self.total.wrapping_add(1);
        Ok(())
    }

    fn add_binary(&mut self, data: &[u8]) -> Result<()> {
        if self.width == 0 {
            return Err(invalid(
                "count_min_sketch width is zero, cannot add".to_string(),
            ));
        }
        let buckets = binary_buckets(data, self.depth, self.width);
        for (row, bucket) in buckets.iter().enumerate() {
            let slot = row * self.width + bucket;
            self.table[slot] = self.table[slot].wrapping_add(1);
        }
        self.total = self.total.wrapping_add(1);
        Ok(())
    }

    fn merge_from(&mut self, other: &Self) -> Result<()> {
        if self.depth != other.depth {
            return Err(invalid(
                "Cannot merge estimators of different depth".to_string(),
            ));
        }
        if self.width != other.width {
            return Err(invalid(
                "Cannot merge estimators of different width".to_string(),
            ));
        }
        if self.seeds != other.seeds {
            return Err(invalid(
                "Cannot merge estimators of different seed".to_string(),
            ));
        }
        for (left, right) in self.table.iter_mut().zip(other.table.iter()) {
            *left = left.wrapping_add(*right);
        }
        self.total = self.total.wrapping_add(other.total);
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + 8 * self.depth + 8 * self.table.len());
        out.extend_from_slice(&SKETCH_VERSION.to_be_bytes());
        out.extend_from_slice(&self.total.to_be_bytes());
        out.extend_from_slice(&(self.depth as i32).to_be_bytes());
        out.extend_from_slice(&(self.width as i32).to_be_bytes());
        for seed in &self.seeds {
            out.extend_from_slice(&seed.to_be_bytes());
        }
        for cell in &self.table {
            out.extend_from_slice(&cell.to_be_bytes());
        }
        out
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = SketchCursor { bytes, at: 0 };
        let version = cursor.take_u32()?;
        if version != SKETCH_VERSION {
            return Err(invalid(format!(
                "count_min_sketch version {version} is not supported"
            )));
        }
        let total = cursor.take_i64()?;
        let depth = cursor.take_i32()?;
        let width = cursor.take_i32()?;
        if depth < 0 || width < 0 {
            return Err(invalid(
                "count_min_sketch state holds negative dimensions".to_string(),
            ));
        }
        let mut seeds = Vec::with_capacity(depth as usize);
        for _ in 0..depth {
            seeds.push(cursor.take_i64()?);
        }
        let cells = (depth as usize)
            .checked_mul(width as usize)
            .ok_or_else(|| invalid("count_min_sketch state dimensions overflow".to_string()))?;
        let mut table = Vec::with_capacity(cells);
        for _ in 0..cells {
            table.push(cursor.take_i64()?);
        }
        Ok(Self {
            depth: depth as usize,
            width: width as usize,
            seeds,
            table,
            total,
        })
    }
}

fn physical_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let erased: &dyn std::any::Any = expr.as_ref();
    erased
        .downcast_ref::<Literal>()
        .map(|literal| literal.value().clone())
}

#[allow(clippy::cast_precision_loss)]
fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Decimal128(Some(value), _, scale) => {
            Some(*value as f64 / 10f64.powi(i32::from(*scale)))
        }
        ScalarValue::Int8(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        ScalarValue::UInt8(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(f64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(*value as f64),
        _ => None,
    }
}

fn scalar_to_i32(value: &ScalarValue) -> Option<i32> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i32::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i32::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(*value),
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        ScalarValue::Int64(Some(value)) => Some(*value as i32),
        ScalarValue::UInt8(Some(value)) => Some(i32::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i32::from(*value)),
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        ScalarValue::UInt32(Some(value)) => Some(*value as i32),
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        ScalarValue::UInt64(Some(value)) => Some(*value as i32),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct SketchParams {
    eps: f64,
    confidence: f64,
    seed: i32,
}

fn resolve_params(exprs: &[Arc<dyn PhysicalExpr>], display: &str) -> Result<SketchParams> {
    if exprs.len() != 4 {
        return Err(invalid(format!(
            "count_min_sketch requires 4 parameters but got {}",
            exprs.len()
        )));
    }
    let param = |index: usize, what: &str| -> Result<ScalarValue> {
        physical_literal(&exprs[index]).ok_or_else(|| {
            invalid(format!(
                "count_min_sketch {what} must be a literal in {display}"
            ))
        })
    };
    let eps = scalar_to_f64(&param(1, "eps")?).ok_or_else(|| {
        invalid(format!(
            "count_min_sketch eps must be a double literal in {display}"
        ))
    })?;
    let confidence = scalar_to_f64(&param(2, "confidence")?).ok_or_else(|| {
        invalid(format!(
            "count_min_sketch confidence must be a double literal in {display}"
        ))
    })?;
    let seed = scalar_to_i32(&param(3, "seed")?).ok_or_else(|| {
        invalid(format!(
            "count_min_sketch seed must be an integral literal in {display}"
        ))
    })?;
    Ok(SketchParams {
        eps,
        confidence,
        seed,
    })
}

fn render_double(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

fn render_param_scalar(value: &ScalarValue) -> String {
    scalar_to_i32(value)
        .map(|seed| seed.to_string())
        .or_else(|| scalar_to_f64(value).map(render_double))
        .unwrap_or_default()
}

fn render_param_expr(expr: &Expr) -> String {
    match expr {
        Expr::Literal(value, _) => render_param_scalar(value),
        Expr::Column(column) => column.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn sketch_display(args: &[Expr]) -> String {
    let value = args.first().map(render_param_expr).unwrap_or_default();
    let eps = args.get(1).map(render_param_expr).unwrap_or_default();
    let confidence = args.get(2).map(render_param_expr).unwrap_or_default();
    let seed = args.get(3).map(render_param_expr).unwrap_or_default();
    format!("count_min_sketch({value}, {eps}, {confidence}, {seed})")
}

fn physical_display(exprs: &[Arc<dyn PhysicalExpr>]) -> String {
    let parts = exprs
        .iter()
        .map(|expr| {
            physical_literal(expr)
                .as_ref()
                .map(render_param_scalar)
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    format!("count_min_sketch({})", parts.join(", "))
}

impl AggregateUDFImpl for SparkCountMinSketch {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "count_min_sketch"
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        Ok(sketch_display(&params.args))
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.schema_name(params)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 4 {
            return Err(invalid(format!(
                "count_min_sketch requires 4 parameters but got {}",
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let display = physical_display(acc_args.exprs);
        let params = resolve_params(acc_args.exprs, &display)?;
        Ok(Box::new(CountMinAccumulator::new(params)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "sketch"),
                DataType::Binary,
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct CountMinAccumulator {
    sketch: Sketch,
}

impl CountMinAccumulator {
    fn new(params: SketchParams) -> Result<Self> {
        Ok(Self {
            sketch: Sketch::create(params.eps, params.confidence, params.seed)?,
        })
    }

    fn push_integral(&mut self, values: &ArrayRef) -> Result<()> {
        let longs = cast(values, &DataType::Int64)
            .map_err(|err| invalid(format!("count_min_sketch value must be integral: {err}")))?;
        let longs = longs
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("count_min_sketch value must be integral".to_string()))?;
        for row in 0..longs.len() {
            if longs.is_null(row) {
                continue;
            }
            self.sketch.add_long(longs.value(row))?;
        }
        Ok(())
    }

    fn push_utf8(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Utf8 => {
                let strings = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        invalid("count_min_sketch value must be a string".to_string())
                    })?;
                for row in 0..strings.len() {
                    if strings.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(strings.value(row).as_bytes())?;
                }
                Ok(())
            }
            DataType::LargeUtf8 => {
                let strings = values
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        invalid("count_min_sketch value must be a string".to_string())
                    })?;
                for row in 0..strings.len() {
                    if strings.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(strings.value(row).as_bytes())?;
                }
                Ok(())
            }
            _ => {
                let strings = values
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        invalid("count_min_sketch value must be a string".to_string())
                    })?;
                for row in 0..strings.len() {
                    if strings.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(strings.value(row).as_bytes())?;
                }
                Ok(())
            }
        }
    }

    fn push_binary(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Binary => {
                let items = values
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| invalid("count_min_sketch value must be binary".to_string()))?;
                for row in 0..items.len() {
                    if items.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(items.value(row))?;
                }
                Ok(())
            }
            DataType::LargeBinary => {
                let items = values
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| invalid("count_min_sketch value must be binary".to_string()))?;
                for row in 0..items.len() {
                    if items.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(items.value(row))?;
                }
                Ok(())
            }
            DataType::FixedSizeBinary(_) => {
                let items = values
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| invalid("count_min_sketch value must be binary".to_string()))?;
                for row in 0..items.len() {
                    if items.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(items.value(row))?;
                }
                Ok(())
            }
            _ => {
                let items = values
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| invalid("count_min_sketch value must be binary".to_string()))?;
                for row in 0..items.len() {
                    if items.is_null(row) {
                        continue;
                    }
                    self.sketch.add_binary(items.value(row))?;
                }
                Ok(())
            }
        }
    }
}

impl Accumulator for CountMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.to_bytes()))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values.first().ok_or_else(|| {
            invalid("count_min_sketch requires 4 parameters but got 0".to_string())
        })?;
        match values.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                self.push_integral(values)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => self.push_utf8(values),
            DataType::Null => Ok(()),
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => self.push_binary(values),
            other => Err(invalid(format!(
                "count_min_sketch value must be integral, string or binary but got {other}"
            ))),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states.first().ok_or_else(|| {
            invalid("count_min_sketch state must hold binary sketches".to_string())
        })?;
        let states = states
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                invalid("count_min_sketch state must hold binary sketches".to_string())
            })?;
        for group in 0..states.len() {
            if states.is_null(group) {
                continue;
            }
            let other = Sketch::from_bytes(states.value(group))?;
            self.sketch.merge_from(&other)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.to_bytes())))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + 8 * (self.sketch.seeds.len() + self.sketch.table.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::fmt::Write as _;

    const GROUP_ONE: &str = "0000000100000000000000030000000100000004000000005d8d6ab90000000000000001000000000000000100000000000000000000000000000001";
    const GROUP_TWO: &str = "0000000100000000000000010000000100000004000000005d8d6ab90000000000000000000000000000000000000000000000010000000000000000";
    const GROUP_THREE: &str = "0000000100000000000000000000000100000004000000005d8d6ab90000000000000000000000000000000000000000000000000000000000000000";
    const STRINGS_ALL: &str = "0000000100000000000000040000000100000004000000005d8d6ab90000000000000000000000000000000100000000000000010000000000000002";

    fn unhex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex pair"))
            .collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            })
    }

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(count_min_sketch_udaf().as_ref().clone());
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().expect("one batch")
    }

    async fn failure(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(failure) => failure.to_string(),
            Ok(frame) => frame.collect().await.expect_err("must fail").to_string(),
        }
    }

    fn sketch_bytes(batch: &RecordBatch, column: usize, row: usize) -> Vec<u8> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray")
            .value(row)
            .to_vec()
    }

    const FRAME: &str = "(VALUES (1, 'a', 10), (1, 'b', 20), (1, 'b', NULL), (1, 'c', 30), (2, NULL, 5), (3, NULL, NULL)) AS t(g, k, v)";

    #[test]
    fn java_random_draws_match_spark() {
        let mut random = JavaRandom::new(1);
        let draws = (0..3)
            .map(|_| random.next_int_bounded(i32::MAX))
            .collect::<Vec<_>>();
        assert_eq!(draws, vec![1_569_548_985, 215_764_588, 880_641_847]);
    }

    #[test]
    fn dimensions_match_oracle_grid() {
        let grid: Vec<(f64, f64, i32, i32)> = vec![
            (0.5, 0.5, 1, 4),
            (0.5, 0.75, 2, 4),
            (0.5, 0.9, 4, 4),
            (0.5, 0.99, 7, 4),
            (0.5, 0.999, 10, 4),
            (0.5, 0.999_999, 20, 4),
            (0.5, 0.1, 1, 4),
            (0.5, 1e-9, 1, 4),
            (0.1, 0.5, 1, 20),
            (0.1, 0.9, 4, 20),
            (0.1, 0.99, 7, 20),
            (0.01, 0.99, 7, 200),
            (0.001, 0.999_999, 20, 2000),
            (0.0001, 0.75, 2, 20000),
            (0.3, 0.5, 1, 7),
            (0.3, 0.9, 4, 7),
            (0.7, 0.5, 1, 3),
            (2.0, 0.5, 1, 1),
            (100.0, 0.9, 4, 1),
            (0.9, 0.5, 1, 3),
        ];
        for (eps, confidence, depth, width) in grid {
            assert_eq!(
                sketch_dimensions(eps, confidence),
                (depth, width),
                "eps {eps} confidence {confidence}"
            );
        }
    }

    #[tokio::test]
    async fn grouped_int_sketches_match_fixture_bytes() {
        let batch = run(
            &ctx(),
            &format!(
                "SELECT g, count_min_sketch(v, 0.5, 0.5, 1) AS sketch FROM {FRAME} GROUP BY g ORDER BY g"
            ),
        )
        .await;
        assert_eq!(hex(&sketch_bytes(&batch, 1, 0)), GROUP_ONE);
        assert_eq!(hex(&sketch_bytes(&batch, 1, 1)), GROUP_TWO);
        assert_eq!(hex(&sketch_bytes(&batch, 1, 2)), GROUP_THREE);
    }

    #[tokio::test]
    async fn global_string_sketch_matches_fixture_bytes() {
        let batch = run(
            &ctx(),
            &format!("SELECT count_min_sketch(k, 0.5, 0.5, 1) AS sketch FROM {FRAME}"),
        )
        .await;
        assert_eq!(hex(&sketch_bytes(&batch, 0, 0)), STRINGS_ALL);
    }

    #[tokio::test]
    async fn extreme_ints_land_on_oracle_cells() {
        let batch = run(
            &ctx(),
            "SELECT count_min_sketch(v, 0.00002, 0.5, 1) AS sketch FROM (VALUES (2147483647), (-2147483648)) AS t(v)",
        )
        .await;
        let sketch = Sketch::from_bytes(&sketch_bytes(&batch, 0, 0)).expect("parse");
        assert_eq!((sketch.depth, sketch.width), (1, 100_000));
        assert_eq!(sketch.total, 2);
        let hot = sketch
            .table
            .iter()
            .enumerate()
            .filter(|(_, cell)| **cell > 0)
            .collect::<Vec<_>>();
        assert_eq!(hot, vec![(9155, &2)]);
    }

    #[test]
    fn two_partition_merge_equals_single() {
        let params = SketchParams {
            eps: 0.5,
            confidence: 0.5,
            seed: 1,
        };
        let feed = |values: Vec<Option<i32>>| -> CountMinAccumulator {
            let mut acc = CountMinAccumulator::new(params).expect("params");
            let array: ArrayRef = Arc::new(Int32Array::from(values));
            acc.update_batch(&[array]).expect("update");
            acc
        };
        let mut whole = feed(vec![Some(10), Some(20), None, Some(30)]);
        let whole_bytes = whole.evaluate().expect("evaluate");
        let mut left = feed(vec![Some(10), Some(20)]);
        let right = feed(vec![None, Some(30)]);
        let state = right.sketch.to_bytes();
        let state_array: ArrayRef = Arc::new(BinaryArray::from(vec![state.as_slice()]));
        left.merge_batch(&[state_array]).expect("merge");
        let merged_bytes = left.evaluate().expect("evaluate");
        assert_eq!(merged_bytes, whole_bytes);
        assert_eq!(merged_bytes, ScalarValue::Binary(Some(unhex(GROUP_ONE))));
    }

    #[test]
    fn incompatible_merge_is_refused() {
        let params = SketchParams {
            eps: 0.5,
            confidence: 0.5,
            seed: 1,
        };
        let other_seed = SketchParams { seed: 2, ..params };
        let other_eps = SketchParams { eps: 0.1, ..params };
        let mut acc = CountMinAccumulator::new(params).expect("params");
        for foreign in [
            CountMinAccumulator::new(other_seed).expect("params"),
            CountMinAccumulator::new(other_eps).expect("params"),
        ] {
            let state_array: ArrayRef = Arc::new(BinaryArray::from(vec![
                foreign.sketch.to_bytes().as_slice(),
            ]));
            acc.merge_batch(&[state_array]).expect_err("must refuse");
        }
    }

    #[tokio::test]
    async fn all_null_input_yields_empty_sketch() {
        let batch = run(
            &ctx(),
            "SELECT count_min_sketch(v, 0.5, 0.5, 1) AS sketch FROM (VALUES (NULL), (NULL)) AS t(v)",
        )
        .await;
        assert_eq!(hex(&sketch_bytes(&batch, 0, 0)), GROUP_THREE);
    }

    #[tokio::test]
    async fn bad_params_are_refused() {
        let ctx = ctx();
        let message = failure(
            &ctx,
            "SELECT count_min_sketch(v, 0.0, 0.5, 1) FROM (VALUES (10)) AS t(v)",
        )
        .await;
        assert!(
            message.contains("Relative error must be positive"),
            "{message}"
        );
        let message = failure(
            &ctx,
            "SELECT count_min_sketch(v, 0.5, 1.0, 1) FROM (VALUES (10)) AS t(v)",
        )
        .await;
        assert!(
            message.contains("Confidence must be within range"),
            "{message}"
        );
    }

    #[test]
    fn corrupt_state_is_refused() {
        assert!(Sketch::from_bytes(&[]).is_err());
        assert!(Sketch::from_bytes(&[0, 0, 0, 2, 0, 0, 0, 0]).is_err());
        assert!(Sketch::from_bytes(&unhex(GROUP_ONE)[..20]).is_err());
    }
}
