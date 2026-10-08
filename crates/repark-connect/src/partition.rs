use std::collections::BTreeMap;
use std::fmt;

use arrow::datatypes::i256;

use crate::error::{ConnectError, REGISTRY, Result};
use crate::settings::{DeclaredSetting, SpecRefusal, Spelling};

pub const PARTITION_COLUMN_KEY: &str = "partitionColumn";
pub const LOWER_BOUND_KEY: &str = "lowerBound";
pub const UPPER_BOUND_KEY: &str = "upperBound";
pub const NUM_PARTITIONS_KEY: &str = "numPartitions";
pub const PARTITIONED_READ_ROW: &str = "CONNECT-DECL-pg-partitioned-read";

pub const MAX_STRIDES: i64 = 10_000;

const PRECISION: u32 = 34;
const STRIDE_SCALE: i32 = 18;
const MAX_QUOTIENT_STEPS: u32 = 160;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionRefusal {
    Incomplete,
    NotInteger {
        option: &'static str,
    },
    Reversed {
        lower: i64,
        upper: i64,
    },
    QueryOption,
    ColumnNotFound {
        column: String,
        columns: Vec<String>,
    },
    AmbiguousColumn {
        column: String,
        matches: Vec<String>,
    },
    ColumnType {
        found: &'static str,
    },
    DeclaredColumnType {
        column: String,
        postgres_type: &'static str,
    },
    Strides,
    TooManyStrides {
        strides: i64,
    },
}

impl fmt::Display for PartitionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionRefusal::Incomplete => f.write_str(
                "When reading JDBC data sources, users need to specify all or none for the \
                 following options: 'partitionColumn', 'lowerBound', 'upperBound', and \
                 'numPartitions'",
            ),
            PartitionRefusal::NotInteger { option } => {
                let bits = if *option == NUM_PARTITIONS_KEY {
                    32
                } else {
                    64
                };
                write!(f, "`{option}` must be a {bits}-bit integer")
            }
            PartitionRefusal::TooManyStrides { strides } => write!(
                f,
                "a partitioned read of {strides} strides is declared but not supported yet: at \
                 most {MAX_STRIDES} strides run under one snapshot; lower `numPartitions` \
                 (registry row {PARTITIONED_READ_ROW} in {REGISTRY})"
            ),
            PartitionRefusal::Reversed { lower, upper } => write!(
                f,
                "Operation not allowed: the lower bound of partitioning column is larger than \
                 the upper bound. Lower bound: {lower}; Upper bound: {upper}"
            ),
            PartitionRefusal::QueryOption => f.write_str(
                "Options 'query' and 'partitionColumn' can not be specified together. Please \
                 define the query using `dbtable` option instead, as a parenthesised subquery \
                 with an alias: `(select c1, c2 from t1) as subq`",
            ),
            PartitionRefusal::ColumnNotFound { column, columns } => write!(
                f,
                "User-defined partition column {column} not found in the JDBC relation: {}",
                columns.join(", ")
            ),
            PartitionRefusal::AmbiguousColumn { column, matches } => write!(
                f,
                "partition column {column} matches more than one column of the JDBC relation: \
                 {}; give the exact name",
                matches.join(", ")
            ),
            PartitionRefusal::ColumnType { found } => write!(
                f,
                "Partition column type should be numeric, date, or timestamp, but {found} found."
            ),
            PartitionRefusal::DeclaredColumnType {
                column,
                postgres_type,
            } => write!(
                f,
                "partition column `{column}` has Postgres type `{postgres_type}`: only `int2`, \
                 `int4` and `int8` columns partition yet (registry row {PARTITIONED_READ_ROW} \
                 in {REGISTRY})"
            ),
            PartitionRefusal::Strides => f.write_str(
                "the partition bounds give strides that do not increase; widen the bounds or \
                 lower `numPartitions`",
            ),
        }
    }
}

fn refuse<T>(refusal: PartitionRefusal) -> Result<T> {
    Err(ConnectError::PartitionedRead { refusal })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionOptions {
    pub column: Option<String>,
    pub lower_bound: Option<String>,
    pub upper_bound: Option<String>,
    pub num_partitions: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    pub column: String,
    pub lower_bound: String,
    pub upper_bound: String,
    pub num_partitions: i32,
}

impl PartitionSpec {
    #[allow(clippy::missing_errors_doc)]
    pub fn bounds(&self) -> Result<(i64, i64)> {
        let lower = integer(LOWER_BOUND_KEY, &self.lower_bound)?;
        let upper = integer(UPPER_BOUND_KEY, &self.upper_bound)?;
        Ok((lower, upper))
    }
}

fn take_key(props: &mut BTreeMap<String, String>, key: &str) -> Result<Option<String>> {
    let spellings: Vec<String> = props
        .keys()
        .filter(|name| name.eq_ignore_ascii_case(key))
        .cloned()
        .collect();
    let mut taken = None;
    for spelling in spellings {
        let value = props.remove(&spelling);
        if taken.is_some() {
            return Err(ConnectError::InvalidSpecification {
                key: Spelling::Key(key.to_string()),
                reason: SpecRefusal::Conflict {
                    other: Spelling::Key(spelling),
                },
            });
        }
        taken = value;
    }
    Ok(taken)
}

fn merged<T>(key: &'static str, explicit: Option<T>, prop: Option<T>) -> Result<Option<T>> {
    match (explicit, prop) {
        (Some(_), Some(_)) => Err(ConnectError::InvalidSpecification {
            key: Spelling::Key(key.to_string()),
            reason: SpecRefusal::Conflict {
                other: Spelling::Key(format!("{key} in the properties")),
            },
        }),
        (explicit, prop) => Ok(explicit.or(prop)),
    }
}

fn integer(option: &'static str, text: &str) -> Result<i64> {
    match text.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(_) => refuse(PartitionRefusal::NotInteger { option }),
    }
}

fn not_a_count() -> PartitionRefusal {
    PartitionRefusal::NotInteger {
        option: NUM_PARTITIONS_KEY,
    }
}

impl PartitionOptions {
    #[allow(clippy::missing_errors_doc)]
    pub fn of(
        column: Option<String>,
        lower_bound: Option<i64>,
        upper_bound: Option<i64>,
        num_partitions: Option<i64>,
    ) -> Result<PartitionOptions> {
        let num_partitions = match num_partitions.map(i32::try_from) {
            None => None,
            Some(Ok(count)) => Some(count),
            Some(Err(_)) => return refuse(not_a_count()),
        };
        Ok(PartitionOptions {
            column,
            lower_bound: lower_bound.map(|bound| bound.to_string()),
            upper_bound: upper_bound.map(|bound| bound.to_string()),
            num_partitions,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn with_props(self, props: &mut BTreeMap<String, String>) -> Result<PartitionOptions> {
        if let Some(key) = props
            .keys()
            .find(|name| name.eq_ignore_ascii_case("predicates"))
        {
            return Err(ConnectError::DeclaredSetting {
                key: Spelling::Key(key.clone()),
                declared: DeclaredSetting::PartitionedRead,
            });
        }
        let column = take_key(props, PARTITION_COLUMN_KEY)?;
        let lower = take_key(props, LOWER_BOUND_KEY)?;
        let upper = take_key(props, UPPER_BOUND_KEY)?;
        let count = match take_key(props, NUM_PARTITIONS_KEY)? {
            Some(text) => match text.parse::<i32>() {
                Ok(count) => Some(count),
                Err(_) => return refuse(not_a_count()),
            },
            None => None,
        };
        Ok(PartitionOptions {
            column: merged(PARTITION_COLUMN_KEY, self.column, column)?,
            lower_bound: merged(LOWER_BOUND_KEY, self.lower_bound, lower)?,
            upper_bound: merged(UPPER_BOUND_KEY, self.upper_bound, upper)?,
            num_partitions: merged(NUM_PARTITIONS_KEY, self.num_partitions, count)?,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn spec(self) -> Result<Option<PartitionSpec>> {
        match (
            self.column,
            self.lower_bound,
            self.upper_bound,
            self.num_partitions,
        ) {
            (None, None, None, _) => Ok(None),
            (Some(column), Some(lower_bound), Some(upper_bound), Some(num_partitions)) => {
                Ok(Some(PartitionSpec {
                    column,
                    lower_bound,
                    upper_bound,
                    num_partitions,
                }))
            }
            _ => refuse(PartitionRefusal::Incomplete),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rounding {
    Down,
    HalfUp,
    HalfEven,
}

#[derive(Clone, Copy)]
struct Decimal {
    negative: bool,
    magnitude: i256,
    scale: i32,
}

fn ten() -> i256 {
    i256::from_i128(10)
}

fn pow10(exponent: u32) -> Option<i256> {
    (0..exponent).try_fold(i256::ONE, |power, _| power.checked_mul(ten()))
}

fn digits(magnitude: i256) -> u32 {
    let mut rest = magnitude;
    let mut count = 0;
    while rest > i256::ZERO {
        rest = rest.wrapping_div(ten());
        count += 1;
    }
    count
}

fn is_odd(value: i256) -> bool {
    value.wrapping_rem(i256::from_i128(2)) != i256::ZERO
}

fn cut(magnitude: i256, drop: u32, rounding: Rounding, sticky: bool) -> Option<i256> {
    if drop == 0 {
        return Some(magnitude);
    }
    let unit = pow10(drop)?;
    let kept = magnitude.checked_div(unit)?;
    let rest = magnitude.checked_rem(unit)?;
    let half = unit.checked_div(i256::from_i128(2))?;
    let up = match rounding {
        Rounding::Down => false,
        Rounding::HalfUp => rest >= half,
        Rounding::HalfEven => rest > half || (rest == half && (sticky || is_odd(kept))),
    };
    if up {
        kept.checked_add(i256::ONE)
    } else {
        Some(kept)
    }
}

impl Decimal {
    fn whole(value: i64) -> Decimal {
        let wide = i256::from_i128(i128::from(value));
        Decimal {
            negative: value < 0,
            magnitude: if value < 0 { wide.wrapping_neg() } else { wide },
            scale: 0,
        }
    }

    fn signed(self) -> i256 {
        if self.negative {
            self.magnitude.wrapping_neg()
        } else {
            self.magnitude
        }
    }

    fn of(signed: i256, scale: i32) -> Decimal {
        let negative = signed < i256::ZERO;
        Decimal {
            negative,
            magnitude: if negative {
                signed.wrapping_neg()
            } else {
                signed
            },
            scale,
        }
    }

    fn rounded(mut self, mut sticky: bool) -> Option<Decimal> {
        loop {
            let excess = digits(self.magnitude).saturating_sub(PRECISION);
            if excess == 0 {
                return Some(self);
            }
            self.magnitude = cut(self.magnitude, excess, Rounding::HalfEven, sticky)?;
            self.scale = self.scale.checked_sub(i32::try_from(excess).ok()?)?;
            sticky = false;
        }
    }

    fn rescaled(self, scale: i32, rounding: Rounding) -> Option<Decimal> {
        let magnitude = if scale >= self.scale {
            let grow = u32::try_from(scale.checked_sub(self.scale)?).ok()?;
            self.magnitude.checked_mul(pow10(grow)?)?
        } else {
            let drop = u32::try_from(self.scale.checked_sub(scale)?).ok()?;
            cut(self.magnitude, drop, rounding, false)?
        };
        Some(Decimal {
            negative: self.negative,
            magnitude,
            scale,
        })
    }

    fn minus(self, other: Decimal) -> Option<Decimal> {
        let scale = self.scale.max(other.scale);
        let left = self.rescaled(scale, Rounding::Down)?.signed();
        let right = other.rescaled(scale, Rounding::Down)?.signed();
        Decimal::of(left.checked_sub(right)?, scale).rounded(false)
    }

    fn times(self, other: Decimal) -> Option<Decimal> {
        Decimal {
            negative: self.negative != other.negative,
            magnitude: self.magnitude.checked_mul(other.magnitude)?,
            scale: self.scale.checked_add(other.scale)?,
        }
        .rounded(false)
    }

    fn over(self, other: Decimal) -> Option<Decimal> {
        if other.magnitude == i256::ZERO {
            return None;
        }
        let mut quotient = self.magnitude.checked_div(other.magnitude)?;
        let mut rest = self.magnitude.checked_rem(other.magnitude)?;
        let mut scale = self.scale.checked_sub(other.scale)?;
        let mut steps = 0;
        while rest != i256::ZERO && digits(quotient) <= PRECISION {
            if steps == MAX_QUOTIENT_STEPS {
                return None;
            }
            rest = rest.checked_mul(ten())?;
            quotient = quotient
                .checked_mul(ten())?
                .checked_add(rest.checked_div(other.magnitude)?)?;
            rest = rest.checked_rem(other.magnitude)?;
            scale = scale.checked_add(1)?;
            steps += 1;
        }
        Decimal {
            negative: self.negative != other.negative,
            magnitude: quotient,
            scale,
        }
        .rounded(rest != i256::ZERO)
    }

    fn to_long(self) -> Option<i64> {
        let whole = self.rescaled(0, Rounding::Down)?.signed();
        let bytes = whole.to_le_bytes();
        let low: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
        Some(i64::from_le_bytes(low))
    }
}

fn spark_cuts(lower: i64, upper: i64, count: i64) -> Option<Vec<i64>> {
    let parts = Decimal::whole(count);
    let upper_stride = Decimal::whole(upper)
        .over(parts)?
        .rescaled(STRIDE_SCALE, Rounding::HalfEven)?;
    let lower_stride = Decimal::whole(lower)
        .over(parts)?
        .rescaled(STRIDE_SCALE, Rounding::HalfEven)?;
    let precise = upper_stride.minus(lower_stride)?;
    let stride = precise.to_long()?;
    let step = Decimal::whole(stride);
    let lost = precise.minus(step)?.times(parts)?.over(step)?;
    let shift = lost
        .over(Decimal::whole(2))?
        .times(step)?
        .rescaled(0, Rounding::HalfUp)?
        .to_long()?;
    let mut current = lower.wrapping_add(shift);
    let mut cuts = Vec::new();
    for _ in 1..count {
        current = current.wrapping_add(stride);
        cuts.push(current);
    }
    Some(cuts)
}

#[allow(clippy::missing_errors_doc)]
pub fn stride_cuts(lower: i64, upper: i64, num_partitions: i64) -> Result<Vec<i64>> {
    if num_partitions <= 1 || lower == upper {
        return Ok(Vec::new());
    }
    if lower > upper {
        return refuse(PartitionRefusal::Reversed { lower, upper });
    }
    let span = upper.wrapping_sub(lower);
    let count = if span >= num_partitions || span < 0 {
        num_partitions
    } else {
        span
    };
    if count > MAX_STRIDES {
        return refuse(PartitionRefusal::TooManyStrides { strides: count });
    }
    let cuts = spark_cuts(lower, upper, count).filter(|cuts| {
        cuts.windows(2)
            .all(|pair| matches!(pair, [low, high] if low < high))
    });
    match cuts {
        Some(cuts) => Ok(cuts),
        None => refuse(PartitionRefusal::Strides),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stride {
    pub lower: Option<i64>,
    pub upper: Option<i64>,
}

#[must_use]
pub fn strides(cuts: &[i64]) -> Vec<Stride> {
    if cuts.is_empty() {
        return Vec::new();
    }
    let lowers = std::iter::once(None).chain(cuts.iter().copied().map(Some));
    let uppers = cuts.iter().copied().map(Some).chain(std::iter::once(None));
    lowers
        .zip(uppers)
        .map(|(lower, upper)| Stride { lower, upper })
        .collect()
}
