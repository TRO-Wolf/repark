//! Spark `CAST(TIMESTAMP AS <numeric>)` scaling — the embedded UDFs the analyzer rewrite uses.
//!
//! # The class (divergence registry row TZ-5)
//!
//! Spark's `Cast(TimestampType, LongType)` is **epoch SECONDS**; repark stored timestamps as
//! `Timestamp(Nanosecond, _)` and let DataFusion's cast reinterpret the raw tick value, so
//! `CAST(ts AS BIGINT)` came back a factor of 10⁹ too large — a silently-wrong answer, correctly
//! signed, on the one shape a migrated job writes to get an epoch. `to_timestamp('1969-12-31T
//! 23:30:00Z')` cast to `BIGINT` was `-1800000000000` where Spark says `-1800`.
//!
//! # What Spark actually does (probed against live Spark 4.1.2, `task/tz5-cast-seconds-ledger.md`)
//!
//! | Target | Spark | Note |
//! |---|---|---|
//! | `BIGINT` / `INT` / `SMALLINT` | **floor** of epoch seconds | `Math.floorDiv`, not truncation |
//! | `DOUBLE` / `FLOAT` / `DECIMAL(p,s)` | exact fractional epoch seconds | `-0.5s → -0.5` |
//!
//! **Floor, not truncate-toward-zero** — that is the whole reason this module exists rather than
//! a two-line arrow cast hop through `Timestamp(Second, _)`. Arrow's timestamp down-scale divides
//! toward zero, so `1969-12-31T23:59:59.5Z` would come back `0` where Spark says `-1`, and
//! `1969-12-31T23:59:58.75Z` would come back `-1` where Spark says `-2`. The sign only shows up
//! before 1970, which is exactly where nobody looks. [`seconds_floor_from_ticks`] and its
//! `epoch_seconds_floor_is_floor_not_truncation` pin hold that edge.
//!
//! The value is **zone-independent** on both engines (probed under `America/New_York`,
//! `Asia/Tokyo` and `UTC`): the cast reads the instant, never a wall clock, so nothing here
//! touches `spark.sql.session.timeZone`. A session-zone-sensitive epoch would be the bug the
//! session-timezone unit fixed, in reverse.
//!
//! # Why two UDFs and not one
//!
//! An integer target needs exact integer floor division, and a float/decimal target needs the
//! fractional remainder. One `Decimal128`-returning UDF cannot serve both: arrow's decimal →
//! integer cast truncates toward zero, which loses the floor edge again; and one `Float64`
//! -returning UDF cannot serve the integer case either, because f64 has ~2·10⁻⁷ s of resolution
//! at present-day epochs, so a sub-microsecond instant can floor to the wrong second. So
//! [`spark_epoch_seconds_floor_udf`] carries the integer path (exact `i64` arithmetic) and
//! [`spark_epoch_seconds_real_udf`] carries the real path (Spark itself computes its decimal cast
//! through a double, so the double hop is the oracle's own mechanism, not an approximation of it).
//!
//! Both are **embedded** by [`crate::analyzer`] into rewritten `CAST` expressions and never
//! registered, so they are not user-callable and the rewrite (which matches on the *source* type
//! being a timestamp) cannot re-fire on its own output — the injected child is `Int64` / `Float64`
//! by then. Idempotency matters: the analyzer runs once eagerly on the passthrough plan and again
//! at physical planning.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Float64Array, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// ===========================================================================================
/// The embedded integer-target UDF (`__repark_epoch_seconds_floor__`): `Timestamp` → `Int64`
/// epoch seconds, floored.
///
/// Embedded by [`crate::analyzer`] under every `CAST(ts AS <signed integer>)`; the analyzer keeps
/// an outer `CAST` to the *user's* width, so narrowing (and whatever DataFusion does when the
/// seconds value does not fit) stays the ordinary integer-cast path this module does not own.
/// ===========================================================================================
#[must_use]
pub fn spark_epoch_seconds_floor_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochSecondsFloor::new()))
}

/// ===========================================================================================
/// The embedded real-target UDF (`__repark_epoch_seconds_real__`): `Timestamp` → `Float64`
/// epoch seconds, fraction kept.
///
/// Embedded by [`crate::analyzer`] under every `CAST(ts AS DOUBLE / FLOAT / DECIMAL(p,s))`, with
/// an outer `CAST` to the user's target so DataFusion applies the requested width and scale.
/// ===========================================================================================
#[must_use]
pub fn spark_epoch_seconds_real_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochSecondsReal::new()))
}

/// ===========================================================================================
/// Ticks per second for an arrow [`TimeUnit`] — the divisor both UDFs scale by.
///
/// Total by construction (arrow has exactly four units) and always ≥ 1, which is what makes the
/// `div_euclid` in [`seconds_floor_from_ticks`] unable to divide by zero.
/// ===========================================================================================
const fn ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// ===========================================================================================
/// Spark's `Math.floorDiv(ticks, ticks_per_second)` — epoch seconds, rounded toward −∞.
///
/// `i64::div_euclid` is floor division for a POSITIVE divisor, and [`ticks_per_second`] is
/// positive by construction, so this cannot panic (the two `div_euclid` panics are a zero divisor
/// and `i64::MIN / -1`, neither reachable here). Truncating division would answer `0` for
/// `-0.5 s`; Spark answers `-1`.
/// ===========================================================================================
const fn seconds_floor_from_ticks(ticks: i64, per_second: i64) -> i64 {
    ticks.div_euclid(per_second)
}

/// The timestamp `TimeUnit` of the single argument, or a planning error naming what arrived.
fn argument_time_unit(name: &str, data_type: &DataType) -> Result<TimeUnit> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(DataFusionError::Plan(format!(
            "'{name}' expects a TIMESTAMP argument, got {other}"
        ))),
    }
}

/// The raw tick values of a timestamp array as `Int64`, with the argument's null mask intact.
///
/// The `cast` is exact for every timestamp unit (arrow reinterprets the backing `i64` buffer) and
/// is the defensive step the SAF-002 discipline asks for before `as_primitive`.
fn timestamp_ticks(array: &dyn Array) -> Result<Int64Array> {
    let ticks = cast(array, &DataType::Int64)?;
    Ok(ticks.as_primitive::<Int64Type>().clone())
}

/// ===========================================================================================
/// `SparkEpochSecondsFloor` — the integer path: floored epoch seconds as `Int64`.
/// ===========================================================================================
#[derive(Debug)]
struct SparkEpochSecondsFloor {
    signature: Signature,
}

impl SparkEpochSecondsFloor {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkEpochSecondsFloor {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkEpochSecondsFloor {}

impl Hash for SparkEpochSecondsFloor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkEpochSecondsFloor {
    crate::shim_udf_boilerplate!("__repark_epoch_seconds_floor__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Prefer `return_field_from_args` (nullability follows the argument); this is the
        // fallback DataFusion calls when it has types but no fields.
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        // The cast of a non-nullable timestamp is non-nullable in Spark too, and the recorded
        // corpus asserts Arrow nullability, so the argument's flag must ride through rather than
        // the `true` a default `return_type` would produce.
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let unit = argument_time_unit(self.name(), arrays[0].data_type())?;
        let per_second = ticks_per_second(unit);
        let seconds: Int64Array = timestamp_ticks(arrays[0].as_ref())?
            .iter()
            .map(|ticks| ticks.map(|value| seconds_floor_from_ticks(value, per_second)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(seconds)))
    }
}

/// ===========================================================================================
/// `SparkEpochSecondsReal` — the float / decimal path: epoch seconds with the fraction kept.
/// ===========================================================================================
#[derive(Debug)]
struct SparkEpochSecondsReal {
    signature: Signature,
}

impl SparkEpochSecondsReal {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkEpochSecondsReal {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkEpochSecondsReal {}

impl Hash for SparkEpochSecondsReal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkEpochSecondsReal {
    crate::shim_udf_boilerplate!("__repark_epoch_seconds_real__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let unit = argument_time_unit(self.name(), arrays[0].data_type())?;
        #[expect(
            clippy::cast_precision_loss,
            reason = "Spark computes its own TIMESTAMP -> DOUBLE/DECIMAL cast through a double \
                      (`t / MICROS_PER_SECOND.toDouble`), so the f64 hop IS the oracle's \
                      mechanism; the integer path that needs exactness uses \
                      `SparkEpochSecondsFloor` instead"
        )]
        let per_second = ticks_per_second(unit) as f64;
        #[expect(
            clippy::cast_precision_loss,
            reason = "same double hop as the divisor above — see `SparkEpochSecondsFloor` for \
                      the exact-integer path"
        )]
        let seconds: Float64Array = timestamp_ticks(arrays[0].as_ref())?
            .iter()
            .map(|ticks| ticks.map(|value| value as f64 / per_second))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(seconds)))
    }
}

/// True when the sole argument is nullable (or absent, which the signature makes unreachable).
fn nullable_like_argument(args: &ReturnFieldArgs<'_>) -> bool {
    args.arg_fields
        .first()
        .is_none_or(|field| field.is_nullable())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The floor edge, stated in ticks so the arithmetic is checkable by eye. Live Spark 4.1.2
    /// (ledger §2): `-0.5 s → -1`, `-1.25 s → -2`, `+0.75 s → 0`, `1.999999 s → 1`.
    ///
    /// This is the pin that a "simplify it to an arrow `Timestamp(Second)` hop" rewrite reddens:
    /// truncation toward zero answers `0` and `-1` for the first two rows.
    #[test]
    fn epoch_seconds_floor_is_floor_not_truncation() {
        const NANOS: i64 = 1_000_000_000;
        assert_eq!(seconds_floor_from_ticks(-500_000_000, NANOS), -1);
        assert_eq!(seconds_floor_from_ticks(-1_250_000_000, NANOS), -2);
        assert_eq!(seconds_floor_from_ticks(-1_800_000_000_000, NANOS), -1800);
        assert_eq!(seconds_floor_from_ticks(750_000_000, NANOS), 0);
        assert_eq!(seconds_floor_from_ticks(1_999_999_000, NANOS), 1);
        assert_eq!(seconds_floor_from_ticks(0, NANOS), 0);
        // A whole negative second is the case truncation gets RIGHT, so it cannot be the only
        // negative row in the file — it is here to prove the fix did not overshoot into
        // "always subtract one".
        assert_eq!(seconds_floor_from_ticks(-1_000_000_000, NANOS), -1);
    }

    /// Every arrow timestamp unit scales by its own divisor — a µs-backed column (what Spark's
    /// own Arrow export uses) must not be read as if it were nanoseconds.
    #[test]
    fn every_time_unit_scales_by_its_own_divisor() {
        assert_eq!(ticks_per_second(TimeUnit::Second), 1);
        assert_eq!(ticks_per_second(TimeUnit::Millisecond), 1_000);
        assert_eq!(ticks_per_second(TimeUnit::Microsecond), 1_000_000);
        assert_eq!(ticks_per_second(TimeUnit::Nanosecond), 1_000_000_000);
        // -1800 s expressed in each unit floors back to -1800 s.
        for (unit, ticks) in [
            (TimeUnit::Second, -1_800_i64),
            (TimeUnit::Millisecond, -1_800_000),
            (TimeUnit::Microsecond, -1_800_000_000),
            (TimeUnit::Nanosecond, -1_800_000_000_000),
        ] {
            assert_eq!(
                seconds_floor_from_ticks(ticks, ticks_per_second(unit)),
                -1800,
                "{unit:?}"
            );
        }
        // A half-second before the epoch floors to -1 in EVERY unit that can express it.
        for (unit, ticks) in [
            (TimeUnit::Millisecond, -500_i64),
            (TimeUnit::Microsecond, -500_000),
            (TimeUnit::Nanosecond, -500_000_000),
        ] {
            assert_eq!(
                seconds_floor_from_ticks(ticks, ticks_per_second(unit)),
                -1,
                "{unit:?}"
            );
        }
    }

    /// The extreme `i64` tick values a `Timestamp(Nanosecond, _)` column can hold do not panic
    /// and stay floored — the no-panic rule applied to the one arithmetic op in this module.
    #[test]
    fn extreme_tick_values_floor_without_panicking() {
        const NANOS: i64 = 1_000_000_000;
        assert_eq!(
            seconds_floor_from_ticks(i64::MAX, NANOS),
            i64::MAX / NANOS,
            "the positive extreme truncates and floors alike"
        );
        // i64::MIN is NOT a multiple of 10^9, so floor is one BELOW truncation here.
        assert_eq!(
            seconds_floor_from_ticks(i64::MIN, NANOS),
            i64::MIN / NANOS - 1
        );
        // `Second`-backed ticks divide by one: the identity case, including at i64::MIN, which is
        // the value that would panic under a `-1` divisor.
        assert_eq!(seconds_floor_from_ticks(i64::MIN, 1), i64::MIN);
    }

    /// A non-timestamp argument is a LOUD planning error, never a silent reinterpretation — the
    /// UDF is embedded by the analyzer only under a timestamp source, so reaching this arm means
    /// something else called it.
    #[test]
    fn a_non_timestamp_argument_is_a_planning_error() {
        let error = argument_time_unit("__repark_epoch_seconds_floor__", &DataType::Int64)
            .expect_err("Int64 is not a timestamp");
        assert!(
            error.to_string().contains("expects a TIMESTAMP argument"),
            "got {error}"
        );
        assert!(
            argument_time_unit("x", &DataType::Timestamp(TimeUnit::Nanosecond, None)).is_ok(),
            "a timestamp resolves"
        );
    }
}
