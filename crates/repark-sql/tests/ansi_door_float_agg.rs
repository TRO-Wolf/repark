//! G8 / R-3 — ANSI-door `f64::to_bits` float-aggregation twins of G7.
//!
//! Mold: `crates/repark-spark/src/tests/float_agg.rs`. Same catastrophic-cancellation
//! fixture, same `target_partitions` ∈ {1, 2, 8}, same bit goldens. This file is the
//! **Native-profile** half: `AnsiDialect`, no `SessionExtension`,
//! `ReparkSessionBuilder::target_partitions`. Do not rewrite the Spark-door tests.
//!
//! Input `MemTable` partitions match the engine count so partial aggregation fans
//! out. Cross-count equality does **not** hold (p=1/2 → 3.75; p=8 → 2.25).
//! Calibration: `docs/testing.md` "calibration-sensitive" (`f64::to_bits`).

use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;

// =================================================================================================
// Catastrophic-cancellation fixture (exact bit patterns fixed in the G7 / X-3 ledger)
// =================================================================================================

/// Large ± magnitudes that cancel, interleaved with small values lost under some
/// accumulation orders. Exact `f64::to_bits` of each element is re-asserted by
/// [`ansi_door_pin_fixture_element_bit_patterns`].
const FIXTURE: [f64; 8] = [
    1.0e16,  // bits 0x4341c37937e08000
    1.0,     // bits 0x3ff0000000000000
    -1.0e16, // bits 0xc341c37937e08000
    2.0,     // bits 0x4000000000000000
    1.0e16,  // bits 0x4341c37937e08000
    0.5,     // bits 0x3fe0000000000000
    -1.0e16, // bits 0xc341c37937e08000
    0.25,    // bits 0x3fd0000000000000
];

/// Measured goldens (native ANSI Arrow path, 2026-08-14). Same bits as the G7
/// Spark-door pins on this fixture — stock DataFusion aggregation, no Spark
/// analyzer. p=1 and p=2 land the accurate compensated sum; p=8 loses small
/// addends under final merge.
const SUM_BITS_P1: u64 = 0x400e_0000_0000_0000; // 3.75
const SUM_BITS_P2: u64 = 0x400e_0000_0000_0000; // 3.75
const SUM_BITS_P8: u64 = 0x4002_0000_0000_0000; // 2.25
const AVG_BITS_P1: u64 = 0x3fde_0000_0000_0000; // 0.46875 = 3.75/8
const AVG_BITS_P2: u64 = 0x3fde_0000_0000_0000; // 0.46875
const AVG_BITS_P8: u64 = 0x3fd2_0000_0000_0000; // 0.28125 = 2.25/8

// =================================================================================================
// Setup — input partitions + engine target_partitions locked together
// =================================================================================================

fn native_ansi_with_float_fixture(target_partitions: usize) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .target_partitions(target_partitions)
        .build()
        .expect("native session");
    register_float_fixture(&session, "float_src", &FIXTURE, target_partitions);
    session
}

/// Register `values` as a one-column `v DOUBLE` [`MemTable`] with `input_partitions`
/// outer partitions (round-robin row assignment).
fn register_float_fixture(
    session: &ReparkSession,
    name: &str,
    values: &[f64],
    input_partitions: usize,
) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
    let partition_count = input_partitions.max(1);
    let mut buckets: Vec<Vec<f64>> = (0..partition_count).map(|_| Vec::new()).collect();
    for (index, value) in values.iter().enumerate() {
        buckets[index % partition_count].push(*value);
    }
    let partitions: Vec<Vec<RecordBatch>> = buckets
        .into_iter()
        .map(|bucket| {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(bucket))])
                    .expect("float fixture batch");
            vec![batch]
        })
        .collect();
    let table = MemTable::try_new(schema, partitions).expect("MemTable");
    session
        .context()
        .register_table(name, Arc::new(table))
        .expect("register float_src");
}

async fn collect_float64_bits(session: &ReparkSession, sql: &str) -> (bool, u64) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute failed for `{sql}`: {error}"));
    let schema = frame.schema();
    let field = schema.field(0);
    let nullable = field.is_nullable();
    assert_eq!(
        field.data_type(),
        &DataType::Float64,
        "expected Float64 for `{sql}`"
    );
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect failed for `{sql}`: {error}"));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "`{sql}` must yield exactly one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("column 0 of `{sql}` is not Float64Array"));
    assert!(
        !array.is_null(0),
        "`{sql}` cell must be non-null for bits pin"
    );
    (nullable, array.value(0).to_bits())
}

async fn sum_bits_at(target_partitions: usize) -> (bool, u64) {
    let session = native_ansi_with_float_fixture(target_partitions);
    collect_float64_bits(&session, "SELECT sum(v) AS s FROM float_src").await
}

async fn avg_bits_at(target_partitions: usize) -> (bool, u64) {
    let session = native_ansi_with_float_fixture(target_partitions);
    collect_float64_bits(&session, "SELECT avg(v) AS a FROM float_src").await
}

// =================================================================================================
// Fixture SSOT — a silent edit of FIXTURE reds immediately
// =================================================================================================

/// Element bit patterns the G7 ledger cites. Not a sum/avg pin; guards the fixture itself.
#[test]
fn ansi_door_pin_fixture_element_bit_patterns() {
    let expected: [u64; 8] = [
        0x4341_c379_37e0_8000, // 1e16
        0x3ff0_0000_0000_0000, // 1.0
        0xc341_c379_37e0_8000, // -1e16
        0x4000_0000_0000_0000, // 2.0
        0x4341_c379_37e0_8000, // 1e16
        0x3fe0_0000_0000_0000, // 0.5
        0xc341_c379_37e0_8000, // -1e16
        0x3fd0_0000_0000_0000, // 0.25
    ];
    for (index, (value, bits)) in FIXTURE.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            value.to_bits(),
            *bits,
            "fixture[{index}] bit pattern drifted: value={value}"
        );
    }
}

// =================================================================================================
// sum(f64) absolute pins — one per partition count
// =================================================================================================

/// `sum(v)` at `target_partitions=1` — bits of 3.75. Matrix cite for
/// `SEMANTICS_FLOAT_DETERMINISM` on the ANSI door.
#[tokio::test]
async fn ansi_door_sum_f64_bits_at_target_partitions_1() {
    let (nullable, bits) = sum_bits_at(1).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P1, "sum p=1 f64::to_bits (3.75)");
}

/// `sum(v)` at `target_partitions=2` — bits of 3.75 (same as p=1 for this fixture).
#[tokio::test]
async fn ansi_door_sum_f64_bits_at_target_partitions_2() {
    let (nullable, bits) = sum_bits_at(2).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P2, "sum p=2 f64::to_bits (3.75)");
}

/// `sum(v)` at `target_partitions=8` — bits of 2.25 (cross-count spread vs p=1/2).
#[tokio::test]
async fn ansi_door_sum_f64_bits_at_target_partitions_8() {
    let (nullable, bits) = sum_bits_at(8).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P8, "sum p=8 f64::to_bits (2.25)");
}

// =================================================================================================
// avg(f64) absolute pins
// =================================================================================================

/// `avg(v)` at `target_partitions=1` — bits of 0.46875.
#[tokio::test]
async fn ansi_door_avg_f64_bits_at_target_partitions_1() {
    let (nullable, bits) = avg_bits_at(1).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P1, "avg p=1 f64::to_bits (0.46875)");
}

/// `avg(v)` at `target_partitions=2` — bits of 0.46875.
#[tokio::test]
async fn ansi_door_avg_f64_bits_at_target_partitions_2() {
    let (nullable, bits) = avg_bits_at(2).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P2, "avg p=2 f64::to_bits (0.46875)");
}

/// `avg(v)` at `target_partitions=8` — bits of 0.28125 (cross-count spread vs p=1/2).
#[tokio::test]
async fn ansi_door_avg_f64_bits_at_target_partitions_8() {
    let (nullable, bits) = avg_bits_at(8).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P8, "avg p=8 f64::to_bits (0.28125)");
}

// =================================================================================================
// Run-to-run stability (determinism claim) + explicit cross-count spread disclosure
// =================================================================================================

/// Same input + same config → same bits, twice, at each of the three partition counts.
#[tokio::test]
async fn ansi_door_sum_f64_run_to_run_stable_at_three_partition_counts() {
    for parts in [1_usize, 2, 8] {
        let (nullable_a, bits_a) = sum_bits_at(parts).await;
        let (nullable_b, bits_b) = sum_bits_at(parts).await;
        assert_eq!(nullable_a, nullable_b, "sum nullability drift at p={parts}");
        assert_eq!(bits_a, bits_b, "sum run-to-run bit drift at p={parts}");
    }
}

/// Same input + same config → same bits, twice, at each of the three partition counts.
#[tokio::test]
async fn ansi_door_avg_f64_run_to_run_stable_at_three_partition_counts() {
    for parts in [1_usize, 2, 8] {
        let (nullable_a, bits_a) = avg_bits_at(parts).await;
        let (nullable_b, bits_b) = avg_bits_at(parts).await;
        assert_eq!(nullable_a, nullable_b, "avg nullability drift at p={parts}");
        assert_eq!(bits_a, bits_b, "avg run-to-run bit drift at p={parts}");
    }
}

/// Cross-count spread is REAL for this fixture: p=1/2 share bits; p=8 differs.
#[tokio::test]
async fn ansi_door_sum_f64_cross_count_spread_p8_differs_from_p1() {
    let (_nullable_1, bits_1) = sum_bits_at(1).await;
    let (_nullable_2, bits_2) = sum_bits_at(2).await;
    let (_nullable_8, bits_8) = sum_bits_at(8).await;
    assert_eq!(
        bits_1, bits_2,
        "p=1 and p=2 currently agree for this fixture"
    );
    assert_eq!(bits_1, SUM_BITS_P1);
    assert_eq!(bits_8, SUM_BITS_P8);
    assert_ne!(
        bits_1, bits_8,
        "cross-count spread must remain observable (p1=3.75 vs p8=2.25); if they converge, \
         flip this disclosure to a cross-count equality pin"
    );
}
