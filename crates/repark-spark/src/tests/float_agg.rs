//! G7 — float aggregation determinism pins (`f64::to_bits`) for the Spark SQL door.
//!
//! Fixture: a catastrophic-cancellation vector whose sum accuracy depends on accumulation
//! order. Pins `sum(f64)` / `avg(f64)` at three `target_partitions` counts (1, 2, 8).
//! Claim under test: same input + same config → same bits (run-to-run). Cross-count equality
//! does **not** hold for this fixture (p=1/2 → 3.75; p=8 → 2.25) — each count is pinned
//! independently and the ledger discloses the spread. Never fudge a bit pattern.
//!
//! Engine knob: DataFusion `target_partitions` via `SessionConfig::with_target_partitions`
//! (builder `target_partitions` / conf `datafusion.execution.target_partitions`). Input
//! `MemTable` partitions match the count so partial aggregation genuinely fans out.
//!
//! Out of scope: fixing aggregation order; the registry file; Python corpus (separate file).

use super::super::*;
use super::common::*;

use datafusion::arrow::array::Float64Array;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionConfig;

// =================================================================================================
// Catastrophic-cancellation fixture (exact bit patterns fixed in the unit ledger)
// =================================================================================================

/// Large ± magnitudes that cancel, interleaved with small values lost under some accumulation
/// orders. Exact `f64::to_bits` of each element is re-asserted by
/// [`pin_fixture_element_bit_patterns`].
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

/// Measured goldens (Spark-door Arrow path, 2026-08-11). p=1 and p=2 land the accurate
/// compensated sum; p=8 (one row per input partition) loses small addends under final merge.
const SUM_BITS_P1: u64 = 0x400e_0000_0000_0000; // 3.75
const SUM_BITS_P2: u64 = 0x400e_0000_0000_0000; // 3.75
const SUM_BITS_P8: u64 = 0x4002_0000_0000_0000; // 2.25
const AVG_BITS_P1: u64 = 0x3fde_0000_0000_0000; // 0.46875 = 3.75/8
const AVG_BITS_P2: u64 = 0x3fde_0000_0000_0000; // 0.46875
const AVG_BITS_P8: u64 = 0x3fd2_0000_0000_0000; // 0.28125 = 2.25/8

// =================================================================================================
// Setup — input partitions + engine target_partitions locked together
// =================================================================================================

async fn setup_with_target_partitions(
    warehouse_dir: &TempDir,
    target_partitions: usize,
) -> (SessionContext, CatalogRegistry) {
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap(),
    );
    let ns_props = HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]);
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), ns_props)
        .await
        .unwrap();

    let settings = repark_functions::cardinality::ReparkSqlSettings::default();
    let config = repark_functions::cardinality::with_repark_sql_config(
        SessionConfig::new().with_target_partitions(target_partitions),
        settings,
    );
    let ctx = SessionContext::new_with_config(config);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();

    // Match input MemTable partitions to target_partitions so the aggregate's partial stage
    // fans out for real — a single-partition MemTable can keep partial aggregation sequential
    // even when the config advertises more target_partitions.
    register_float_fixture(&ctx, "float_src", &FIXTURE, target_partitions);

    let mut catalogs = CatalogRegistry::from([("ice".to_string(), catalog)]);
    catalogs.note_local_warehouse_root(warehouse);
    (ctx, catalogs)
}

/// Register `values` as a one-column `v DOUBLE` [`MemTable`] with `input_partitions` outer
/// partitions (round-robin row assignment).
fn register_float_fixture(
    ctx: &SessionContext,
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
                    .unwrap();
            vec![batch]
        })
        .collect();
    let table = MemTable::try_new(schema, partitions).unwrap();
    ctx.register_table(name, Arc::new(table)).unwrap();
}

// =================================================================================================
// Collect helpers — one-column Float64 on the Arrow path (value bits AND type AND nullability)
// =================================================================================================

async fn collect_float64_bits(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (bool, u64) {
    let frame = execute(ctx, catalogs, sql)
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
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_target_partitions(&warehouse, target_partitions).await;
    collect_float64_bits(&ctx, &catalogs, "SELECT sum(v) AS s FROM float_src").await
}

async fn avg_bits_at(target_partitions: usize) -> (bool, u64) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_target_partitions(&warehouse, target_partitions).await;
    collect_float64_bits(&ctx, &catalogs, "SELECT avg(v) AS a FROM float_src").await
}

// =================================================================================================
// Fixture SSOT — a silent edit of FIXTURE reds immediately
// =================================================================================================

/// Element bit patterns the ledger cites. Not a sum/avg pin; guards the fixture itself.
#[test]
fn pin_fixture_element_bit_patterns() {
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
// sum(f64) absolute pins — one per partition count (6 of the 6–8 f64::to_bits budget with avg)
// =================================================================================================

/// `sum(v)` at `target_partitions=1` — bits of 3.75.
#[tokio::test]
async fn pin_sum_f64_bits_at_target_partitions_1() {
    let (nullable, bits) = sum_bits_at(1).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P1, "sum p=1 f64::to_bits (3.75)");
}

/// `sum(v)` at `target_partitions=2` — bits of 3.75 (same as p=1 for this fixture).
#[tokio::test]
async fn pin_sum_f64_bits_at_target_partitions_2() {
    let (nullable, bits) = sum_bits_at(2).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P2, "sum p=2 f64::to_bits (3.75)");
}

/// `sum(v)` at `target_partitions=8` — bits of 2.25 (cross-count spread vs p=1/2).
#[tokio::test]
async fn pin_sum_f64_bits_at_target_partitions_8() {
    let (nullable, bits) = sum_bits_at(8).await;
    assert!(nullable, "sum is nullable");
    assert_eq!(bits, SUM_BITS_P8, "sum p=8 f64::to_bits (2.25)");
}

// =================================================================================================
// avg(f64) absolute pins
// =================================================================================================

/// `avg(v)` at `target_partitions=1` — bits of 0.46875.
#[tokio::test]
async fn pin_avg_f64_bits_at_target_partitions_1() {
    let (nullable, bits) = avg_bits_at(1).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P1, "avg p=1 f64::to_bits (0.46875)");
}

/// `avg(v)` at `target_partitions=2` — bits of 0.46875.
#[tokio::test]
async fn pin_avg_f64_bits_at_target_partitions_2() {
    let (nullable, bits) = avg_bits_at(2).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P2, "avg p=2 f64::to_bits (0.46875)");
}

/// `avg(v)` at `target_partitions=8` — bits of 0.28125 (cross-count spread vs p=1/2).
#[tokio::test]
async fn pin_avg_f64_bits_at_target_partitions_8() {
    let (nullable, bits) = avg_bits_at(8).await;
    assert!(nullable, "avg is nullable");
    assert_eq!(bits, AVG_BITS_P8, "avg p=8 f64::to_bits (0.28125)");
}

// =================================================================================================
// Run-to-run stability (determinism claim) + explicit cross-count spread disclosure
// =================================================================================================

/// Same input + same config → same bits, twice, at each of the three partition counts.
#[tokio::test]
async fn pin_sum_f64_run_to_run_stable_at_three_partition_counts() {
    for parts in [1_usize, 2, 8] {
        let (nullable_a, bits_a) = sum_bits_at(parts).await;
        let (nullable_b, bits_b) = sum_bits_at(parts).await;
        assert_eq!(nullable_a, nullable_b, "sum nullability drift at p={parts}");
        assert_eq!(bits_a, bits_b, "sum run-to-run bit drift at p={parts}");
    }
}

/// Same input + same config → same bits, twice, at each of the three partition counts.
#[tokio::test]
async fn pin_avg_f64_run_to_run_stable_at_three_partition_counts() {
    for parts in [1_usize, 2, 8] {
        let (nullable_a, bits_a) = avg_bits_at(parts).await;
        let (nullable_b, bits_b) = avg_bits_at(parts).await;
        assert_eq!(nullable_a, nullable_b, "avg nullability drift at p={parts}");
        assert_eq!(bits_a, bits_b, "avg run-to-run bit drift at p={parts}");
    }
}

/// Cross-count spread is REAL for this fixture: p=1/2 share bits; p=8 differs. Pin that the
/// spread still holds so a future "make all counts equal" fix flips this disclosure red
/// (honest outcome — never pin cross-count equality that does not hold).
#[tokio::test]
async fn pin_sum_f64_cross_count_spread_p8_differs_from_p1() {
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
