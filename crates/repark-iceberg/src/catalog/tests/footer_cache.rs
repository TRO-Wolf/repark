use super::super::*;

use std::collections::HashMap;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

const FILES: i64 = 3;

const ROWS_PER_FILE: i64 = 60_000;

const FULL: &str = "SELECT id FROM r.sales.orders";

const FILTERED: &str = "SELECT id FROM r.sales.orders WHERE id >= 100500 AND id < 100600";

struct Bed {
    _dir: TempDir,
    warehouse: String,
    current: String,
}

fn schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

fn sales() -> NamespaceIdent {
    NamespaceIdent::new("sales".to_string())
}

fn orders() -> TableIdent {
    TableIdent::from_strs(["sales", "orders"]).unwrap()
}

async fn bed() -> Bed {
    let dir = TempDir::new().unwrap();
    let warehouse = dir.path().to_str().unwrap().to_string();
    let writer = memory_catalog_cached(&warehouse, &CatalogCaches::disabled())
        .await
        .unwrap();
    writer
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{warehouse}/sales/orders"))
        .schema(schema())
        .properties(HashMap::new())
        .build();
    writer.create_table(&sales(), creation).await.unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "w", Arc::clone(&writer))
        .await
        .unwrap();
    for file in 0..FILES {
        let first = file * ROWS_PER_FILE;
        let last = first + ROWS_PER_FILE - 1;
        ctx.sql(&format!(
            "INSERT INTO w.sales.orders SELECT value FROM generate_series({first}, {last})"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    }
    let table = writer.load_table(&orders()).await.unwrap();
    let current = table.metadata_location().unwrap().to_string();
    Bed {
        _dir: dir,
        warehouse,
        current,
    }
}

async fn reader(bed: &Bed, caches: &CatalogCaches) -> SessionContext {
    let catalog = memory_catalog_cached(&bed.warehouse, caches).await.unwrap();
    catalog
        .create_namespace(&sales(), HashMap::new())
        .await
        .unwrap();
    catalog
        .register_table(&orders(), bed.current.clone())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "r", catalog).await.unwrap();
    ctx
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Answer {
    rows: usize,
    sum: i64,
}

async fn scan(ctx: &SessionContext, caches: &CatalogCaches, sql: &str) -> (Answer, IcebergIoStats) {
    caches.reset_io_stats();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let sum = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..ids.len()).map(move |i| ids.value(i))
        })
        .sum();
    (Answer { rows, sum }, caches.io_stats())
}

fn footers(stats: &IcebergIoStats) -> u64 {
    stats
        .get(IcebergIoOp::FooterRead, IcebergFileClass::DataFile)
        .requests
}

fn pages(stats: &IcebergIoStats) -> IcebergIoCount {
    stats.get(IcebergIoOp::RangedRead, IcebergFileClass::DataFile)
}

fn footer_off() -> CatalogCaches {
    CatalogCaches::new(IcebergCacheSettings {
        footer_cache_bytes: 0,
        ..IcebergCacheSettings::default()
    })
}

fn full_answer() -> Answer {
    let rows = FILES * ROWS_PER_FILE;
    Answer {
        rows: usize::try_from(rows).unwrap(),
        sum: rows * (rows - 1) / 2,
    }
}

#[tokio::test]
async fn a_warm_rescan_reads_no_data_file_footer_with_the_cache_on() {
    let bed = bed().await;
    let on = CatalogCaches::default();
    let ctx = reader(&bed, &on).await;
    let (cold_answer, cold) = scan(&ctx, &on, FULL).await;
    let (warm_answer, warm) = scan(&ctx, &on, FULL).await;
    assert_eq!(cold_answer, full_answer());
    assert_eq!(warm_answer, full_answer());
    assert!(footers(&cold) > 0, "{cold:?}");
    assert_eq!(footers(&warm), 0, "{warm:?}");
    let stats = on.footer_stats().unwrap();
    assert!(stats.hits > 0, "{stats:?}");
    assert_eq!(stats.fetches, footers(&cold), "{stats:?}");

    let off = footer_off();
    let ctx = reader(&bed, &off).await;
    let (_, cold_off) = scan(&ctx, &off, FULL).await;
    let (warm_off_answer, warm_off) = scan(&ctx, &off, FULL).await;
    assert_eq!(warm_off_answer, full_answer());
    assert!(
        footers(&cold) <= footers(&cold_off),
        "{cold:?} {cold_off:?}"
    );
    assert_eq!(footers(&warm_off), footers(&cold_off));
    assert_eq!(off.footer_stats(), None);
}

#[tokio::test]
async fn the_counter_still_counts_every_page_read_with_the_footer_cache_on() {
    let bed = bed().await;
    let on = CatalogCaches::default();
    let ctx = reader(&bed, &on).await;
    scan(&ctx, &on, FULL).await;
    let (_, warm_on) = scan(&ctx, &on, FULL).await;
    let off = footer_off();
    let ctx = reader(&bed, &off).await;
    scan(&ctx, &off, FULL).await;
    let (_, warm_off) = scan(&ctx, &off, FULL).await;
    assert!(pages(&warm_on).requests > 0, "{warm_on:?}");
    assert_eq!(pages(&warm_on), pages(&warm_off));
}

#[tokio::test]
async fn a_filtered_scan_after_an_unfiltered_one_upgrades_the_footer_and_answers_right() {
    let bed = bed().await;
    let expected = Answer {
        rows: 100,
        sum: (100_500..100_600).sum(),
    };
    let off = footer_off();
    let ctx = reader(&bed, &off).await;
    let (answer_off, _) = scan(&ctx, &off, FILTERED).await;
    assert_eq!(answer_off, expected);

    let on = CatalogCaches::default();
    let ctx = reader(&bed, &on).await;
    assert_eq!(scan(&ctx, &on, FULL).await.0, full_answer());
    let before = on.footer_stats().unwrap();
    let (answer, filtered) = scan(&ctx, &on, FILTERED).await;
    assert_eq!(answer, expected);
    let after = on.footer_stats().unwrap();
    assert!(after.hits > before.hits, "{before:?} {after:?}");
    assert!(after.upgrades > before.upgrades, "{before:?} {after:?}");
    assert_eq!(after.fetches, before.fetches, "{before:?} {after:?}");
    assert_eq!(footers(&filtered), 0, "{filtered:?}");
    let (again, _) = scan(&ctx, &on, FILTERED).await;
    assert_eq!(again, expected);
    assert_eq!(on.footer_stats().unwrap().upgrades, after.upgrades);
}
