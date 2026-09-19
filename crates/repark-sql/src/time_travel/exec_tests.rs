use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct Fixture {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Fixture {
    async fn open() -> Self {
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let warehouse = warehouse_dir
            .path()
            .to_str()
            .expect("utf8 warehouse")
            .to_string();
        let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
            .await
            .expect("memory catalog");
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
            .await
            .expect("register catalog");
        let mut catalogs = CatalogRegistry::new();
        catalogs.insert(
            "ice".to_string(),
            Arc::clone(&catalog),
            LocationPolicy::TempFallbackAllowed {
                root: warehouse_dir.path().to_path_buf(),
            },
        );
        catalogs.note_local_warehouse_root(&warehouse);
        let fixture = Self {
            ctx,
            catalogs,
            catalog,
            warehouse,
            _warehouse_dir: warehouse_dir,
        };
        let location = format!("{}/sales", fixture.warehouse);
        fixture
            .ok(&format!(
                "CREATE SCHEMA ice.sales WITH (location = '{location}')"
            ))
            .await;
        fixture
    }

    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
        frame
            .collect()
            .await
            .unwrap_or_else(|error| panic!("`{sql}` must collect: {error}"))
    }

    async fn ids(&self, sql: &str) -> Vec<i64> {
        let mut ids = Vec::new();
        for batch in self.ok(sql).await {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 id");
            for index in 0..column.len() {
                ids.push(column.value(index));
            }
        }
        ids
    }

    async fn snapshot(&self, table: &str) -> (i64, i64) {
        let loaded = self
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .expect("table must load");
        let id = loaded
            .metadata()
            .current_snapshot_id()
            .expect("current snapshot");
        let timestamp_ms = loaded
            .metadata()
            .snapshot_by_id(id)
            .expect("snapshot by id")
            .timestamp_ms();
        (id, timestamp_ms)
    }
}

struct Seed {
    fixture: Fixture,
    table: String,
    s1: i64,
    mid_sec: i64,
    wall: String,
}

impl Seed {
    async fn two_snapshots(table: &str) -> Self {
        let fixture = Fixture::open().await;
        fixture
            .ok(&format!(
                "CREATE TABLE ice.sales.{table} AS SELECT 1 AS id, 'a' AS label \
                 UNION ALL SELECT 2 AS id, 'b' AS label"
            ))
            .await;
        let (s1, s1_ms) = fixture.snapshot(table).await;
        std::thread::sleep(Duration::from_millis(1_200));
        fixture
            .ok(&format!("INSERT INTO ice.sales.{table} VALUES (3, 'c')"))
            .await;
        let mid_sec = s1_ms.div_euclid(1_000) + 1;
        let wall = civil_wall(mid_sec);
        Self {
            fixture,
            table: table.to_string(),
            s1,
            mid_sec,
            wall,
        }
    }
}

fn civil_wall(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let clock = seconds.rem_euclid(86_400);
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = if month_part < 10 {
        month_part + 3
    } else {
        month_part - 9
    };
    let full_year = year + i64::from(month <= 2);
    format!(
        "{full_year:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        month,
        day,
        clock / 3_600,
        clock / 60 % 60,
        clock % 60
    )
}

#[test]
fn civil_wall_renders_a_known_utc_midnight() {
    assert_eq!(civil_wall(1_577_836_800), "2020-01-01 00:00:00");
}

#[tokio::test]
async fn timestamp_as_of_string_wall_pins_the_first_snapshot() {
    let seed = Seed::two_snapshots("tt_exec_str").await;
    assert_eq!(
        seed.fixture
            .ids(&format!(
                "SELECT id FROM ice.sales.{} FOR TIMESTAMP AS OF '{}' ORDER BY id",
                seed.table, seed.wall
            ))
            .await,
        vec![1, 2]
    );
    assert_eq!(
        seed.fixture
            .ids(&format!(
                "SELECT id FROM ice.sales.{} ORDER BY id",
                seed.table
            ))
            .await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn timestamp_as_of_integer_seconds_pins_the_first_snapshot() {
    let seed = Seed::two_snapshots("tt_exec_int").await;
    assert_eq!(
        seed.fixture
            .ids(&format!(
                "SELECT id FROM ice.sales.{} FOR TIMESTAMP AS OF {} ORDER BY id",
                seed.table, seed.mid_sec
            ))
            .await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn timestamp_as_of_cast_expression_pins_the_first_snapshot() {
    let seed = Seed::two_snapshots("tt_exec_cast").await;
    assert_eq!(
        seed.fixture
            .ids(&format!(
                "SELECT id FROM ice.sales.{} FOR TIMESTAMP AS OF CAST('{}' AS TIMESTAMP) ORDER BY id",
                seed.table, seed.wall
            ))
            .await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn version_as_of_pins_the_first_snapshot() {
    let seed = Seed::two_snapshots("tt_exec_ver").await;
    assert_eq!(
        seed.fixture
            .ids(&format!(
                "SELECT id FROM ice.sales.{} FOR VERSION AS OF {} ORDER BY id",
                seed.table, seed.s1
            ))
            .await,
        vec![1, 2]
    );
}
