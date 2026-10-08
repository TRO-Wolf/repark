use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};

use super::{ScanPlan, WallClockLocaliser, place_until_refusal};
use crate::discover::{ResolvedSource, ScanColumn, ScanSource};
use crate::error::{ConnectError, ValueRefusal};
use crate::ident::{PgIdent, QualifiedRelation};
use crate::pool::{PoolLimits, PostgresConnector, QueryPool};
use crate::read::postgres::{ScanOptions, ScanRequest};
use crate::settings::{PostgresSettings, SettingsDoor};
use crate::types::postgres::{PgTypeKind, TypeMod};

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const NEW_YORK_GAP_START: i64 = 1_772_935_200_000_000;
const NEW_YORK_GAP_END: i64 = NEW_YORK_GAP_START + MICROS_PER_HOUR;
const ZONE: &str = "America/New_York";

#[derive(Debug)]
struct NewYorkSpringForward;

impl WallClockLocaliser for NewYorkSpringForward {
    fn localise(
        &self,
        wall: &TimestampMicrosecondArray,
    ) -> crate::error::Result<TimestampMicrosecondArray> {
        let mut placed = Vec::with_capacity(wall.len());
        for (index, micros) in wall.values().iter().copied().enumerate() {
            if (NEW_YORK_GAP_START..NEW_YORK_GAP_END).contains(&micros) {
                return Err(ConnectError::UnrepresentableValue {
                    column: "".into(),
                    postgres_type: "timestamp",
                    index,
                    reason: ValueRefusal::WallClockGap,
                });
            }
            let offset_hours = if micros < NEW_YORK_GAP_START { 5 } else { 4 };
            placed.push(micros + offset_hours * MICROS_PER_HOUR);
        }
        Ok(TimestampMicrosecondArray::from(placed))
    }

    fn zone_label(&self) -> Arc<str> {
        ZONE.into()
    }
}

fn plan() -> ScanPlan {
    let props: BTreeMap<String, String> = [("host", "127.0.0.1"), ("user", "app")]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let settings =
        PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect("settings");
    let connector = PostgresConnector::new(&settings).expect("a connector opens no connection");
    let pool = QueryPool::new(connector, PoolLimits::from_settings(&settings));
    let column = ScanColumn::resolve(
        PgIdent::new("wall").expect("identifier"),
        "timestamp",
        PgTypeKind::Base,
        TypeMod::new(-1),
        true,
    )
    .expect("a mapped column");
    let relation = QualifiedRelation::new(
        PgIdent::new("public").expect("schema"),
        PgIdent::new("events").expect("table"),
    );
    let resolved = Arc::new(ResolvedSource {
        source: ScanSource::Relation(relation.clone()),
        columns: vec![column],
        server_version_num: 160_000,
        server_encoding: "UTF8".into(),
    });
    let schema = Arc::new(Schema::new(vec![Field::new(
        "wall",
        DataType::Timestamp(TimeUnit::Microsecond, Some(ZONE.into())),
        true,
    )]));
    ScanPlan {
        source: "company_db".into(),
        target: ScanSource::Relation(relation),
        schema,
        pushed: Vec::new(),
        residual: Vec::new(),
        limit: None,
        request: ScanRequest::new(resolved),
        options: ScanOptions::from_settings(&settings),
        pool,
        placed: vec![(0, ZONE.into())],
        localiser: Arc::new(NewYorkSpringForward),
        partition: None,
    }
}

fn wall_batch(walls: Vec<i64>) -> RecordBatch {
    let column: ArrayRef = Arc::new(TimestampMicrosecondArray::from(walls));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "wall",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    RecordBatch::try_new(schema, vec![column]).expect("a one-column batch")
}

#[test]
fn a_placement_refusal_emits_the_rows_before_it_once_then_refuses_once() {
    let plan = plan();
    let before = [
        NEW_YORK_GAP_START - 2 * MICROS_PER_HOUR,
        NEW_YORK_GAP_START - MICROS_PER_HOUR / 2,
    ];
    let walls = vec![
        before[0],
        before[1],
        NEW_YORK_GAP_START + MICROS_PER_HOUR / 2,
        NEW_YORK_GAP_END + MICROS_PER_HOUR / 2,
    ];
    let mut emitted = place_until_refusal(&plan, Ok(wall_batch(walls))).into_iter();
    let kept = emitted
        .next()
        .expect("the rows before the gap are emitted first")
        .expect("and they are placed");
    assert_eq!(kept.num_rows(), 2);
    let placed = kept.column(0).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(
        placed.values().to_vec(),
        before.map(|micros| micros + 5 * MICROS_PER_HOUR).to_vec()
    );
    assert_eq!(placed.timezone(), Some(ZONE));
    let refusal = emitted
        .next()
        .expect("the refusal follows the rows before it")
        .expect_err("the gap row refuses");
    assert_eq!(
        refusal,
        ConnectError::UnrepresentableValue {
            column: "wall".into(),
            postgres_type: "timestamp",
            index: 2,
            reason: ValueRefusal::WallClockGap,
        }
    );
    assert!(emitted.next().is_none(), "the stream ends at the refusal");
}

#[test]
fn a_placement_refusal_on_the_first_row_has_nothing_to_emit() {
    let plan = plan();
    let walls = vec![NEW_YORK_GAP_START + MICROS_PER_HOUR / 2, NEW_YORK_GAP_END];
    let emitted = place_until_refusal(&plan, Ok(wall_batch(walls)));
    assert_eq!(emitted.len(), 1);
    assert!(matches!(
        emitted[0],
        Err(ConnectError::UnrepresentableValue { index: 0, .. })
    ));
}

#[test]
fn rows_without_a_gap_are_emitted_whole() {
    let plan = plan();
    let walls = vec![NEW_YORK_GAP_START - MICROS_PER_HOUR, NEW_YORK_GAP_END];
    let emitted = place_until_refusal(&plan, Ok(wall_batch(walls)));
    assert_eq!(emitted.len(), 1);
    let batch = emitted[0].as_ref().expect("every row places");
    assert_eq!(batch.num_rows(), 2);
}
