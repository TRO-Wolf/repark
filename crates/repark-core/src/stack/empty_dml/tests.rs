use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray};
use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result as DataFusionResult, TableReference};
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    DmlStatement, EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, TableType, WriteOp, lit,
};
use datafusion::physical_plan::ExecutionPlan;

use super::plan_empty_dml_as_no_op;
use crate::ReparkSession;

const EXPLAIN_ANALYZE_REFUSAL: &str = "EXPLAIN ANALYZE is refused: its statement writes to a table";

const PREPARE_REFUSAL: &str = "PREPARE `p` is refused: its statement writes to a table";

const EMPTY_PREDICATES: [&str; 8] = [
    "false",
    "NULL",
    "1 = 0",
    "id IS NULL",
    "prim_col = NULL",
    "st = CAST(NULL AS STRUCT<x INT>)",
    "arr[1] = 1 AND 1 = 0",
    "id = 1 AND false",
];

const PROVIDER_COUNT: u64 = 7;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Calls {
    deletes: Vec<Vec<String>>,
    updates: Vec<(Vec<String>, Vec<String>)>,
    truncates: usize,
    inserts: usize,
}

#[derive(Debug)]
struct RecordingTable {
    rows: MemTable,
    calls: Arc<Mutex<Calls>>,
}

impl RecordingTable {
    fn record(&self, change: impl FnOnce(&mut Calls)) {
        let mut calls = self.calls.lock().expect("calls");
        change(&mut calls);
    }
}

fn struct_fields() -> Fields {
    Fields::from(vec![Field::new("x", DataType::Int32, true)])
}

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("prim_col", DataType::Int32, true),
        Field::new("flag", DataType::Utf8, true),
        Field::new("st", DataType::Struct(struct_fields()), true),
        Field::new_list("arr", Field::new_list_field(DataType::Int32, true), true),
    ]))
}

fn seeded_batch() -> RecordBatch {
    let st: ArrayRef = Arc::new(StructArray::new(
        struct_fields(),
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef],
        Some(vec![true, true, false].into()),
    ));
    let arr: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2)]),
        None,
    ]));
    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![Some(10), Some(20), None])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            st,
            arr,
        ],
    )
    .expect("seeded batch")
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

async fn provider_count(state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let batch = RecordBatch::try_new(
        count_schema(),
        vec![Arc::new(UInt64Array::from(vec![PROVIDER_COUNT]))],
    )?;
    MemTable::try_new(count_schema(), vec![vec![batch]])?
        .scan(state, None, &[], None)
        .await
}

fn rendered(exprs: &[Expr]) -> Vec<String> {
    exprs.iter().map(ToString::to_string).collect()
}

#[async_trait]
impl TableProvider for RecordingTable {
    fn schema(&self) -> SchemaRef {
        self.rows.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.rows.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.record(|calls| calls.inserts += 1);
        self.rows.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.record(|calls| calls.deletes.push(rendered(&filters)));
        provider_count(state).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let assigned = assignments
            .iter()
            .map(|(name, value)| format!("{name} := {value}"))
            .collect();
        self.record(|calls| calls.updates.push((assigned, rendered(&filters))));
        provider_count(state).await
    }

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.record(|calls| calls.truncates += 1);
        provider_count(state).await
    }
}

fn recording_table() -> (Arc<RecordingTable>, Arc<Mutex<Calls>>) {
    let calls = Arc::new(Mutex::new(Calls::default()));
    let rows = MemTable::try_new(table_schema(), vec![vec![seeded_batch()]]).expect("rows");
    let table = Arc::new(RecordingTable {
        rows,
        calls: Arc::clone(&calls),
    });
    (table, calls)
}

fn recorded(calls: &Arc<Mutex<Calls>>) -> Calls {
    calls.lock().expect("calls").clone()
}

fn session_with_recording_table() -> (ReparkSession, Arc<Mutex<Calls>>) {
    let session = ReparkSession::builder().build().expect("session");
    let (table, calls) = recording_table();
    session
        .context()
        .register_table("rec", table)
        .expect("register rec");
    (session, calls)
}

async fn answered(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn refused(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map_or_else(|error| error.to_string(), |_| panic!("{sql} must refuse")),
        Err(error) => error.to_string(),
    }
}

fn counts(batches: &[RecordBatch]) -> Vec<u64> {
    assert!(!batches.is_empty(), "a DML answers one count batch");
    let mut values = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema(), count_schema(), "the DML result schema");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count is UInt64");
        values.extend(column.values().iter().copied());
    }
    values
}

async fn seeded_ids(session: &ReparkSession) -> Vec<i64> {
    let batches = answered(session, "SELECT id FROM rec ORDER BY id").await;
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64")
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn an_empty_delete_or_update_never_reaches_the_provider_and_answers_a_zero_count() {
    let (session, calls) = session_with_recording_table();
    for predicate in EMPTY_PREDICATES {
        for sql in [
            format!("DELETE FROM rec WHERE {predicate}"),
            format!("UPDATE rec SET flag = 'z' WHERE {predicate}"),
        ] {
            let batches = answered(&session, &sql).await;
            assert_eq!(counts(&batches), vec![0], "{sql}");
            assert_eq!(recorded(&calls), Calls::default(), "{sql}");
        }
    }
    assert_eq!(seeded_ids(&session).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_selective_unrestricted_or_identity_dml_still_reaches_the_provider() {
    let (session, calls) = session_with_recording_table();
    for sql in [
        "DELETE FROM rec WHERE id = 1",
        "DELETE FROM rec WHERE true",
        "DELETE FROM rec",
        "UPDATE rec SET flag = 'z' WHERE id = 1",
        "UPDATE rec SET flag = 'z' WHERE true",
        "UPDATE rec SET flag = 'z'",
        "UPDATE rec SET flag = flag WHERE id = 1",
    ] {
        assert_eq!(
            counts(&answered(&session, sql).await),
            vec![PROVIDER_COUNT],
            "{sql}"
        );
    }
    let id_one = vec!["id = Int64(1)".to_string()];
    assert_eq!(
        recorded(&calls),
        Calls {
            deletes: vec![id_one.clone(), Vec::new(), Vec::new()],
            updates: vec![
                (vec!["flag := Utf8(\"z\")".to_string()], id_one.clone()),
                (vec!["flag := Utf8(\"z\")".to_string()], Vec::new()),
                (vec!["flag := Utf8(\"z\")".to_string()], Vec::new()),
                (Vec::new(), id_one),
            ],
            truncates: 0,
            inserts: 0,
        }
    );
}

#[tokio::test]
async fn truncate_and_an_empty_insert_source_are_left_to_the_provider() {
    let (session, calls) = session_with_recording_table();
    assert_eq!(
        counts(&answered(&session, "TRUNCATE TABLE rec").await),
        vec![PROVIDER_COUNT]
    );
    answered(&session, "INSERT INTO rec SELECT * FROM rec WHERE false").await;
    assert_eq!(
        recorded(&calls),
        Calls {
            truncates: 1,
            inserts: 1,
            ..Calls::default()
        }
    );
    assert_eq!(seeded_ids(&session).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn an_empty_dml_nested_in_a_wider_plan_is_guarded_too() {
    let (session, calls) = session_with_recording_table();
    for sql in [
        "DELETE FROM rec WHERE false",
        "UPDATE rec SET flag = 'z' WHERE 1 = 0",
    ] {
        let frame = session.sql(sql).await.expect(sql);
        assert_eq!(frame.count().await.expect(sql), 1, "{sql}");
        let plan = answered(&session, &format!("EXPLAIN {sql}")).await;
        assert!(!plan.is_empty(), "EXPLAIN {sql}");
    }
    assert_eq!(recorded(&calls), Calls::default());
}

#[tokio::test]
async fn a_refused_wrapper_of_an_empty_dml_stays_refused() {
    let (session, calls) = session_with_recording_table();
    for (sql, refusal) in [
        (
            "EXPLAIN ANALYZE DELETE FROM rec WHERE false",
            EXPLAIN_ANALYZE_REFUSAL,
        ),
        (
            "EXPLAIN ANALYZE UPDATE rec SET flag = 'z' WHERE 1 = 0",
            EXPLAIN_ANALYZE_REFUSAL,
        ),
        ("PREPARE p AS DELETE FROM rec WHERE false", PREPARE_REFUSAL),
        (
            "PREPARE p AS UPDATE rec SET flag = 'z' WHERE NULL",
            PREPARE_REFUSAL,
        ),
    ] {
        let error = refused(&session, sql).await;
        assert!(error.contains(refusal), "{sql}: {error}");
    }
    let missing = refused(&session, "EXECUTE p").await;
    assert!(
        missing.contains("Prepared statement 'p' does not exist"),
        "{missing}"
    );
    assert_eq!(recorded(&calls), Calls::default());
}

async fn optimized(session: &ReparkSession, sql: &str) -> LogicalPlan {
    let state = session.context().state();
    let plan = state.create_logical_plan(sql).await.expect(sql);
    state.optimize(&plan).expect(sql)
}

fn assert_zero_count(guarded: Cow<'_, LogicalPlan>, original: &LogicalPlan) {
    let Cow::Owned(LogicalPlan::Values(values)) = guarded else {
        panic!("an empty DML plans as a zero-count Values node: {guarded:?}")
    };
    assert_eq!(values.schema, original.schema().clone());
    assert_eq!(values.values, vec![vec![lit(0_u64)]]);
}

#[tokio::test]
async fn the_rewrite_replaces_only_a_delete_or_update_over_an_empty_relation() {
    let (session, _calls) = session_with_recording_table();
    for sql in [
        "DELETE FROM rec WHERE false",
        "UPDATE rec SET flag = 'z' WHERE NULL",
    ] {
        let plan = optimized(&session, sql).await;
        let LogicalPlan::Dml(dml) = &plan else {
            panic!("{sql} optimizes to a Dml root: {plan:?}")
        };
        assert!(
            matches!(dml.input.as_ref(), LogicalPlan::EmptyRelation(_)),
            "{sql}"
        );
        assert_zero_count(plan_empty_dml_as_no_op(&plan).expect(sql), &plan);
    }
    for sql in [
        "DELETE FROM rec WHERE id = 1",
        "DELETE FROM rec",
        "UPDATE rec SET flag = flag WHERE id = 1",
        "TRUNCATE TABLE rec",
        "INSERT INTO rec SELECT * FROM rec WHERE false",
        "SELECT * FROM rec WHERE false",
    ] {
        let plan = optimized(&session, sql).await;
        let guarded = plan_empty_dml_as_no_op(&plan).expect(sql);
        assert!(
            matches!(guarded, Cow::Borrowed(_)),
            "{sql} is left as planned"
        );
    }
}

fn hand_built_dml(op: &WriteOp, input: LogicalPlan) -> LogicalPlan {
    let (table, _calls) = recording_table();
    LogicalPlan::Dml(DmlStatement::new(
        TableReference::bare("rec"),
        provider_as_source(table),
        op.clone(),
        Arc::new(input),
    ))
}

fn empty_relation(produce_one_row: bool) -> LogicalPlan {
    let schema = DFSchema::try_from(table_schema().as_ref().clone()).expect("schema");
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row,
        schema: Arc::new(schema),
    })
}

fn projected(input: LogicalPlan) -> LogicalPlan {
    LogicalPlanBuilder::from(input)
        .project(vec![datafusion::prelude::col("id"), lit("z").alias("flag")])
        .expect("project")
        .build()
        .expect("projection")
}

#[test]
fn the_rewrite_sees_through_projections_but_not_a_one_row_relation() {
    for op in [WriteOp::Delete, WriteOp::Update] {
        for input in [
            empty_relation(false),
            projected(empty_relation(false)),
            projected(projected(empty_relation(false))),
        ] {
            let plan = hand_built_dml(&op, input);
            assert_zero_count(plan_empty_dml_as_no_op(&plan).expect("guard"), &plan);
        }
        for input in [empty_relation(true), projected(empty_relation(true))] {
            let plan = hand_built_dml(&op, input);
            let guarded = plan_empty_dml_as_no_op(&plan).expect("guard");
            assert!(
                matches!(guarded, Cow::Borrowed(_)),
                "{op}: one row is not empty"
            );
        }
    }
    for op in [WriteOp::Truncate, WriteOp::Insert(InsertOp::Append)] {
        let plan = hand_built_dml(&op, empty_relation(false));
        let guarded = plan_empty_dml_as_no_op(&plan).expect("guard");
        assert!(
            matches!(guarded, Cow::Borrowed(_)),
            "{op} is not a matches-nothing DML"
        );
    }
}

#[test]
fn the_rewrite_reaches_a_dml_below_another_node() {
    let dml = hand_built_dml(&WriteOp::Delete, empty_relation(false));
    let wrapped = LogicalPlanBuilder::from(dml)
        .limit(0, Some(1))
        .expect("limit")
        .build()
        .expect("wrapped");
    let guarded = plan_empty_dml_as_no_op(&wrapped).expect("guard");
    let Cow::Owned(LogicalPlan::Limit(limit)) = guarded else {
        panic!("the wrapper stays and its input is rewritten: {guarded:?}")
    };
    assert!(matches!(limit.input.as_ref(), LogicalPlan::Values(_)));
    assert_eq!(limit.input.schema().field(0).name(), "count");
    assert_eq!(limit.input.schema().fields().len(), 1);
    assert!(limit.input.schema().field(0).data_type() == &DataType::UInt64);
    assert!(!limit.input.schema().field(0).is_nullable());
}
