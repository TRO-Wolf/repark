use std::sync::{Arc, Mutex};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::SessionContext;
use repark_common::{SourceIdentity, SourceKind};
use repark_connect::{LISTING_ROW, PostgresScanExec, PostgresSource, SettingsDoor};

use crate::pushdown::{FixedZone, context, physical};

const STATEMENT: &str = "SELECT id, note FROM orders WHERE amount > 100 AND lower(note) = 'x'";

async fn explain(context: &SessionContext, statement: &str) -> String {
    let batches: Vec<RecordBatch> = context
        .sql(statement)
        .await
        .expect(statement)
        .collect()
        .await
        .expect(statement);
    batches
        .iter()
        .flat_map(|batch| {
            let plans = batch.column(1).as_string::<i32>();
            plans
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn scan_line(text: &str) -> &str {
    text.lines()
        .map(str::trim)
        .find(|line| line.starts_with("PostgresScanExec:"))
        .expect("a PostgresScanExec line")
}

#[tokio::test]
async fn explain_renders_pushed_and_residual_per_scan() {
    let context = context(&[], "UTF8");
    let text = explain(&context, &format!("EXPLAIN {STATEMENT}")).await;
    assert_eq!(
        scan_line(&text),
        "PostgresScanExec: source=company_db, relation=\"public\".\"orders\", projection=[id, \
         note], pushed_filters=[amount > Decimal128(Some(10000),10,2)], \
         residual_filters=[lower(note) = Utf8(\"x\")], pushed_limit=None"
    );
    let limited = explain(
        &context,
        "EXPLAIN SELECT id FROM orders WHERE qty = 1 LIMIT 3",
    )
    .await;
    assert_eq!(
        scan_line(&limited),
        "PostgresScanExec: source=company_db, relation=\"public\".\"orders\", projection=[id], \
         pushed_filters=[qty = Int32(1)], residual_filters=[], pushed_limit=3"
    );
}

#[tokio::test]
async fn explain_verbose_shows_placeholders_never_values() {
    let context = context(&[], "UTF8");
    let statement = "SELECT id FROM orders WHERE note = 'paid-7f3a9c' AND lower(code) = 'x'";
    let text = explain(&context, &format!("EXPLAIN VERBOSE {statement}")).await;
    let line = scan_line(&text);
    assert!(
        line.contains(
            ", remote_sql=COPY (SELECT \"id\"::pg_catalog.int8, \"code\"::pg_catalog.varchar \
             FROM \"public\".\"orders\" WHERE (\"note\" COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) \
             pg_catalog.current_setting('repark.p0')::pg_catalog.text)) TO STDOUT (FORMAT \
             BINARY), bound_values=1"
        ),
        "{line}"
    );
    let remote = line.split(", remote_sql=").nth(1).expect("remote_sql");
    assert!(!remote.contains("paid-7f3a9c"), "{remote}");
    let plain = explain(&context, &format!("EXPLAIN {statement}")).await;
    assert!(!scan_line(&plain).contains("remote_sql"), "{plain}");
}

#[tokio::test]
async fn explain_never_renders_endpoint() {
    let context = context(&[], "UTF8");
    for format in ["EXPLAIN", "EXPLAIN VERBOSE", "EXPLAIN FORMAT TREE"] {
        let text = explain(&context, &format!("{format} {STATEMENT}")).await;
        assert!(text.contains("company_db"), "{format}: {text}");
        for secret in [
            "db.internal.example",
            "6543",
            "warehouse",
            "reader",
            "S3cretPw",
            "host=",
            "sslmode",
        ] {
            assert!(!text.contains(secret), "{format} renders {secret}: {text}");
        }
    }
    let debug = format!("{:?}", crate::pushdown::source(&[]));
    assert!(
        !debug.contains("S3cretPw") && !debug.contains("db.internal"),
        "{debug}"
    );
}

fn filter_above<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    parent: Option<&'a FilterExec>,
) -> Option<(&'a FilterExec, &'a PostgresScanExec)> {
    if let Some(scan) = plan.downcast_ref::<PostgresScanExec>() {
        return parent.map(|filter| (filter, scan));
    }
    let parent = plan.downcast_ref::<FilterExec>().or(parent);
    plan.children()
        .into_iter()
        .find_map(|child| filter_above(child, parent))
}

#[tokio::test]
async fn explain_residual_matches_filter_exec_above() {
    let context = context(&[], "UTF8");
    for statement in [
        STATEMENT,
        "SELECT id FROM orders WHERE score > 1 AND qty = 2 AND padded = 'a'",
        "SELECT id FROM orders WHERE qty = 1 OR lower(note) = 'x'",
    ] {
        let plan = physical(&context, statement).await;
        let (filter, scan) = filter_above(&plan, None).expect("a FilterExec above the scan");
        let residual = conjunction(scan.residual_filters().to_vec()).expect("a residual");
        let schema = DFSchema::try_from(scan.schema().as_ref().clone()).expect("schema");
        let expected = create_physical_expr(&residual, &schema, &ExecutionProps::new())
            .expect("physical residual");
        assert_eq!(
            filter.predicate().to_string(),
            expected.to_string(),
            "{statement}"
        );
    }
    let exact = physical(&context, "SELECT id FROM orders WHERE qty = 1").await;
    assert!(
        filter_above(&exact, None).is_none(),
        "an exact filter leaves no FilterExec"
    );
}

type Seen = Arc<Mutex<Vec<(Vec<String>, Option<usize>)>>>;

#[derive(Debug)]
struct Recorder {
    schema: SchemaRef,
    seen: Seen,
}

#[async_trait]
impl TableProvider for Recorder {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| match filter.column_refs().iter().next() {
                Some(column) if column.name == "a" => TableProviderFilterPushDown::Exact,
                _ => TableProviderFilterPushDown::Inexact,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let shown = filters.iter().map(ToString::to_string).collect();
        self.seen.lock().expect("seen").push((shown, limit));
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

#[tokio::test]
async fn datafusion_hands_inexact_filters_to_scan_and_withholds_limit() {
    let cases = [
        (
            "a > 1 AND b = 'x'",
            vec!["a > Int32(1)", "b = Utf8(\"x\")"],
            None,
        ),
        ("b = 'x'", vec!["b = Utf8(\"x\")"], None),
        ("a > 1", vec!["a > Int32(1)"], Some(5)),
    ];
    for (filter, filters, limit) in cases {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let recorder = Recorder {
            schema,
            seen: Arc::clone(&seen),
        };
        let context = SessionContext::new();
        context
            .register_table("t", Arc::new(recorder))
            .expect("register");
        let sql = format!("SELECT a FROM t WHERE {filter} LIMIT 5");
        physical(&context, &sql).await;
        let seen = seen.lock().expect("seen").clone();
        let expected: Vec<String> = filters.iter().map(ToString::to_string).collect();
        assert_eq!(seen, [(expected, limit)], "{filter}");
    }
}

#[tokio::test]
async fn listing_a_postgres_source_is_empty_and_declared() {
    let identity = SourceIdentity::unassigned("company_db".to_string(), SourceKind::Postgres);
    let zone = Arc::new(FixedZone {
        offset_micros: 0,
        label: "UTC",
    });
    let catalog = PostgresSource::mount(&identity, std::collections::BTreeMap::new(), zone);
    assert!(catalog.schema_names().is_empty());
    let schema = catalog
        .schema("any_schema")
        .expect("a lazy schema for any name");
    assert!(schema.table_names().is_empty());
    assert!(!schema.table_exist("orders"));
    assert_eq!(LISTING_ROW, "CONNECT-DECL-pg-listing");
    let refused = schema
        .table("orders")
        .await
        .expect_err("no url or host refuses");
    assert!(
        refused.to_string().contains("invalid specification"),
        "{refused}"
    );
    let source = PostgresSource::new(
        &identity,
        std::collections::BTreeMap::new(),
        SettingsDoor::ReadPostgres,
        Arc::new(FixedZone {
            offset_micros: 0,
            label: "UTC",
        }),
    );
    assert_eq!(source.name().as_ref(), "company_db");
}
