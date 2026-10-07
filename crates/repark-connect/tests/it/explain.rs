use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::SessionContext;

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
        let plan = context.sql(&sql).await.expect(&sql);
        plan.create_physical_plan().await.expect(&sql);
        let seen = seen.lock().expect("seen").clone();
        let expected: Vec<String> = filters.iter().map(ToString::to_string).collect();
        assert_eq!(seen, [(expected, limit)], "{filter}");
    }
}
