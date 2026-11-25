use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, Decimal128Array, Int64Array, RecordBatch,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, Schema};
use datafusion::common::{Result, exec_err};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use super::guard_narrow_decimal_plan;
use crate::ReparkSession;

#[derive(Debug, PartialEq, Eq, Hash)]
struct RaisingNarrow {
    signature: Signature,
}

impl ScalarUDFImpl for RaisingNarrow {
    fn name(&self) -> &'static str {
        "__repark_narrow_to_decimal_5_0__"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(5, 0))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(5, 0),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("one argument");
        };
        let array = value.to_array(args.number_rows)?;
        if let DataType::Decimal128(_, _) = array.data_type() {
            return Ok(ColumnarValue::Array(cast(
                &array,
                &DataType::Decimal128(5, 0),
            )?));
        }
        let wide = cast(&array, &DataType::Int64)?;
        let values = wide.as_primitive::<Int64Type>();
        if values
            .iter()
            .flatten()
            .any(|value| value.unsigned_abs() > 99_999)
        {
            return exec_err!("NARROW OVERFLOW");
        }
        let out: Decimal128Array = values.unary(i128::from);
        Ok(ColumnarValue::Array(Arc::new(
            out.with_precision_and_scale(5, 0)?,
        )))
    }
}

fn session() -> ReparkSession {
    let session = ReparkSession::builder().build().expect("core session");
    let context = session.context();
    context.register_udf(ScalarUDF::from(RaisingNarrow {
        signature: Signature::any(1, Volatility::Immutable),
    }));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("bi", DataType::Int64, true),
        Field::new("nb", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(123_456),
                None,
                Some(2),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                Some(false),
            ])),
        ],
    )
    .expect("batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("table");
    context
        .register_table("t", Arc::new(table))
        .expect("register");
    session
}

async fn outcome(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| {
                    datafusion::arrow::util::display::array_value_to_string(column, row)
                        .unwrap_or_default()
                })
                .collect();
            rows.push(cells.join(","));
        }
    }
    rows.sort();
    Ok(rows)
}

const N: &str = "__repark_narrow_to_decimal_5_0__";

#[tokio::test]
async fn a_cast_is_evaluated_only_where_spark_evaluates_it() {
    let session = session();
    let answers: [(String, &[&str]); 9] = [
        (
            format!("SELECT id FROM t WHERE id IN (1, 3) AND {N}(bi) = 1"),
            &["1"],
        ),
        (
            format!("SELECT id FROM t WHERE id > 0 AND id <> 2 AND {N}(bi) = 1"),
            &["1"],
        ),
        (format!("SELECT id FROM t WHERE nb AND {N}(bi) = 1"), &["1"]),
        (
            format!("SELECT id FROM t WHERE id NOT IN (1, 3, 4) OR {N}(bi) = 1"),
            &["1", "2"],
        ),
        (
            format!("SELECT id FROM t WHERE id IN (1, 3) AND ({N}(bi) = 1 OR id = 3)"),
            &["1", "3"],
        ),
        (
            format!("SELECT id FROM t WHERE id IN (1, 3, 4) AND {N}(bi) IN (1, 2)"),
            &["1", "4"],
        ),
        (
            format!("SELECT id, id IN (1, 3) AND {N}(bi) = 1 FROM t"),
            &["1,true", "2,false", "3,", "4,false"],
        ),
        (
            format!("SELECT count(*) FILTER (WHERE id <> 2 AND {N}(bi) = 1) FROM t"),
            &["1"],
        ),
        (
            format!("SELECT id FROM t WHERE id <> 2 AND {N}(CAST(bi AS DECIMAL(10,0))) = 1"),
            &["1"],
        ),
    ];
    for (sql, expected) in answers {
        assert_eq!(
            outcome(&session, &sql).await,
            Ok(expected.iter().map(ToString::to_string).collect()),
            "{sql}"
        );
    }
    let raises = [
        format!("SELECT id FROM t WHERE {N}(bi) = 1 AND id IN (1, 3)"),
        format!("SELECT id FROM t WHERE random() >= 0 AND {N}(bi) = 1"),
        format!("SELECT id, nb AND {N}(bi) = 1 FROM t"),
        format!("SELECT id, nb OR {N}(bi) = 1 FROM t"),
        format!("SELECT id FROM t WHERE {N}(bi) = 1"),
        format!("SELECT {N}(bi) FROM t"),
        format!(
            "SELECT * FROM (VALUES (CAST(NULL AS BOOLEAN), CAST(123456 AS BIGINT))) v(nb, bi) \
             WHERE nb AND {N}(bi) = 1"
        ),
    ];
    for sql in raises {
        let error = outcome(&session, &sql).await.expect_err(&sql);
        assert!(error.contains("NARROW OVERFLOW"), "{sql}: {error}");
    }
}

#[tokio::test]
async fn plans_without_an_integer_narrowing_call_are_left_borrowed() {
    let session = session();
    for sql in [
        "SELECT id FROM t WHERE id IN (1, 3) AND bi = 1",
        "SELECT id FROM t WHERE id IN (1, 3) AND CAST(bi AS DECIMAL(5,0)) = 1",
        "SELECT id FROM t WHERE id IN (1, 3) AND __repark_narrow_to_decimal_5_0__(CAST(bi AS DECIMAL(10,0))) = 1",
        "SELECT __repark_narrow_to_decimal_5_0__(bi) FROM t",
    ] {
        let plan = session
            .sql(sql)
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimized");
        let guarded = guard_narrow_decimal_plan(&plan).expect("guard");
        assert!(matches!(guarded, Cow::Borrowed(_)), "{sql}");
    }
    let plan = session
        .sql(&format!(
            "SELECT id FROM t WHERE id IN (1, 3) AND {N}(bi) = 1"
        ))
        .await
        .expect("plan")
        .into_optimized_plan()
        .expect("optimized");
    let guarded = guard_narrow_decimal_plan(&plan).expect("guard");
    let shown = guarded.display_indent().to_string();
    assert!(matches!(guarded, Cow::Owned(_)), "{shown}");
    assert!(shown.contains("CASE WHEN t.id"), "{shown}");
    assert!(shown.contains(&format!("THEN {N}(t.bi) END")), "{shown}");
    assert_eq!(guarded.schema(), plan.schema());
    let plan = session
        .sql(&format!(
            "SELECT id FROM t WHERE random() >= 0 AND id <> 2 AND {N}(bi) = 1"
        ))
        .await
        .expect("plan")
        .into_optimized_plan()
        .expect("optimized");
    let shown = guard_narrow_decimal_plan(&plan)
        .expect("guard")
        .display_indent()
        .to_string();
    assert!(shown.contains("CASE WHEN t.id != Int64(2) THEN"), "{shown}");
    assert!(!shown.contains("CASE WHEN random()"), "{shown}");
}
