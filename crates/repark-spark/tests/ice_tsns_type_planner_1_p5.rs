use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::WindowUDFFieldArgs;
use datafusion::logical_expr::{AggregateUDF, ReturnFieldArgs, ScalarUDF, WindowUDF};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap_or_else(|error| panic!("build spark session: {error}"))
}

fn probe_inputs() -> Vec<(String, DataType)> {
    vec![
        (
            String::from("ts_us_tz"),
            DataType::Timestamp(TimeUnit::Microsecond, Some(String::from("UTC").into())),
        ),
        (
            String::from("ts_us"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (String::from("utf8"), DataType::Utf8),
        (String::from("int64"), DataType::Int64),
        (String::from("date32"), DataType::Date32),
        (String::from("null"), DataType::Null),
    ]
}

fn contains_ns(dtype: &DataType) -> bool {
    match dtype {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => true,
        DataType::List(field) => contains_ns(field.data_type()),
        DataType::LargeList(field) => contains_ns(field.data_type()),
        DataType::FixedSizeList(field, _) => contains_ns(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_ns(field.data_type())),
        DataType::Dictionary(_, values) => contains_ns(values),
        DataType::Map(field, _) => contains_ns(field.data_type()),
        _ => false,
    }
}

fn scalar_ns(udf: &ScalarUDF, input: &DataType) -> Option<DataType> {
    let arg_types = [input.clone()];
    match udf.return_type(&arg_types) {
        Ok(dtype) => {
            if contains_ns(&dtype) {
                Some(dtype)
            } else {
                None
            }
        }
        Err(_) => {
            let field: FieldRef = Arc::new(Field::new("arg", input.clone(), true));
            let fields = [field];
            let scalars: Vec<Option<&ScalarValue>> = vec![None];
            let args = ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            };
            match udf.return_field_from_args(args) {
                Ok(field) => {
                    let dtype = field.data_type().clone();
                    if contains_ns(&dtype) {
                        Some(dtype)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        }
    }
}

fn aggregate_ns(udaf: &AggregateUDF, input: &DataType) -> Option<DataType> {
    let arg_types = [input.clone()];
    match udaf.return_type(&arg_types) {
        Ok(dtype) => {
            if contains_ns(&dtype) {
                Some(dtype)
            } else {
                None
            }
        }
        Err(_) => {
            let field: FieldRef = Arc::new(Field::new("arg", input.clone(), true));
            let fields = [field];
            match udaf.return_field(&fields) {
                Ok(field) => {
                    let dtype = field.data_type().clone();
                    if contains_ns(&dtype) {
                        Some(dtype)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        }
    }
}

fn guarded_scalar(udf: &ScalarUDF, input: &DataType) -> Option<DataType> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scalar_ns(udf, input))) {
        Ok(found) => found,
        Err(_) => None,
    }
}

fn guarded_aggregate(udaf: &AggregateUDF, input: &DataType) -> Option<DataType> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| aggregate_ns(udaf, input))) {
        Ok(found) => found,
        Err(_) => None,
    }
}

fn guarded_window(udwf: &WindowUDF, name: &str, input: &DataType) -> Option<DataType> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        window_ns(udwf, name, input)
    })) {
        Ok(found) => found,
        Err(_) => None,
    }
}

fn window_ns(udwf: &WindowUDF, name: &str, input: &DataType) -> Option<DataType> {
    let field: FieldRef = Arc::new(Field::new("arg", input.clone(), true));
    let fields = [field];
    let args = WindowUDFFieldArgs::new(&fields, name);
    match udwf.field(args) {
        Ok(field) => {
            let dtype = field.data_type().clone();
            if contains_ns(&dtype) {
                Some(dtype)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

#[test]
fn p5_ns_returns_walk_runs() {
    let session = spark_session();
    let state = session.context().state();
    let inputs = probe_inputs();
    let mut hits: Vec<String> = Vec::new();
    let mut scalar_names: Vec<String> = state.scalar_functions().keys().cloned().collect();
    scalar_names.sort();
    for name in &scalar_names {
        let Some(udf) = state.scalar_functions().get(name) else {
            continue;
        };
        for (label, input) in &inputs {
            if let Some(dtype) = guarded_scalar(udf, input) {
                hits.push(format!("scalar.{name} | {label} | {dtype:?}"));
            }
        }
    }
    let mut aggregate_names: Vec<String> = state.aggregate_functions().keys().cloned().collect();
    aggregate_names.sort();
    for name in &aggregate_names {
        let Some(udaf) = state.aggregate_functions().get(name) else {
            continue;
        };
        for (label, input) in &inputs {
            if let Some(dtype) = guarded_aggregate(udaf, input) {
                hits.push(format!("aggregate.{name} | {label} | {dtype:?}"));
            }
        }
    }
    let mut window_names: Vec<String> = state.window_functions().keys().cloned().collect();
    window_names.sort();
    for name in &window_names {
        let Some(udwf) = state.window_functions().get(name) else {
            continue;
        };
        for (label, input) in &inputs {
            if let Some(dtype) = guarded_window(udwf, name, input) {
                hits.push(format!("window.{name} | {label} | {dtype:?}"));
            }
        }
    }
    hits.sort();
    for line in &hits {
        println!("{line}");
    }
    let walked = scalar_names.len() + aggregate_names.len() + window_names.len();
    println!(
        "walked {} scalar {} aggregate {} window, {} ns lines",
        scalar_names.len(),
        aggregate_names.len(),
        window_names.len(),
        hits.len()
    );
    assert!(
        walked > 0,
        "walk saw no functions: {} scalar {} aggregate {} window",
        scalar_names.len(),
        aggregate_names.len(),
        window_names.len()
    );
}
