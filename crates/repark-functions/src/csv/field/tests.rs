use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::prelude::SessionContext;

use super::{
    check_csv_field_target, csv_decimal, csv_field_array, csv_field_expr, java_double, misfit_rows,
};

const ORACLE_COLUMNS: [&str; 13] = [
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal(10,2)",
    "boolean",
    "date",
    "timestamp",
    "timestamp_ntz",
    "string",
    "binary",
];

const ADDED_COLUMNS: [&str; 16] = [
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal(10,2)",
    "boolean",
    "date",
    "timestamp",
    "timestamp_ntz",
    "string",
    "binary",
    "decimal(38,10)",
    "decimal(3,0)",
    "decimal(4,4)",
];

const DECIMAL_COLUMNS: [&str; 3] = ["decimal(38,10)", "decimal(3,0)", "decimal(4,4)"];

const DECLARED: &[(&str, &str)] = &[
    ("\u{661}\u{662}\u{663}", "tinyint"),
    ("\u{661}\u{662}\u{663}", "smallint"),
    ("\u{661}\u{662}\u{663}", "int"),
    ("\u{661}\u{662}\u{663}", "bigint"),
    ("\u{661}\u{662}\u{663}", "decimal(10,2)"),
    ("\u{661}\u{662}\u{663}", "decimal(38,10)"),
    ("\u{661}\u{662}\u{663}", "decimal(3,0)"),
    ("\u{ff11}\u{ff12}", "tinyint"),
    ("\u{ff11}\u{ff12}", "smallint"),
    ("\u{ff11}\u{ff12}", "int"),
    ("\u{ff11}\u{ff12}", "bigint"),
    ("\u{ff11}\u{ff12}", "decimal(10,2)"),
    ("\u{ff11}\u{ff12}", "decimal(38,10)"),
    ("\u{ff11}\u{ff12}", "decimal(3,0)"),
];

fn target(name: &str) -> DataType {
    match name {
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "decimal(10,2)" => DataType::Decimal128(10, 2),
        "decimal(38,10)" => DataType::Decimal128(38, 10),
        "decimal(3,0)" => DataType::Decimal128(3, 0),
        "decimal(4,4)" => DataType::Decimal128(4, 4),
        "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        other => panic!("no test type {other}"),
    }
}

fn now() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

fn parse_one(text: &str, name: &str, zone: &str) -> ArrayRef {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![text]));
    let zone = Tz::from_str(zone).expect("zone");
    csv_field_array(&texts, &target(name), zone, now()).expect("kernel")
}

fn float_matches(found: f64, expected: &str) -> bool {
    let wanted: f64 = expected.parse().expect("float cell");
    if wanted.is_nan() {
        found.is_nan()
    } else {
        found.to_bits() == wanted.to_bits()
    }
}

fn matches_cell(field: &ArrayRef, name: &str, text: &str, expected: &str) -> bool {
    if expected == "N" {
        return field.is_null(0);
    }
    if field.is_null(0) {
        return false;
    }
    match name {
        "tinyint" => field.as_primitive::<Int8Type>().value(0).to_string() == expected,
        "smallint" => field.as_primitive::<Int16Type>().value(0).to_string() == expected,
        "int" => field.as_primitive::<Int32Type>().value(0).to_string() == expected,
        "bigint" => field.as_primitive::<Int64Type>().value(0).to_string() == expected,
        "float" => float_matches(
            f64::from(field.as_primitive::<Float32Type>().value(0)),
            expected,
        ),
        "double" => float_matches(field.as_primitive::<Float64Type>().value(0), expected),
        "boolean" => field.as_boolean().value(0).to_string() == expected,
        "date" => field.as_primitive::<Date32Type>().value(0).to_string() == expected,
        "timestamp" | "timestamp_ntz" => {
            field
                .as_primitive::<TimestampMicrosecondType>()
                .value(0)
                .to_string()
                == expected
        }
        "string" => expected == "=" && field.as_string::<i32>().value(0) == text,
        "binary" => expected == "=" && field.as_binary::<i32>().value(0) == text.as_bytes(),
        _ => field.as_primitive::<Decimal128Type>().value(0).to_string() == expected,
    }
}

fn declared(text: &str, name: &str) -> bool {
    DECLARED.contains(&(text, name))
}

fn run_table(rows: &[(&str, &str)], columns: &[&str]) -> (usize, usize, Vec<String>) {
    let mut passed = 0;
    let mut declared_cells = 0;
    let mut misses = Vec::new();
    for (text, cells) in rows {
        let expected: Vec<&str> = cells.split(' ').collect();
        assert_eq!(expected.len(), columns.len(), "row {text:?}");
        for (name, wanted) in columns.iter().zip(expected) {
            let field = parse_one(text, name, "UTC");
            if declared(text, name) {
                assert_ne!(
                    wanted, "N",
                    "a declared cell is one Spark answers: {text:?} {name}"
                );
                assert!(field.is_null(0), "declared {text:?} {name}");
                declared_cells += 1;
            } else if matches_cell(&field, name, text, wanted) {
                passed += 1;
            } else {
                misses.push(format!("{text:?} {name}: Spark {wanted}"));
            }
        }
    }
    (passed, declared_cells, misses)
}

#[test]
fn every_oracle_cell_answers_spark() {
    let (passed, declared_cells, misses) = run_table(ORACLE, &ORACLE_COLUMNS);
    assert!(misses.is_empty(), "{misses:#?}");
    assert_eq!((passed, declared_cells), (962, 0));
}

#[test]
fn every_added_cell_answers_spark_or_is_declared() {
    let (passed, declared_cells, misses) = run_table(ADDED, &ADDED_COLUMNS);
    assert!(misses.is_empty(), "{misses:#?}");
    assert_eq!((passed, declared_cells), (978, 14));
    let (passed, declared_cells, misses) = run_table(ADDED_DECIMALS, &DECIMAL_COLUMNS);
    assert!(misses.is_empty(), "{misses:#?}");
    assert_eq!((passed, declared_cells), (12, 0));
}

#[test]
fn every_zone_cell_answers_spark_and_ntz_never_reads_the_zone() {
    for (zone, name, text, wanted) in ZONE_CELLS {
        let field = parse_one(text, name, zone);
        assert!(
            matches_cell(&field, name, text, wanted),
            "{zone} {name} {text:?}: Spark {wanted}"
        );
        if *name == "timestamp_ntz" {
            let utc = parse_one(text, name, "UTC");
            assert!(matches_cell(&utc, name, text, wanted), "{text:?}");
        }
    }
    assert_eq!(ZONE_CELLS.len(), 84);
}

#[test]
fn a_misfit_is_reported_apart_from_null_input() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![
        Some("7"),
        None,
        Some("abc"),
        Some(""),
    ]));
    let zone = Tz::from_str("UTC").expect("zone");
    let fields = csv_field_array(&texts, &DataType::Int32, zone, now()).expect("kernel");
    assert_eq!(fields.null_count(), 3);
    let misfits = misfit_rows(texts.as_ref(), fields.as_ref());
    assert_eq!(
        misfits.iter().collect::<Vec<_>>(),
        [false, false, true, true]
    );
    let strings = csv_field_array(&texts, &DataType::Utf8, zone, now()).expect("kernel");
    let kept = misfit_rows(texts.as_ref(), strings.as_ref());
    assert_eq!(kept.count_set_bits(), 0);
    assert_eq!(strings.as_string::<i32>().value(3), "");
}

#[test]
fn a_huge_decimal_exponent_answers_without_expanding_it() {
    assert_eq!(csv_decimal("1e1000000000", 10, 2), None);
    assert_eq!(csv_decimal("1e-1000000000", 10, 2), Some(0));
    assert_eq!(csv_decimal("-1e-1000000000", 38, 10), Some(0));
    assert_eq!(csv_decimal("0e1000000000", 10, 2), Some(0));
    assert_eq!(csv_decimal("1e2147483647", 38, 0), None);
    assert_eq!(java_double("1e1000000000"), Some(f64::INFINITY));
}

#[test]
fn intervals_variant_and_nested_types_refuse_by_name() {
    for target in [
        DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::YearMonth),
        DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        DataType::Duration(TimeUnit::Microsecond),
        DataType::Decimal128(39, 0),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        let error = check_csv_field_target(&target).expect_err("refused");
        assert!(
            error.to_string().contains("[UNSUPPORTED_DATATYPE]"),
            "{error}"
        );
        let expr = csv_field_expr(datafusion::prelude::col("x"), &target);
        assert!(expr.is_err());
    }
}

fn sql_cell(query: &str) -> String {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let batches = runtime
        .block_on(async { ctx.sql(query).await?.collect().await })
        .expect("query");
    datafusion::common::ScalarValue::try_from_array(batches[0].column(0), 0)
        .expect("scalar")
        .to_string()
}

#[test]
fn the_registered_udf_answers_the_declared_type() {
    assert_eq!(
        sql_cell("SELECT __repark_csv_field__('007', CAST(NULL AS INT))"),
        "7"
    );
    assert_eq!(
        sql_cell(
            "SELECT __repark_csv_field__(x, CAST(NULL AS DECIMAL(10,2))) FROM (VALUES ('1,000.5')) AS t(x)"
        ),
        "Some(100050),10,2"
    );
    assert_eq!(
        sql_cell(
            "SELECT __repark_csv_field__(x, CAST(NULL AS DOUBLE)) FROM (VALUES (' 7 ')) AS t(x)"
        ),
        "7"
    );
    assert_eq!(
        sql_cell(
            "SELECT __repark_csv_field__(x, CAST(NULL AS BOOLEAN)) FROM (VALUES ('t')) AS t(x)"
        ),
        "NULL"
    );
}

const ORACLE: &[(&str, &str)] = &[
    ("0", "0 0 0 0 0.0 0.0 0 N N N N = ="),
    ("-0", "0 0 0 0 -0.0 -0.0 0 N N N N = ="),
    ("007", "7 7 7 7 7.0 7.0 700 N N N N = ="),
    ("+5", "5 5 5 5 5.0 5.0 500 N N N N = ="),
    ("-007", "-7 -7 -7 -7 -7.0 -7.0 -700 N N N N = ="),
    ("127", "127 127 127 127 127.0 127.0 12700 N N N N = ="),
    ("128", "N 128 128 128 128.0 128.0 12800 N N N N = ="),
    ("-129", "N -129 -129 -129 -129.0 -129.0 -12900 N N N N = ="),
    ("200", "N 200 200 200 200.0 200.0 20000 N N N N = ="),
    (
        "32767",
        "N 32767 32767 32767 32767.0 32767.0 3276700 N 11248373 971859427200000000 971859427200000000 = =",
    ),
    (
        "32768",
        "N N 32768 32768 32768.0 32768.0 3276800 N 11248738 971890963200000000 971890963200000000 = =",
    ),
    (
        "40000",
        "N N 40000 40000 40000.0 40000.0 4000000 N 13890172 1200110860800000000 1200110860800000000 = =",
    ),
    (
        "2147483647",
        "N N 2147483647 2147483647 2147483648.0 2147483647.0 N N N N N = =",
    ),
    (
        "2147483648",
        "N N N 2147483648 2147483648.0 2147483648.0 N N N N N = =",
    ),
    (
        "9223372036854775807",
        "N N N 9223372036854775807 9.223372036854776e+18 9.223372036854776e+18 N N N N N = =",
    ),
    (
        "9223372036854775808",
        "N N N N 9.223372036854776e+18 9.223372036854776e+18 N N N N N = =",
    ),
    (
        "99999999999999999999",
        "N N N N 1.0000000200408773e+20 1e+20 N N N N N = =",
    ),
    (" 7", "N N N N 7.0 7.0 N N N N N = ="),
    ("7 ", "N N N N 7.0 7.0 N N N N N = ="),
    ("\t7", "N N N N 7.0 7.0 N N N N N = ="),
    (" 7 ", "N N N N 7.0 7.0 N N N N N = ="),
    ("1.50", "N N N N 1.5 1.5 150 N N N N = ="),
    ("1.5", "N N N N 1.5 1.5 150 N N N N = ="),
    ("5.", "N N N N 5.0 5.0 500 N N N N = ="),
    (".5", "N N N N 0.5 0.5 50 N N N N = ="),
    ("1e3", "N N N N 1000.0 1000.0 100000 N N N N = ="),
    ("1E-2", "N N N N 0.009999999776482582 0.01 1 N N N N = ="),
    ("1.555", "N N N N 1.5549999475479126 1.555 156 N N N N = ="),
    (
        "-1.555",
        "N N N N -1.5549999475479126 -1.555 -156 N N N N = =",
    ),
    ("1.554", "N N N N 1.5540000200271606 1.554 155 N N N N = ="),
    (
        "99999999.995",
        "N N N N 100000000.0 99999999.995 N N N N N = =",
    ),
    (
        "123456789012",
        "N N N 123456789012 123456790528.0 123456789012.0 N N N N N = =",
    ),
    ("1e400", "N N N N inf inf N N N N N = ="),
    ("1,5", "N N N N N N 1500 N N N N = ="),
    ("1,000.5", "N N N N N N 100050 N N N N = ="),
    ("1d", "N N N N 1.0 1.0 N N N N N = ="),
    ("1f", "N N N N 1.0 1.0 N N N N N = ="),
    ("0x10", "N N N N N N N N N N N = ="),
    ("0x1p3", "N N N N 8.0 8.0 N N N N N = ="),
    ("1_000", "N N N N N N N N N N N = ="),
    ("NaN", "N N N N nan nan N N N N N = ="),
    ("nan", "N N N N N N N N N N N = ="),
    ("Inf", "N N N N inf inf N N N N N = ="),
    ("+Inf", "N N N N N N N N N N N = ="),
    ("-Inf", "N N N N -inf -inf N N N N N = ="),
    ("inf", "N N N N N N N N N N N = ="),
    ("Infinity", "N N N N inf inf N N N N N = ="),
    ("-Infinity", "N N N N -inf -inf N N N N N = ="),
    ("true", "N N N N N N N true N N N = ="),
    ("TRUE", "N N N N N N N true N N N = ="),
    ("True", "N N N N N N N true N N N = ="),
    ("false", "N N N N N N N false N N N = ="),
    ("t", "N N N N N N N N N N N = ="),
    ("1", "1 1 1 1 1.0 1.0 100 N N N N = ="),
    ("y", "N N N N N N N N N N N = ="),
    ("yes", "N N N N N N N N N N N = ="),
    (" true", "N N N N N N N N N N N = ="),
    (
        "2024",
        "N 2024 2024 2024 2024.0 2024.0 202400 N 19723 1704067200000000 1704067200000000 = =",
    ),
    (
        "2024-1",
        "N N N N N N N N 19723 1704067200000000 1704067200000000 = =",
    ),
    (
        "2024-1-5",
        "N N N N N N N N 19727 1704412800000000 1704412800000000 = =",
    ),
    (
        "2024-01-15",
        "N N N N N N N N 19737 1705276800000000 1705276800000000 = =",
    ),
    (
        "20240115",
        "N N 20240115 20240115 20240116.0 20240115.0 2024011500 N N N N = =",
    ),
    (
        " 2024-01-15",
        "N N N N N N N N 19737 1705276800000000 1705276800000000 = =",
    ),
    (
        "2024-01-15T10:30:00",
        "N N N N N N N N 19737 1705314600000000 1705314600000000 = =",
    ),
    (
        "2024-01-15 10:30:00",
        "N N N N N N N N 19737 1705314600000000 1705314600000000 = =",
    ),
    (
        "2024-01-15 10:30",
        "N N N N N N N N 19737 1705314600000000 1705314600000000 = =",
    ),
    (
        "2024-01-15T10:30:00Z",
        "N N N N N N N N 19737 1705314600000000 N = =",
    ),
    (
        "2024-01-15 10:30:00+02:00",
        "N N N N N N N N 19737 1705307400000000 N = =",
    ),
    (
        "2024-01-15 10:30:00.123456",
        "N N N N N N N N 19737 1705314600123456 1705314600123456 = =",
    ),
    (
        "1705314600",
        "N N 1705314600 1705314600 1705314560.0 1705314600.0 N N N N N = =",
    ),
    ("1-2", "N N N N N N N N N N N = ="),
    ("1 02:03:04", "N N N N N N N N N N N = ="),
    ("INTERVAL '1-2' YEAR TO MONTH", "N N N N N N N N N N N = ="),
    ("abc", "N N N N N N N N N N N = ="),
];

const ADDED: &[(&str, &str)] = &[
    (" ", "N N N N N N N N N N N = = N N N"),
    (".", "N N N N N N N N N N N = = N N N"),
    ("+NaN", "N N N N nan nan N N N N N = = N N N"),
    ("-NaN", "N N N N nan nan N N N N N = = N N N"),
    ("NaN ", "N N N N nan nan N N N N N = = N N N"),
    (" Inf", "N N N N N N N N N N N = = N N N"),
    ("+Infinity", "N N N N inf inf N N N N N = = N N N"),
    ("1D", "N N N N 1.0 1.0 N N N N N = = N N N"),
    ("1e3f", "N N N N 1000.0 1000.0 N N N N N = = N N N"),
    ("\u{1}7", "N N N N 7.0 7.0 N N N N N = = N N N"),
    ("\u{a0}7", "N N N N N N N N N N N = = N N N"),
    ("\u{2003}7", "N N N N N N N N N N N = = N N N"),
    ("fal\u{17f}e", "N N N N N N N false N N N = = N N N"),
    ("FAL\u{17f}E", "N N N N N N N false N N N = = N N N"),
    (
        "\u{661}\u{662}\u{663}",
        "123 123 123 123 N N 12300 N N N N = = 1230000000000 123 N",
    ),
    (
        "\u{ff11}\u{ff12}",
        "12 12 12 12 N N 1200 N N N N = = 120000000000 12 N",
    ),
    ("1e-400", "N N N N 0.0 0.0 0 N N N N = = 0 0 0"),
    ("0x1.8p1", "N N N N 3.0 3.0 N N N N N = = N N N"),
    ("-0x1p-2", "N N N N -0.25 -0.25 N N N N N = = N N N"),
    ("0X10P0", "N N N N 16.0 16.0 N N N N N = = N N N"),
    ("0x1p3d", "N N N N 8.0 8.0 N N N N N = = N N N"),
    ("1e", "N N N N N N N N N N N = = N N N"),
    ("-", "N N N N N N N N N N N = = N N N"),
    ("+", "N N N N N N N N N N N = = N N N"),
    ("1.2.3", "N N N N N N N N N N N = = N N N"),
    ("--1", "N N N N N N N N N N N = = N N N"),
    (",", "N N N N N N N N N N N = = N N N"),
    (
        "1,,000",
        "N N N N N N 100000 N N N N = = 10000000000000 N N",
    ),
    ("-1,5", "N N N N N N -1500 N N N N = = -150000000000 -15 N"),
    ("2024-02-30", "N N N N N N N N N N N = = N N N"),
    (
        "2024-02-29",
        "N N N N N N N N 19782 1709164800000000 1709164800000000 = = N N N",
    ),
    (
        "GMT2024-01-15",
        "N N N N N N N N 19737 1705276800000000 N = = N N N",
    ),
    (
        "2024-01-15GMT",
        "N N N N N N N N 19737 1705276800000000 N = = N N N",
    ),
    (
        "2024-01-15 10:30:00GMT",
        "N N N N N N N N 19737 1705314600000000 N = = N N N",
    ),
    (
        "2024-01-15 10:30:00UTC",
        "N N N N N N N N 19737 1705314600000000 N = = N N N",
    ),
    (
        "2024-01-15T10:30:00.1234567",
        "N N N N N N N N 19737 1705314600123456 1705314600123456 = = N N N",
    ),
    (
        "+2024-01-15",
        "N N N N N N N N 19737 1705276800000000 1705276800000000 = = N N N",
    ),
    (
        "-0001-01-01",
        "N N N N N N N N -719893 -62198755200000000 -62198755200000000 = = N N N",
    ),
    (
        "12024-01-15",
        "N N N N N N N N 3672162 317274796800000000 317274796800000000 = = N N N",
    ),
    ("1234567-01-01", "N N N N N N N N 450196810 N N = = N N N"),
    ("2024-1-5 garbage", "N N N N N N N N 19727 N N = = N N N"),
    (
        "2024-01-15  10:30:00",
        "N N N N N N N N 19737 N N = = N N N",
    ),
    (
        "2024-01-15\t",
        "N N N N N N N N 19737 1705276800000000 1705276800000000 = = N N N",
    ),
    (
        "1e10",
        "N N N N 10000000000.0 10000000000.0 N N N N N = = 100000000000000000000 N N",
    ),
    (
        "-1e-3",
        "N N N N -0.0010000000474974513 -0.001 0 N N N N = = -10000000 0 -10",
    ),
    (
        "0.005",
        "N N N N 0.004999999888241291 0.005 1 N N N N = = 50000000 0 50",
    ),
    (
        "-0.005",
        "N N N N -0.004999999888241291 -0.005 -1 N N N N = = -50000000 0 -50",
    ),
    (
        "0.0049999",
        "N N N N 0.00499989977106452 0.0049999 0 N N N N = = 49999000 0 50",
    ),
    (
        "1E+2",
        "N N N N 100.0 100.0 10000 N N N N = = 1000000000000 100 N",
    ),
    ("00.50", "N N N N 0.5 0.5 50 N N N N = = 5000000000 1 5000"),
    ("1.0e0", "N N N N 1.0 1.0 100 N N N N = = 10000000000 1 N"),
    ("1e-2147483648", "N N N N 0.0 0.0 N N N N N = = N N N"),
    ("1e2147483648", "N N N N inf inf N N N N N = = N N N"),
    ("1e9999999999", "N N N N inf inf N N N N N = = N N N"),
    (
        "-128",
        "-128 -128 -128 -128 -128.0 -128.0 -12800 N N N N = = -1280000000000 -128 N",
    ),
    (
        "-32768",
        "N -32768 -32768 -32768 -32768.0 -32768.0 -3276800 N -12687794 -1096225401600000000 -1096225401600000000 = = -327680000000000 N N",
    ),
    (
        "-2147483648",
        "N N -2147483648 -2147483648 -2147483648.0 -2147483648.0 N N N N N = = -21474836480000000000 N N",
    ),
    (
        "-9223372036854775808",
        "N N N -9223372036854775808 -9.223372036854776e+18 -9.223372036854776e+18 N N N N N = = -92233720368547758080000000000 N N",
    ),
    (
        "0000000000000000000000000000007",
        "7 7 7 7 7.0 7.0 700 N N N N = = 70000000000 7 N",
    ),
    (
        "99999999999999999999999999999999999999",
        "N N N N 9.999999680285692e+37 1e+38 N N N N N = = N N N",
    ),
    (
        "999999999999999999999999999999999999999",
        "N N N N inf 1e+39 N N N N N = = N N N",
    ),
    (
        "0.99995",
        "N N N N 0.9999499917030334 0.99995 100 N N N N = = 9999500000 1 N",
    ),
];

const ADDED_DECIMALS: &[(&str, &str)] = &[
    ("-0", "0 0 0"),
    ("32767", "327670000000000 N N"),
    ("32768", "327680000000000 N N"),
    ("40000", "400000000000000 N N"),
];

const ZONE_CELLS: &[(&str, &str, &str, &str)] = &[
    ("America/Los_Angeles", "date", "2024-01-15", "19737"),
    (
        "America/Los_Angeles",
        "date",
        "2024-01-15 10:30:00",
        "19737",
    ),
    (
        "America/Los_Angeles",
        "date",
        "2024-01-15T10:30:00Z",
        "19737",
    ),
    (
        "America/Los_Angeles",
        "date",
        "2024-01-15 10:30:00+02:00",
        "19737",
    ),
    (
        "America/Los_Angeles",
        "date",
        "2024-03-10 02:30:00",
        "19792",
    ),
    (
        "America/Los_Angeles",
        "date",
        "2024-11-03 01:30:00",
        "20030",
    ),
    ("America/Los_Angeles", "date", "007", "N"),
    ("America/Los_Angeles", "string", "2024-01-15", "="),
    ("America/Los_Angeles", "string", "2024-01-15 10:30:00", "="),
    ("America/Los_Angeles", "string", "2024-01-15T10:30:00Z", "="),
    (
        "America/Los_Angeles",
        "string",
        "2024-01-15 10:30:00+02:00",
        "=",
    ),
    ("America/Los_Angeles", "string", "2024-03-10 02:30:00", "="),
    ("America/Los_Angeles", "string", "2024-11-03 01:30:00", "="),
    ("America/Los_Angeles", "string", "007", "="),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-01-15",
        "1705305600000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-01-15 10:30:00",
        "1705343400000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-01-15T10:30:00Z",
        "1705314600000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-01-15 10:30:00+02:00",
        "1705307400000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-03-10 02:30:00",
        "1710066600000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp",
        "2024-11-03 01:30:00",
        "1730622600000000",
    ),
    ("America/Los_Angeles", "timestamp", "007", "N"),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-01-15",
        "1705276800000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-01-15 10:30:00",
        "1705314600000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-01-15T10:30:00Z",
        "N",
    ),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-01-15 10:30:00+02:00",
        "N",
    ),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-03-10 02:30:00",
        "1710037800000000",
    ),
    (
        "America/Los_Angeles",
        "timestamp_ntz",
        "2024-11-03 01:30:00",
        "1730597400000000",
    ),
    ("America/Los_Angeles", "timestamp_ntz", "007", "N"),
    ("America/New_York", "date", "2024-01-15", "19737"),
    ("America/New_York", "date", "2024-01-15 10:30:00", "19737"),
    ("America/New_York", "date", "2024-01-15T10:30:00Z", "19737"),
    (
        "America/New_York",
        "date",
        "2024-01-15 10:30:00+02:00",
        "19737",
    ),
    ("America/New_York", "date", "2024-03-10 02:30:00", "19792"),
    ("America/New_York", "date", "2024-11-03 01:30:00", "20030"),
    ("America/New_York", "date", "007", "N"),
    ("America/New_York", "string", "2024-01-15", "="),
    ("America/New_York", "string", "2024-01-15 10:30:00", "="),
    ("America/New_York", "string", "2024-01-15T10:30:00Z", "="),
    (
        "America/New_York",
        "string",
        "2024-01-15 10:30:00+02:00",
        "=",
    ),
    ("America/New_York", "string", "2024-03-10 02:30:00", "="),
    ("America/New_York", "string", "2024-11-03 01:30:00", "="),
    ("America/New_York", "string", "007", "="),
    (
        "America/New_York",
        "timestamp",
        "2024-01-15",
        "1705294800000000",
    ),
    (
        "America/New_York",
        "timestamp",
        "2024-01-15 10:30:00",
        "1705332600000000",
    ),
    (
        "America/New_York",
        "timestamp",
        "2024-01-15T10:30:00Z",
        "1705314600000000",
    ),
    (
        "America/New_York",
        "timestamp",
        "2024-01-15 10:30:00+02:00",
        "1705307400000000",
    ),
    (
        "America/New_York",
        "timestamp",
        "2024-03-10 02:30:00",
        "1710055800000000",
    ),
    (
        "America/New_York",
        "timestamp",
        "2024-11-03 01:30:00",
        "1730611800000000",
    ),
    ("America/New_York", "timestamp", "007", "N"),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-01-15",
        "1705276800000000",
    ),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-01-15 10:30:00",
        "1705314600000000",
    ),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-01-15T10:30:00Z",
        "N",
    ),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-01-15 10:30:00+02:00",
        "N",
    ),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-03-10 02:30:00",
        "1710037800000000",
    ),
    (
        "America/New_York",
        "timestamp_ntz",
        "2024-11-03 01:30:00",
        "1730597400000000",
    ),
    ("America/New_York", "timestamp_ntz", "007", "N"),
    ("Asia/Tokyo", "date", "2024-01-15", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15 10:30:00", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15T10:30:00Z", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15 10:30:00+02:00", "19737"),
    ("Asia/Tokyo", "date", "2024-03-10 02:30:00", "19792"),
    ("Asia/Tokyo", "date", "2024-11-03 01:30:00", "20030"),
    ("Asia/Tokyo", "date", "007", "N"),
    ("Asia/Tokyo", "string", "2024-01-15", "="),
    ("Asia/Tokyo", "string", "2024-01-15 10:30:00", "="),
    ("Asia/Tokyo", "string", "2024-01-15T10:30:00Z", "="),
    ("Asia/Tokyo", "string", "2024-01-15 10:30:00+02:00", "="),
    ("Asia/Tokyo", "string", "2024-03-10 02:30:00", "="),
    ("Asia/Tokyo", "string", "2024-11-03 01:30:00", "="),
    ("Asia/Tokyo", "string", "007", "="),
    ("Asia/Tokyo", "timestamp", "2024-01-15", "1705244400000000"),
    (
        "Asia/Tokyo",
        "timestamp",
        "2024-01-15 10:30:00",
        "1705282200000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp",
        "2024-01-15T10:30:00Z",
        "1705314600000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp",
        "2024-01-15 10:30:00+02:00",
        "1705307400000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp",
        "2024-03-10 02:30:00",
        "1710005400000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp",
        "2024-11-03 01:30:00",
        "1730565000000000",
    ),
    ("Asia/Tokyo", "timestamp", "007", "N"),
    (
        "Asia/Tokyo",
        "timestamp_ntz",
        "2024-01-15",
        "1705276800000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp_ntz",
        "2024-01-15 10:30:00",
        "1705314600000000",
    ),
    ("Asia/Tokyo", "timestamp_ntz", "2024-01-15T10:30:00Z", "N"),
    (
        "Asia/Tokyo",
        "timestamp_ntz",
        "2024-01-15 10:30:00+02:00",
        "N",
    ),
    (
        "Asia/Tokyo",
        "timestamp_ntz",
        "2024-03-10 02:30:00",
        "1710037800000000",
    ),
    (
        "Asia/Tokyo",
        "timestamp_ntz",
        "2024-11-03 01:30:00",
        "1730597400000000",
    ),
    ("Asia/Tokyo", "timestamp_ntz", "007", "N"),
];
