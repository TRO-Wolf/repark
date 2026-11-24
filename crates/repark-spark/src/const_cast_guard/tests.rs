use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use repark_core::ReparkSession;
use repark_functions::ansi::SparkAnsiConfig;
use tempfile::TempDir;

use crate::{SparkDialect, SparkExtension};

async fn spark_session(warehouse: &TempDir, ansi: bool) -> Result<ReparkSession> {
    let path = warehouse.path().to_string_lossy().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("repark.sql.catalogExtensions", "true")
        .config(
            "spark.sql.ansi.enabled",
            if ansi { "true" } else { "false" },
        )
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", path)
        .build()
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    session
        .register_configured_catalogs()
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    session
        .create_namespace("ice", "ns", HashMap::new())
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    Ok(session)
}

fn set_ansi(session: &ReparkSession, enabled: bool) {
    let state_lock = session.context().state_ref();
    let mut state = state_lock.write();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(SparkAnsiConfig { enabled });
}

async fn exec(session: &ReparkSession, sql: &str) -> Result<usize> {
    let frame = session
        .sql(sql)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    let batches = frame.collect().await?;
    Ok(batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum())
}

async fn row_count(session: &ReparkSession, table: &str) -> Result<usize> {
    exec(session, &format!("SELECT * FROM {table}")).await
}

#[tokio::test]
async fn empty_insert_overwrite_refuses_const_casts_under_ansi_and_preserves_rows() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, true).await?;

    let cases: &[(&str, &str, &str, &str, &str)] = &[
        (
            "t_dbl",
            "DOUBLE",
            "1.0",
            "INSERT OVERWRITE ice.ns.t_dbl SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            "[CAST_INVALID_INPUT]",
        ),
        (
            "t_dbl_cte",
            "DOUBLE",
            "1.0",
            "INSERT OVERWRITE ice.ns.t_dbl_cte WITH c AS (SELECT 'x' AS s) SELECT CAST(s AS DOUBLE) FROM c WHERE 1 = 0",
            "[CAST_INVALID_INPUT]",
        ),
        (
            "t_ts",
            "TIMESTAMP",
            "TIMESTAMP '2024-01-01 00:00:00'",
            "INSERT OVERWRITE ice.ns.t_ts SELECT CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            "[CAST_INVALID_INPUT]",
        ),
        (
            "t_ntz",
            "TIMESTAMP_NTZ",
            "CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)",
            "INSERT OVERWRITE ice.ns.t_ntz SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            "[CAST_INVALID_INPUT]",
        ),
        (
            "t_dec",
            "DECIMAL(5, 0)",
            "CAST(1 AS DECIMAL(5, 0))",
            "INSERT OVERWRITE ice.ns.t_dec SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT 1e30 AS d) src WHERE 1 = 0",
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]",
        ),
        (
            "t_bdec",
            "DECIMAL(1, 1)",
            "CAST(0.0 AS DECIMAL(1, 1))",
            "INSERT OVERWRITE ice.ns.t_bdec SELECT CAST(b AS DECIMAL(1, 1)) FROM (SELECT true AS b) src WHERE 1 = 0",
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]",
        ),
        (
            "t_int",
            "INT",
            "1",
            "INSERT OVERWRITE ice.ns.t_int SELECT CAST(s AS INT) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
            "[CAST_INVALID_INPUT]",
        ),
        (
            "t_ovf",
            "INT",
            "1",
            "INSERT OVERWRITE ice.ns.t_ovf SELECT CAST(b AS INT) FROM (SELECT 2147483648 AS b) src WHERE 1 = 0",
            "[CAST_OVERFLOW]",
        ),
    ];

    for (name, col_type, seed, write_sql, expected_class) in cases {
        let fq = format!("ice.ns.{name}");
        exec(&session, &format!("CREATE TABLE {fq} (x {col_type})")).await?;
        exec(&session, &format!("INSERT INTO {fq} VALUES ({seed})")).await?;
        let outcome = exec(&session, write_sql).await;
        let Err(error) = outcome else {
            panic!("{name}: expected {expected_class}, got Ok");
        };
        let message = error.to_string();
        assert!(
            message.contains(expected_class),
            "{name}: expected {expected_class}, got: {message}"
        );
        assert_eq!(row_count(&session, &fq).await?, 1, "{name}: rows wiped");
    }
    Ok(())
}

#[tokio::test]
async fn dead_branches_and_controls_succeed_under_ansi() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, true).await?;

    exec(&session, "CREATE TABLE ice.ns.t_ctrl (x DOUBLE)").await?;
    exec(
        &session,
        "CREATE TABLE ice.ns.empty_src (s STRING, flag BOOLEAN)",
    )
    .await?;

    let ok_double_sources = [
        "SELECT CASE WHEN false THEN CAST(s AS DOUBLE) ELSE 1.0 END FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT CASE WHEN true THEN 1.0 ELSE CAST(s AS DOUBLE) END FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT IF(false, CAST(s AS DOUBLE), 1.0) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT coalesce(1.0, CAST(s AS DOUBLE)) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT TRY_CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT '1.5' AS s) src WHERE 1 = 0",
        "SELECT CAST(s AS DOUBLE) FROM ice.ns.empty_src",
        "SELECT CASE WHEN flag THEN 1.0 ELSE CAST(q.s AS DOUBLE) END FROM (SELECT 'x' AS s) q CROSS JOIN ice.ns.empty_src WHERE 1 = 0",
        "SELECT CAST(s AS DOUBLE) FROM VALUES ('x') AS v(s) WHERE 1 = 0",
    ];

    for source in ok_double_sources {
        exec(&session, "INSERT OVERWRITE ice.ns.t_ctrl VALUES (9.0)").await?;
        let sql = format!("INSERT OVERWRITE ice.ns.t_ctrl {source}");
        exec(&session, &sql).await?;
        assert_eq!(
            row_count(&session, "ice.ns.t_ctrl").await?,
            0,
            "source: {source}"
        );
    }

    exec(&session, "CREATE TABLE ice.ns.t_ts_ctrl (x TIMESTAMP)").await?;
    let ok_ts_sources = [
        "SELECT CASE WHEN false THEN CAST(s AS TIMESTAMP) ELSE TIMESTAMP '2024-01-01 00:00:00' END FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT IF(false, CAST(s AS TIMESTAMP), TIMESTAMP '2024-01-01 00:00:00') FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT coalesce(TIMESTAMP '2024-01-01 00:00:00', CAST(s AS TIMESTAMP)) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT 'x' AS s) src WHERE false AND CAST(s AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT 'x' AS s) src WHERE (true OR CAST(s AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00') AND 1 = 0",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s LIMIT 0) src",
        "SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT CAST(s AS TIMESTAMP) AS d FROM (SELECT 'x' AS s) inner_q) q WHERE 1 = 0",
        "SELECT TRY_CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT '2024-01-01 00:00:00' AS s) src WHERE 1 = 0",
        "SELECT CAST(s AS TIMESTAMP) FROM ice.ns.empty_src",
        "SELECT CAST(s AS TIMESTAMP) FROM VALUES ('x') AS v(s) WHERE 1 = 0",
    ];

    for source in ok_ts_sources {
        exec(
            &session,
            "INSERT OVERWRITE ice.ns.t_ts_ctrl VALUES (TIMESTAMP '2024-01-01 00:00:00')",
        )
        .await?;
        let sql = format!("INSERT OVERWRITE ice.ns.t_ts_ctrl {source}");
        exec(&session, &sql).await?;
        assert_eq!(
            row_count(&session, "ice.ns.t_ts_ctrl").await?,
            0,
            "source: {source}"
        );
    }

    exec(&session, "CREATE TABLE ice.ns.t_dec_nan (x DECIMAL(5, 0))").await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_dec_nan SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST('NaN' AS DOUBLE) AS d) src WHERE 1 = 0",
    )
    .await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_dec_nan SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT CAST('Infinity' AS DOUBLE) AS d) src WHERE 1 = 0",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn live_branches_short_circuit_order_and_dml_doors() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, true).await?;

    exec(&session, "CREATE TABLE ice.ns.t_dml (x TIMESTAMP)").await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_dml VALUES (TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await?;

    let err = exec(
        &session,
        "INSERT INTO ice.ns.t_dml SELECT TIMESTAMP '2024-01-01 00:00:00' FROM (SELECT 'x' AS s) src WHERE CAST(s AS TIMESTAMP) = TIMESTAMP '2024-01-01 00:00:00' AND false",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "lhs bad cast before AND false must raise"
    );

    let err = exec(
        &session,
        "DELETE FROM ice.ns.t_dml WHERE 1 = 0 AND x = CAST('x' AS TIMESTAMP)",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "DELETE WHERE 1=0 AND col = bad const cast must raise"
    );

    let err = exec(
        &session,
        "UPDATE ice.ns.t_dml SET x = TIMESTAMP '2024-01-02 00:00:00' WHERE 1 = 0 AND x = CAST('x' AS TIMESTAMP)",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "UPDATE WHERE 1=0 AND col = bad const cast must raise"
    );

    exec(
        &session,
        "UPDATE ice.ns.t_dml SET x = CAST('x' AS TIMESTAMP) WHERE 1 = 0",
    )
    .await?;
    assert_eq!(row_count(&session, "ice.ns.t_dml").await?, 1);

    let err = exec(
        &session,
        "CREATE TABLE ice.ns.t_ctas AS SELECT CAST(s AS TIMESTAMP) AS x FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "CTAS must refuse before creating table"
    );

    let err = exec(
        &session,
        "MERGE INTO ice.ns.t_dml t USING (SELECT CAST(s AS TIMESTAMP) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0) s ON t.x = s.x WHEN MATCHED THEN DELETE",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "MERGE source must refuse"
    );

    let err = exec(
        &session,
        "MERGE INTO ice.ns.t_dml t USING (SELECT TIMESTAMP '2024-01-01 00:00:00' AS x WHERE 1 = 0) s ON t.x = s.x WHEN MATCHED THEN UPDATE SET x = CAST('x' AS TIMESTAMP)",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "MERGE action must refuse even with empty source"
    );

    assert_eq!(row_count(&session, "ice.ns.t_dml").await?, 1);
    Ok(())
}

#[tokio::test]
async fn ansi_off_allows_empty_writes_and_temp_view_captures_ansi() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, false).await?;

    exec(&session, "CREATE TABLE ice.ns.t_off (x DOUBLE)").await?;
    exec(&session, "INSERT INTO ice.ns.t_off VALUES (1.0)").await?;

    exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_off SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0",
    )
    .await?;
    assert_eq!(row_count(&session, "ice.ns.t_off").await?, 0);

    exec(&session, "CREATE TEMP VIEW tv_src AS SELECT 'x' AS s").await?;
    exec(
        &session,
        "CREATE TEMP VIEW tv_cast_off AS SELECT CAST(s AS DOUBLE) AS x FROM (SELECT 'x' AS s) q WHERE 1 = 0",
    )
    .await?;

    set_ansi(&session, true);
    exec(&session, "INSERT INTO ice.ns.t_off VALUES (1.0)").await?;

    let err = exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_off SELECT CAST(s AS DOUBLE) FROM tv_src WHERE 1 = 0",
    )
    .await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")),
        "temp view created off + cast in outer ANSI=true write must raise"
    );
    assert_eq!(row_count(&session, "ice.ns.t_off").await?, 1);

    exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_off SELECT x FROM tv_cast_off",
    )
    .await?;
    assert_eq!(row_count(&session, "ice.ns.t_off").await?, 0);
    Ok(())
}

#[test]
fn is_cast_error_ignores_non_cast_spark_errors() {
    let ordering = DataFusionError::Plan(
        "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve \"(c = m)\"".to_string(),
    );
    assert!(!super::probes::is_cast_error(&ordering));

    for msg in [
        "[CAST_INVALID_INPUT] bad",
        "[CAST_OVERFLOW] big",
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] big",
        "[NUMERIC_OUT_OF_SUPPORTED_RANGE] big",
        "[CANNOT_PARSE_TIMESTAMP] bad",
        "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] bad",
    ] {
        assert!(
            super::probes::is_cast_error(&DataFusionError::Execution(msg.to_string())),
            "{msg}"
        );
    }
}

#[test]
fn spark_grammar_date_and_timestamp_boundaries() {
    use super::spark_grammar::{
        is_valid_spark_timestamp_ltz, parse_spark_date, parse_spark_timestamp_ntz,
    };

    for valid in [
        " 2024-01-01 ",
        "\t2024-01-01\n",
        "2024",
        "2024-01",
        "2024-1",
        "2024-1-1",
        "2024-02-29",
        "2024-01-01T12:34:56",
        "2024-01-01 12:34:56",
        "2024-01-01T",
        "2024-01-01 garbage",
        "+2024-01-01",
        "02024-01-01",
        "0000-01-01",
        "-0001-01-01",
        "10000-01-01",
    ] {
        assert!(parse_spark_date(valid).is_some(), "DATE valid: {valid:?}");
    }
    for invalid in [
        "",
        "   ",
        "x",
        "2023-02-29",
        "2024-13-01",
        "2024-00-01",
        "2024-01-32",
        "2024-01-01Z",
        "2024-01-01+02:00",
        "2024 T",
        "2024-01 00:00:00",
        "2024-01-01\t12:00:00",
        "2024-01-",
        "999-01-01",
    ] {
        assert!(
            parse_spark_date(invalid).is_none(),
            "DATE invalid: {invalid:?}"
        );
    }

    for valid in [
        "2024",
        "2024-01",
        "2024-1",
        "2024-1-1",
        " 2024-01-01 00:00:00 ",
        "2024-01-01T12:34:56",
        "2024-01-01T12:34:56.1234567890",
        "2024-01-01 1:2:3",
        "2024-01-01 12:34:56.",
        "2024-01-01T12:34",
        "2024-01-01 12",
        "2024-01-01 12:34:56Z",
        "2024-01-01 12:34:56+02:00",
        "2024-01-01 12:34:56+02",
        "2024-01-01 12:34:56-05:30:10",
        "2024-01-01 12:34:56 America/New_York",
        "2024-01-01 12:34:56 UTC",
        "-0001-01-01 00:00:00",
    ] {
        assert!(
            parse_spark_timestamp_ntz(valid).is_some(),
            "NTZ valid: {valid:?}"
        );
        assert!(is_valid_spark_timestamp_ltz(valid), "LTZ valid: {valid:?}");
    }
    assert!(is_valid_spark_timestamp_ltz("12:34:56"));
    assert!(is_valid_spark_timestamp_ltz("T12:34:56"));
    for invalid in [
        "",
        "x",
        "12:34:56",
        "T12:34:56",
        "2023-02-29 00:00:00",
        "2024-01-01 24:00:00",
        "2024-01-01 23:60:00",
        "2024-01-01 23:59:60",
        "2024-01-01 12:34:56 BADZONE",
        "2024-01-01T",
    ] {
        assert!(
            parse_spark_timestamp_ntz(invalid).is_none(),
            "NTZ invalid: {invalid:?}"
        );
    }
    assert!(!is_valid_spark_timestamp_ltz("2024-01-01 12:34:56 BADZONE"));
    assert!(!is_valid_spark_timestamp_ltz("2024-01-01T"));
    assert!(!is_valid_spark_timestamp_ltz("x"));
}

#[test]
fn spark_grammar_float_boundaries() {
    use super::spark_grammar::{parse_spark_double, parse_spark_float};

    for valid in [
        "0x1.0p0",
        " 0x1.0p0 ",
        "-0x1.8p+1",
        "+0X1p-2",
        "0x1p0d",
        "0x1p0f",
        "0x1.p0",
        "0x.8p0",
        "0x10p0f",
        "0x1p-1075",
        "0x1p1024",
        "+0x0p0",
        "1d",
        "1f",
        " 1.5D ",
        "\t-2.5f\n",
        "Infinity",
        "+Infinity",
        "-Infinity",
        "Inf",
        "inf",
        "-inf",
        "NaN",
        "nan",
        "-NaN",
        "1e309",
        ".5",
        "1.",
        "1.0e+10",
    ] {
        assert!(parse_spark_double(valid).is_some(), "DBL valid: {valid:?}");
        assert!(parse_spark_float(valid).is_some(), "FLT valid: {valid:?}");
    }
    for invalid in [
        "",
        "   ",
        "x",
        "0x10",
        "0x1p",
        "Infinityd",
        concat!("NaN", "d"),
        "NaNf",
        "Inff",
        "1 d",
        ".",
        "1e",
        "1_000.0",
        "1dd",
    ] {
        assert!(
            parse_spark_double(invalid).is_none(),
            "DBL invalid: {invalid:?}"
        );
    }
}

#[test]
fn spark_grammar_decimal_and_integral_boundaries() {
    use super::spark_grammar::{SparkDecimalOutcome, parse_spark_decimal, parse_spark_integral};
    use datafusion::arrow::datatypes::DataType;

    assert!(matches!(
        parse_spark_decimal("1e4", 5, 0),
        SparkDecimalOutcome::Ok(10_000)
    ));
    assert!(matches!(
        parse_spark_decimal(" 1e4 ", 5, 0),
        SparkDecimalOutcome::Ok(10_000)
    ));
    assert!(matches!(
        parse_spark_decimal("9.99994e4", 5, 0),
        SparkDecimalOutcome::Ok(99_999)
    ));
    assert!(matches!(
        parse_spark_decimal("9.99995e4", 5, 0),
        SparkDecimalOutcome::OutOfRange
    ));
    assert!(matches!(
        parse_spark_decimal("1e5", 5, 0),
        SparkDecimalOutcome::OutOfRange
    ));
    assert!(matches!(
        parse_spark_decimal("100000e-1", 5, 0),
        SparkDecimalOutcome::Ok(10_000)
    ));
    assert!(matches!(
        parse_spark_decimal("0.0000001e10", 5, 0),
        SparkDecimalOutcome::Ok(1_000)
    ));
    assert!(matches!(
        parse_spark_decimal("0e37", 5, 0),
        SparkDecimalOutcome::Ok(0)
    ));
    assert!(matches!(
        parse_spark_decimal("0e38", 5, 0),
        SparkDecimalOutcome::OutOfSupportedRange
    ));
    assert!(matches!(
        parse_spark_decimal("1e38", 5, 0),
        SparkDecimalOutcome::OutOfSupportedRange
    ));
    assert!(matches!(
        parse_spark_decimal("0e-1000", 5, 0),
        SparkDecimalOutcome::Ok(0)
    ));
    assert!(matches!(
        parse_spark_decimal("1e-1000", 5, 0),
        SparkDecimalOutcome::Ok(0)
    ));
    assert!(matches!(
        parse_spark_decimal(".5", 5, 0),
        SparkDecimalOutcome::Ok(1)
    ));
    assert!(matches!(
        parse_spark_decimal("999.994", 5, 2),
        SparkDecimalOutcome::Ok(99_999)
    ));
    assert!(matches!(
        parse_spark_decimal("999.995", 5, 2),
        SparkDecimalOutcome::OutOfRange
    ));
    assert!(matches!(
        parse_spark_decimal("-999.995", 5, 2),
        SparkDecimalOutcome::OutOfRange
    ));
    assert!(matches!(
        parse_spark_decimal("999.994999999999", 5, 2),
        SparkDecimalOutcome::Ok(99_999)
    ));
    for invalid in ["", "   ", ".", "1d", "0x1p0", "NaN", "Infinity", "1_000"] {
        assert!(
            matches!(
                parse_spark_decimal(invalid, 5, 0),
                SparkDecimalOutcome::InvalidInput
            ),
            "DEC invalid: {invalid:?}"
        );
    }

    assert!(parse_spark_integral(" 42 ", &DataType::Int32).is_some());
    assert!(parse_spark_integral("+42", &DataType::Int32).is_some());
    assert!(parse_spark_integral("-0", &DataType::Int32).is_some());
    assert!(parse_spark_integral("2147483647", &DataType::Int32).is_some());
    assert!(parse_spark_integral("2147483648", &DataType::Int32).is_none());
    assert!(parse_spark_integral("1.0", &DataType::Int32).is_none());
    assert!(parse_spark_integral("1e3", &DataType::Int32).is_none());
    assert!(parse_spark_integral("9223372036854775807", &DataType::Int64).is_some());
    assert!(parse_spark_integral("9223372036854775808", &DataType::Int64).is_none());
}

#[tokio::test]
async fn outer_join_and_merge_action_rules_match_spark() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, true).await?;

    exec(&session, "CREATE TABLE ice.ns.real2 (id INT)").await?;
    exec(&session, "INSERT INTO ice.ns.real2 VALUES (2)").await?;
    exec(&session, "CREATE TABLE ice.ns.t_dbl (x DOUBLE)").await?;
    exec(&session, "INSERT INTO ice.ns.t_dbl VALUES (9.0)").await?;
    exec(&session, "CREATE TABLE ice.ns.t_ts (x TIMESTAMP)").await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_ts VALUES (TIMESTAMP '2020-01-01 00:00:00')",
    )
    .await?;

    exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_dbl SELECT CAST(r.s AS DOUBLE) FROM ice.ns.real2 l \
         LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id",
    )
    .await?;
    assert_eq!(row_count(&session, "ice.ns.t_dbl").await?, 1);

    exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_ts SELECT CAST(r.s AS TIMESTAMP) FROM ice.ns.real2 l \
         LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON false",
    )
    .await?;
    exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_ts SELECT CAST(l.s AS TIMESTAMP) FROM (SELECT 1 AS id, 'x' AS s) l \
         RIGHT JOIN ice.ns.real2 r ON l.id = r.id",
    )
    .await?;

    let err = exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_dbl SELECT CAST(r.s AS DOUBLE) FROM (SELECT 1 AS id) l \
         LEFT JOIN (SELECT 1 AS id, 'x' AS s) r ON l.id = r.id WHERE 1 = 0",
    )
    .await;
    assert!(err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")));

    exec(&session, "CREATE TABLE ice.ns.t_mg (id INT, x TIMESTAMP)").await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_mg VALUES (1, TIMESTAMP '2020-01-01 00:00:00')",
    )
    .await?;
    exec(
        &session,
        "MERGE INTO ice.ns.t_mg t USING (SELECT 2 AS id, 'x' AS s) src ON t.id = src.id \
         WHEN MATCHED THEN UPDATE SET x = CAST(src.s AS TIMESTAMP)",
    )
    .await?;
    exec(
        &session,
        "MERGE INTO ice.ns.t_mg t USING (SELECT 1 AS id, 'x' AS s) src ON false \
         WHEN MATCHED THEN UPDATE SET x = CAST(src.s AS TIMESTAMP)",
    )
    .await?;
    let err = exec(
        &session,
        "MERGE INTO ice.ns.t_mg t USING (SELECT 2 AS id, 'x' AS s) src ON t.id = src.id \
         WHEN MATCHED THEN UPDATE SET x = CAST('x' AS TIMESTAMP)",
    )
    .await;
    assert!(err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")));
    Ok(())
}

#[tokio::test]
async fn replaced_temp_view_and_fc_rules_match_spark() -> Result<()> {
    let wh = TempDir::new()?;
    let session = spark_session(&wh, true).await?;

    exec(&session, "CREATE TABLE ice.ns.t_ts (x TIMESTAMP)").await?;
    exec(
        &session,
        "INSERT INTO ice.ns.t_ts VALUES (TIMESTAMP '2020-01-01 00:00:00')",
    )
    .await?;

    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_r1 AS SELECT 'x' AS s",
    )
    .await?;
    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_r2 AS SELECT CAST(s AS TIMESTAMP) AS x FROM tv_r1",
    )
    .await?;
    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_r1 AS SELECT '2024-01-01 00:00:00' AS s",
    )
    .await?;
    exec(&session, "INSERT OVERWRITE ice.ns.t_ts SELECT x FROM tv_r2").await?;
    assert_eq!(row_count(&session, "ice.ns.t_ts").await?, 1);

    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_c1 AS SELECT '2024-01-01 00:00:00' AS s",
    )
    .await?;
    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_c2 AS SELECT s FROM tv_c1",
    )
    .await?;
    exec(
        &session,
        "CREATE OR REPLACE TEMP VIEW tv_c1 AS SELECT 'x' AS s",
    )
    .await?;
    let err = exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_ts SELECT CAST(s AS TIMESTAMP) FROM tv_c2 WHERE 1 = 0",
    )
    .await;
    assert!(err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")));
    assert_eq!(row_count(&session, "ice.ns.t_ts").await?, 1);

    let err = exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_ts SELECT to_timestamp_ltz(s, 'yyyy-MM-dd') \
         FROM (SELECT 'x' AS s) q WHERE 1 = 0",
    )
    .await;
    assert!(err.is_err_and(|e| e.to_string().contains("[CANNOT_PARSE_TIMESTAMP]")));

    let err = exec(
        &session,
        "INSERT OVERWRITE ice.ns.t_ts SELECT CAST('x' AS TIMESTAMP) \
         FROM (SELECT COUNT(*) AS c FROM (SELECT 1 AS a LIMIT 0) q) a WHERE 1 = 0",
    )
    .await;
    assert!(err.is_err_and(|e| e.to_string().contains("[CAST_INVALID_INPUT]")));

    Ok(())
}
