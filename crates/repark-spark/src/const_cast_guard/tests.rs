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
