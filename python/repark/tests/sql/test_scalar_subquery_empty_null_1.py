from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _scalar_subquery_empty_null_1_cells import (
    ANSI_MODES,
    DF_EXPECTED,
    NATIVE_SQL_BUG_CELLS,
    PHYS_UNCORR_CELLS,
    SPARK_SQL_BUG_CELLS,
    SQL_CONTROL_CELLS,
    STORED_EXPECTED,
)

import repark
from repark._native import PyReparkSession
from repark.errors import AnalysisException
from repark.spark import SparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("scalar_subquery_empty_null_1")


def capture_arrow(fn: Callable[[], pa.Table]) -> dict[str, Any]:
    try:
        table = fn()
        columns = [column.to_pylist() for column in table.columns]
        rows = [
            [columns[col_idx][row_idx] for col_idx in range(len(columns))]
            for row_idx in range(table.num_rows)
        ]
        nonnull_with_nulls = [
            field.name
            for field, column in zip(table.schema, table.columns, strict=True)
            if (not field.nullable) and column.null_count > 0
        ]
        nullability = {field.name: bool(field.nullable) for field in table.schema}
        return {
            "ok": True,
            "rows": rows,
            "nonnull_with_nulls": nonnull_with_nulls,
            "nullability": nullability,
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def assert_cell_matches(cell: dict[str, Any], expected: list[list[Any]], label: str) -> None:
    assert cell["ok"], f"{label}: {cell.get('error')}"
    assert cell["nonnull_with_nulls"] == [], f"{label}: invalid non-null schema {cell}"
    assert cell["rows"] == expected, f"{label}: got {cell['rows']!r}, expected {expected!r}"


def build_spark_session(warehouse: Path, name: str, ansi: str) -> SparkSession:
    _reset_active_session_for_tests()
    return (
        SparkSession.builder.appName(name)
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


def seed_spark_tables(session: SparkSession) -> None:
    session.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns").collect()
    session.sql(
        "CREATE TABLE ice.ns.empty_nn (id INT NOT NULL, b INT NOT NULL, "
        "s STRING NOT NULL, flag BOOLEAN NOT NULL, d DATE NOT NULL) USING iceberg"
    ).collect()
    session.sql(
        "CREATE TABLE ice.ns.l_nn (id INT NOT NULL, v INT NOT NULL) USING iceberg"
    ).collect()
    session.sql("INSERT INTO ice.ns.l_nn VALUES (1, 10), (2, 20)").collect()
    session.sql(
        "CREATE TABLE ice.ns.r_nn (id INT NOT NULL, b INT NOT NULL) USING iceberg"
    ).collect()
    session.sql("INSERT INTO ice.ns.r_nn VALUES (2, 20)").collect()


def measure_df_cells(session: SparkSession) -> dict[str, dict[str, Any]]:
    left = session.table("ice.ns.l_nn").orderBy("id")
    empty = session.table("ice.ns.empty_nn")
    scalar_col = empty.select("b").scalar()
    f = spark_functions
    return {
        "fexpr_withcol_isnull": capture_arrow(
            lambda: (
                left.withColumn("r", f.expr("(SELECT 1 WHERE false) IS NULL"))
                .select("id", "r")
                .toArrow()
            )
        ),
        "fexpr_withcol_isnotnull": capture_arrow(
            lambda: (
                left.withColumn("r", f.expr("(SELECT 1 WHERE false) IS NOT NULL"))
                .select("id", "r")
                .toArrow()
            )
        ),
        "fexpr_withcol_coalesce": capture_arrow(
            lambda: (
                left.withColumn("r", f.expr("coalesce((SELECT 1 WHERE false), 42)"))
                .select("id", "r")
                .toArrow()
            )
        ),
        "fexpr_withcol_coalesce_id": capture_arrow(
            lambda: (
                left.withColumn("r", f.expr("coalesce((SELECT 1 WHERE false), id)"))
                .select("id", "r")
                .toArrow()
            )
        ),
        "fexpr_withcol_bare": capture_arrow(
            lambda: (
                left.withColumn("r", f.expr("(SELECT 1 WHERE false)")).select("id", "r").toArrow()
            )
        ),
        "fexpr_withcol_cast_date_ts": capture_arrow(
            lambda: (
                left.withColumn(
                    "r", f.expr("CAST((SELECT DATE '2024-01-01' WHERE false) AS TIMESTAMP)")
                )
                .select("id", "r")
                .toArrow()
            )
        ),
        "fexpr_filter_col_isnull": capture_arrow(
            lambda: left.filter(f.expr("(SELECT 1 WHERE false) IS NULL")).select("id").toArrow()
        ),
        "fexpr_filter_col_isnotnull": capture_arrow(
            lambda: left.filter(f.expr("(SELECT 1 WHERE false) IS NOT NULL")).select("id").toArrow()
        ),
        "filter_str_inline_isnull": capture_arrow(
            lambda: left.filter("(SELECT 1 WHERE false) IS NULL").select("id").toArrow()
        ),
        "filter_str_inline_isnotnull": capture_arrow(
            lambda: left.filter("(SELECT 1 WHERE false) IS NOT NULL").select("id").toArrow()
        ),
        "selectexpr_tbl_isnull": capture_arrow(
            lambda: left.selectExpr("id", "(SELECT b FROM ice.ns.empty_nn) IS NULL AS r").toArrow()
        ),
        "selectexpr_tbl_coalesce": capture_arrow(
            lambda: left.selectExpr(
                "id", "coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS r"
            ).toArrow()
        ),
        "withcol_scalar_isnull": capture_arrow(
            lambda: left.withColumn("r", scalar_col.isNull()).select("id", "r").toArrow()
        ),
        "withcol_scalar_coalesce": capture_arrow(
            lambda: (
                left.withColumn("r", f.coalesce(scalar_col, f.lit(42))).select("id", "r").toArrow()
            )
        ),
        "filter_scalar_isnull": capture_arrow(
            lambda: left.filter(scalar_col.isNull()).select("id").toArrow()
        ),
        "filter_scalar_isnotnull": capture_arrow(
            lambda: left.filter(scalar_col.isNotNull()).select("id").toArrow()
        ),
    }


def measure_spark_writes(warehouse: Path, ansi: str) -> dict[str, Any]:
    session = build_spark_session(warehouse, f"scalar-subq-wr-{ansi}", ansi)
    out: dict[str, Any] = {}
    try:
        seed_spark_tables(session)
        session.sql(
            "CREATE TABLE ice.ns.dst_ins (id INT NOT NULL, v INT NOT NULL) USING iceberg"
        ).collect()
        session.sql(
            "INSERT INTO ice.ns.dst_ins SELECT id, v * 10 FROM ice.ns.l_nn "
            "WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL"
        ).collect()
        out["insert_where"] = capture_arrow(
            lambda: session.sql("SELECT id, v FROM ice.ns.dst_ins ORDER BY id").toArrow()
        )
        session.sql(
            "CREATE TABLE ice.ns.dst_ctas USING iceberg AS "
            "SELECT id, coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS c "
            "FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL"
        ).collect()
        out["ctas_where"] = capture_arrow(
            lambda: session.sql("SELECT id, c FROM ice.ns.dst_ctas ORDER BY id").toArrow()
        )
        session.sql(
            "CREATE TABLE ice.ns.dst_bare USING iceberg AS "
            "SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn"
        ).collect()
        out["ctas_bare"] = capture_arrow(
            lambda: session.sql("SELECT id, r FROM ice.ns.dst_bare ORDER BY id").toArrow()
        )
        for key, stmt in (
            (
                "delete_refused",
                "DELETE FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL",
            ),
            (
                "update_refused",
                "UPDATE ice.ns.l_nn SET v = 99 WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL",
            ),
        ):
            try:
                session.sql(stmt).collect()
                out[key] = "OK"
            except AnalysisException:
                out[key] = "AnalysisException"
            except Exception as exc:
                out[key] = type(exc).__name__
    finally:
        session.stop()
        _reset_active_session_for_tests()
    return out


def measure_native_all(warehouse: Path) -> dict[str, Any]:
    config = {
        "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ice.type": "hadoop",
        "spark.sql.catalog.ice.warehouse": str(warehouse),
    }
    native = PyReparkSession.native(config=config)
    native.create_namespace("ice", "ns", None)
    out: dict[str, Any] = {"sql": {}, "stored": {}, "writes": {}}
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(repark, "_ANSI_NATIVE", native)
        repark.sql(
            "CREATE TABLE ice.ns.empty_nn (id INT NOT NULL, b INT NOT NULL, "
            "s VARCHAR NOT NULL, flag BOOLEAN NOT NULL, d DATE NOT NULL)"
        ).to_arrow()
        repark.sql("CREATE TABLE ice.ns.l_nn (id INT NOT NULL, v INT NOT NULL)").to_arrow()
        repark.sql("INSERT INTO ice.ns.l_nn VALUES (1, 10), (2, 20)").to_arrow()
        repark.sql("CREATE TABLE ice.ns.r_nn (id INT NOT NULL, b INT NOT NULL)").to_arrow()
        repark.sql("INSERT INTO ice.ns.r_nn VALUES (2, 20)").to_arrow()
        repark.sql("CREATE TABLE ice.ns.dst_nat (id INT NOT NULL, v INT NOT NULL)").to_arrow()

        for cell_id, sql, _ in NATIVE_SQL_BUG_CELLS:
            out["sql"][cell_id] = capture_arrow(lambda q=sql: repark.sql(q).to_arrow())

        repark.sql(
            "CREATE VIEW nv_subq AS "
            "SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn"
        ).to_arrow()
        out["stored"]["native_view_desc"] = capture_arrow(
            lambda: repark.sql("DESCRIBE nv_subq").to_arrow()
        )
        out["stored"]["native_view_where"] = capture_arrow(
            lambda: repark.sql("SELECT id FROM nv_subq WHERE r IS NULL ORDER BY id").to_arrow()
        )
        repark.sql(
            "PREPARE np_p1(INT) AS SELECT id, "
            "coalesce((SELECT b FROM ice.ns.empty_nn), $1) AS c "
            "FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id"
        ).to_arrow()
        out["stored"]["native_prep_param"] = capture_arrow(
            lambda: repark.sql("EXECUTE np_p1(42)").to_arrow()
        )

        repark.sql(
            "INSERT INTO ice.ns.dst_nat SELECT id, v * 10 FROM ice.ns.l_nn "
            "WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL"
        ).to_arrow()
        out["writes"]["insert_where"] = capture_arrow(
            lambda: repark.sql("SELECT id, v FROM ice.ns.dst_nat ORDER BY id").to_arrow()
        )
        repark.sql(
            "CREATE TABLE mem_ctas AS "
            "SELECT id, coalesce((SELECT b FROM ice.ns.empty_nn), 42) AS c "
            "FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL"
        ).to_arrow()
        out["writes"]["ctas_mem"] = capture_arrow(
            lambda: repark.sql("SELECT id, c FROM mem_ctas ORDER BY id").to_arrow()
        )
        try:
            repark.sql(
                "UPDATE ice.ns.l_nn SET v = 99 WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL"
            ).to_arrow()
            out["writes"]["update_refused"] = "OK"
        except AnalysisException:
            out["writes"]["update_refused"] = "AnalysisException"
        except Exception as exc:
            out["writes"]["update_refused"] = type(exc).__name__
    return out


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("scalar_subq_empty_null_1")
    results: dict[str, Any] = {
        "spark_sql": {},
        "spark_ctrl": {},
        "df": {},
        "stored": {},
        "phys": {},
        "explain": {},
        "spark_writes": {},
    }

    session = build_spark_session(root / "wh_read", "scalar-subq-read", "false")
    try:
        seed_spark_tables(session)
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.ansi.enabled", ansi)
            results["spark_sql"][ansi] = {
                cell_id: capture_arrow(lambda q=sql: session.sql(q).toArrow())
                for cell_id, sql, _ in SPARK_SQL_BUG_CELLS
            }
            results["spark_ctrl"][ansi] = {
                cell_id: capture_arrow(lambda q=sql: session.sql(q).toArrow())
                for cell_id, sql, _ in SQL_CONTROL_CELLS
            }
            results["df"][ansi] = measure_df_cells(session)

        session.conf.set("spark.sql.ansi.enabled", "false")
        session.sql(
            "CREATE OR REPLACE TEMP VIEW tv_sql_subq AS "
            "SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn"
        ).collect()
        results["stored"]["spark_tv_sql_select"] = capture_arrow(
            lambda: session.sql(
                "SELECT id, r, r IS NULL AS n, coalesce(r, 42) AS c FROM tv_sql_subq ORDER BY id"
            ).toArrow()
        )
        results["stored"]["spark_tv_sql_where"] = capture_arrow(
            lambda: session.sql("SELECT id FROM tv_sql_subq WHERE r IS NULL ORDER BY id").toArrow()
        )
        session.sql(
            "SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn"
        ).createOrReplaceTempView("tv_df_subq")
        results["stored"]["spark_tv_df_where"] = capture_arrow(
            lambda: session.sql("SELECT id FROM tv_df_subq WHERE r IS NULL ORDER BY id").toArrow()
        )
        session.conf.set("spark.sql.ansi.enabled", "true")
        results["stored"]["spark_tv_df_ansi_flip"] = capture_arrow(
            lambda: session.sql(
                "SELECT id, r IS NULL AS n, coalesce(r, 42) AS c FROM tv_df_subq ORDER BY id"
            ).toArrow()
        )
        session.conf.set("spark.sql.ansi.enabled", "false")

        cached = session.sql(
            "SELECT id, (SELECT b FROM ice.ns.empty_nn) AS r FROM ice.ns.l_nn"
        ).cache()
        try:
            results["stored"]["spark_cached_where"] = capture_arrow(
                lambda: cached.filter("r IS NULL").orderBy("id").toArrow()
            )
        finally:
            cached.unpersist()

        session.sql(
            "PREPARE sp_p1 AS SELECT (SELECT b FROM ice.ns.empty_nn) IS NULL AS r"
        ).collect()
        results["stored"]["spark_prep_select"] = capture_arrow(
            lambda: session.sql("EXECUTE sp_p1").toArrow()
        )
        session.sql(
            "PREPARE sp_p2(INT) AS SELECT id, "
            "coalesce((SELECT b FROM ice.ns.empty_nn), $1) AS c "
            "FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL ORDER BY id"
        ).collect()
        results["stored"]["spark_prep_param"] = capture_arrow(
            lambda: session.sql("EXECUTE sp_p2(42)").toArrow()
        )

        for key, sql in (
            (
                "empty_where",
                "EXPLAIN SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) IS NULL",
            ),
            ("count_star", "EXPLAIN SELECT (SELECT COUNT(*) FROM ice.ns.empty_nn) IS NULL AS r"),
            ("lit_1", "EXPLAIN SELECT (SELECT 1) IS NULL AS r"),
            (
                "outer_elim",
                "EXPLAIN SELECT id FROM ice.ns.l_nn WHERE (SELECT b FROM ice.ns.empty_nn) = 1",
            ),
        ):
            results["explain"][key] = "\n".join(str(row[1]) for row in session.sql(sql).collect())

        session.sql(
            "SET datafusion.optimizer.enable_physical_uncorrelated_scalar_subquery = true"
        ).collect()
        results["phys"] = {
            cell_id: capture_arrow(lambda q=sql: session.sql(q).toArrow())
            for cell_id, sql, _ in PHYS_UNCORR_CELLS
        }
    finally:
        session.stop()
        _reset_active_session_for_tests()

    for ansi in ANSI_MODES:
        results["spark_writes"][ansi] = measure_spark_writes(root / f"wh_wr_{ansi}", ansi)

    native_out = measure_native_all(root / "wh_native")
    results["native_sql"] = native_out["sql"]
    results["stored"].update(native_out["stored"])
    results["native_writes"] = native_out["writes"]

    yield results


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(
    ("cell_id", "sql", "expected"),
    SPARK_SQL_BUG_CELLS,
    ids=[cell[0] for cell in SPARK_SQL_BUG_CELLS],
)
def test_spark_sql_empty_scalar_subquery_cells(
    measured: dict[str, Any],
    ansi: str,
    cell_id: str,
    sql: str,
    expected: list[list[Any]],
) -> None:
    _ = sql
    assert_cell_matches(measured["spark_sql"][ansi][cell_id], expected, f"{cell_id} (ansi={ansi})")


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(
    ("cell_id", "sql", "expected"),
    SQL_CONTROL_CELLS,
    ids=[cell[0] for cell in SQL_CONTROL_CELLS],
)
def test_spark_sql_guaranteed_row_controls(
    measured: dict[str, Any],
    ansi: str,
    cell_id: str,
    sql: str,
    expected: list[list[Any]],
) -> None:
    _ = sql
    assert_cell_matches(measured["spark_ctrl"][ansi][cell_id], expected, f"{cell_id} (ansi={ansi})")


@pytest.mark.parametrize(
    ("cell_id", "sql", "expected"),
    NATIVE_SQL_BUG_CELLS,
    ids=[cell[0] for cell in NATIVE_SQL_BUG_CELLS],
)
def test_native_sql_empty_scalar_subquery_cells(
    measured: dict[str, Any],
    cell_id: str,
    sql: str,
    expected: list[list[Any]],
) -> None:
    _ = sql
    assert_cell_matches(measured["native_sql"][cell_id], expected, f"native:{cell_id}")


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("cell_id", list(DF_EXPECTED.keys()))
def test_dataframe_and_fexpr_doors(
    measured: dict[str, Any],
    ansi: str,
    cell_id: str,
) -> None:
    cell = measured["df"][ansi][cell_id]
    assert_cell_matches(cell, DF_EXPECTED[cell_id], f"df:{cell_id} (ansi={ansi})")
    if cell_id in ("fexpr_withcol_bare", "fexpr_withcol_cast_date_ts"):
        assert cell["nullability"]["r"] is True


@pytest.mark.parametrize("cell_id", list(STORED_EXPECTED.keys()))
def test_stored_plans_views_cache_and_prepared(
    measured: dict[str, Any],
    cell_id: str,
) -> None:
    assert_cell_matches(measured["stored"][cell_id], STORED_EXPECTED[cell_id], cell_id)


def test_native_view_describe_reports_nullable(measured: dict[str, Any]) -> None:
    cell = measured["stored"]["native_view_desc"]
    assert cell["ok"], cell.get("error")
    r_row = next(row for row in cell["rows"] if row[0] == "r")
    assert r_row[2] == "YES"


@pytest.mark.parametrize(
    ("cell_id", "sql", "expected"),
    PHYS_UNCORR_CELLS,
    ids=[cell[0] for cell in PHYS_UNCORR_CELLS],
)
def test_physical_uncorrelated_scalar_subquery_enabled(
    measured: dict[str, Any],
    cell_id: str,
    sql: str,
    expected: list[list[Any]],
) -> None:
    _ = sql
    assert_cell_matches(measured["phys"][cell_id], expected, f"phys:{cell_id}")


def test_plan_quality_and_marker_strip(measured: dict[str, Any]) -> None:
    assert "__repark_scalar_nullable__" not in measured["explain"]["empty_where"]
    assert "Boolean(false)" in measured["explain"]["count_star"]
    assert "Boolean(false)" in measured["explain"]["lit_1"]
    assert "__repark_scalar_nullable__" not in measured["explain"]["outer_elim"]
    assert "Left Join" not in measured["explain"]["outer_elim"]


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_iceberg_hadoop_write_cells_spark_door(
    measured: dict[str, Any],
    ansi: str,
) -> None:
    writes = measured["spark_writes"][ansi]
    assert_cell_matches(writes["insert_where"], [[1, 100], [2, 200]], f"insert (ansi={ansi})")
    assert_cell_matches(writes["ctas_where"], [[1, 42], [2, 42]], f"ctas (ansi={ansi})")
    assert_cell_matches(writes["ctas_bare"], [[1, None], [2, None]], f"ctas_bare (ansi={ansi})")
    assert writes["ctas_bare"]["nullability"]["r"] is True
    assert writes["delete_refused"] == "AnalysisException"
    assert writes["update_refused"] == "AnalysisException"


def test_iceberg_hadoop_write_cells_native_door(measured: dict[str, Any]) -> None:
    writes = measured["native_writes"]
    assert_cell_matches(writes["insert_where"], [[1, 100], [2, 200]], "native_insert")
    assert_cell_matches(writes["ctas_mem"], [[1, 42], [2, 42]], "native_ctas_mem")
    assert writes["update_refused"] == "AnalysisException"
