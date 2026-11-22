from __future__ import annotations

import json
import signal
from pathlib import Path
from typing import Any

import _live_parity as lp
import pytest
import test_aws_acceptance_ctas_failure as cf
import test_aws_acceptance_flat_matrix as fm
from _acceptance import ACCEPTANCE_NAMESPACE, RUN_ATTEMPT_ENV
from _flat_matrix_cells import (
    CTAS_SCHEMA,
    DECLARED_STAGE_CREATE,
    OBSERVATION_MISSING,
    SEED_CANONICAL,
    SEED_ROWS,
    STAGE_CREATE_ANSWERED,
    cells_for_group,
    matrix_cells,
    passes,
    universe,
)

from repark import ReparkSession

FM_CATALOG = "fmsmoke"
CF_CATALOG = "cfsmoke"
SPARK_CONF = (("spark.sql.session.timeZone", "UTC"), ("spark.sql.ansi.enabled", "true"))
EXTRA_CELLS = (
    "spark-sql|update|v2-cow|p08",
    "spark-sql|update-set|v2-cow|p08",
    "spark-sql|merge-when|v2-mor|p01",
    "f-expr|merge-into-matched|v2-mor|p01",
    "float|K1-group-by|double",
    "float|W1-merge-on|double",
)
S3TABLES_CELLS = (
    "column-api|create-or-replace|v2-cow|p15",
    "column-api|create-or-replace|v2-cow|p01",
    "spark-sql|update|v2-cow|p15",
)
KILLED = -signal.SIGKILL
CONTROL_READS = cf.CONTROL_READS
HISTORICAL: dict[tuple[str, str], tuple[str, bool, Any, int | None, Any]] = {
    **{
        (kind, cell): ("REFUSED", False, None, None, None)
        for kind in ("glue", "s3tables")
        for cell in ("C1", "C2", "C3")
    },
    ("glue", "C4"): ("ANSWERED", True, CONTROL_READS["C4"], None, None),
    ("s3tables", "C4"): ("ANSWERED", True, CONTROL_READS["C4"], None, None),
    ("glue", "C5"): ("REFUSED", True, CONTROL_READS["C5"], None, None),
    ("s3tables", "C5"): ("REFUSED", True, CONTROL_READS["C5"], None, None),
    ("glue", "C6"): ("ANSWERED", True, CONTROL_READS["C6"], None, None),
    ("s3tables", "C6"): ("ANSWERED", True, CONTROL_READS["C6"], None, None),
    ("glue", "K"): ("KILLED", False, None, KILLED, None),
    ("s3tables", "K"): ("KILLED", True, cf.EMPTY_READ, KILLED, cf.RECOVERED_READ),
}
LOOSENED: tuple[tuple[str, str, tuple[str, bool, Any, int | None, Any]], ...] = (
    (
        "glue",
        "C1",
        ("REFUSED", True, {"count": 10, "min": 1, "max": 10, "error": None}, None, None),
    ),
    ("s3tables", "C2", ("ANSWERED", False, None, None, None)),
    ("glue", "C4", ("REFUSED", True, CONTROL_READS["C4"], None, None)),
    ("glue", "C4", ("ANSWERED", True, {**CONTROL_READS["C4"], "count": 98}, None, None)),
    ("glue", "C5", ("REFUSED", True, {**CONTROL_READS["C5"], "count": 4}, None, None)),
    ("s3tables", "C5", ("REFUSED", True, {**CONTROL_READS["C5"], "min": 2}, None, None)),
    ("glue", "C6", ("ANSWERED", False, None, None, None)),
    ("glue", "K", ("KILLED", True, cf.EMPTY_READ, KILLED, None)),
    ("s3tables", "K", ("KILLED", True, cf.EMPTY_READ, 0, cf.RECOVERED_READ)),
)
EMPTY_SUMMARY = {
    "count": 0,
    "distinct_ids": 0,
    "min_id": None,
    "max_id": None,
    "min_n": None,
    "max_n": None,
    "nonnull_n": 0,
    "error": None,
}


def _spec(cell: str) -> cf.CellSpec:
    return next(spec for spec in cf.CELLS if spec.cell == cell)


def _steps(ctas: dict[str, Any], merge: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "ctas": {"error": None, "seconds": 1.0, "spark_read": ctas},
        "merge": {"error": None, "seconds": 1.0, "spark_read": merge},
    }


@pytest.mark.parametrize(("kind", "cell"), sorted(HISTORICAL), ids=lambda value: str(value))
def test_g5_passes_every_historical_observation(kind: str, cell: str) -> None:
    outcome, listed, read, return_code, recovery = HISTORICAL[(kind, cell)]
    assert cf.cell_failures(_spec(cell), kind, outcome, listed, read, return_code, recovery) == []


@pytest.mark.parametrize(("kind", "cell", "observed"), LOOSENED, ids=lambda value: str(value))
def test_g5_fails_an_observation_looser_than_the_historical_one(
    kind: str, cell: str, observed: tuple[str, bool, Any, int | None, Any]
) -> None:
    outcome, listed, read, return_code, recovery = observed
    assert cf.cell_failures(_spec(cell), kind, outcome, listed, read, return_code, recovery)


def test_g5_retry_passes_the_historical_observations_and_fails_looser_ones() -> None:
    expected = cf.retry_expected()
    assert cf.retry_failures("glue", KILLED, False, _steps(expected, expected)) == []
    assert cf.retry_failures("s3tables", KILLED, True, _steps(EMPTY_SUMMARY, expected)) == []
    assert cf.retry_failures("glue", KILLED, True, _steps(expected, expected))
    assert cf.retry_failures("glue", KILLED, False, _steps(EMPTY_SUMMARY, expected))
    assert cf.retry_failures("glue", 0, False, _steps(expected, expected))
    short = {**expected, "nonnull_n": cf.SOURCE_ROWS - 1}
    assert cf.retry_failures("s3tables", KILLED, True, _steps(EMPTY_SUMMARY, short))
    raised = _steps(expected, expected)
    raised["merge"]["error"] = "RuntimeError: boom"
    assert cf.retry_failures("glue", KILLED, False, raised)


def _repark(catalog: str, warehouse: Path) -> ReparkSession:
    builder = ReparkSession.builder.appName(f"{catalog}-repark")
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config(
        f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog"
    )
    builder = builder.config(f"spark.sql.catalog.{catalog}.type", "hadoop")
    builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    return builder.getOrCreate()


def _records(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return {(row["catalog"], row["cell"]): row for row in rows}


def _smoke_cells() -> list[Any]:
    by_id = {cell.cell_id: cell for cell in universe()}
    return list(cells_for_group("p15")) + [by_id[cell_id] for cell_id in EXTRA_CELLS]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_g6_observer_runs_both_halves_on_a_local_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyspark")
    warehouse = tmp_path / "warehouse"
    engine = lp.build_spark_iceberg_engine(warehouse, catalog=FM_CATALOG)
    records = tmp_path / "flat_matrix.jsonl"
    labels: dict[tuple[str, str], str] = {}
    by_id = {cell.cell_id: cell for cell in matrix_cells()}
    with lp.spark_session_conf(engine, SPARK_CONF):
        spark = engine.session
        repark = _repark(FM_CATALOG, warehouse)
        try:
            repark.sql(f"CREATE NAMESPACE IF NOT EXISTS {FM_CATALOG}.{ACCEPTANCE_NAMESPACE}")
            for cell in _smoke_cells():
                labels[("glue", cell.cell_id)] = fm.run_cell(
                    cell, "glue", FM_CATALOG, repark, spark, records, "smoke"
                )
            monkeypatch.setenv(RUN_ATTEMPT_ENV, "1")
            for cell_id in S3TABLES_CELLS:
                labels[("s3tables", cell_id)] = fm.run_cell(
                    by_id[cell_id], "s3tables", FM_CATALOG, repark, spark, records, "smoke"
                )
            spark_ctas_schema = _spark_ctas_schema(spark)
        finally:
            repark.stop()
    found = _records(records)
    print(json.dumps({f"{kind}|{cell}": label for (kind, cell), label in labels.items()}))
    assert set(found) == set(labels)
    assert spark_ctas_schema == CTAS_SCHEMA
    select = found[("glue", "spark-sql|select|v2-cow|p15")]
    assert [tuple(row) for row in select["repark_state"]] == sorted(SEED_CANONICAL, key=repr)
    for (kind, cell_id), row in found.items():
        assert OBSERVATION_MISSING not in row["label"], (kind, cell_id, row)
        assert row["repark_files_error"] is None, (kind, cell_id, row["repark_files_error"])
        if cell_id.startswith("float|"):
            continue
        assert row["repark_state"] is not None and row["repark_schema"] is not None, cell_id
        if row["expected_source"] == "live-spark-oracle" and row["oracle_outcome"] == "ANSWERED":
            assert row["oracle_state"] is not None and row["oracle_files_error"] is None, cell_id
    for kind, cell_id in (("glue", c.cell_id) for c in cells_for_group("p15")):
        assert passes(labels[(kind, cell_id)]), (cell_id, labels[(kind, cell_id)])
    for cell_id in S3TABLES_CELLS[:2]:
        row = found[("s3tables", cell_id)]
        assert row["label"] == STAGE_CREATE_ANSWERED, row["label"]
        assert row["stage_create"]["readback_equal"] is True, row["stage_create"]
        assert row["stage_create"]["exact_refusal"] is False
    assert DECLARED_STAGE_CREATE not in " ".join(labels.values())


def _spark_ctas_schema(spark: Any) -> Any:
    source = f"{FM_CATALOG}.{ACCEPTANCE_NAMESPACE}.spark_ctas_source"
    target = f"{FM_CATALOG}.{ACCEPTANCE_NAMESPACE}.spark_ctas_target"
    fm._create_matrix_table(spark, source, "v2-cow")
    spark.sql(f"INSERT INTO {source} VALUES " + ", ".join(SEED_ROWS))
    spark.sql(f"CREATE TABLE {target} USING iceberg AS SELECT * FROM {source} WHERE d = 0.0")
    return fm._spark_state(spark, target)[1]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_g5_cells_run_both_halves_on_a_local_catalog(tmp_path: Path) -> None:
    pytest.importorskip("pyspark")
    warehouse = tmp_path / "warehouse"
    engine = lp.build_spark_iceberg_engine(warehouse, catalog=CF_CATALOG)
    records = tmp_path / "ctas_failure.jsonl"
    failures: dict[str, list[str]] = {}
    with lp.spark_session_conf(engine, SPARK_CONF):
        spark = engine.session
        repark = _repark(CF_CATALOG, warehouse)
        try:
            repark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CF_CATALOG}.{ACCEPTANCE_NAMESPACE}")
            src = cf._table_names("glue", "src", CF_CATALOG)[0]
            cf.seed_source(repark, src)
            for spec in (spec for spec in cf.CELLS if not spec.kill):
                failures[spec.cell] = cf.run_ctas_cell(
                    spec, "glue", CF_CATALOG, (repark, spark, src), records
                )
        finally:
            repark.stop()
    lines = records.read_text(encoding="utf-8").splitlines()
    print("\n".join(lines))
    assert [json.loads(line)["cell"] for line in lines] == list(failures)
    assert failures == {cell: [] for cell in failures}
