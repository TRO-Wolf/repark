from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    assert_glue_scratch_namespace_location,
    assert_real_buckets_configured,
    glue_catalog_config,
    run_table_prefix,
    s3tables_catalog_config,
    s3tables_spark_catalog_config,
)
from _flat_matrix_cells import record_line
from test_aws_acceptance_cross_read import _spark_session_for_catalog

from repark import ReparkSession

SOURCE_ROWS = 100_000
KILL_AFTER_SECONDS = 3.0
PARITY_DIR = Path(__file__).resolve().parent

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason=(
        "real-AWS CTAS-failure harness: set REPARK_AWS_ACCEPTANCE=1 to run "
        "(companion to the AWS acceptance legs — never routine development)"
    ),
)

CHILD_CODE = (
    "import sys, pathlib; sys.path[:0] = [sys.argv[1], str(pathlib.Path(sys.argv[1]).parent)]; "
    "import test_aws_acceptance_ctas_failure as m; m.child_main(sys.argv[2], sys.argv[3])"
)


@dataclass(frozen=True)
class CellSpec:
    cell: str
    failure: str
    control_rows: int | None
    kill: bool


CELLS: tuple[CellSpec, ...] = (
    CellSpec("C1", "planning: CAST('q' AS INT) folded", None, False),
    CellSpec("C2", "execution, first row", None, False),
    CellSpec("C3", "execution, late row", None, False),
    CellSpec("C4", "none (plain create control)", 99, False),
    CellSpec("C5", "execution, first row (replace control)", 3, False),
    CellSpec("C6", "none (successful replace control)", 5, False),
    CellSpec("K", "killed while the query runs", None, True),
)


def _statement(cell: str, src: str, target: str) -> str:
    texts = {
        "C1": f"CREATE TABLE {target} AS SELECT id, CAST('q' AS INT) AS n FROM {src} LIMIT 10",
        "C2": f"CREATE TABLE {target} AS SELECT id, CAST(s AS INT) AS n FROM {src}",
        "C3": f"CREATE TABLE {target} AS SELECT id, CAST(s AS INT) AS n FROM {src} WHERE id > 1",
        "C4": (
            f"CREATE TABLE {target} AS SELECT id, CAST('1' AS INT) AS n FROM {src} "
            "WHERE id BETWEEN 2 AND 100"
        ),
        "C5": f"CREATE OR REPLACE TABLE {target} AS SELECT id, CAST(s AS INT) AS n FROM {src}",
        "C6": (
            f"CREATE OR REPLACE TABLE {target} AS SELECT id, CAST('1' AS INT) AS n FROM {src} "
            "WHERE id BETWEEN 2 AND 6"
        ),
        "K": (
            f"CREATE TABLE {target} AS SELECT a.id, CAST(a.s AS INT) AS n "
            f"FROM {src} a CROSS JOIN {src} b WHERE a.s = '1' AND b.id <= 2000"
        ),
    }
    return texts[cell]


def _kind() -> str:
    kind = os.environ.get("REPARK_CTAS_CATALOG", "glue")
    if kind not in {"glue", "s3tables"}:
        raise ValueError(f"REPARK_CTAS_CATALOG must be glue or s3tables, got {kind!r}")
    return kind


def _catalog_name(kind: str) -> str:
    return SILVER_CATALOG if kind == "glue" else S3TABLES_CATALOG


def _configs(kind: str) -> tuple[dict[str, str], dict[str, str]]:
    if kind == "glue":
        config = glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE)
        return config, config
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip("S3 Tables CTAS-failure cells need TABLE_BUCKET_ARN; absent, so skipped")
    return (
        s3tables_catalog_config(S3TABLES_CATALOG, arn),
        s3tables_spark_catalog_config(S3TABLES_CATALOG, arn),
    )


def _repark(kind: str, config: dict[str, str]) -> ReparkSession:
    builder = ReparkSession.builder.appName(f"ctas-failure-{kind}")
    for key, value in config.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    if kind == "glue":
        assert_glue_scratch_namespace_location(session, GLUE_WAREHOUSE)
    return session


def child_main(kind: str, statement: str) -> None:
    repark_config, _ = _configs(kind)
    session = _repark(kind, repark_config)
    print("STARTED", flush=True)
    session.sql(statement).collect()


def _table_names(kind: str, cell: str) -> tuple[str, str, str]:
    catalog = _catalog_name(kind)
    prefix = run_table_prefix()
    return (
        f"{catalog}.{ACCEPTANCE_NAMESPACE}.{prefix}g5_src",
        f"{catalog}.{ACCEPTANCE_NAMESPACE}.{prefix}g5_{kind}_{cell.lower()}",
        f"{prefix}g5_{kind}_{cell.lower()}",
    )


def _listed(spark: Any, kind: str, short: str) -> bool:
    rows = spark.sql(f"SHOW TABLES IN {_catalog_name(kind)}.{ACCEPTANCE_NAMESPACE}").collect()
    return short in {str(row["tableName"]) for row in rows}


def _read(spark: Any, table: str) -> dict[str, Any]:
    try:
        row = spark.sql(f"SELECT count(*), min(id), max(id) FROM {table}").collect()[0]
        return {"count": int(row[0]), "min": row[1], "max": row[2], "error": None}
    except Exception as failure:
        return {
            "count": None,
            "min": None,
            "max": None,
            "error": f"{type(failure).__name__}: {failure}"[:500],
        }


@pytest.fixture(autouse=True)
def _isolate_active_session() -> Iterator[None]:
    yield


@pytest.fixture(scope="module")
def records_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.getbasetemp() / "ctas_failure.jsonl"


def _record(path: Path, kind: str, cell: str, **fields: Any) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record_line(cell=cell, catalog=kind, **fields) + "\n")


@pytest.fixture(scope="module")
def kind() -> str:
    pytest.importorskip("pyspark")
    assert_real_buckets_configured()
    return _kind()


@pytest.fixture(scope="module")
def sessions(kind: str) -> Any:
    repark_config, spark_config = _configs(kind)
    repark = _repark(kind, repark_config)
    spark = _spark_session_for_catalog(spark_config, f"ctas-failure-{kind}-spark")
    src = _table_names(kind, "src")[0]
    repark.range(1, SOURCE_ROWS + 1).selectExpr(
        "CAST(id AS BIGINT) AS id",
        "CASE WHEN id IN (1, 99999) THEN 'q' ELSE '1' END AS s",
    ).writeTo(src).createOrReplace()
    try:
        yield repark, spark, src
    finally:
        repark.stop()


def _run_kill(kind: str, statement: str) -> int:
    process = subprocess.Popen(
        [sys.executable, "-c", CHILD_CODE, str(PARITY_DIR), kind, statement],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    started = process.stdout.readline().strip()
    if started != "STARTED":
        process.kill()
        process.wait()
        raise AssertionError(f"kill child did not start: {started!r}")
    time.sleep(KILL_AFTER_SECONDS)
    process.kill()
    return process.wait()


@pytest.mark.timeout(1800)
@pytest.mark.parametrize("spec", CELLS, ids=lambda spec: spec.cell)
def test_ctas_failure_cell(spec: CellSpec, kind: str, sessions: Any, records_path: Path) -> None:
    repark, spark, src_fq = sessions
    _, target_fq, target_short = _table_names(kind, spec.cell)
    statement = _statement(spec.cell, src_fq, target_fq)
    replace = spec.cell in {"C5", "C6"}
    if replace:
        repark.sql(f"CREATE TABLE {target_fq} (id BIGINT, n INT) USING iceberg")
        repark.sql(f"INSERT INTO {target_fq} VALUES (1, 1), (2, 2), (3, 3)")
    outcome = "ANSWERED"
    error = None
    return_code = None
    if spec.kill:
        return_code = _run_kill(kind, statement)
        outcome = "KILLED"
    else:
        try:
            repark.sql(statement).collect()
        except Exception as failure:
            outcome = "REFUSED"
            error = f"{type(failure).__name__}: {failure}"[:500]
    listed = _listed(spark, kind, target_short)
    read = _read(spark, target_fq) if listed else None
    _record(
        records_path,
        kind,
        spec.cell,
        failure=spec.failure,
        statement=statement,
        outcome=outcome,
        error=error,
        kill_return_code=return_code,
        listed=listed,
        spark_read=read,
        leftover="not-measured",
    )
    if spec.cell == "C5":
        assert read is not None and read["error"] is None and read["count"] >= spec.control_rows, (
            f"C5 blocks: the live table after a failed replace is unreadable or short: {read}"
        )
        return
    if listed and spec.control_rows is None:
        assert read is not None and read["error"] is None and read["count"] != 0, (
            f"{spec.cell} blocks: listed table reads as empty or unreadable: {read}"
        )
