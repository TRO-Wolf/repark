"""CTAS-QUERY-FAILURE-CLEANUP-1 — a CTAS whose query fails leaves no metadata.

Every failing cell asserts the statement raises past the begin call, no
metadata json is left, the same-session catalog refuses, and the path read
refuses with RePark's text (CTAS-PATH-READ-TEXT-1 names the Spark text gap).
Fast-path cells also assert no data file is left; F6 options-path cells assert
metadata only, since orphan data is carded separately.

Struck seats, ruling 1 after item 1: the 18 saveAsTable times v3 F seats have
no public channel to request v3 (write option ignored for creation, catalog
table-default refused as reserved, no tableProperty API), so they never enter
the matrix. K4 keeps its saveAsTable times v3 control: a v3 seed plus an
overwrite keeps v3, measured intact.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

TEXT_CARD = "CTAS-PATH-READ-TEXT-1"
CODEC_KEY = "spark.sql.iceberg.compression-codec"
REFUSAL_TEXT = "no Iceberg table found at"
TARGET_KEY = "target-file-size-bytes"

FAIL_QUERIES = {
    "F1": "SELECT CAST('q' AS INT) AS id, 'r' AS v",
    "F3": "SELECT id, 10 / (5 - id) AS q FROM range(1, 6)",
    "F6": "SELECT id, 10 / (199999 - id) AS q FROM range(0, 200000)",
}
FAIL_MARKS = {"F1": "simplify_expressions", "F3": "DIVIDE_BY_ZERO", "F6": "DIVIDE_BY_ZERO"}
CREATE_SPELLINGS = ("sql", "cor", "wcreate", "wcor", "savedef", "saveapp", "saveovr")
REPLACE_SPELLINGS = ("cor", "sqlreplace", "wcor", "wreplace", "saveovr")
KINDS = ("hadoop", "memory")
VERSIONS = ("2", "3")
Q_SEED = "SELECT id, id AS q FROM range(1, 4)"

FAIL_PARAMS: list[pytest.ParamSet] = []
for _row in ("F1", "F3", "F6"):
    for _spelling in CREATE_SPELLINGS:
        for _kind in KINDS:
            for _version in VERSIONS:
                if _spelling.startswith("save") and _version == "3":
                    continue
                FAIL_PARAMS.append(
                    pytest.param(
                        _row,
                        _spelling,
                        _kind,
                        _version,
                        id=f"{_row}-{_spelling}-{_kind}-v{_version}-{TEXT_CARD}",
                    )
                )

STRUCK_SAVE_V3_SEATS: list[str] = [
    f"{row}-{spelling}-{kind}-v3"
    for row in ("F1", "F3", "F6")
    for spelling in ("savedef", "saveapp", "saveovr")
    for kind in KINDS
]

K4_PARAMS: list[pytest.ParamSet] = [
    pytest.param(spelling, kind, version, id=f"K4-{spelling}-{kind}-v{version}")
    for spelling in REPLACE_SPELLINGS
    for kind in KINDS
    for version in VERSIONS
]


@pytest.fixture
def session(tmp_path: Path) -> Any:
    """Yield a session with hadoop and memory catalogs, v3 creates allowed."""
    _reset_active_session_for_tests()
    handle = (
        ReparkSession.builder.appName("test-ctas-query-failure-cleanup-1")
        .config("spark.sql.catalog.hr", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hr.type", "hadoop")
        .config("spark.sql.catalog.hr.warehouse", str(tmp_path / "hadoop"))
        .config("spark.sql.ansi.enabled", "true")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    handle.register_memory_catalog("mm", tmp_path / "memory")
    handle.sql("CREATE NAMESPACE hr.ns")
    handle.sql("CREATE NAMESPACE mm.ns")
    yield handle
    handle.stop()
    _reset_active_session_for_tests()


def catalog_of(kind: str) -> str:
    """Map the cell catalog kind to its session catalog name."""
    return "hr" if kind == "hadoop" else "mm"


def table_dir_of(tmp_path: Path, kind: str, table: str) -> Path:
    """Locate the table directory for the cell catalog kind."""
    return tmp_path / kind / "ns" / table


def metadata_files(table_dir: Path) -> list[Path]:
    """List every metadata json under the table directory."""
    meta = table_dir / "metadata"
    if not meta.is_dir():
        return []
    return sorted(meta.glob("*.metadata.json"))


def data_files(table_dir: Path) -> list[Path]:
    """List every file under the table data directory."""
    data = table_dir / "data"
    if not data.is_dir():
        return []
    return sorted(path for path in data.rglob("*") if path.is_file())


def run_failing_spelling(
    session: Any, catalog: str, table: str, query: str, version: str, spelling: str, row: str
) -> None:
    """Run one failing create spelling; F6 carries the 1 KB target size."""
    target = ", 'write.target-file-size-bytes'='1024'" if row == "F6" else ""
    props = f"TBLPROPERTIES ('format-version'='{version}'{target})"
    if spelling == "sql":
        session.sql(f"CREATE TABLE {catalog}.ns.{table} USING iceberg {props} AS {query}")
        return
    if spelling == "cor":
        session.sql(
            f"CREATE OR REPLACE TABLE {catalog}.ns.{table} USING iceberg {props} AS {query}"
        )
        return
    if spelling in ("wcreate", "wcor"):
        writer = session.sql(query).writeTo(f"{catalog}.ns.{table}").using("iceberg")
        writer.tableProperty("format-version", version)
        if row == "F6":
            writer.option(TARGET_KEY, "1024")
        if spelling == "wcreate":
            writer.create()
        else:
            writer.createOrReplace()
        return
    assert version == "2", "saveAsTable v3 seats are struck by ruling 1"
    writer = session.sql(query).write.format("iceberg")
    if spelling == "saveapp":
        writer = writer.mode("append")
    elif spelling == "saveovr":
        writer = writer.mode("overwrite")
    if row == "F6":
        writer = writer.option(TARGET_KEY, "1024")
    writer.saveAsTable(f"{catalog}.ns.{table}")


def run_replace_spelling(
    session: Any, catalog: str, table: str, query: str, version: str, spelling: str
) -> None:
    """Run one failing replace spelling over a seeded live table."""
    props = f"TBLPROPERTIES ('format-version'='{version}')"
    if spelling == "cor":
        session.sql(
            f"CREATE OR REPLACE TABLE {catalog}.ns.{table} USING iceberg {props} AS {query}"
        )
        return
    if spelling == "sqlreplace":
        session.sql(f"REPLACE TABLE {catalog}.ns.{table} USING iceberg {props} AS {query}")
        return
    if spelling in ("wcor", "wreplace"):
        writer = session.sql(query).writeTo(f"{catalog}.ns.{table}").using("iceberg")
        writer.tableProperty("format-version", version)
        if spelling == "wcor":
            writer.createOrReplace()
        else:
            writer.replace()
        return
    session.sql(query).write.format("iceberg").mode("overwrite").saveAsTable(
        f"{catalog}.ns.{table}"
    )


def assert_catalog_refuses(session: Any, catalog: str, table: str, cell: str) -> None:
    """Assert the same-session catalog read of the target fails as missing."""
    with pytest.raises(Exception, match="not found"):
        session.sql(f"SELECT count(*) AS c FROM {catalog}.ns.{table}").to_arrow()


def assert_path_refuses(session: Any, table_dir: Path, cell: str) -> None:
    """Assert the path read refuses with RePark's existing missing-table text."""
    with pytest.raises(Exception) as caught:
        session.read.format("iceberg").load(str(table_dir)).count()
    assert REFUSAL_TEXT in str(caught.value), f"{cell}: {caught.value}"


@pytest.mark.parametrize(("row", "spelling", "kind", "version"), FAIL_PARAMS)
def test_failing_ctas_leaves_no_metadata(
    session: Any, tmp_path: Path, row: str, spelling: str, kind: str, version: str
) -> None:
    """Failing create cells clean the begin file on every spelling and kind."""
    catalog = catalog_of(kind)
    if row == "F6" and spelling in ("sql", "cor"):
        session.conf.set(CODEC_KEY, "zstd")
    with pytest.raises(Exception, match=FAIL_MARKS[row]):
        run_failing_spelling(session, catalog, "c", FAIL_QUERIES[row], version, spelling, row)
    table_dir = table_dir_of(tmp_path, kind, "c")
    cell = f"{row}-{spelling}-{kind}-v{version}"
    assert metadata_files(table_dir) == [], f"{cell}: {metadata_files(table_dir)}"
    if row in ("F1", "F3"):
        assert data_files(table_dir) == [], f"{cell}: {data_files(table_dir)}"
    assert_catalog_refuses(session, catalog, "c", cell)
    assert_path_refuses(session, table_dir, cell)


@pytest.mark.parametrize(("spelling", "kind", "version"), K4_PARAMS)
def test_failing_replace_keeps_the_live_table(
    session: Any, tmp_path: Path, spelling: str, kind: str, version: str
) -> None:
    """Failing replaces keep the live three rows and every metadata byte."""
    catalog = catalog_of(kind)
    session.sql(f"DROP TABLE IF EXISTS {catalog}.ns.t")
    session.sql(
        f"CREATE TABLE {catalog}.ns.t USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}') AS {Q_SEED}"
    )
    table_dir = table_dir_of(tmp_path, kind, "t")
    before = {path.name: path.read_bytes() for path in metadata_files(table_dir)}
    with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
        run_replace_spelling(session, catalog, "t", FAIL_QUERIES["F3"], version, spelling)
    count = session.sql(f"SELECT count(*) AS c FROM {catalog}.ns.t").to_arrow().to_pylist()
    assert int(count[0]["c"]) == 3, f"K4-{spelling}-{kind}-v{version}"
    after = {path.name: path.read_bytes() for path in metadata_files(table_dir)}
    assert before == after, f"K4-{spelling}-{kind}-v{version}"
    assert len(before) > 0, f"K4-{spelling}-{kind}-v{version}"


def test_struck_save_v3_seats_stay_out_of_the_matrix() -> None:
    """The 18 ruling-1 struck seats never enter the pin matrix."""
    live = {param.id for param in FAIL_PARAMS}
    assert len(STRUCK_SAVE_V3_SEATS) == 18
    assert not (set(STRUCK_SAVE_V3_SEATS) & live)
