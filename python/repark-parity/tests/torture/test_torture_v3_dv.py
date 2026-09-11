"""Both-door pins for the v3_dv torture family: a Spark-written format-v3 MoR DV table."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from _support import materialize_table

from repark import ReparkSession
from repark_parity.torture.v3_dv import (
    CANONICAL_TABLE_DIR,
    DECLARED_SCHEMA,
    FIXTURE_TABLE_DIR,
    LIVE_ENV,
    NAMESPACE,
    TABLE,
    TRUTH_NAME,
    V3DV_FAMILY,
)

REPARK_CATALOG = "torture_v3dv"
REPARK_LIVE_CATALOG = "torture_v3dv_live"
VIEW_NAME = "torture_v3dv_view"
LIVE = os.environ.get(LIVE_ENV) == "1"
LIVE_SKIP = f"{LIVE_ENV} != 1 — the v3_dv generator needs live Spark (routine CI is JVM-free)"
_SRC_DIR = Path(__file__).resolve().parents[2] / "src"


def _truth(table_root: Path) -> dict[str, Any]:
    """Load the truth record the generator wrote at the table root."""
    return json.loads((table_root / TRUTH_NAME).read_text(encoding="utf-8"))


def _expected_surviving_ids(truth: dict[str, Any]) -> list[int]:
    """Recompute the surviving id list from the truth record's delete rule."""
    written = int(truth["rows_written"])
    delete = truth["delete"]
    if "modulus" in delete:
        modulus = int(delete["modulus"])
        residue = int(delete["residue"])
        deleted = {i for i in range(1, written + 1) if i % modulus == residue}
    else:
        deleted = {int(v) for v in delete["ids"]}
    return [i for i in range(1, written + 1) if i not in deleted]


def _register_fixture_table(
    session: ReparkSession, catalog: str, warehouse: Path, metadata_file: str, table: str
) -> str:
    """Register one materialized table under catalog and return its qualified name."""
    session.register_memory_catalog(catalog, warehouse)
    session.sql(f"CREATE NAMESPACE {catalog}.{NAMESPACE}")
    session.sql(
        f"CALL {catalog}.system.register_table("
        f"table => '{NAMESPACE}.{table}', metadata_file => '{metadata_file}')"
    )
    return f"{catalog}.{NAMESPACE}.{table}"


@pytest.fixture(scope="module")
def v3dv_table(spark: ReparkSession, tmp_path_factory: pytest.TempPathFactory) -> str:
    """The committed fixture materialized at its baked-in location and registered."""
    metadata_file = materialize_table(FIXTURE_TABLE_DIR, CANONICAL_TABLE_DIR)
    warehouse = tmp_path_factory.mktemp("torture-v3dv-warehouse")
    return _register_fixture_table(spark, REPARK_CATALOG, warehouse, metadata_file, TABLE)


def _assert_true_rows(frame: Any, truth: dict[str, Any]) -> None:
    """Assert one door's Arrow output carries exactly the surviving ids and schema."""
    arrow = frame.to_arrow()
    assert arrow.num_rows == int(truth["true_rows"])
    assert arrow.schema.equals(DECLARED_SCHEMA)
    assert sorted(arrow.column("id").to_pylist()) == _expected_surviving_ids(truth)


def test_v3_dv_module_contract() -> None:
    """The v3_dv module exposes the family, the fixture dir, and the canonical location."""
    assert V3DV_FAMILY.name == "v3_dv"
    assert FIXTURE_TABLE_DIR.name == "v3_dv"
    assert CANONICAL_TABLE_DIR.name == TABLE


def test_v3_dv_generate_refuses_without_live_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Without REPARK_PARITY_LIVE=1 the generator refuses loud, naming the flag."""
    monkeypatch.delenv(LIVE_ENV, raising=False)
    with pytest.raises(RuntimeError, match=LIVE_ENV):
        V3DV_FAMILY.generate(rows=8, seed=7, out=tmp_path / "warehouse")


def test_v3_dv_cli_registers_and_refuses(tmp_path: Path) -> None:
    """The committed CLI accepts the family name and refuses loud without the live flag."""
    env = {key: value for key, value in os.environ.items() if key != LIVE_ENV}
    env["PYTHONPATH"] = str(_SRC_DIR)
    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "repark_parity.torture",
            "generate",
            "v3_dv",
            "--rows",
            "8",
            "--seed",
            "7",
            "--out",
            str(tmp_path / "warehouse"),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert run.returncode != 0
    assert LIVE_ENV in run.stderr or LIVE_ENV in run.stdout


def test_v3_dv_fixture_truth_is_self_consistent(v3dv_table: str) -> None:
    """The fixture's truth record agrees with its on-disk layout and canonical location."""
    truth = _truth(CANONICAL_TABLE_DIR)
    assert truth["family"] == "v3_dv"
    assert truth["format_version"] == 3
    assert truth["table_location"] == str(CANONICAL_TABLE_DIR)
    assert int(truth["rows_written"]) - int(truth["rows_deleted"]) == int(truth["true_rows"])
    assert int(truth["delete_files"]) >= 1
    assert int(truth["data_files"]) >= 4
    metadata_rel = truth["metadata_file"]
    assert isinstance(metadata_rel, str)
    assert (CANONICAL_TABLE_DIR / metadata_rel).is_file()
    assert _expected_surviving_ids(truth)


def test_v3_dv_fixture_carries_live_dvs(spark: ReparkSession, v3dv_table: str) -> None:
    """The registered fixture exposes Puffin deletion-vector delete files (content 1)."""
    deletes = spark.sql(f"SELECT content, file_format FROM {v3dv_table}.delete_files").to_arrow()
    rows = deletes.to_pylist()
    assert rows, "no delete files — the fixture lost its DVs"
    assert any(
        int(row["content"]) == 1 and str(row["file_format"]).upper() == "PUFFIN" for row in rows
    ), rows


def test_v3_dv_dataframe_door(spark: ReparkSession, v3dv_table: str) -> None:
    """The DataFrame door reads exactly the post-delete surviving rows under the declared schema."""
    truth = _truth(CANONICAL_TABLE_DIR)
    frame = spark.read.table(v3dv_table)
    _assert_true_rows(frame, truth)


def test_v3_dv_sql_door(spark: ReparkSession, v3dv_table: str) -> None:
    """The spark.sql door over a temp view reads the same surviving rows and schema."""
    truth = _truth(CANONICAL_TABLE_DIR)
    frame = spark.read.table(v3dv_table)
    frame.createOrReplaceTempView(VIEW_NAME)
    table = spark.sql(f"SELECT * FROM {VIEW_NAME}")
    _assert_true_rows(table, truth)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_v3_dv_live_generate_and_read(spark: ReparkSession, tmp_path: Path) -> None:
    """A freshly generated table reads the same truth through repark on both doors."""
    warehouse = tmp_path / "warehouse"
    result = V3DV_FAMILY.generate(rows=120, seed=7, out=warehouse)
    truth = _truth(result.table_root)
    assert int(truth["true_rows"]) == result.rows
    fq_table = _register_fixture_table(
        spark,
        REPARK_LIVE_CATALOG,
        tmp_path / "repark-warehouse",
        str(result.metadata_file),
        "v3dv_live",
    )
    frame = spark.read.table(fq_table)
    _assert_true_rows(frame, truth)
    frame.createOrReplaceTempView(f"{VIEW_NAME}_live")
    sql_table = spark.sql(f"SELECT * FROM {VIEW_NAME}_live")
    _assert_true_rows(sql_table, truth)
    deletes = spark.sql(f"SELECT content, file_format FROM {fq_table}.delete_files").to_arrow()
    assert any(int(row["content"]) == 1 for row in deletes.to_pylist()), (
        "live generate wrote no deletion vectors"
    )
