"""TEXT-WRITE-TIMESTAMP-ZONE-1: CSV and JSON writes format TIMESTAMP like Spark.

The oracle is ``text_write_timestamp_zone_1_fixture.json`` (Spark 4.1.2 file
bytes recorded per session zone, plus the error-class token of each refusal
cell). Every bytes cell compares the written part file byte for byte; every
error cell compares the Spark error class or lazy message carried in the
raised message. The ``_divergence`` legs pin loud refusals where Spark
succeeds: zone-name patterns need JRE locale data, so the write refuses with
``INVALID_DATETIME_PATTERN.WITH_SUGGESTION`` instead of guessing bytes.
Residues the pins do not cover: post-2100 DST labels follow the engine-wide
tzdata and stay read-consistent; empty strings stay unquoted in CSV; map keys
stay unformatted; hive-partitioned writes stay refused.

pins: text-write-timestamp-zone-1/C-001 through P-006
"""

from __future__ import annotations

import datetime
import json
import os
import re
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark.session import _reset_active_session_for_tests

_FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("text_write_timestamp_zone_1_fixture.json").read_text(encoding="utf-8")
)
_BYTES_CELLS: list[dict[str, Any]] = [cell for cell in _FIXTURE["cells"] if "bytes" in cell]
_ERROR_CELLS: list[dict[str, Any]] = [
    cell for cell in _FIXTURE["cells"] if "error_token" in cell and "divergence" not in cell
]
_DIVERGENCE_CELLS: list[dict[str, Any]] = [
    cell for cell in _FIXTURE["cells"] if "divergence" in cell
]

_REPO_ROOT: Path = Path(__file__).resolve().parents[3]
_MOTOPY: Path = _REPO_ROOT / "target" / "u12" / "moto-venv" / "bin" / "python"
_BUCKET: str = "text-write-timestamp-zone-1"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Isolated session with a scratch warehouse (no AWS)."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-text-write-timestamp-zone-1").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _part_text(root: Path, suffix: str) -> str:
    """Read the single written part file below a write destination."""
    parts = sorted(root.rglob(f"*{suffix}"))
    assert len(parts) == 1, f"expected one {suffix} part, found {parts}"
    return parts[0].read_text(encoding="utf-8")


def _run_write(spark: ReparkSession, cell: dict[str, Any], dest: Path) -> None:
    """Execute one fixture write with the cell zone and options."""
    spark.conf.set("spark.sql.session.timeZone", cell["zone"])
    writer = spark.sql(cell["sql"]).write.mode("overwrite")
    for key, value in cell["options"].items():
        writer = writer.option(key, value)
    if cell["fmt"] == "csv":
        writer.csv(str(dest))
    else:
        writer.json(str(dest))


@pytest.mark.parametrize("cell", _BYTES_CELLS, ids=[cell["key"] for cell in _BYTES_CELLS])
def test_write_bytes_match_spark(
    spark: ReparkSession, tmp_path: Path, cell: dict[str, Any]
) -> None:
    """Written part bytes equal Spark's recorded bytes for the cell zone."""
    dest = tmp_path / "out"
    _run_write(spark, cell, dest)
    assert _part_text(dest, f".{cell['fmt']}") == cell["bytes"]


@pytest.mark.parametrize("cell", _ERROR_CELLS, ids=[cell["key"] for cell in _ERROR_CELLS])
def test_write_error_carries_spark_token(
    spark: ReparkSession, tmp_path: Path, cell: dict[str, Any]
) -> None:
    """Refused patterns carry Spark's error class or lazy message."""
    with pytest.raises(PySparkException, match=re.escape(cell["error_token"])):
        _run_write(spark, cell, tmp_path / "out")


@pytest.mark.parametrize("cell", _DIVERGENCE_CELLS, ids=[cell["key"] for cell in _DIVERGENCE_CELLS])
def test_zone_name_pattern_refuses_loud_divergence(
    spark: ReparkSession, tmp_path: Path, cell: dict[str, Any]
) -> None:
    """Zone-name patterns refuse loudly; Spark renders them from JRE locale data."""
    with pytest.raises(PySparkException, match=re.escape(cell["error_token"])):
        _run_write(spark, cell, tmp_path / "out")


def test_csv_read_back_returns_original_values(spark: ReparkSession, tmp_path: Path) -> None:
    """A same-zone CSV read-back of a DST-gap write returns the original instant."""
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    dest = tmp_path / "out"
    spark.sql("SELECT 1 AS id, TIMESTAMP '2024-03-10 02:30:00' AS t").write.mode(
        "overwrite"
    ).option("header", "true").csv(str(dest))
    loaded = spark.read.csv(str(dest), header=True, inferSchema=True)
    rows = loaded.orderBy("id").to_arrow().to_pylist()
    assert [row["id"] for row in rows] == [1]
    assert rows[0]["t"] == datetime.datetime(2024, 3, 10, 7, 30, tzinfo=datetime.UTC)


def test_parquet_temporal_round_trip_keeps_types(spark: ReparkSession, tmp_path: Path) -> None:
    """Parquet writes stay binary: temporal columns keep their types."""
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    dest = tmp_path / "out"
    spark.sql(
        "SELECT 1 AS id, TIMESTAMP '2024-03-10 02:30:00' AS t, DATE '2024-06-15' AS d"
    ).write.mode("overwrite").parquet(str(dest))
    loaded = spark.read.parquet(str(dest))
    assert [field.dataType.simpleString() for field in loaded.schema.fields] == [
        "int",
        "timestamp",
        "date",
    ]
    rows = loaded.orderBy("id").to_arrow().to_pylist()
    assert rows[0]["t"] == datetime.datetime(2024, 3, 10, 7, 30, tzinfo=datetime.UTC)
    assert rows[0]["d"] == datetime.date(2024, 6, 15)


def test_json_read_back_keeps_written_string(spark: ReparkSession, tmp_path: Path) -> None:
    """A JSON read-back keeps the written bytes; CAST recovers the instant."""
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    dest = tmp_path / "out"
    spark.sql("SELECT 1 AS id, TIMESTAMP '2024-03-10 02:30:00' AS t").write.mode("overwrite").json(
        str(dest)
    )
    loaded = spark.read.json(str(dest))
    rows = loaded.orderBy("id").to_arrow().to_pylist()
    assert rows[0]["t"] == "2024-03-10T03:30:00.000-04:00"
    cast = loaded.selectExpr("CAST(t AS TIMESTAMP) AS t").to_arrow().to_pylist()
    assert cast[0]["t"] == datetime.datetime(2024, 3, 10, 7, 30, tzinfo=datetime.UTC)


def _free_port() -> int:
    """Return a free loopback port for a test-only moto server."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _safe_env(endpoint: str) -> dict[str, str]:
    """Fake-credential environment for moto and its boto3 clients."""
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": os.environ.get("HOME", "/tmp"),
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ENDPOINT_URL": endpoint,
    }


def _boto(endpoint: str, snippet: str) -> str:
    """Run a boto3 snippet in the moto venv and return its stdout."""
    program = f"import boto3\ns3 = boto3.client('s3', endpoint_url='{endpoint}')\n{snippet}\n"
    completed = subprocess.run(
        [str(_MOTOPY), "-c", program],
        capture_output=True,
        text=True,
        env=_safe_env(endpoint),
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr[-400:])
    return completed.stdout


@pytest.fixture(scope="module")
def moto_endpoint() -> Any:
    """Start moto_server with one bucket; skip the s3a leg without the venv."""
    if not _MOTOPY.exists():
        pytest.skip("moto/boto3 are not installed in target/u12/moto-venv")
    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}"
    server = subprocess.Popen(
        [str(_MOTOPY), "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=_safe_env(endpoint),
    )
    try:
        deadline = time.time() + 30.0
        while True:
            try:
                _boto(endpoint, f"s3.create_bucket(Bucket='{_BUCKET}')\nprint('null')")
                break
            except RuntimeError:
                if time.time() > deadline:
                    raise RuntimeError("moto_server did not start within 30s") from None
                time.sleep(0.2)
        yield endpoint
    finally:
        server.terminate()
        server.wait(timeout=20)


def test_s3a_csv_write_matches_spark(moto_endpoint: str) -> None:
    """The s3a leg writes the DST-gap bytes of w0 America/New_York|csv|gap."""
    saved = dict(os.environ)
    os.environ.update(_safe_env(moto_endpoint))
    _reset_active_session_for_tests()
    try:
        session = (
            ReparkSession.builder.appName("pytest-text-write-ts-s3a")
            .config("spark.sql.session.timeZone", "America/New_York")
            .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("repark.hadoop.fs.s3a.endpoint", moto_endpoint)
            .config("repark.hadoop.fs.s3a.path.style.access", "true")
            .config("repark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        try:
            session.sql("SELECT 1 AS id, TIMESTAMP '2024-03-10 02:30:00' AS t").write.mode(
                "overwrite"
            ).option("header", "true").csv(f"s3a://{_BUCKET}/gap")
        finally:
            session.stop()
    finally:
        os.environ.clear()
        os.environ.update(saved)
        _reset_active_session_for_tests()
    list_snippet = (
        f"print([o['Key'] for o in s3.list_objects_v2(Bucket='{_BUCKET}', "
        "Prefix='gap/').get('Contents', [])])"
    )
    listed = _boto(moto_endpoint, list_snippet)
    keys = [key for key in listed.replace("'", "").strip("[]\n ").split(", ") if key]
    bodies = [
        _boto(
            moto_endpoint,
            f"print(s3.get_object(Bucket='{_BUCKET}', "
            f"Key='{key}')['Body'].read().decode(), end='')",
        )
        for key in keys
        if key.endswith(".csv")
    ]
    assert len(bodies) == 1
    assert bodies[0] == "id,t\n1,2024-03-10T03:30:00.000-04:00\n"
