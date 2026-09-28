"""Moto pins for the 38 W-PATH-S3-* oracle cells in ``u12-spark.json``.

Each cell replays on ``moto_server``: write outcome, normalised listing, and
read-back rows. Behaviour that diverges from the oracle pins actual repark
output; the ledger classifies it: ``R-S3-CSV-HEADER`` (csv header default),
``R-S3-READBACK-NOFILES`` / ``R-S3-READBACK-FOREIGN`` (read-back errors),
``R-S3-SLASH-READ`` (slashless S3 prefix reads refuse, so pins read with a
trailing slash), ``R-S3-PART-NAME`` (part filename shape, normalised away).

pins: s3-path-write-1/C-007, C-008, C-009, C-012, C-013
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

REPO_ROOT = Path(__file__).resolve().parents[3]
MOTOPY = REPO_ROOT / "target" / "u12" / "moto-venv" / "bin" / "python"
ORACLE_PATH = REPO_ROOT / "task" / "ledgers" / "staging" / "u12-probes" / "u12-spark.json"
BUCKET = "u12cell"
ROWS = [(1, "a"), (2, "b"), (3, "a")]
COLUMNS = ["id", "grp"]
ROWS3 = [(1, "a", "x"), (2, "b", "y"), (3, "a", "z")]
COLUMNS3 = ["id", "grp", "val"]
SEED_ROWS = [(0, "seed")]
PART_EXTENSIONS = (".parquet", ".csv", ".json")
LITERAL_LEAVES = frozenset({"_SUCCESS", "foreign.txt"})

if not MOTOPY.exists():
    pytest.skip(
        f"moto venv python is missing at {MOTOPY}; build target/u12/moto-venv first",
        allow_module_level=True,
    )
_probe = subprocess.run(
    [str(MOTOPY), "-c", "import boto3, moto"],
    capture_output=True,
    text=True,
)
if _probe.returncode != 0:
    pytest.skip("moto/boto3 are not installed in target/u12/moto-venv", allow_module_level=True)


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _safe_env(endpoint: str) -> dict[str, str]:
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
    }


@pytest.fixture(scope="module")
def moto_endpoint() -> Any:
    """Start ``moto_server`` on a free port with one bucket; stop it at teardown."""
    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}"
    server = subprocess.Popen(
        [str(MOTOPY), "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=_safe_env(endpoint),
    )
    try:
        deadline = time.time() + 30.0
        while True:
            try:
                _boto(endpoint, "s3.create_bucket(Bucket=BUCKET)\nprint('null')")
                break
            except RuntimeError:
                if time.time() > deadline:
                    raise RuntimeError("moto_server did not start within 30s") from None
                time.sleep(0.2)
        yield endpoint
    finally:
        server.terminate()
        server.wait(timeout=20)


def _boto(endpoint: str, snippet: str) -> Any:
    program = (
        "import boto3, json\n"
        f"s3 = boto3.client('s3', endpoint_url='{endpoint}')\n"
        f"BUCKET = '{BUCKET}'\n" + snippet
    )
    done = subprocess.run(
        [str(MOTOPY), "-c", program], capture_output=True, text=True, env=_safe_env(endpoint)
    )
    if done.returncode != 0:
        raise RuntimeError(f"boto helper failed: {done.stderr[-800:]}")
    return json.loads(done.stdout)


def _s3_list(endpoint: str, prefix: str) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        token_src = "" if token is None else f", ContinuationToken={token!r}"
        page = _boto(
            endpoint,
            f"page = s3.list_objects_v2(Bucket=BUCKET, Prefix={prefix!r}{token_src})\n"
            "print(json.dumps({'keys': ["
            "{'key': o['Key'], 'size': o['Size'], 'modified': o['LastModified'].isoformat()}"
            " for o in page.get('Contents', [])], 'token': page.get('NextContinuationToken')}))",
        )
        found.extend(page["keys"])
        token = page["token"]
        if token is None:
            return found


def _s3_reset(endpoint: str, prefix: str) -> None:
    while True:
        page = _boto(
            endpoint,
            f"page = s3.list_objects_v2(Bucket=BUCKET, Prefix={prefix!r})\n"
            "print(json.dumps([o['Key'] for o in page.get('Contents', [])]))",
        )
        if not page:
            return
        _boto(
            endpoint,
            "s3.delete_objects(Bucket=BUCKET, Delete={'Objects': ["
            f"{{'Key': key}} for key in {page!r}]}})\nprint('null')",
        )


def _s3_put(endpoint: str, key: str, body: str) -> None:
    _boto(
        endpoint,
        f"s3.put_object(Bucket=BUCKET, Key={key!r}, Body={body!r}.encode())\nprint('null')",
    )


def _size_class(size: int) -> str:
    return "0" if size == 0 else ">0"


def _normalise_leaf(leaf: str) -> str:
    if leaf in LITERAL_LEAVES:
        return leaf
    for extension in PART_EXTENSIONS:
        if leaf.endswith(extension):
            return f"part{extension}"
    return leaf


def _normalise_actual(objects: list[dict[str, Any]], prefix: str) -> list[dict[str, str]]:
    rows = []
    for found in sorted(objects, key=lambda row: row["key"]):
        key = found["key"]
        assert key.startswith(prefix + "/"), f"stray key {key} outside {prefix}"
        suffix = key[len(prefix) + 1 :]
        head, _, leaf = suffix.rpartition("/")
        rows.append(
            {
                "key": f"{head + '/' if head else ''}{_normalise_leaf(leaf)}",
                "size": _size_class(found["size"]),
            }
        )
    return sorted(rows, key=lambda row: row["key"])


def _normalise_oracle(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for entry in entries:
        if entry["key"].endswith("<DIR-MARKER>"):
            continue
        suffix = entry["key"][len("p/") :]
        head, _, leaf = suffix.rpartition("/")
        if "<UUID>" in leaf:
            leaf = "part" + leaf[leaf.rindex(".") :]
        normalised = _normalise_leaf(leaf)
        rows.append({"key": f"{head + '/' if head else ''}{normalised}", "size": entry["size"]})
    return sorted(rows, key=lambda row: row["key"])


def _norm_value(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_norm_value(item) for item in value]
    text = str(value)
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _row_dicts(cols: list[str], rows: list[list[Any]]) -> list[str]:
    return sorted(
        repr(sorted(zip(cols, [_norm_value(v) for v in row], strict=True))) for row in rows
    )


@pytest.fixture()
def spark(moto_endpoint: str) -> Any:
    """Build one session against moto; the endpoint keys carry the addressing."""
    saved = dict(os.environ)
    os.environ.update(_safe_env(moto_endpoint))
    try:
        session = (
            ReparkSession.builder.appName("u12-s3-path-write-1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("repark.hadoop.fs.s3a.endpoint", moto_endpoint)
            .config("repark.hadoop.fs.s3a.path.style.access", "true")
            .config("repark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        yield session
        session.stop()
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _oracle_cells() -> dict[str, dict[str, Any]]:
    with ORACLE_PATH.open() as handle:
        return json.load(handle)["cells"]


def _write_frame(spark: Any, cell: dict[str, Any]) -> Any:
    rows, columns = (ROWS3, COLUMNS3) if len(cell["frame_columns"]) == 3 else (ROWS, COLUMNS)
    frame = spark.createDataFrame(rows, columns)
    return frame.limit(0) if cell["empty"] else frame


def _apply_write(spark: Any, url: str, cell: dict[str, Any]) -> None:
    writer = _write_frame(spark, cell).write.mode(cell["mode"])
    if cell["partition_by"]:
        writer = writer.partitionBy(*cell["partition_by"])
    getattr(writer, cell["format"])(url)


def _seed_prefix(spark: Any, endpoint: str, cell: dict[str, Any], prefix: str) -> None:
    pre = cell["pre"]
    if pre == "existing":
        seed = spark.createDataFrame(SEED_ROWS, COLUMNS)
        seed.write.mode("overwrite").format(cell["format"]).save(
            f"{cell['scheme']}://{BUCKET}/{prefix}/p"
        )
    elif pre == "success-only":
        _s3_put(endpoint, f"{prefix}/p/_SUCCESS", "")
    elif pre == "foreign":
        _s3_put(endpoint, f"{prefix}/p/foreign.txt", "not-a-part-file")


def _read_back(spark: Any, url: str, cell: dict[str, Any]) -> dict[str, Any]:
    reader = getattr(spark.read, cell["format"])
    try:
        if cell["format"] == "csv" and cell.get("csv_header_read"):
            frame = reader(url + "/", header=True)
        else:
            frame = reader(url + "/")
        rows = [[_norm_value(value) for value in row] for row in frame.collect()]
        cols = [[field.name, field.dataType.simpleString()] for field in frame.schema.fields]
        return {"status": "ok", "cols": cols, "count": len(rows), "rows": rows}
    except Exception as exc:
        condition = exc.getCondition() if hasattr(exc, "getCondition") else None
        return {
            "status": "error",
            "class": type(exc).__name__,
            "condition": condition,
            "message": str(exc).splitlines()[0][:200] if str(exc) else "",
        }


def _assert_write_outcome(spark: Any, url: str, cell: dict[str, Any]) -> None:
    expected = cell["write"]
    if expected["status"] == "ok":
        _apply_write(spark, url, cell)
        return
    with pytest.raises(AnalysisException) as caught:
        _apply_write(spark, url, cell)
    assert expected["getCondition"] in str(caught.value)


def _assert_listing(endpoint: str, cell: dict[str, Any], prefix: str) -> list[dict[str, Any]]:
    objects = _s3_list(endpoint, f"{prefix}/p")
    if cell["key"] == "W-PATH-S3-csv-empty-df":
        assert _normalise_actual(objects, f"{prefix}/p") == [
            {"key": "_SUCCESS", "size": "0"},
            {"key": "part.csv", "size": ">0"},
        ]
        return objects
    assert _normalise_actual(objects, f"{prefix}/p") == _normalise_oracle(cell["list_after"])
    return objects


def _assert_success_last(objects: list[dict[str, Any]]) -> None:
    markers = [found for found in objects if found["key"].endswith("/_SUCCESS")]
    parts = [found for found in objects if not found["key"].endswith("/_SUCCESS")]
    if not markers or not parts:
        return
    assert markers[0]["modified"] >= max(found["modified"] for found in parts)


def _assert_readback_equal(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    assert actual["status"] == expected["status"] == "ok"
    assert actual["count"] == expected["count"]
    actual_cols = [name for name, _ in actual["cols"]]
    expected_cols = [name for name, _ in expected["cols"]]
    assert _row_dicts(actual_cols, actual["rows"]) == _row_dicts(expected_cols, expected["rows"])


@pytest.mark.parametrize("key", sorted(_oracle_cells()), ids=lambda key: key)
def test_oracle_cell(spark: Any, moto_endpoint: str, key: str) -> None:
    """Replay one oracle cell: write outcome, listing shape, read-back rows."""
    cell = _oracle_cells()[key]
    prefix = key
    _s3_reset(moto_endpoint, f"{prefix}/p")
    _seed_prefix(spark, moto_endpoint, cell, prefix)
    url = f"{cell['scheme']}://{BUCKET}/{prefix}/p"
    _assert_write_outcome(spark, url, cell)
    objects = _assert_listing(moto_endpoint, cell, prefix)
    _assert_success_last(objects)
    if cell["format"] == "csv":
        headed = dict(cell, csv_header_read=True)
        actual = _read_back(spark, url, headed)
        if cell["readback"]["status"] == "ok":
            assert actual["status"] == "ok"
            assert actual["count"] == cell["readback"]["count"]
            assert sorted(map(repr, actual["rows"])) == sorted(map(repr, cell["readback"]["rows"]))
        else:
            assert actual["status"] == "error"
        plain = _read_back(spark, url, cell)
        assert plain["status"] == "ok"
        assert [name for name, _ in plain["cols"]] == ["_c0", "_c1"]
        part_count = sum(1 for found in objects if found["key"].endswith(".csv"))
        assert plain["count"] == cell["readback"]["count"] + part_count
        return
    actual = _read_back(spark, url, cell)
    expected = cell["readback"]
    if cell["partition_by"]:
        assert actual["status"] == "ok", f"partition read-back must succeed: {actual}"
        assert actual["count"] == expected["count"]
        data_columns = [c for c in cell["frame_columns"] if c not in cell["partition_by"]]
        assert [name for name, _ in actual["cols"]] == data_columns
        return
    if key == "W-PATH-S3-parquet-foreign-append":
        assert expected["status"] == "error"
        assert actual["status"] == "ok", f"oracle errors here, repark must read: {actual}"
        assert actual["count"] == len(ROWS)
        assert sorted(map(repr, actual["rows"])) == sorted(map(repr, [list(row) for row in ROWS]))
        return
    if expected["status"] == "error":
        assert actual["status"] == "error", f"oracle errors here: {expected}"
        assert "Cannot infer schema" in actual["message"], actual
        assert actual["class"] == "AnalysisException", actual
        return
    _assert_readback_equal(actual, expected)


def test_slashless_prefix_read_refuses(spark: Any, moto_endpoint: str) -> None:
    """Slashless S3 prefix reads refuse; the pins read with a trailing slash."""
    url = f"s3a://{BUCKET}/slashless/p"
    _s3_reset(moto_endpoint, "slashless/p")
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    with pytest.raises(PySparkException) as caught:
        spark.read.parquet(url).collect()
    assert "does not match the expected extension" in str(caught.value)


def test_s3_write_makes_no_local_filesystem_calls(
    spark: Any, moto_endpoint: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S3 writes leave the local directory untouched: no staging, no ``s3:`` shadow."""
    monkeypatch.chdir(tmp_path)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/nolocal/p"
    )
    assert [child.name for child in tmp_path.iterdir()] == []
