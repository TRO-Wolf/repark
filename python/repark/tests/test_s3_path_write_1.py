"""Moto pins for the 38 W-PATH-S3-* oracle cells in ``u12-spark.json``.

Each cell replays on ``moto_server``: write outcome, normalised listing, and
read-back rows through the same slashless prefix Spark reads. Behaviour that
diverges from the oracle pins actual repark output; the ledger classifies it:
``R-S3-CSV-HEADER`` (csv header default), ``R-S3-READBACK-NOFILES`` /
``R-S3-READBACK-FOREIGN`` (read-back errors), ``R-S3-PART-NAME`` (part filename
shape, normalised away). Round 2 retired ``R-S3-SLASH-READ``: a slashless S3
prefix with objects under ``<path>/`` reads as a directory.

pins: s3-path-write-1/C-007, C-008, C-009, C-012, C-013

The VU-1..VU-9 verifier pins below replay the ``u12-spark-2.json`` cells: every
S1 pin is red at aa471249 and green after the fold. The RU-1..RU-4 re-verify
pins below are red at b04ee90e and green after the re-verify fold.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException
from repark.spark.dataframe.writer_s3 import is_s3_url

REPO_ROOT = Path(__file__).resolve().parents[3]
MOTOPY = REPO_ROOT / "target" / "u12" / "moto-venv" / "bin" / "python"
ORACLE_PATH = REPO_ROOT / "task" / "ledgers" / "staging" / "u12-probes" / "u12-spark.json"
BUCKET = "u12cell"
ROWS = [(1, "a"), (2, "b"), (3, "a")]
ROWS2 = [(10, "x"), (11, "y")]
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
            frame = reader(url, header=True)
        else:
            frame = reader(url)
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
    assert actual["cols"] == expected["cols"]
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


def test_slashless_prefix_reads_the_written_parts(spark: Any, moto_endpoint: str) -> None:
    """A slashless S3 prefix reads as a directory, the Spark spelling of the round trip."""
    url = f"s3a://{BUCKET}/slashless/p"
    _s3_reset(moto_endpoint, "slashless/p")
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    frame = spark.read.parquet(url)
    assert sorted(map(list, frame.collect())) == sorted(map(list, ROWS))


def test_trailing_slash_prefix_read_still_works(spark: Any, moto_endpoint: str) -> None:
    """The trailing-slash read keeps working beside the slashless spelling."""
    url = f"s3a://{BUCKET}/trailingslash/p"
    _s3_reset(moto_endpoint, "trailingslash/p")
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    slashed = spark.read.parquet(url + "/")
    assert slashed.count() == len(ROWS)


@pytest.mark.parametrize("extension", ["parquet", "csv", "json"])
def test_exact_object_url_reads_one_file(spark: Any, moto_endpoint: str, extension: str) -> None:
    """An exact S3 key with an extension reads as one file, never as a prefix."""
    prefix = f"exact-{extension}/p"
    _s3_reset(moto_endpoint, prefix)
    url = f"s3a://{BUCKET}/{prefix}"
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").format(extension).save(url)
    objects = _s3_list(moto_endpoint, prefix)
    parts = [found["key"] for found in objects if found["key"].endswith(f".{extension}")]
    assert len(parts) == 1
    reader = getattr(spark.read, extension)
    if extension == "csv":
        frame = reader(f"s3a://{BUCKET}/{parts[0]}", header=True)
    else:
        frame = reader(f"s3a://{BUCKET}/{parts[0]}")
    assert frame.count() == len(ROWS)


def test_s3_write_makes_no_local_filesystem_calls(
    spark: Any, moto_endpoint: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S3 writes leave the local directory untouched: no staging, no ``s3:`` shadow."""
    monkeypatch.chdir(tmp_path)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/nolocal/p"
    )
    assert [child.name for child in tmp_path.iterdir()] == []


def _s3_copy(endpoint: str, source: str, dest: str) -> None:
    _boto(
        endpoint,
        "s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, "
        f"'Key': {source!r}}}, Key={dest!r})\nprint('null')",
    )


def _seed_exact_parquet_object(spark: Any, endpoint: str, prefix: str, key: str) -> None:
    staging = f"{prefix}/seed-src"
    _s3_reset(endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/{staging}"
    )
    parts = [
        found["key"] for found in _s3_list(endpoint, staging) if found["key"].endswith(".parquet")
    ]
    assert len(parts) == 1
    _s3_copy(endpoint, parts[0], key)
    _s3_reset(endpoint, staging)


def _exact_keys(endpoint: str, key: str) -> list[str]:
    return [found["key"] for found in _s3_list(endpoint, key)]


@contextlib.contextmanager
def _endpoint_session(endpoint_value: str | None, ssl_value: str | None) -> Any:
    saved = dict(os.environ)
    os.environ.update(_safe_env(""))
    try:
        builder = (
            ReparkSession.builder.appName("u12-s3-path-write-endpoint")
            .config("spark.sql.session.timeZone", "UTC")
            .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("repark.hadoop.fs.s3a.path.style.access", "true")
        )
        if endpoint_value is not None:
            builder = builder.config("repark.hadoop.fs.s3a.endpoint", endpoint_value)
        if ssl_value is not None:
            builder = builder.config("repark.hadoop.fs.s3a.connection.ssl.enabled", ssl_value)
        session = builder.getOrCreate()
        try:
            yield session
        finally:
            session.stop()
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _collect_sorted(frame: Any) -> list[list[Any]]:
    return sorted([[_norm_value(value) for value in row] for row in frame.collect()], key=repr)


@pytest.mark.parametrize("extension", ["parquet", "csv", "json"])
def test_vu1_extension_path_is_a_directory_for_append(
    spark: Any, moto_endpoint: str, extension: str
) -> None:
    """VU-1: overwrite then append on ``out.<ext>`` keeps both parts, 6 rows."""
    prefix = f"vu1-{extension}"
    url = f"s3a://{BUCKET}/{prefix}/out.{extension}"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").format(extension).save(url)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("append").format(extension).save(url)
    keys = _exact_keys(moto_endpoint, f"{prefix}/out.{extension}")
    assert len(keys) > 1
    assert all(key.startswith(f"{prefix}/out.{extension}/") for key in keys)
    reader = getattr(spark.read, extension)
    if extension == "csv":
        assert reader(url, header=True).count() == 2 * len(ROWS)
    else:
        assert reader(url).count() == 2 * len(ROWS)


@pytest.mark.parametrize("mode", ["error", "ignore", "overwrite"])
def test_vu2_exact_key_object_is_visible_to_save_modes(
    spark: Any, moto_endpoint: str, mode: str
) -> None:
    """VU-2: error refuses, ignore no-ops, overwrite removes an exact-key object."""
    prefix = f"vu2-{mode}"
    key = f"{prefix}/solo.parquet"
    url = f"s3a://{BUCKET}/{key}"
    _seed_exact_parquet_object(spark, moto_endpoint, prefix, key)
    before = _exact_keys(moto_endpoint, key)
    assert before == [key]
    if mode == "error":
        with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
            spark.createDataFrame(ROWS2, COLUMNS).write.mode(mode).parquet(url)
        assert _exact_keys(moto_endpoint, key) == [key]
        assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
            spark.createDataFrame(ROWS, COLUMNS)
        )
        return
    if mode == "ignore":
        spark.createDataFrame(ROWS2, COLUMNS).write.mode(mode).parquet(url)
        assert _exact_keys(moto_endpoint, key) == [key]
        assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
            spark.createDataFrame(ROWS, COLUMNS)
        )
        return
    spark.createDataFrame(ROWS2, COLUMNS).write.mode(mode).parquet(url)
    after = _exact_keys(moto_endpoint, key)
    assert key not in after
    assert len(after) > 1
    assert all(name.startswith(f"{key}/") for name in after)
    assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
        spark.createDataFrame(ROWS2, COLUMNS)
    )


def test_vu3_self_overwrite_refuses_before_deleting(spark: Any, moto_endpoint: str) -> None:
    """VU-3: overwrite of a prefix the frame reads from refuses, source intact."""
    prefix = "vu3self"
    url = f"s3a://{BUCKET}/{prefix}/src"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    frame = spark.read.parquet(url)
    grown = frame.withColumn("id", frame["id"] + 100)
    with pytest.raises(AnalysisException, match="UNSUPPORTED_OVERWRITE"):
        grown.write.mode("overwrite").parquet(url)
    assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )


@pytest.mark.parametrize("leaf", ["data#v2", "data?v=2"])
def test_vu4_hash_and_query_keys_stay_literal(spark: Any, moto_endpoint: str, leaf: str) -> None:
    """VU-4: ``#`` and ``?`` keys write beside ``data/`` and never touch it."""
    tag = leaf.replace("?", "q").replace("=", "e").replace("#", "h")
    prefix = f"vu4-{tag}"
    data_url = f"s3a://{BUCKET}/{prefix}/data"
    weird_url = f"s3a://{BUCKET}/{prefix}/{leaf}"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(data_url)
    spark.createDataFrame(ROWS2, COLUMNS).write.mode("overwrite").parquet(weird_url)
    assert _collect_sorted(spark.read.parquet(data_url)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )
    weird_keys = _exact_keys(moto_endpoint, f"{prefix}/{leaf}/")
    assert len(weird_keys) > 1
    assert all(name.startswith(f"{prefix}/{leaf}/") for name in weird_keys)


def test_vu5_bare_host_endpoint_round_trips(moto_endpoint: str) -> None:
    """VU-5: a bare ``host:port`` endpoint takes its scheme from ssl.enabled."""
    bare = moto_endpoint.split("://", 1)[1]
    url = f"s3a://{BUCKET}/vu5bare/p"
    _s3_reset(moto_endpoint, "vu5bare")
    with _endpoint_session(bare, "false") as session:
        session.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
        assert _collect_sorted(session.read.parquet(url)) == _collect_sorted(
            session.createDataFrame(ROWS, COLUMNS)
        )


def test_vu5_bare_host_with_default_ssl_never_panics(moto_endpoint: str) -> None:
    """VU-5: a bare host with default ssl fails loud, never with a panic."""
    bare = moto_endpoint.split("://", 1)[1]
    url = f"s3a://{BUCKET}/vu5bare-ssl/p"
    _s3_reset(moto_endpoint, "vu5bare-ssl")
    with (
        _endpoint_session(bare, None) as session,
        pytest.raises(Exception) as caught,
    ):
        session.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    assert "panic" not in str(caught.value).lower()


def test_vu6_explicit_http_endpoint_needs_no_ssl_key(moto_endpoint: str) -> None:
    """VU-6: an explicit ``http://`` endpoint is honoured without ssl.enabled."""
    url = f"s3a://{BUCKET}/vu6http/p"
    _s3_reset(moto_endpoint, "vu6http")
    with _endpoint_session(moto_endpoint, None) as session:
        session.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
        assert _collect_sorted(session.read.parquet(url)) == _collect_sorted(
            session.createDataFrame(ROWS, COLUMNS)
        )


def test_vu6_boolean_keys_accept_only_true_and_false(moto_endpoint: str) -> None:
    """VU-6: endpoint booleans refuse Hadoop-foreign spellings loud."""
    url = f"s3a://{BUCKET}/vu6bool/p"
    _s3_reset(moto_endpoint, "vu6bool")
    with (
        _endpoint_session(moto_endpoint, "yes") as session,
        pytest.raises(IllegalArgumentException, match="expects a boolean"),
    ):
        session.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)


def test_vu6_read_surfaces_the_store_error(moto_endpoint: str) -> None:
    """VU-6: a dead endpoint fails the read with the store error, not a masked one."""
    closed = _free_port()
    url = f"s3a://{BUCKET}/vu6dead/p"
    with (
        _endpoint_session(f"http://127.0.0.1:{closed}", "false") as session,
        pytest.raises(Exception, match="cannot list S3 prefix") as caught,
    ):
        session.read.parquet(url)
    assert "expected extension" not in str(caught.value)


def test_vu7_text_write_to_s3_refuses_without_local_shadow(
    spark: Any, moto_endpoint: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VU-7: text to ``s3a://`` refuses loud and writes nowhere."""
    monkeypatch.chdir(tmp_path)
    prefix = "vu7refuse"
    url = f"s3a://{BUCKET}/{prefix}/txt"
    _s3_reset(moto_endpoint, prefix)
    with pytest.raises(AnalysisException, match="not supported"):
        spark.createDataFrame([("l1",), ("l2",)], ["line"]).write.mode("overwrite").text(url)
    assert [child.name for child in tmp_path.iterdir()] == []
    assert _exact_keys(moto_endpoint, prefix) == []


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("s3://bucket/p", True),
        ("s3a://bucket/p", True),
        ("S3A://bucket/p", True),
        ("s3:foo", False),
        ("s3:", False),
        ("s3a:b", False),
        (" s3a://bucket/p", False),
        ("s3a://bucket/p ", True),
        ("/tmp/x.parquet", False),
        ("file:///tmp/x.parquet", False),
        ("./s3:foo", False),
    ],
)
def test_vu8_s3_url_spellings(path: str, expected: bool) -> None:
    """VU-8: only ``s3://`` and ``s3a://`` spellings route to S3."""
    assert is_s3_url(path) is expected


def _subquery_frame(spark: Any, shape: str) -> Any:
    """Build the RU-1 IN / EXISTS / scalar frame over two temp views."""
    if shape == "in":
        return spark.sql("select * from ru1base where id in (select id from ru1view)")
    if shape == "exists":
        return spark.sql(
            "select * from ru1base b where exists (select 1 from ru1view s where s.id = b.id)"
        )
    return spark.sql("select * from ru1base where id <= (select max(id) from ru1view)")


@pytest.mark.parametrize("shape", ["in", "exists", "scalar"])
def test_ru1_subquery_self_overwrite_refuses(spark: Any, moto_endpoint: str, shape: str) -> None:
    """RU-1: a subquery read of the destination refuses the overwrite, source intact."""
    prefix = f"ru1-{shape}"
    url = f"s3a://{BUCKET}/{prefix}/dst"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    spark.read.parquet(url).createOrReplaceTempView("ru1view")
    spark.createDataFrame(ROWS, COLUMNS).createOrReplaceTempView("ru1base")
    frame = _subquery_frame(spark, shape)
    grown = frame.withColumn("id", frame["id"] + 100)
    before = _exact_keys(moto_endpoint, f"{prefix}/dst")
    with pytest.raises(AnalysisException, match="UNSUPPORTED_OVERWRITE"):
        grown.write.mode("overwrite").parquet(url)
    assert _exact_keys(moto_endpoint, f"{prefix}/dst") == before
    assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )


def _ru_tag(leaf: str) -> str:
    """Map an RU-2 / RU-4 leaf to a distinct S3-safe prefix tag."""
    return "".join(char if char.isalnum() else "_" for char in leaf)


@pytest.mark.parametrize("leaf", ["a b", "a%20b", "d#v2", "données"])
def test_ru2_encoded_key_self_overwrite_refuses(spark: Any, moto_endpoint: str, leaf: str) -> None:
    """RU-2: self-overwrite of a key needing percent-encoding refuses, source intact."""
    prefix = f"ru2-{_ru_tag(leaf)}"
    url = f"s3a://{BUCKET}/{prefix}/{leaf}"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    frame = spark.read.parquet(url)
    grown = frame.withColumn("id", frame["id"] + 100)
    before = _exact_keys(moto_endpoint, f"{prefix}/{leaf}")
    with pytest.raises(AnalysisException, match="UNSUPPORTED_OVERWRITE"):
        grown.write.mode("overwrite").parquet(url)
    assert _exact_keys(moto_endpoint, f"{prefix}/{leaf}") == before
    assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )


def test_ru2_sibling_prefix_overwrite_passes_through(spark: Any, moto_endpoint: str) -> None:
    """RU-2: reading ``src2`` or ``srcx`` while overwriting ``src`` succeeds."""
    prefix = "ru2pass"
    _s3_reset(moto_endpoint, prefix)
    for leaf in ("src", "src2", "srcx"):
        spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
            f"s3a://{BUCKET}/{prefix}/{leaf}"
        )
    grown_rows = [(row[0] + 100, row[1]) for row in ROWS]
    for leaf in ("src2", "srcx"):
        frame = spark.read.parquet(f"s3a://{BUCKET}/{prefix}/{leaf}")
        frame.withColumn("id", frame["id"] + 100).write.mode("overwrite").parquet(
            f"s3a://{BUCKET}/{prefix}/src"
        )
        assert _collect_sorted(spark.read.parquet(f"s3a://{BUCKET}/{prefix}/src")) == (
            _collect_sorted(spark.createDataFrame(grown_rows, COLUMNS))
        )
        assert _collect_sorted(spark.read.parquet(f"s3a://{BUCKET}/{prefix}/{leaf}")) == (
            _collect_sorted(spark.createDataFrame(ROWS, COLUMNS))
        )


def test_ru2_other_bucket_and_local_source_pass_through(
    spark: Any, moto_endpoint: str, tmp_path: Path
) -> None:
    """RU-2: the same key on another bucket and a local source overwrite cleanly."""
    _boto(moto_endpoint, "s3.create_bucket(Bucket='u12cell2')\nprint('null')")
    other = "s3a://u12cell2/ru2other/src"
    dest = f"s3a://{BUCKET}/ru2other/dst"
    _s3_reset(moto_endpoint, "ru2other")
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(other)
    spark.read.parquet(other).write.mode("overwrite").parquet(dest)
    assert _collect_sorted(spark.read.parquet(dest)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )
    local = tmp_path / "ru2local"
    spark.createDataFrame(ROWS2, COLUMNS).write.mode("overwrite").parquet(str(local))
    spark.read.parquet(str(local)).write.mode("overwrite").parquet(dest)
    assert _collect_sorted(spark.read.parquet(dest)) == _collect_sorted(
        spark.createDataFrame(ROWS2, COLUMNS)
    )


def test_ru3_append_onto_exact_object_refuses(spark: Any, moto_endpoint: str) -> None:
    """RU-3: append onto a destination that is an exact object refuses loud."""
    prefix = "ru3exact"
    key = f"{prefix}/solo.parquet"
    url = f"s3a://{BUCKET}/{key}"
    _seed_exact_parquet_object(spark, moto_endpoint, prefix, key)
    with pytest.raises(AnalysisException, match="cannot append"):
        spark.createDataFrame(ROWS2, COLUMNS).write.mode("append").parquet(url)
    assert _exact_keys(moto_endpoint, key) == [key]
    assert _collect_sorted(spark.read.parquet(url)) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )


@pytest.mark.parametrize("leaf", ["data#v2", "data?v=2", "a%20b"])
def test_ru4_trailing_slash_read_of_encoded_key(spark: Any, moto_endpoint: str, leaf: str) -> None:
    """RU-4: a trailing-slash read of a ``#`` / ``?`` / ``%`` key reads the rows."""
    prefix = f"ru4-{_ru_tag(leaf)}"
    url = f"s3a://{BUCKET}/{prefix}/{leaf}"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(url)
    assert _collect_sorted(spark.read.parquet(url + "/")) == _collect_sorted(
        spark.createDataFrame(ROWS, COLUMNS)
    )


def test_vu9_sibling_prefix_survives_every_mode(spark: Any, moto_endpoint: str) -> None:
    """VU-9: overwrite, error and ignore on ``p`` leave ``p2/`` intact."""
    prefix = "vu9"
    _s3_reset(moto_endpoint, prefix)
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/{prefix}/p2"
    )
    before = _exact_keys(moto_endpoint, f"{prefix}/p2")
    assert len(before) > 1
    spark.createDataFrame(ROWS, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/{prefix}/p"
    )
    spark.createDataFrame(ROWS2, COLUMNS).write.mode("overwrite").parquet(
        f"s3a://{BUCKET}/{prefix}/p"
    )
    with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
        spark.createDataFrame(ROWS2, COLUMNS).write.mode("error").parquet(
            f"s3a://{BUCKET}/{prefix}/p"
        )
    spark.createDataFrame(ROWS2, COLUMNS).write.mode("ignore").parquet(f"s3a://{BUCKET}/{prefix}/p")
    assert _exact_keys(moto_endpoint, f"{prefix}/p2") == before
    assert _collect_sorted(spark.read.parquet(f"s3a://{BUCKET}/{prefix}/p2")) == (
        _collect_sorted(spark.createDataFrame(ROWS, COLUMNS))
    )
