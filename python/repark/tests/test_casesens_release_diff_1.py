"""RD-1 and RD-2 pins from the v1.5.1 release differential, oracle Spark 4.1.2.

RD-1: under ``caseSensitive=false`` parquet and json path writes refuse a frame
whose columns collide ignoring case (``COLUMN_ALREADY_EXISTS``, nothing
written); csv twin writes keep writing like Spark; orc keeps its declared
refusal; under true nothing twin-related refuses. RD-2: an exact-mode
``F.col`` miss raises ``UNRESOLVED_COLUMN.WITH_SUGGESTION`` with Spark's text;
false-path misses stay byte-identical. Spark's answers live in
``casesens_release_diff_1_spark.json`` beside this file.

Recorded residues: R-RD1-PART-TWIN (partitionBy naming a twin refuses
``COLUMN_ALREADY_EXISTS`` here, ``AMBIGUOUS_REFERENCE`` on Spark),
R-RD1-EACUTE-NAME (the e-acute refusal names ``É`` here, ``é`` on Spark),
R-RD1-PART-DIR (partition dir keeps the frame spelling ``id=1``, Spark writes
``ID=1``), R-RD1-CSV-HEADER (csv twin write without a header option emits an
``a,A`` header here, none on Spark; carded as CSV-HEADER-DEFAULT-1),
R-RD1-ORC (orc twin writes refuse ``NOT_IMPLEMENTED`` here,
``COLUMN_ALREADY_EXISTS`` on Spark; orc is unimplemented),
R-RD2-FALSE-MISS (false-path ``F.col`` misses keep the classless DataFusion
text on both settings by ruling),
R-RD1-S3-TRUE-TWIN (s3a twin parquet under true fails with an internal
temp-table error here, writes on Spark; pre-existing at base),
R-RD1-PART-T-RESPELL (partitionBy a respelled column under true writes here,
Spark refuses legacy ``_LEGACY_ERROR_TEMP_1155``).

pins: casesens-release-diff-1/C-001, C-002, C-003, C-004, C-005
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
from repark.errors import PySparkNotImplementedError
from repark.spark import functions

ORACLE_PATH: Path = Path(__file__).with_name("casesens_release_diff_1_spark.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["cells"]
_REPARK_PREFIX: str = "Error during planning: "
_SUGGESTION_MARK: str = "Did you mean one of the following? ["
MOTOPY: Path = (
    Path(__file__).resolve().parents[3] / "target" / "u12" / "moto-venv" / "bin" / "python"
)
BUCKET: str = "rd1cell"


def _open(warehouse: Path) -> ReparkSession:
    """Open a session with hadoop catalog sc."""
    return (
        ReparkSession.builder.appName("casesens-release-diff-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _setup_t(session: ReparkSession) -> None:
    """Create the sc.ns.t probe table."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.t (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')").collect()


def _condition(error: BaseException) -> Any:
    """Read the attached Spark condition, if the error carries one."""
    method = getattr(error, "getCondition", None)
    if not callable(method):
        return None
    return method()


def _sql_state(error: BaseException) -> Any:
    """Read the attached SQLSTATE, if the error carries one."""
    method = getattr(error, "getSqlState", None)
    if not callable(method):
        return None
    return method()


def _plain(message: str) -> str:
    """Strip engine framings, keeping the comparable first line."""
    return message.splitlines()[0].removeprefix(_REPARK_PREFIX).removesuffix(";")


def _candidates(message: str) -> set[str]:
    """Read the suggestion-list candidate set from a refusal message."""
    _, _, tail = message.partition(_SUGGESTION_MARK)
    head, _, _ = tail.partition("]")
    return {entry.strip() for entry in head.split(",") if entry.strip()}


def _assert_error(key: str, error: BaseException) -> None:
    """Replay one refusal against Spark's recorded answer."""
    spark = _ORACLE[key]
    assert type(error).__name__ == spark["error"], key
    assert _condition(error) == spark["cond"], key
    assert _sql_state(error) == spark["sqlstate"], key
    mine = _plain(str(error))
    want = _plain(spark["msg"])
    assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0], key
    if _SUGGESTION_MARK in want:
        assert _candidates(mine) == _candidates(want), key


def _norm(value: Any) -> Any:
    """Normalize one collected value the way the recording probe did."""
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _norm(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into normalized rows sorted by repr."""
    return sorted([[_norm(value) for value in row] for row in frame.collect()], key=repr)


def _csv_text(path: Path) -> str:
    """Read the single csv part file under a written path."""
    parts = sorted(path.glob("*.csv"))
    assert len(parts) == 1, sorted(item.name for item in path.iterdir())
    return parts[0].read_text()


def _twins(session: ReparkSession) -> Any:
    """Build the a/A twin frame."""
    return session.sql("SELECT 1 AS a, 2 AS A")


def test_twin_parquet_and_json_refuse_and_write_nothing(tmp_path: Path) -> None:
    """Twin parquet/json path writes refuse COLUMN_ALREADY_EXISTS; the dir stays absent."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        for name, write in (
            ("pq", lambda p: _twins(session).write.mode("overwrite").parquet(str(p))),
            ("js", lambda p: _twins(session).write.mode("overwrite").json(str(p))),
        ):
            path = tmp_path / name
            try:
                write(path)
            except Exception as error:
                _assert_error("tw_parquet", error)
            else:
                raise AssertionError(f"{name} wrote a twin frame")
            assert not path.exists(), name
    finally:
        session.stop()


def test_twin_save_with_format_refuses(tmp_path: Path) -> None:
    """Twin save(path) with format parquet/json refuses like the direct door."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        for source in ("parquet", "json"):
            path = tmp_path / f"save-{source}"
            try:
                _twins(session).write.format(source).mode("overwrite").save(str(path))
            except Exception as error:
                _assert_error("tw_parquet", error)
            else:
                raise AssertionError(f"save({source}) wrote a twin frame")
            assert not path.exists(), source
    finally:
        session.stop()


def test_twin_parquet_refuses_before_save_modes(tmp_path: Path) -> None:
    """Twins refuse even in ignore mode on an existing path; the seed is unchanged."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        seed = tmp_path / "seed"
        session.sql("SELECT 1 AS a, 2 AS b").write.mode("overwrite").parquet(str(seed))
        before = sorted(item.name for item in seed.iterdir())
        try:
            _twins(session).write.mode("ignore").parquet(str(seed))
        except Exception as error:
            _assert_error("pq_tw_ignore_exist", error)
        else:
            raise AssertionError("ignore mode wrote a twin frame")
        assert sorted(item.name for item in seed.iterdir()) == before
        assert _rows(session.read.parquet(str(seed))) == [[1, 2]]
    finally:
        session.stop()


def test_twin_frame_with_clean_partition_column_refuses(tmp_path: Path) -> None:
    """A twin frame partitioned by a clean column still refuses on the twins."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        path = tmp_path / "tw3"
        try:
            session.sql("SELECT 1 AS a, 2 AS A, 3 AS p").write.mode("overwrite").partitionBy(
                "p"
            ).parquet(str(path))
        except Exception as error:
            _assert_error("tw3_part_p", error)
        else:
            raise AssertionError("partitioned twin frame wrote")
        assert not path.exists()
    finally:
        session.stop()


def test_twin_csv_writes_with_header_settings(tmp_path: Path) -> None:
    """Twin csv writes keep writing; file bytes and read-back pin the output."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        legs = (
            ("unset", {}, "a,A\n1,2\n", [["1", "2"], ["a", "A"]]),
            ("hdr", {"header": "true"}, "a,A\n1,2\n", [["1", "2"], ["a", "A"]]),
            ("nohdr", {"header": "false"}, "1,2\n", [["1", "2"]]),
        )
        for name, options, text, rows in legs:
            path = tmp_path / f"csv-{name}"
            writer = _twins(session).write.mode("overwrite")
            for key, value in options.items():
                writer = writer.option(key, value)
            writer.csv(str(path))
            assert _csv_text(path) == text, name
            assert _rows(session.read.csv(str(path))) == rows, name
    finally:
        session.stop()


def test_twin_orc_keeps_its_declared_refusal(tmp_path: Path) -> None:
    """Twin orc writes keep refusing NOT_IMPLEMENTED; orc is unimplemented."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        path = tmp_path / "orc"
        with pytest.raises(PySparkNotImplementedError, match="NOT_IMPLEMENTED"):
            _twins(session).write.mode("overwrite").orc(str(path))
        assert not path.exists()
    finally:
        session.stop()


def test_twin_writes_proceed_under_true(tmp_path: Path) -> None:
    """Under true a/A are distinct; twin writes land and read back on every door."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        legs = (
            (
                "pq",
                lambda p: _twins(session).write.mode("overwrite").parquet(str(p)),
                lambda p: session.read.parquet(str(p)),
                {"a": 1, "A": 2},
            ),
            (
                "js",
                lambda p: _twins(session).write.mode("overwrite").json(str(p)),
                lambda p: session.read.json(str(p)),
                {"a": 1, "A": 2},
            ),
            (
                "csv",
                lambda p: _twins(session).write.mode("overwrite").csv(str(p)),
                lambda p: session.read.csv(str(p), header=True),
                {"a": "1", "A": "2"},
            ),
        )
        for name, write, read, want in legs:
            path = tmp_path / name
            write(path)
            got = [
                {key: _norm(value) for key, value in row.asDict().items()}
                for row in read(path).collect()
            ]
            assert got == [want], name
    finally:
        session.stop()


def test_non_ascii_twins_follow_spark(tmp_path: Path) -> None:
    """e-acute twins refuse; sz/SS writes like Spark (R-RD1-EACUTE-NAME)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        acute = tmp_path / "eac"
        try:
            session.createDataFrame([(1, 2)], ["é", "É"]).write.mode("overwrite").parquet(
                str(acute)
            )
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == _ORACLE["uni_eac"]["cond"]
            assert _sql_state(error) == _ORACLE["uni_eac"]["sqlstate"]
            assert _plain(str(error)) == (
                "[COLUMN_ALREADY_EXISTS] The column `É` already exists. Choose another "
                "name or rename the existing column. SQLSTATE: 42711"
            )
        else:
            raise AssertionError("e-acute twins wrote")
        assert not acute.exists()
        ess = tmp_path / "ess"
        session.createDataFrame([(1, 2)], ["ß", "SS"]).write.mode("overwrite").parquet(str(ess))
        assert _rows(session.read.parquet(str(ess))) == [[1, 2]]
    finally:
        session.stop()


def test_plain_frame_writes_every_format(tmp_path: Path) -> None:
    """A frame without twins writes parquet, csv and json."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = session.sql("SELECT 1 AS a, 2 AS b")
        legs = (
            (
                "pq",
                lambda p: frame.write.mode("overwrite").parquet(str(p)),
                lambda p: session.read.parquet(str(p)),
                [[1, 2]],
            ),
            (
                "csv",
                lambda p: frame.write.mode("overwrite").csv(str(p)),
                lambda p: session.read.csv(str(p), header=True),
                [["1", "2"]],
            ),
            (
                "js",
                lambda p: frame.write.mode("overwrite").json(str(p)),
                lambda p: session.read.json(str(p)),
                [[1, 2]],
            ),
        )
        for name, write, read, rows in legs:
            path = tmp_path / name
            write(path)
            assert _rows(read(path)) == rows, name
    finally:
        session.stop()


def test_save_as_table_and_ctas_twins_still_refuse(tmp_path: Path) -> None:
    """Twin saveAsTable and CTAS keep refusing COLUMN_ALREADY_EXISTS."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        _setup_t(session)
        try:
            _twins(session).write.mode("overwrite").saveAsTable("sc.ns.tw_sat")
        except Exception as error:
            _assert_error("sat_twins", error)
        else:
            raise AssertionError("twin saveAsTable wrote")
        try:
            session.sql("CREATE TABLE sc.ns.tw_ctas USING iceberg AS SELECT 1 AS a, 2 AS A")
        except Exception as error:
            _assert_error("ctas_twins", error)
        else:
            raise AssertionError("twin CTAS wrote")
    finally:
        session.stop()


def test_partition_by_naming_a_twin_refuses(tmp_path: Path) -> None:
    """partitionBy naming a twin refuses and writes nothing (R-RD1-PART-TWIN)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        path = tmp_path / "part-tw"
        try:
            _twins(session).write.mode("overwrite").partitionBy("a").parquet(str(path))
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "COLUMN_ALREADY_EXISTS"
            assert _sql_state(error) == "42711"
        else:
            raise AssertionError("twin partitionBy wrote")
        assert not path.exists()
    finally:
        session.stop()


def test_partition_by_respelled_column_writes(tmp_path: Path) -> None:
    """partitionBy a respelled column writes under false; rows read back."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        path = tmp_path / "part-rs"
        session.sql("SELECT 1 AS id, 'x' AS Data").write.mode("overwrite").partitionBy(
            "ID"
        ).parquet(str(path))
        assert sorted(item.name for item in path.iterdir()) == ["id=1"]
        assert _rows(session.read.parquet(str(path))) == [["x"]]
    finally:
        session.stop()


def _moto_port() -> int:
    """Return a free loopback port for one moto server."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _moto_env() -> dict[str, str]:
    """Return the fake-credential environment every S3 touch runs under."""
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
    """Start moto_server with one bucket; stop it at teardown."""
    if not MOTOPY.exists():
        pytest.skip(f"moto venv python is missing at {MOTOPY}")
    port = _moto_port()
    endpoint = f"http://127.0.0.1:{port}"
    server = subprocess.Popen(
        [str(MOTOPY), "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=_moto_env(),
    )
    try:
        deadline = time.time() + 30.0
        while True:
            try:
                _moto_boto(endpoint, "s3.create_bucket(Bucket=BUCKET)\nprint('null')")
                break
            except RuntimeError:
                if time.time() > deadline:
                    raise RuntimeError("moto_server did not start within 30s") from None
                time.sleep(0.2)
        yield endpoint
    finally:
        server.terminate()
        server.wait(timeout=20)


def _moto_boto(endpoint: str, snippet: str) -> Any:
    """Run one boto snippet against moto and return its decoded stdout."""
    program = (
        "import boto3, json\n"
        f"s3 = boto3.client('s3', endpoint_url='{endpoint}')\n"
        f"BUCKET = '{BUCKET}'\n" + snippet
    )
    done = subprocess.run(
        [str(MOTOPY), "-c", program], capture_output=True, text=True, env=_moto_env()
    )
    if done.returncode != 0:
        raise RuntimeError(f"boto helper failed: {done.stderr[-800:]}")
    return json.loads(done.stdout)


def _moto_keys(endpoint: str, prefix: str) -> list[str]:
    """List every object key under one moto prefix."""
    return [
        found["Key"]
        for found in _moto_boto(
            endpoint,
            f"print(json.dumps(s3.list_objects_v2(Bucket=BUCKET, Prefix='{prefix}')"
            ".get('Contents', [])))",
        )
    ]


def test_twin_parquet_refuses_on_s3a(moto_endpoint: str) -> None:
    """Twin s3a parquet refuses COLUMN_ALREADY_EXISTS and stores no objects."""
    saved = dict(os.environ)
    os.environ.update(_moto_env())
    try:
        session = (
            ReparkSession.builder.appName("casesens-release-diff-1-s3")
            .config("spark.sql.session.timeZone", "UTC")
            .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("repark.hadoop.fs.s3a.endpoint", moto_endpoint)
            .config("repark.hadoop.fs.s3a.path.style.access", "true")
            .config("repark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        try:
            session.conf.set("spark.sql.caseSensitive", "false")
            try:
                _twins(session).write.mode("overwrite").parquet(f"s3a://{BUCKET}/tw-pq")
            except Exception as error:
                _assert_error("s3_tw_pq_f", error)
            else:
                raise AssertionError("s3a twin parquet wrote")
            assert _moto_keys(moto_endpoint, "tw-pq") == []
        finally:
            session.stop()
    finally:
        os.environ.clear()
        os.environ.update(saved)


def test_exact_qualified_miss_keeps_unresolved_column(tmp_path: Path) -> None:
    """F.col(t.ID) under true raises UNRESOLVED_COLUMN with Spark's text."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        _setup_t(session)
        try:
            session.sql("SELECT id AS ID, Data AS data FROM sc.ns.t t").select(
                functions.col("t.ID")
            ).collect()
        except Exception as error:
            _assert_error("rd2_qual_t", error)
        else:
            raise AssertionError("qualified miss answered")
    finally:
        session.stop()


def test_exact_bare_miss_keeps_unresolved_column(tmp_path: Path) -> None:
    """F.col(ID) under true raises UNRESOLVED_COLUMN where it misses."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        _setup_t(session)
        try:
            session.sql("SELECT id AS idx, Data AS data FROM sc.ns.t t").select(
                functions.col("ID")
            ).collect()
        except Exception as error:
            _assert_error("rd2_bare_miss_t", error)
        else:
            raise AssertionError("bare miss answered")
        frame = session.sql("SELECT id AS ID, Data AS data FROM sc.ns.t t").select(
            functions.col("ID")
        )
        assert frame.columns == ["ID"]
        assert _rows(frame) == _ORACLE["rd2_bare_ok_t"]["rows"]
    finally:
        session.stop()


def test_exact_string_miss_keeps_unresolved_column(tmp_path: Path) -> None:
    """select(t.ID) under true raises UNRESOLVED_COLUMN like the F.col door."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        _setup_t(session)
        try:
            session.sql("SELECT id AS ID, Data AS data FROM sc.ns.t t").select("t.ID").collect()
        except Exception as error:
            _assert_error("rd2_str_qual_t", error)
        else:
            raise AssertionError("string miss answered")
    finally:
        session.stop()


def test_qualified_name_binds_where_it_matches(tmp_path: Path) -> None:
    """An aliased table binds F.col(t.ID) under false on both engines."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        _setup_t(session)
        frame = session.table("sc.ns.t").alias("t").select(functions.col("t.ID"))
        assert frame.columns == ["ID"]
        assert _rows(frame) == [[1], [2]]
    finally:
        session.stop()


def test_false_path_misses_stay_byte_identical(tmp_path: Path) -> None:
    """False-path F.col misses keep their exact pre-fix text (R-RD2-FALSE-MISS)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        _setup_t(session)
        try:
            session.sql("SELECT id AS ID, Data AS data FROM sc.ns.t t").select(
                functions.col("t.ID")
            ).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) is None
            assert str(error).splitlines()[0] == (
                "Schema error: No field named t.\"ID\". Did you mean 'ID'?."
            )
        else:
            raise AssertionError("false qualified miss answered")
        try:
            session.sql("SELECT id AS idx, Data AS data FROM sc.ns.t t").select(
                functions.col("ID")
            ).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) is None
            assert str(error).splitlines()[0] == (
                'Schema error: No field named "ID". Valid fields are idx, data, t.id, t."Data".'
            )
        else:
            raise AssertionError("false bare miss answered")
    finally:
        session.stop()
