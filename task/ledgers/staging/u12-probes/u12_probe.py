from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

ENGINE = sys.argv[1]
OUT_PATH = sys.argv[2]
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:5599")
MOTOPY = os.environ.get("U12_MOTOPY", "/tmp/xu-s3w/target/u12/moto-venv/bin/python")
BUCKET = f"u12{ENGINE}"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
APP_RE = re.compile(r"application_\d+_\d+")
JOB_RE = re.compile(r"job_\d+_\d+")
ROWS: list[tuple[int, str]] = [(1, "a"), (2, "b"), (3, "a")]
SEED_ROWS: list[tuple[int, str]] = [(0, "seed")]
COLUMNS: list[str] = ["id", "grp"]
ROWS3: list[tuple[int, str, str]] = [(1, "a", "x"), (2, "b", "y"), (3, "a", "z")]
COLUMNS3: list[str] = ["id", "grp", "val"]
FORMATS: tuple[str, ...] = ("parquet", "csv", "json")
MODES: tuple[str, ...] = ("error", "ignore", "overwrite", "append")


def s3_run(snippet: str) -> Any:
    env = {
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
        "AWS_ENDPOINT_URL": ENDPOINT,
    }
    program = (
        "import boto3, json\n"
        f"s3 = boto3.client('s3', endpoint_url='{ENDPOINT}')\n"
        f"BUCKET = '{BUCKET}'\n" + snippet
    )
    done = subprocess.run([MOTOPY, "-c", program], capture_output=True, text=True, env=env)
    if done.returncode != 0:
        raise RuntimeError(f"s3 helper failed: {done.stderr[-800:]}")
    return json.loads(done.stdout)


def s3_reset_prefix(prefix: str) -> None:
    s3_run(
        "tok = None\n"
        "while True:\n"
        "    kw = {'Bucket': BUCKET, 'Prefix': '" + prefix + "'}\n"
        "    if tok:\n"
        "        kw['ContinuationToken'] = tok\n"
        "    page = s3.list_objects_v2(**kw)\n"
        "    got = page.get('Contents', [])\n"
        "    if got:\n"
        "        s3.delete_objects(Bucket=BUCKET, "
        "Delete={'Objects': [{'Key': o['Key']} for o in got]})\n"
        "    tok = page.get('NextContinuationToken')\n"
        "    if not tok:\n"
        "        break\n"
        "print('null')"
    )


def s3_put(key: str, body: str) -> None:
    s3_run(f"s3.put_object(Bucket=BUCKET, Key='{key}', Body={body!r}.encode())\nprint('null')")


def s3_list(prefix: str) -> list[dict[str, Any]]:
    return s3_run(
        "out = []\n"
        "tok = None\n"
        "while True:\n"
        "    kw = {'Bucket': BUCKET, 'Prefix': '" + prefix + "'}\n"
        "    if tok:\n"
        "        kw['ContinuationToken'] = tok\n"
        "    page = s3.list_objects_v2(**kw)\n"
        "    for o in page.get('Contents', []):\n"
        "        out.append({'key': o['Key'], 'size': o['Size']})\n"
        "    tok = page.get('NextContinuationToken')\n"
        "    if not tok:\n"
        "        break\n"
        "print(json.dumps(out))"
    )


def norm_key(key: str, prefix: str) -> str:
    short = key[len(prefix) :] if key.startswith(prefix) else key
    if short == "":
        return "<DIR-MARKER>"
    short = UUID_RE.sub("<UUID>", short)
    short = APP_RE.sub("<APP>", short)
    return JOB_RE.sub("<JOB>", short)


def size_class(size: int) -> str:
    return "0" if size == 0 else ">0"


def norm_value(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {str(k): norm_value(v) for k, v in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [norm_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): norm_value(v) for k, v in value.items()}
    if value is None or isinstance(value, (int, float, bool, str)):
        return value
    return repr(value)


def err_record(exc: Exception) -> dict[str, Any]:
    lines = str(exc).splitlines()
    record: dict[str, Any] = {
        "status": "error",
        "error": type(exc).__name__,
        "msg": (lines[0] if lines else "")[:600],
    }
    for attr in ("getCondition", "getSqlState"):
        func = getattr(exc, attr, None)
        if callable(func):
            with contextlib.suppress(Exception):
                record[attr] = func()
    if type(exc).__name__ == "Py4JJavaError":
        for line in lines[1:8]:
            if "Exception" in line or "Error" in line:
                record["java"] = line.strip()[:600]
                break
    return record


def spark_session() -> Any:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("u12-probe")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.23.19",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", "testing")
        .config("spark.hadoop.fs.s3a.secret.key", "testing")
        .config("spark.hadoop.fs.s3a.session.token", "testing")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def repark_session() -> Any:
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("u12-probe")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def apply_write(
    session: Any,
    spec: dict[str, Any],
    url: str,
    rows: list[tuple[Any, ...]],
    columns: list[str],
) -> dict[str, Any]:
    try:
        data = session.createDataFrame(rows, columns)
        if spec["empty"]:
            data = data.limit(0)
        writer = data.write.mode(spec["mode"])
        if spec["partition_by"]:
            writer = writer.partitionBy(spec["partition_by"])
        write_call = getattr(writer, spec["format"])
        write_call(url)
        return {"status": "ok"}
    except Exception as exc:
        return err_record(exc)


def session_banner(session: Any) -> dict[str, str]:
    banner: dict[str, str] = {"version": "unknown", "timezone": "unknown"}
    with contextlib.suppress(Exception):
        banner["version"] = str(session.version)
    with contextlib.suppress(Exception):
        banner["timezone"] = str(session.conf.get("spark.sql.session.timeZone"))
    return banner


def read_back(session: Any, spec: dict[str, Any], url: str) -> dict[str, Any]:
    try:
        read_call = getattr(session.read, spec["format"])
        data = read_call(url)
        cols = [[f.name, f.dataType.simpleString()] for f in data.schema.fields]
        rows = sorted([norm_value(list(r)) for r in data.collect()], key=repr)
        return {"status": "ok", "cols": cols, "count": len(rows), "rows": rows}
    except Exception as exc:
        return err_record(exc)


def listing(prefix: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for obj in s3_list(prefix):
        out.append({"key": norm_key(str(obj["key"]), prefix), "size": size_class(obj["size"])})
    return sorted(out, key=lambda item: item["key"])


def build_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for fmt in FORMATS:
        for mode in MODES:
            for pre in ("empty", "existing"):
                cells.append(
                    {
                        "key": f"W-PATH-S3-{fmt}-{mode}-{pre}",
                        "format": fmt,
                        "mode": mode,
                        "pre": pre,
                        "partition_by": [],
                        "empty": False,
                        "scheme": "s3a",
                    }
                )
    cells.append(
        {
            "key": "W-PATH-S3-parquet-partitionby-1col",
            "format": "parquet",
            "mode": "overwrite",
            "pre": "empty",
            "partition_by": ["grp"],
            "empty": False,
            "scheme": "s3a",
        }
    )
    cells.append(
        {
            "key": "W-PATH-S3-parquet-partitionby-2col",
            "format": "parquet",
            "mode": "overwrite",
            "pre": "empty",
            "partition_by": ["grp", "id"],
            "empty": False,
            "scheme": "s3a",
            "frame_rows": ROWS3,
            "frame_columns": COLUMNS3,
        }
    )
    for fmt in ("parquet", "csv"):
        cells.append(
            {
                "key": f"W-PATH-S3-{fmt}-empty-df",
                "format": fmt,
                "mode": "overwrite",
                "pre": "empty",
                "partition_by": [],
                "empty": True,
                "scheme": "s3a",
            }
        )
    for mode in MODES:
        cells.append(
            {
                "key": f"W-PATH-S3-parquet-success-only-{mode}",
                "format": "parquet",
                "mode": mode,
                "pre": "success-only",
                "partition_by": [],
                "empty": False,
                "scheme": "s3a",
            }
        )
    for mode in MODES:
        cells.append(
            {
                "key": f"W-PATH-S3-parquet-foreign-{mode}",
                "format": "parquet",
                "mode": mode,
                "pre": "foreign",
                "partition_by": [],
                "empty": False,
                "scheme": "s3a",
            }
        )
    cells.append(
        {
            "key": "W-PATH-S3-parquet-scheme-s3",
            "format": "parquet",
            "mode": "overwrite",
            "pre": "empty",
            "partition_by": [],
            "empty": False,
            "scheme": "s3",
        }
    )
    cells.append(
        {
            "key": "W-PATH-S3-parquet-scheme-s3a",
            "format": "parquet",
            "mode": "overwrite",
            "pre": "empty",
            "partition_by": [],
            "empty": False,
            "scheme": "s3a",
        }
    )
    return cells


def run_cell(session: Any, spec: dict[str, Any]) -> dict[str, Any]:
    prefix = spec["key"] + "/"
    url = f"{spec['scheme']}://{BUCKET}/{prefix}p"
    record: dict[str, Any] = {
        "key": spec["key"],
        "format": spec["format"],
        "mode": spec["mode"],
        "pre": spec["pre"],
        "partition_by": spec["partition_by"],
        "empty": spec["empty"],
        "scheme": spec["scheme"],
        "url": url,
    }
    s3_reset_prefix(prefix)
    frame_rows = spec.get("frame_rows", ROWS)
    frame_columns = spec.get("frame_columns", COLUMNS)
    record["frame_columns"] = frame_columns
    if spec["pre"] == "existing":
        seed = dict(spec)
        seed["mode"] = "overwrite"
        record["seed"] = apply_write(session, seed, url, SEED_ROWS, COLUMNS)
    elif spec["pre"] == "success-only":
        s3_put(prefix + "p/_SUCCESS", "")
        record["seed"] = {"status": "seeded-success-only"}
    elif spec["pre"] == "foreign":
        s3_put(prefix + "p/foreign.txt", "not-a-part-file")
        record["seed"] = {"status": "seeded-foreign"}
    record["list_before"] = listing(prefix)
    record["write"] = apply_write(session, spec, url, frame_rows, frame_columns)
    record["list_after"] = listing(prefix)
    record["readback"] = read_back(session, spec, url)
    return record


def main() -> None:
    if ENGINE not in ("spark", "repark"):
        raise SystemExit(f"engine must be spark or repark, got {ENGINE!r}")
    s3_run(
        "try:\n    s3.create_bucket(Bucket=BUCKET)\n"
        "except s3.exceptions.BucketAlreadyOwnedByYou:\n    pass\nprint('null')"
    )
    session = spark_session() if ENGINE == "spark" else repark_session()
    out: dict[str, Any] = {
        "engine": ENGINE,
        "endpoint": ENDPOINT,
        "bucket": BUCKET,
        "banner": session_banner(session),
    }
    cells: dict[str, Any] = {}
    out["cells"] = cells
    for spec in build_cells():
        try:
            cells[spec["key"]] = run_cell(session, spec)
        except Exception as exc:
            cells[spec["key"]] = {"key": spec["key"], "fatal": err_record(exc)}
        print(f"cell done: {spec['key']}", flush=True)
    with Path(OUT_PATH).open("w", encoding="utf-8") as handle:
        json.dump(out, handle, indent=1, default=str)
    with contextlib.suppress(Exception):
        session.stop()
    print(f"done cells={len(cells)}", flush=True)


main()
