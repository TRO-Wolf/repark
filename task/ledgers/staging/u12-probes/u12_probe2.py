from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ENGINE = sys.argv[1]
OUT_PATH = sys.argv[2]
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:5599")
MOTOPY = os.environ.get("U12_MOTOPY", "/tmp/xu-s3w/target/u12/moto-venv/bin/python")
BUCKET = f"u12{ENGINE}2"
ROWS3: list[tuple[int, str]] = [(1, "a"), (2, "b"), (3, "a")]
ROWS2: list[tuple[int, str]] = [(10, "x"), (11, "y")]
COLUMNS: list[str] = ["id", "grp"]


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


def s3_put(key: str, body: bytes) -> None:
    s3_run(
        "s3.put_object(Bucket=BUCKET, Key="
        + json.dumps(key)
        + ", Body="
        + repr(body)
        + ")\nprint('null')"
    )


def s3_copy(src: str, dest: str) -> None:
    s3_run(
        "s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': "
        + json.dumps(src)
        + "}, Key="
        + json.dumps(dest)
        + ")\nprint('null')"
    )


def s3_head(key: str) -> Any:
    return s3_run(
        "import botocore\n"
        "try:\n"
        "    head = s3.head_object(Bucket=BUCKET, Key=" + json.dumps(key) + ")\n"
        "    print(json.dumps({'found': True, 'size': head['ContentLength']}))\n"
        "except botocore.exceptions.ClientError as exc:\n"
        "    print(json.dumps({'found': False, 'code': exc.response['Error']['Code']}))"
    )


def s3_list(prefix: str) -> list[dict[str, Any]]:
    return s3_run(
        "out = []\n"
        "tok = None\n"
        "while True:\n"
        "    kw = {'Bucket': BUCKET, 'Prefix': " + json.dumps(prefix) + "}\n"
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
        .appName("u12-probe2")
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
        ReparkSession.builder.appName("u12-probe2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("repark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("repark.hadoop.fs.s3a.path.style.access", "true")
        .config("repark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def listing(prefix: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for obj in s3_list(prefix):
        key = str(obj["key"])
        short = key[len(prefix) :] if key.startswith(prefix) else key
        out.append({"key": short, "size": obj["size"]})
    return sorted(out, key=lambda item: str(item["key"]))


def read_parquet(session: Any, url: str) -> dict[str, Any]:
    try:
        data = session.read.parquet(url)
        cols = [[f.name, f.dataType.simpleString()] for f in data.schema.fields]
        rows = sorted([norm_value(list(r)) for r in data.collect()], key=repr)
        return {"status": "ok", "cols": cols, "count": len(rows), "rows": rows}
    except Exception as exc:
        return err_record(exc)


def write_parquet(session: Any, rows: list[tuple[Any, ...]], mode: str, url: str) -> dict[str, Any]:
    try:
        session.createDataFrame(rows, COLUMNS).write.mode(mode).parquet(url)
        return {"status": "ok"}
    except Exception as exc:
        return err_record(exc)


def cell_ext_append(session: Any, fmt: str) -> dict[str, Any]:
    prefix = f"vu1-{fmt}/"
    url = f"s3a://{BUCKET}/{prefix}t.{fmt}"
    s3_reset_prefix(prefix)
    record: dict[str, Any] = {"url": url}
    try:
        session.createDataFrame(ROWS3, COLUMNS).write.mode("overwrite").format(fmt).save(url)
        record["overwrite"] = {"status": "ok"}
    except Exception as exc:
        record["overwrite"] = err_record(exc)
    record["list_after_overwrite"] = listing(prefix)
    try:
        session.createDataFrame(ROWS2, COLUMNS).write.mode("append").format(fmt).save(url)
        record["append"] = {"status": "ok"}
    except Exception as exc:
        record["append"] = err_record(exc)
    record["list_after_append"] = listing(prefix)
    try:
        reader = getattr(session.read, fmt)
        data = reader(url, header=True) if fmt == "csv" else reader(url)
        cols = [[f.name, f.dataType.simpleString()] for f in data.schema.fields]
        rows = sorted([norm_value(list(r)) for r in data.collect()], key=repr)
        record["readback"] = {"status": "ok", "cols": cols, "count": len(rows), "rows": rows}
    except Exception as exc:
        record["readback"] = err_record(exc)
    return record


def seed_exact_key(session: Any, prefix: str, key: str) -> dict[str, Any]:
    tmp = prefix + "seed-src"
    session.createDataFrame(ROWS3, COLUMNS).write.mode("overwrite").parquet(f"s3a://{BUCKET}/{tmp}")
    parts = [o for o in s3_list(tmp) if o["key"].endswith(".parquet")]
    if not parts:
        raise RuntimeError(f"no parquet part under {tmp}: {s3_list(prefix)}")
    s3_copy(parts[0]["key"], key)
    s3_reset_prefix(tmp)
    return s3_head(key)


def cell_exact_key(session: Any, mode: str) -> dict[str, Any]:
    prefix = f"vu2-{mode}/"
    key = prefix + "solo.parquet"
    url = f"s3a://{BUCKET}/{key}"
    s3_reset_prefix(prefix)
    record: dict[str, Any] = {"url": url}
    record["seed_head"] = seed_exact_key(session, prefix, key)
    try:
        session.createDataFrame(ROWS2, COLUMNS).write.mode(mode).parquet(url)
        record["write"] = {"status": "ok"}
    except Exception as exc:
        record["write"] = err_record(exc)
        record["write_full"] = str(exc)[:4000]
    record["list_after"] = listing(prefix)
    record["readback"] = read_parquet(session, url)
    return record


def cell_self_overwrite(session: Any) -> dict[str, Any]:
    prefix = "vu3/"
    url = f"s3a://{BUCKET}/{prefix}src"
    s3_reset_prefix(prefix)
    record: dict[str, Any] = {"url": url}
    try:
        session.createDataFrame(ROWS3, COLUMNS).write.mode("overwrite").parquet(url)
        record["seed"] = {"status": "ok"}
    except Exception as exc:
        record["seed"] = err_record(exc)
    try:
        frame = session.read.parquet(url)
        grown = frame.withColumn("id", frame["id"] + 100)
        grown.write.mode("overwrite").parquet(url)
        record["write"] = {"status": "ok"}
    except Exception as exc:
        record["write"] = err_record(exc)
        record["write_full"] = str(exc)[:4000]
    record["list_after"] = listing(prefix)
    record["readback"] = read_parquet(session, url)
    return record


def cell_hash_query(session: Any, leaf: str) -> dict[str, Any]:
    prefix = f"vu4-{leaf.replace('?', 'q').replace('=', 'e').replace('#', 'h')}/"
    data_url = f"s3a://{BUCKET}/{prefix}data"
    weird_url = f"s3a://{BUCKET}/{prefix}{leaf}"
    s3_reset_prefix(prefix)
    record: dict[str, Any] = {"data_url": data_url, "weird_url": weird_url}
    try:
        session.createDataFrame(ROWS3, COLUMNS).write.mode("overwrite").parquet(data_url)
        record["seed"] = {"status": "ok"}
    except Exception as exc:
        record["seed"] = err_record(exc)
    try:
        session.createDataFrame(ROWS2, COLUMNS).write.mode("overwrite").parquet(weird_url)
        record["write"] = {"status": "ok"}
    except Exception as exc:
        record["write"] = err_record(exc)
        record["write_full"] = str(exc)[:4000]
    record["list_after"] = listing(prefix)
    record["data_readback"] = read_parquet(session, data_url)
    return record


def cell_text(session: Any) -> dict[str, Any]:
    prefix = "vu7/"
    url = f"s3a://{BUCKET}/{prefix}txt"
    s3_reset_prefix(prefix)
    record: dict[str, Any] = {"url": url}
    try:
        session.createDataFrame([(r,) for r in ["l1", "l2", "l3"]], ["line"]).write.mode(
            "overwrite"
        ).text(url)
        record["write"] = {"status": "ok"}
    except Exception as exc:
        record["write"] = err_record(exc)
        record["write_full"] = str(exc)[:4000]
    record["list_after"] = listing(prefix)
    try:
        data = session.read.text(url)
        cols = [[f.name, f.dataType.simpleString()] for f in data.schema.fields]
        rows = sorted([norm_value(list(r)) for r in data.collect()], key=repr)
        record["readback"] = {"status": "ok", "cols": cols, "count": len(rows), "rows": rows}
    except Exception as exc:
        record["readback"] = err_record(exc)
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
    }
    with contextlib.suppress(Exception):
        out["version"] = str(session.version)
    cells: dict[str, Any] = {}
    out["cells"] = cells
    jobs: list[tuple[str, Any]] = [
        ("VU1-EXT-APPEND-parquet", lambda: cell_ext_append(session, "parquet")),
        ("VU1-EXT-APPEND-csv", lambda: cell_ext_append(session, "csv")),
        ("VU1-EXT-APPEND-json", lambda: cell_ext_append(session, "json")),
        ("VU2-EXACT-KEY-error", lambda: cell_exact_key(session, "error")),
        ("VU2-EXACT-KEY-ignore", lambda: cell_exact_key(session, "ignore")),
        ("VU2-EXACT-KEY-overwrite", lambda: cell_exact_key(session, "overwrite")),
        ("VU3-SELF-OVERWRITE", lambda: cell_self_overwrite(session)),
        ("VU4-HASH", lambda: cell_hash_query(session, "data#v2")),
        ("VU4-QUERY", lambda: cell_hash_query(session, "data?v=2")),
        ("VU7-TEXT", lambda: cell_text(session)),
    ]
    for key, job in jobs:
        try:
            cells[key] = job()
        except Exception as exc:
            cells[key] = {"key": key, "fatal": err_record(exc)}
        print(f"cell done: {key}", flush=True)
    with Path(OUT_PATH).open("w", encoding="utf-8") as handle:
        json.dump(out, handle, indent=1, default=str)
    with contextlib.suppress(Exception):
        session.stop()
    print(f"done cells={len(cells)}", flush=True)


main()
