import gzip
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any

out, engine = Path(sys.argv[1]), sys.argv[2]
BUCKET = "csvwire"
LATE = "".join(f"{index},k\n" for index in range(1, 1201)) + "abc,k\n1.5,k\n"
OBJECTS = {
    "one/a.csv": b"x,k\n007,1e3\n",
    "multi/a.csv": b"x,k\n007,k1\n",
    "multi/b.csv": b"x,k\n1.50,k2\n",
    "two/a.csv": b"x,k\n007,k1\n",
    "two/b.csv": b"k,x\nk2,1e3\nk3,TRUE\n",
    "gz/a.csv.gz": gzip.compress(b"x,k\n007,1e3\n"),
    "late/a.csv": LATE.encode(),
}
ENV = {
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_REGION": "us-east-1",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_EC2_METADATA_DISABLED": "true",
}
LATE_AGG = ("count(*) AS n", "count(x) AS nx", "max(CAST(x AS STRING)) AS mx")


def cells(s: Any, root: str) -> dict[str, Any]:
    return {
        "one/none": lambda: s.read.option("header", "true").csv(f"{root}/one/a.csv"),
        "one/string_double": lambda: (
            s.read.schema("x STRING, k DOUBLE").option("header", "true").csv(f"{root}/one/a.csv")
        ),
        "one/int_string": lambda: (
            s.read.schema("x INT, k STRING").option("header", "true").csv(f"{root}/one/a.csv")
        ),
        "prefix/none": lambda: s.read.option("header", "true").csv(f"{root}/multi/"),
        "prefix/decimal": lambda: (
            s.read.schema("x DECIMAL(10,2), k STRING")
            .option("header", "true")
            .csv(f"{root}/multi/")
        ),
        "glob/none": lambda: s.read.option("header", "true").csv(f"{root}/multi/*.csv"),
        "glob/string": lambda: (
            s.read.schema("x STRING, k STRING").option("header", "true").csv(f"{root}/multi/*.csv")
        ),
        "two/schema": lambda: (
            s.read.schema("x STRING, k STRING").option("header", "true").csv(f"{root}/two/")
        ),
        "two/values": lambda: s.read.option("header", "true").csv(f"{root}/two/").toDF("p", "q"),
        "gzip/none": lambda: (
            s.read.option("header", "true").option("compression", "gzip").csv(f"{root}/gz/")
        ),
        "late/int": lambda: (
            s.read.schema("x INT, k STRING").csv(f"{root}/late/a.csv").selectExpr(*LATE_AGG)
        ),
        "late/string": lambda: (
            s.read.schema("x STRING, k STRING").csv(f"{root}/late/a.csv").selectExpr(*LATE_AGG)
        ),
    }


def record_all(s: Any, root: str) -> None:
    with out.open("w") as handle:
        for ansi in ("true", "false"):
            s.sql(f"SET spark.sql.ansi.enabled={ansi}").collect()
            for tag, build in cells(s, root).items():
                try:
                    frame = build()
                    got = {
                        "names": list(frame.columns),
                        "types": [kind for _, kind in frame.dtypes],
                        "rows": sorted(repr(tuple(row)) for row in frame.collect()),
                    }
                except Exception as error:
                    got = {
                        "error": type(error).__name__ + ": " + " ".join(str(error).split())[:240]
                    }
                handle.write(json.dumps({"ansi": ansi, "tag": tag, **got}) + "\n")


def spark_local() -> None:
    from pyspark.sql import SparkSession

    root = Path(tempfile.mkdtemp(prefix="s3-local-"))
    for key, body in OBJECTS.items():
        (root / key).parent.mkdir(parents=True, exist_ok=True)
        (root / key).write_bytes(body)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    record_all(s, root.as_uri())


def put(endpoint: str, key: str, body: bytes) -> None:
    headers = {"Content-Type": "application/octet-stream"}
    request = urllib.request.Request(f"{endpoint}/{BUCKET}/{key}", body, headers, method="PUT")
    urllib.request.urlopen(request, timeout=10).read()


def repark_moto() -> None:
    import repark._native as native
    from repark import ReparkSession

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    endpoint = f"http://127.0.0.1:{port}"
    command = ["uv", "run", "--offline", "--no-project", "--with", "moto[server]", "python"]
    server = subprocess.Popen(
        [*command, "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, **ENV},
    )
    try:
        deadline = time.time() + 60
        while True:
            try:
                put(endpoint, "", b"")
                break
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.3)
        for key, body in OBJECTS.items():
            put(endpoint, key, body)
        os.environ.update(ENV)
        print("NATIVE", native.__file__, flush=True)
        s = (
            ReparkSession.builder.appName("csv-wiring-s3")
            .config("spark.sql.session.timeZone", "UTC")
            .config("repark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("repark.hadoop.fs.s3a.endpoint", endpoint)
            .config("repark.hadoop.fs.s3a.path.style.access", "true")
            .config("repark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        record_all(s, f"s3://{BUCKET}")
        s.stop()
    finally:
        server.terminate()
        server.wait(timeout=30)


if engine == "spark":
    spark_local()
else:
    repark_moto()
