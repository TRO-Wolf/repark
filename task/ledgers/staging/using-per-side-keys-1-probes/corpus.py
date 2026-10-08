from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "python" / "repark" / "tests"))

import _using_corpus as corpus

ENGINE = sys.argv[1]
OUT = sys.argv[2]

if ENGINE == "spark":
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("OFF")
else:
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("corpus")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", tempfile.mkdtemp(prefix="corpus-") + "/sc")
        .getOrCreate()
    )

corpus.load(session)
answers = {name: corpus.answer(session, text) for name, text in corpus.statements()}
lines = ",\n".join(
    f"{json.dumps(name)}: {json.dumps(answers[name], sort_keys=True)}" for name in sorted(answers)
)
Path(OUT).write_text("{\n" + lines + "\n}\n")
print(len(answers), "statements")
session.stop()
