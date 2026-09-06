"""Read CSV and JSON files: defaults, header, null value, and explicit schemas."""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameReader.csv",
    "DataFrameReader.json",
    "DataFrameReader.option",
    "DataFrameReader.schema",
]


def main() -> None:
    """Run the measured csv and json read arms with their schema rows."""
    repark = ReparkSession.builder.appName("ex26-reader-files").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            root = Path(workdir)
            csv_path = root / "ex26_letters.csv"
            csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
            default = repark.read.csv(str(csv_path))
            default_dtypes = [("_c0", "string"), ("_c1", "string")]
            if default.dtypes != default_dtypes:
                raise SystemExit(f"csv dtypes {default.dtypes!r} != {default_dtypes!r}")
            default_rows = [tuple(row) for row in default.collect()]
            default_expected = [("id", "name"), ("1", "a"), ("2", "b"), ("3", "c")]
            if default_rows != default_expected:
                raise SystemExit(f"csv rows {default_rows!r} != {default_expected!r}")
            headed = repark.read.option("header", "true").csv(str(csv_path))
            headed_dtypes = [("id", "string"), ("name", "string")]
            if headed.dtypes != headed_dtypes:
                raise SystemExit(f"csv header dtypes {headed.dtypes!r} != {headed_dtypes!r}")
            headed_rows = [tuple(row) for row in headed.collect()]
            headed_expected = [("1", "a"), ("2", "b"), ("3", "c")]
            if headed_rows != headed_expected:
                raise SystemExit(f"csv header rows {headed_rows!r} != {headed_expected!r}")
            typed = repark.read.schema("id INT, name STRING").csv(str(csv_path), header=True)
            typed_dtypes = [("id", "int"), ("name", "string")]
            if typed.dtypes != typed_dtypes:
                raise SystemExit(f"csv schema dtypes {typed.dtypes!r} != {typed_dtypes!r}")
            typed_rows = [tuple(row) for row in typed.collect()]
            typed_expected = [(1, "a"), (2, "b"), (3, "c")]
            if typed_rows != typed_expected:
                raise SystemExit(f"csv schema rows {typed_rows!r} != {typed_expected!r}")
            nulled = repark.read.csv(str(csv_path), header=True, nullValue="b")
            nulled_rows = [tuple(row) for row in nulled.collect()]
            nulled_expected = [("1", "a"), ("2", None), ("3", "c")]
            if nulled_rows != nulled_expected:
                raise SystemExit(f"csv nullValue rows {nulled_rows!r} != {nulled_expected!r}")
            bare_path = root / "ex26_bare.csv"
            bare_path.write_text("1,a\n2,b\n", encoding="utf-8")
            bare = repark.read.csv(str(bare_path))
            if bare.columns != ["_c0", "_c1"]:
                raise SystemExit(f"csv bare columns {bare.columns!r} != ['_c0', '_c1']")
            bare_rows = [tuple(row) for row in bare.collect()]
            bare_expected = [("1", "a"), ("2", "b")]
            if bare_rows != bare_expected:
                raise SystemExit(f"csv bare rows {bare_rows!r} != {bare_expected!r}")
            json_path = root / "ex26_rows.json"
            json_path.write_text(
                '{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n', encoding="utf-8"
            )
            parsed = repark.read.json(str(json_path))
            parsed_dtypes = [("id", "bigint"), ("name", "string")]
            if parsed.dtypes != parsed_dtypes:
                raise SystemExit(f"json dtypes {parsed.dtypes!r} != {parsed_dtypes!r}")
            parsed_rows = [tuple(row) for row in parsed.collect()]
            parsed_expected = [(1, "a"), (2, "b")]
            if parsed_rows != parsed_expected:
                raise SystemExit(f"json rows {parsed_rows!r} != {parsed_expected!r}")
            shaped = repark.read.schema("id INT, name STRING").json(str(json_path))
            shaped_dtypes = [("id", "int"), ("name", "string")]
            if shaped.dtypes != shaped_dtypes:
                raise SystemExit(f"json schema dtypes {shaped.dtypes!r} != {shaped_dtypes!r}")
            shaped_rows = [tuple(row) for row in shaped.collect()]
            if shaped_rows != parsed_expected:
                raise SystemExit(f"json schema rows {shaped_rows!r} != {parsed_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
