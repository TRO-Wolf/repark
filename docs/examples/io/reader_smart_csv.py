"""Smart CSV ingest of messy files; a repark extension with no Spark analog."""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameReader.smartCsv",
]


def main() -> None:
    """Run the measured messy-file and explicit-header smartCsv arms."""
    repark = ReparkSession.builder.appName("ex26-smart-csv").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            root = Path(workdir)
            messy_path = root / "ex26_messy.csv"
            messy_path.write_text("report generated monday\nid,name\n1,a\n2,b\n", encoding="utf-8")
            messy = repark.read.smartCsv(str(messy_path))
            if messy.columns != ["id", "name"]:
                raise SystemExit(f"smartCsv columns {messy.columns!r} != ['id', 'name']")
            messy_dtypes = [("id", "int"), ("name", "string")]
            if messy.dtypes != messy_dtypes:
                raise SystemExit(f"smartCsv dtypes {messy.dtypes!r} != {messy_dtypes!r}")
            messy_rows = [tuple(row) for row in messy.collect()]
            messy_expected = [(1, "a"), (2, "b")]
            if messy_rows != messy_expected:
                raise SystemExit(f"smartCsv rows {messy_rows!r} != {messy_expected!r}")
            clean_path = root / "ex26_smart_clean.csv"
            clean_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
            clean = repark.read.smartCsv(str(clean_path), header=True)
            clean_rows = [tuple(row) for row in clean.collect()]
            clean_expected = [(1, "a"), (2, "b"), (3, "c")]
            if clean_rows != clean_expected:
                raise SystemExit(f"smartCsv header rows {clean_rows!r} != {clean_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
