"""Read a messy CSV with the smart reader and inspect the ingest decisions it recorded.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.describe_ingest"]


def main() -> None:
    """Run the measured smartCsv ingest report and the empty report on a plain frame."""
    repark = ReparkSession.builder.appName("ex-df-ingest").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            csv_path = Path(workdir) / "orders.csv"
            csv_path.write_text(
                "id,name,qty\n1,aa,2.5\n2,bb,\n3,cc,7.0\n",
                encoding="utf-8",
            )
            ingested = repark.read.smartCsv(str(csv_path), sep=",")
            report = ingested.describe_ingest()
            assert report["source"] == "smartCsv"
            assert report["delimiter"] == ","
            assert report["data_row_count"] == 3
            assert report["inference_capped"] is False
            columns = {column["name"]: column for column in report["columns"]}
            assert columns["id"]["resolved_type"] == "int32"
            assert columns["name"]["resolved_type"] == "string"
            assert columns["qty"]["resolved_type"] == "decimal128(2,1)"
            assert columns["qty"]["null_count"] == 1

        plain = repark.createDataFrame([(1, "a")], ["n", "label"])
        assert plain.describe_ingest() == {}
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
