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
            source = report["source"]
            if source != "smartCsv":
                raise SystemExit(f"DataFrame.describe_ingest source {source!r} != 'smartCsv'")
            delimiter = report["delimiter"]
            if delimiter != ",":
                raise SystemExit(f"DataFrame.describe_ingest delimiter {delimiter!r} != ','")
            data_rows = report["data_row_count"]
            if data_rows != 3:
                raise SystemExit(f"DataFrame.describe_ingest data_row_count {data_rows!r} != 3")
            capped = report["inference_capped"]
            if capped is not False:
                raise SystemExit(f"DataFrame.describe_ingest inference_capped {capped!r} != False")
            columns = {column["name"]: column for column in report["columns"]}
            resolved = {name: column["resolved_type"] for name, column in columns.items()}
            resolved_expected = {"id": "int32", "name": "string", "qty": "decimal128(2,1)"}
            if resolved != resolved_expected:
                raise SystemExit(
                    f"DataFrame.describe_ingest types {resolved!r} != {resolved_expected!r}"
                )
            null_count = columns["qty"]["null_count"]
            if null_count != 1:
                raise SystemExit(f"DataFrame.describe_ingest null_count {null_count!r} != 1")

        plain = repark.createDataFrame([(1, "a")], ["n", "label"])
        plain_report = plain.describe_ingest()
        if plain_report != {}:
            raise SystemExit(f"DataFrame.describe_ingest report {plain_report!r} != {{}}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
