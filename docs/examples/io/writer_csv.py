"""Write CSV files: explicit header arms, separator, and the format plus save spellings."""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriter.csv",
    "DataFrameWriter.format",
    "DataFrameWriter.option",
    "DataFrameWriter.options",
    "DataFrameWriter.save",
]


def data_bytes(root: Path, suffix: str) -> list[str]:
    return sorted(
        path.read_text(encoding="utf-8") for path in root.rglob(f"*{suffix}") if path.is_file()
    )


def main() -> None:
    """Run the measured csv write arms and assert their file bytes."""
    repark = ReparkSession.builder.appName("ex26-writer-csv").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            root = Path(workdir)
            frame = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
            headed = root / "ex26_headed_csv"
            frame.write.mode("overwrite").csv(str(headed), header=True)
            headed_bytes = data_bytes(headed, ".csv")
            headed_expected = ["id,name\n1,a\n2,b\n"]
            if headed_bytes != headed_expected:
                raise SystemExit(f"csv header bytes {headed_bytes!r} != {headed_expected!r}")
            bare = root / "ex26_bare_csv"
            frame.write.mode("overwrite").csv(str(bare), header=False)
            bare_bytes = data_bytes(bare, ".csv")
            bare_expected = ["1,a\n2,b\n"]
            if bare_bytes != bare_expected:
                raise SystemExit(f"csv bare bytes {bare_bytes!r} != {bare_expected!r}")
            plural_bare = root / "ex26_plural_csv"
            frame.write.mode("overwrite").options(header="false").csv(str(plural_bare))
            plural_bytes = data_bytes(plural_bare, ".csv")
            if plural_bytes != bare_expected:
                raise SystemExit(f"options csv bytes {plural_bytes!r} != {bare_expected!r}")
            piped = root / "ex26_piped_csv"
            frame.write.mode("overwrite").format("csv").option("header", "true").option(
                "sep", "|"
            ).save(str(piped))
            piped_bytes = data_bytes(piped, ".csv")
            piped_expected = ["id|name\n1|a\n2|b\n"]
            if piped_bytes != piped_expected:
                raise SystemExit(f"save csv bytes {piped_bytes!r} != {piped_expected!r}")
            plural_piped = root / "ex26_plural_piped_csv"
            frame.write.mode("overwrite").format("csv").options(header="true", sep="|").save(
                str(plural_piped)
            )
            plural_piped_bytes = data_bytes(plural_piped, ".csv")
            if plural_piped_bytes != piped_expected:
                raise SystemExit(f"options save bytes {plural_piped_bytes!r} != {piped_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
