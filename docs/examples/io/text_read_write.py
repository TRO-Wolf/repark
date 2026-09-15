"""Text read and write: one string column, null as an empty line. pins: io-text-1/C-003"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameReader.text",
    "DataFrameWriter.text",
]


def data_bytes(root: Path, suffix: str) -> list[str]:
    return sorted(
        path.read_text(encoding="utf-8") for path in root.rglob(f"*{suffix}") if path.is_file()
    )


def main() -> None:
    """Run the text round trip and assert rows plus file bytes."""
    repark = ReparkSession.builder.appName("ex-io-text").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            root = Path(workdir)
            frame = repark.createDataFrame([("a",), ("b c",), (None,)], "value string")
            plain = root / "ex_text_plain"
            frame.write.text(str(plain))
            written = data_bytes(plain, ".txt")
            if sorted("".join(written).splitlines(keepends=True)) != ["\n", "a\n", "b c\n"]:
                raise SystemExit(f"text bytes {written!r} carry every row plus the empty line")
            back = repark.read.text(str(plain)).orderBy("value")
            if [repr(row) for row in back.collect()] != [
                "Row(value='')",
                "Row(value='a')",
                "Row(value='b c')",
            ]:
                raise SystemExit("text read does not answer the written rows")
            if back.schema.simpleString() != "struct<value:string>":
                raise SystemExit(f"text schema {back.schema.simpleString()!r} is not value:string")
            piped = root / "ex_text_piped"
            frame.write.format("text").save(str(piped))
            whole = repark.read.format("text").option("wholetext", "true").load(str(piped))
            if whole.count() != len(data_bytes(piped, ".txt")):
                raise SystemExit("wholetext does not answer one row per file")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
