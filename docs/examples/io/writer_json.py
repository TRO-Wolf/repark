"""Write newline-delimited JSON files through the shorthand and the format spelling."""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriter.json",
]


def data_bytes(root: Path, suffix: str) -> list[str]:
    return sorted(
        path.read_text(encoding="utf-8") for path in root.rglob(f"*{suffix}") if path.is_file()
    )


def main() -> None:
    repark = ReparkSession.builder.appName("ex26-writer-json").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
        plain = Path("ex26_plain_json")
        frame.write.mode("overwrite").json(str(plain))
        plain_bytes = data_bytes(plain, ".json")
        plain_expected = ['{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n']
        if plain_bytes != plain_expected:
            raise SystemExit(f"json bytes {plain_bytes!r} != {plain_expected!r}")
        if len(plain_bytes) != 1:
            raise SystemExit(f"json file count {len(plain_bytes)} != 1")
        shaped = Path("ex26_shaped_json")
        frame.write.mode("overwrite").format("json").options().save(str(shaped))
        shaped_bytes = data_bytes(shaped, ".json")
        if shaped_bytes != plain_expected:
            raise SystemExit(f"format json bytes {shaped_bytes!r} != {plain_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
