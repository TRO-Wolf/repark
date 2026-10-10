"""The session's streaming entry points answer: a stream reader and the query manager.

pins: mb-4-foreach-eo/C-038
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.readStream",
    "SparkSession.streams",
]


def main() -> None:
    """Read both names on an idle session: a fresh reader each time, and no active query."""
    repark = ReparkSession.builder.appName("ex-ses-streaming").master("local[1]").getOrCreate()
    try:
        reader = repark.readStream
        reader_expected = "DataStreamReader"
        if type(reader).__name__ != reader_expected:
            raise SystemExit(f"readStream is a {type(reader).__name__}, not {reader_expected}")
        if repark.readStream is reader:
            raise SystemExit("readStream answered the same reader twice")

        manager = repark.streams
        manager_expected = "StreamingQueryManager"
        if type(manager).__name__ != manager_expected:
            raise SystemExit(f"streams is a {type(manager).__name__}, not {manager_expected}")
        active = manager.active
        if active != []:
            raise SystemExit(f"an idle session lists active queries: {active!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
