"""Job tags round-trip on the session and the interrupt trio answers ``[]``.

pins: session-surface-1/C-001, C-002
"""

from __future__ import annotations

from repark.errors import IllegalArgumentException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.addTag",
    "SparkSession.removeTag",
    "SparkSession.getTags",
    "SparkSession.clearTags",
    "SparkSession.interruptAll",
    "SparkSession.interruptTag",
    "SparkSession.interruptOperation",
]


def main() -> None:
    """Run the tag set through Spark's answers and the idle interrupts to ``[]``."""
    repark = ReparkSession.builder.appName("ex-ses-tags").master("local[1]").getOrCreate()
    try:
        repark.addTag("etl")
        repark.addTag("nightly")
        if repark.getTags() != {"etl", "nightly"}:
            raise SystemExit(f"getTags {repark.getTags()!r} != {{'etl', 'nightly'}}")
        repark.removeTag("etl")
        repark.removeTag("missing")
        if repark.getTags() != {"nightly"}:
            raise SystemExit(f"getTags after removeTag {repark.getTags()!r} != {{'nightly'}}")
        empty_refused: str | None = None
        try:
            repark.addTag("")
        except IllegalArgumentException as error:
            empty_refused = str(error)
        if empty_refused != "Spark job tag cannot be an empty string.":
            raise SystemExit(f"empty tag refusal {empty_refused!r}")
        repark.clearTags()
        if repark.getTags() != set():
            raise SystemExit(f"getTags after clearTags {repark.getTags()!r} != set()")
        if repark.interruptAll() != []:
            raise SystemExit(f"interruptAll {repark.interruptAll()!r} != []")
        if repark.interruptTag("etl") != []:
            raise SystemExit(f"interruptTag {repark.interruptTag('etl')!r} != []")
        if repark.interruptOperation("42") != []:
            raise SystemExit(f"interruptOperation {repark.interruptOperation('42')!r} != []")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
