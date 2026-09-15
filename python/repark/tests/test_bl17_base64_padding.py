"""BL-17 FIXED: F.base64 applies RFC 4648 padding (registry §7).

pins: door-converge-1/C-001
"""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_bl17_base64_applies_rfc4648_padding() -> None:
    """Spark 4.1.2 pads: U3Bhcms= and QQ== for Spark and A."""
    repark = ReparkSession.builder.appName("bl17").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("Spark",), ("Apache",), ("A",), ("",), (None,)], ["s"])
        values = [row[0] for row in frame.select(F.base64(F.col("s"))).collect()]
        assert values == ["U3Bhcms=", "QXBhY2hl", "QQ==", "", None]
    finally:
        repark.stop()
