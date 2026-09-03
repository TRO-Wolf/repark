"""Demonstrate the ``F.*`` URL codec and part-extraction names on URL literals.

pins: ex-11-functions-hash-url-random/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.url_encode",
    "F.url_decode",
    "F.try_url_decode",
    "F.parse_url",
    "F.try_parse_url",
    "F.lit",
]

URL = "https://spark.apache.org/docs/latest/api.html?q=spark+sql#example"


def main() -> None:
    """Check the codec round trip and every URL part, with the try spellings answering NULL."""
    repark = ReparkSession.builder.appName("ex-url").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("u",)], ["s"])
        row = frame.select(
            F.url_encode(F.lit("hello world & more")).alias("encoded"),
            F.url_decode(F.lit("hello+world")).alias("decoded"),
            F.url_decode(F.url_encode(F.lit("hello world & more"))).alias("round_trip"),
            F.url_encode(F.lit(None)).alias("encode_null"),
            F.url_decode(F.lit("a%2Fb")).alias("percent"),
            F.url_decode(F.lit(None)).alias("decode_null"),
            F.try_url_decode(F.lit("a%2Fb")).alias("try_percent"),
            F.try_url_decode(F.lit("%ZZ")).alias("try_malformed"),
            F.try_url_decode(F.lit(None)).alias("try_null"),
            F.parse_url(F.lit(URL), F.lit("PROTOCOL")).alias("protocol"),
            F.parse_url(F.lit(URL), F.lit("HOST")).alias("host"),
            F.parse_url(F.lit(URL), F.lit("PATH")).alias("path"),
            F.parse_url(F.lit(URL), F.lit("QUERY")).alias("query"),
            F.parse_url(F.lit(URL), F.lit("REF")).alias("ref"),
            F.parse_url(F.lit(URL), F.lit("FILE")).alias("file"),
            F.parse_url(F.lit(URL), F.lit("AUTHORITY")).alias("authority"),
            F.parse_url(F.lit(URL), F.lit("PORT")).alias("port"),
            F.parse_url(F.lit(None), F.lit("HOST")).alias("host_null"),
            F.try_parse_url(F.lit(URL), F.lit("HOST")).alias("try_host"),
            F.try_parse_url(F.lit("%ZZ"), F.lit("HOST")).alias("try_bad_url"),
            F.try_parse_url(F.lit(None), F.lit("HOST")).alias("try_null_url"),
        ).collect()[0]
        print(f"URL row: {row!r}")
        expected = {
            "encoded": "hello+world+%26+more",
            "decoded": "hello world",
            "round_trip": "hello world & more",
            "encode_null": None,
            "percent": "a/b",
            "decode_null": None,
            "try_percent": "a/b",
            "try_malformed": None,
            "try_null": None,
            "protocol": "https",
            "host": "spark.apache.org",
            "path": "/docs/latest/api.html",
            "query": "q=spark+sql",
            "ref": "example",
            "file": "/docs/latest/api.html?q=spark+sql",
            "authority": "spark.apache.org",
            "port": None,
            "host_null": None,
            "try_host": "spark.apache.org",
            "try_bad_url": None,
            "try_null_url": None,
        }
        for name, want in expected.items():
            if row[name] != want:
                raise SystemExit(f"{name} gave {row[name]!r}, expected {want!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
