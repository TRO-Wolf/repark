"""Demonstrate the ``F.*`` scalar-string remainder: code points, choice, caps, extract, digest."""
from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.char",
    "F.chr",
    "F.elt",
    "F.initcap",
    "F.regexp_extract",
    "F.sha2",
    "F.col",
    "F.lit",
]


def main() -> None:
    repark = ReparkSession.builder.appName("ex-strings-more").master("local[1]").getOrCreate()
    try:
        ints = repark.createDataFrame(
            [(65,), (256,), (300,), (321,), (65601,), (-1,), (0,), (None,)], "n INT"
        )
        rows = ints.select(
            F.chr("n").alias("chr"),
            F.char("n").alias("char"),
        ).collect()
        checked = (
            ("chr", ["A", "\x00", ",", "A", "A", "", "\x00", None]),
            ("char", ["A", "\x00", ",", "A", "A", "", "\x00", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        choices = repark.createDataFrame([(1,), (2,), (3,), (None,)], "n INT")
        rows = choices.select(
            F.elt("n", F.lit("a"), F.lit("b"), F.lit("c")).alias("picked")
        ).collect()
        values = [row["picked"] for row in rows]
        print(f"F.elt: {values!r}")
        if values != ["a", "b", "c", None]:
            raise SystemExit(f"F.elt values {values!r} != ['a', 'b', 'c', None]")
        bad = repark.createDataFrame([(0,)], "n INT")
        try:
            bad.select(F.elt("n", F.lit("a"), F.lit("b")).alias("picked")).collect()
        except Exception as error:
            print(f"F.elt out of range raises: {error}")
            if "INVALID_ARRAY_INDEX" not in str(error):
                raise SystemExit(f"F.elt raised without INVALID_ARRAY_INDEX: {error}") from error
        else:
            raise SystemExit("F.elt(0, ...) did not raise")

        words = repark.createDataFrame(
            [("a-b c.d",), ("o'neil",), ("ab_cd",), ("x\ty",), ("ünï_9 ab",), ("",), (None,)],
            "s STRING",
        )
        rows = words.select(F.initcap("s").alias("capped")).collect()
        values = [row["capped"] for row in rows]
        print(f"F.initcap: {values!r}")
        if values != ["A-b C.d", "O'neil", "Ab_cd", "X\ty", "Ünï_9 Ab", "", None]:
            raise SystemExit(f"F.initcap values {values!r} != the space-split caps")

        codes = repark.createDataFrame([("abc123",), ("no digits",), ("",), (None,)], "s STRING")
        rows = codes.select(
            F.regexp_extract("s", "([a-z]+)([0-9]+)", 2).alias("group2"),
            F.regexp_extract("s", "[0-9]+", 0).alias("whole"),
            F.regexp_extract("s", "z+", 0).alias("nomatch"),
        ).collect()
        checked = (
            ("group2", ["123", "", "", None]),
            ("whole", ["123", "", "", None]),
            ("nomatch", ["", "", "", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        digests = repark.createDataFrame([("Spark",), ("",), (None,)], "s STRING")
        rows = digests.select(
            F.sha2("s", 256).alias("sha256"),
            F.sha2("s", 224).alias("sha224"),
        ).collect()
        checked = (
            (
                "sha256",
                [
                    "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b",
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    None,
                ],
            ),
            (
                "sha224",
                [
                    "dbeab94971678d36af2195851c0f7485775a2a7c60073d62fc04549c",
                    "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
                    None,
                ],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
