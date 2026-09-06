"""Demonstrate session identity, ``F.randstr`` lengths, ``F.isnan``, and seeded ``F.uniform``."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.current_user",
    "F.session_user",
    "F.user",
    "F.version",
    "F.uniform",
    "F.randstr",
    "F.isnan",
]


def main() -> None:
    """Run the measured session-identity, randstr, uniform, and isnan arms."""
    repark = ReparkSession.builder.appName("ex-session-misc").master("local[1]").getOrCreate()
    try:
        single = repark.createDataFrame([(1,)], "x INT")
        rows = single.select(
            F.current_user().alias("current"),
            F.session_user().alias("session"),
            F.randstr(10).alias("token"),
            F.randstr(10, 7).alias("seeded"),
        ).collect()
        current = rows[0]["current"]
        session = rows[0]["session"]
        print(f"F.current_user: {current!r}")
        print(f"F.session_user: {session!r}")
        if not isinstance(current, str) or not current:
            raise SystemExit(f"F.current_user {current!r} is not a non-empty string")
        if not isinstance(session, str) or not session:
            raise SystemExit(f"F.session_user {session!r} is not a non-empty string")
        for name in ("token", "seeded"):
            value = rows[0][name]
            print(f"F.{name}: {value!r}")
            if not isinstance(value, str) or len(value) != 10:
                raise SystemExit(f"F.randstr {name} {value!r} is not a 10-character string")

        identity = (
            repark.range(2)
            .select(
                F.user().alias("user"),
                F.version().alias("version"),
            )
            .collect()
        )
        users = [row["user"] for row in identity]
        versions = [row["version"] for row in identity]
        print(f"F.user: {users!r}")
        print(f"F.version: {versions!r}")
        if users != [current, current]:
            raise SystemExit(f"F.user {users!r} != [{current!r}, {current!r}]")
        if (
            len(versions) != 2
            or versions[0] != versions[1]
            or not isinstance(versions[0], str)
            or not versions[0]
        ):
            raise SystemExit(f"F.version {versions!r} is not a stable non-empty string")

        drawn = (
            repark.range(8)
            .select(
                F.uniform(5, 9, 3).alias("ints"),
                F.uniform(-1.5, 2.5, 3).alias("floats"),
                F.uniform(5, 9).alias("unseeded"),
            )
            .collect()
        )
        ints = [row["ints"] for row in drawn]
        floats_drawn = [row["floats"] for row in drawn]
        unseeded = [row["unseeded"] for row in drawn]
        print(f"F.uniform ints: {ints!r}")
        print(f"F.uniform floats: {floats_drawn!r}")
        print(f"F.uniform unseeded: {unseeded!r}")
        if ints != [6, 7, 8, 7, 5, 5, 8, 8]:
            raise SystemExit(f"F.uniform seeded ints {ints!r} != [6, 7, 8, 7, 5, 5, 8, 8]")
        if floats_drawn != [
            -0.4704742597615086,
            1.1795542855184729,
            2.26666047939205,
            1.4194013697092451,
            -1.040397133282677,
            -0.6706361279762554,
            1.8837653178204983,
            1.5340848185017206,
        ]:
            raise SystemExit(f"F.uniform seeded floats {floats_drawn!r} != the Spark-measured list")
        if len(unseeded) != 8 or any(
            not isinstance(value, int) or not 5 <= value < 9 for value in unseeded
        ):
            raise SystemExit(f"F.uniform unseeded {unseeded!r} is not eight ints in [5, 9)")

        floats = repark.createDataFrame([(float("nan"),), (1.0,), (None,)], "x DOUBLE")
        rows = floats.select(F.isnan("x").alias("is_nan")).collect()
        values = [row["is_nan"] for row in rows]
        print(f"F.isnan: {values!r}")
        if values != [True, False, False]:
            raise SystemExit(f"F.isnan values {values!r} != [True, False, False]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
