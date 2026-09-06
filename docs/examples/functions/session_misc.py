"""Demonstrate the session-user names, ``F.randstr`` lengths and ``F.isnan``."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.current_user",
    "F.session_user",
    "F.randstr",
    "F.isnan",
]


def main() -> None:
    """Run the measured session-user, randstr, and isnan arms."""
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
