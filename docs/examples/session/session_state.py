"""The process active session: active(), getActiveSession(), and newSession().

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.active",
    "SparkSession.getActiveSession",
    "SparkSession.newSession",
]


def main() -> None:
    """Run the measured active-session answers: the builder session is active, the spare is not."""
    repark = ReparkSession.builder.appName("ex21-ses-active").master("local[1]").getOrCreate()
    try:
        active = repark.active()
        if active is not repark:
            raise SystemExit(f"active session {active!r} is not the builder session")

        live = repark.getActiveSession()
        if live is not repark:
            raise SystemExit(f"getActiveSession {live!r} is not the builder session")

        spare = repark.newSession()
        if spare is repark:
            raise SystemExit("newSession returned the same session object")

        if repark.getActiveSession() is spare:
            raise SystemExit("newSession stole the active session before any action")

        spare_rows = [tuple(row) for row in spare.sql("SELECT 7 AS seven").collect()]
        spare_rows_expected = [(7,)]
        if spare_rows != spare_rows_expected:
            raise SystemExit(f"newSession rows {spare_rows!r} != {spare_rows_expected!r}")
        spare.stop()
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
