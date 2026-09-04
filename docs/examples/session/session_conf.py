"""Round-trip runtime configuration through the session conf map.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.conf",
]


def main() -> None:
    """Run the measured conf answers: string and bool round-trips plus the unset-key default."""
    repark = ReparkSession.builder.appName("ex21-ses-conf").master("local[1]").getOrCreate()
    try:
        conf = repark.conf
        conf.set("ex21.greeting", "hola")
        greeting = conf.get("ex21.greeting")
        greeting_expected = "hola"
        if greeting != greeting_expected:
            raise SystemExit(f"conf.get greeting {greeting!r} != {greeting_expected!r}")

        conf.set("ex21.debug", True)
        debug = conf.get("ex21.debug")
        debug_expected = "true"
        if debug != debug_expected:
            raise SystemExit(f"conf.get debug {debug!r} != {debug_expected!r}")

        fallback = conf.get("ex21.unset.key", "fallback")
        fallback_expected = "fallback"
        if fallback != fallback_expected:
            raise SystemExit(f"conf.get default {fallback!r} != {fallback_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
