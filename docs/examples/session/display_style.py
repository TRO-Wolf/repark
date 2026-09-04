"""Read and set the session display style that backs ``DataFrame.show``.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.display_style",
]


def main() -> None:
    """Run the measured style answers: the spark default, the polars switch, the conf mirror."""
    repark = ReparkSession.builder.appName("ex21-ses-display").master("local[1]").getOrCreate()
    try:
        default = repark.display_style
        default_expected = "spark"
        if default != default_expected:
            raise SystemExit(f"display_style default {default!r} != {default_expected!r}")

        repark.display_style = "polars"
        changed = repark.display_style
        changed_expected = "polars"
        if changed != changed_expected:
            raise SystemExit(f"display_style after set {changed!r} != {changed_expected!r}")

        mirrored = repark.conf.get("repark.display.style")
        if mirrored != changed_expected:
            raise SystemExit(f"conf.get display style {mirrored!r} != {changed_expected!r}")

        repark.display_style = "spark"
        restored = repark.display_style
        if restored != default_expected:
            raise SystemExit(f"display_style restored {restored!r} != {default_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
