"""Record live Spark's ``sequence`` answers over microsecond timestamps.

Run with live Spark 4.1.2 (``pyspark``): every form over ``ntz``
(``TIMESTAMP_NTZ``) and ``ts`` (``TIMESTAMP``) in zones UTC and
America/New_York. Writes ``ice_tsns_coercion_1_slice1_fold2_spark.json``
mapping each cell to Spark's ``typeof`` and the microsecond ticks of every
row's list, or to the error class.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

TESTS = Path(__file__).resolve().parent
OUT = TESTS / "ice_tsns_coercion_1_slice1_fold2_spark.json"
ZONES = ("UTC", "America/New_York")
SOURCES = ("ntz", "ts")
US_MOMENTS = ("2026-01-02 03:04:05.123456", "1969-12-31 23:59:59.999999")
_SITE = "coalesce({c}, NULL)"
FORMS: tuple[tuple[str, str], ...] = (
    ("plain", "sequence({c}, {c})"),
    ("plain_step", "sequence({c}, {c} + INTERVAL 2 SECOND, INTERVAL 1 SECOND)"),
    ("plain_down", "sequence({c} + INTERVAL 2 SECOND, {c}, INTERVAL -1 SECOND)"),
    ("plain_day", "sequence({c}, {c} + INTERVAL 2 DAY, INTERVAL 1 DAY)"),
    ("plain_month", "sequence({c}, {c} + INTERVAL 2 MONTH, INTERVAL 1 MONTH)"),
    ("coalesce", f"sequence({_SITE}, {{c}})"),
    ("both", f"sequence({_SITE}, {_SITE})"),
    ("step", f"sequence({_SITE}, {_SITE} + INTERVAL 2 SECOND, INTERVAL 1 SECOND)"),
    ("greatest", "sequence(greatest({c}, NULL), {c})"),
    ("if", "sequence(if(id > 0, {c}, NULL), {c})"),
    ("case", "sequence(CASE WHEN id > 0 THEN {c} ELSE NULL END, {c})"),
    ("nullif", "sequence(nullif({c}, NULL), {c})"),
)
TICKS = {
    "ntz": "timestampdiff(MICROSECOND, TIMESTAMP_NTZ '1970-01-01 00:00:00', x)",
    "ts": "unix_micros(x)",
}


def statement(expression: str, table: str) -> str:
    """Return the query both engines run for one form."""
    return f"SELECT id, {expression} AS v FROM {table} ORDER BY id"


def _source_view(spark: Any) -> None:
    """Register the two microsecond rows as ``src``."""
    rows = ", ".join(
        f"({index + 1}, TIMESTAMP '{moment}', TIMESTAMP_NTZ '{moment}')"
        for index, moment in enumerate(US_MOMENTS)
    )
    spark.sql(f"SELECT * FROM VALUES {rows} AS t(id, ts, ntz)").createOrReplaceTempView("src")


def _measure(spark: Any, source: str, template: str) -> dict[str, Any]:
    """Return Spark's type and ticks for one form, or its error class."""
    expression = template.format(c=source)
    try:
        kind = spark.sql(f"SELECT typeof({expression}) AS t FROM src LIMIT 1").collect()[0][0]
        ticks = f"transform({expression}, x -> {TICKS[source]})"
        rows = spark.sql(statement(ticks, "src")).collect()
    except Exception as error:
        condition = getattr(error, "getCondition", None)
        return {"error": condition() if condition is not None else type(error).__name__}
    return {"type": kind, "values": [row[1] for row in rows]}


def main() -> None:
    """Record every form in every zone on live Spark."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").appName("ice-tsns-fold2").getOrCreate()
    outcomes: dict[str, dict[str, Any]] = {}
    for zone in ZONES:
        spark.conf.set("spark.sql.session.timeZone", zone)
        _source_view(spark)
        for source in SOURCES:
            for form, template in FORMS:
                outcomes[f"{zone}|{source}|{form}"] = _measure(spark, source, template)
    spark.stop()
    OUT.write_text(json.dumps(outcomes, indent=1, sort_keys=True) + "\n")
    errors = sum(1 for outcome in outcomes.values() if "error" in outcome)
    print(f"spark={spark.version} cells={len(outcomes)} errors={errors}")


if __name__ == "__main__":
    main()
