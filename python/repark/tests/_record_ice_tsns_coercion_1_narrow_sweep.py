"""Record the registry sweep: every runtime function over an ns site.

For each catalog function name at run time (``SHOW FUNCTIONS`` itself errors on
repark, so the sweep reads the same set through ``catalog.listFunctions``), runs
``INSERT INTO t`` with ``f(ns, NULL)`` and ``f(NULL, ns)`` and records whether the
cell answers.
Run once under the main module and once under this head, then join the two
role files into ``ice_tsns_coercion_1_narrow_sweep.json``.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors

TESTS = Path(__file__).resolve().parent
FORMS = ("first", "second")


def _names(spark: Any) -> list[str]:
    """Return every runtime function name the catalog reports."""
    return sorted({str(item.name).split(".")[-1] for item in spark.catalog.listFunctions()})


def _answers(spark: Any, name: str, form: str) -> bool:
    """Return whether one sweep insert answers."""
    column = "ns" if form == "first" else "NULL"
    other = "NULL" if form == "first" else "ns"
    try:
        spark.sql("DROP TABLE IF EXISTS ice.ns.sweep_tgt")
        spark.sql(
            "CREATE TABLE ice.ns.sweep_tgt (v timestamp_ns) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
        spark.sql(
            f"INSERT INTO ice.ns.sweep_tgt SELECT {name}({column}, {other}) AS v "
            "FROM ice.ns.src"
        )
        spark.sql("SELECT * FROM ice.ns.sweep_tgt").toArrow()
    except Exception:
        return False
    finally:
        with contextlib.suppress(Exception):
            spark.sql("DROP TABLE IF EXISTS ice.ns.sweep_tgt")
    return True


def measure_all(scratch: Path) -> dict[str, dict[str, bool]]:
    """Return the answered flag of every sweep cell on this module."""
    warehouse = scratch / "sweep"
    warehouse.mkdir()
    spark = doors.open_session("UTC", warehouse)
    try:
        found: dict[str, dict[str, bool]] = {}
        for name in _names(spark):
            found[name] = {form: _answers(spark, name, form) for form in FORMS}
        return found
    finally:
        spark.stop()


def main() -> None:
    """Record every sweep cell for one module role."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as scratch:
        found = measure_all(Path(scratch))
    Path(args.out).write_text(json.dumps(found, indent=1, sort_keys=True) + "\n")
    answered = sum(1 for forms in found.values() for flag in forms.values() if flag)
    print(f"functions={len(found)} answered={answered}")


if __name__ == "__main__":
    main()
