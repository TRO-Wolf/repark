"""Run one interleaved cost set for the ICE-TSNS-COERCION-1 slice 1 follow-up.

The setup and the timed statements are the slice-0 timing script's own
objects (`_time_ice_tsns_coercion_1`, loaded from this checkout): the same
session, the same source tables, the same INSERT and MERGE texts. Only the
timed set is trimmed to the statements the follow-up names. Each invocation
runs one set under one Python and writes one JSON file with the per-repeat
times, the set medians, and the gate-counter snapshots around the set.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import statistics
import tempfile
import types
from pathlib import Path
from typing import Any

from repark import ReparkSession, _native

NAMES = ("insert", "merge", "merge_mor")


def _load_timer() -> types.ModuleType:
    """Return the slice-0 timing module from this checkout's facade tests."""
    root = Path(__file__).resolve().parents[3]
    path = root / "python" / "repark" / "tests" / "_time_ice_tsns_coercion_1.py"
    spec = importlib.util.spec_from_file_location("_time_ice_tsns_coercion_1", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _counts() -> tuple[int | None, int | None]:
    """Return the gate counters, or nulls on a module that predates them."""
    reader = getattr(_native, "_tsns_gate_counts", None)
    if reader is None:
        return None, None
    opened: int
    fallback: int
    opened, fallback = reader()
    return opened, fallback


def _open_session() -> Any:
    """Return the timing script's session."""
    return (
        ReparkSession.builder.appName("ice-tsns-coercion-1-cost")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def _seed(timer: types.ModuleType, spark: Any, warehouse: Path) -> None:
    """Create and seed the timing script's source tables."""
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, t TIMESTAMP, n TIMESTAMP_NTZ, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        "INSERT INTO ice.ns.src SELECT CAST(id AS INT), "
        "timestamp_micros(1767323045123456 + id), "
        "CAST(timestamp_micros(1767323045123456 + id) AS TIMESTAMP_NTZ), CAST(id AS STRING) "
        f"FROM range({timer.ROWS})"
    )
    spark.sql(
        "CREATE TABLE ice.ns.nsrc (id INT, n timestamp_ns) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(
        "INSERT INTO ice.ns.nsrc SELECT CAST(id AS INT), "
        "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns) "
        f"FROM range({timer.ROWS})"
    )


def _merge_text(timer: types.ModuleType) -> str:
    """Return the timing script's MERGE text."""
    return (
        "MERGE INTO {t} t USING (SELECT id, " + timer.VALUE + " FROM ice.ns.src) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET a = s.a, b = coalesce(s.b, NULL), c = s.c "
        "WHEN NOT MATCHED THEN INSERT (id, a, b, c, d, s) VALUES (s.id, s.a, s.b, s.c, s.d, s.s)"
    )


def _run_repeats(
    timer: types.ModuleType,
    spark: Any,
    merge: str,
    names: list[str],
    repeats: int,
) -> dict[str, list[float]]:
    """Return the wall times of each named statement over ``repeats`` repeats."""
    times: dict[str, list[float]] = {name: [] for name in names}
    for index in range(repeats):
        timer.create(spark, f"ice.ns.i{index}")
        timer.create(spark, f"ice.ns.m{index}")
        timer.create(spark, f"ice.ns.r{index}", timer.MOR)
        if "insert" in names:
            times["insert"].append(
                timer.timed(
                    spark,
                    f"INSERT INTO ice.ns.i{index} SELECT id, {timer.VALUE} FROM ice.ns.src",
                )
            )
        if "merge" in names:
            times["merge"].append(timer.timed(spark, merge.format(t=f"ice.ns.m{index}")))
        if "merge_mor" in names:
            times["merge_mor"].append(timer.timed(spark, merge.format(t=f"ice.ns.r{index}")))
    return times


def run_set(timer: types.ModuleType, names: list[str], repeats: int) -> dict[str, Any]:
    """Run one set and return its times, medians and counter snapshots."""
    before = _counts()
    with tempfile.TemporaryDirectory(prefix="tsns-interleaved-") as scratch:
        spark = _open_session()
        _seed(timer, spark, Path(scratch))
        times = _run_repeats(timer, spark, _merge_text(timer), names, repeats)
        spark.stop()
    after = _counts()
    medians = {name: round(statistics.median(times[name]), 2) for name in names}
    return {
        "repeats": repeats,
        "times_ms": times,
        "medians_ms": medians,
        "counters_before": before,
        "counters_after": after,
    }


def run_around(timer: types.ModuleType, name: str) -> dict[str, Any]:
    """Run one ``name`` statement and return its time and counter brackets."""
    with tempfile.TemporaryDirectory(prefix="tsns-interleaved-") as scratch:
        spark = _open_session()
        _seed(timer, spark, Path(scratch))
        timer.create(spark, "ice.ns.i0")
        timer.create(spark, "ice.ns.m0")
        timer.create(spark, "ice.ns.r0", timer.MOR)
        merge = _merge_text(timer)
        table = {"insert": "ice.ns.i0", "merge": "ice.ns.m0", "merge_mor": "ice.ns.r0"}[name]
        if name == "insert":
            statement = f"INSERT INTO {table} SELECT id, {timer.VALUE} FROM ice.ns.src"
        else:
            statement = merge.format(t=table)
        before = _counts()
        elapsed = timer.timed(spark, statement)
        after = _counts()
        spark.stop()
    return {"statement": statement, "elapsed_ms": elapsed, "before": before, "after": after}


def parse_args() -> argparse.Namespace:
    """Return the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--set", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=15)
    parser.add_argument("--statements", default=",".join(NAMES))
    parser.add_argument("--around", choices=NAMES, default="")
    return parser.parse_args()


def main() -> None:
    """Run one set, or bracket one statement, and write the JSON report."""
    args = parse_args()
    timer = _load_timer()
    if args.around:
        payload = run_around(timer, args.around)
    else:
        names = [name for name in args.statements.split(",") if name]
        unknown = [name for name in names if name not in NAMES]
        if unknown:
            raise RuntimeError(f"unknown statements: {','.join(unknown)}")
        payload = run_set(timer, names, args.repeats)
    payload["label"] = args.label
    payload["set"] = args.set
    args.output.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
