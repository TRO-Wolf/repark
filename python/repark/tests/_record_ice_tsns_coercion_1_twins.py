"""Record the ICE-TSNS-COERCION-1 twin matrix from the installed build.

``python _record_ice_tsns_coercion_1_twins.py --commit <sha> --output <path>``
runs every door. ``--shard-zone=ZONE --shard-target=TARGET`` runs one zone and target.
Paths come from the arguments. The fixture packs repeated outcomes once.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import pyarrow as pa
from _ice_tsns_coercion_1_spellings import (
    DOORS,
    FORMS,
    MOMENTS,
    RANGE_SIBLINGS,
    ROLES,
    SIBLINGS,
    SOURCES,
    SRC,
    TARGETS,
    ZONES,
)


def split_list(text: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Return ``text`` split on commas, or ``default`` when ``text`` is empty."""
    if not text:
        return default
    return tuple(item for item in text.split(",") if item)


def metadata_of(field: pa.Field) -> dict[str, str]:
    """Return field metadata as text."""
    raw = field.metadata or {}
    decoded: dict[str, str] = {}
    for key, value in raw.items():
        name = key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
        text = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
        decoded[name] = text
    return decoded


def capture(spark: Any, statement: str) -> dict[str, Any]:
    """Return the Arrow schema, field metadata and rows of ``statement``."""
    try:
        table = spark.sql(statement).toArrow()
    except Exception as error:
        return {
            "error": doors.error_text(error),
            "error_type": type(error).__name__,
        }
    field = table.schema.field("v")
    column = table.column("v")
    ticks: list[Any] | None = None
    if pa.types.is_timestamp(column.type):
        ticks = column.cast(pa.int64()).to_pylist()
    rows: list[dict[str, Any]] = []
    ids = table.column("id").to_pylist() if "id" in table.schema.names else [None] * len(column)
    values = column.to_pylist()
    for index, value in enumerate(values):
        row: dict[str, Any] = {
            "id": ids[index],
            "v": None if value is None else str(value),
        }
        if ticks is not None and ticks[index] is not None:
            row["tick"] = ticks[index]
        rows.append(row)
    return {
        "type": str(field.type),
        "nullable": field.nullable,
        "metadata": metadata_of(field),
        "rows": rows,
    }


def read_sql(spark: Any, source: str, form: str, sibling: str, role: str) -> str:
    """Return the read statement for one spelling."""
    cell = doors.Cell(spark, "ice.ns.unused", source, form, sibling, role)
    if form == "union" and role != "plain":
        return cell.select()
    return f"SELECT id, {cell.value_sql(source, 'id')} AS v FROM {SRC}"


def run_shard(
    zone: str,
    target: str,
    forms: tuple[str, ...],
    siblings: tuple[str, ...],
    sources: tuple[str, ...],
    door_list: tuple[str, ...],
    roles: tuple[str, ...],
) -> dict[str, Any]:
    """Measure one zone and target."""
    with tempfile.TemporaryDirectory(prefix="tsns-coercion-") as scratch:
        warehouse = Path(scratch)
        spark = doors.open_session(zone, warehouse)
        try:
            return _measure_shard(
                spark, warehouse, zone, target, forms, siblings, sources, door_list, roles
            )
        finally:
            spark.stop()


def _measure_shard(
    spark: Any,
    warehouse: Path,
    zone: str,
    target: str,
    forms: tuple[str, ...],
    siblings: tuple[str, ...],
    sources: tuple[str, ...],
    door_list: tuple[str, ...],
    roles: tuple[str, ...],
) -> dict[str, Any]:
    """Measure one open session. The caller stops it."""
    cells: dict[str, Any] = {}
    ansi = str(spark.conf.get("spark.sql.ansi.enabled"))
    schema = doors.source_schema(spark)
    print(f"schema {zone} {schema}", file=sys.stderr, flush=True)
    done = 0
    total = (
        len(sources)
        * len(door_list)
        * (
            (1 if "plain" in roles else 0)
            + len(forms) * len(siblings) * len([role for role in roles if role != "plain"])
        )
    )
    started = time.perf_counter()
    for source in sources:
        if "plain" in roles:
            for door in door_list:
                cells[f"plain|{source}|{door}"] = doors.measure(
                    spark, warehouse, target, source, "plain", "", "plain", door
                )
                done += 1
        for form in forms:
            for sibling in siblings:
                for role in roles:
                    if role == "plain":
                        continue
                    for door in door_list:
                        cells[f"{role}|{source}|{form}|{sibling}|{door}"] = doors.measure(
                            spark,
                            warehouse,
                            target,
                            source,
                            form,
                            sibling,
                            role,
                            door,
                        )
                        done += 1
                        if done % 50 == 0:
                            rate = done / (time.perf_counter() - started)
                            print(
                                f"{zone} {target} {done}/{total} {rate:.2f}/s",
                                file=sys.stderr,
                                flush=True,
                            )
    return {"ansi": ansi, "source_schema": schema, "cells": cells}


def record_reads(zone: str) -> dict[str, Any]:
    """Record the read of every spelling and twin in ``zone``."""
    found: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="tsns-coercion-read-") as scratch:
        spark = doors.open_session(zone, Path(scratch))
        for source in SOURCES:
            for form in FORMS:
                for sibling in SIBLINGS:
                    for role in ROLES:
                        statement = read_sql(spark, source, form, sibling, role)
                        found[f"{zone}|{source}|{form}|{sibling}|{role}"] = capture(
                            spark, statement
                        )
        spark.stop()
    return found


def record_range(zone: str, ansi: str) -> dict[str, Any]:
    """Record out-of-range literals and the far column, statement and twin."""
    found: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="tsns-coercion-range-") as scratch:
        spark = doors.open_session(zone, Path(scratch))
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        for source in SOURCES:
            for form in FORMS:
                for sibling in RANGE_SIBLINGS:
                    for role in ROLES:
                        statement = read_sql(spark, source, form, sibling, role)
                        found[f"{zone}|{ansi}|{source}|{form}|{sibling}|{role}"] = capture(
                            spark, statement
                        )
        spark.stop()
    return found


def pack(commit: str, shards: list[dict[str, Any]]) -> dict[str, Any]:
    """Pack shard cells into one outcome table."""
    outcomes: list[str] = []
    index: dict[str, int] = {}
    statement: dict[str, list[int]] = {}
    twin: dict[str, list[int]] = {}
    plain: dict[str, list[int]] = {}
    schemas: dict[str, Any] = {}
    ansi = ""
    for shard in shards:
        zone = shard["zone"]
        target = shard["target"]
        ansi = shard["ansi"]
        schemas[zone] = shard["source_schema"]
        for name, cell in shard["cells"].items():
            text = json.dumps(cell, sort_keys=True, separators=(",", ":"))
            slot = index.get(text)
            if slot is None:
                slot = len(outcomes)
                index[text] = slot
                outcomes.append(text)
            role, source, *rest = name.split("|")
            if role == "plain":
                door = rest[0]
                key = f"{zone}|{target}|{source}"
                slots = plain.setdefault(key, [-1] * len(DOORS))
                slots[DOORS.index(door)] = slot
                continue
            form, sibling, door = rest
            key = f"{zone}|{target}|{source}|{form}|{sibling}"
            bucket = statement if role == "statement" else twin
            slots = bucket.setdefault(key, [-1] * len(DOORS))
            slots[DOORS.index(door)] = slot
    return {
        "commit": commit,
        "ansi": ansi,
        "source_schema": schemas,
        "doors": list(DOORS),
        "forms": list(FORMS),
        "siblings": list(SIBLINGS),
        "range_siblings": list(RANGE_SIBLINGS),
        "sources": list(SOURCES),
        "targets": list(TARGETS),
        "zones": list(ZONES),
        "moments": list(MOMENTS),
        "outcomes": [json.loads(text) for text in outcomes],
        "statement": statement,
        "twin": twin,
        "plain": plain,
    }


def shard_command(args: argparse.Namespace, zone: str, target: str, output: Path) -> list[str]:
    """Return the argv of one shard process.

    The zone is bound with ``=`` so a value such as ``-03:30`` is not read as a flag.
    """
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--commit",
        args.commit,
        f"--shard-zone={zone}",
        f"--shard-target={target}",
        "--output",
        str(output),
        "--forms",
        args.forms,
        "--siblings",
        args.siblings,
        "--sources",
        args.sources,
        "--doors",
        args.doors,
        "--roles",
        args.roles,
    ]


def shard_ready(path: Path, zone: str, target: str) -> bool:
    """Return whether ``path`` is a finished shard for ``zone`` and ``target``."""
    if not path.is_file():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    return payload.get("zone") == zone and payload.get("target") == target and "cells" in payload


def run_pool(commands: list[list[str]], jobs: int) -> None:
    """Run ``commands`` with at most ``jobs`` processes, and wait for all of them."""
    pending = list(commands)
    running: list[subprocess.Popen[bytes]] = []
    while pending or running:
        while pending and len(running) < jobs:
            running.append(subprocess.Popen(pending.pop(0)))
        time.sleep(0.3)
        alive: list[subprocess.Popen[bytes]] = []
        failed = 0
        for proc in running:
            code = proc.poll()
            if code is None:
                alive.append(proc)
            elif code != 0:
                failed = code
        if failed:
            for proc in alive:
                proc.terminate()
            raise RuntimeError(f"shard exited {failed}")
        running = alive


def parse_args() -> argparse.Namespace:
    """Return the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-zone", default="")
    parser.add_argument("--shard-target", default="")
    parser.add_argument("--shard-dir", type=Path)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--forms", default="")
    parser.add_argument("--siblings", default="")
    parser.add_argument("--sources", default="")
    parser.add_argument("--doors", default="")
    parser.add_argument("--roles", default="")
    parser.add_argument("--zones", default="")
    parser.add_argument("--reads-only", action="store_true")
    parser.add_argument("--no-reads", action="store_true")
    parser.add_argument("--no-range", action="store_true")
    return parser.parse_args()


def main() -> None:
    """Record the matrix, the reads and the range cells."""
    args = parse_args()
    forms = split_list(args.forms, FORMS)
    siblings = split_list(args.siblings, SIBLINGS)
    sources = split_list(args.sources, tuple(SOURCES))
    door_list = split_list(args.doors, DOORS)
    roles = split_list(args.roles, ("statement", "twin", "plain"))
    zones = split_list(args.zones, ZONES)
    doors.door_names()
    if args.shard_zone or args.shard_target:
        if not args.shard_zone or not args.shard_target:
            raise SystemExit("a shard needs both --shard-zone and --shard-target")
        zone, target = args.shard_zone, args.shard_target
        payload = run_shard(zone, target, forms, siblings, sources, door_list, roles)
        payload["zone"] = zone
        payload["target"] = target
        args.output.write_text(json.dumps(payload), encoding="utf-8")
        return
    if not args.reads_only:
        scratch_cm = (
            nullcontext(args.shard_dir)
            if args.shard_dir is not None
            else tempfile.TemporaryDirectory(prefix="tsns-coercion-shards-")
        )
        with scratch_cm as scratch:
            root = Path(scratch)
            root.mkdir(parents=True, exist_ok=True)
            commands = []
            paths = []
            for zone in zones:
                for target in TARGETS:
                    path = root / f"{zone.replace('/', '_')}-{target}.json"
                    paths.append(path)
                    if shard_ready(path, zone, target):
                        print(f"reuse {zone} {target}", file=sys.stderr, flush=True)
                        continue
                    commands.append(shard_command(args, zone, target, path))
            run_pool(commands, args.jobs)
            shards = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        fixture = pack(args.commit, shards)
    else:
        fixture = {
            "commit": args.commit,
            "doors": list(DOORS),
            "forms": list(FORMS),
            "siblings": list(SIBLINGS),
            "range_siblings": list(RANGE_SIBLINGS),
            "sources": list(SOURCES),
            "targets": list(TARGETS),
            "zones": list(ZONES),
            "moments": list(MOMENTS),
            "outcomes": [],
            "statement": {},
            "twin": {},
            "plain": {},
        }
    if not args.no_reads:
        reads: dict[str, Any] = {}
        for zone in zones:
            reads.update(record_reads(zone))
            print(f"reads {zone}", file=sys.stderr, flush=True)
        fixture["reads"] = reads
    if not args.no_range:
        ranged: dict[str, Any] = {}
        for zone in zones:
            for ansi in ("true", "false"):
                ranged.update(record_range(zone, ansi))
                print(f"range {zone} {ansi}", file=sys.stderr, flush=True)
        fixture["range"] = ranged
    args.output.write_text(json.dumps(fixture, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
