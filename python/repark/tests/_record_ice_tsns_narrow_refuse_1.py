"""ICE-TSNS-NARROW-REFUSE-1: record what the installed build stores in every matrix cell.

``python _record_ice_tsns_narrow_refuse_1.py <commit>`` measures every cell in five zones and
rewrites ``ice_tsns_narrow_refuse_1_base.json``; ``--output <path>`` measures a build for a
comparison without touching the fixture, and ``--compare <path>`` prints how the cells of that
file differ from the fixture. ``--extend`` measures only the cells the output file does not hold
yet and adds them, which is how a spelling or a door added later is recorded on the same build.
``--shard <zone> <target>`` prints one shard as JSON; the parent uses it to run the twenty
shards in parallel, ``--jobs`` at a time.

The fixture is compact: ``outcomes`` lists each distinct result once, and ``cells`` maps
``zone|target|source|spelling`` to one outcome index per door, in the order of ``doors``. An
error is kept to its first 240 characters; a cell measured since the unit carries ``refused``
when the whole text holds the refusal, which a long table name pushes past that cut.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import _ice_tsns_narrow_refuse_1_doors as doors

FIXTURE = Path(__file__).with_name("ice_tsns_narrow_refuse_1_base.json")


def record_shard(zone: str, target: str, known: frozenset[str]) -> dict[str, Any]:
    """Measure the cells of one zone and target in one session, but for the ``known`` keys."""
    cells: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="tsns-narrow-") as scratch:
        warehouse = Path(scratch)
        spark = doors.open_session(zone, warehouse)
        for source in doors.SOURCES:
            for spelling in doors.SPELLINGS:
                for door in doors.DOORS:
                    name = f"{zone}|{target}|{source}|{spelling}|{door}"
                    if name in known:
                        continue
                    key = (target, source, spelling, door)
                    table = f"ice.ns.x{doors.table_number(source, spelling, door)}"
                    cells[name] = doors.measure(spark, warehouse, table, key)
        spark.stop()
    return cells


def run_shard(arguments: tuple[str, str, str]) -> dict[str, Any]:
    """Run one shard in a child process and return its cells."""
    zone, target, extend = arguments
    done = subprocess.run(
        [sys.executable, __file__, "shard", "--shard", zone, target, *extend.split()],
        capture_output=True,
        text=True,
        check=False,
    )
    if done.returncode != 0:
        raise RuntimeError(f"{zone} {target}: {done.stderr[-2000:]}")
    return json.loads(done.stdout)


def pack(commit: str, cells: dict[str, Any]) -> dict[str, Any]:
    """Return the compact fixture of ``cells``."""
    outcomes: list[str] = []
    index: dict[str, int] = {}
    packed: dict[str, list[int]] = {}
    for key, cell in sorted(cells.items()):
        row, door = key.rsplit("|", 1)
        text = json.dumps(cell, sort_keys=True, separators=(",", ":"))
        if text not in index:
            index[text] = len(outcomes)
            outcomes.append(text)
        slots = packed.setdefault(row, [-1] * len(doors.DOORS))
        slots[doors.DOORS.index(door)] = index[text]
    return {
        "commit": commit,
        "doors": list(doors.DOORS),
        "outcomes": [json.loads(text) for text in outcomes],
        "cells": packed,
    }


def unpack(fixture: dict[str, Any]) -> dict[str, Any]:
    """Return the cells of a compact fixture keyed ``zone|target|source|spelling|door``."""
    cells: dict[str, Any] = {}
    for row, slots in fixture["cells"].items():
        for door, slot in zip(fixture["doors"], slots, strict=True):
            if slot >= 0:
                cells[f"{row}|{door}"] = fixture["outcomes"][slot]
    return cells


def classify(cells: dict[str, Any], key: str) -> str:
    """Name what the cell ``key`` stored against what INSERT stores for the plain column."""
    cell = cells[key]
    if cell.get("skip"):
        return "skip"
    if "error" in cell:
        named = cell.get("refused") or doors.REFUSAL_HEAD.search(cell["error"])
        return ("refused" if named else "error") + ("" if not cell["stored"] else "+stored")
    zone, target, source, _, _ = key.split("|")
    full = dict(map(tuple, cells[f"{zone}|{target}|{source}|plain|insert_select"]["stored"]))
    got = cell["stored"]
    if not got:
        return "nothing"
    if any(isinstance(tick, str) or row not in full for row, tick in got):
        return "other"
    if all(tick == full[row] for row, tick in got):
        return "full"
    if target not in doors.NANOSECOND_TARGETS:
        return "other"
    if all(tick == full[row] // 1_000 * 1_000 for row, tick in got):
        return "cut"
    scale = max(abs(tick) for _, tick in got)
    if scale > doors.NANOSECOND_SCALE and all(tick % 1_000 == 0 for _, tick in got):
        return "cut+wall"
    return "other"


def summarize(cells: dict[str, Any]) -> dict[str, Counter]:
    """Count the classes of the 650-cell core and of every group of spellings and targets."""
    counts: dict[str, Counter] = {}
    for key in cells:
        _, target, _, spelling, door = key.split("|")
        kind = classify(cells, key)
        if spelling in doors.FAMILY and target == "ts_ns" and door in doors.CORE_DOORS:
            counts.setdefault("core 650", Counter())[kind] += 1
        group = next(name for name, spellings in doors.GROUPS.items() if spelling in spellings)
        counts.setdefault(f"{group} {target}", Counter())[kind] += 1
    return counts


def compare(base: dict[str, Any], head: dict[str, Any]) -> Counter:
    """Count the cells of ``head`` by their class on ``base`` and on ``head``."""
    moves: Counter = Counter()
    for key in sorted(base):
        if key not in head:
            moves[("missing", "")] += 1
            continue
        before, after = classify(base, key), classify(head, key)
        same = doors.comparable(base[key]) == doors.comparable(head[key])
        moves[(before, "same" if same else after)] += 1
    return moves


def main() -> None:
    """Record one shard to stdout, or every shard into the fixture or ``--output``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit")
    parser.add_argument("--shard", nargs=2)
    parser.add_argument("--jobs", type=int, default=10)
    parser.add_argument("--output", type=Path, default=FIXTURE)
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--extend", action="store_true")
    arguments = parser.parse_args()
    known: dict[str, Any] = {}
    if arguments.extend:
        known = unpack(json.loads(arguments.output.read_text()))
    if arguments.shard:
        json.dump(record_shard(*arguments.shard, frozenset(known)), sys.stdout)
        return
    if arguments.compare:
        base = unpack(json.loads(FIXTURE.read_text()))
        head = unpack(json.loads(arguments.compare.read_text()))
        for (before, after), count in sorted(compare(base, head).items()):
            print(f"{count:6d}  {before} -> {after}")
        return
    extend = f"--extend --output {arguments.output}" if arguments.extend else ""
    shards = [(zone, target, extend) for zone in doors.ZONES for target in doors.TARGETS]
    cells: dict[str, Any] = dict(known)
    with ThreadPoolExecutor(max_workers=arguments.jobs) as pool:
        for shard in pool.map(run_shard, shards):
            cells.update(shard)
    arguments.output.write_text(
        json.dumps(pack(arguments.commit, cells), sort_keys=True, separators=(",", ":")) + "\n"
    )
    for group, counter in sorted(summarize(cells).items()):
        print(group, dict(sorted(counter.items())))


if __name__ == "__main__":
    main()
