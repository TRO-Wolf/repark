"""ICE-RESOURCE-BOUND-1 cost gate: the write set below the bound, base against head.

``cell`` runs one statement ``--runs`` times in this process and prints one JSON line with the
median wall time. ``rounds`` runs every cell in its own process for the base and the head
interpreters, alternating which goes first in each round, and writes the JSON rows and a
markdown table of the per-round medians.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel

CATALOG = "cost"
TABLE = f"{CATALOG}.w.t"
COLUMNS_DDL = "id BIGINT, k BIGINT, p INT, a DOUBLE, b DOUBLE, s STRING, c INT, flag BOOLEAN"
SELECT_LIST = (
    "id, pmod(id * 7919, {rows}) AS k, CAST(pmod(id, 32) AS INT) AS p, "
    "CAST(id AS DOUBLE) * 0.5 AS a, CAST(pmod(id * 13, 1000) AS DOUBLE) AS b, "
    "concat('s', lpad(CAST(pmod(id * 31, 100000000) AS STRING), 12, '0')) AS s, "
    "CAST(pmod(id, 1000) AS INT) AS c, pmod(id, 2) = 0 AS flag"
)
MOR = (
    ", 'write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
    "'write.delete.mode'='merge-on-read'"
)
MERGE = f"MERGE INTO {TABLE} t USING msrc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.a = s.a"
NOT_MATCHED_BY_SOURCE = " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.c = -1"
PINNING = ["taskset", "-c", "0-25"]
PROBE = "task/wo/ice-resource-bound-1/probe_cost.py"


class Cell(BaseModel):
    """One statement of the write set and the target it runs against."""

    name: str
    rows: int
    writers: int = 1
    ordered: bool = False
    partitioned: bool = False
    merge_on_read: bool = False
    seed_files: int = 1
    statement: str


CELLS: tuple[Cell, ...] = (
    Cell(name="ordered INSERT unpartitioned w1", rows=100_000, ordered=True, statement="append"),
    Cell(
        name="ordered INSERT 32 partitions w4",
        rows=100_000,
        writers=4,
        ordered=True,
        partitioned=True,
        statement="append",
    ),
    Cell(name="unordered INSERT unpartitioned w1", rows=100_000, statement="append"),
    Cell(
        name="unordered INSERT 32 partitions w4",
        rows=100_000,
        writers=4,
        partitioned=True,
        statement="append",
    ),
    Cell(
        name="MoR MERGE 10%",
        rows=200_000,
        merge_on_read=True,
        seed_files=32,
        statement=MERGE,
    ),
    Cell(
        name="MoR MERGE 10% + NOT MATCHED BY SOURCE",
        rows=200_000,
        merge_on_read=True,
        seed_files=32,
        statement=MERGE + NOT_MATCHED_BY_SOURCE,
    ),
    Cell(
        name="changelog count(id) 1M",
        rows=1_000_000,
        seed_files=10,
        statement="SELECT count(id) FROM t_changes",
    ),
    Cell(
        name="ordered UPDATE copy-on-write",
        rows=100_000,
        ordered=True,
        statement=f"UPDATE {TABLE} SET a = a + 1.0 WHERE id >= 0",
    ),
)


def read_load() -> float:
    """Return the one-minute load average."""
    return float(Path("/proc/loadavg").read_text().split()[0])


def build_session(cell: Cell, warehouse: Path) -> Any:
    """Build a facade session at the 8 GiB pool with the cell's writers and default keys."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("ice-resource-bound-1-cost")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("datafusion.runtime.memory_limit", "8192M")
        .config("repark.write.max-concurrent-files", str(cell.writers))
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return session


def prepare(session: Any, cell: Cell) -> int | None:
    """Create and seed the target; return the snapshot each timed run rolls back to."""
    session.range(cell.rows).createOrReplaceTempView("src_range")
    columns = SELECT_LIST.format(rows=cell.rows)
    session.sql(f"SELECT {columns} FROM src_range").createOrReplaceTempView("src")
    partition = " PARTITIONED BY (p)" if cell.partitioned else ""
    modes = MOR if cell.merge_on_read else ""
    session.sql(
        f"CREATE TABLE {TABLE} ({COLUMNS_DDL}) USING iceberg{partition} "
        f"TBLPROPERTIES ('format-version'='2'{modes})"
    )
    if cell.statement == "append":
        session.sql(f"INSERT INTO {TABLE} SELECT * FROM src WHERE id = 0").collect()
    else:
        step = cell.rows // cell.seed_files
        for index in range(cell.seed_files):
            start = index * step
            stop = cell.rows if index == cell.seed_files - 1 else start + step
            session.sql(
                f"INSERT INTO {TABLE} SELECT * FROM src WHERE id >= {start} AND id < {stop}"
            ).collect()
    if cell.ordered:
        session.sql(f"ALTER TABLE {TABLE} WRITE ORDERED BY k")
    session.sql(
        "SELECT id, a + 1.0 AS a FROM src WHERE pmod(id * 7, 100) < 10"
    ).createOrReplaceTempView("msrc")
    if cell.statement.startswith("SELECT"):
        session.sql(f"CALL {CATALOG}.system.create_changelog_view(table => 'w.t')").collect()
        return None
    return session.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()[-1][0]


def run_once(session: Any, cell: Cell, snapshot: int | None) -> float:
    """Run the timed statement once and roll the table back; return the wall time in ms."""
    started = time.perf_counter()
    if cell.statement == "append":
        session.sql("SELECT * FROM src").writeTo(TABLE).append()
    else:
        session.sql(cell.statement).collect()
    elapsed = (time.perf_counter() - started) * 1000.0
    if snapshot is not None:
        session.sql(
            f"CALL {CATALOG}.system.rollback_to_snapshot(table => 'w.t', snapshot_id => {snapshot})"
        ).collect()
    return elapsed


def run_cell(name: str, runs: int) -> dict[str, Any]:
    """Measure one cell in this process and return its JSON row."""
    cell = next(candidate for candidate in CELLS if candidate.name == name)
    load_start = read_load()
    with tempfile.TemporaryDirectory(prefix="rb-cost-") as scratch:
        session = build_session(cell, Path(scratch))
        snapshot = prepare(session, cell)
        run_once(session, cell, snapshot)
        timings = [run_once(session, cell, snapshot) for _ in range(runs)]
        session.stop()
    return {
        "cell": name,
        "median_ms": statistics.median(timings),
        "load_start": load_start,
        "load_end": read_load(),
    }


def measure(python: str, name: str, runs: int) -> dict[str, Any]:
    """Run one cell in its own process under ``python``; return the parsed JSON row."""
    command = [*PINNING, python, "-I", PROBE, "cell", "--name", name, "--runs", str(runs)]
    completed = subprocess.run(
        command, check=True, capture_output=True, text=True, env=dict(os.environ)
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def summarize(rows: list[dict[str, Any]]) -> str:
    """Return the markdown table of per-round medians, base against head."""
    lines = [
        "| cell | base ms | head ms | ratio | n | head slower |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for cell in CELLS:
        rounds = sorted({row["round"] for row in rows if row["cell"] == cell.name})
        base = [
            next(
                r
                for r in rows
                if r["round"] == n and r["cell"] == cell.name and r["side"] == "base"
            )
            for n in rounds
        ]
        head = [
            next(
                r
                for r in rows
                if r["round"] == n and r["cell"] == cell.name and r["side"] == "head"
            )
            for n in rounds
        ]
        base_median = statistics.median(row["median_ms"] for row in base)
        head_median = statistics.median(row["median_ms"] for row in head)
        slower = sum(1 for b, h in zip(base, head, strict=True) if h["median_ms"] > b["median_ms"])
        lines.append(
            f"| {cell.name} | {base_median:.2f} | {head_median:.2f} | "
            f"{head_median / base_median:.3f} | {len(rounds)} | {slower} |"
        )
    loads = [row["load_start"] for row in rows]
    lines.append("")
    lines.append(f"Start loads {min(loads):.2f} to {max(loads):.2f}.")
    return "\n".join(lines)


def run_rounds(base: str, head: str, rounds: int, runs: int, output: Path) -> None:
    """Alternate base and head per round over every cell; write the rows and the table."""
    rows: list[dict[str, Any]] = []
    for number in range(rounds):
        order = [("base", base), ("head", head)]
        if number % 2 == 1:
            order.reverse()
        for cell in CELLS:
            for side, python in order:
                row = measure(python, cell.name, runs)
                row.update({"round": number, "side": side})
                rows.append(row)
                print(json.dumps(row), flush=True)
        output.with_suffix(".json").write_text(json.dumps(rows, indent=1))
    output.with_suffix(".md").write_text(summarize(rows) + "\n")


def main() -> None:
    """Parse the subcommand and run it."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    cell = commands.add_parser("cell")
    cell.add_argument("--name", required=True)
    cell.add_argument("--runs", type=int, default=30)
    rounds = commands.add_parser("rounds")
    rounds.add_argument("--base-python", required=True)
    rounds.add_argument("--head-python", required=True)
    rounds.add_argument("--rounds", type=int, default=12)
    rounds.add_argument("--runs", type=int, default=30)
    rounds.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.command == "cell":
        print(json.dumps(run_cell(arguments.name, arguments.runs)))
        return
    run_rounds(
        arguments.base_python,
        arguments.head_python,
        arguments.rounds,
        arguments.runs,
        arguments.output,
    )


if __name__ == "__main__":
    main()
