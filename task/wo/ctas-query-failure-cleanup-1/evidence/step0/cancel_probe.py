"""Step-0 cancel cell: kill a CTAS mid-write after its begin file lands.

Spawns cancel_worker.py, waits for the begin file and the first data file,
terminates the worker, then records the files left behind plus the catalog
and path reads. Takes the warehouse root, the output path, the worker
interpreter and the worker script as arguments so no absolute path is
committed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from repark import ReparkSession

BEGIN_GLOB = "00000-*.metadata.json"
BEGIN_TIMEOUT_SECONDS = 300.0
DATA_TIMEOUT_SECONDS = 10.0
POLL_SECONDS = 0.05


def tree(root: Path) -> list[str] | str:
    """List every file under root relative to it, or ABSENT when missing."""
    if not root.exists():
        return "ABSENT"
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def scrub(text: str, warehouse: Path) -> str:
    """Replace machine paths in recorded text with stable placeholders."""
    return text.replace(str(warehouse), "<warehouse>").replace(str(Path.home()), "<home>")


def short_error(error: Exception, warehouse: Path) -> str:
    """Render one exception as a scrubbed single-line record."""
    first = str(error).splitlines()[0][:200] if str(error) else ""
    return scrub(f"{type(error).__name__}: {first}", warehouse)


def wait_for_begin(table_dir: Path) -> bool:
    """Wait until the begin file lands, or False on timeout or early exit."""
    meta = table_dir / "metadata"
    start = time.monotonic()
    while time.monotonic() - start < BEGIN_TIMEOUT_SECONDS:
        if meta.is_dir() and next(meta.glob(BEGIN_GLOB), None) is not None:
            return True
        time.sleep(POLL_SECONDS)
    return False


def wait_for_data(table_dir: Path) -> bool:
    """Wait until the first data file lands, or False on a short timeout."""
    data = table_dir / "data"
    start = time.monotonic()
    while time.monotonic() - start < DATA_TIMEOUT_SECONDS:
        if data.is_dir() and next(data.iterdir(), None) is not None:
            return True
        time.sleep(POLL_SECONDS)
    return False


def main(argv: list[str] | None = None) -> int:
    """Run the kill-after-begin probe and record what the kill leaves."""
    parser = argparse.ArgumentParser(description="Measure the leftover of a killed CTAS.")
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--table", type=str, default="cx1")
    parser.add_argument("--worker", type=Path, default=None)
    args = parser.parse_args(argv)
    worker = args.worker
    if worker is None:
        worker = Path(__file__).resolve().parent / "cancel_worker.py"
    warehouse = args.warehouse
    (warehouse / "hadoop").mkdir(parents=True, exist_ok=True)
    session = (
        ReparkSession.builder.appName("ctas-cleanup-step0-cancel-probe")
        .config("spark.sql.catalog.hr", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hr.type", "hadoop")
        .config("spark.sql.catalog.hr.warehouse", str(warehouse / "hadoop"))
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS hr.ns")
    session.sql(f"DROP TABLE IF EXISTS hr.ns.{args.table}")
    table_dir = warehouse / "hadoop" / "ns" / args.table
    if table_dir.exists():
        shutil.rmtree(table_dir)
    child = subprocess.Popen(
        [str(args.python), str(worker), "--warehouse", str(warehouse), "--table", args.table],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    began = wait_for_begin(table_dir)
    seen_data = wait_for_data(table_dir) if began else False
    child.terminate()
    try:
        output, _ = child.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        child.kill()
        output, _ = child.communicate(timeout=60)
    record: dict[str, Any] = {
        "cell": "cancel-hr-v2",
        "began": began,
        "seen_data": seen_data,
        "returncode": child.returncode,
        "worker_output": scrub(output[-500:] if output else "", warehouse),
        "files_after": tree(table_dir),
    }
    try:
        rows = session.sql(f"SELECT count(*) AS c FROM hr.ns.{args.table}")
        record["catalog"] = int(rows.to_arrow().to_pylist()[0]["c"])
    except Exception as error:
        record["catalog"] = short_error(error, warehouse)
    try:
        frame = session.read.format("iceberg").load(str(table_dir))
        record["path"] = {"columns": list(frame.columns), "count": int(frame.count())}
    except Exception as error:
        record["path"] = {"error": short_error(error, warehouse)}
    args.out.write_text(json.dumps(record, indent=1, default=str) + "\n")
    print(json.dumps(record, default=str), flush=True)
    return 0 if began else 1


if __name__ == "__main__":
    sys.exit(main())
