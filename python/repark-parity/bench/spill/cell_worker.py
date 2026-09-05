"""One H3-SPILL-1 cell in its own process: bounded pool, address-space cap, JSON out."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sys
import time
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from spill.plan_metrics import parse_nodes, plan_text_from_rows  # noqa: E402
from spill.roster import BASE_COLUMNS, spec_for  # noqa: E402

CATALOG = "spill_cat"
NAMESPACE = "spill_ns"
RESOURCE_MARKERS: tuple[str, ...] = ("Resources exhausted", "ResourcesExhausted")
PANIC_MARKERS: tuple[str, ...] = ("a Rust panic was caught", "repark internal error")


def apply_as_cap(cap_bytes: int) -> None:
    """Cap this process's address space so a runaway cell dies instead of the box."""
    resource.setrlimit(resource.RLIMIT_AS, (cap_bytes, cap_bytes))


def peak_rss_bytes() -> int:
    """Peak resident set of this process, read from `VmHWM` because `ru_maxrss` survives exec."""
    with Path("/proc/self/status").open(encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    return 0


def build_session(pool: str, conf: dict[str, str], partitions: int) -> Any:
    """Build a facade session whose FairSpillPool is `pool` (`none` means unbounded)."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName("h3-spill-cell")
    builder = builder.config("datafusion.runtime.memory_limit", "0" if pool == "none" else pool)
    builder = builder.config("datafusion.execution.target_partitions", str(partitions))
    builder = builder.config("repark.sql.allowCreateFormatVersion3", "true")
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def register_base(session: Any, name: str, rows: int) -> None:
    """Register a wide deterministic view of `rows` rows under `name`."""
    session.range(rows).selectExpr(*BASE_COLUMNS).createOrReplaceTempView(name)


def digest_table(table: Any) -> str:
    """Stable digest of a small Arrow result (schema names plus python values)."""
    payload = json.dumps(
        {"columns": list(table.column_names), "rows": table.to_pylist()},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def classify(totals: dict[str, dict[str, int]]) -> str:
    """Read `spilled` / `degraded` / `ok` off the parsed plan metrics."""
    spills = sum(bucket.get("spill_count", 0) for bucket in totals.values())
    skipped = sum(bucket.get("skipped_aggregation_rows", 0) for bucket in totals.values())
    if spills > 0:
        return "spilled"
    if skipped > 0:
        return "degraded"
    return "ok"


def is_clean_resource_error(message: str) -> bool:
    """True when the engine surfaced a bounded-pool refusal rather than a crash."""
    return any(marker in message for marker in RESOURCE_MARKERS)


def is_caught_panic(message: str) -> bool:
    """True when a Rust panic reached the Python boundary instead of a typed error."""
    return any(marker in message for marker in PANIC_MARKERS)


def classify_failure(message: str) -> str:
    """A caught panic is `internal_error`; a pool refusal is `clean_error`."""
    if is_caught_panic(message):
        return "internal_error"
    if is_clean_resource_error(message):
        return "clean_error"
    return "error"


def explain_analyze(session: Any, sql: str) -> str:
    """Run `EXPLAIN ANALYZE` and return the whole plan text."""
    return plan_text_from_rows(session.sql(f"EXPLAIN ANALYZE {sql}").collect())


def run_sql_cell(spec: Any, pool: str, rows: int, partitions: int, digest: bool) -> dict[str, Any]:
    """Measure one SQL operator cell: metrics pass, then the optional answer probe."""
    session = build_session(pool, spec.conf, partitions)
    register_base(session, "base", rows)
    if spec.right_rows is not None:
        register_base(session, "other", spec.right_rows)
    started = time.perf_counter()
    plan_text = explain_analyze(session, spec.sql)
    wall_ms = (time.perf_counter() - started) * 1000.0
    totals = parse_nodes(plan_text)
    outcome = classify(totals)
    answer, digest_error = run_digest(session, spec.digest_sql if digest else None)
    return {
        "outcome": outcome,
        "wall_ms": wall_ms,
        "nodes": totals,
        "answer_digest": answer,
        "digest_error": digest_error,
        "plan_head": plan_text[:2000],
    }


def run_digest(session: Any, digest_sql: str | None) -> tuple[str | None, str | None]:
    """Run the small-output answer probe; a probe failure never masks the cell outcome."""
    if not digest_sql:
        return None, None
    try:
        return digest_table(session.sql(digest_sql).to_arrow()), None
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        return None, f"{type(error).__name__}: {error}"[:400]


def api_collect(session: Any, rows: int) -> dict[str, Any]:
    """The facade boundary: build python Row objects for every row."""
    register_base(session, "base", rows)
    collected = session.sql("SELECT id, h, g, payload, v FROM base").collect()
    return {"rows_out": len(collected)}


def api_to_pandas(session: Any, rows: int) -> dict[str, Any]:
    """The facade boundary: materialize the frame as pandas."""
    register_base(session, "base", rows)
    frame = session.sql("SELECT id, h, g, payload, v FROM base").toPandas()
    return {"rows_out": int(frame.shape[0])}


def api_dynamic_flatten(session: Any, rows: int) -> dict[str, Any]:
    """`dynamicFlatten` over a nested struct-and-list source of `rows` rows."""
    nested = session.range(rows).selectExpr(
        "id",
        "named_struct('a', id, 'b', md5(cast(id as string))) AS s",
        "array(md5(cast(id as string)), md5(cast(id + 1 as string))) AS l",
    )
    table = nested.dynamicFlatten().to_arrow()
    return {"rows_out": int(table.num_rows)}


def _iceberg_namespace(session: Any, warehouse: Path) -> str:
    """Register a private memory catalog under `warehouse` and return the table name."""
    from repark.spark._idents import sql_string_literal

    session.register_memory_catalog(CATALOG, str(warehouse))
    location = (warehouse / NAMESPACE).resolve()
    location.mkdir(parents=True, exist_ok=True)
    session.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE} "
        f"LOCATION {sql_string_literal(str(location))}"
    )
    return f"{CATALOG}.{NAMESPACE}.t"


def api_iceberg_scan_dv(session: Any, rows: int, warehouse: Path) -> dict[str, Any]:
    """CTAS a v3 MoR table, delete 1 %, then scan every row through the deletion vector."""
    table = _iceberg_namespace(session, warehouse)
    register_base(session, "base", rows)
    session.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ("
        "'format-version' = '3', 'write.delete.mode' = 'merge-on-read', "
        "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read') "
        "AS SELECT * FROM base"
    )
    session.sql(f"DELETE FROM {table} WHERE id % 100 = 0")
    scanned = session.sql(f"SELECT count(*) AS n, sum(id) AS s FROM {table}").to_arrow()
    return {"rows_out": int(scanned.to_pylist()[0]["n"])}


def api_merge_staging(session: Any, rows: int, warehouse: Path) -> dict[str, Any]:
    """MERGE a 1 % source into a copy-on-write target: the staging join under the pool."""
    table = _iceberg_namespace(session, warehouse)
    register_base(session, "base", rows)
    session.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ('format-version' = '2') "
        "AS SELECT * FROM base"
    )
    source = max(rows // 100, 1)
    session.range(source).selectExpr(
        "id * 3 AS id",
        "md5(cast(id as string)) AS h",
        "id % 1024 AS g",
        "concat(md5(cast(id as string)), md5(cast(id + 1 as string))) AS payload",
        "cast(id as double) * 2.5 AS v",
    ).createOrReplaceTempView("src")
    session.sql(
        f"MERGE INTO {table} t USING src s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = s.v "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    merged = session.sql(f"SELECT count(*) AS n FROM {table}").to_arrow()
    return {"rows_out": int(merged.to_pylist()[0]["n"])}


API_CELLS = {
    "collect": api_collect,
    "to_pandas": api_to_pandas,
    "dynamic_flatten": api_dynamic_flatten,
}

WAREHOUSE_CELLS = {
    "iceberg_scan_dv": api_iceberg_scan_dv,
    "merge_staging": api_merge_staging,
}


def run_api_cell(
    spec: Any, pool: str, rows: int, partitions: int, warehouse: Path
) -> dict[str, Any]:
    """Measure one facade or Iceberg cell: outcome from the call, no plan metrics."""
    session = build_session(pool, spec.conf, partitions)
    started = time.perf_counter()
    if spec.api in WAREHOUSE_CELLS:
        payload = WAREHOUSE_CELLS[spec.api](session, rows, warehouse)
    else:
        payload = API_CELLS[str(spec.api)](session, rows)
    payload["wall_ms"] = (time.perf_counter() - started) * 1000.0
    payload["outcome"] = "ok"
    payload["nodes"] = {}
    payload["answer_digest"] = str(payload.get("rows_out"))
    return payload


def _measure(args: argparse.Namespace) -> dict[str, Any]:
    """Dispatch one cell and fold every failure into a typed outcome."""
    spec = spec_for(args.operator)
    warehouse = Path(args.warehouse) if args.warehouse else Path.cwd()
    try:
        if spec.kind == "api":
            return run_api_cell(spec, args.pool, args.scale, args.partitions, warehouse)
        return run_sql_cell(spec, args.pool, args.scale, args.partitions, args.digest)
    except MemoryError as error:
        return {"outcome": "abort_at_cap", "message": f"MemoryError: {error}", "nodes": {}}
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        message = f"{type(error).__name__}: {error}"
        return {"outcome": classify_failure(message), "message": message, "nodes": {}}


def main(argv: list[str] | None = None) -> int:
    """CLI for one isolated spill-matrix cell."""
    parser = argparse.ArgumentParser(description="H3-SPILL-1 one-cell worker")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--pool", required=True)
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--as-cap-bytes", type=int, required=True)
    parser.add_argument("--warehouse", default="")
    parser.add_argument("--digest", action="store_true")
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args(argv)
    apply_as_cap(args.as_cap_bytes)
    payload = _measure(args)
    payload["operator"] = args.operator
    payload["pool"] = args.pool
    payload["scale"] = args.scale
    payload["as_cap_bytes"] = args.as_cap_bytes
    payload["peak_rss_bytes"] = peak_rss_bytes()
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
