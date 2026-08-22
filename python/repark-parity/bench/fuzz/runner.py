"""Fuzzer runner: load fixture → generate queries → RePark vs DuckDB → bank.

Statuses per query: OK | WRONG-RESULT | ERROR.

Env / CLI knobs (no time-based seeding):

- ``REPARK_FUZZ_SEED`` — integer seed (default 42)
- ``REPARK_FUZZ_N`` — query count (smoke default 200)
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from .bank import (
    BankedRepro,
    bank_repro,
    default_repros_dir,
    list_banked_repros,
    next_bank_sequence,
    write_corpus_index,
)
from .compare import compare_result_sets
from .datagen import FuzzDatabase, generate_database
from .generator import (
    DEFAULT_SEED,
    SMOKE_QUERY_COUNT,
    GeneratedQuery,
    generate_queries,
)
from .minimizer import MinimizedRepro, minimize_divergence

LOGGER = logging.getLogger(__name__)

StatusKind = Literal["OK", "WRONG-RESULT", "ERROR"]

ENV_SEED: Final[str] = "REPARK_FUZZ_SEED"
ENV_N: Final[str] = "REPARK_FUZZ_N"


class QueryOutcome(BaseModel):
    """Per-query fuzzer result."""

    model_config = ConfigDict(extra="forbid")

    index: int
    sql: str
    status: StatusKind
    has_order_by: bool
    repark_rows: int | None = None
    duckdb_rows: int | None = None
    message: str | None = None
    error_class: str | None = None
    wall_s: float | None = None
    minimized: bool = False
    banked_path: str | None = None
    pin_id: str | None = None


class FuzzRunResult(BaseModel):
    """Full fuzzer run census."""

    model_config = ConfigDict(extra="forbid")

    seed: int
    query_count: int
    environment: dict[str, str] = Field(default_factory=dict)
    outcomes: list[QueryOutcome] = Field(default_factory=list)
    banked: list[BankedRepro] = Field(default_factory=list)
    wall_s: float = 0.0

    @property
    def ok_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "OK")

    @property
    def wrong_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "WRONG-RESULT")

    @property
    def error_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == "ERROR")

    def census(self) -> dict[str, Any]:
        error_classes: dict[str, int] = {}
        for outcome in self.outcomes:
            if outcome.status == "ERROR" and outcome.error_class:
                error_classes[outcome.error_class] = error_classes.get(outcome.error_class, 0) + 1
        return {
            "seed": self.seed,
            "query_count": self.query_count,
            "ok": self.ok_count,
            "wrong_result": self.wrong_count,
            "error": self.error_count,
            "banked_repros": len(self.banked),
            "wall_s": round(self.wall_s, 3),
            "error_classes": dict(
                sorted(error_classes.items(), key=lambda item: (-item[1], item[0]))
            ),
            "wrong_indices": [o.index for o in self.outcomes if o.status == "WRONG-RESULT"],
        }

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "query_count": self.query_count,
            "environment": self.environment,
            "wall_s": self.wall_s,
            "census": self.census(),
            "outcomes": [outcome.model_dump() for outcome in self.outcomes],
            "banked": [item.model_dump() for item in self.banked],
        }


def resolve_seed(explicit: int | None = None) -> int:
    """Resolve seed: explicit arg > env > default 42. Never time-based.

    Seeds must be **non-negative** so bank filenames ``<seed>-<n>.sql`` stay
    unambiguous for ``list_banked_repros`` (C3-L-001 / C3-Q-002).
    """
    if explicit is not None:
        seed = int(explicit)
    else:
        env_value = os.environ.get(ENV_SEED)
        seed = int(env_value) if env_value is not None and env_value.strip() != "" else DEFAULT_SEED
    if seed < 0:
        msg = f"seed must be >= 0; got {seed}"
        raise ValueError(msg)
    return seed


def resolve_count(explicit: int | None = None, *, default: int = SMOKE_QUERY_COUNT) -> int:
    """Resolve query count: explicit arg > env REPARK_FUZZ_N > default."""
    if explicit is not None:
        return int(explicit)
    env_value = os.environ.get(ENV_N)
    if env_value is not None and env_value.strip() != "":
        return int(env_value)
    return default


def run_fuzzer(
    *,
    seed: int | None = None,
    count: int | None = None,
    bank: bool = True,
    repros_dir: Path | None = None,
    minimize: bool = True,
    stop_on_first_wrong: bool = False,
) -> FuzzRunResult:
    """Run the differential fuzzer end-to-end."""
    resolved_seed = resolve_seed(seed)
    resolved_count = resolve_count(count)
    if resolved_count < 0:
        msg = f"count must be >= 0; got {resolved_count}"
        raise ValueError(msg)

    started = time.perf_counter()
    database = generate_database(resolved_seed)
    queries = generate_queries(resolved_seed, resolved_count, database=database)

    result = FuzzRunResult(
        seed=resolved_seed,
        query_count=resolved_count,
        environment={
            "seed": str(resolved_seed),
            "count": str(resolved_count),
            "bank": str(bank),
            "minimize": str(minimize),
            "null_density_target": "0.18 draw (≥0.10 overall incl. non-null row_id)",
            "compare": "ints exact; non-integral floats 1e-6 rel; ORDER BY → order-sensitive",
        },
    )

    # Persistent sessions across the run for speed (re-register tables only when
    # the minimizer mutates the fixture for a specific repro).
    repark_session = _open_repark(database)
    duck_conn = _open_duckdb(database)
    # Continue sequence from on-disk corpus so re-runs do not clobber prior pins
    # (C4-L-001). ``result.banked`` still lists only this run's new files.
    bank_sequence = next_bank_sequence(repros_dir, seed=resolved_seed) - 1 if bank else 0

    try:
        for query in queries:
            outcome = _run_one(
                query=query,
                database=database,
                repark_session=repark_session,
                duck_conn=duck_conn,
            )
            if outcome.status == "WRONG-RESULT" and minimize:
                repro = _try_minimize(
                    seed=resolved_seed,
                    query=query,
                    database=database,
                )
                # Minimizer opens/stops its own getOrCreate session — that is the
                # process-wide singleton, so re-bind the runner session afterward.
                _close_repark(repark_session)
                repark_session = _open_repark(database)
                if repro is not None and bank:
                    bank_sequence += 1
                    banked = bank_repro(repro, repros_dir=repros_dir, sequence=bank_sequence)
                    result.banked.append(banked)
                    outcome.minimized = True
                    outcome.banked_path = banked.path
                    outcome.pin_id = banked.pin_id
                    LOGGER.warning(
                        "WRONG-RESULT seed=%s index=%s banked=%s msg=%s",
                        resolved_seed,
                        query.index,
                        banked.path,
                        outcome.message,
                    )
                elif repro is not None:
                    outcome.minimized = True
                    LOGGER.warning(
                        "WRONG-RESULT seed=%s index=%s (not banked) msg=%s",
                        resolved_seed,
                        query.index,
                        outcome.message,
                    )
            result.outcomes.append(outcome)
            if stop_on_first_wrong and outcome.status == "WRONG-RESULT":
                break
    finally:
        _close_repark(repark_session)
        _close_duckdb(duck_conn)

    result.wall_s = time.perf_counter() - started
    if bank:
        # Index the **full on-disk corpus**, not only this run (C4-L-002).
        index_dir = repros_dir if repros_dir is not None else default_repros_dir()
        write_corpus_index(
            list_banked_repros(index_dir),
            path=index_dir / "corpus_index.json",
        )
    return result


def _run_one(
    *,
    query: GeneratedQuery,
    database: FuzzDatabase,
    repark_session: Any,
    duck_conn: Any,
) -> QueryOutcome:
    del database  # tables already registered on the live sessions
    started = time.perf_counter()
    repark_rows: list[tuple[Any, ...]] | None = None
    duck_rows: list[tuple[Any, ...]] | None = None
    repark_err: str | None = None
    duck_err: str | None = None

    try:
        repark_rows = _repark_collect(repark_session, query.sql)
    except Exception as exc:
        repark_err = f"{type(exc).__name__}: {exc}"

    try:
        duck_rows = _duck_collect(duck_conn, query.sql)
    except Exception as exc:
        duck_err = f"{type(exc).__name__}: {exc}"

    wall_s = time.perf_counter() - started

    if repark_err is not None or duck_err is not None:
        parts = []
        if repark_err:
            parts.append(f"repark: {repark_err}")
        if duck_err:
            parts.append(f"duckdb: {duck_err}")
        message = " | ".join(parts)
        # Dual success is required for OK; any engine error is ERROR (engine-fix
        # out of scope — census only). If only one side errors, still ERROR.
        return QueryOutcome(
            index=query.index,
            sql=query.sql,
            status="ERROR",
            has_order_by=query.has_order_by,
            repark_rows=None if repark_rows is None else len(repark_rows),
            duckdb_rows=None if duck_rows is None else len(duck_rows),
            message=message,
            error_class=_classify_error(repark_err or duck_err or ""),
            wall_s=wall_s,
        )

    if repark_rows is None or duck_rows is None:
        # Both error paths already returned; this is a logic guard (C1-SAF-001 —
        # do not use bare assert, which is stripped under python -O).
        msg = "internal: expected both engines to return rows after error check"
        raise RuntimeError(msg)
    compare = compare_result_sets(
        repark_rows,
        duck_rows,
        order_sensitive=query.has_order_by,
    )
    if compare.equal:
        return QueryOutcome(
            index=query.index,
            sql=query.sql,
            status="OK",
            has_order_by=query.has_order_by,
            repark_rows=compare.repark_rows,
            duckdb_rows=compare.duckdb_rows,
            message="ok",
            wall_s=wall_s,
        )
    return QueryOutcome(
        index=query.index,
        sql=query.sql,
        status="WRONG-RESULT",
        has_order_by=query.has_order_by,
        repark_rows=compare.repark_rows,
        duckdb_rows=compare.duckdb_rows,
        message=compare.message,
        wall_s=wall_s,
    )


def _execute_minimize_pair(
    sql: str,
    db: FuzzDatabase,
    order_sensitive: bool,
) -> tuple[
    list[tuple[Any, ...]] | None,
    list[tuple[Any, ...]] | None,
    str | None,
    str | None,
]:
    """Run one candidate SQL on repark and DuckDB for the minimizer."""
    del order_sensitive
    repark_session = None
    duck_conn = None
    try:
        repark_session = _open_repark(db)
        duck_conn = _open_duckdb(db)
        try:
            repark_rows = _repark_collect(repark_session, sql)
            repark_err = None
        except Exception as exc:
            repark_rows = None
            repark_err = f"{type(exc).__name__}: {exc}"
        try:
            duck_rows = _duck_collect(duck_conn, sql)
            duck_err = None
        except Exception as exc:
            duck_rows = None
            duck_err = f"{type(exc).__name__}: {exc}"
        return repark_rows, duck_rows, repark_err, duck_err
    finally:
        if repark_session is not None:
            _close_repark(repark_session)
        if duck_conn is not None:
            _close_duckdb(duck_conn)


def _try_minimize(
    *,
    seed: int,
    query: GeneratedQuery,
    database: FuzzDatabase,
) -> MinimizedRepro | None:
    try:
        return minimize_divergence(
            seed=seed,
            query=query,
            database=database,
            execute=_execute_minimize_pair,
        )
    except Exception:
        LOGGER.exception("minimizer failed for seed=%s index=%s", seed, query.index)
        return None


# ---------------------------------------------------------------------------
# Engine adapters
# ---------------------------------------------------------------------------


def _open_repark(database: FuzzDatabase) -> Any:
    from repark import ReparkSession

    session = ReparkSession.builder.appName(f"sql-fuzzer-seed-{database.seed}").getOrCreate()
    for table_name, table in database.tables.items():
        frame = session.createDataFrame(list(table.rows), table.column_names)
        frame.createOrReplaceTempView(table_name)
    return session


def _close_repark(session: Any) -> None:
    try:
        session.stop()
    except Exception:
        LOGGER.debug("repark session stop failed:\n%s", traceback.format_exc())


def _open_duckdb(database: FuzzDatabase) -> Any:
    import duckdb

    connection = duckdb.connect(database=":memory:")
    for table_name, table in database.tables.items():
        _duck_register_table(connection, table_name, table.column_names, list(table.rows))
    return connection


def _close_duckdb(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        LOGGER.debug("duckdb close failed:\n%s", traceback.format_exc())


def _duck_register_table(
    connection: Any,
    table_name: str,
    column_names: list[str],
    rows: list[tuple[Any, ...]],
) -> None:
    """Create a DuckDB table from Python rows via Arrow (identical cell values)."""
    import pyarrow as pa

    connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    arrays: dict[str, list[Any]] = {name: [] for name in column_names}
    for row in rows:
        for name, value in zip(column_names, row, strict=True):
            arrays[name].append(value)
    arrow_table = pa.table(arrays)
    view_name = f"_fuzz_arrow_{table_name}"
    connection.register(view_name, arrow_table)
    connection.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM "{view_name}"')
    connection.unregister(view_name)


def _repark_collect(session: Any, sql: str) -> list[tuple[Any, ...]]:
    frame = session.sql(sql)
    if hasattr(frame, "to_arrow"):
        table = frame.to_arrow()
        names = list(table.column_names)
        return [tuple(row[name] for name in names) for row in table.to_pylist()]
    return [tuple(row) for row in frame.collect()]


def _duck_collect(connection: Any, sql: str) -> list[tuple[Any, ...]]:
    result = connection.execute(sql)
    rows = result.fetchall()
    return [tuple(row) for row in rows]


def _classify_error(message: str) -> str:
    lower = message.lower()
    patterns: list[tuple[str, str]] = [
        ("not implemented", "NotImplemented"),
        ("unsupported", "Unsupported"),
        ("syntax", "Syntax"),
        ("parse", "Parse"),
        ("binder", "Binder"),
        ("catalog", "Catalog"),
        ("schema", "Schema"),
        ("type", "Type"),
        ("column", "Column"),
        ("subquery", "Subquery"),
        ("aggregate", "Aggregate"),
        ("group", "GroupBy"),
        ("join", "Join"),
        ("overflow", "Overflow"),
        ("division by zero", "DivByZero"),
        ("null", "NullHandling"),
    ]
    for needle, label in patterns:
        if needle in lower:
            return label
    short = message.strip().split("\n", maxsplit=1)[0][:80]
    return f"Other({short})"
