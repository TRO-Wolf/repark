"""R-TPCH-HARNESS — SF0.01 facade oracle battery (DuckDB differential).

Every query the SF1 scoreboard marks OK must pass DuckDB-diff at SF0.01 forever.
Queries marked ERROR are pinned as EXPECTED-ERROR with the error class (silent
regressions AND silent fixes both go red → ledger update).

DuckDB is hard-provisioned in the root ``dev`` group (``duckdb==1.5.5``) so the
gate venv runs this battery; ``importorskip`` remains so a bare venv degrades to
skip, never failure. This does **not** change the polars/pandas skip-based
precedent for their tests — duckdb is scoreboard-guard only.

Never touches AWS.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from repark import ReparkSession  # noqa: E402

# Harness imports (bench/ is not under repark_parity/src — load as package)
# test file lives at python/repark/tests/ → peer is python/repark-parity/bench/tpch
_TPCH_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "tpch"
_LEDGER_PATH = _TPCH_DIR / "sf1_status_ledger.json"


def _load_tpch_package() -> Any:
    import importlib
    import types

    package_name = "repark_tpch_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_TPCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.runner")


def _require_tpch_extension() -> None:
    """INSTALL+LOAD tpch; skip loudly if the extension repo is unreachable."""
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("INSTALL tpch")
        connection.execute("LOAD tpch")
    except Exception as exc:
        pytest.skip(f"DuckDB tpch extension unavailable (INSTALL tpch failed): {exc}")
    finally:
        connection.close()


def _load_sf1_ledger() -> dict[str, dict[str, str]]:
    """Return the per-query map from the SF1 ledger (handles nested provenance schema)."""
    if not _LEDGER_PATH.is_file():
        pytest.fail(
            f"SF1 status ledger missing at {_LEDGER_PATH}; "
            "run V1 scoreboard (run_tpch.py --sf 1 --ledger …) first"
        )
    payload = json.loads(_LEDGER_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        pytest.fail(f"SF1 ledger must be a JSON object; got {payload!r}")
    # Nested schema (octo C3-L-008): {scale_factor, queries: {...}}
    if "queries" in payload and isinstance(payload["queries"], dict):
        queries = payload["queries"]
        if payload.get("scale_factor") not in (1, 1.0, "1", "1.0"):
            pytest.fail(f"SF1 ledger scale_factor must be 1; got {payload.get('scale_factor')!r}")
    else:
        # Legacy flat map {"1": {"status": "OK"}, ...}
        queries = payload
    if len(queries) != 22:
        pytest.fail(f"SF1 ledger must map 22 queries; got {len(queries)}")
    return queries


def _collect_repark_rows(frame: Any) -> list[tuple[Any, ...]]:
    """Schema-name ordered rows (matches runner._repark_collect)."""
    if hasattr(frame, "to_arrow"):
        table = frame.to_arrow()
        names = list(table.column_names)
        return [tuple(row[name] for name in names) for row in table.to_pylist()]
    return [tuple(row) for row in frame.collect()]


@pytest.fixture(scope="module")
def tpch_sf001_paths(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Materialize SF0.01 parquet once per module under a private pytest cache.

    Never uses sticky ``/tmp/tpch-data`` (E1-SEC-001 — poisonable shared cache).
    """
    _require_tpch_extension()
    _load_tpch_package()
    import importlib

    datagen = importlib.import_module("repark_tpch_bench.datagen")
    root = tmp_path_factory.mktemp("tpch-data")
    return datagen.ensure_parquet_sf(0.01, data_root=root)


@pytest.fixture
def spark_tpch(tpch_sf001_paths: Path) -> Any:
    session = ReparkSession.builder.appName("pytest-tpch-smoke").getOrCreate()
    import importlib

    datagen = importlib.import_module("repark_tpch_bench.datagen")
    for table_name in datagen.TABLES:
        path = tpch_sf001_paths / f"{table_name}.parquet"
        session.read.parquet(str(path)).createOrReplaceTempView(table_name)
    yield session
    session.stop()


@pytest.fixture
def duck_tpch(tpch_sf001_paths: Path) -> Any:
    _require_tpch_extension()
    connection = duckdb.connect(database=":memory:")
    import importlib

    datagen = importlib.import_module("repark_tpch_bench.datagen")
    for table_name in datagen.TABLES:
        path = tpch_sf001_paths / f"{table_name}.parquet"
        path_sql = str(path).replace("'", "''")
        connection.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_sql}')"
        )
    yield connection
    connection.close()


def _query_numbers() -> list[int]:
    return list(range(1, 23))


@pytest.mark.parametrize("query_nr", _query_numbers())
def test_tpch_sf001_matches_sf1_ledger(
    query_nr: int,
    spark_tpch: Any,
    duck_tpch: Any,
) -> None:
    """Pin each Q against the SF1 scoreboard status (OK → diff; ERROR → expected class)."""
    _require_tpch_extension()
    ledger = _load_sf1_ledger()
    entry = ledger[str(query_nr)]
    expected_status = entry["status"]

    import importlib

    queries_mod = importlib.import_module("repark_tpch_bench.queries")
    compare_mod = importlib.import_module("repark_tpch_bench.compare")
    runner_mod = importlib.import_module("repark_tpch_bench.runner")

    all_queries = {query.query_nr: query for query in queries_mod.load_queries()}
    query = all_queries[query_nr]

    if expected_status == "OK":
        duck_rows = list(duck_tpch.execute(query.original_sql).fetchall())
        repark_rows = _collect_repark_rows(spark_tpch.sql(query.sql_for_repark))
        result = compare_mod.compare_result_sets(repark_rows, duck_rows)
        assert result.equal, (
            f"Q{query_nr} ledger=OK but SF0.01 DuckDB-diff failed: {result.message}"
        )
        return

    if expected_status == "WRONG-RESULT":
        # Still first-class: pin that we still disagree (do not silently become OK).
        duck_rows = list(duck_tpch.execute(query.original_sql).fetchall())
        try:
            repark_rows = _collect_repark_rows(spark_tpch.sql(query.sql_for_repark))
        except Exception as exc:
            pytest.fail(
                f"Q{query_nr} ledger=WRONG-RESULT but repark now raises "
                f"({type(exc).__name__}: {exc}); update sf1_status_ledger.json"
            )
        result = compare_mod.compare_result_sets(repark_rows, duck_rows)
        assert not result.equal, (
            f"Q{query_nr} ledger=WRONG-RESULT but SF0.01 now matches DuckDB; "
            "update sf1_status_ledger.json (silent fix)"
        )
        return

    if expected_status == "TIMEOUT":
        # TIMEOUT is scale/machine-dependent — do not require SF0.01 to raise (C3-L-007).
        # Still run SF0.01 DuckDB-diff so TIMEOUT cannot escape-hatch wrong results (C3-L-001).
        error_class = entry.get("error_class") or ""
        assert error_class.lower().startswith("timeout"), (
            f"Q{query_nr} ledger=TIMEOUT requires error_class Timeout*; got {error_class!r}"
        )
        duck_rows = list(duck_tpch.execute(query.original_sql).fetchall())
        repark_rows = _collect_repark_rows(spark_tpch.sql(query.sql_for_repark))
        result = compare_mod.compare_result_sets(repark_rows, duck_rows)
        assert result.equal, (
            f"Q{query_nr} ledger=TIMEOUT but SF0.01 DuckDB-diff failed "
            f"(correctness still required at SF0.01): {result.message}"
        )
        return

    if expected_status == "ERROR":
        expected_class = entry.get("error_class")
        try:
            repark_frame = spark_tpch.sql(query.sql_for_repark)
            if hasattr(repark_frame, "to_arrow"):
                _ = repark_frame.to_arrow()
            else:
                _ = repark_frame.collect()
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            error_class, _hint = runner_mod.classify_error(message)
            # Allow prefix-family match for Other(...) instability
            class_mismatch = (
                expected_class is not None
                and error_class != expected_class
                and not (expected_class.startswith("Other") and error_class.startswith("Other"))
            )
            if class_mismatch:
                pytest.fail(
                    f"Q{query_nr} EXPECTED-ERROR class {expected_class!r} "
                    f"but got {error_class!r}: {message}"
                )
            return
        pytest.fail(
            f"Q{query_nr} ledger=ERROR ({expected_class}) but SF0.01 "
            "now succeeds; update sf1_status_ledger.json (silent fix)"
        )

    pytest.fail(f"Q{query_nr}: unknown ledger status {expected_status!r}")


def test_sf1_ledger_covers_all_twenty_two_queries() -> None:
    """Mutation-proof: ledger must name every TPC-H query exactly once."""
    ledger = _load_sf1_ledger()
    assert set(ledger.keys()) == {str(number) for number in range(1, 23)}
    for key, entry in ledger.items():
        assert "status" in entry, f"Q{key} missing status"
        assert entry["status"] in {"OK", "WRONG-RESULT", "ERROR", "TIMEOUT"}
        if entry["status"] == "ERROR":
            assert entry.get("error_class"), (
                f"Q{key} ERROR requires non-empty error_class (C3-L-002)"
            )
        if entry["status"] == "TIMEOUT":
            assert (entry.get("error_class") or "").lower().startswith("timeout"), (
                f"Q{key} TIMEOUT requires error_class Timeout*"
            )
